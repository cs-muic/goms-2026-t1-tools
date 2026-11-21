//! The membership/fault schedule run alongside the workload.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::history::Event;

#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    Sleep(Duration),
    Add,
    Remove(Option<String>),
    Stop,
    Cont,
}

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1e-3)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60.0)
    } else {
        (s, 1.0)
    };
    let x: f64 = num.trim().parse().map_err(|_| format!("bad duration {s:?} (try 5, 2.5s, 500ms or 1m)"))?;
    if !(x >= 0.0 && x.is_finite()) {
        return Err(format!("bad duration {s:?}"));
    }
    Ok(Duration::from_secs_f64(x * mult))
}

/// Parses e.g. `sleep:5,stop,add,sleep:2,cont,remove,remove:127.0.0.1:7001`.
pub fn parse_schedule(s: &str) -> Result<Vec<Step>> {
    let mut steps = Vec::new();
    for raw in s.split(',') {
        let item = raw.trim();
        if item.is_empty() {
            continue;
        }
        let (name, arg) = match item.split_once(':') {
            Some((n, a)) => (n, Some(a)),
            None => (item, None),
        };
        let step = match (name, arg) {
            ("sleep", Some(a)) => Step::Sleep(parse_duration(a).map_err(|e| anyhow!(e))?),
            ("add", None) => Step::Add,
            ("remove", None) => Step::Remove(None),
            ("remove", Some(a)) if !a.is_empty() => Step::Remove(Some(a.to_string())),
            ("stop", None) => Step::Stop,
            ("cont", None) => Step::Cont,
            _ => bail!("bad schedule step {item:?}; expected sleep:<dur>, add, remove, remove:<addr>, stop or cont"),
        };
        steps.push(step);
    }
    Ok(steps)
}

pub struct ShellOut {
    pub status: Option<i32>,
    pub timed_out: bool,
    pub stdout: String,
    pub stderr: String,
}

impl ShellOut {
    pub fn success(&self) -> bool {
        self.status == Some(0) && !self.timed_out
    }
    pub fn summary(&self) -> String {
        let why = if self.timed_out {
            "timed out".to_string()
        } else {
            match self.status {
                Some(c) => format!("exit status {c}"),
                None => "killed by a signal".into(),
            }
        };
        let err = self.stderr.trim();
        let out = self.stdout.trim();
        let tail = if !err.is_empty() { err } else { out };
        let tail: String = tail.lines().rev().take(5).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join(" | ");
        if tail.is_empty() {
            why
        } else {
            format!("{why}: {tail}")
        }
    }
}

/// Runs `sh -c script mrc-check args...`. Returns when the shell itself exits
/// (not when its stdout closes), so a background process started by the script
/// that keeps the pipe open does not hang us.
pub async fn run_shell(script: &str, args: &[&str], env: &[(&str, &str)], timeout: Duration) -> Result<ShellOut> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(script).arg("mrc-check").args(args);
    for (k, val) in env {
        cmd.env(k, val);
    }
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().with_context(|| format!("spawning sh -c {script:?}"))?;
    let buf_out = Arc::new(Mutex::new(Vec::new()));
    let buf_err = Arc::new(Mutex::new(Vec::new()));
    let mut readers = Vec::new();
    for (pipe, buf) in [
        (child.stdout.take().map(|p| Box::new(p) as Box<dyn tokio::io::AsyncRead + Unpin + Send>), buf_out.clone()),
        (child.stderr.take().map(|p| Box::new(p) as Box<dyn tokio::io::AsyncRead + Unpin + Send>), buf_err.clone()),
    ] {
        if let Some(mut pipe) = pipe {
            readers.push(tokio::spawn(async move {
                let mut chunk = [0u8; 4096];
                while let Ok(n) = pipe.read(&mut chunk).await {
                    if n == 0 {
                        break;
                    }
                    buf.lock().unwrap().extend_from_slice(&chunk[..n]);
                }
            }));
        }
    }
    let (status, timed_out) = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(st) => (st?.code(), false),
        Err(_) => {
            let _ = child.kill().await;
            (None, true)
        }
    };
    for r in readers {
        if tokio::time::timeout(Duration::from_millis(300), r).await.is_err() {
            // A background child still holds the pipe; keep what we have.
        }
    }
    let take = |b: &Arc<Mutex<Vec<u8>>>| String::from_utf8_lossy(&b.lock().unwrap()).into_owned();
    Ok(ShellOut { status, timed_out, stdout: take(&buf_out), stderr: take(&buf_err) })
}

/// Reads a PID given as a number or as `@path/to/pidfile`.
pub fn resolve_pid(s: &str) -> Result<i32> {
    let text = match s.strip_prefix('@') {
        Some(path) => std::fs::read_to_string(path).with_context(|| format!("reading pid file {path}"))?,
        None => s.to_string(),
    };
    let pid: i32 = text.trim().parse().with_context(|| format!("bad pid {:?}", text.trim()))?;
    if pid <= 1 {
        bail!("refusing to signal pid {pid}");
    }
    Ok(pid)
}

pub fn signal(pid: i32, sig: &str) -> Result<()> {
    let st = std::process::Command::new("kill")
        .arg(format!("-{sig}"))
        .arg(pid.to_string())
        .status()
        .context("running kill")?;
    if !st.success() {
        bail!("kill -{sig} {pid} failed");
    }
    Ok(())
}

pub struct Scheduler {
    pub start_node: Option<String>,
    pub ctl: Option<String>,
    pub stop_node: Option<String>,
    pub ctl_timeout: Duration,
    /// Storage nodes the checker believes are members, oldest first.
    pub members: Vec<String>,
    pub pids: Vec<i32>,
    /// Router PIDs currently stopped; shared so an interrupted run can resume them.
    pub stopped: Arc<Mutex<Vec<i32>>>,
    pub t0: Instant,
    pub verbose: bool,
    next_pid: usize,
    node_seq: usize,
    pub events: Vec<Event>,
}

impl Scheduler {
    pub fn new(t0: Instant, stopped: Arc<Mutex<Vec<i32>>>) -> Scheduler {
        Scheduler {
            start_node: None,
            ctl: None,
            stop_node: None,
            ctl_timeout: Duration::from_secs(300),
            members: Vec::new(),
            pids: Vec::new(),
            stopped,
            t0,
            verbose: true,
            next_pid: 0,
            node_seq: 0,
            events: Vec::new(),
        }
    }

    fn now(&self) -> u64 {
        self.t0.elapsed().as_micros() as u64
    }

    fn log(&self, msg: &str) {
        if self.verbose {
            eprintln!("[{:>7.1}s] {msg}", self.t0.elapsed().as_secs_f64());
        }
    }

    /// Checks up front that the steps can be carried out with the given options.
    pub fn validate(&self, steps: &[Step]) -> Result<()> {
        for s in steps {
            match s {
                Step::Add if self.start_node.is_none() || self.ctl.is_none() => {
                    bail!("schedule step `add` needs --start-node and --ctl")
                }
                Step::Remove(_) if self.ctl.is_none() => bail!("schedule step `remove` needs --ctl"),
                Step::Stop if self.pids.is_empty() => bail!("schedule step `stop` needs --router-pid"),
                _ => {}
            }
        }
        Ok(())
    }

    async fn ctl(&self, sub: &str, addr: &str) -> Result<ShellOut> {
        let script = format!("{} \"$@\"", self.ctl.as_deref().unwrap());
        run_shell(&script, &[sub, addr], &[], self.ctl_timeout).await
    }

    fn record(&mut self, what: String, start: u64, ok: bool, detail: String) {
        let end = self.now();
        let dur = (end - start) as f64 / 1e6;
        if ok {
            self.log(&format!("{what}: done in {dur:.2}s"));
        } else {
            self.log(&format!("{what}: FAILED after {dur:.2}s: {detail}"));
        }
        self.events.push(Event { what, start, end, ok, detail });
    }

    async fn step(&mut self, step: &Step) -> bool {
        match step {
            Step::Sleep(d) => {
                tokio::time::sleep(*d).await;
                true
            }
            Step::Add => {
                let start = self.now();
                self.node_seq += 1;
                let seq = self.node_seq.to_string();
                self.log(&format!("starting storage node #{seq}"));
                let out = run_shell(
                    self.start_node.as_deref().unwrap(),
                    &[],
                    &[("MRC_NODE_SEQ", &seq)],
                    self.ctl_timeout,
                )
                .await;
                let addr = match out {
                    Ok(o) if o.success() => match o.stdout.lines().map(str::trim).rfind(|l| !l.is_empty()) {
                        Some(a) if a.contains(':') => a.to_string(),
                        _ => {
                            self.record(
                                format!("start-node #{seq}"),
                                start,
                                false,
                                format!("expected the node's host:port as the last line of stdout, got {:?}", o.stdout.trim()),
                            );
                            return false;
                        }
                    },
                    Ok(o) => {
                        self.record(format!("start-node #{seq}"), start, false, o.summary());
                        return false;
                    }
                    Err(e) => {
                        self.record(format!("start-node #{seq}"), start, false, e.to_string());
                        return false;
                    }
                };
                self.log(&format!("add-node {addr}"));
                let start = self.now();
                let what = format!("add-node {addr}");
                match self.ctl("add-node", &addr).await {
                    Ok(o) if o.success() => {
                        self.members.push(addr);
                        self.record(what, start, true, String::new());
                        true
                    }
                    Ok(o) => {
                        self.record(what, start, false, o.summary());
                        false
                    }
                    Err(e) => {
                        self.record(what, start, false, e.to_string());
                        false
                    }
                }
            }
            Step::Remove(explicit) => {
                let addr = match explicit {
                    Some(a) => a.clone(),
                    None if self.members.len() >= 2 => self.members[0].clone(),
                    None => {
                        let now = self.now();
                        self.record(
                            "remove-node".into(),
                            now,
                            false,
                            format!(
                                "the checker knows {} member(s) and won't remove the last one; pass the initial nodes with --node",
                                self.members.len()
                            ),
                        );
                        return false;
                    }
                };
                self.log(&format!("remove-node {addr}"));
                let start = self.now();
                let what = format!("remove-node {addr}");
                let ok = match self.ctl("remove-node", &addr).await {
                    Ok(o) if o.success() => {
                        self.members.retain(|m| *m != addr);
                        self.record(what, start, true, String::new());
                        true
                    }
                    Ok(o) => {
                        self.record(what, start, false, o.summary());
                        false
                    }
                    Err(e) => {
                        self.record(what, start, false, e.to_string());
                        false
                    }
                };
                if ok {
                    if let Some(cmd) = self.stop_node.clone() {
                        let start = self.now();
                        let what = format!("stop-node {addr}");
                        match run_shell(&cmd, &[], &[("MRC_NODE_ADDR", &addr)], self.ctl_timeout).await {
                            Ok(o) if o.success() => self.record(what, start, true, String::new()),
                            // The cluster already let go of the node; a failing
                            // cleanup command is worth a warning, not a failed run.
                            Ok(o) => self.record(what, start, true, format!("warning: {}", o.summary())),
                            Err(e) => self.record(what, start, true, format!("warning: {e}")),
                        }
                    }
                }
                ok
            }
            Step::Stop => {
                let pid = self.pids[self.next_pid % self.pids.len()];
                self.next_pid += 1;
                let start = self.now();
                if self.stopped.lock().unwrap().contains(&pid) {
                    self.record(format!("SIGSTOP router pid {pid}"), start, true, "already stopped".into());
                    return true;
                }
                match signal(pid, "STOP") {
                    Ok(()) => {
                        self.stopped.lock().unwrap().push(pid);
                        self.record(format!("SIGSTOP router pid {pid}"), start, true, String::new());
                        true
                    }
                    Err(e) => {
                        self.record(format!("SIGSTOP router pid {pid}"), start, false, e.to_string());
                        false
                    }
                }
            }
            Step::Cont => {
                self.cont_all();
                true
            }
        }
    }

    /// Resumes every router this scheduler stopped.
    pub fn cont_all(&mut self) {
        let pids: Vec<i32> = std::mem::take(&mut *self.stopped.lock().unwrap());
        for pid in pids {
            let start = self.now();
            let res = signal(pid, "CONT");
            let ok = res.is_ok();
            self.record(format!("SIGCONT router pid {pid}"), start, ok, res.err().map(|e| e.to_string()).unwrap_or_default());
        }
    }

    /// Runs the steps (repeatedly, if `repeat`, until `deadline`). Stops at the
    /// first failed step. Always resumes stopped routers before returning.
    pub async fn run(&mut self, steps: &[Step], repeat: bool, deadline: Instant) -> bool {
        let mut ok = true;
        'outer: loop {
            for s in steps {
                if !self.step(s).await {
                    ok = false;
                    break 'outer;
                }
            }
            if !repeat || steps.is_empty() || Instant::now() >= deadline {
                break;
            }
        }
        self.cont_all();
        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_schedules() {
        assert_eq!(
            parse_schedule("sleep:5, stop,add,sleep:500ms,cont,remove,remove:10.0.0.1:7001").unwrap(),
            vec![
                Step::Sleep(Duration::from_secs(5)),
                Step::Stop,
                Step::Add,
                Step::Sleep(Duration::from_millis(500)),
                Step::Cont,
                Step::Remove(None),
                Step::Remove(Some("10.0.0.1:7001".into())),
            ]
        );
        assert!(parse_schedule("").unwrap().is_empty());
        assert!(parse_schedule("sleep").is_err());
        assert!(parse_schedule("explode").is_err());
    }

    #[tokio::test]
    async fn shell_does_not_wait_for_background_children() {
        let t = Instant::now();
        let out = run_shell("sleep 5 & echo 127.0.0.1:9; echo $MRC_NODE_SEQ", &[], &[("MRC_NODE_SEQ", "3")], Duration::from_secs(10))
            .await
            .unwrap();
        assert!(out.success());
        assert_eq!(out.stdout.trim(), "127.0.0.1:9\n3");
        assert!(t.elapsed() < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn shell_passes_args_safely() {
        let out = run_shell("printf '%s|' \"$@\"", &["add-node", "a b;echo pwned"], &[], Duration::from_secs(5)).await.unwrap();
        assert_eq!(out.stdout, "add-node|a b;echo pwned|");
        let out = run_shell("exit 3", &[], &[], Duration::from_secs(5)).await.unwrap();
        assert_eq!(out.status, Some(3));
        let out = run_shell("sleep 5", &[], &[], Duration::from_millis(200)).await.unwrap();
        assert!(out.timed_out && !out.success());
    }
}
