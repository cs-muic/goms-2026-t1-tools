//! The `run` subcommand: workload, schedule, final reads, then the check.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::check::{self, Violation};
use crate::history::{classify, Cmd, History, Op, Outcome};
use crate::resp::{Conn, Reply};
use crate::schedule::{parse_duration, parse_schedule, resolve_pid, Scheduler, Step};

#[derive(clap::Args, Clone, Debug)]
pub struct RunArgs {
    /// Router address to send client traffic to. Repeat for several routers;
    /// clients are spread round-robin across them.
    #[arg(long = "router", value_name = "HOST:PORT", required = true)]
    pub routers: Vec<String>,

    /// Number of concurrent clients (one TCP connection each).
    #[arg(long, default_value_t = 16)]
    pub clients: usize,

    /// How long to run the workload. The run lasts longer if the schedule does.
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    pub duration: Duration,

    /// Number of string keys (SET/GET).
    #[arg(long, default_value_t = 100)]
    pub string_keys: usize,

    /// Number of list keys (RPUSH/LRANGE/LLEN).
    #[arg(long, default_value_t = 50)]
    pub list_keys: usize,

    /// Fraction of operations that go to list keys.
    #[arg(long, default_value_t = 0.5)]
    pub list_frac: f64,

    /// Stop pushing to a list once this many RPUSHes have been sent to it
    /// (reads continue), so LRANGE replies stay small.
    #[arg(long, default_value_t = 300)]
    pub max_list_len: usize,

    /// Give up on a request after this long and record its outcome as unknown.
    /// Keep it longer than any router pause so paused requests are answered.
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub op_timeout: Duration,

    /// Target total request rate across all clients, in requests per second
    /// (0 = as fast as possible). Keeps the history a manageable size.
    #[arg(long, default_value_t = 2000.0)]
    pub rate: f64,

    /// Shell command that starts a fresh, empty storage node in the background
    /// and prints its HOST:PORT as the last line of stdout, then exits.
    /// Gets MRC_NODE_SEQ=1,2,... in its environment.
    #[arg(long, value_name = "CMD")]
    pub start_node: Option<String>,

    /// Shell command prefix for cluster-ctl. The checker runs
    /// `<CMD> add-node <addr>` and `<CMD> remove-node <addr>` and expects exit
    /// status 0 once the change has finished.
    #[arg(long, value_name = "CMD")]
    pub ctl: Option<String>,

    /// Optional shell command run after a successful remove-node to shut the
    /// removed node down. Gets MRC_NODE_ADDR in its environment.
    #[arg(long, value_name = "CMD")]
    pub stop_node: Option<String>,

    /// A storage node already in the cluster at the start, so `remove` can
    /// pick it. Repeat for each initial node.
    #[arg(long = "node", value_name = "HOST:PORT")]
    pub nodes: Vec<String>,

    /// PID of a router that `stop` may SIGSTOP (and `cont` SIGCONT), or
    /// @FILE to read it from a pid file. Repeat to rotate among several.
    #[arg(long = "router-pid", value_name = "PID|@FILE")]
    pub router_pids: Vec<String>,

    /// Comma-separated steps run during the workload: sleep:<dur>, add,
    /// remove, remove:<addr>, stop, cont. Default depends on the other flags;
    /// pass "" for no membership changes (e.g. when changes are made by hand).
    #[arg(long)]
    pub schedule: Option<String>,

    /// Repeat the schedule until --duration has elapsed.
    #[arg(long)]
    pub repeat: bool,

    /// Timeout for each --start-node / --ctl / --stop-node command.
    #[arg(long, default_value = "300s", value_parser = parse_duration)]
    pub ctl_timeout: Duration,

    /// Where to write the history (JSON lines). Default:
    /// mrc-history-<run id>.jsonl in the current directory.
    #[arg(long, value_name = "FILE")]
    pub history: Option<PathBuf>,

    /// Fail the run if more than this many requests got an error reply or no
    /// reply. These don't affect the safety checks, only availability.
    #[arg(long, default_value_t = 0)]
    pub max_failed_ops: u64,

    /// Show at most this many examples of each kind of violation.
    #[arg(long, default_value_t = 5)]
    pub max_examples: usize,

    /// Random seed for the workload (default: random).
    #[arg(long)]
    pub seed: Option<u64>,

    /// Prefix for the keys the checker creates: <prefix>:<run id>:s<i> and
    /// <prefix>:<run id>:l<i>.
    #[arg(long, default_value = "mrc")]
    pub key_prefix: String,

    /// Less output while running.
    #[arg(long, short)]
    pub quiet: bool,
}

pub struct RunResult {
    pub run_id: String,
    pub history: History,
    pub violations: Vec<Violation>,
    pub schedule_ok: bool,
    pub failed_ops: u64,
    pub history_path: Option<PathBuf>,
}

impl RunResult {
    pub fn passed(&self, max_failed_ops: u64) -> bool {
        self.violations.is_empty() && self.schedule_ok && self.failed_ops <= max_failed_ops
    }
}

struct Shared {
    stop: AtomicBool,
    next_id: AtomicU64,
    done: AtomicU64,
    failed: AtomicU64,
    pushes: Vec<AtomicUsize>,
}

struct Keys {
    strings: Vec<String>,
    lists: Vec<String>,
}

fn now_us(t0: Instant) -> u64 {
    t0.elapsed().as_micros() as u64
}

fn default_schedule(a: &RunArgs) -> String {
    let can_add = a.start_node.is_some() && a.ctl.is_some();
    let can_stop = !a.router_pids.is_empty();
    match (can_add, can_stop) {
        (true, true) => "sleep:5,stop,add,sleep:2,cont,sleep:5,remove,sleep:5".into(),
        (true, false) => "sleep:5,add,sleep:5,remove,sleep:5".into(),
        (false, true) => "sleep:5,stop,sleep:5,cont,sleep:5".into(),
        (false, false) => String::new(),
    }
}

/// Sends one command and records it. Drops the connection on I/O errors.
#[allow(clippy::too_many_arguments)]
async fn do_op(
    conn: &mut Option<Conn>,
    t0: Instant,
    sh: &Shared,
    timeout: Duration,
    client: u32,
    router: &str,
    key: &str,
    cmd: Cmd,
    value: Option<String>,
    final_read: bool,
) -> Op {
    let c = conn.as_mut().expect("connected");
    let args: Vec<&[u8]> = match (cmd, &value) {
        (Cmd::Set, Some(v)) => vec![b"SET", key.as_bytes(), v.as_bytes()],
        (Cmd::Rpush, Some(v)) => vec![b"RPUSH", key.as_bytes(), v.as_bytes()],
        (Cmd::Get, _) => vec![b"GET", key.as_bytes()],
        (Cmd::Lrange, _) => vec![b"LRANGE", key.as_bytes(), b"0", b"-1"],
        (Cmd::Llen, _) => vec![b"LLEN", key.as_bytes()],
        _ => unreachable!(),
    };
    let id = sh.next_id.fetch_add(1, Ordering::Relaxed);
    let inv = now_us(t0);
    let res = tokio::time::timeout(timeout, c.call(&args)).await;
    let resp = now_us(t0);
    let outcome = match res {
        Ok(Ok(reply)) => classify(cmd, reply),
        Ok(Err(e)) => {
            *conn = None;
            Outcome::Unknown { msg: e.to_string() }
        }
        Err(_) => {
            *conn = None;
            Outcome::Unknown { msg: format!("no reply within {:?}", timeout) }
        }
    };
    sh.done.fetch_add(1, Ordering::Relaxed);
    if matches!(outcome, Outcome::Error { .. } | Outcome::Unknown { .. } | Outcome::Protocol { .. }) {
        sh.failed.fetch_add(1, Ordering::Relaxed);
    }
    Op { id, client, router: router.to_string(), key: key.to_string(), cmd, value, inv, resp, outcome, final_read }
}

async fn ensure_conn(conn: &mut Option<Conn>, router: &str, timeout: Duration) -> bool {
    if conn.is_some() {
        return true;
    }
    match tokio::time::timeout(timeout.min(Duration::from_secs(5)), Conn::connect(router)).await {
        Ok(Ok(c)) => {
            *conn = Some(c);
            true
        }
        _ => false,
    }
}

async fn client_loop(cid: u32, router: String, keys: Arc<Keys>, sh: Arc<Shared>, a: Arc<RunArgs>, seed: u64, t0: Instant) -> Vec<Op> {
    let mut rng = StdRng::seed_from_u64(seed ^ (cid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut ops = Vec::new();
    let mut conn: Option<Conn> = None;
    let mut seq = 0u64;
    // Each client paces itself at rate/clients, starting at a random offset.
    let gap = (a.rate > 0.0).then(|| Duration::from_secs_f64(a.clients as f64 / a.rate));
    let mut next = tokio::time::Instant::now() + gap.map_or(Duration::ZERO, |g| g.mul_f64(rng.gen()));
    while !sh.stop.load(Ordering::Relaxed) {
        if let Some(g) = gap {
            tokio::time::sleep_until(next).await;
            next = (next + g).max(tokio::time::Instant::now());
        }
        if !ensure_conn(&mut conn, &router, a.op_timeout).await {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        let use_list = if keys.lists.is_empty() {
            false
        } else if keys.strings.is_empty() {
            true
        } else {
            rng.gen_bool(a.list_frac.clamp(0.0, 1.0))
        };
        let (key, cmd, value) = if use_list {
            let k = rng.gen_range(0..keys.lists.len());
            let can_push = sh.pushes[k].load(Ordering::Relaxed) < a.max_list_len;
            let x: f64 = rng.gen();
            let cmd = if can_push && x < 0.5 {
                Cmd::Rpush
            } else if x < 0.8 || (!can_push && x < 0.7) {
                Cmd::Lrange
            } else {
                Cmd::Llen
            };
            if cmd == Cmd::Rpush {
                sh.pushes[k].fetch_add(1, Ordering::Relaxed);
            }
            (&keys.lists[k], cmd, None)
        } else {
            let k = rng.gen_range(0..keys.strings.len());
            let cmd = if rng.gen_bool(0.5) { Cmd::Set } else { Cmd::Get };
            (&keys.strings[k], cmd, None)
        };
        let value = match cmd {
            Cmd::Set | Cmd::Rpush => {
                seq += 1;
                Some(format!("c{cid}-{seq}"))
            }
            _ => value,
        };
        ops.push(do_op(&mut conn, t0, &sh, a.op_timeout, cid, &router, key, cmd, value, false).await);
    }
    ops
}

async fn final_reads(router: String, client: u32, keys: Arc<Keys>, sh: Arc<Shared>, timeout: Duration, t0: Instant) -> Vec<Op> {
    let mut ops = Vec::new();
    let mut conn = None;
    let all = keys.strings.iter().map(|k| (k, Cmd::Get)).chain(keys.lists.iter().map(|k| (k, Cmd::Lrange)));
    for (key, cmd) in all {
        let mut tries = 0;
        while !ensure_conn(&mut conn, &router, timeout).await {
            tries += 1;
            if tries > 10 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if conn.is_none() {
            continue;
        }
        ops.push(do_op(&mut conn, t0, &sh, timeout, client, &router, key, cmd, None, true).await);
    }
    ops
}

async fn preflight(routers: &[String]) -> Result<()> {
    for r in routers {
        let res = tokio::time::timeout(Duration::from_secs(5), async {
            let mut c = Conn::connect(r).await?;
            c.call(&[b"PING"]).await
        })
        .await;
        match res {
            Ok(Ok(Reply::Simple(s))) if s == "PONG" => {}
            Ok(Ok(other)) => bail!("router {r} answered PING with {}", other.describe()),
            Ok(Err(e)) => bail!("router {r}: {e}"),
            Err(_) => bail!("router {r} did not answer PING within 5s"),
        }
    }
    Ok(())
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

pub fn summarize(h: &History) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    let mut lat: Vec<u64> = Vec::new();
    let (mut ok, mut err, mut unk, mut bad) = (0, 0, 0, 0);
    let mut per_cmd: std::collections::BTreeMap<&str, u64> = Default::default();
    for o in h.ops.iter().filter(|o| !o.final_read) {
        *per_cmd.entry(o.cmd.name()).or_default() += 1;
        match o.outcome {
            Outcome::Error { .. } => err += 1,
            Outcome::Unknown { .. } => unk += 1,
            Outcome::Protocol { .. } => bad += 1,
            _ => {
                ok += 1;
                lat.push(o.resp - o.inv);
            }
        }
    }
    lat.sort_unstable();
    let span = h.ops.iter().filter(|o| !o.final_read).map(|o| o.resp).max().unwrap_or(0) as f64 / 1e6;
    let total = ok + err + unk + bad;
    let _ = writeln!(
        s,
        "workload: {total} ops in {span:.1}s ({:.0}/s): {}",
        if span > 0.0 { total as f64 / span } else { 0.0 },
        per_cmd.iter().map(|(k, n)| format!("{k} {n}")).collect::<Vec<_>>().join(", ")
    );
    let _ = writeln!(s, "outcomes: {ok} ok, {err} error replies, {unk} no reply (timeout/connection), {bad} malformed");
    let ms = |us: u64| us as f64 / 1e3;
    let _ = writeln!(
        s,
        "latency of ok ops: p50 {:.2}ms, p99 {:.2}ms, max {:.2}ms",
        ms(percentile(&lat, 0.5)),
        ms(percentile(&lat, 0.99)),
        ms(lat.last().copied().unwrap_or(0))
    );
    let mut errs: std::collections::BTreeMap<String, u64> = Default::default();
    for o in &h.ops {
        if let Outcome::Error { msg } | Outcome::Unknown { msg } = &o.outcome {
            let head: String = msg.chars().take(60).collect();
            *errs.entry(head).or_default() += 1;
        }
    }
    for (m, n) in errs.iter().rev().take(5) {
        let _ = writeln!(s, "  {n} x {m:?}");
    }
    let finals = h.ops.iter().filter(|o| o.final_read).count();
    let _ = writeln!(s, "final reads: {finals}");
    if !h.events.is_empty() {
        let _ = writeln!(s, "schedule:");
        for e in &h.events {
            let _ = writeln!(
                s,
                "  [{:>7.2}s .. {:>7.2}s] {}{}{}",
                e.start as f64 / 1e6,
                e.end as f64 / 1e6,
                e.what,
                if e.ok { "" } else { "  FAILED" },
                if e.detail.is_empty() { String::new() } else { format!(" ({})", e.detail) }
            );
        }
    }
    s
}

pub async fn run(a: RunArgs, stopped: Arc<Mutex<Vec<i32>>>) -> Result<RunResult> {
    if a.clients == 0 {
        bail!("--clients must be at least 1");
    }
    if a.string_keys + a.list_keys == 0 {
        bail!("need at least one key");
    }
    let schedule_text = a.schedule.clone().unwrap_or_else(|| default_schedule(&a));
    let steps: Vec<Step> = parse_schedule(&schedule_text)?;
    let pids = a.router_pids.iter().map(|p| resolve_pid(p)).collect::<Result<Vec<_>>>()?;

    let seed = a.seed.unwrap_or_else(rand::random);
    let run_id = format!("{:08x}", StdRng::seed_from_u64(seed).gen::<u32>() ^ rand::random::<u32>());
    let keys = Arc::new(Keys {
        strings: (0..a.string_keys).map(|i| format!("{}:{run_id}:s{i}", a.key_prefix)).collect(),
        lists: (0..a.list_keys).map(|i| format!("{}:{run_id}:l{i}", a.key_prefix)).collect(),
    });

    preflight(&a.routers).await?;

    let t0 = Instant::now();
    let mut sched = Scheduler::new(t0, stopped);
    sched.start_node = a.start_node.clone();
    sched.ctl = a.ctl.clone();
    sched.stop_node = a.stop_node.clone();
    sched.ctl_timeout = a.ctl_timeout;
    sched.members = a.nodes.clone();
    sched.pids = pids;
    sched.verbose = !a.quiet;
    sched.validate(&steps)?;

    let say = |m: String| {
        if !a.quiet {
            eprintln!("[{:>7.1}s] {m}", t0.elapsed().as_secs_f64());
        }
    };
    say(format!(
        "run {run_id}: {} clients on {} router(s), {} string + {} list keys, schedule {:?}",
        a.clients,
        a.routers.len(),
        a.string_keys,
        a.list_keys,
        schedule_text
    ));

    let sh = Arc::new(Shared {
        stop: AtomicBool::new(false),
        next_id: AtomicU64::new(1),
        done: AtomicU64::new(0),
        failed: AtomicU64::new(0),
        pushes: (0..a.list_keys).map(|_| AtomicUsize::new(0)).collect(),
    });
    let args = Arc::new(a.clone());
    let mut clients = Vec::new();
    for cid in 0..a.clients {
        let router = a.routers[cid % a.routers.len()].clone();
        clients.push(tokio::spawn(client_loop(cid as u32, router, keys.clone(), sh.clone(), args.clone(), seed, t0)));
    }
    let ticker = {
        let sh = sh.clone();
        let quiet = a.quiet;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if !quiet {
                    eprintln!(
                        "[{:>7.1}s] {} ops, {} failed",
                        t0.elapsed().as_secs_f64(),
                        sh.done.load(Ordering::Relaxed),
                        sh.failed.load(Ordering::Relaxed)
                    );
                }
            }
        })
    };

    let deadline = t0 + a.duration;
    let schedule_ok = sched.run(&steps, a.repeat, deadline).await;
    let now = Instant::now();
    if now < deadline {
        tokio::time::sleep(deadline - now).await;
    } else if !steps.is_empty() {
        // Keep traffic going briefly after a schedule that overran.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    sh.stop.store(true, Ordering::Relaxed);
    say("stopping clients".into());
    let mut history = History::default();
    for c in clients {
        history.ops.extend(c.await?);
    }
    ticker.abort();
    sched.cont_all();

    say("final reads".into());
    let mut finals = Vec::new();
    for (i, r) in a.routers.iter().enumerate() {
        finals.push(tokio::spawn(final_reads(r.clone(), (a.clients + i) as u32, keys.clone(), sh.clone(), a.op_timeout, t0)));
    }
    for f in finals {
        history.ops.extend(f.await?);
    }
    history.ops.sort_by_key(|o| o.id);
    history.events = std::mem::take(&mut sched.events);

    let failed_ops = history
        .ops
        .iter()
        .filter(|o| matches!(o.outcome, Outcome::Error { .. } | Outcome::Unknown { .. }))
        .count() as u64;

    let path = a.history.clone().unwrap_or_else(|| PathBuf::from(format!("mrc-history-{run_id}.jsonl")));
    let history_path = match std::fs::File::create(&path) {
        Ok(f) => {
            history.write_jsonl(std::io::BufWriter::new(f))?;
            Some(path)
        }
        Err(e) => {
            eprintln!("warning: could not write history to {}: {e}", path.display());
            None
        }
    };

    say("checking".into());
    let violations = check::check(&history);
    Ok(RunResult { run_id, history, violations, schedule_ok, failed_ops, history_path })
}
