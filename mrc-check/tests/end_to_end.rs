//! Runs the full checker against an in-process fake "router": a correct one
//! must pass, deliberately broken ones must fail.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use clap::Parser;
use mrc_check::check::Kind;
use mrc_check::resp::{read_reply, Reply};
use mrc_check::run::{run, RunArgs};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

#[derive(Clone, Copy, PartialEq)]
enum Bug {
    None,
    /// Acknowledge every 20th write without applying it.
    LoseWrites,
    /// Every 10th GET returns the value before the current one.
    StaleReads,
    /// Every 15th RPUSH is inserted at the front instead of the back.
    ReorderPushes,
}

#[derive(Default)]
struct Store {
    strings: HashMap<Vec<u8>, (Vec<u8>, Option<Vec<u8>>)>, // (current, previous)
    lists: HashMap<Vec<u8>, Vec<Vec<u8>>>,
}

fn bulk(b: &[u8]) -> Vec<u8> {
    let mut v = format!("${}\r\n", b.len()).into_bytes();
    v.extend_from_slice(b);
    v.extend_from_slice(b"\r\n");
    v
}

fn handle(store: &Mutex<Store>, n: &AtomicU64, bug: Bug, args: &[Vec<u8>]) -> Vec<u8> {
    let i = n.fetch_add(1, Ordering::Relaxed);
    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let mut s = store.lock().unwrap();
    match (cmd.as_str(), args.len()) {
        ("PING", _) => b"+PONG\r\n".to_vec(),
        ("SET", 3) => {
            if !(bug == Bug::LoseWrites && i % 20 == 0) {
                let e = s.strings.entry(args[1].clone()).or_insert((Vec::new(), None));
                let old = std::mem::replace(&mut e.0, args[2].clone());
                e.1 = Some(old).filter(|o| !o.is_empty());
            }
            b"+OK\r\n".to_vec()
        }
        ("GET", 2) => match s.strings.get(&args[1]) {
            Some((_, Some(prev))) if bug == Bug::StaleReads && i % 10 == 0 => bulk(prev),
            Some((cur, _)) => bulk(cur),
            None => b"$-1\r\n".to_vec(),
        },
        ("RPUSH", 3) => {
            let l = s.lists.entry(args[1].clone()).or_default();
            if bug == Bug::LoseWrites && i % 20 == 0 {
                format!(":{}\r\n", l.len() + 1).into_bytes()
            } else {
                if bug == Bug::ReorderPushes && i % 15 == 0 {
                    l.insert(0, args[2].clone());
                } else {
                    l.push(args[2].clone());
                }
                format!(":{}\r\n", l.len()).into_bytes()
            }
        }
        ("LLEN", 2) => format!(":{}\r\n", s.lists.get(&args[1]).map_or(0, |l| l.len())).into_bytes(),
        ("LRANGE", 4) => {
            let l = s.lists.get(&args[1]).cloned().unwrap_or_default();
            let mut out = format!("*{}\r\n", l.len()).into_bytes();
            for x in &l {
                out.extend(bulk(x));
            }
            out
        }
        _ => b"-ERR unsupported\r\n".to_vec(),
    }
}

async fn fake_router(bug: Bug) -> String {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let store = Arc::new(Mutex::new(Store::default()));
    let n = Arc::new(AtomicU64::new(1));
    tokio::spawn(async move {
        loop {
            let (sock, _) = lis.accept().await.unwrap();
            let (store, n) = (store.clone(), n.clone());
            tokio::spawn(async move {
                let (r, mut w) = sock.into_split();
                let mut r = BufReader::new(r);
                while let Ok(Reply::Array(Some(items))) = read_reply(&mut r).await {
                    let args: Vec<Vec<u8>> = items
                        .into_iter()
                        .map(|it| match it {
                            Reply::Bulk(Some(b)) => b,
                            _ => Vec::new(),
                        })
                        .collect();
                    let out = handle(&store, &n, bug, &args);
                    if w.write_all(&out).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    addr
}

#[derive(Parser)]
struct P {
    #[command(flatten)]
    a: RunArgs,
}

fn args(extra: &[&str], dir: &std::path::Path) -> RunArgs {
    let hist = dir.join(format!("h-{}.jsonl", rand_suffix()));
    let mut v = vec![
        "mrc-check",
        "--clients",
        "8",
        "--duration",
        "1.5s",
        "--string-keys",
        "10",
        "--list-keys",
        "5",
        "--quiet",
    ];
    if !extra.contains(&"--schedule") {
        v.extend_from_slice(&["--schedule", ""]);
    }
    v.extend_from_slice(extra);
    let hist = hist.to_string_lossy().into_owned();
    let mut v: Vec<String> = v.into_iter().map(String::from).collect();
    v.push("--history".into());
    v.push(hist);
    P::parse_from(v).a
}

fn rand_suffix() -> u64 {
    static N: AtomicU64 = AtomicU64::new(0);
    std::process::id() as u64 * 1000 + N.fetch_add(1, Ordering::Relaxed)
}

fn tmpdir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("mrc-check-test-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn correct_server_passes_with_schedule_and_pause() {
    let r1 = fake_router(Bug::None).await;
    // A process we can safely SIGSTOP/SIGCONT, standing in for a router.
    let mut victim = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let pid = victim.id().to_string();
    let a = args(
        &[
            "--router",
            &r1,
            "--start-node",
            "echo starting >&2; echo 127.0.0.1:$((7000 + MRC_NODE_SEQ))",
            "--ctl",
            "true",
            "--node",
            "127.0.0.1:7000",
            "--router-pid",
            &pid,
            "--schedule",
            "stop,add,sleep:200ms,cont,remove,add",
        ],
        &tmpdir(),
    );
    let res = run(a, Arc::default()).await.unwrap();
    victim.kill().ok();
    assert!(res.violations.is_empty(), "{}", mrc_check::check::render(&res.violations, &res.history, 5));
    assert!(res.schedule_ok, "{:?}", res.history.events);
    assert_eq!(res.failed_ops, 0);
    let what: Vec<&str> = res.history.events.iter().map(|e| e.what.as_str()).collect();
    assert_eq!(
        what,
        [
            format!("SIGSTOP router pid {pid}").as_str(),
            "add-node 127.0.0.1:7001",
            format!("SIGCONT router pid {pid}").as_str(),
            "remove-node 127.0.0.1:7000",
            "add-node 127.0.0.1:7002",
        ]
    );
    assert!(res.history.ops.iter().any(|o| o.final_read));
    assert!(res.history.ops.len() > 100);
    // The history round-trips and re-checks clean.
    let f = std::fs::File::open(res.history_path.unwrap()).unwrap();
    let h = mrc_check::history::History::read_jsonl(std::io::BufReader::new(f)).unwrap();
    assert_eq!(h.ops.len(), res.history.ops.len());
    assert!(mrc_check::check::check(&h).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_routers_on_one_store_pass() {
    // Two listeners would be two stores; instead point both "routers" at one.
    let r = fake_router(Bug::None).await;
    let res = run(args(&["--router", &r, "--router", &r], &tmpdir()), Arc::default()).await.unwrap();
    assert!(res.violations.is_empty());
}

async fn kinds_for(bug: Bug) -> Vec<Kind> {
    let r = fake_router(bug).await;
    let res = run(args(&["--router", &r], &tmpdir()), Arc::default()).await.unwrap();
    let mut k: Vec<Kind> = res.violations.iter().map(|v| v.kind).collect();
    k.sort();
    k.dedup();
    k
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_writes_are_caught() {
    let k = kinds_for(Bug::LoseWrites).await;
    assert!(k.contains(&Kind::StaleRead) || k.contains(&Kind::LostWrite), "{k:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_reads_are_caught() {
    let k = kinds_for(Bug::StaleReads).await;
    assert!(k.contains(&Kind::StaleRead), "{k:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reordered_pushes_are_caught() {
    let k = kinds_for(Bug::ReorderPushes).await;
    assert!(!k.is_empty(), "{k:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_brain_is_caught() {
    // Two independent stores behind two "routers": keys served twice.
    let r1 = fake_router(Bug::None).await;
    let r2 = fake_router(Bug::None).await;
    let res = run(args(&["--router", &r1, "--router", &r2], &tmpdir()), Arc::default()).await.unwrap();
    assert!(!res.violations.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_ctl_fails_the_run() {
    let r = fake_router(Bug::None).await;
    let a = args(
        &["--router", &r, "--start-node", "echo 127.0.0.1:7001", "--ctl", "echo nope >&2; exit 1", "--schedule", "add"],
        &tmpdir(),
    );
    let res = run(a, Arc::default()).await.unwrap();
    assert!(!res.schedule_ok);
    assert!(!res.passed(0));
    assert!(res.history.events[0].detail.contains("nope"), "{:?}", res.history.events);
}
