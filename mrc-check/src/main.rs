use std::io::BufReader;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use clap::{Parser, Subcommand};

use mrc_check::check;
use mrc_check::history::History;
use mrc_check::run::{run, summarize, RunArgs};

#[derive(Parser)]
#[command(name = "mrc-check", version, about = "Black-box correctness checker for Micro-Redis Cluster (Mini 3)")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a workload against the routers, change membership on schedule,
    /// and check the resulting history.
    Run(Box<RunArgs>),
    /// Re-check a history file written by `run`.
    Check {
        /// The JSON-lines history file.
        file: PathBuf,
        /// Show at most this many examples of each kind of violation.
        #[arg(long, default_value_t = 5)]
        max_examples: usize,
    },
}

fn report(h: &History, violations: &[check::Violation], max_examples: usize) {
    print!("{}", summarize(h));
    if violations.is_empty() {
        println!("safety checks: no violations found");
    } else {
        println!("safety checks: {} violation(s)", violations.len());
        print!("{}", check::render(violations, h, max_examples));
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Check { file, max_examples } => {
            let h = match std::fs::File::open(&file)
                .map_err(anyhow::Error::from)
                .and_then(|f| History::read_jsonl(BufReader::new(f)))
            {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("error: {}: {e}", file.display());
                    return ExitCode::from(2);
                }
            };
            let v = check::check(&h);
            report(&h, &v, max_examples);
            let failed_steps = h.events.iter().any(|e| !e.ok);
            if v.is_empty() && !failed_steps {
                println!("PASS");
                ExitCode::SUCCESS
            } else {
                println!("FAIL");
                ExitCode::from(1)
            }
        }
        Command::Run(args) => {
            let stopped: Arc<Mutex<Vec<i32>>> = Arc::default();
            let max_failed = args.max_failed_ops;
            let max_examples = args.max_examples;
            let res = tokio::select! {
                r = run(*args, stopped.clone()) => r,
                _ = tokio::signal::ctrl_c() => {
                    for pid in stopped.lock().unwrap().drain(..) {
                        let _ = mrc_check::schedule::signal(pid, "CONT");
                        eprintln!("resumed router pid {pid}");
                    }
                    eprintln!("interrupted");
                    return ExitCode::from(130);
                }
            };
            let res = match res {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: {e:#}");
                    return ExitCode::from(2);
                }
            };
            println!("run {}", res.run_id);
            report(&res.history, &res.violations, max_examples);
            if let Some(p) = &res.history_path {
                println!("history: {}", p.display());
            }
            let mut reasons = Vec::new();
            if !res.violations.is_empty() {
                reasons.push(format!("{} safety violation(s)", res.violations.len()));
            }
            if !res.schedule_ok {
                reasons.push("a schedule step failed".to_string());
            }
            if res.failed_ops > max_failed {
                reasons.push(format!(
                    "{} requests failed or got no reply (allowed: {max_failed}; see --max-failed-ops)",
                    res.failed_ops
                ));
            }
            if res.passed(max_failed) {
                println!("PASS");
                ExitCode::SUCCESS
            } else {
                println!("FAIL: {}", reasons.join("; "));
                ExitCode::from(1)
            }
        }
    }
}
