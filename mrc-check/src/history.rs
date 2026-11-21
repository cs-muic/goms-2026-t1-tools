//! The recorded history: every invocation and its outcome, plus schedule events.
//!
//! Times are microseconds since the start of the run, from a monotonic clock.
//! `inv` is taken just before the request is written and `resp` just after the
//! reply is read, so each recorded interval contains the real one.

use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};

use crate::resp::Reply;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Cmd {
    Set,
    Get,
    Rpush,
    Lrange,
    Llen,
}

impl Cmd {
    pub fn name(self) -> &'static str {
        match self {
            Cmd::Set => "SET",
            Cmd::Get => "GET",
            Cmd::Rpush => "RPUSH",
            Cmd::Lrange => "LRANGE",
            Cmd::Llen => "LLEN",
        }
    }
    pub fn is_list(self) -> bool {
        matches!(self, Cmd::Rpush | Cmd::Lrange | Cmd::Llen)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outcome {
    /// `+OK` to a SET.
    Ok,
    /// Reply to a GET (`None` is a nil bulk string).
    Value { value: Option<String> },
    /// Reply to RPUSH or LLEN.
    Int { n: i64 },
    /// Reply to LRANGE.
    List { items: Vec<String> },
    /// An error reply (`-ERR ...`). Writes with this outcome are treated as
    /// indeterminate: they may or may not have taken effect.
    Error { msg: String },
    /// A well-formed reply of the wrong type for the command. Reported as a
    /// violation; also indeterminate.
    Protocol { msg: String },
    /// Timeout or connection failure after the request may have been sent.
    /// Indeterminate.
    Unknown { msg: String },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Op {
    pub id: u64,
    pub client: u32,
    pub router: String,
    pub key: String,
    pub cmd: Cmd,
    /// Argument of SET / RPUSH.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub inv: u64,
    pub resp: u64,
    pub outcome: Outcome,
    /// True for the quiescent reads issued after the workload stops.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub final_read: bool,
}

impl Op {
    /// True if this write definitely took effect (an acknowledged SET/RPUSH).
    pub fn acked(&self) -> bool {
        matches!(
            (self.cmd, &self.outcome),
            (Cmd::Set, Outcome::Ok) | (Cmd::Rpush, Outcome::Int { .. })
        )
    }

    pub fn describe(&self) -> String {
        let arg = match &self.value {
            Some(v) => format!(" {v}"),
            None => match self.cmd {
                Cmd::Lrange => " 0 -1".into(),
                _ => String::new(),
            },
        };
        let out = match &self.outcome {
            Outcome::Ok => "OK".to_string(),
            Outcome::Value { value: Some(v) } => format!("{v:?}"),
            Outcome::Value { value: None } => "nil".into(),
            Outcome::Int { n } => format!("(integer) {n}"),
            Outcome::List { items } => summarize_list(items),
            Outcome::Error { msg } => format!("error {msg:?}"),
            Outcome::Protocol { msg } => format!("bad reply ({msg})"),
            Outcome::Unknown { msg } => format!("unknown ({msg})"),
        };
        format!(
            "#{} client {} via {}{}: {} {}{} [{} .. {}] -> {}",
            self.id,
            self.client,
            self.router,
            if self.final_read { " (final read)" } else { "" },
            self.cmd.name(),
            self.key,
            arg,
            secs(self.inv),
            secs(self.resp),
            out
        )
    }
}

fn summarize_list(items: &[String]) -> String {
    if items.len() <= 6 {
        format!("{items:?}")
    } else {
        format!(
            "[{} items: {:?}, {:?}, {:?}, ..., {:?}]",
            items.len(),
            items[0],
            items[1],
            items[2],
            items[items.len() - 1]
        )
    }
}

pub fn secs(us: u64) -> String {
    if us == u64::MAX {
        "never".into()
    } else {
        format!("{:.3}s", us as f64 / 1e6)
    }
}

/// A membership change, pause or resume performed by the scheduler.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Event {
    pub what: String,
    pub start: u64,
    pub end: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "rec", rename_all = "lowercase")]
pub enum Record {
    Op(Op),
    Event(Event),
}

#[derive(Default, Clone, Debug)]
pub struct History {
    pub ops: Vec<Op>,
    pub events: Vec<Event>,
}

impl History {
    pub fn write_jsonl<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        for e in &self.events {
            serde_json::to_writer(&mut w, &Record::Event(e.clone()))?;
            w.write_all(b"\n")?;
        }
        for op in &self.ops {
            serde_json::to_writer(&mut w, &Record::Op(op.clone()))?;
            w.write_all(b"\n")?;
        }
        w.flush()
    }

    pub fn read_jsonl<R: BufRead>(r: R) -> anyhow::Result<History> {
        let mut h = History::default();
        for (i, line) in r.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let rec: Record = serde_json::from_str(&line)
                .map_err(|e| anyhow::anyhow!("line {}: {e}", i + 1))?;
            match rec {
                Record::Op(o) => h.ops.push(o),
                Record::Event(e) => h.events.push(e),
            }
        }
        Ok(h)
    }
}

/// Turns a raw reply into an outcome for the given command.
pub fn classify(cmd: Cmd, reply: Reply) -> Outcome {
    let bad = |r: &Reply| Outcome::Protocol { msg: format!("{} returned {}", cmd.name(), r.describe()) };
    if let Reply::Error(msg) = reply {
        return Outcome::Error { msg };
    }
    match (cmd, &reply) {
        (Cmd::Set, Reply::Simple(s)) if s == "OK" => Outcome::Ok,
        (Cmd::Get, Reply::Bulk(b)) => Outcome::Value {
            value: b.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()),
        },
        (Cmd::Rpush | Cmd::Llen, Reply::Int(n)) => Outcome::Int { n: *n },
        (Cmd::Lrange, Reply::Array(Some(items))) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    Reply::Bulk(Some(b)) => out.push(String::from_utf8_lossy(b).into_owned()),
                    other => {
                        return Outcome::Protocol {
                            msg: format!("LRANGE item was {}", other.describe()),
                        }
                    }
                }
            }
            Outcome::List { items: out }
        }
        _ => bad(&reply),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_round_trip() {
        let h = History {
            ops: vec![Op {
                id: 1,
                client: 2,
                router: "r".into(),
                key: "k".into(),
                cmd: Cmd::Lrange,
                value: None,
                inv: 10,
                resp: 20,
                outcome: Outcome::List { items: vec!["a".into()] },
                final_read: true,
            }],
            events: vec![Event { what: "add x".into(), start: 1, end: 2, ok: true, detail: String::new() }],
        };
        let mut buf = Vec::new();
        h.write_jsonl(&mut buf).unwrap();
        let back = History::read_jsonl(&buf[..]).unwrap();
        assert_eq!(back.ops, h.ops);
        assert_eq!(back.events, h.events);
    }

    #[test]
    fn classify_replies() {
        assert_eq!(classify(Cmd::Set, Reply::Simple("OK".into())), Outcome::Ok);
        assert!(matches!(classify(Cmd::Set, Reply::Int(1)), Outcome::Protocol { .. }));
        assert_eq!(classify(Cmd::Get, Reply::Bulk(None)), Outcome::Value { value: None });
        assert_eq!(classify(Cmd::Rpush, Reply::Error("MOVED".into())), Outcome::Error { msg: "MOVED".into() });
        assert!(matches!(classify(Cmd::Lrange, Reply::Array(None)), Outcome::Protocol { .. }));
    }
}
