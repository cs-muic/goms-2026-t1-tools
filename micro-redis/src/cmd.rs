//! Parsing requests into commands, and running them.
//!
//! To add a command (say, an internal one for moving keys between nodes),
//! add a variant to [`Command`], parse it in [`Command::parse`], and run it in
//! [`Command::execute`].

use bytes::Bytes;

use crate::db::{Db, End, WrongType};
use crate::resp::Frame;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Ping(Option<Bytes>),
    Select(i64),
    Get(Bytes),
    Set(Bytes, Bytes),
    Exists(Vec<Bytes>),
    Push { key: Bytes, values: Vec<Bytes>, end: End },
    LRange { key: Bytes, start: i64, stop: i64 },
    LLen(Bytes),
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_AN_INTEGER: &str = "ERR value is not an integer or out of range";

impl Command {
    /// Turn a request frame into a command. On failure, the error is the
    /// reply to send back.
    pub fn parse(frame: Frame) -> Result<Command, Frame> {
        let args = into_args(frame)?;
        let Some(name) = args.first() else {
            return Err(Frame::error("ERR empty command"));
        };
        let name = String::from_utf8_lossy(name).to_ascii_lowercase();
        let n = args.len();
        let mut args = args.into_iter().skip(1);
        let mut next = || args.next().expect("arity was checked");

        let cmd = match name.as_str() {
            "ping" if n <= 2 => Command::Ping(if n == 2 { Some(next()) } else { None }),
            "select" if n == 2 => Command::Select(int(&next())?),
            "get" if n == 2 => Command::Get(next()),
            "set" if n == 3 => Command::Set(next(), next()),
            // Real SET takes options (EX, NX, ...); we don't.
            "set" if n > 3 => return Err(Frame::error("ERR syntax error")),
            "exists" if n >= 2 => Command::Exists(args.collect()),
            "rpush" | "lpush" if n >= 3 => Command::Push {
                key: next(),
                values: args.collect(),
                end: if name == "lpush" { End::Left } else { End::Right },
            },
            "lrange" if n == 4 => Command::LRange { key: next(), start: int(&next())?, stop: int(&next())? },
            "llen" if n == 2 => Command::LLen(next()),
            "ping" | "select" | "get" | "set" | "exists" | "rpush" | "lpush" | "lrange" | "llen" => {
                return Err(Frame::error(format!("ERR wrong number of arguments for '{name}' command")));
            }
            _ => return Err(Frame::error(format!("ERR unknown command '{name}'"))),
        };
        Ok(cmd)
    }

    /// Run the command and return the reply.
    pub fn execute(self, db: &Db) -> Frame {
        match self {
            Command::Ping(None) => Frame::Simple("PONG".into()),
            Command::Ping(Some(msg)) => Frame::Bulk(msg),
            // As in Redis Cluster, only database 0 exists.
            Command::Select(0) => Frame::ok(),
            Command::Select(_) => Frame::error("ERR DB index is out of range"),
            Command::Get(key) => reply(db.get(&key), |v| v.map_or(Frame::Null, Frame::Bulk)),
            Command::Set(key, value) => {
                db.set(key, value);
                Frame::ok()
            }
            Command::Exists(keys) => Frame::Integer(db.exists(&keys)),
            Command::Push { key, values, end } => reply(db.push(key, values, end), Frame::Integer),
            Command::LRange { key, start, stop } => reply(db.lrange(&key, start, stop), |items| {
                Frame::Array(items.into_iter().map(Frame::Bulk).collect())
            }),
            Command::LLen(key) => reply(db.llen(&key), Frame::Integer),
        }
    }
}

fn reply<T>(result: Result<T, WrongType>, ok: impl FnOnce(T) -> Frame) -> Frame {
    result.map_or_else(|WrongType| Frame::error(WRONGTYPE), ok)
}

/// A request must be an array of bulk strings.
fn into_args(frame: Frame) -> Result<Vec<Bytes>, Frame> {
    let Frame::Array(items) = frame else {
        return Err(Frame::error("ERR Protocol error: expected an array of bulk strings"));
    };
    items
        .into_iter()
        .map(|item| match item {
            Frame::Bulk(b) => Ok(b),
            _ => Err(Frame::error("ERR Protocol error: expected an array of bulk strings")),
        })
        .collect()
}

fn int(arg: &[u8]) -> Result<i64, Frame> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Frame::error(NOT_AN_INTEGER))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &Db, args: &[&str]) -> Frame {
        let frame = Frame::Array(args.iter().map(|a| Frame::bulk(a.to_string())).collect());
        match Command::parse(frame) {
            Ok(cmd) => cmd.execute(db),
            Err(err) => err,
        }
    }

    fn is_error(f: &Frame, prefix: &str) -> bool {
        matches!(f, Frame::Error(e) if e.starts_with(prefix))
    }

    #[test]
    fn commands_are_case_insensitive() {
        let db = Db::new();
        assert_eq!(run(&db, &["sEt", "k", "v"]), Frame::ok());
        assert_eq!(run(&db, &["GET", "k"]), Frame::bulk("v"));
    }

    #[test]
    fn ping_and_select() {
        let db = Db::new();
        assert_eq!(run(&db, &["PING"]), Frame::Simple("PONG".into()));
        assert_eq!(run(&db, &["PING", "hi"]), Frame::bulk("hi"));
        assert_eq!(run(&db, &["SELECT", "0"]), Frame::ok());
        assert!(is_error(&run(&db, &["SELECT", "1"]), "ERR DB index"));
        assert!(is_error(&run(&db, &["SELECT", "x"]), "ERR value is not an integer"));
    }

    #[test]
    fn lists() {
        let db = Db::new();
        assert_eq!(run(&db, &["RPUSH", "l", "a", "b"]), Frame::Integer(2));
        assert_eq!(run(&db, &["LPUSH", "l", "z"]), Frame::Integer(3));
        assert_eq!(run(&db, &["LLEN", "l"]), Frame::Integer(3));
        assert_eq!(
            run(&db, &["LRANGE", "l", "0", "-1"]),
            Frame::Array(vec![Frame::bulk("z"), Frame::bulk("a"), Frame::bulk("b")])
        );
        assert_eq!(run(&db, &["EXISTS", "l", "nope", "l"]), Frame::Integer(2));
    }

    #[test]
    fn errors() {
        let db = Db::new();
        assert_eq!(run(&db, &["GET", "missing"]), Frame::Null);
        assert!(is_error(&run(&db, &["GET"]), "ERR wrong number of arguments for 'get'"));
        assert!(is_error(&run(&db, &["SET", "k", "v", "EX", "10"]), "ERR syntax error"));
        assert!(is_error(&run(&db, &["BLPOP", "l", "0"]), "ERR unknown command 'blpop'"));
        assert!(is_error(&run(&db, &["LRANGE", "l", "0", "x"]), "ERR value is not an integer"));
        run(&db, &["SET", "s", "v"]);
        assert!(is_error(&run(&db, &["RPUSH", "s", "a"]), "WRONGTYPE"));
        assert!(is_error(&run(&db, &[]), "ERR empty command"));
    }
}
