//! A minimal RESP2 client: one request at a time, no pipelining.

use std::future::Future;
use std::io;
use std::pin::Pin;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

const MAX_BULK: i64 = 64 * 1024 * 1024;
const MAX_ARRAY: i64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

impl Reply {
    /// Short human-readable description, used in "unexpected reply" messages.
    pub fn describe(&self) -> String {
        match self {
            Reply::Simple(s) => format!("+{s}"),
            Reply::Error(s) => format!("-{s}"),
            Reply::Int(n) => format!(":{n}"),
            Reply::Bulk(None) => "nil bulk".into(),
            Reply::Bulk(Some(b)) => format!("bulk {:?}", String::from_utf8_lossy(b)),
            Reply::Array(None) => "nil array".into(),
            Reply::Array(Some(a)) => format!("array of {}", a.len()),
        }
    }
}

pub struct Conn {
    rd: BufReader<OwnedReadHalf>,
    wr: OwnedWriteHalf,
}

fn proto_err(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

pub fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + args.iter().map(|a| a.len() + 16).sum::<usize>());
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

impl Conn {
    pub async fn connect(addr: &str) -> io::Result<Conn> {
        let s = TcpStream::connect(addr).await?;
        s.set_nodelay(true)?;
        let (r, w) = s.into_split();
        Ok(Conn { rd: BufReader::new(r), wr: w })
    }

    pub async fn call(&mut self, args: &[&[u8]]) -> io::Result<Reply> {
        self.wr.write_all(&encode(args)).await?;
        read_reply(&mut self.rd).await
    }
}

async fn read_line<R: AsyncBufReadExt + Unpin>(rd: &mut R) -> io::Result<String> {
    let mut buf = Vec::new();
    let n = rd.read_until(b'\n', &mut buf).await?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed"));
    }
    if buf.len() < 2 || buf[buf.len() - 2] != b'\r' {
        return Err(proto_err("line not terminated by CRLF"));
    }
    buf.truncate(buf.len() - 2);
    String::from_utf8(buf).map_err(|_| proto_err("non-UTF-8 line"))
}

fn parse_int(s: &str) -> io::Result<i64> {
    s.parse::<i64>().map_err(|_| proto_err(format!("bad integer {s:?}")))
}

/// Reads one RESP2 reply. Boxed because arrays recurse.
pub fn read_reply<'a, R: AsyncBufReadExt + Unpin + Send>(
    rd: &'a mut R,
) -> Pin<Box<dyn Future<Output = io::Result<Reply>> + Send + 'a>> {
    Box::pin(async move {
        let line = read_line(rd).await?;
        let tag = line.as_bytes().first().copied().unwrap_or(0);
        let rest = if tag.is_ascii() && !line.is_empty() { &line[1..] } else { "" };
        match tag {
            b'+' => Ok(Reply::Simple(rest.to_string())),
            b'-' => Ok(Reply::Error(rest.to_string())),
            b':' => Ok(Reply::Int(parse_int(rest)?)),
            b'$' => {
                let n = parse_int(rest)?;
                if n == -1 {
                    return Ok(Reply::Bulk(None));
                }
                if !(0..=MAX_BULK).contains(&n) {
                    return Err(proto_err(format!("bad bulk length {n}")));
                }
                let mut buf = vec![0u8; n as usize + 2];
                rd.read_exact(&mut buf).await?;
                if &buf[n as usize..] != b"\r\n" {
                    return Err(proto_err("bulk string not terminated by CRLF"));
                }
                buf.truncate(n as usize);
                Ok(Reply::Bulk(Some(buf)))
            }
            b'*' => {
                let n = parse_int(rest)?;
                if n == -1 {
                    return Ok(Reply::Array(None));
                }
                if !(0..=MAX_ARRAY).contains(&n) {
                    return Err(proto_err(format!("bad array length {n}")));
                }
                let mut items = Vec::with_capacity(n.min(1024) as usize);
                for _ in 0..n {
                    items.push(read_reply(rd).await?);
                }
                Ok(Reply::Array(Some(items)))
            }
            _ => Err(proto_err(format!("unknown reply type in line {line:?}"))),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn parse(bytes: &[u8]) -> io::Result<Reply> {
        let mut rd = BufReader::new(bytes);
        read_reply(&mut rd).await
    }

    #[tokio::test]
    async fn parses_all_reply_types() {
        assert_eq!(parse(b"+OK\r\n").await.unwrap(), Reply::Simple("OK".into()));
        assert_eq!(parse(b"-ERR x\r\n").await.unwrap(), Reply::Error("ERR x".into()));
        assert_eq!(parse(b":-12\r\n").await.unwrap(), Reply::Int(-12));
        assert_eq!(parse(b"$-1\r\n").await.unwrap(), Reply::Bulk(None));
        assert_eq!(parse(b"$5\r\na\r\nbc\r\n").await.unwrap(), Reply::Bulk(Some(b"a\r\nbc".to_vec())));
        assert_eq!(
            parse(b"*2\r\n$1\r\na\r\n$0\r\n\r\n").await.unwrap(),
            Reply::Array(Some(vec![Reply::Bulk(Some(b"a".to_vec())), Reply::Bulk(Some(vec![]))]))
        );
        assert_eq!(parse(b"*0\r\n").await.unwrap(), Reply::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn rejects_garbage() {
        assert!(parse(b"hello\r\n").await.is_err());
        assert!(parse(b"$3\r\nabcd\r\n").await.is_err());
        assert!(parse(b"+OK\n").await.is_err());
        assert!(parse(b"").await.is_err());
    }

    #[test]
    fn encodes_arrays_of_bulks() {
        assert_eq!(encode(&[b"GET", b"k"]), b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n".to_vec());
    }
}
