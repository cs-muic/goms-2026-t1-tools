//! RESP2, the Redis wire protocol: parsing and encoding.
//!
//! The parser handles every RESP2 type, so you can use it both for requests
//! coming into a server and for replies coming back to a client (a router
//! talking to storage nodes, say). See
//! <https://redis.io/docs/latest/develop/reference/protocol-spec/>.

use bytes::Bytes;

/// One RESP2 value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR something\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n`
    Bulk(Bytes),
    /// `$-1\r\n`, the null bulk string (what `GET` returns for a missing key).
    Null,
    /// `*-1\r\n`, the null array.
    NullArray,
    /// `*2\r\n...`
    Array(Vec<Frame>),
}

impl Frame {
    pub fn ok() -> Frame {
        Frame::Simple("OK".into())
    }

    pub fn error(msg: impl Into<String>) -> Frame {
        Frame::Error(msg.into())
    }

    pub fn bulk(data: impl Into<Bytes>) -> Frame {
        Frame::Bulk(data.into())
    }

    /// Append this frame's wire encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Frame::Simple(s) => line(out, b'+', s),
            Frame::Error(s) => line(out, b'-', s),
            Frame::Integer(n) => line(out, b':', &n.to_string()),
            Frame::Bulk(data) => {
                line(out, b'$', &data.len().to_string());
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Null => out.extend_from_slice(b"$-1\r\n"),
            Frame::NullArray => out.extend_from_slice(b"*-1\r\n"),
            Frame::Array(items) => {
                line(out, b'*', &items.len().to_string());
                for item in items {
                    item.encode(out);
                }
            }
        }
    }

    /// The wire encoding of this frame.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }
}

/// Simple strings and errors can't contain CR or LF, so we replace them.
fn line(out: &mut Vec<u8>, prefix: u8, s: &str) {
    out.push(prefix);
    out.extend(s.bytes().map(|b| if b == b'\r' || b == b'\n' { b' ' } else { b }));
    out.extend_from_slice(b"\r\n");
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    /// The buffer doesn't hold a whole frame yet. Read more and try again.
    Incomplete,
    /// The bytes aren't valid RESP. The connection should be closed.
    Protocol(String),
}

fn protocol(msg: impl Into<String>) -> ParseError {
    ParseError::Protocol(msg.into())
}

// The same limits real Redis uses, more or less, so a bad client can't make
// us allocate unbounded memory.
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;
const MAX_ARRAY_LEN: i64 = 1024 * 1024;
const MAX_LINE_LEN: usize = 64 * 1024;
const MAX_DEPTH: usize = 32;

/// Parse one frame from the front of `buf`.
///
/// On success, returns the frame and how many bytes it used.
pub fn parse(buf: &[u8]) -> Result<(Frame, usize), ParseError> {
    let mut pos = 0;
    let frame = parse_at(buf, &mut pos, 0)?;
    Ok((frame, pos))
}

/// Parse one client request from the front of `buf`.
///
/// Clients normally send an array of bulk strings, but Redis also accepts
/// "inline" commands (a plain line such as `PING`), which is handy with
/// `nc` or `telnet`. Inline commands are split on whitespace, without quoting.
pub fn parse_request(buf: &[u8]) -> Result<(Frame, usize), ParseError> {
    match buf.first() {
        None => Err(ParseError::Incomplete),
        Some(b'*') => parse(buf),
        Some(_) => parse_inline(buf),
    }
}

fn parse_inline(buf: &[u8]) -> Result<(Frame, usize), ParseError> {
    let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
        return if buf.len() > MAX_LINE_LEN {
            Err(protocol("too big inline request"))
        } else {
            Err(ParseError::Incomplete)
        };
    };
    let line = buf[..nl].strip_suffix(b"\r").unwrap_or(&buf[..nl]);
    let args = line
        .split(|b| b.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
        .map(|part| Frame::Bulk(Bytes::copy_from_slice(part)))
        .collect();
    Ok((Frame::Array(args), nl + 1))
}

fn parse_at(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Frame, ParseError> {
    if depth > MAX_DEPTH {
        return Err(protocol("arrays nested too deeply"));
    }
    let kind = *buf.get(*pos).ok_or(ParseError::Incomplete)?;
    *pos += 1;
    match kind {
        b'+' => Ok(Frame::Simple(read_str(buf, pos)?)),
        b'-' => Ok(Frame::Error(read_str(buf, pos)?)),
        b':' => Ok(Frame::Integer(read_int(buf, pos)?)),
        b'$' => {
            let len = read_int(buf, pos)?;
            if len == -1 {
                return Ok(Frame::Null);
            }
            if !(0..=MAX_BULK_LEN).contains(&len) {
                return Err(protocol("invalid bulk length"));
            }
            let len = len as usize;
            let end = *pos + len;
            if buf.len() < end + 2 {
                return Err(ParseError::Incomplete);
            }
            if &buf[end..end + 2] != b"\r\n" {
                return Err(protocol("bulk string not terminated by CRLF"));
            }
            let data = Bytes::copy_from_slice(&buf[*pos..end]);
            *pos = end + 2;
            Ok(Frame::Bulk(data))
        }
        b'*' => {
            let len = read_int(buf, pos)?;
            if len == -1 {
                return Ok(Frame::NullArray);
            }
            if !(0..=MAX_ARRAY_LEN).contains(&len) {
                return Err(protocol("invalid multibulk length"));
            }
            let mut items = Vec::with_capacity((len as usize).min(1024));
            for _ in 0..len {
                items.push(parse_at(buf, pos, depth + 1)?);
            }
            Ok(Frame::Array(items))
        }
        other => Err(protocol(format!(
            "expected a RESP type byte, got '{}'",
            other.escape_ascii()
        ))),
    }
}

/// Read up to the next CRLF and return what came before it.
fn read_line<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8], ParseError> {
    let rest = &buf[*pos..];
    match rest.windows(2).position(|w| w == b"\r\n") {
        Some(i) => {
            *pos += i + 2;
            Ok(&rest[..i])
        }
        None if rest.len() > MAX_LINE_LEN => Err(protocol("line too long")),
        None => Err(ParseError::Incomplete),
    }
}

fn read_str(buf: &[u8], pos: &mut usize) -> Result<String, ParseError> {
    Ok(String::from_utf8_lossy(read_line(buf, pos)?).into_owned())
}

fn read_int(buf: &[u8], pos: &mut usize) -> Result<i64, ParseError> {
    std::str::from_utf8(read_line(buf, pos)?)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| protocol("invalid integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(frame: Frame) {
        let bytes = frame.to_bytes();
        assert_eq!(parse(&bytes), Ok((frame, bytes.len())));
    }

    #[test]
    fn roundtrips_every_type() {
        roundtrip(Frame::ok());
        roundtrip(Frame::error("ERR nope"));
        roundtrip(Frame::Integer(-7));
        roundtrip(Frame::bulk("hello"));
        roundtrip(Frame::bulk(""));
        roundtrip(Frame::bulk(&b"bin\r\nary\0"[..]));
        roundtrip(Frame::Null);
        roundtrip(Frame::NullArray);
        roundtrip(Frame::Array(vec![]));
        roundtrip(Frame::Array(vec![
            Frame::bulk("a"),
            Frame::Integer(1),
            Frame::Array(vec![Frame::Null]),
        ]));
    }

    #[test]
    fn incomplete_at_every_prefix() {
        let bytes = Frame::Array(vec![Frame::bulk("SET"), Frame::bulk("k"), Frame::bulk("v")]).to_bytes();
        for cut in 0..bytes.len() {
            assert_eq!(parse(&bytes[..cut]), Err(ParseError::Incomplete), "cut at {cut}");
        }
    }

    #[test]
    fn leaves_trailing_bytes_alone() {
        let mut bytes = Frame::bulk("one").to_bytes();
        let first = bytes.len();
        bytes.extend(Frame::bulk("two").to_bytes());
        assert_eq!(parse(&bytes), Ok((Frame::bulk("one"), first)));
    }

    #[test]
    fn inline_requests() {
        assert_eq!(
            parse_request(b"SET  k v\r\nGET k\r\n"),
            Ok((Frame::Array(vec![Frame::bulk("SET"), Frame::bulk("k"), Frame::bulk("v")]), 10))
        );
        assert_eq!(parse_request(b"PING\n"), Ok((Frame::Array(vec![Frame::bulk("PING")]), 5)));
        assert_eq!(parse_request(b"PIN"), Err(ParseError::Incomplete));
    }

    #[test]
    fn protocol_errors() {
        assert!(matches!(parse(b"?\r\n"), Err(ParseError::Protocol(_))));
        assert!(matches!(parse(b":abc\r\n"), Err(ParseError::Protocol(_))));
        assert!(matches!(parse(b"$-5\r\n"), Err(ParseError::Protocol(_))));
        assert!(matches!(parse(b"$3\r\nabcXY"), Err(ParseError::Protocol(_))));
    }

    #[test]
    fn simple_strings_stay_on_one_line() {
        assert_eq!(Frame::error("a\r\nb").to_bytes(), b"-a  b\r\n");
    }
}
