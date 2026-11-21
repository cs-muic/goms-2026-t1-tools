//! Reading and writing frames on a TCP connection.

use std::io;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::resp::{self, Frame, ParseError};

/// A TCP connection that speaks RESP.
///
/// Writes are buffered. They're flushed automatically right before a read has
/// to wait for the network, so a client that pipelines several commands gets
/// all their replies in one go, and a client that sends one command at a time
/// still gets each reply right away. Call [`Connection::flush`] if you write
/// without reading afterward.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

type Parser = fn(&[u8]) -> Result<(Frame, usize), ParseError>;

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Read one frame of any type (for clients reading replies).
    ///
    /// Returns `Ok(None)` if the peer closed the connection cleanly.
    pub async fn read_frame(&mut self) -> io::Result<Option<Frame>> {
        self.read_with(resp::parse).await
    }

    /// Read one request, accepting inline commands too (for servers).
    ///
    /// Returns `Ok(None)` if the peer closed the connection cleanly. Malformed
    /// input is reported as an error of kind [`io::ErrorKind::InvalidData`].
    pub async fn read_request(&mut self) -> io::Result<Option<Frame>> {
        self.read_with(resp::parse_request).await
    }

    async fn read_with(&mut self, parse: Parser) -> io::Result<Option<Frame>> {
        loop {
            match parse(&self.buf) {
                Ok((frame, used)) => {
                    self.buf.advance(used);
                    return Ok(Some(frame));
                }
                Err(ParseError::Incomplete) => {}
                Err(ParseError::Protocol(msg)) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                }
            }
            // Nothing complete is buffered, so we're about to wait. Send any
            // pending replies first.
            self.stream.flush().await?;
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed in the middle of a frame",
                    ))
                };
            }
        }
    }

    /// Queue a frame to be sent.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.stream.write_all(&frame.to_bytes()).await
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }
}
