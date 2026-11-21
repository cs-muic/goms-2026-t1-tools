//! A tiny RESP client: enough for one server (or router) to talk to another.

use std::io;

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::connection::Connection;
use crate::resp::Frame;

/// One connection to a Redis-compatible server. Commands are sent one at a
/// time: each call waits for its reply.
pub struct Client {
    conn: Connection,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Client> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        Ok(Client { conn: Connection::new(socket) })
    }

    /// Send a command and wait for its reply. An error reply from the server
    /// comes back as `Ok(Frame::Error(..))`; `Err` means the connection failed.
    ///
    /// ```no_run
    /// # async fn demo() -> std::io::Result<()> {
    /// let mut client = micro_redis::Client::connect("127.0.0.1:6379").await?;
    /// client.command(["SET", "greeting", "hello"]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn command<I, A>(&mut self, args: I) -> io::Result<Frame>
    where
        I: IntoIterator<Item = A>,
        A: Into<Bytes>,
    {
        let request = Frame::Array(args.into_iter().map(|a| Frame::Bulk(a.into())).collect());
        self.conn.write_frame(&request).await?;
        match self.conn.read_frame().await? {
            Some(reply) => Ok(reply),
            None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "server closed the connection")),
        }
    }
}
