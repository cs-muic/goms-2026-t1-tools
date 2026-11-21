//! Accepting connections and serving them.

use std::io;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use crate::cmd::Command;
use crate::connection::Connection;
use crate::db::Db;
use crate::resp::Frame;

/// Serve clients on `listener` forever, one task per connection.
pub async fn run(listener: TcpListener, db: Db) -> io::Result<()> {
    loop {
        let (socket, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                // Usually "too many open files". Back off and keep going.
                eprintln!("accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = socket.set_nodelay(true);
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = serve(socket, db).await {
                if !matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe) {
                    eprintln!("connection error: {e}");
                }
            }
        });
    }
}

/// Answer one client's requests, in order, until it hangs up.
async fn serve(socket: TcpStream, db: Db) -> io::Result<()> {
    let mut conn = Connection::new(socket);
    loop {
        let request = match conn.read_request().await {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                // Like Redis: report the protocol error, then hang up, since
                // we can't tell where the next request starts.
                conn.write_frame(&Frame::error(format!("ERR Protocol error: {e}"))).await?;
                conn.flush().await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        // An empty inline line is ignored, as in Redis.
        if request == Frame::Array(vec![]) {
            continue;
        }
        let reply = match Command::parse(request) {
            Ok(cmd) => cmd.execute(&db),
            Err(err) => err,
        };
        conn.write_frame(&reply).await?;
    }
}
