//! Micro-Redis: a small, linearizable, Redis-compatible server.
//!
//! - [`resp`]: the wire protocol (RESP2).
//! - [`connection`]: reading and writing frames on a socket.
//! - [`db`]: the key space.
//! - [`cmd`]: parsing and running commands.
//! - [`server`]: the accept loop.

pub mod cmd;
pub mod connection;
pub mod db;
pub mod resp;
pub mod server;

pub use db::Db;
pub use resp::Frame;
