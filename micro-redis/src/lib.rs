//! Micro-Redis: a small, linearizable, Redis-compatible server.
//!
//! - [`resp`]: the wire protocol (RESP2).
//! - [`connection`]: reading and writing frames on a socket.
//! - [`db`]: the key space.
//! - [`cmd`]: parsing and running commands.
//! - [`server`]: the accept loop.
//! - [`client`]: a minimal client, for talking to other servers.

pub mod client;
pub mod cmd;
pub mod connection;
pub mod db;
pub mod resp;
pub mod server;

pub use client::Client;
pub use db::Db;
pub use resp::Frame;
