//! Micro-Redis: a small, linearizable, Redis-compatible server.
//!
//! - [`resp`]: the wire protocol (RESP2).
//! - [`connection`]: reading and writing frames on a socket.

pub mod connection;
pub mod resp;

pub use resp::Frame;
