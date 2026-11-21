//! Micro-Redis: a small, linearizable, Redis-compatible server.
//!
//! - [`resp`]: the wire protocol (RESP2).

pub mod resp;

pub use resp::Frame;
