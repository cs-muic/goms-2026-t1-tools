# goms-2026-t1-tools

Course-provided tools for **Mini 3: Micro-Redis Cluster**, in which students
shard a small Redis-compatible server across storage nodes using a
consistent-hash ring, with routers, a coordinator and live membership changes.

| Crate | What it is |
|---|---|
| [`micro-redis`](micro-redis/) | A reference Micro-Redis: a small Redis-compatible server on `tokio` (RESP2, `GET`/`SET`/`EXISTS`/`RPUSH`/`LPUSH`/`LRANGE`/`LLEN`, database 0 only). Build on your own Mini 2 if you can, and fall back on this or borrow from it as you like. |
| [`mrc-check`](mrc-check/) | A black-box checker for a Micro-Redis Cluster. It runs concurrent clients against your routers while adding and removing nodes and pausing routers, then checks the recorded history for lost writes and lists that don't add up. |

Both are written in Rust and form one Cargo workspace:

```bash
cargo build --release    # binaries land in target/release/
cargo test               # tests for both crates
```

Each crate's README explains how to use it.
