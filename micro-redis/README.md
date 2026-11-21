# Micro-Redis (reference)

A small, linearizable, Redis-compatible server in Rust on `tokio`. It's the starting point for **Mini 3: Micro-Redis Cluster**. You're welcome to build on your own Mini 2 instead; this is here so nobody has to.

It differs from Mini 2 in a few ways that match Mini 3. `BLPOP`/`BRPOP` are gone, `LRANGE` and `LLEN` are new, and only database 0 exists.

## Running it

```bash
cargo run --release -- --port 6379
```

`--bind 0.0.0.0` accepts connections from other machines; the default is `127.0.0.1`. Then talk to it with `redis-cli`:

```bash
redis-cli -p 6379 SET greeting hello
redis-cli -p 6379 GET greeting
```

`redis-benchmark` works too, for the commands we support (try `-t set,get,lpush,rpush,lrange_100`). Ignore its "Could not fetch server CONFIG" warning, which appears because we don't implement `CONFIG`. If you don't have the Redis tools installed, the official Docker image has them:

```bash
docker run --rm -it redis:7.4 redis-cli -h host.docker.internal -p 6379
```

## Commands

Behavior follows the [Redis command reference](https://redis.io/docs/latest/commands/), except where noted.

| Command | Notes |
|---|---|
| `PING [message]` | |
| `SELECT index` | Only database 0 exists, as in Redis Cluster. |
| `GET key` | |
| `SET key value` | No options (`EX`, `NX`, ...). |
| `EXISTS key [key ...]` | |
| `RPUSH`, `LPUSH key value [value ...]` | |
| `LRANGE key start stop` | |
| `LLEN key` | |

Errors (wrong arity, `WRONGTYPE`, non-integer arguments, unknown commands) look like real Redis errors. Malformed RESP gets an `ERR Protocol error` reply, and then the connection is closed, as in Redis. Inline commands (`PING\r\n` typed into `nc`) are accepted as well.

## How it works

The code is in `src/`:

- `resp.rs`: RESP2 parsing and encoding. It handles every RESP2 type, so it works both for requests coming in and for replies coming back from another server.
- `connection.rs`: frames on a TCP socket, with buffered writes.
- `db.rs`: the key space.
- `cmd.rs`: turning a request into a `Command`, and running it.
- `server.rs`: the accept loop, with one task per connection.
- `client.rs`: a minimal client, `Client::connect(addr)` then `client.command(["GET", "k"])`.

*Linearizability.* The whole key space sits behind one `std::sync::Mutex`. Every command takes it once, does a little work, and lets go, never across an `.await`. Each command therefore takes effect atomically at some instant between its request and its reply, which is all linearizability asks. A single lock sounds slow, but it's held so briefly that it isn't a bottleneck at the scale we care about.

*Pipelining.* Each connection's requests are answered in order. Replies are buffered and flushed whenever the server is about to wait for more input. A client that sends many commands at once (such as the `redis` crate's multiplexed connection, or `redis-benchmark -P 16`) gets its replies in one batch, and a client that sends one at a time gets each reply immediately.

## Extending it for Mini 3

- *New commands* (for example, internal ones for moving keys between nodes) go in `cmd.rs`: add a variant to `Command`, parse it in `Command::parse`, and run it in `Command::execute`.
- *Working with many keys at once* (for example, finding every key in a range of the ring) can use `Db::with_data`, which gives you the whole map under the lock. Keep what you do inside it quick, since every other request waits.
- *Talking to other servers* (a router forwarding to a node, or one node to another) can use `Client`, or the [redis](https://docs.rs/redis) crate if you prefer.

## Tests

```bash
cargo test
```

There are unit tests for the parser, the key space and the commands. The tests in `tests/server.rs` start a real server and check concurrent clients, pipelining, protocol errors, and compatibility with the `redis` crate.
