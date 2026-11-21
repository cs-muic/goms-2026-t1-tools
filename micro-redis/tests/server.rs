//! End-to-end tests: start a real server on a random port and talk to it.

use std::net::SocketAddr;

use micro_redis::{server, Client, Db, Frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(server::run(listener, Db::new()));
    addr
}

fn bulks(items: &[&str]) -> Frame {
    Frame::Array(items.iter().map(|s| Frame::bulk(s.to_string())).collect())
}

#[tokio::test]
async fn basic_commands() {
    let mut c = Client::connect(start().await).await.unwrap();
    assert_eq!(c.command(["PING"]).await.unwrap(), Frame::Simple("PONG".into()));
    assert_eq!(c.command(["GET", "k"]).await.unwrap(), Frame::Null);
    assert_eq!(c.command(["SET", "k", "v"]).await.unwrap(), Frame::ok());
    assert_eq!(c.command(["GET", "k"]).await.unwrap(), Frame::bulk("v"));
    assert_eq!(c.command(["RPUSH", "l", "a", "b"]).await.unwrap(), Frame::Integer(2));
    assert_eq!(c.command(["LRANGE", "l", "0", "-1"]).await.unwrap(), bulks(&["a", "b"]));
    assert_eq!(c.command(["EXISTS", "k", "l", "x"]).await.unwrap(), Frame::Integer(2));
    assert!(matches!(c.command(["GET", "l"]).await.unwrap(), Frame::Error(e) if e.starts_with("WRONGTYPE")));
}

#[tokio::test]
async fn clients_share_one_key_space() {
    let addr = start().await;
    let mut a = Client::connect(addr).await.unwrap();
    let mut b = Client::connect(addr).await.unwrap();
    a.command(["SET", "shared", "from a"]).await.unwrap();
    assert_eq!(b.command(["GET", "shared"]).await.unwrap(), Frame::bulk("from a"));
}

/// Many clients push to one list at once. Nothing may be lost, and each
/// client's pushes must appear in the order it made them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pushes() {
    const CLIENTS: usize = 32;
    const PUSHES: usize = 200;
    let addr = start().await;

    let tasks: Vec<_> = (0..CLIENTS)
        .map(|c| {
            tokio::spawn(async move {
                let mut client = Client::connect(addr).await.unwrap();
                for i in 0..PUSHES {
                    let reply = client.command(["RPUSH".to_string(), "log".into(), format!("{c}:{i}")]).await;
                    assert!(matches!(reply.unwrap(), Frame::Integer(_)));
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }

    let mut client = Client::connect(addr).await.unwrap();
    assert_eq!(client.command(["LLEN", "log"]).await.unwrap(), Frame::Integer((CLIENTS * PUSHES) as i64));
    let Frame::Array(items) = client.command(["LRANGE", "log", "0", "-1"]).await.unwrap() else {
        panic!("LRANGE should return an array");
    };
    let mut next = vec![0; CLIENTS];
    for item in items {
        let Frame::Bulk(item) = item else { panic!("expected bulk strings") };
        let (c, i) = std::str::from_utf8(&item).unwrap().split_once(':').unwrap();
        let (c, i): (usize, usize) = (c.parse().unwrap(), i.parse().unwrap());
        assert_eq!(i, next[c], "client {c}'s pushes are out of order");
        next[c] += 1;
    }
}

/// Several requests sent in one write get their replies in order.
#[tokio::test]
async fn pipelining_and_inline_commands() {
    let mut socket = TcpStream::connect(start().await).await.unwrap();
    socket
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\nGET k\r\n\r\nPING\r\n")
        .await
        .unwrap();
    let expected = b"+OK\r\n$1\r\nv\r\n+PONG\r\n";
    let mut reply = vec![0; expected.len()];
    socket.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, expected);
}

#[tokio::test]
async fn protocol_errors_close_the_connection() {
    let mut socket = TcpStream::connect(start().await).await.unwrap();
    socket.write_all(b"*1\r\n$abc\r\n").await.unwrap();
    let mut reply = Vec::new();
    socket.read_to_end(&mut reply).await.unwrap();
    assert!(reply.starts_with(b"-ERR Protocol error"), "{:?}", String::from_utf8_lossy(&reply));
}

/// The `redis` crate (what students used in Mini 1) works out of the box,
/// including its multiplexed connection, which pipelines.
#[tokio::test]
async fn works_with_the_redis_crate() {
    let addr = start().await;
    let client = redis::Client::open(format!("redis://{addr}/0")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("k").arg("v").query_async(&mut con).await.unwrap();
    let v: String = redis::cmd("GET").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(v, "v");

    let n: i64 = redis::cmd("LPUSH").arg("l").arg("a").arg("b").query_async(&mut con).await.unwrap();
    assert_eq!(n, 2);
    let items: Vec<String> = redis::cmd("LRANGE").arg("l").arg(0).arg(-1).query_async(&mut con).await.unwrap();
    assert_eq!(items, ["b", "a"]);

    let missing: Option<String> = redis::cmd("GET").arg("nope").query_async(&mut con).await.unwrap();
    assert_eq!(missing, None);
}
