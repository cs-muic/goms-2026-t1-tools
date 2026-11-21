use clap::Parser;
use micro_redis::{server, Db};
use tokio::net::TcpListener;

/// A small Redis-compatible server.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Port to listen on.
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Address to bind to. Use 0.0.0.0 to accept connections from other machines.
    #[arg(short, long, default_value = "127.0.0.1")]
    bind: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind((args.bind.as_str(), args.port)).await?;
    eprintln!("micro-redis listening on {}", listener.local_addr()?);
    tokio::select! {
        result = server::run(listener, Db::new()) => result,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("shutting down");
            Ok(())
        }
    }
}
