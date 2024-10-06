use anyhow::Result;
use std::{io, net::SocketAddr};
use tokio::{io::AsyncWriteExt, net::TcpListener};

use tracing::{info, warn};

const BUF_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";

    let listener = TcpListener::bind(addr).await?;
    info!("Listening on: {}", addr);

    loop {
        let (stream, raddr) = listener.accept().await?;
        info!("Accepted connection from: {}", raddr);
        tokio::spawn(async move {
            if let Err(e) = process_redis_conn(stream, raddr).await {
                warn!("Error processing connection from {}: {:?}", raddr, e);
            }
        });
    }
}

async fn process_redis_conn(mut stream: tokio::net::TcpStream, addr: SocketAddr) -> Result<()> {
    loop {
        stream.readable().await?;
        let mut buf = Vec::with_capacity(BUF_SIZE);

        match stream.try_read_buf(&mut buf) {
            Ok(0) => {
                info!("Connection closed by peer: {}", addr);
                break;
            }
            Ok(n) => {
                info!("Received {} bytes from: {}", n, addr);
                let line = String::from_utf8_lossy(&buf);
                info!("{}", line);
                stream.write_all(b"+OK\r\n").await?;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    warn!("Connection closed: {}", addr);

    Ok(())
}
