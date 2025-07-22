mod handler;

use crate::broker::Broker;
use crate::config::ServerConfig;
use crate::protocol::{Request, codec::ServerCodec};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio::signal;

pub struct Server {
    broker: Arc<Broker>,
    config: Arc<ServerConfig>,
}

impl Server {
    pub fn new(broker: Arc<Broker>, config: ServerConfig) -> Self {
        Self {
            broker,
            config: Arc::new(config),
        }
    }

    pub async fn start(&self, listener: TcpListener) -> Result<()> {
        println!(
            "Bifrost server started, listening on {}:{}",
            self.config.host, self.config.port
        );

        loop {
            tokio::select! {
                biased; // 优先处理关机信号

                _ = signal::ctrl_c() => {
                    break;
                }

                result = listener.accept() => {
                    let (socket, addr) = match result {
                        Ok(res) => res,
                        Err(e) => {
                            eprintln!("[Error] Failed to accept connection: {}", e);
                            continue;
                        }
                    };

                    let broker = self.broker.clone();

                    tokio::spawn(async move {
                        println!("[Connection] New connection from: {}", addr);
                        if let Err(e) = Self::handle_connection(socket, addr, broker).await {
                            eprintln!("[Connection] Connection from {} error: {}", addr, e);
                        }
                    });
                }
            }
        }

        self.broker.shutdown().await?;
        Ok(())
    }

    async fn handle_connection(
        socket: TcpStream,
        _addr: SocketAddr,
        broker: Arc<Broker>,
    ) -> Result<()> {
        let mut framed = Framed::new(socket, ServerCodec::default());

        while let Some(result) = framed.next().await {
            let request: Request = match result {
                Ok(req) => req,
                Err(e) => return Err(e.into()),
            };

            let response = handler::handle_request(request, &broker).await?;
            framed.send(response).await?;
        }

        Ok(())
    }
} 