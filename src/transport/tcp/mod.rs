mod connection;
pub mod config;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::Bytes;
use config::TcpTransportConfig;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use std::sync::atomic::Ordering;

use crate::transport::tcp::connection::TcpConnection;
use crate::error::{Error, ErrorKind};
use crate::transport::Transport;

pub struct TcpTransport {
    connection: Arc<TcpConnection>,
    incoming_stream: Option<ReceiverStream<Bytes>>,
    is_connected: AtomicBool,
}

impl Transport for TcpTransport {
    type Config = TcpTransportConfig;

    async fn new(config: Self::Config, error_tx: mpsc::Sender<Error>) -> Result<Self, Error> {
        let stream = TcpStream::connect(&config.server_address).await.map_err(|e| {
            Error::new(ErrorKind::TransportError, format!("Could not connect to address: {}: {}", config.server_address, e))
        })?;
        let (read_stream, write_stream) = stream.into_split();
        let (connection, incoming_stream) = TcpConnection::new(read_stream, write_stream, error_tx).await;
        let connection = Arc::new(connection);
        Ok(TcpTransport {
            connection,
            incoming_stream: Some(incoming_stream),
            is_connected: AtomicBool::new(true),
        })
    }

    async fn send(&self, data: Bytes) -> Result<(), Error> {
        self.connection.send(data).await
    }

    fn into_stream(&mut self) -> ReceiverStream<Bytes> {
        self.incoming_stream.take().expect("Stream already taken")
    }

    fn shutdown(&self) {
        if !self.is_connected.load(Ordering::Acquire) {
            return
        }
        self.connection.shutdown();
        self.is_connected.store(false, Ordering::Release);
    }
}
