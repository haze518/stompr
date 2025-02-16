use std::future::Future;

use bytes::Bytes;
use tcp::config::TcpTransportConfig;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::Error;

pub mod tcp;

pub trait Transport: Sized {
    type Config;

    fn new(config: Self::Config, error_tx: mpsc::Sender<Error>) -> impl Future<Output = Result<Self, Error>>;
    fn send(&self, data: Bytes) -> impl Future<Output = Result<(), Error>> + Send;
    fn into_stream(&mut self) -> ReceiverStream<Bytes>;
    fn shutdown(&self);
}

pub enum TransportConfig {
    TCP(TcpTransportConfig)
}
