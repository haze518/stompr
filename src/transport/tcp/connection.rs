use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, broadcast};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::{Error, ErrorKind};

pub(crate) struct TcpConnection {
    outgoing_tx: mpsc::Sender<Bytes>,
    shutdown_tx: broadcast::Sender<()>,
    _read_handle: JoinHandle<()>,
    _write_handle: JoinHandle<()>,
}

impl TcpConnection {
    pub(crate) async fn new<R, W>(mut read_stream: R, mut write_stream: W, error_tx: mpsc::Sender<Error>) -> (Self, ReceiverStream<Bytes>)
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let (shutdown_tx, _) = broadcast::channel(1);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Bytes>(32);
        let (incoming_tx, incoming_rx) = mpsc::channel::<Bytes>(32);
        let incoming_stream = ReceiverStream::new(incoming_rx);
        let mut shutdown_rx_reader = shutdown_tx.subscribe();
        let mut shutdown_rx_writer = shutdown_tx.subscribe();
        let error_tx_reader = error_tx.clone();
        let error_tx_writer = error_tx.clone();

        let read_incoming_tx = incoming_tx;
        let read_handle = tokio::spawn(async move {
            let mut buffer = BytesMut::with_capacity(4 * 1024);
            loop {
                tokio::select! {
                    _ = shutdown_rx_reader.recv() => {
                        break;
                    },
                    result = read_stream.read_buf(&mut buffer) => {
                        match result {
                            Ok(n) => {
                                if n == 0 {
                                    return
                                }
                                while let Some(pos) = buffer.iter().position(|&b| b == b'\0') {
                                    let frame_data = buffer.split_to(pos + 1).freeze();
            
                                    if let Err(e) = read_incoming_tx.send(frame_data).await {
                                        let _ = error_tx_reader.send(Error::new(
                                            ErrorKind::ParseError,
                                            format!("Failed to transfer parsed frame: {e}")
                                        )).await;
                                    }
                                }
                            },
                            Err(e) => {
                                let _ = error_tx_reader.send(Error::new(
                                    ErrorKind::TransportError,
                                    format!("Failed to read from socket: {e}")
                                )).await;
                                break;
                            }
                        }
                    }
                }
            }
        });

        let write_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx_writer.recv() => {
                        if let Err(e) = write_stream.shutdown().await {
                            let _ = error_tx_writer.send(Error::new(
                                ErrorKind::TransportConnectionError,
                                format!("could not shutdown connection, err: {e}"))
                            ).await;
                        };
                        break;
                    },
                    maybe_data = outgoing_rx.recv() => {
                        if let Some(data) = maybe_data {
                            if let Err(e) = write_stream.write(&data).await {
                                let _ = error_tx_writer.send(Error::new(
                                    ErrorKind::SendError,
                                    format!("Failed to write in socket: {e}")
                                )).await;
                                break;
                            }
                            if let Err(e) = write_stream.flush().await {
                                let _ = error_tx_writer.send(Error::new(
                                    ErrorKind::SendError,
                                    format!("Failed to flush socket: {}", e)
                                )).await;
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        });

        (TcpConnection {
            outgoing_tx,
            shutdown_tx,
            _read_handle: read_handle,
            _write_handle: write_handle,
        }, incoming_stream)
    }

    pub(crate) async fn send(&self, data: Bytes) -> Result<(), Error> {
        self.outgoing_tx.send(data)
            .await
            .map_err(|e| Error::new(ErrorKind::SendError, format!("Failed to pass frame to write task: {e}")))
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl Drop for TcpConnection {
    fn drop(&mut self) {
        self._write_handle.abort();
        self._read_handle.abort();
    }
}
