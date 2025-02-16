use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, ErrorKind};
use crate::frame::{CommandKind, Frame};
use crate::receipt::Receipt;
use crate::subscription::Subscription;
use crate::transport::Transport;
use bytes::Bytes;
use tokio_stream::StreamExt;

use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::error;

pub struct StompClient<T>
where
    T: Transport,
{
    transport: Arc<T>,
    subscriptions: Arc<Subscription>,
    recipients: Arc<Receipt>,
    is_connected: AtomicBool,
    _listen_handle: JoinHandle<()>,
}

impl<T> StompClient<T>
where
    T: Transport + Send + Sync + 'static,
{
    pub async fn connect(config: T::Config) -> Result<Self, Error> {
        let (err_tx, mut err_rx) = mpsc::channel::<Error>(32);
        let mut transport = T::new(config, err_tx).await?;
        let mut incoming_stream = transport.into_stream();
        let transport = Arc::new(transport);
        let subscriptions = Arc::new(Subscription::new());
        let receipts = Arc::new(Receipt::new());

        let subs_clone = Arc::clone(&subscriptions);
        let receipts_clone = Arc::clone(&receipts);
        let transport_clone = transport.clone();

        let listen_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_data = incoming_stream.next() => {
                        if maybe_data.is_none() {
                            error!("Got empty data from incming stream");
                            break
                        }
                        match parse(maybe_data.unwrap()) {
                            Ok(frame) => {
                                match frame.command {
                                    CommandKind::Message => {
                                        if let Some(topic) = frame.subscription() {
                                            if 0 == subs_clone.publish(&topic, frame).await {
                                                error!("Error publishing message in subscription: {topic}");
                                            }
                                        } else {
                                            error!("Received MESSAGE without 'subscription' header");
                                        }
                                    },
                                    CommandKind::Receipt => {
                                        match frame.receipt_id() {
                                            Ok(maybe_receipt_id) => {
                                                if let Some(receipt_id) = maybe_receipt_id {
                                                    if let Err(e) = receipts_clone.publish(receipt_id, frame).await {
                                                        error!("Error publishing message in receipt channel: {}, err: {:?}", receipt_id, e);
                                                    }
                                                } else {
                                                    error!("Received RECEIPT without header 'receipt-id'");
                                                }
                                            },
                                            Err(e) => {
                                                error!("Failed to get RECEIPT, error: {:?}", e);
                                            }
                                        }
                                    },
                                    CommandKind::Error => {
                                        error!("Received ERROR frame: {:?}", frame);
                                        subs_clone.publish_all(frame.clone()).await;
                                        receipts_clone.publish_all(frame).await;
                                        transport_clone.shutdown();
                                        break;
                                    },
                                    _ => {
                                        error!("Raw frame command: {:?}", frame.command);
                                    }
                                }        
                            },
                            Err(e) => {
                                error!("Could not parse frame: {:?}", e);
                            }
                        }
                    },
                    maybe_err = err_rx.recv() => {
                        if let Some(err) = maybe_err {
                            let error_frame = Frame::new(CommandKind::Error)
                                .with_header("message".to_string(), err.to_string());
                            let _ = transport_clone.send(error_frame.to_bytes()).await;
                            transport_clone.shutdown();
                            break;
                        }
                    },
                }
            }
        });

        Ok(Self {
            transport,
            subscriptions,
            recipients: receipts,
            is_connected: AtomicBool::new(true),
            _listen_handle: listen_handle,
        })
    }

    pub async fn send(&self, frame: Frame) -> Result<(), Error> {
        self.transport.send(frame.to_bytes()).await
    }

    pub async fn subscribe(&self, destination: String) -> Receiver<Frame> {
        self.subscriptions.subscribe(destination).await
    }

    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), Error> {
        let frame = Frame::new(CommandKind::Unsubscribe).with_header("id".to_string(), subscription_id.to_string());
        self.transport.send(frame.to_bytes()).await?;
        self.subscriptions.unsubscribe(subscription_id).await;
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), Error> {
        if !self.is_connected.load(Ordering::Acquire) {
            return Ok(())
        }

        let recepient_id = self.recipients.next_id();
        let receipt_rx = self.recipients.subscribe(recepient_id).await?;
        let frame = Frame::new(CommandKind::Disconnect).with_header("receipt".to_string(), recepient_id.to_string());
        self.transport.send(frame.to_bytes()).await?;

        timeout(Duration::from_secs(5), receipt_rx)
            .await
            .map_err(|_| Error::new(ErrorKind::TimeoutError, "timeout error on disconnect"))?
            .map_err(|e| Error::new(ErrorKind::ReceiptError, format!("Failed to get frame from receipt {e}")))?;

        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }
}

impl<T> Drop for StompClient<T>
where
    T: Transport,
{
    fn drop(&mut self) {
        self.transport.shutdown();
        self._listen_handle.abort();
    }
}

fn parse(data: Bytes) -> Result<Frame, Error> {
    let mut cursor = Cursor::new(&data);
    Frame::parse(&mut cursor)
}
