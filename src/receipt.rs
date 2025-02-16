use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::timeout;

use crate::error::{Error, ErrorKind};
use crate::frame::Frame;

pub struct Receipt {
    receipts: RwLock<HashMap<u32, Sender<Frame>>>,
    current_id: AtomicU32
}

impl Receipt {
    pub fn new() -> Self {
        Self {
            receipts: RwLock::new(HashMap::new()),
            current_id: AtomicU32::new(1),
        }
    }

    pub async fn subscribe(&self, id: u32) -> Result<Receiver<Frame>, Error> {
        let mut receipts = self.receipts.write().await;
        if receipts.contains_key(&id) {
            return Err(Error::new(
                ErrorKind::ReceiptError,
                "receipt with same key is already exist",
            ));
        }

        let (tx, rx) = oneshot::channel::<Frame>();
        receipts.insert(id, tx);
        Ok(rx)
    }

    pub async fn publish(&self, id: u32, frame: Frame) -> Result<(), Error> {
        let mut receipts = self.receipts.write().await;
        let _ = receipts
            .remove(&id)
            .ok_or_else(|| Error::new(ErrorKind::ReceiptError, format!("unknown recepient id: {id}")))?
            .send(frame);
        Ok(())
    }

    pub async fn publish_all(&self, frame: Frame) {
        let mut receipts = self.receipts.write().await;

        receipts.drain().for_each(|(_, tx)| {
            let _ = tx.send(frame.clone());
        });
    }

    pub fn next_id(&self) -> u32 {
        self.current_id.fetch_add(1, Ordering::SeqCst)
    }
}
