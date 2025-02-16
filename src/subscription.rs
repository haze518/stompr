use std::collections::hash_map::HashMap;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio::sync::RwLock;

use crate::frame::Frame;

pub struct Subscription {
    pub_sub: RwLock<HashMap<String, Sender<Frame>>>,
}

impl Subscription {
    pub fn new() -> Self {
        Self { pub_sub: RwLock::new(HashMap::new()) }
    }

    pub async fn subscribe(&self, key: String) -> Receiver<Frame> {
        let mut pub_sub = self.pub_sub.write().await;
        pub_sub.entry(key).or_insert_with(|| broadcast::channel(1024).0).subscribe()
    }

    pub async fn publish(&self, key: &str, frame: Frame) -> usize {
        let pub_sub = self.pub_sub.read().await;
        pub_sub.get(key).map(|tx| tx.send(frame).unwrap_or(0)).unwrap_or(0)
    }

    pub async fn publish_all(&self, frame: Frame) -> usize {
        let pub_sub = self.pub_sub.read().await;

        pub_sub.iter().map(|(_, tx)| tx.send(frame.clone()).unwrap_or(0)).sum()
    }

    pub async fn unsubscribe(&self, key: &str) {
        let mut pub_sub = self.pub_sub.write().await;
        pub_sub.remove(key);
    }
}
