use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ChannelHealth {
    pub healthy: bool,
    pub detail: String,
}

#[async_trait]
pub trait ChannelSkill: Send + Sync {
    fn id(&self) -> &str;
    async fn init(&self) -> Result<()>;
    async fn health(&self) -> Result<ChannelHealth>;
    async fn send(&self, conversation_ref: &str, content: &str) -> Result<String>;
}

#[derive(Default)]
pub struct ChannelRegistry {
    channels: RwLock<HashMap<String, Arc<dyn ChannelSkill>>>,
}

impl ChannelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, channel: Arc<dyn ChannelSkill>) {
        self.channels
            .write()
            .await
            .insert(channel.id().to_string(), channel);
    }

    pub async fn get(&self, id: &str) -> Option<Arc<dyn ChannelSkill>> {
        self.channels.read().await.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<String> {
        self.channels.read().await.keys().cloned().collect()
    }
}

pub struct LocalCliChannel;

#[async_trait]
impl ChannelSkill for LocalCliChannel {
    fn id(&self) -> &str {
        "local-cli"
    }

    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn health(&self) -> Result<ChannelHealth> {
        Ok(ChannelHealth {
            healthy: true,
            detail: "local channel ready".to_string(),
        })
    }

    async fn send(&self, conversation_ref: &str, content: &str) -> Result<String> {
        Ok(format!("local:{}:{}", conversation_ref, content.len()))
    }
}
