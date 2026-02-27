use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RuntimeAdapterInfo {
    pub id: String,
    pub version: String,
    pub healthy: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionStartInput {
    pub session_id: Uuid,
    pub working_dir: Option<String>,
    pub selected_skills: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionHandle {
    pub runtime_session_id: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeTurnInput {
    pub runtime_session_id: String,
    pub prompt: String,
    pub selected_skills: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    TextDelta(String),
    Status(String),
    Done,
    Error(String),
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn info(&self) -> RuntimeAdapterInfo;
    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
    async fn turn(&self, input: RuntimeTurnInput) -> Result<Vec<RuntimeEvent>>;
    async fn cancel(&self, handle: &RuntimeSessionHandle, reason: Option<String>) -> Result<()>;
    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()>;
}

#[derive(Default)]
pub struct RuntimeRegistry {
    adapters: RwLock<HashMap<String, Arc<dyn RuntimeAdapter>>>,
}

impl RuntimeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, id: impl Into<String>, adapter: Arc<dyn RuntimeAdapter>) {
        self.adapters.write().await.insert(id.into(), adapter);
    }

    pub async fn get(&self, id: &str) -> Option<Arc<dyn RuntimeAdapter>> {
        self.adapters.read().await.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<String> {
        self.adapters.read().await.keys().cloned().collect()
    }
}

pub struct MockRuntimeAdapter;

#[async_trait]
impl RuntimeAdapter for MockRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "mock".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("mock-{}", Uuid::new_v4()),
        })
    }

    async fn turn(&self, input: RuntimeTurnInput) -> Result<Vec<RuntimeEvent>> {
        let mut events = Vec::new();
        events.push(RuntimeEvent::Status(
            "mock runtime started turn".to_string(),
        ));

        let skill_context = if input.selected_skills.is_empty() {
            "no skill context selected".to_string()
        } else {
            format!("selected skills: {}", input.selected_skills.join(", "))
        };

        events.push(RuntimeEvent::TextDelta(format!(
            "[mock] {} | prompt: {}",
            skill_context, input.prompt
        )));
        events.push(RuntimeEvent::Done);
        Ok(events)
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}
