use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use super::policy::Capability;

pub mod adapters;
pub mod builtins;

pub use adapters::{
    CodexRuntimeAdapter, CodexRuntimeConfig, MockRuntimeAdapter, OpenCodeRuntimeAdapter,
    OpenCodeRuntimeConfig,
};
pub use builtins::{
    register_builtin_runtime_adapters, BUILTIN_RUNTIME_CODEX, BUILTIN_RUNTIME_MOCK,
    BUILTIN_RUNTIME_OPENCODE,
};

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
    pub environment: Vec<(String, String)>,
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
pub struct RuntimeCapabilityRequest {
    pub request_id: String,
    pub skill_id: String,
    pub capability: Capability,
    pub scope: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct RuntimeCapabilityResult {
    pub request_id: String,
    pub allowed: bool,
    pub reason: Option<String>,
    pub output: Value,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeTurnResult {
    pub capability_requests: Vec<RuntimeCapabilityRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMessageLane {
    Answer,
    Reasoning,
}

impl RuntimeMessageLane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Answer => "answer",
            Self::Reasoning => "reasoning",
        }
    }
}

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    MessageDelta {
        lane: RuntimeMessageLane,
        text: String,
    },
    Status {
        code: Option<String>,
        text: String,
    },
    Done,
    Error {
        code: Option<String>,
        text: String,
    },
}

pub type RuntimeEventSender = mpsc::UnboundedSender<RuntimeEvent>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiddenTurnSupport {
    Unsupported,
    SideEffectFree,
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn info(&self) -> RuntimeAdapterInfo;
    fn hidden_turn_support(&self) -> HiddenTurnSupport {
        HiddenTurnSupport::Unsupported
    }
    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult>;
    async fn resolve_capability_requests(
        &self,
        handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()>;
    async fn cancel(&self, handle: &RuntimeSessionHandle, reason: Option<String>) -> Result<()>;
    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()>;
}

#[derive(Default, Clone)]
pub struct RuntimeRegistry {
    adapters: Arc<RwLock<HashMap<String, Arc<dyn RuntimeAdapter>>>>,
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
