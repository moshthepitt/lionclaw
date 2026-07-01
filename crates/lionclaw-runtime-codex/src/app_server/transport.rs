use anyhow::{Context, Result};
use async_trait::async_trait;
use lionclaw_runtime_api::{ExecutionOutput, RuntimeProgramSession};
use serde_json::Value;

#[async_trait]
pub(crate) trait AppServerTransport {
    async fn send(&mut self, message: &Value) -> Result<()>;
    async fn recv(&mut self) -> Result<Option<AppServerMessage>>;
    async fn shutdown(&mut self) -> Result<ExecutionOutput>;
}

#[derive(Debug, Clone)]
pub(crate) struct AppServerMessage {
    pub(crate) raw: String,
    pub(crate) value: Value,
}

impl AppServerMessage {
    pub(crate) fn value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn into_value(self) -> Value {
        self.value
    }
}

impl From<Value> for AppServerMessage {
    fn from(value: Value) -> Self {
        let raw = serde_json::to_string(&value).unwrap_or_else(|_| value.to_string());
        Self { raw, value }
    }
}

pub(crate) struct ExecutionSessionTransport {
    session: Option<Box<dyn RuntimeProgramSession>>,
}

impl ExecutionSessionTransport {
    pub(crate) fn new(session: Box<dyn RuntimeProgramSession>) -> Self {
        Self {
            session: Some(session),
        }
    }
}

#[async_trait]
impl AppServerTransport for ExecutionSessionTransport {
    async fn send(&mut self, message: &Value) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .context("codex app-server session is already closed")?;
        session.write_line(&serde_json::to_string(message)?).await
    }

    async fn recv(&mut self) -> Result<Option<AppServerMessage>> {
        let session = self
            .session
            .as_mut()
            .context("codex app-server session is already closed")?;
        loop {
            let Some(line) = session.read_line().await? else {
                return Ok(None);
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value = serde_json::from_str(trimmed)
                .with_context(|| format!("invalid codex app-server JSON-RPC line: {trimmed}"))?;
            return Ok(Some(AppServerMessage {
                raw: trimmed.to_string(),
                value,
            }));
        }
    }

    async fn shutdown(&mut self) -> Result<ExecutionOutput> {
        let Some(session) = self.session.take() else {
            return Ok(ExecutionOutput::default());
        };
        session.shutdown().await
    }
}
