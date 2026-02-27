use std::sync::Arc;

use super::{adapters::CodexRuntimeAdapter, MockRuntimeAdapter, RuntimeRegistry};

pub const BUILTIN_RUNTIME_MOCK: &str = "mock";
pub const BUILTIN_RUNTIME_CODEX: &str = "codex";

pub async fn register_builtin_runtime_adapters(registry: &RuntimeRegistry) {
    registry
        .register(BUILTIN_RUNTIME_MOCK, Arc::new(MockRuntimeAdapter))
        .await;
    registry
        .register(
            BUILTIN_RUNTIME_CODEX,
            Arc::new(CodexRuntimeAdapter::from_env()),
        )
        .await;
}
