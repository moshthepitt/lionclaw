use std::sync::Arc;

use super::{
    adapters::{CodexRuntimeAdapter, OpenCodeRuntimeAdapter},
    MockRuntimeAdapter, RuntimeRegistry,
};

pub const BUILTIN_RUNTIME_MOCK: &str = "mock";
pub const BUILTIN_RUNTIME_CODEX: &str = "codex";
pub const BUILTIN_RUNTIME_OPENCODE: &str = "opencode";

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
    registry
        .register(
            BUILTIN_RUNTIME_OPENCODE,
            Arc::new(OpenCodeRuntimeAdapter::from_env()),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::{
        register_builtin_runtime_adapters, RuntimeRegistry, BUILTIN_RUNTIME_CODEX,
        BUILTIN_RUNTIME_MOCK, BUILTIN_RUNTIME_OPENCODE,
    };

    #[tokio::test]
    async fn registers_all_builtin_runtime_ids() {
        let registry = RuntimeRegistry::new();
        register_builtin_runtime_adapters(&registry).await;
        let runtime_ids = registry.list().await;

        assert!(
            runtime_ids.contains(&BUILTIN_RUNTIME_MOCK.to_string()),
            "mock runtime should be registered"
        );
        assert!(
            runtime_ids.contains(&BUILTIN_RUNTIME_CODEX.to_string()),
            "codex runtime should be registered"
        );
        assert!(
            runtime_ids.contains(&BUILTIN_RUNTIME_OPENCODE.to_string()),
            "opencode runtime should be registered"
        );
    }
}
