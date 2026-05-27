use std::sync::Arc;

use super::{MockRuntimeAdapter, RuntimeRegistry};

pub const BUILTIN_RUNTIME_MOCK: &str = "mock";

pub async fn register_builtin_runtime_adapters(registry: &RuntimeRegistry) {
    registry
        .register(BUILTIN_RUNTIME_MOCK, Arc::new(MockRuntimeAdapter))
        .await;
}

#[cfg(test)]
mod tests {
    use super::{register_builtin_runtime_adapters, RuntimeRegistry, BUILTIN_RUNTIME_MOCK};

    #[tokio::test]
    async fn registers_mock_runtime_only() {
        let registry = RuntimeRegistry::new();
        register_builtin_runtime_adapters(&registry).await;
        let runtime_ids = registry.list().await;

        assert!(
            runtime_ids.contains(&BUILTIN_RUNTIME_MOCK.to_string()),
            "mock runtime should be registered"
        );
        assert_eq!(runtime_ids, vec![BUILTIN_RUNTIME_MOCK.to_string()]);
    }
}
