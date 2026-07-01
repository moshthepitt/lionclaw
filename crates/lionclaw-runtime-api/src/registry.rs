use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::RwLock;

use crate::adapter::RuntimeAdapter;

#[derive(Default, Clone)]
pub struct RuntimeRegistry {
    adapters: Arc<RwLock<BTreeMap<String, Arc<dyn RuntimeAdapter>>>>,
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
