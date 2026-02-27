use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Grant {
    pub grant_id: Uuid,
    pub skill_id: String,
    pub capability: String,
    pub scope: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
pub struct PolicyStore {
    grants: RwLock<HashMap<Uuid, Grant>>,
}

impl PolicyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn grant(
        &self,
        skill_id: String,
        capability: String,
        scope: String,
        ttl_seconds: Option<i64>,
    ) -> Grant {
        let created_at = Utc::now();
        let expires_at = ttl_seconds.map(|seconds| created_at + Duration::seconds(seconds));

        let grant = Grant {
            grant_id: Uuid::new_v4(),
            skill_id,
            capability,
            scope,
            created_at,
            expires_at,
        };

        self.grants
            .write()
            .await
            .insert(grant.grant_id, grant.clone());
        grant
    }

    pub async fn revoke(&self, grant_id: Uuid) -> bool {
        self.grants.write().await.remove(&grant_id).is_some()
    }

    pub async fn is_allowed(&self, skill_id: &str, capability: &str, scope: &str) -> bool {
        let now = Utc::now();
        self.grants
            .read()
            .await
            .values()
            .filter(|grant| {
                grant
                    .expires_at
                    .map(|expires_at| expires_at >= now)
                    .unwrap_or(true)
            })
            .any(|grant| {
                let skill_match = grant.skill_id == skill_id;
                let capability_match = grant.capability == capability || grant.capability == "*";
                let scope_match = grant.scope == scope || grant.scope == "*";
                skill_match && capability_match && scope_match
            })
    }
}

#[cfg(test)]
mod tests {
    use super::PolicyStore;

    #[tokio::test]
    async fn defaults_to_deny() {
        let store = PolicyStore::new();
        assert!(!store.is_allowed("skill-a", "skill.use", "*").await);
    }

    #[tokio::test]
    async fn allows_after_grant() {
        let store = PolicyStore::new();
        store
            .grant(
                "skill-a".to_string(),
                "skill.use".to_string(),
                "*".to_string(),
                None,
            )
            .await;

        assert!(store.is_allowed("skill-a", "skill.use", "*").await);
    }
}
