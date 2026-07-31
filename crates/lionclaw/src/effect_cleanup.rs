use async_trait::async_trait;

use crate::model::EffectResource;
use crate::ports::{EffectCleaner, EffectCleanupFailure, EffectCleanupRequest};
use crate::workspace;

pub struct LocalEffectCleaner {
    oci_engine: String,
}

impl LocalEffectCleaner {
    pub fn new(oci_engine: String) -> Self {
        Self { oci_engine }
    }
}

#[async_trait]
impl EffectCleaner for LocalEffectCleaner {
    async fn quiesce(&self, request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        let resource_name = request.effect_id.resource_name();
        lionclaw_confinement::remove_oci_container(&self.oci_engine, &resource_name)
            .await
            .map_err(|error| EffectCleanupFailure {
                resource: EffectResource::Container,
                detail: error.to_string(),
            })?;
        lionclaw_confinement::remove_oci_container(
            &self.oci_engine,
            &format!("{resource_name}-proxy"),
        )
        .await
        .map_err(|error| EffectCleanupFailure {
            resource: EffectResource::NetworkProxyContainer,
            detail: error.to_string(),
        })
    }

    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        let resource_name = request.effect_id.resource_name();
        let effect_dir =
            crate::resources::MissionDirs::new(&request.state_dir, &request.mission_id)
                .effect(&request.effect_id);
        let mut failures = Vec::new();

        if let Err(error) =
            lionclaw_confinement::remove_oci_secret(&self.oci_engine, &resource_name).await
        {
            failures.push((EffectResource::RuntimeSecret, error.to_string()));
        }
        if let Err(error) = lionclaw_confinement::remove_oci_container(
            &self.oci_engine,
            &format!("{resource_name}-proxy"),
        )
        .await
        {
            failures.push((EffectResource::NetworkProxyContainer, error.to_string()));
        }
        if let Err(error) = lionclaw_confinement::remove_oci_network(
            &self.oci_engine,
            &format!("{resource_name}-net"),
        )
        .await
        {
            failures.push((EffectResource::Network, error.to_string()));
        }
        if let Err(error) = lionclaw_confinement::remove_oci_network(
            &self.oci_engine,
            &format!("{resource_name}-egress"),
        )
        .await
        {
            failures.push((EffectResource::Network, error.to_string()));
        }
        if let Err(error) = effect_dir.remove().await {
            failures.push((EffectResource::EffectDirectory, error.to_string()));
        }
        if request.discard_artifact {
            if let Err(error) = workspace::discard_worker_result(
                &request.workspace_dir,
                request.mission_id.as_str(),
                &request.effect_id,
            )
            .await
            {
                failures.push((EffectResource::WriterRef, error.to_string()));
            }
        }

        let Some((resource, detail)) = failures.first().cloned() else {
            return Ok(());
        };
        let detail = if failures.len() == 1 {
            detail
        } else {
            failures
                .into_iter()
                .map(|(resource, detail)| format!("{resource:?}: {detail}"))
                .collect::<Vec<_>>()
                .join("; ")
        };
        Err(EffectCleanupFailure { resource, detail })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn cleanup_removes_internal_and_egress_networks() {
        let temp = tempfile::tempdir().unwrap();
        let engine = temp.path().join("fake-oci");
        let log = temp.path().join("oci.log");
        std::fs::write(
            &engine,
            format!("#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n", log.display()),
        )
        .unwrap();
        std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mission_id = crate::model::MissionId::parse("m123456789abc").unwrap();
        let effect_id = crate::model::EffectId::for_parts(&["oracle", "cleanup-egress"]);
        crate::resources::MissionDirs::new(temp.path(), &mission_id)
            .effect(&effect_id)
            .oracle()
            .prepare()
            .unwrap();

        LocalEffectCleaner::new(engine.to_string_lossy().into_owned())
            .cleanup(EffectCleanupRequest {
                mission_id,
                effect_id: effect_id.clone(),
                workspace_dir: temp.path().join("workspace"),
                state_dir: temp.path().to_path_buf(),
                discard_artifact: false,
            })
            .await
            .expect("cleanup");

        let log = std::fs::read_to_string(log).unwrap();
        let resource_name = effect_id.resource_name();
        assert!(
            log.contains(&format!("network rm --force {resource_name}-net")),
            "missing internal network cleanup in {log}"
        );
        assert!(
            log.contains(&format!("network rm --force {resource_name}-egress")),
            "missing egress network cleanup in {log}"
        );
    }
}
