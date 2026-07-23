use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, Result};

use crate::program::NetworkMode;
use crate::RuntimeStateDir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMcpServerSpec {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeExecutionContext {
    pub network_mode: NetworkMode,
    /// Runtime-visible current working directory for program-backed turns.
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub runtime_state: Option<RuntimeStateDir>,
    pub runtime_path_projections: Vec<RuntimePathProjection>,
    pub mcp_servers: Vec<RuntimeMcpServerSpec>,
}

impl RuntimeExecutionContext {
    pub fn host_path_for_runtime_path(&self, runtime_path: impl AsRef<Path>) -> Option<PathBuf> {
        let runtime_path = runtime_path.as_ref();
        let mut best_match = None;
        let mut best_component_count = 0;

        for projection in &self.runtime_path_projections {
            let Some((component_count, resolution)) = projection.resolve_host_path(runtime_path)
            else {
                continue;
            };
            if component_count < best_component_count {
                continue;
            }
            if component_count == best_component_count
                && matches!(&best_match, Some(RuntimePathProjectionResolution::Blocked))
                && matches!(&resolution, RuntimePathProjectionResolution::Resolved(_))
            {
                continue;
            }
            best_match = Some(resolution);
            best_component_count = component_count;
        }

        match best_match {
            Some(RuntimePathProjectionResolution::Resolved(host_path)) => Some(host_path),
            Some(RuntimePathProjectionResolution::Blocked) | None => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePathProjectionKind {
    /// The runtime path maps a directory tree onto a host directory tree.
    Directory,
    /// The runtime path maps one exact runtime path onto one host path.
    Exact,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePathProjection {
    runtime_path: String,
    host_path: PathBuf,
    kind: RuntimePathProjectionKind,
}

impl RuntimePathProjection {
    pub fn directory(
        runtime_path: impl Into<String>,
        host_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        Self::new(
            runtime_path,
            host_path,
            RuntimePathProjectionKind::Directory,
        )
    }

    pub fn exact(runtime_path: impl Into<String>, host_path: impl Into<PathBuf>) -> Result<Self> {
        Self::new(runtime_path, host_path, RuntimePathProjectionKind::Exact)
    }

    fn new(
        runtime_path: impl Into<String>,
        host_path: impl Into<PathBuf>,
        kind: RuntimePathProjectionKind,
    ) -> Result<Self> {
        let runtime_path = normalize_absolute_runtime_path(runtime_path.into())?;
        let host_path = host_path.into();
        if !host_path.is_absolute() {
            return Err(anyhow!(
                "runtime path projection host path '{}' must be absolute",
                host_path.display()
            ));
        }
        Ok(Self {
            runtime_path,
            host_path,
            kind,
        })
    }

    fn resolve_host_path(
        &self,
        runtime_path: &Path,
    ) -> Option<(usize, RuntimePathProjectionResolution)> {
        let projected_runtime_path = Path::new(&self.runtime_path);
        let Ok(relative_path) = runtime_path.strip_prefix(projected_runtime_path) else {
            return None;
        };
        let component_count = projected_runtime_path.components().count();
        if relative_path.as_os_str().is_empty() {
            return Some((
                component_count,
                RuntimePathProjectionResolution::Resolved(self.host_path.clone()),
            ));
        }

        match self.kind {
            RuntimePathProjectionKind::Directory => {
                let Some(relative_path) = safe_relative_path(relative_path) else {
                    return Some((component_count, RuntimePathProjectionResolution::Blocked));
                };
                Some((
                    component_count,
                    RuntimePathProjectionResolution::Resolved(self.host_path.join(relative_path)),
                ))
            }
            RuntimePathProjectionKind::Exact => {
                Some((component_count, RuntimePathProjectionResolution::Blocked))
            }
        }
    }
}

fn normalize_absolute_runtime_path(runtime_path: String) -> Result<String> {
    let path = Path::new(&runtime_path);
    if !path.is_absolute() {
        return Err(anyhow!(
            "runtime path projection '{runtime_path}' must be absolute"
        ));
    }

    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(segment) => normalized.push(segment),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(anyhow!(
                    "runtime path projection '{runtime_path}' must be normalized"
                ));
            }
        }
    }

    normalized
        .to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("runtime path projection '{runtime_path}' is not valid UTF-8"))
}

enum RuntimePathProjectionResolution {
    Resolved(PathBuf),
    Blocked,
}

/// Normalize a relative path while rejecting absolute paths and parent traversal.
pub fn safe_relative_path(path: impl AsRef<Path>) -> Option<PathBuf> {
    let path = path.as_ref();
    let mut safe_path = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => safe_path.push(segment),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(safe_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeStateDir;

    #[test]
    fn host_runtime_state_is_never_an_implicit_runtime_path_projection() {
        let root = tempfile::tempdir().unwrap();
        let control_root = root.path().join("session-control");
        let runtime_state = RuntimeStateDir::new(
            root.path(),
            &control_root,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let mut context = RuntimeExecutionContext {
            network_mode: NetworkMode::None,
            working_dir: None,
            environment: Vec::new(),
            runtime_state: Some(runtime_state),
            runtime_path_projections: Vec::new(),
            mcp_servers: Vec::new(),
        };

        assert_eq!(context.host_path_for_runtime_path("/runtime/private"), None);

        let projected = root.path().join("projected-runtime");
        context.runtime_path_projections =
            vec![RuntimePathProjection::directory("/runtime", &projected).unwrap()];
        assert_eq!(
            context.host_path_for_runtime_path("/runtime/private"),
            Some(projected.join("private"))
        );
        assert!(!projected.starts_with(control_root));
    }
}
