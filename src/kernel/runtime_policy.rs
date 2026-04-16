use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct RuntimeExecutionRule {
    pub working_dir_roots: Vec<PathBuf>,
    pub allowed_env_passthrough_keys: BTreeSet<String>,
    pub min_timeout: Duration,
    pub max_timeout: Duration,
}

impl Default for RuntimeExecutionRule {
    fn default() -> Self {
        let working_dir_roots = std::env::current_dir()
            .ok()
            .and_then(|path| canonicalize_if_exists(&path).ok())
            .into_iter()
            .collect();

        Self {
            working_dir_roots,
            allowed_env_passthrough_keys: BTreeSet::new(),
            min_timeout: Duration::from_millis(1),
            max_timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeExecutionPolicy {
    default_rule: RuntimeExecutionRule,
    rules: HashMap<String, RuntimeExecutionRule>,
}

impl RuntimeExecutionPolicy {
    pub fn for_working_dir_root(root: PathBuf) -> Self {
        Self::default().with_default_rule(RuntimeExecutionRule {
            working_dir_roots: vec![root],
            ..RuntimeExecutionRule::default()
        })
    }

    pub fn with_default_rule(mut self, rule: RuntimeExecutionRule) -> Self {
        self.default_rule = rule;
        self
    }

    pub fn with_rule(mut self, runtime_id: impl Into<String>, rule: RuntimeExecutionRule) -> Self {
        self.rules.insert(runtime_id.into(), rule);
        self
    }

    pub fn evaluate(
        &self,
        runtime_id: &str,
        request: RuntimeExecutionRequest,
        default_idle_timeout: Duration,
        default_hard_timeout: Duration,
    ) -> Result<RuntimeExecutionContext, String> {
        let rule = self.rules.get(runtime_id).unwrap_or(&self.default_rule);

        let working_dir =
            resolve_working_dir(request.working_dir.as_deref(), &rule.working_dir_roots)?;
        let environment = resolve_environment(
            &request.env_passthrough_keys,
            &rule.allowed_env_passthrough_keys,
        )?;

        let requested_timeout = request
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(default_idle_timeout);

        if requested_timeout < rule.min_timeout || requested_timeout > rule.max_timeout {
            return Err(format!(
                "requested timeout {} ms is outside policy bounds ({}..={} ms)",
                requested_timeout.as_millis(),
                rule.min_timeout.as_millis(),
                rule.max_timeout.as_millis()
            ));
        }

        Ok(RuntimeExecutionContext {
            working_dir,
            environment,
            idle_timeout: requested_timeout,
            hard_timeout: default_hard_timeout.max(requested_timeout),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeExecutionRequest {
    pub working_dir: Option<String>,
    pub env_passthrough_keys: Vec<String>,
    pub timeout_ms: Option<u64>,
}

impl RuntimeExecutionRequest {
    pub fn new(
        working_dir: Option<String>,
        env_passthrough_keys: Vec<String>,
        timeout_ms: Option<u64>,
    ) -> Self {
        Self {
            working_dir,
            env_passthrough_keys,
            timeout_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeExecutionContext {
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub idle_timeout: Duration,
    pub hard_timeout: Duration,
}

fn resolve_working_dir(
    requested_working_dir: Option<&str>,
    allowed_roots: &[PathBuf],
) -> Result<Option<String>, String> {
    let Some(raw_working_dir) = requested_working_dir else {
        return Ok(None);
    };
    if raw_working_dir.trim().is_empty() {
        return Err("runtime_working_dir cannot be empty".to_string());
    }

    if allowed_roots.is_empty() {
        return Err("runtime working directory requests are not allowed by policy".to_string());
    }

    let requested = resolve_requested_dir(raw_working_dir)?;
    let requested_canonical = canonicalize_if_exists(&requested)
        .map_err(|err| format!("runtime working directory is invalid: {err}"))?;

    let mut allowed = false;
    for root in allowed_roots {
        let root_canonical =
            canonicalize_if_exists(root).map_err(|err| format!("invalid policy root: {err}"))?;
        if requested_canonical == root_canonical || requested_canonical.starts_with(&root_canonical)
        {
            allowed = true;
            break;
        }
    }

    if !allowed {
        return Err(format!(
            "runtime_working_dir '{}' is outside allowed roots",
            requested_canonical.display()
        ));
    }

    Ok(Some(requested_canonical.to_string_lossy().to_string()))
}

fn resolve_requested_dir(raw: &str) -> Result<PathBuf, String> {
    let requested = PathBuf::from(raw);
    if requested.is_absolute() {
        return Ok(requested);
    }

    let cwd =
        std::env::current_dir().map_err(|err| format!("failed to read current dir: {err}"))?;
    Ok(cwd.join(requested))
}

fn resolve_environment(
    requested_keys: &[String],
    allowed_keys: &BTreeSet<String>,
) -> Result<Vec<(String, String)>, String> {
    if requested_keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut deduped = BTreeSet::new();
    let mut env = Vec::new();

    for key in requested_keys {
        let normalized = key.trim().to_string();
        if normalized.is_empty() {
            return Err("runtime_env_passthrough cannot include empty keys".to_string());
        }
        if !allowed_keys.contains(&normalized) {
            return Err(format!(
                "runtime_env_passthrough key '{normalized}' is not allowed by policy"
            ));
        }
        if !deduped.insert(normalized.clone()) {
            continue;
        }
        if let Ok(value) = std::env::var(&normalized) {
            env.push((normalized, value));
        }
    }

    Ok(env)
}

fn canonicalize_if_exists(path: &Path) -> std::io::Result<PathBuf> {
    path.canonicalize()
}

#[cfg(test)]
mod tests {
    use super::{RuntimeExecutionPolicy, RuntimeExecutionRequest, RuntimeExecutionRule};
    use std::collections::BTreeSet;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn policy_rejects_timeout_outside_bounds() {
        let rule = RuntimeExecutionRule {
            min_timeout: Duration::from_millis(200),
            max_timeout: Duration::from_millis(500),
            ..RuntimeExecutionRule::default()
        };
        let policy = RuntimeExecutionPolicy::default().with_rule("mock", rule);

        let err = policy
            .evaluate(
                "mock",
                RuntimeExecutionRequest::new(None, Vec::new(), Some(100)),
                Duration::from_millis(300),
                Duration::from_millis(900),
            )
            .expect_err("timeout should be rejected");

        assert!(err.contains("outside policy bounds"));
    }

    #[test]
    fn policy_allows_working_dir_inside_root() {
        let sandbox = tempdir().expect("temp dir");
        let child = sandbox.path().join("child");
        std::fs::create_dir(&child).expect("create child");

        let rule = RuntimeExecutionRule {
            working_dir_roots: vec![sandbox.path().to_path_buf()],
            ..RuntimeExecutionRule::default()
        };
        let policy = RuntimeExecutionPolicy::default().with_rule("mock", rule);

        let context = policy
            .evaluate(
                "mock",
                RuntimeExecutionRequest::new(
                    Some(child.to_string_lossy().to_string()),
                    Vec::new(),
                    Some(300),
                ),
                Duration::from_millis(300),
                Duration::from_millis(900),
            )
            .expect("working dir should be allowed");

        assert!(
            context.working_dir.expect("working dir").contains("child"),
            "resolved working dir should match requested path"
        );
        assert_eq!(context.idle_timeout, Duration::from_millis(300));
        assert_eq!(context.hard_timeout, Duration::from_millis(900));
    }

    #[test]
    fn policy_rejects_unknown_passthrough_keys() {
        let mut allowed = BTreeSet::new();
        allowed.insert("ALLOWED_KEY".to_string());
        let rule = RuntimeExecutionRule {
            allowed_env_passthrough_keys: allowed,
            ..RuntimeExecutionRule::default()
        };
        let policy = RuntimeExecutionPolicy::default().with_rule("mock", rule);

        let err = policy
            .evaluate(
                "mock",
                RuntimeExecutionRequest::new(None, vec!["FORBIDDEN_KEY".to_string()], Some(300)),
                Duration::from_millis(300),
                Duration::from_millis(900),
            )
            .expect_err("unknown key should be denied");

        assert!(err.contains("not allowed by policy"));
    }
}
