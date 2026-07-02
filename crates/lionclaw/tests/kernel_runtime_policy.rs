use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

use lionclaw::{
    contracts::{SessionOpenRequest, SessionTurnRequest, TrustTier},
    kernel::{
        runtime::{ExecutionPreset, InstallPolicy, NetworkMode},
        Kernel, KernelError, KernelOptions, RuntimeExecutionPolicy, RuntimeExecutionRule,
    },
};
use tempfile::TempDir;

#[tokio::test]
async fn runtime_policy_allows_configured_execution_overrides() {
    let env = TestEnv::new();
    let work_dir = env.path().join("work");
    std::fs::create_dir(&work_dir).expect("create work dir");

    let mut allowed_keys = BTreeSet::new();
    allowed_keys.insert("PATH".to_string());
    let rule = RuntimeExecutionRule {
        working_dir_roots: vec![env.path().to_path_buf()],
        allowed_env_passthrough_keys: allowed_keys,
        min_timeout: Duration::from_millis(10),
        max_timeout: Duration::from_millis(2_000),
    };

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(300),
            runtime_turn_hard_timeout: Duration::from_millis(900),
            runtime_execution_policy: RuntimeExecutionPolicy::default().with_rule("mock", rule),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "runtime-policy-allow".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let turn = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "run with runtime overrides".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: Some(work_dir.to_string_lossy().to_string()),
            runtime_timeout_ms: Some(750),
            runtime_env_passthrough: Some(vec!["PATH".to_string()]),
        })
        .await
        .expect("turn should succeed");
    assert_eq!(turn.runtime_id, "mock", "mock runtime should execute");

    let policy_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("runtime.plan.allow".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query plan audit");

    assert_eq!(
        policy_events.events.len(),
        1,
        "one plan allow event expected"
    );
    let details = &policy_events.events[0].details;
    assert_eq!(details["effective_preset_name"].as_str(), Some("everyday"));
    assert_eq!(details["effective_timeout_ms"].as_u64(), Some(750));
    assert_eq!(details["effective_hard_timeout_ms"].as_u64(), Some(900));
    assert_eq!(details["effective_env_passthrough_count"].as_u64(), Some(1));
    assert_eq!(details["effective_environment_count"].as_u64(), Some(2));
}

#[tokio::test]
async fn runtime_policy_denies_unallowed_env_passthrough() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "runtime-policy-deny".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "deny runtime env passthrough".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: Some(vec!["PATH".to_string()]),
        })
        .await
        .expect_err("turn should be denied by runtime policy");

    match err {
        KernelError::BadRequest(message) => {
            assert!(
                message.contains("not allowed by policy"),
                "deny reason should mention policy restriction"
            );
        }
        other => panic!("unexpected error variant: {other}"),
    }

    let deny_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("runtime.plan.deny".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query deny audit");

    assert_eq!(deny_events.events.len(), 1, "one plan deny event expected");
    assert!(
        deny_events.events[0].details["reason"]
            .as_str()
            .expect("deny reason")
            .contains("not allowed by policy"),
        "audit should capture deny reason"
    );
}

#[tokio::test]
async fn install_policy_system_allow_audit_records_effective_policy_and_root_posture() {
    let env = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            default_preset_name: Some("system-installs".to_string()),
            execution_presets: BTreeMap::from([(
                "system-installs".to_string(),
                ExecutionPreset {
                    install_policy: InstallPolicy::System,
                    ..ExecutionPreset::default()
                },
            )]),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "runtime-policy-system-allow".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "run with system install policy".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should succeed");

    let policy_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("runtime.plan.allow".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query plan audit");

    assert_eq!(policy_events.events.len(), 1);
    let details = &policy_events.events[0].details;
    assert_eq!(
        details["effective_preset_name"].as_str(),
        Some("system-installs")
    );
    assert_eq!(details["effective_install_policy"].as_str(), Some("system"));
    assert_eq!(details["effective_root_in_userns"].as_bool(), Some(true));
}

#[tokio::test]
async fn install_policy_system_deny_audit_records_requested_policy_context() {
    let env = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            default_preset_name: Some("offline-system".to_string()),
            execution_presets: BTreeMap::from([(
                "offline-system".to_string(),
                ExecutionPreset {
                    install_policy: InstallPolicy::System,
                    network_mode: NetworkMode::None,
                    ..ExecutionPreset::default()
                },
            )]),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "runtime-policy-system-deny".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "deny unsafe system install policy".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should be denied before runtime launch");

    match err {
        KernelError::BadRequest(message) => {
            assert!(message.contains("install-policy=system"), "{message}");
            assert!(message.contains("network-mode=none"), "{message}");
        }
        other => panic!("unexpected error variant: {other}"),
    }

    let deny_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("runtime.plan.deny".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query deny audit");

    assert_eq!(deny_events.events.len(), 1);
    let details = &deny_events.events[0].details;
    assert_eq!(
        details["requested_effective_preset_name"].as_str(),
        Some("offline-system")
    );
    assert_eq!(details["requested_install_policy"].as_str(), Some("system"));
    assert_eq!(details["requested_network_mode"].as_str(), Some("none"));
    assert!(details["reason"]
        .as_str()
        .expect("deny reason")
        .contains("install-policy=system"));
}

#[tokio::test]
async fn runtime_policy_does_not_reject_kernel_default_timeout() {
    let env = TestEnv::new();
    let rule = RuntimeExecutionRule {
        working_dir_roots: vec![env.path().to_path_buf()],
        allowed_env_passthrough_keys: BTreeSet::new(),
        min_timeout: Duration::from_millis(10),
        max_timeout: Duration::from_millis(100),
    };

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: Duration::from_millis(600),
            runtime_turn_hard_timeout: Duration::from_millis(1_200),
            runtime_execution_policy: RuntimeExecutionPolicy::default().with_rule("mock", rule),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "runtime-policy-default-timeout".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "run with kernel timeout defaults".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("kernel defaults should not be denied by per-request timeout policy");

    let policy_events = kernel
        .query_audit(
            Some(session.session_id),
            Some("runtime.plan.allow".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query plan audit");
    assert_eq!(
        policy_events.events[0].details["effective_timeout_ms"].as_u64(),
        Some(600)
    );
}

struct TestEnv {
    temp_dir: TempDir,
}

impl TestEnv {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create temp dir"),
        }
    }

    fn path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }

    fn db_path(&self) -> PathBuf {
        self.path().join("lionclaw.db")
    }
}
