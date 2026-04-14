use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use lionclaw::{
    contracts::{
        ChannelBindRequest, ChannelPeerApproveRequest, ContinuityPathRequest,
        ContinuitySearchRequest, JobCreateRequest, PolicyGrantRequest, SessionHistoryPolicy,
        SessionOpenRequest, SessionTurnRequest, SkillInstallRequest, TrustTier,
    },
    kernel::{
        policy::Capability,
        runtime::{
            HiddenTurnSupport, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityRequest,
            RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender, RuntimeMessageLane,
            RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnResult,
        },
        InboundChannelText, Kernel, KernelOptions,
    },
    workspace::bootstrap_workspace,
};
use serde_json::json;
use sqlx::{Row, SqlitePool};
use tempfile::TempDir;
use uuid::Uuid;

fn normalized_title_key(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn sanitize_slug(slug: &str) -> String {
    let mut normalized = slug
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while normalized.contains("--") {
        normalized = normalized.replace("--", "-");
    }
    normalized.trim_matches('-').to_string()
}

fn managed_title_file_name(title: &str) -> String {
    let normalized = normalized_title_key(title);
    let slug = sanitize_slug(&normalized);
    let slug = if slug.is_empty() {
        "item"
    } else {
        slug.as_str()
    };
    let key = Uuid::new_v5(
        &Uuid::from_u128(0x5f026ae9551b4d3ea511f0f2d74cf241),
        normalized.as_bytes(),
    );
    format!("{slug}--{key}.md")
}

#[tokio::test]
async fn prompt_loads_assistant_continuity_and_fs_read_uses_project_root() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    tokio::fs::write(
        env.workspace_root().join("MEMORY.md"),
        "assistant durable memory\n",
    )
    .await
    .expect("write assistant memory");
    tokio::fs::create_dir_all(env.workspace_root().join("continuity/open-loops"))
        .await
        .expect("create assistant open-loops dir");
    let assistant_open_loop_key = managed_title_file_name("Assistant Open Loop");
    tokio::fs::write(
        env.workspace_root()
            .join("continuity/open-loops")
            .join(&assistant_open_loop_key),
        format!(
            "# Assistant Open Loop\n\n- Status: open\n- Key: {assistant_open_loop_key}\n- Summary: remember the assistant follow-up\n- Next Step: keep it visible\n"
        ),
    )
    .await
    .expect("write assistant open loop");

    tokio::fs::create_dir_all(env.project_root().join("continuity"))
        .await
        .expect("create project continuity dir");
    tokio::fs::write(
        env.project_root().join("README.md"),
        "project readme content\n",
    )
    .await
    .expect("write project readme");
    tokio::fs::write(
        env.project_root().join("MEMORY.md"),
        "project-only-memory\n",
    )
    .await
    .expect("write project memory");
    tokio::fs::write(
        env.project_root().join("continuity").join("ACTIVE.md"),
        "project-only-active\n",
    )
    .await
    .expect("write project active");

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let prompts = Arc::new(Mutex::new(Vec::new()));
    let capability_results = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture-fs",
            Arc::new(CapturePromptAdapter {
                prompts: prompts.clone(),
                capability_results: capability_results.clone(),
                request_fs_read: true,
                allow_hidden_compaction: false,
                emit_compaction_continuity: false,
                reply: "read complete".to_string(),
            }),
        )
        .await;

    let skill_id = install_enabled_skill(&kernel, "root-split-reader").await;
    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: skill_id.clone(),
            capability: "fs.read".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant fs.read");

    let session = open_local_session(&kernel, "continuity-root-peer").await;
    kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "use root-split-reader to inspect README.md".to_string(),
            runtime_id: Some("capture-fs".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn succeeds");

    let prompt = prompts.lock().expect("prompt lock")[0].clone();
    assert!(prompt.contains("assistant durable memory"));
    assert!(prompt.contains("Assistant Open Loop"));
    assert!(!prompt.contains("project-only-memory"));
    assert!(!prompt.contains("project-only-active"));

    let results = capability_results.lock().expect("result lock");
    let content = results[0][0].output["content"]
        .as_str()
        .expect("fs.read content");
    assert_eq!(content, "project readme content\n");
}

#[tokio::test]
async fn prompt_loading_rejects_symlinked_identity_files() {
    use std::os::unix::fs::symlink;

    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");

    let outside = env.temp_dir.path().join("outside-soul.md");
    std::fs::write(&outside, "# Outside Soul\n").expect("write outside file");
    std::fs::remove_file(env.workspace_root().join("SOUL.md")).expect("remove soul file");
    symlink(&outside, env.workspace_root().join("SOUL.md")).expect("symlink soul file");

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture-boundary",
            Arc::new(CapturePromptAdapter {
                prompts,
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: false,
                emit_compaction_continuity: false,
                reply: "ok".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "prompt-boundary-peer").await;
    let err = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "hello".to_string(),
            runtime_id: Some("capture-boundary".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should fail");

    let message = err.to_string();
    assert!(message.contains("failed to open") || message.contains("not a regular file"));
}

#[tokio::test]
async fn pairing_and_failed_turn_update_active_and_daily_continuity() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter("boom", Arc::new(FailingRuntimeAdapter))
        .await;
    install_and_bind_channel(&kernel, "terminal").await;

    kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            text: "hello".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: None,
            external_message_id: None,
        })
        .await
        .expect("process pending peer");

    let session = open_local_session(&kernel, "continuity-failure-peer").await;
    let _ = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "this should fail".to_string(),
            runtime_id: Some("boom".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect_err("turn should fail");

    let active = tokio::fs::read_to_string(env.workspace_root().join("continuity/ACTIVE.md"))
        .await
        .expect("read active");
    let daily = read_all_markdown(env.workspace_root().join("continuity/daily"));

    assert!(active.contains("terminal/alice"));
    assert!(active.contains("failed"));
    assert!(daily.contains("Pairing required for terminal/alice"));
    assert!(daily.contains("Session turn failed"));
}

#[tokio::test]
async fn scheduler_success_records_artifact_and_updates_active_in_home_workspace() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    tokio::fs::create_dir_all(env.project_root())
        .await
        .expect("create project root");

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let skill_id = install_enabled_skill(&kernel, "scheduler-brief").await;
    let created = kernel
        .create_job(JobCreateRequest {
            name: "daily brief".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "review the current workspace".to_string(),
            skill_ids: vec![skill_id],
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create job");

    let tick = kernel.scheduler_tick().await.expect("scheduler tick");
    assert_eq!(tick.claimed_runs, 1);

    let active = tokio::fs::read_to_string(env.workspace_root().join("continuity/ACTIVE.md"))
        .await
        .expect("read active");
    let artifacts = read_all_markdown(env.workspace_root().join("continuity/artifacts"));

    assert!(active.contains("Scheduled Output: daily brief"));
    assert!(artifacts.contains("Scheduled Output: daily brief"));
    assert!(artifacts.contains(&created.job.job_id.to_string()));
    assert!(!env.project_root().join("continuity").exists());
}

#[tokio::test]
async fn continuity_refresh_indexes_exist() {
    let env = TestEnv::new();
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");
    let _ = kernel;

    let pool = SqlitePool::connect(&env.db_url())
        .await
        .expect("open sqlite pool");

    let session_turn_indexes = sqlx::query("PRAGMA index_list('session_turns')")
        .fetch_all(&pool)
        .await
        .expect("session_turns index list")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(session_turn_indexes
        .iter()
        .any(|name| name == "idx_session_turns_recent_failures"));

    let channel_peer_indexes = sqlx::query("PRAGMA index_list('channel_peers')")
        .fetch_all(&pool)
        .await
        .expect("channel_peers index list")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(channel_peer_indexes
        .iter()
        .any(|name| name == "idx_channel_peers_pending_recent"));

    let scheduler_job_indexes = sqlx::query("PRAGMA index_list('scheduler_jobs')")
        .fetch_all(&pool)
        .await
        .expect("scheduler_jobs index list")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(scheduler_job_indexes
        .iter()
        .any(|name| name == "idx_scheduler_jobs_attention_recent"));
}

#[tokio::test]
async fn active_continuity_global_slices_are_bounded_to_recent_items() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter("boom", Arc::new(FailingRuntimeAdapter))
        .await;
    install_and_bind_channel(&kernel, "terminal").await;

    let session = open_local_session(&kernel, "bounded-active-peer").await;
    for index in 0..7 {
        let _ = kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("fail turn {}", index),
                runtime_id: Some("boom".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect_err("seed failed turn");
    }

    let pool = SqlitePool::connect(&env.db_url())
        .await
        .expect("open sqlite pool");
    for sequence_no in 1_i64..=7 {
        sqlx::query(
            "UPDATE session_turns \
             SET started_at_ms = ?3 \
             WHERE session_id = ?1 AND sequence_no = ?2",
        )
        .bind(session.session_id.to_string())
        .bind(sequence_no)
        .bind(sequence_no * 1000)
        .execute(&pool)
        .await
        .expect("seed failure ordering");
    }

    for index in 0..7 {
        let created = kernel
            .create_job(JobCreateRequest {
                name: format!("attention-{index:02}"),
                runtime_id: "mock".to_string(),
                schedule: lionclaw::contracts::JobScheduleDto::Once {
                    run_at: Utc::now() + ChronoDuration::minutes(5 + i64::from(index)),
                },
                prompt_text: "attention job".to_string(),
                skill_ids: Vec::new(),
                allow_capabilities: Vec::new(),
                delivery: None,
                retry_attempts: Some(0),
            })
            .await
            .expect("create attention job");
        sqlx::query(
            "UPDATE scheduler_jobs \
             SET last_status = 'failed', last_error = ?2, updated_at_ms = ?3 \
             WHERE job_id = ?1",
        )
        .bind(created.job.job_id.to_string())
        .bind(format!("attention-{index:02} failed"))
        .bind(i64::from(index + 1) * 1000)
        .execute(&pool)
        .await
        .expect("seed attention job state");
    }

    for index in 0..7 {
        sqlx::query(
            "INSERT INTO channel_peers \
             (channel_id, peer_id, status, trust_tier, pairing_code, first_seen_ms, updated_at_ms) \
             VALUES (?1, ?2, 'pending', 'untrusted', ?3, ?4, ?4)",
        )
        .bind("terminal")
        .bind(format!("peer-{index:02}"))
        .bind(format!("code-{index:02}"))
        .bind(i64::from(index + 1) * 1000)
        .execute(&pool)
        .await
        .expect("seed pending peer ordering");
    }

    kernel
        .create_job(JobCreateRequest {
            name: "refresh-trigger".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() + ChronoDuration::minutes(30),
            },
            prompt_text: "refresh active continuity".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create refresh trigger job");

    let active = tokio::fs::read_to_string(env.workspace_root().join("continuity/ACTIVE.md"))
        .await
        .expect("read active");

    assert!(!active.contains("terminal/peer-00"));
    assert!(!active.contains("terminal/peer-01"));
    assert!(active.contains("terminal/peer-02"));
    assert!(active.contains("terminal/peer-06"));

    assert!(!active.contains("Job 'attention-00' needs attention"));
    assert!(!active.contains("Job 'attention-01' needs attention"));
    assert!(active.contains("Job 'attention-02' needs attention"));
    assert!(active.contains("Job 'attention-06' needs attention"));

    assert!(!active.contains(&format!("Session {} turn 1 failed", session.session_id)));
    assert!(!active.contains(&format!("Session {} turn 2 failed", session.session_id)));
    assert!(active.contains(&format!("Session {} turn 3 failed", session.session_id)));
    assert!(active.contains(&format!("Session {} turn 7 failed", session.session_id)));
}

#[tokio::test]
async fn create_job_rolls_back_when_audit_append_fails() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let pool = SqlitePool::connect(&env.db_url())
        .await
        .expect("open sqlite pool");
    sqlx::query("DROP TABLE audit_events")
        .execute(&pool)
        .await
        .expect("drop audit_events");

    let err = kernel
        .create_job(JobCreateRequest {
            name: "audit rollback create".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() + ChronoDuration::minutes(5),
            },
            prompt_text: "job should not persist".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect_err("create_job should fail when audit append fails");
    assert!(err.to_string().contains("failed to append audit event"));

    let jobs = kernel.list_jobs().await.expect("list jobs after rollback");
    assert!(jobs.jobs.is_empty(), "job row should roll back with audit");
}

#[tokio::test]
async fn approve_channel_peer_rolls_back_when_audit_append_fails() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");
    install_and_bind_channel(&kernel, "terminal").await;

    let pairing_code = seed_pending_peer(&kernel, "terminal", "alice").await;

    let pool = SqlitePool::connect(&env.db_url())
        .await
        .expect("open sqlite pool");
    sqlx::query("DROP TABLE audit_events")
        .execute(&pool)
        .await
        .expect("drop audit_events");

    let err = kernel
        .approve_channel_peer(ChannelPeerApproveRequest {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            pairing_code,
            trust_tier: Some(TrustTier::Main),
        })
        .await
        .expect_err("approve should fail when audit append fails");
    assert!(err.to_string().contains("failed to append audit event"));

    let peers = kernel
        .list_channel_peers(Some("terminal".to_string()))
        .await
        .expect("list peers after rollback");
    let alice = peers
        .peers
        .iter()
        .find(|peer| peer.peer_id == "alice")
        .expect("alice peer");
    assert_eq!(alice.status, "pending");
}

#[tokio::test]
async fn pause_job_rolls_back_when_audit_append_fails() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let created = kernel
        .create_job(JobCreateRequest {
            name: "audit rollback pause".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() + ChronoDuration::minutes(5),
            },
            prompt_text: "job should stay enabled".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create job");

    let pool = SqlitePool::connect(&env.db_url())
        .await
        .expect("open sqlite pool");
    sqlx::query("DROP TABLE audit_events")
        .execute(&pool)
        .await
        .expect("drop audit_events");

    let err = kernel
        .pause_job(created.job.job_id)
        .await
        .expect_err("pause should fail when audit append fails");
    assert!(err.to_string().contains("failed to append audit event"));

    let job = kernel
        .get_job(created.job.job_id)
        .await
        .expect("get job after rollback");
    assert!(job.job.enabled, "job should remain enabled");
}

#[cfg(unix)]
#[tokio::test]
async fn create_job_succeeds_when_active_continuity_refresh_fails() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    break_active_continuity_refresh(&env);

    let created = kernel
        .create_job(JobCreateRequest {
            name: "refresh best effort create".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() + ChronoDuration::minutes(5),
            },
            prompt_text: "refresh failure should not fail create".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create should still succeed");

    let job = kernel
        .get_job(created.job.job_id)
        .await
        .expect("get created job");
    assert_eq!(job.job.job_id, created.job.job_id);

    assert_refresh_failure_event(&kernel, "job.create", "api").await;
}

#[cfg(unix)]
#[tokio::test]
async fn approve_channel_peer_succeeds_when_active_continuity_refresh_fails() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");
    install_and_bind_channel(&kernel, "terminal").await;

    let pairing_code = seed_pending_peer(&kernel, "terminal", "alice").await;
    break_active_continuity_refresh(&env);

    let response = kernel
        .approve_channel_peer(ChannelPeerApproveRequest {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            pairing_code,
            trust_tier: Some(TrustTier::Main),
        })
        .await
        .expect("approve should still succeed");
    assert_eq!(response.peer.status, "approved");

    assert_refresh_failure_event(&kernel, "channel.peer.approved", "api").await;
}

#[cfg(unix)]
#[tokio::test]
async fn pairing_pending_continuity_succeeds_when_active_refresh_fails() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");
    install_and_bind_channel(&kernel, "terminal").await;

    break_active_continuity_refresh(&env);

    kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            text: "hello".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(1),
            external_message_id: Some("pairing-refresh-failure".to_string()),
        })
        .await
        .expect("pending peer should still be recorded");

    let peers = kernel
        .list_channel_peers(Some("terminal".to_string()))
        .await
        .expect("list channel peers");
    let alice = peers
        .peers
        .iter()
        .find(|peer| peer.peer_id == "alice")
        .expect("alice peer");
    assert_eq!(alice.status, "pending");

    let daily = read_all_markdown(env.workspace_root().join("continuity/daily"));
    assert!(daily.contains("Pairing required for terminal/alice"));

    assert_refresh_failure_event(&kernel, "channel.peer.pairing_pending", "kernel").await;
}

#[tokio::test]
async fn older_turns_are_compacted_into_prompt_context() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: prompts.clone(),
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: true,
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "compaction-peer").await;
    for index in 0..30 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("continuity-marker turn {}", index),
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("turn succeeds");
    }

    let last_prompt = prompts
        .lock()
        .expect("prompt lock")
        .iter()
        .rev()
        .find(|prompt| !prompt.contains("lionclaw_compaction_handoff_v1"))
        .cloned()
        .expect("last user-facing prompt");
    assert!(last_prompt.contains("Compacted Prior Turns"));
    assert!(last_prompt.contains("Compacted Prior Turns 1-17"));
    assert!(last_prompt.contains("### Goal"));
    assert!(last_prompt.contains("### Key Decisions"));
    assert!(last_prompt.contains("Continuity stays file-backed under assistant home."));
    assert!(last_prompt.contains("### Next Steps"));
    assert!(last_prompt.contains("turn 16"));
    assert!(!last_prompt.contains("## Prior Turn 1\n\n### User\n\nturn 0"));
    assert!(last_prompt.contains("turn 29"));

    let status = kernel.continuity_status().await.expect("continuity status");
    assert!(!status.memory_proposals.is_empty());
    assert!(!status.open_loops.is_empty());
}

#[tokio::test]
async fn continuity_surface_lists_searches_and_manages_proposals_and_loops() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: prompts.clone(),
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: true,
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "continuity-surface-peer").await;
    for index in 0..18 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!(
                    "continuity-marker review turn {} in src/kernel/continuity.rs",
                    index
                ),
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("turn succeeds");
    }

    let proposals = kernel
        .list_continuity_memory_proposals()
        .await
        .expect("list proposals");
    assert_eq!(proposals.proposals.len(), 1);

    let loops = kernel
        .list_continuity_open_loops()
        .await
        .expect("list loops");
    assert_eq!(loops.loops.len(), 1);

    let search = kernel
        .continuity_search(ContinuitySearchRequest {
            query: "continuity module".to_string(),
            limit: Some(10),
        })
        .await
        .expect("search continuity");
    assert!(!search.matches.is_empty());

    let proposal_path = proposals.proposals[0].relative_path.clone();
    let proposal = kernel
        .continuity_get(ContinuityPathRequest {
            relative_path: proposal_path.clone(),
        })
        .await
        .expect("get proposal");
    assert!(proposal.content.contains("Memory Proposal"));

    let merge = kernel
        .merge_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: proposal_path,
        })
        .await
        .expect("merge proposal");
    assert!(merge.archived_path.contains("archive/merged"));

    let loop_path = loops.loops[0].relative_path.clone();
    let resolved = kernel
        .resolve_continuity_open_loop(ContinuityPathRequest {
            relative_path: loop_path,
        })
        .await
        .expect("resolve loop");
    assert!(resolved.archived_path.contains("open-loops/archive"));

    let memory_search = kernel
        .continuity_search(ContinuitySearchRequest {
            query: "stay small explicit".to_string(),
            limit: Some(10),
        })
        .await
        .expect("search merged memory");
    assert!(memory_search
        .matches
        .iter()
        .any(|item| item.relative_path == "MEMORY.md"));

    let archived_loop_search = kernel
        .continuity_search(ContinuitySearchRequest {
            query: "review turn".to_string(),
            limit: Some(10),
        })
        .await
        .expect("search archived loop");
    assert!(archived_loop_search
        .matches
        .iter()
        .any(|item| item.relative_path == resolved.archived_path));

    let memory = tokio::fs::read_to_string(env.workspace_root().join("MEMORY.md"))
        .await
        .expect("read memory");
    assert!(memory.contains("Prefers continuity module work to stay small and explicit."));
}

#[tokio::test]
async fn continuity_search_handles_unicode_cross_line_fallback_without_panicking() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    tokio::fs::write(
        env.workspace_root().join("MEMORY.md"),
        "# Memory\n\n## Entries\n- Ä\nÖ continuity marker.\n",
    )
    .await
    .expect("write unicode memory");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let search = kernel
        .continuity_search(ContinuitySearchRequest {
            query: "Ä\nÖ".to_string(),
            limit: Some(10),
        })
        .await
        .expect("unicode fallback search");

    assert!(search
        .matches
        .iter()
        .any(|item| item.relative_path == "MEMORY.md"));
    assert!(search
        .matches
        .iter()
        .any(|item| item.snippet.contains("Ä Ö continuity marker.")));
}

#[tokio::test]
async fn continuity_search_does_not_short_circuit_mixed_unicode_queries_to_lossy_index_hits() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    tokio::fs::write(
        env.workspace_root().join("MEMORY.md"),
        "# Memory\n\n## Entries\n- Plain cafe review only.\n",
    )
    .await
    .expect("write ascii memory");
    tokio::fs::write(
        env.workspace_root()
            .join("continuity/open-loops/review-audit.md"),
        "# Review Audit\n\n- Summary: Need café review before release.\n",
    )
    .await
    .expect("write unicode continuity file");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let search = kernel
        .continuity_search(ContinuitySearchRequest {
            query: "café review".to_string(),
            limit: Some(10),
        })
        .await
        .expect("mixed unicode search");

    assert!(search
        .matches
        .iter()
        .any(|item| item.relative_path == "continuity/open-loops/review-audit.md"));
    assert!(!search
        .matches
        .iter()
        .any(|item| item.relative_path == "MEMORY.md"));
}

#[tokio::test]
async fn continuity_search_tolerates_missing_memory_file() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    tokio::fs::write(
        env.workspace_root()
            .join("continuity/open-loops")
            .join("review-audit.md"),
        "# Review Audit\n\n- Summary: Need release review before shipping.\n",
    )
    .await
    .expect("write continuity file");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    tokio::fs::remove_file(env.workspace_root().join("MEMORY.md"))
        .await
        .expect("remove memory");

    let search = kernel
        .continuity_search(ContinuitySearchRequest {
            query: "release review".to_string(),
            limit: Some(10),
        })
        .await
        .expect("search without memory");

    assert!(search
        .matches
        .iter()
        .any(|item| item.relative_path == "continuity/open-loops/review-audit.md"));
}

#[tokio::test]
async fn unsafe_runtimes_do_not_receive_hidden_compaction_prompts() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "unsafe-capture",
            Arc::new(CapturePromptAdapter {
                prompts: prompts.clone(),
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: false,
                emit_compaction_continuity: false,
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "unsafe-compaction-peer").await;
    for index in 0..18 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("turn {}", index),
                runtime_id: Some("unsafe-capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("turn succeeds");
    }

    let prompts = prompts.lock().expect("prompt lock");
    assert!(!prompts
        .iter()
        .any(|prompt| prompt.contains("lionclaw_compaction_handoff_v1")));
    let last_prompt = prompts.last().expect("last prompt");
    assert!(last_prompt.contains("Compacted Prior Turns"));
}

#[tokio::test]
async fn compaction_failures_are_audited_without_failing_completed_turns() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts,
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: true,
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "compaction-failure-peer").await;
    for index in 0..12 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: if index == 0 {
                    "continuity-marker turn 0".to_string()
                } else {
                    format!("turn {}", index)
                },
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("seed turn succeeds");
    }

    let proposals_dir = env.workspace_root().join("continuity/proposals/memory");
    let mut permissions = std::fs::metadata(&proposals_dir)
        .expect("proposals metadata")
        .permissions();
    #[cfg(unix)]
    let original_mode = {
        use std::os::unix::fs::PermissionsExt;
        permissions.mode()
    };
    permissions.set_readonly(true);
    std::fs::set_permissions(&proposals_dir, permissions).expect("freeze proposals dir");

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id: session.session_id,
            user_text: "trigger compaction despite continuity write failure".to_string(),
            runtime_id: Some("capture".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should still succeed");
    assert_eq!(response.assistant_text, "captured");

    let mut restore = std::fs::metadata(&proposals_dir)
        .expect("proposals metadata")
        .permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        restore.set_mode(original_mode);
    }
    #[cfg(not(unix))]
    restore.set_readonly(false);
    std::fs::set_permissions(&proposals_dir, restore).expect("restore proposals dir");

    let audit = kernel
        .query_audit(
            Some(session.session_id),
            Some("session.compaction.failed".to_string()),
            None,
            Some(10),
        )
        .await
        .expect("query compaction failure audit");
    assert_eq!(audit.events.len(), 1);
    assert!(audit.events[0].details["error"]
        .as_str()
        .expect("error text")
        .contains("failed"));
}

#[tokio::test]
async fn continuity_mutations_are_audited() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let merged_key = managed_title_file_name("Merge Me");
    let rejected_key = managed_title_file_name("Reject Me");
    let loop_key = managed_title_file_name("Review Audit Trail");
    let merged_path = env
        .workspace_root()
        .join("continuity/proposals/memory")
        .join(&merged_key);
    let rejected_path = env
        .workspace_root()
        .join("continuity/proposals/memory")
        .join(&rejected_key);
    let loop_path = env
        .workspace_root()
        .join("continuity/open-loops")
        .join(&loop_key);
    std::fs::write(
        &merged_path,
        format!(
            "# Memory Proposal: Merge Me\n\n- Status: proposed\n- Key: {merged_key}\n- Proposed: {} UTC\n- Rationale: keep durable preference\n\n## Candidate Entries\n- Remember merged proposal\n",
            Utc::now().to_rfc3339(),
        ),
    )
    .expect("write merged proposal");
    std::fs::write(
        &rejected_path,
        format!(
            "# Memory Proposal: Reject Me\n\n- Status: proposed\n- Key: {rejected_key}\n- Proposed: {} UTC\n- Rationale: reject duplicate\n\n## Candidate Entries\n- Remember rejected proposal\n",
            Utc::now().to_rfc3339(),
        ),
    )
    .expect("write rejected proposal");
    std::fs::write(
        &loop_path,
        format!(
            "# Review Audit Trail\n\n- Status: open\n- Key: {loop_key}\n- Updated: {} UTC\n- Summary: verify continuity audit coverage\n- Next Step: query the audit log\n",
            Utc::now().to_rfc3339(),
        ),
    )
    .expect("write open loop");

    kernel
        .merge_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: format!("continuity/proposals/memory/{merged_key}"),
        })
        .await
        .expect("merge proposal");
    kernel
        .reject_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: format!("continuity/proposals/memory/{rejected_key}"),
        })
        .await
        .expect("reject proposal");
    kernel
        .resolve_continuity_open_loop(ContinuityPathRequest {
            relative_path: format!("continuity/open-loops/{loop_key}"),
        })
        .await
        .expect("resolve open loop");

    let merged = kernel
        .query_audit(
            None,
            Some("continuity.memory_proposal.merged".to_string()),
            None,
            Some(5),
        )
        .await
        .expect("query merge audit");
    assert_eq!(merged.events.len(), 1);
    assert_eq!(merged.events[0].actor.as_deref(), Some("api"));

    let rejected = kernel
        .query_audit(
            None,
            Some("continuity.memory_proposal.rejected".to_string()),
            None,
            Some(5),
        )
        .await
        .expect("query reject audit");
    assert_eq!(rejected.events.len(), 1);
    assert_eq!(rejected.events[0].actor.as_deref(), Some("api"));

    let resolved = kernel
        .query_audit(
            None,
            Some("continuity.open_loop.resolved".to_string()),
            None,
            Some(5),
        )
        .await
        .expect("query resolve audit");
    assert_eq!(resolved.events.len(), 1);
    assert_eq!(resolved.events[0].actor.as_deref(), Some("api"));
}

#[tokio::test]
async fn merged_and_resolved_items_do_not_reappear_without_new_reintroduction() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let prompts = Arc::new(Mutex::new(Vec::new()));
    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts,
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: true,
                reply: "captured".to_string(),
            }),
        )
        .await;
    kernel
        .register_runtime_adapter(
            "capture-empty",
            Arc::new(CapturePromptAdapter {
                prompts: Arc::new(Mutex::new(Vec::new())),
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: false,
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "continuity-resurrection-peer").await;
    for index in 0..13 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: if index == 0 {
                    "continuity-marker seed turn 0".to_string()
                } else {
                    format!("seed turn {}", index)
                },
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("seed turn succeeds");
    }

    let proposals = kernel
        .list_continuity_memory_proposals()
        .await
        .expect("list proposals");
    assert_eq!(proposals.proposals.len(), 1);
    let loops = kernel
        .list_continuity_open_loops()
        .await
        .expect("list loops");
    assert_eq!(loops.loops.len(), 1);

    let proposal_path = env
        .workspace_root()
        .join(&proposals.proposals[0].relative_path);
    let proposal_key = proposal_path
        .file_name()
        .expect("proposal file name")
        .to_string_lossy()
        .to_string();
    tokio::fs::write(
        &proposal_path,
        format!(
            "# Memory Proposal: Renamed In Content\n\n- Status: proposed\n- Key: {proposal_key}\n- Proposed: {} UTC\n- Rationale: edited before merge\n\n## Candidate Entries\n- Prefers continuity module work to stay small and explicit.\n",
            Utc::now().to_rfc3339(),
        ),
    )
    .await
    .expect("edit proposal content");
    let loop_path = env.workspace_root().join(&loops.loops[0].relative_path);
    let loop_key = loop_path
        .file_name()
        .expect("loop file name")
        .to_string_lossy()
        .to_string();
    tokio::fs::write(
        &loop_path,
        format!(
            "# Renamed In Content\n\n- Status: open\n- Key: {loop_key}\n- Updated: {} UTC\n- Summary: edited before resolve\n- Next Step: Run continuity search\n",
            Utc::now().to_rfc3339(),
        ),
    )
    .await
    .expect("edit loop content");

    kernel
        .merge_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: proposals.proposals[0].relative_path.clone(),
        })
        .await
        .expect("merge proposal");
    kernel
        .resolve_continuity_open_loop(ContinuityPathRequest {
            relative_path: loops.loops[0].relative_path.clone(),
        })
        .await
        .expect("resolve loop");

    for index in 13..26 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("follow-up turn {}", index),
                runtime_id: Some("capture-empty".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("follow-up turn succeeds");
    }

    let proposals = kernel
        .list_continuity_memory_proposals()
        .await
        .expect("list proposals after follow-up");
    assert!(proposals.proposals.is_empty());

    let loops = kernel
        .list_continuity_open_loops()
        .await
        .expect("list loops after follow-up");
    assert!(loops.loops.is_empty());

    let active = tokio::fs::read_to_string(env.workspace_root().join("continuity/ACTIVE.md"))
        .await
        .expect("read active");
    assert!(!active.contains("Continuity Product Preferences"));
    assert!(!active.contains("Follow up on continuity surface"));
}

#[tokio::test]
async fn merged_and_resolved_items_do_not_reappear_from_other_sessions() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: Arc::new(Mutex::new(Vec::new())),
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: true,
                reply: "captured".to_string(),
            }),
        )
        .await;
    kernel
        .register_runtime_adapter(
            "capture-empty",
            Arc::new(CapturePromptAdapter {
                prompts: Arc::new(Mutex::new(Vec::new())),
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: false,
                reply: "captured".to_string(),
            }),
        )
        .await;

    let first = open_local_session(&kernel, "continuity-global-first").await;
    let second = open_local_session(&kernel, "continuity-global-second").await;
    for index in 0..13 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: first.session_id,
                user_text: if index == 0 {
                    "continuity-marker first session turn 0".to_string()
                } else {
                    format!("first session turn {}", index)
                },
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("first session seed turn succeeds");
        kernel
            .turn_session(SessionTurnRequest {
                session_id: second.session_id,
                user_text: if index == 0 {
                    "continuity-marker second session turn 0".to_string()
                } else {
                    format!("second session turn {}", index)
                },
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("second session seed turn succeeds");
    }

    let proposals = kernel
        .list_continuity_memory_proposals()
        .await
        .expect("list proposals");
    let loops = kernel
        .list_continuity_open_loops()
        .await
        .expect("list loops");
    assert_eq!(proposals.proposals.len(), 1);
    assert_eq!(loops.loops.len(), 1);

    kernel
        .merge_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: proposals.proposals[0].relative_path.clone(),
        })
        .await
        .expect("merge proposal");
    kernel
        .resolve_continuity_open_loop(ContinuityPathRequest {
            relative_path: loops.loops[0].relative_path.clone(),
        })
        .await
        .expect("resolve loop");

    for index in 13..26 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: second.session_id,
                user_text: format!("second session follow-up turn {}", index),
                runtime_id: Some("capture-empty".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("second session follow-up turn succeeds");
    }

    assert!(kernel
        .list_continuity_memory_proposals()
        .await
        .expect("list proposals after follow-up")
        .proposals
        .is_empty());
    assert!(kernel
        .list_continuity_open_loops()
        .await
        .expect("list loops after follow-up")
        .loops
        .is_empty());
}

#[tokio::test]
async fn merged_and_resolved_items_can_reappear_when_later_compaction_reintroduces_them() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    kernel
        .register_runtime_adapter(
            "capture",
            Arc::new(CapturePromptAdapter {
                prompts: Arc::new(Mutex::new(Vec::new())),
                capability_results: Arc::new(Mutex::new(Vec::new())),
                request_fs_read: false,
                allow_hidden_compaction: true,
                emit_compaction_continuity: true,
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "continuity-reintroduce-peer").await;
    for index in 0..13 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: if index == 0 {
                    "continuity-marker seed turn 0".to_string()
                } else {
                    format!("seed turn {}", index)
                },
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("seed turn succeeds");
    }

    let proposal_path = kernel
        .list_continuity_memory_proposals()
        .await
        .expect("list proposals")
        .proposals[0]
        .relative_path
        .clone();
    let loop_path = kernel
        .list_continuity_open_loops()
        .await
        .expect("list loops")
        .loops[0]
        .relative_path
        .clone();

    kernel
        .merge_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: proposal_path,
        })
        .await
        .expect("merge proposal");
    kernel
        .resolve_continuity_open_loop(ContinuityPathRequest {
            relative_path: loop_path,
        })
        .await
        .expect("resolve loop");

    for index in 13..30 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("continuity-marker reintroduce turn {}", index),
                runtime_id: Some("capture".to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
            .expect("reintroduce turn succeeds");
    }

    assert_eq!(
        kernel
            .list_continuity_memory_proposals()
            .await
            .expect("list proposals after reintroduction")
            .proposals
            .len(),
        1
    );
    assert_eq!(
        kernel
            .list_continuity_open_loops()
            .await
            .expect("list loops after reintroduction")
            .loops
            .len(),
        1
    );
}

#[cfg(unix)]
#[tokio::test]
async fn continuity_get_rejects_symlink_escape_outside_canonical_roots() {
    use std::os::unix::fs::symlink;

    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let outside = env.temp_dir.path().join("outside-secret.txt");
    std::fs::write(&outside, "secret").expect("write outside file");

    std::fs::create_dir_all(env.workspace_root().join("continuity/artifacts"))
        .expect("create artifacts dir");
    let escaped = env
        .workspace_root()
        .join("continuity/artifacts/escape-secret.md");
    symlink(&outside, &escaped).expect("create symlink");

    let err = kernel
        .continuity_get(ContinuityPathRequest {
            relative_path: "continuity/artifacts/escape-secret.md".to_string(),
        })
        .await
        .expect_err("symlink escape should fail");
    let message = err.to_string();
    assert!(
        message.contains("failed to open") || message.contains("is a symlink"),
        "unexpected error: {message}"
    );

    let memory_path = env.workspace_root().join("MEMORY.md");
    std::fs::remove_file(&memory_path).expect("remove memory file");
    symlink(&outside, &memory_path).expect("link memory file");

    let err = kernel
        .continuity_get(ContinuityPathRequest {
            relative_path: "MEMORY.md".to_string(),
        })
        .await
        .expect_err("memory symlink escape should fail");
    let message = err.to_string();
    assert!(
        message.contains("failed to open") || message.contains("is a symlink"),
        "unexpected error: {message}"
    );

    let err = kernel
        .continuity_search(ContinuitySearchRequest {
            query: "secret".to_string(),
            limit: Some(10),
        })
        .await
        .expect_err("memory symlink should also break continuity search");
    let message = err.to_string();
    assert!(
        message.contains("failed to open") || message.contains("is a symlink"),
        "unexpected error: {message}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn continuity_lists_reject_symlinked_continuity_roots() {
    use std::os::unix::fs::symlink;

    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(env.project_root()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");

    let outside = env.temp_dir.path().join("outside-open-loops");
    std::fs::create_dir_all(&outside).expect("create outside loop dir");
    std::fs::write(
        outside.join("escape.md"),
        "# Escape Loop\n\n- Status: open\n- Summary: outside\n- Next Step: outside\n",
    )
    .expect("write outside loop");

    let open_loops = env.workspace_root().join("continuity/open-loops");
    std::fs::remove_dir_all(&open_loops).expect("remove open-loops dir");
    symlink(&outside, &open_loops).expect("link open-loops dir");

    let err = kernel
        .list_continuity_open_loops()
        .await
        .expect_err("symlinked open-loops dir should fail");
    let message = err.to_string();
    assert!(
        message.contains("failed to open"),
        "unexpected error: {message}"
    );
}

async fn install_enabled_skill(kernel: &Kernel, name: &str) -> String {
    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: format!("local/{}", name),
            reference: Some("main".to_string()),
            hash: Some(format!("{}-hash", name)),
            skill_md: Some(format!(
                "---\nname: {}\ndescription: Handles {}\n---",
                name, name
            )),
            snapshot_path: None,
        })
        .await
        .expect("install skill");
    kernel
        .enable_skill(skill.skill_id.clone())
        .await
        .expect("enable skill");
    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: skill.skill_id.clone(),
            capability: "skill.use".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant skill.use");
    skill.skill_id
}

async fn install_and_bind_channel(kernel: &Kernel, channel_id: &str) {
    let skill_id = install_enabled_skill(kernel, &format!("channel-{}", channel_id)).await;
    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: channel_id.to_string(),
            skill_id,
            enabled: None,
            config: None,
        })
        .await
        .expect("bind channel");
}

async fn open_local_session(
    kernel: &Kernel,
    peer_id: &str,
) -> lionclaw::contracts::SessionOpenResponse {
    kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open session")
}

fn read_all_markdown(root: PathBuf) -> String {
    if !root.exists() {
        return String::new();
    }

    let mut pending = vec![root];
    let mut files = Vec::new();
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "md") {
                files.push(path);
            }
        }
    }
    files.sort();

    files
        .into_iter()
        .map(|path| std::fs::read_to_string(path).expect("read file"))
        .collect::<Vec<_>>()
        .join("\n")
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

    fn db_path(&self) -> PathBuf {
        self.temp_dir.path().join("lionclaw.db")
    }

    fn db_url(&self) -> String {
        format!("sqlite://{}", self.db_path().display())
    }

    fn workspace_root(&self) -> PathBuf {
        self.temp_dir.path().join("home-workspace")
    }

    fn project_root(&self) -> PathBuf {
        self.temp_dir.path().join("project-root")
    }
}

async fn enqueue_pending_peer(kernel: &Kernel, channel_id: &str, peer_id: &str) {
    kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: channel_id.to_string(),
            peer_id: peer_id.to_string(),
            text: "seed approval".to_string(),
            session_id: None,
            runtime_id: Some("mock".to_string()),
            update_id: Some(1),
            external_message_id: Some(format!("seed-{channel_id}-{peer_id}")),
        })
        .await
        .expect("seed pending peer");
}

async fn seed_pending_peer(kernel: &Kernel, channel_id: &str, peer_id: &str) -> String {
    enqueue_pending_peer(kernel, channel_id, peer_id).await;
    let peers = kernel
        .list_channel_peers(Some(channel_id.to_string()))
        .await
        .expect("list channel peers");
    peers
        .peers
        .iter()
        .find(|peer| peer.peer_id == peer_id)
        .and_then(|peer| peer.pairing_code.clone())
        .expect("pairing code")
}

async fn assert_refresh_failure_event(kernel: &Kernel, source_action: &str, actor: &str) {
    let audit = kernel
        .query_audit(
            None,
            Some("continuity.refresh_failed".to_string()),
            None,
            Some(20),
        )
        .await
        .expect("query refresh failure audit");
    let event = audit
        .events
        .iter()
        .find(|event| {
            event.actor.as_deref() == Some(actor)
                && event.details["source_action"].as_str() == Some(source_action)
        })
        .unwrap_or_else(|| panic!("missing refresh failure audit for {source_action}"));
    assert!(event.details["error"].as_str().is_some());
}

#[cfg(unix)]
fn break_active_continuity_refresh(env: &TestEnv) {
    let active = env.workspace_root().join("continuity/ACTIVE.md");
    std::fs::remove_file(&active).expect("remove active");
    std::fs::create_dir(&active).expect("replace active file with directory");
}

struct CapturePromptAdapter {
    prompts: Arc<Mutex<Vec<String>>>,
    capability_results: Arc<Mutex<Vec<Vec<RuntimeCapabilityResult>>>>,
    request_fs_read: bool,
    allow_hidden_compaction: bool,
    emit_compaction_continuity: bool,
    reply: String,
}

#[async_trait]
impl RuntimeAdapter for CapturePromptAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "capture".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    fn hidden_turn_support(&self) -> HiddenTurnSupport {
        if self.allow_hidden_compaction {
            HiddenTurnSupport::SideEffectFree
        } else {
            HiddenTurnSupport::Unsupported
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("capture-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let current_prompt = input.prompt;
        self.prompts
            .lock()
            .expect("prompt lock")
            .push(current_prompt.clone());

        if self.allow_hidden_compaction
            && input.selected_skills.is_empty()
            && current_prompt.contains("lionclaw_compaction_handoff_v1")
        {
            let first_user = extract_compaction_user(&current_prompt, false);
            let last_user = extract_compaction_user(&current_prompt, true);
            let emit_continuity =
                self.emit_compaction_continuity && last_user.contains("continuity-marker");
            let memory_proposals = if emit_continuity {
                vec![json!({
                    "title": "Continuity Product Preferences",
                    "rationale": "durable product preference surfaced during compaction",
                    "entries": ["Prefers continuity module work to stay small and explicit."]
                })]
            } else {
                Vec::new()
            };
            let open_loops = if emit_continuity {
                vec![json!({
                    "title": "Follow up on continuity surface",
                    "summary": "Need to review continuity product commands and proposal flow.",
                    "next_step": last_user
                })]
            } else {
                Vec::new()
            };
            let _ = events.send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: json!({
                    "goal": first_user,
                    "constraints_preferences": ["Keep the core small and explicit."],
                    "progress_done": ["Compacted prior transcript state into a structured handoff."],
                    "progress_in_progress": [],
                    "progress_blocked": [],
                    "key_decisions": ["Continuity stays file-backed under assistant home."],
                    "relevant_files": ["src/kernel/continuity.rs"],
                    "next_steps": [last_user],
                    "critical_context": ["Use the handoff summary plus the recent raw tail to continue."],
                    "memory_proposals": memory_proposals,
                    "open_loops": open_loops
                })
                .to_string(),
            });
            let _ = events.send(RuntimeEvent::Done);
            return Ok(RuntimeTurnResult::default());
        }

        if self.request_fs_read {
            let skill_id = input
                .selected_skills
                .first()
                .cloned()
                .ok_or_else(|| anyhow!("selected skill required"))?;
            return Ok(RuntimeTurnResult {
                capability_requests: vec![RuntimeCapabilityRequest {
                    request_id: "req-1".to_string(),
                    skill_id,
                    capability: Capability::FsRead,
                    scope: None,
                    payload: json!({"path": "README.md"}),
                }],
            });
        }

        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text: self.reply.clone(),
        });
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult::default())
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        self.capability_results
            .lock()
            .expect("results lock")
            .push(results.clone());
        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text: self.reply.clone(),
        });
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

fn extract_compaction_user(prompt: &str, last: bool) -> String {
    let users = prompt
        .lines()
        .filter_map(|line| line.trim().strip_prefix("User: "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if users.is_empty() {
        return "continue the session".to_string();
    }
    if last {
        users.last().cloned().unwrap()
    } else {
        users.first().cloned().unwrap()
    }
}

struct FailingRuntimeAdapter;

#[async_trait]
impl RuntimeAdapter for FailingRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "boom".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("boom-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        _events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        Err(anyhow!("boom"))
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        _results: Vec<RuntimeCapabilityResult>,
        _events: RuntimeEventSender,
    ) -> Result<()> {
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}
