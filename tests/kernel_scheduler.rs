use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use lionclaw::{
    contracts::{
        ChannelBindRequest, ChannelStreamPullRequest, ChannelStreamStartMode, JobCreateRequest,
        JobRefRequest, JobRunsRequest, PolicyGrantRequest, SessionHistoryPolicy,
        SessionHistoryRequest, SessionLatestQuery, SessionOpenRequest, SessionTurnRequest,
        SkillInstallRequest, StreamEventKindDto, TrustTier,
    },
    home::LionClawHome,
    kernel::{
        runtime::{
            ConfinementConfig, OciConfinementConfig, RuntimeAdapter, RuntimeAdapterInfo,
            RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender, RuntimeExecutionProfile,
            RuntimeMessageLane, RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput,
            RuntimeTurnResult,
        },
        InboundChannelText, Kernel, KernelError, KernelOptions,
    },
    workspace::bootstrap_workspace,
};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::sync::Notify;
use uuid::Uuid;

#[tokio::test]
async fn scheduled_job_tick_runs_in_fresh_scheduler_session() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    let skill_id = install_enabled_skill(&kernel, "scheduler-brief").await;

    let created = kernel
        .create_job(JobCreateRequest {
            name: "daily brief".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "review the current workspace".to_string(),
            skill_ids: vec![skill_id.clone()],
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create job");

    let tick = kernel.scheduler_tick().await.expect("run scheduler tick");
    assert_eq!(tick.claimed_runs, 1);

    let job = kernel
        .get_job(created.job.job_id)
        .await
        .expect("load job")
        .job;
    assert!(!job.enabled);
    assert_eq!(
        job.last_status,
        Some(lionclaw::contracts::SchedulerJobRunStatusDto::Completed)
    );

    let runs = kernel
        .list_job_runs(JobRunsRequest {
            job_id: created.job.job_id,
            limit: Some(5),
        })
        .await
        .expect("list job runs")
        .runs;
    assert_eq!(runs.len(), 1);
    assert_eq!(
        runs[0].status,
        lionclaw::contracts::SchedulerJobRunStatusDto::Completed
    );

    let latest = kernel
        .latest_session_snapshot(SessionLatestQuery {
            channel_id: "scheduler".to_string(),
            peer_id: format!("job:{}", created.job.job_id),
            history_policy: Some(SessionHistoryPolicy::Conservative),
        })
        .await
        .expect("load latest scheduler session");
    let session = latest.session.expect("scheduler session should exist");
    assert_eq!(
        session.session_id,
        runs[0].session_id.expect("run session id")
    );
    assert_eq!(session.history_policy, SessionHistoryPolicy::Conservative);

    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id: session.session_id,
            limit: Some(5),
        })
        .await
        .expect("load session history");
    assert_eq!(history.turns.len(), 1);
    assert_eq!(
        history.turns[0].display_user_text,
        "review the current workspace"
    );
    assert!(
        history.turns[0].assistant_text.contains(&skill_id),
        "mock runtime should receive the explicit scheduled skill list"
    );
}

#[tokio::test]
async fn create_job_rejects_missing_runtime_auth_before_persisting() {
    let env = TestEnv::new();
    let home = LionClawHome::new(env.temp_dir.path().join(".lionclaw"));
    home.ensure_base_dirs().await.expect("base dirs");
    let kernel = kernel_with_codex_auth_requirement(&env, &home).await;

    let err = kernel
        .create_job(JobCreateRequest {
            name: "codex brief".to_string(),
            runtime_id: "codex".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once { run_at: Utc::now() },
            prompt_text: "summarize repo".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect_err("missing runtime auth should reject job creation");

    match err {
        KernelError::BadRequest(message) => {
            assert!(message.contains("codex login"));
            assert!(message.contains("auth.json"));
        }
        other => panic!("unexpected error variant: {}", other),
    }

    let jobs = kernel.list_jobs().await.expect("list jobs");
    assert!(jobs.jobs.is_empty(), "job should not be persisted");
}

#[tokio::test]
async fn manual_job_run_rejects_missing_runtime_auth_before_launch() {
    let env = TestEnv::new();
    let home = LionClawHome::new(env.temp_dir.path().join(".lionclaw"));
    home.ensure_base_dirs().await.expect("base dirs");
    write_test_codex_auth(&home).await;
    let turn_calls = Arc::new(AtomicUsize::new(0));
    let kernel = kernel_with_counting_codex_runtime(&env, &home, turn_calls.clone()).await;

    let created = kernel
        .create_job(JobCreateRequest {
            name: "codex manual".to_string(),
            runtime_id: "codex".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() + ChronoDuration::hours(1),
            },
            prompt_text: "manual run".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create job");

    tokio::fs::remove_file(home.root().join(".codex/auth.json"))
        .await
        .expect("remove runtime auth");

    let err = kernel
        .run_job_now(JobRefRequest {
            job_id: created.job.job_id,
        })
        .await
        .expect_err("missing runtime auth should block manual run");

    match err {
        KernelError::Runtime(message) => {
            assert!(message.contains("codex login"));
            assert!(message.contains("auth.json"));
        }
        other => panic!("unexpected error variant: {}", other),
    }
    assert_eq!(turn_calls.load(Ordering::SeqCst), 0);
    let runs = kernel
        .list_job_runs(JobRunsRequest {
            job_id: created.job.job_id,
            limit: Some(5),
        })
        .await
        .expect("list job runs");
    assert!(runs.runs.is_empty(), "manual run should not be claimed");
}

#[tokio::test]
async fn scheduler_tick_dead_letters_missing_runtime_auth_without_launching_runtime() {
    let env = TestEnv::new();
    let home = LionClawHome::new(env.temp_dir.path().join(".lionclaw"));
    home.ensure_base_dirs().await.expect("base dirs");
    write_test_codex_auth(&home).await;
    let turn_calls = Arc::new(AtomicUsize::new(0));
    let kernel = kernel_with_counting_codex_runtime(&env, &home, turn_calls.clone()).await;

    let created = kernel
        .create_job(JobCreateRequest {
            name: "codex scheduled".to_string(),
            runtime_id: "codex".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "scheduled run".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create job");

    tokio::fs::remove_file(home.root().join(".codex/auth.json"))
        .await
        .expect("remove runtime auth");

    let tick = kernel.scheduler_tick().await.expect("scheduler tick");
    assert_eq!(tick.claimed_runs, 1);
    assert_eq!(turn_calls.load(Ordering::SeqCst), 0);
    let job = kernel
        .get_job(created.job.job_id)
        .await
        .expect("load job")
        .job;
    assert_eq!(
        job.last_status,
        Some(lionclaw::contracts::SchedulerJobRunStatusDto::DeadLetter)
    );
    assert!(job
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("codex login")));
    assert!(
        !job.enabled,
        "one-shot auth-invalid jobs should dead-letter"
    );
    let runs = kernel
        .list_job_runs(JobRunsRequest {
            job_id: created.job.job_id,
            limit: Some(5),
        })
        .await
        .expect("list job runs");
    assert_eq!(runs.runs.len(), 1);
    assert_eq!(
        runs.runs[0].status,
        lionclaw::contracts::SchedulerJobRunStatusDto::DeadLetter
    );
    assert!(runs.runs[0].session_id.is_none());
    assert!(runs.runs[0].turn_id.is_none());
}

#[tokio::test]
async fn scheduler_tick_retries_missing_runtime_auth_before_dead_lettering() {
    let env = TestEnv::new();
    let home = LionClawHome::new(env.temp_dir.path().join(".lionclaw"));
    home.ensure_base_dirs().await.expect("base dirs");
    write_test_codex_auth(&home).await;
    let turn_calls = Arc::new(AtomicUsize::new(0));
    let kernel = kernel_with_counting_codex_runtime(&env, &home, turn_calls.clone()).await;

    let created = kernel
        .create_job(JobCreateRequest {
            name: "codex scheduled".to_string(),
            runtime_id: "codex".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "scheduled run".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(1),
        })
        .await
        .expect("create job");

    tokio::fs::remove_file(home.root().join(".codex/auth.json"))
        .await
        .expect("remove runtime auth");

    let tick = kernel.scheduler_tick().await.expect("scheduler tick");
    assert_eq!(tick.claimed_runs, 1);
    assert_eq!(turn_calls.load(Ordering::SeqCst), 0);

    let job = kernel
        .get_job(created.job.job_id)
        .await
        .expect("load job")
        .job;
    assert_eq!(
        job.last_status,
        Some(lionclaw::contracts::SchedulerJobRunStatusDto::DeadLetter)
    );

    let runs = kernel
        .list_job_runs(JobRunsRequest {
            job_id: created.job.job_id,
            limit: Some(5),
        })
        .await
        .expect("list job runs");
    assert_eq!(runs.runs.len(), 2);
    assert_eq!(runs.runs[0].attempt_no, 2);
    assert_eq!(
        runs.runs[0].status,
        lionclaw::contracts::SchedulerJobRunStatusDto::DeadLetter
    );
    assert_eq!(runs.runs[1].attempt_no, 1);
    assert_eq!(
        runs.runs[1].status,
        lionclaw::contracts::SchedulerJobRunStatusDto::Failed
    );
}

#[tokio::test]
async fn scheduler_tick_continues_past_auth_invalid_jobs() {
    let env = TestEnv::new();
    let home = LionClawHome::new(env.temp_dir.path().join(".lionclaw"));
    home.ensure_base_dirs().await.expect("base dirs");
    write_test_codex_auth(&home).await;
    let turn_calls = Arc::new(AtomicUsize::new(0));
    let kernel = kernel_with_counting_codex_runtime(&env, &home, turn_calls.clone()).await;

    let invalid = kernel
        .create_job(JobCreateRequest {
            name: "codex scheduled".to_string(),
            runtime_id: "codex".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(2),
            },
            prompt_text: "scheduled run".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create invalid job");
    let valid = kernel
        .create_job(JobCreateRequest {
            name: "mock scheduled".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "scheduled run".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create valid job");

    tokio::fs::remove_file(home.root().join(".codex/auth.json"))
        .await
        .expect("remove runtime auth");

    let tick = kernel.scheduler_tick().await.expect("scheduler tick");
    assert_eq!(tick.claimed_runs, 2);
    assert_eq!(turn_calls.load(Ordering::SeqCst), 0);

    let invalid_job = kernel
        .get_job(invalid.job.job_id)
        .await
        .expect("load invalid job")
        .job;
    assert_eq!(
        invalid_job.last_status,
        Some(lionclaw::contracts::SchedulerJobRunStatusDto::DeadLetter)
    );

    let valid_job = kernel
        .get_job(valid.job.job_id)
        .await
        .expect("load valid job")
        .job;
    assert_eq!(
        valid_job.last_status,
        Some(lionclaw::contracts::SchedulerJobRunStatusDto::Completed)
    );
}

#[tokio::test]
async fn scheduled_job_capabilities_are_job_scoped_and_delivery_keeps_interactive_session() {
    let env = TestEnv::new();
    bootstrap_workspace(&env.workspace_root())
        .await
        .expect("bootstrap assistant workspace");
    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            workspace_root: Some(env.workspace_root()),
            project_workspace_root: Some(std::env::current_dir().expect("current dir")),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");
    let skill_id = install_enabled_skill(&kernel, "job-scope-reader").await;
    install_and_bind_channel(&kernel, "terminal", "terminal-delivery").await;
    approve_channel_peer(&kernel, "terminal", "alice").await;

    let seed_session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("seed interactive terminal session");

    let created = kernel
        .create_job(JobCreateRequest {
            name: "repo brief".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "please use job-scope-reader [cap:fs.read]".to_string(),
            skill_ids: vec![skill_id.clone()],
            allow_capabilities: vec!["fs.read".to_string()],
            delivery: Some(lionclaw::contracts::JobDeliveryTargetDto {
                channel_id: "terminal".to_string(),
                peer_id: "alice".to_string(),
            }),
            retry_attempts: Some(0),
        })
        .await
        .expect("create scheduled job");

    kernel.scheduler_tick().await.expect("run scheduler tick");

    let runs = kernel
        .list_job_runs(JobRunsRequest {
            job_id: created.job.job_id,
            limit: Some(5),
        })
        .await
        .expect("list job runs")
        .runs;
    assert_eq!(runs.len(), 1);
    let scheduled_session_id = runs[0].session_id.expect("scheduled run session id");

    let scheduled_capability_audit = kernel
        .query_audit(
            Some(scheduled_session_id),
            Some("capability.request".to_string()),
            None,
            Some(5),
        )
        .await
        .expect("query scheduled capability audit");
    assert_eq!(
        scheduled_capability_audit.events[0].details["allowed"].as_bool(),
        Some(true)
    );

    let latest_terminal = kernel
        .latest_session_snapshot(SessionLatestQuery {
            channel_id: "terminal".to_string(),
            peer_id: "alice".to_string(),
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("load latest terminal session");
    assert_eq!(
        latest_terminal
            .session
            .expect("terminal session")
            .session_id,
        seed_session.session_id
    );

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "scheduler-test".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(20),
            wait_ms: Some(0),
        })
        .await
        .expect("pull terminal stream");
    assert!(stream.events.iter().any(|event| {
        event.kind == StreamEventKindDto::MessageDelta
            && event
                .text
                .as_deref()
                .is_some_and(|text| text.contains("[mock]"))
    }));

    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: skill_id.clone(),
            capability: "skill.use".to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant global skill.use");

    let interactive = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: "job-scope-check".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("open interactive session");
    kernel
        .turn_session(SessionTurnRequest {
            session_id: interactive.session_id,
            user_text: "please use job-scope-reader [cap:fs.read]".to_string(),
            runtime_id: Some("mock".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("run interactive session");

    let interactive_capability_audit = kernel
        .query_audit(
            Some(interactive.session_id),
            Some("capability.request".to_string()),
            None,
            Some(5),
        )
        .await
        .expect("query interactive capability audit");
    assert_eq!(
        interactive_capability_audit.events[0].details["allowed"].as_bool(),
        Some(false),
        "job-scoped fs.read grants must not leak into interactive turns"
    );
}

#[tokio::test]
async fn scheduled_job_failure_delivers_summary_and_dead_letters() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    kernel
        .register_runtime_adapter("always-fail", Arc::new(AlwaysFailRuntimeAdapter))
        .await;
    install_and_bind_channel(&kernel, "terminal", "terminal-failure").await;

    let created = kernel
        .create_job(JobCreateRequest {
            name: "failing brief".to_string(),
            runtime_id: "always-fail".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "this will fail".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: Some(lionclaw::contracts::JobDeliveryTargetDto {
                channel_id: "terminal".to_string(),
                peer_id: "bob".to_string(),
            }),
            retry_attempts: Some(0),
        })
        .await
        .expect("create failing job");

    kernel.scheduler_tick().await.expect("run scheduler tick");

    let job = kernel
        .get_job(created.job.job_id)
        .await
        .expect("load failing job")
        .job;
    assert_eq!(
        job.last_status,
        Some(lionclaw::contracts::SchedulerJobRunStatusDto::DeadLetter)
    );
    assert!(!job.enabled);

    let runs = kernel
        .list_job_runs(JobRunsRequest {
            job_id: created.job.job_id,
            limit: Some(5),
        })
        .await
        .expect("list failing runs")
        .runs;
    assert_eq!(
        runs[0].status,
        lionclaw::contracts::SchedulerJobRunStatusDto::DeadLetter
    );
    assert_eq!(
        runs[0].delivery_status,
        Some(lionclaw::contracts::SchedulerJobDeliveryStatusDto::Delivered)
    );

    let stream = kernel
        .pull_channel_stream(ChannelStreamPullRequest {
            channel_id: "terminal".to_string(),
            consumer_id: "scheduler-failure-test".to_string(),
            start_mode: Some(ChannelStreamStartMode::Resume),
            start_after_sequence: None,
            limit: Some(20),
            wait_ms: Some(0),
        })
        .await
        .expect("pull failure delivery stream");
    assert!(stream.events.iter().any(|event| {
        event.kind == StreamEventKindDto::MessageDelta
            && event
                .text
                .as_deref()
                .is_some_and(|text| text.contains("Scheduled job 'failing brief' failed"))
    }));
}

#[tokio::test]
async fn paused_jobs_are_skipped_by_ticks_but_can_run_manually() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");

    let created = kernel
        .create_job(JobCreateRequest {
            name: "paused brief".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "manual-only run".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create paused job");

    kernel
        .pause_job(created.job.job_id)
        .await
        .expect("pause job");

    let tick = kernel.scheduler_tick().await.expect("run scheduler tick");
    assert_eq!(tick.claimed_runs, 0);

    let scheduled_runs = kernel
        .list_job_runs(JobRunsRequest {
            job_id: created.job.job_id,
            limit: Some(5),
        })
        .await
        .expect("list paused job runs")
        .runs;
    assert!(scheduled_runs.is_empty());

    let manual = kernel
        .run_job_now(JobRefRequest {
            job_id: created.job.job_id,
        })
        .await
        .expect("run paused job manually");
    assert_eq!(
        manual.run.status,
        lionclaw::contracts::SchedulerJobRunStatusDto::Completed
    );

    let paused_job = kernel
        .get_job(created.job.job_id)
        .await
        .expect("load paused job")
        .job;
    assert!(
        !paused_job.enabled,
        "manual runs must not implicitly re-enable paused jobs"
    );
    assert!(
        paused_job.next_run_at.is_some(),
        "manual runs must not rewrite the paused schedule"
    );

    let pause_audit = kernel
        .query_audit(None, Some("job.pause".to_string()), None, Some(5))
        .await
        .expect("query pause audit");
    assert!(pause_audit.events.iter().any(|event| {
        event.details["job_id"].as_str() == Some(&created.job.job_id.to_string())
    }));
}

#[tokio::test]
async fn removing_job_revokes_job_scoped_policy_grants() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    let skill_id = install_enabled_skill(&kernel, "scheduler-remove-guard").await;

    let created = kernel
        .create_job(JobCreateRequest {
            name: "grant cleanup".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() + ChronoDuration::minutes(5),
            },
            prompt_text: "grant cleanup".to_string(),
            skill_ids: vec![skill_id],
            allow_capabilities: vec!["fs.read".to_string()],
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create cleanup job");

    let pool = SqlitePool::connect(&env.db_url())
        .await
        .expect("open sqlite pool");
    let scope = format!("job:{}", created.job.job_id);
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM policy_grants WHERE scope = ?1")
        .bind(&scope)
        .fetch_one(&pool)
        .await
        .expect("count job-scoped grants before delete");
    assert_eq!(
        before, 2,
        "create_job should add skill.use and the allowed capability for the job scope"
    );

    let removed = kernel
        .remove_job(created.job.job_id)
        .await
        .expect("remove cleanup job");
    assert!(removed.removed);

    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM policy_grants WHERE scope = ?1")
        .bind(&scope)
        .fetch_one(&pool)
        .await
        .expect("count job-scoped grants after delete");
    assert_eq!(after, 0, "removing a job must revoke its scoped grants");
}

#[tokio::test]
async fn scheduler_tick_surfaces_unexpected_job_completion_failures() {
    let env = TestEnv::new();
    let kernel = Arc::new(Kernel::new(&env.db_path()).await.expect("kernel init"));
    let adapter = Arc::new(BlockingRuntimeAdapter::new());
    kernel
        .register_runtime_adapter("blocking", adapter.clone())
        .await;

    let created = kernel
        .create_job(JobCreateRequest {
            name: "corrupted completion".to_string(),
            runtime_id: "blocking".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "complete after corruption".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create corrupted completion job");

    let kernel_for_tick = kernel.clone();
    let tick = tokio::spawn(async move { kernel_for_tick.scheduler_tick().await });

    adapter.first_turn_started.notified().await;

    let pool = SqlitePool::connect(&env.db_url())
        .await
        .expect("open sqlite pool");
    sqlx::query("DELETE FROM scheduler_jobs WHERE job_id = ?1")
        .bind(created.job.job_id.to_string())
        .execute(&pool)
        .await
        .expect("delete running job row");

    adapter.release_first_turn.notify_one();

    let err = tick
        .await
        .expect("join tick")
        .expect_err("unexpected scheduler state corruption should fail the tick");
    let message = err.to_string();
    assert!(
        message.contains("scheduled job disappeared during completion")
            || message.contains("failed to load scheduler job for success"),
        "unexpected tick failure should surface the completion error, got: {message}"
    );
}

#[tokio::test]
async fn scheduler_ticks_are_single_flight_and_run_due_jobs_serially() {
    let env = TestEnv::new();
    let kernel = Arc::new(Kernel::new(&env.db_path()).await.expect("kernel init"));
    let adapter = Arc::new(BlockingRuntimeAdapter::new());
    kernel
        .register_runtime_adapter("blocking", adapter.clone())
        .await;

    kernel
        .create_job(JobCreateRequest {
            name: "first due job".to_string(),
            runtime_id: "blocking".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(2),
            },
            prompt_text: "first prompt".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create first job");
    kernel
        .create_job(JobCreateRequest {
            name: "second due job".to_string(),
            runtime_id: "blocking".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() - ChronoDuration::minutes(1),
            },
            prompt_text: "second prompt".to_string(),
            skill_ids: Vec::new(),
            allow_capabilities: Vec::new(),
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create second job");

    let kernel_for_tick = kernel.clone();
    let first_tick =
        tokio::spawn(async move { kernel_for_tick.scheduler_tick().await.expect("first tick") });

    adapter.first_turn_started.notified().await;

    let overlapping_tick = kernel.scheduler_tick().await.expect("overlapping tick");
    assert_eq!(
        overlapping_tick.claimed_runs, 0,
        "the scheduler lease must stay held until the active tick finishes executing"
    );

    adapter.release_first_turn.notify_one();

    let first_tick = first_tick.await.expect("join first tick");
    assert_eq!(first_tick.claimed_runs, 2);
    assert_eq!(
        adapter.prompts(),
        vec!["first prompt".to_string(), "second prompt".to_string()]
    );
}

struct AlwaysFailRuntimeAdapter;

struct CountingRuntimeAdapter {
    turn_calls: Arc<AtomicUsize>,
}

struct BlockingRuntimeAdapter {
    first_turn_started: Arc<Notify>,
    release_first_turn: Arc<Notify>,
    block_first_turn: AtomicBool,
    prompts: Mutex<Vec<String>>,
}

impl BlockingRuntimeAdapter {
    fn new() -> Self {
        Self {
            first_turn_started: Arc::new(Notify::new()),
            release_first_turn: Arc::new(Notify::new()),
            block_first_turn: AtomicBool::new(true),
            prompts: Mutex::new(Vec::new()),
        }
    }

    fn prompts(&self) -> Vec<String> {
        self.prompts.lock().expect("lock prompts").clone()
    }
}

#[async_trait]
impl RuntimeAdapter for AlwaysFailRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "always-fail".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("always-fail-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let _ = events.send(RuntimeEvent::Error {
            code: Some("runtime.failed".to_string()),
            text: "intentional test failure".to_string(),
        });
        anyhow::bail!("intentional test failure")
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

#[async_trait]
impl RuntimeAdapter for CountingRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "codex".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("codex-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        self.turn_calls.fetch_add(1, Ordering::SeqCst);
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult::default())
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

#[async_trait]
impl RuntimeAdapter for BlockingRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "blocking".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("blocking-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let observed_prompt = input
            .prompt
            .split("\n\n## User Input\n\n")
            .last()
            .unwrap_or(input.prompt.as_str())
            .to_string();
        self.prompts
            .lock()
            .expect("lock prompts")
            .push(observed_prompt);

        if self.block_first_turn.swap(false, Ordering::SeqCst) {
            self.first_turn_started.notify_one();
            self.release_first_turn.notified().await;
        }

        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text: format!("[blocking] {}", input.prompt),
        });
        let _ = events.send(RuntimeEvent::Done);
        Ok(RuntimeTurnResult::default())
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

async fn install_enabled_skill(kernel: &Kernel, skill_name: &str) -> String {
    let installed = kernel
        .install_skill(SkillInstallRequest {
            source: format!("local/{skill_name}"),
            reference: Some("main".to_string()),
            hash: Some(format!("{skill_name}-hash")),
            skill_md: Some(format!(
                "---\nname: {skill_name}\ndescription: {skill_name} test skill\n---"
            )),
            snapshot_path: None,
        })
        .await
        .expect("install skill");
    kernel
        .enable_skill(installed.skill_id.clone())
        .await
        .expect("enable skill");
    installed.skill_id
}

async fn install_and_bind_channel(kernel: &Kernel, channel_id: &str, skill_name: &str) {
    let skill_id = install_enabled_skill(kernel, skill_name).await;
    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: channel_id.to_string(),
            skill_id,
            enabled: Some(true),
            config: None,
        })
        .await
        .expect("bind channel");
}

async fn approve_channel_peer(kernel: &Kernel, channel_id: &str, peer_id: &str) {
    let _ = kernel
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
    let peers = kernel
        .list_channel_peers(Some(channel_id.to_string()))
        .await
        .expect("list channel peers");
    let pairing_code = peers
        .peers
        .iter()
        .find(|peer| peer.peer_id == peer_id)
        .and_then(|peer| peer.pairing_code.clone())
        .expect("pairing code");
    kernel
        .approve_channel_peer(lionclaw::contracts::ChannelPeerApproveRequest {
            channel_id: channel_id.to_string(),
            peer_id: peer_id.to_string(),
            pairing_code,
            trust_tier: Some(TrustTier::Main),
        })
        .await
        .expect("approve channel peer");
}

struct TestEnv {
    temp_dir: TempDir,
}

async fn write_test_codex_auth(home: &LionClawHome) {
    let codex_home = home.root().join(".codex");
    tokio::fs::create_dir_all(&codex_home)
        .await
        .expect("create codex home");
    tokio::fs::write(
        codex_home.join("auth.json"),
        r#"{
  "OPENAI_API_KEY": "sk-test"
}"#,
    )
    .await
    .expect("write codex auth");
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
        self.temp_dir.path().join("workspace")
    }
}

async fn kernel_with_codex_auth_requirement(env: &TestEnv, home: &LionClawHome) -> Kernel {
    kernel_with_counting_codex_runtime(env, home, Arc::new(AtomicUsize::new(0))).await
}

async fn kernel_with_counting_codex_runtime(
    env: &TestEnv,
    home: &LionClawHome,
    turn_calls: Arc<AtomicUsize>,
) -> Kernel {
    let fake_podman = env.temp_dir.path().join("podman");
    std::fs::write(
        &fake_podman,
        "#!/usr/bin/env bash\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:test-runtime-image\\n'\n  exit 0\nfi\nexit 0\n",
    )
    .expect("write fake podman");
    std::fs::set_permissions(&fake_podman, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake podman");

    let kernel = Kernel::new_with_options(
        &env.db_path(),
        KernelOptions {
            runtime_execution_profiles: BTreeMap::from([(
                "codex".to_string(),
                RuntimeExecutionProfile::new(
                    ConfinementConfig::Oci(OciConfinementConfig {
                        engine: fake_podman.display().to_string(),
                        image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                        ..OciConfinementConfig::default()
                    }),
                    "codex-auth".to_string(),
                    None,
                    Some(lionclaw::kernel::runtime::RuntimeAuthKind::Codex),
                ),
            )]),
            codex_home_override: Some(home.root().join(".codex")),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel init");
    kernel
        .register_runtime_adapter("codex", Arc::new(CountingRuntimeAdapter { turn_calls }))
        .await;
    kernel
}
