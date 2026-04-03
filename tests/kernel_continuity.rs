use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use lionclaw::{
    contracts::{
        ChannelBindRequest, ContinuityPathRequest, ContinuitySearchRequest, JobCreateRequest,
        PolicyGrantRequest, SessionHistoryPolicy, SessionOpenRequest, SessionTurnRequest,
        SkillInstallRequest, TrustTier,
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
use tempfile::TempDir;
use uuid::Uuid;

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
    tokio::fs::write(
        env.workspace_root()
            .join("continuity/open-loops/assistant-follow-up.md"),
        "# Assistant Open Loop\n\nStatus: open\n",
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
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "compaction-peer").await;
    for index in 0..30 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("turn {}", index),
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
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "continuity-surface-peer").await;
    for index in 0..18 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("review turn {} in src/kernel/continuity.rs", index),
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

    let memory = tokio::fs::read_to_string(env.workspace_root().join("MEMORY.md"))
        .await
        .expect("read memory");
    assert!(memory.contains("Prefers continuity module work to stay small and explicit."));
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
                reply: "captured".to_string(),
            }),
        )
        .await;

    let session = open_local_session(&kernel, "compaction-failure-peer").await;
    for index in 0..12 {
        kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: format!("turn {}", index),
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

    let merged_path = env
        .workspace_root()
        .join("continuity/proposals/memory/merge-me.md");
    let rejected_path = env
        .workspace_root()
        .join("continuity/proposals/memory/reject-me.md");
    let loop_path = env
        .workspace_root()
        .join("continuity/open-loops/review-audit.md");
    std::fs::write(
        &merged_path,
        format!(
            "# Memory Proposal: Merge Me\n\n- Status: proposed\n- Proposed: {} UTC\n- Rationale: keep durable preference\n\n## Candidate Entries\n- Remember merged proposal\n",
            Utc::now().to_rfc3339()
        ),
    )
    .expect("write merged proposal");
    std::fs::write(
        &rejected_path,
        format!(
            "# Memory Proposal: Reject Me\n\n- Status: proposed\n- Proposed: {} UTC\n- Rationale: reject duplicate\n\n## Candidate Entries\n- Remember rejected proposal\n",
            Utc::now().to_rfc3339()
        ),
    )
    .expect("write rejected proposal");
    std::fs::write(
        &loop_path,
        format!(
            "# Review Audit Trail\n\n- Status: open\n- Updated: {} UTC\n- Summary: verify continuity audit coverage\n- Next Step: query the audit log\n",
            Utc::now().to_rfc3339()
        ),
    )
    .expect("write open loop");

    kernel
        .merge_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: "continuity/proposals/memory/merge-me.md".to_string(),
        })
        .await
        .expect("merge proposal");
    kernel
        .reject_continuity_memory_proposal(ContinuityPathRequest {
            relative_path: "continuity/proposals/memory/reject-me.md".to_string(),
        })
        .await
        .expect("reject proposal");
    kernel
        .resolve_continuity_open_loop(ContinuityPathRequest {
            relative_path: "continuity/open-loops/review-audit.md".to_string(),
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

    fn workspace_root(&self) -> PathBuf {
        self.temp_dir.path().join("home-workspace")
    }

    fn project_root(&self) -> PathBuf {
        self.temp_dir.path().join("project-root")
    }
}

struct CapturePromptAdapter {
    prompts: Arc<Mutex<Vec<String>>>,
    capability_results: Arc<Mutex<Vec<Vec<RuntimeCapabilityResult>>>>,
    request_fs_read: bool,
    allow_hidden_compaction: bool,
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
                    "memory_proposals": [{
                        "title": "Continuity Product Preferences",
                        "rationale": "durable product preference surfaced during compaction",
                        "entries": ["Prefers continuity module work to stay small and explicit."]
                    }],
                    "open_loops": [{
                        "title": "Follow up on continuity surface",
                        "summary": "Need to review continuity product commands and proposal flow.",
                        "next_step": last_user
                    }]
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
