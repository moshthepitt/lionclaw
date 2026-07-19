//! The Slice 4 production conversation proof.  Only the native agent and
//! oracle transports are scripted; every boundary around them is production.

use std::collections::VecDeque;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use clap::Parser;
use lionclaw::config::RuntimeProfiles;
use lionclaw::engine::MissionDisposition;
use lionclaw::model::{
    apply, fold, Assertion, AssertionId, FinishClass, MissionEvent, MissionPhase, MissionState,
    OracleName, Plan, PlanProposal, Requirement, RequirementDisposition, RequirementId,
    RequirementKind, RoleName, Task, TaskId, TaskKind,
};
use lionclaw::ports::{OracleOutcome, OracleRunRequest, OracleRunner};
use lionclaw::store::MissionStore;
use lionclaw::{cli, workspace};
use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthRegistry, RuntimeCancellation,
    RuntimeDriverConfig, RuntimeDriverProvider, RuntimeDriverRegistry, RuntimeResumeMode,
    RuntimeSessionHandle, RuntimeSessionStartInput, TurnExecution, TurnResult, TypedFailure,
};

#[derive(Clone, Copy)]
enum DeliveryTurn {
    AwaitLead,
    InvalidHandoff,
    Complete,
    Review,
}

struct DeliveryTransport {
    turns: Arc<Mutex<VecDeque<DeliveryTurn>>>,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

impl DeliveryTransport {
    fn handoff(execution: &TurnExecution) -> std::path::PathBuf {
        let runtime = execution
            .context
            .runtime_state_root
            .as_ref()
            .expect("native state root");
        let mission = runtime
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .expect("mission state root");
        std::fs::read_dir(mission.join("effects"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path().join("handoff/handoff.json"))
            .find(|path| path.parent().is_some_and(Path::exists))
            .expect("current effect handoff mount")
    }
}

#[async_trait]
impl RuntimeAdapter for DeliveryTransport {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "delivery-native".into(),
            version: "1".into(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> anyhow::Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: input.session_id.to_string(),
            resume_mode: RuntimeResumeMode::Reconstructed,
        })
    }

    async fn turn(
        &self,
        execution: TurnExecution,
        _journal: lionclaw_runtime_api::RuntimeTurnJournalSender,
    ) -> anyhow::Result<TurnResult> {
        let turn = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted turn");
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        match turn {
            DeliveryTurn::AwaitLead => {}
            DeliveryTurn::InvalidHandoff => {
                std::fs::write(Self::handoff(&execution), b"{}")?;
            }
            DeliveryTurn::Complete => {
                let runtime = execution.context.runtime_state_root.as_ref().unwrap();
                let work = runtime.parent().unwrap().join("work");
                std::fs::write(work.join("delivery.txt"), "complete\n")?;
                git(&work, &["add", "delivery.txt"])?;
                git(&work, &["commit", "-q", "-m", "complete delivery proof"])?;
                std::fs::write(
                    Self::handoff(&execution),
                    r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work","done":true,"report":"delivery complete","request_attention":false}"#,
                )?;
            }
            DeliveryTurn::Review => {
                let nonce = execution
                    .input
                    .prompt
                    .rsplit_once("## Handoff nonce")
                    .expect("terminal review nonce")
                    .1
                    .trim();
                std::fs::write(
                    Self::handoff(&execution),
                    format!(
                        r#"{{"schema":"lionclaw.mission.review-handoff.v2","type":"review","done":true,"report":"delivery state clean","passed":true,"nonce":"{nonce}","gaps":[]}}"#
                    ),
                )?;
            }
        }
        Ok(TurnResult {
            final_response: "scripted delivery turn".into(),
            ..Default::default()
        })
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> anyhow::Result<RuntimeCancellation> {
        Ok(RuntimeCancellation::Acknowledged)
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> anyhow::Result<()> {
        Ok(())
    }
}

struct DeliveryProvider {
    turns: Arc<Mutex<VecDeque<DeliveryTurn>>>,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

impl RuntimeDriverProvider for DeliveryProvider {
    fn driver(&self) -> &'static str {
        "codex"
    }

    fn create_adapter(&self, _config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        Arc::new(DeliveryTransport {
            turns: self.turns.clone(),
            entered: self.entered.clone(),
            release: self.release.clone(),
        })
    }
}

struct NativeTransport {
    turns: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl RuntimeAdapter for NativeTransport {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "scripted-native".into(),
            version: "1".into(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> anyhow::Result<RuntimeSessionHandle> {
        let resume_mode = match input.resume {
            lionclaw_runtime_api::RuntimeResume::Native { ready, .. } if ready.is_ready() => {
                RuntimeResumeMode::Resumed
            }
            _ => RuntimeResumeMode::Reconstructed,
        };
        Ok(RuntimeSessionHandle {
            runtime_session_id: input.session_id.to_string(),
            resume_mode,
        })
    }

    async fn turn(
        &self,
        execution: TurnExecution,
        _journal: lionclaw_runtime_api::RuntimeTurnJournalSender,
    ) -> anyhow::Result<TurnResult> {
        self.turns
            .lock()
            .unwrap()
            .push(execution.input.prompt.clone());
        let runtime = execution
            .context
            .runtime_state_root
            .expect("native state root");
        std::fs::write(runtime.join("transport-session"), b"durable-native-id")?;
        let conversation = runtime.parent().expect("conversation root");
        let mission = conversation
            .parent()
            .and_then(Path::parent)
            .expect("mission state root");
        let handoff = std::fs::read_dir(mission.join("effects"))?
            .filter_map(Result::ok)
            .map(|entry| entry.path().join("handoff/handoff.json"))
            .find(|path| path.parent().is_some_and(Path::exists))
            .expect("current effect handoff mount");

        if execution.input.prompt.contains("## Handoff nonce") {
            let nonce = execution
                .input
                .prompt
                .rsplit_once("## Handoff nonce")
                .unwrap()
                .1
                .trim();
            std::fs::write(
                handoff,
                format!(
                    r#"{{"schema":"lionclaw.mission.review-handoff.v2","type":"review","done":true,"report":"clean production review","passed":true,"nonce":"{nonce}","gaps":[]}}"#
                ),
            )?;
        } else {
            let work = conversation.join("work");
            std::fs::write(work.join("base.txt"), "captured by production runner\n")?;
            git(&work, &["add", "base.txt"])?;
            git(
                &work,
                &["commit", "-q", "-m", "production conversation artifact"],
            )?;
            std::fs::write(
                handoff,
                r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work","done":true,"report":"production artifact captured","request_attention":false}"#,
            )?;
        }
        Ok(TurnResult {
            final_response: "native transport completed".into(),
            ..Default::default()
        })
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> anyhow::Result<RuntimeCancellation> {
        Ok(RuntimeCancellation::Acknowledged)
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> anyhow::Result<()> {
        Ok(())
    }
}

struct NativeProvider {
    turns: Arc<Mutex<Vec<String>>>,
}

impl RuntimeDriverProvider for NativeProvider {
    fn driver(&self) -> &'static str {
        "codex"
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        assert_eq!(config.runtime_id, "codex");
        Arc::new(NativeTransport {
            turns: self.turns.clone(),
        })
    }
}

struct ExternalOracleTransport {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

#[async_trait]
impl OracleRunner for ExternalOracleTransport {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        assert!(request.workspace_dir.join(".git").exists());
        self.calls
            .lock()
            .unwrap()
            .push((request.oracle.to_string(), request.judged_sha));
        Ok(OracleOutcome {
            exit_code: 0,
            exit_signal: None,
            stdout: b"authoritative external oracle passed\n".to_vec(),
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 1,
        })
    }
}

fn git(repo: &Path, args: &[&str]) -> anyhow::Result<()> {
    let status = std::process::Command::new("git")
        .current_dir(repo)
        .args(args)
        .status()?;
    anyhow::ensure!(status.success(), "git {args:?} failed");
    Ok(())
}

fn plan() -> Plan {
    let assertion = AssertionId::new("PRODUCTION-FLOW").unwrap();
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new("PRODUCTION-CONVERSATION").unwrap(),
            kind: RequirementKind::Validation,
            prose: "production conversation flow works".into(),
            disposition: RequirementDisposition::Covered {
                assertion_ids: vec![assertion.clone()],
            },
        }],
        assertions: vec![Assertion {
            id: assertion.clone(),
            prose: "the captured production artifact passes its oracle".into(),
            oracle: Some(OracleName::new("cargo-test").unwrap()),
        }],
        tasks: vec![Task {
            id: TaskId::new("integrate").unwrap(),
            kind: TaskKind::Work,
            body: "exercise production workspace and artifact capture".into(),
            targets: vec![assertion],
            role: Some(RoleName::new("implementer").unwrap()),
            depends_on: Vec::new(),
        }],
    }
}

async fn initialize_repo(repo: &Path) -> String {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-q"]).unwrap();
    git(repo, &["config", "user.name", "Production Flow Test"]).unwrap();
    git(repo, &["config", "user.email", "production@lionclaw.local"]).unwrap();
    git(repo, &["config", "commit.gpgsign", "false"]).unwrap();
    std::fs::write(repo.join("base.txt"), "base\n").unwrap();
    git(repo, &["add", "base.txt"]).unwrap();
    git(repo, &["commit", "-q", "-m", "base"]).unwrap();
    workspace::head_sha(repo).await.unwrap()
}

fn materialize_mission_type(root: &Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::create_dir_all(root.join("oracles")).unwrap();
    std::fs::write(root.join("playbook.md"), "Production flow fixture.\n").unwrap();
    std::fs::write(
        root.join("mission.toml"),
        r#"[mission-type]
name = "production-flow"
stop = "verified"
image = "production-image"

[terminal-review]
role = "gap-reviewer"

[execution]
default-timeout-secs = 60
max-task-time-secs = 120
extension-step-secs = 30
auto-continue-candidate = true
auto-continue-proof = true
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\nruntime: codex\n---\nImplement the production flow.\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/gap-reviewer.md"),
        "---\noutput: emits-gap-verdict\nruntime: codex\n---\nReview the production flow.\n",
    )
    .unwrap();
    let oracle = root.join("oracles/cargo-test");
    std::fs::write(&oracle, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&oracle, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn production_conversation_restarts_and_closes_verified() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(NativeProvider {
            turns: turns.clone(),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(ExternalOracleTransport {
            calls: calls.clone(),
        }),
    );
    let start = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "start",
        "--type",
        mission_type_dir.to_str().unwrap(),
        "--repo",
        repo.to_str().unwrap(),
        "--objective",
        "prove the production conversation",
        "--runtime",
        "codex",
    ])
    .unwrap();
    cli::run_with_transports(start, transports.clone())
        .await
        .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let missions = store.list_missions().await.unwrap();
    let [mission_id] = missions.as_slice() else {
        panic!("start must create exactly one mission")
    };
    let mission_id = mission_id.clone();
    let proposal_path = temp.path().join("proposal.json");
    std::fs::write(
        &proposal_path,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            plan: plan(),
        })
        .unwrap(),
    )
    .unwrap();
    let propose = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "plan",
        "propose",
        mission_id.as_str(),
        "--file",
        proposal_path.to_str().unwrap(),
        "--repo",
        repo.to_str().unwrap(),
    ])
    .unwrap();
    cli::run_with_transports(propose, transports.clone())
        .await
        .unwrap();
    let decide = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "decide",
        mission_id.as_str(),
        "plan_proposal:mission",
        "approve",
        "--justification",
        "approve production proof",
        "--repo",
        repo.to_str().unwrap(),
    ])
    .unwrap();
    cli::run_with_transports(decide, transports.clone())
        .await
        .unwrap();

    let status = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "status",
        mission_id.as_str(),
        "--repo",
        repo.to_str().unwrap(),
        "--json",
    ])
    .unwrap();
    assert_eq!(
        cli::run_with_transports(status, transports.clone())
            .await
            .unwrap(),
        std::process::ExitCode::SUCCESS
    );

    let outcome = loop {
        let handshake = temp
            .path()
            .join(format!("driver-{}.ready", turns.lock().unwrap().len()));
        let driver = cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            handshake.to_str().unwrap(),
        ])
        .unwrap();
        cli::run_with_transports(driver, transports.clone())
            .await
            .unwrap();
        let view = lionclaw::engine::load_mission_view(
            &MissionStore::open(&repo).await.unwrap(),
            &mission_id,
        )
        .await
        .unwrap();
        if view.disposition == MissionDisposition::Terminal {
            break view;
        }
    };
    assert_eq!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );
    for args in [
        vec![
            "lionclaw",
            "mission",
            "report",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--json",
        ],
        vec![
            "lionclaw",
            "mission",
            "inbox",
            "--repo",
            repo.to_str().unwrap(),
            "--json",
        ],
    ] {
        let observation = cli::Cli::try_parse_from(args).unwrap();
        assert_eq!(
            cli::run_with_transports(observation, transports.clone())
                .await
                .unwrap(),
            std::process::ExitCode::SUCCESS
        );
    }
    assert_ne!(outcome.state.current_sha, base);
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(turns.lock().unwrap().len(), 2);
    let restarted = MissionStore::open(&repo).await.unwrap();
    let events = restarted.load(&mission_id).await.unwrap();
    let replayed = fold(events.clone()).expect("full replay");
    assert_eq!(replayed, outcome.state);
    for split in 1..events.len() {
        let mut from_prefix = fold(events[..split].to_vec()).expect("substantive prefix");
        for event in &events[split..] {
            apply(&mut from_prefix, event);
        }
        assert_eq!(from_prefix, replayed, "prefix split {split} diverged");
    }
    let snapshotted = restarted
        .load_state_snapshotted(&mission_id)
        .await
        .expect("snapshot resume")
        .expect("mission state");
    assert_eq!(snapshotted, replayed);
    let snapshot_json = serde_json::to_string(&snapshotted).expect("encode snapshot state");
    assert_eq!(
        serde_json::from_str::<MissionState>(&snapshot_json).expect("decode snapshot state"),
        replayed
    );
    let rebuilt = restarted
        .rebuild_cursors(&mission_id, 9_000_000)
        .await
        .expect("rebuild snapshot from authoritative log");
    assert_eq!(rebuilt, replayed);
    let role = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::RoleRunCompleted { .. }))
        .unwrap();
    let oracle = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::OracleRunCompleted { .. }))
        .unwrap();
    let gate = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::TerminalReviewRequested { .. }))
        .unwrap();
    let review = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::TerminalReviewCompleted { .. }))
        .unwrap();
    assert!(role < oracle && oracle < gate && gate < review);
    assert!(repo
        .join(".lionclaw/missions")
        .read_dir()
        .unwrap()
        .all(|entry| {
            !entry
                .unwrap()
                .path()
                .join("effects")
                .read_dir()
                .is_ok_and(|mut entries| entries.next().is_some())
        }));
}

#[tokio::test]
async fn production_delivery_uses_one_exact_immutable_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::AwaitLead,
        DeliveryTurn::InvalidHandoff,
        DeliveryTurn::Complete,
        DeliveryTurn::Review,
    ])));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(DeliveryProvider {
            turns: turns.clone(),
            entered: entered.clone(),
            release: release.clone(),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(ExternalOracleTransport {
            calls: Arc::new(Mutex::new(Vec::new())),
        }),
    );
    let start = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "start",
        "--type",
        mission_type_dir.to_str().unwrap(),
        "--repo",
        repo.to_str().unwrap(),
        "--objective",
        "prove exact delivery",
        "--runtime",
        "codex",
    ])
    .unwrap();
    cli::run_with_transports(start, transports.clone())
        .await
        .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission_id = store.list_missions().await.unwrap().pop().unwrap();
    let proposal_path = temp.path().join("delivery-plan.json");
    std::fs::write(
        &proposal_path,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            plan: plan(),
        })
        .unwrap(),
    )
    .unwrap();
    for cli in [
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            mission_id.as_str(),
            "--file",
            proposal_path.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission_id.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "approve delivery proof",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    ] {
        cli::run_with_transports(cli, transports.clone())
            .await
            .unwrap();
    }

    let driver_cli = |handshake: &Path| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            handshake.to_str().unwrap(),
        ])
        .unwrap()
    };
    let driver = tokio::spawn({
        let transports = transports.clone();
        let command = driver_cli(&temp.path().join("delivery-first.ready"));
        async move { cli::run_with_transports(command, transports).await }
    });
    entered.acquire().await.unwrap().forget();
    let state = store.require_state(&mission_id).await.unwrap();
    let conversation_id = state.conversations.keys().next().unwrap().to_string();
    let send = |body: &'static str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission_id.as_str(),
            "--to",
            conversation_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            body,
        ])
        .unwrap()
    };
    cli::run_with_transports(send("before"), transports.clone())
        .await
        .unwrap();
    release.add_permits(1);
    entered.acquire().await.unwrap().forget();
    cli::run_with_transports(send("during"), transports.clone())
        .await
        .unwrap();
    let active = store.require_state(&mission_id).await.unwrap();
    let delivery = active.conversations.values().next().unwrap();
    let boundary = delivery.active_delivery.as_ref().unwrap().message_boundary;
    assert_eq!(delivery.queued.len(), 2);
    assert!(delivery.queued[0].sequence_no <= boundary);
    assert!(delivery.queued[1].sequence_no > boundary);

    release.add_permits(1);
    entered.acquire().await.unwrap().forget();
    let reworking = store.require_state(&mission_id).await.unwrap();
    let delivery = reworking.conversations.values().next().unwrap();
    assert_eq!(
        delivery.queued[0].marker,
        lionclaw::model::DeliveryMarker::PreviouslyDelivered
    );
    assert_eq!(
        delivery.queued[1].marker,
        lionclaw::model::DeliveryMarker::Queued
    );
    let retry_boundary = delivery.active_delivery.as_ref().unwrap().message_boundary;
    assert!(delivery.queued[1].sequence_no <= retry_boundary);

    cli::run_with_transports(send("after"), transports.clone())
        .await
        .unwrap();
    release.add_permits(2);
    driver.await.unwrap().unwrap();
    let completed = store.require_state(&mission_id).await.unwrap();
    let delivery = completed.conversations.values().next().unwrap();
    assert_eq!(delivery.queued.len(), 1);
    assert_eq!(delivery.queued[0].body, "after");
    assert!(delivery.queued[0].sequence_no > retry_boundary);
    assert_eq!(
        completed.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );

    assert!(turns.lock().unwrap().is_empty());
}
