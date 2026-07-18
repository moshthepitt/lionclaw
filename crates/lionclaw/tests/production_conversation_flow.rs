//! The Slice 4 production conversation proof.  Only the native agent and
//! oracle transports are scripted; every boundary around them is production.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use clap::Parser;
use lionclaw::authority::AuthorityCeiling;
use lionclaw::config::RuntimeProfiles;
use lionclaw::engine::{Engine, EngineServices, MissionDisposition};
use lionclaw::mission_type::load_mission_type;
use lionclaw::model::{
    Assertion, AssertionId, DecisionAction, FinishClass, MissionEvent, MissionPhase, OracleName,
    Plan, PlanProposal, Requirement, RequirementDisposition, RequirementId, RequirementKind,
    RoleName, Task, TaskId, TaskKind,
};
use lionclaw::ports::{OracleOutcome, OracleRunRequest, OracleRunner};
use lionclaw::runner::OciRoleRunner;
use lionclaw::store::MissionStore;
use lionclaw::{cli, workspace};
use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthRegistry, RuntimeCancellation,
    RuntimeDriverConfig, RuntimeDriverProvider, RuntimeDriverRegistry, RuntimeResumeMode,
    RuntimeSessionHandle, RuntimeSessionStartInput, TurnExecution, TurnResult, TypedFailure,
};

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
    std::fs::write(&fake_oci, "#!/bin/sh\nexit 0\n").unwrap();
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
    let mission_type = load_mission_type(&mission_type_dir, &AuthorityCeiling::default()).unwrap();
    let turns = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let services = || {
        EngineServices::new(
            Arc::new(OciRoleRunner::with_registries(
                profiles.clone(),
                "production-image-id".into(),
                AuthorityCeiling::default(),
                RuntimeDriverRegistry::new([Arc::new(NativeProvider {
                    turns: turns.clone(),
                }) as Arc<dyn RuntimeDriverProvider>]),
                RuntimeAuthRegistry::empty(),
            )),
            Arc::new(ExternalOracleTransport {
                calls: calls.clone(),
            }),
            Arc::new(lionclaw::LocalEffectCleaner::new(
                fake_oci.to_string_lossy().into_owned(),
            )),
            Arc::new(lionclaw::ports::SystemClock),
        )
    };
    let store = MissionStore::open(&repo).await.unwrap();
    let engine = Engine::new(
        store,
        mission_type.clone(),
        "codex".into(),
        "production-image-id".into(),
        services(),
    );
    let mission_id = engine
        .create_mission(
            repo.to_str().unwrap(),
            "prove the production conversation",
            &base,
        )
        .await
        .unwrap();
    engine
        .propose_plan(
            &mission_id,
            PlanProposal {
                base_revision: 0,
                plan: plan(),
            },
        )
        .await
        .unwrap();
    engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "approve production proof",
        )
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
        cli::run(status).await.unwrap(),
        std::process::ExitCode::SUCCESS
    );

    drop(engine);
    let restarted = Engine::new(
        MissionStore::open(&repo).await.unwrap(),
        mission_type,
        "codex".into(),
        "production-image-id".into(),
        services(),
    );
    let outcome = loop {
        let outcome = restarted.advance(&mission_id).await.unwrap();
        if outcome.disposition == MissionDisposition::Terminal {
            break outcome;
        }
    };
    assert_eq!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );
    assert_ne!(outcome.state.current_sha, base);
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(turns.lock().unwrap().len(), 2);
    let events = restarted.store().load(&mission_id).await.unwrap();
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

    // Forbidden-seam audit: this proof intentionally contains none of the
    // mock/test-only constructors named by PRODUCTION-CONVERSATION-FLOW.
    let source = include_str!("production_conversation_flow.rs");
    for parts in [
        ["Mock", "RoleRunner"],
        ["Mock", "OracleRunner"],
        ["Noop", "EffectCleaner"],
        ["test_", "mission_type"],
        ["CapturedArtifact::", "for_testing"],
        ["for_", "testing("],
    ] {
        let forbidden = parts.concat();
        assert!(
            !source.contains(&forbidden),
            "forbidden seam used: {forbidden}"
        );
    }
}
