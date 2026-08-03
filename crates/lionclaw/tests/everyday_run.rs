mod common;

use std::collections::VecDeque;
use std::io::Write;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use clap::Parser;
use lionclaw::cli::{self, Cli, MissionTransports};
use lionclaw::config::RuntimeProfiles;
use lionclaw::everyday::AttachedRuntimeExecutor;
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner, NoopEffectCleaner};
use lionclaw_confinement::{ExecutionRequest, MountAccess};
use lionclaw_runtime_api::{
    ExecutionOutput, RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthIdentity, RuntimeAuthKind,
    RuntimeAuthMaterialization, RuntimeAuthPreparation, RuntimeAuthProjection, RuntimeAuthProvider,
    RuntimeAuthRegistry, RuntimeCancellation, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeDriverRegistry, RuntimeProgramSpec, RuntimeSessionHandle, RuntimeSessionStartInput,
    RuntimeTerminalProgramInput, RuntimeTurnJournalSender, TurnExecution, TurnResult,
};

const NONTERMINAL_EXIT: u8 = 2;
const IMAGE_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const IMAGE_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(Default)]
struct Observations {
    validations: usize,
    auth_preparations: usize,
    terminal_inputs: Vec<RuntimeTerminalProgramInput>,
    requests: Vec<ExecutionRequest>,
    context: Vec<String>,
    bridge_socket_mounts: usize,
    projected_clients: Vec<String>,
    image_resolutions: Vec<(String, String, String)>,
}

struct FakeDriver {
    observations: Arc<Mutex<Observations>>,
}

impl RuntimeDriverProvider for FakeDriver {
    fn driver(&self) -> &'static str {
        "fake-terminal"
    }

    fn validate_config(&self, config: &RuntimeDriverConfig) -> Result<()> {
        assert!(matches!(
            config.runtime_id.as_str(),
            "fake" | "codex" | "opencode"
        ));
        assert_eq!(config.executable, "real-agent");
        assert_eq!(config.auth, Some(RuntimeAuthKind::from_static("fake-auth")));
        self.observations.lock().unwrap().validations += 1;
        Ok(())
    }

    fn create_adapter(&self, _config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        Arc::new(FakeAdapter {
            observations: Arc::clone(&self.observations),
        })
    }
}

struct FakeAdapter {
    observations: Arc<Mutex<Observations>>,
}

#[async_trait]
impl RuntimeAdapter for FakeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "fake".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    fn session_start(&self, _input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        unreachable!("the everyday terminal path does not open a protocol turn")
    }

    async fn turn(
        &self,
        _execution: TurnExecution,
        _journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult> {
        unreachable!("the everyday terminal path does not run a protocol turn")
    }

    fn build_terminal_program(
        &self,
        input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        let mut args = vec!["tui".to_string()];
        if input.resume {
            args.push("--continue".to_string());
        }
        args.push("--prompt".to_string());
        args.push(input.bootstrap_message.clone());
        self.observations
            .lock()
            .unwrap()
            .terminal_inputs
            .push(input);
        Ok(RuntimeProgramSpec {
            executable: "real-agent".to_string(),
            args,
            environment: Vec::new(),
            stdin: String::new(),
            auth: Some(RuntimeAuthKind::from_static("fake-auth")),
        })
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<RuntimeCancellation> {
        Ok(RuntimeCancellation::NoActiveTurn)
    }

    fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

struct FakeAuth {
    observations: Arc<Mutex<Observations>>,
}

#[async_trait]
impl RuntimeAuthProvider for FakeAuth {
    fn kind(&self) -> &'static str {
        "fake-auth"
    }

    async fn prepare(
        &self,
        input: RuntimeAuthPreparation<'_>,
    ) -> Result<RuntimeAuthMaterialization> {
        assert!(matches!(input.runtime_id, "fake" | "codex" | "opencode"));
        assert!(input.auth_staging_root.is_some());
        self.observations.lock().unwrap().auth_preparations += 1;
        Ok(RuntimeAuthMaterialization::new(
            RuntimeAuthKind::from_static("fake-auth"),
            RuntimeAuthIdentity::new("fake-auth-identity").unwrap(),
            RuntimeAuthProjection::default(),
        ))
    }
}

struct FakeAttached {
    observations: Arc<Mutex<Observations>>,
    outputs: Mutex<VecDeque<ExecutionOutput>>,
    image_identities: Mutex<VecDeque<String>>,
}

impl FakeAttached {
    fn new(
        observations: Arc<Mutex<Observations>>,
        outputs: impl IntoIterator<Item = ExecutionOutput>,
        image_identities: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            observations,
            outputs: Mutex::new(outputs.into_iter().collect()),
            image_identities: Mutex::new(image_identities.into_iter().collect()),
        }
    }
}

#[async_trait]
impl AttachedRuntimeExecutor for FakeAttached {
    async fn resolve_image_compatibility_identity(
        &self,
        engine: &str,
        image: &str,
    ) -> Result<String> {
        let mut identities = self.image_identities.lock().unwrap();
        let identity = if identities.len() > 1 {
            identities.pop_front().unwrap()
        } else {
            identities.front().cloned().expect("fake image identity")
        };
        self.observations.lock().unwrap().image_resolutions.push((
            engine.to_string(),
            image.to_string(),
            identity.clone(),
        ));
        Ok(identity)
    }

    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        let runtime_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime")
            .expect("runtime mount");
        let context = std::fs::read_to_string(runtime_mount.source.join("AGENTS.generated.md"))
            .expect("neutral bootstrap context");
        let bridge_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime/lionclaw/operator.sock")
            .expect("operator bridge socket mount");
        assert!(std::fs::symlink_metadata(&bridge_mount.source)
            .expect("live operator bridge socket")
            .file_type()
            .is_socket());
        let skill_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime/home/.agents/skills/lionclaw")
            .expect("standard skill mount");
        let projected_client =
            std::fs::read_to_string(skill_mount.source.join("lionclaw")).unwrap();
        let mut observations = self.observations.lock().unwrap();
        observations.context.push(context);
        observations.bridge_socket_mounts += 1;
        observations.projected_clients.push(projected_client);
        observations.requests.push(request);
        drop(observations);
        Ok(self.outputs.lock().unwrap().pop_front().unwrap_or_default())
    }
}

struct BridgeAttached {
    observations: Arc<Mutex<Observations>>,
    phase: Mutex<u8>,
    proposal: String,
    commands: Arc<Mutex<Vec<Vec<String>>>>,
    mission_id: Arc<Mutex<Option<lionclaw::model::MissionId>>>,
}

impl BridgeAttached {
    fn new(
        observations: Arc<Mutex<Observations>>,
        proposal: String,
        commands: Arc<Mutex<Vec<Vec<String>>>>,
        mission_id: Arc<Mutex<Option<lionclaw::model::MissionId>>>,
    ) -> Self {
        Self {
            observations,
            phase: Mutex::new(0),
            proposal,
            commands,
            mission_id,
        }
    }

    fn run_client(&self, request: &ExecutionRequest, args: &[String], stdin: &str) -> Result<()> {
        let bridge_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime/lionclaw/operator.sock")
            .context("operator bridge socket mount")?;
        let skill_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime/home/.agents/skills/lionclaw")
            .context("standard skill mount")?;
        let client = std::fs::read_to_string(skill_mount.source.join("lionclaw"))?;
        let mapped_client = client.replace(
            &serde_json::to_string("/runtime/lionclaw/operator.sock")?,
            &serde_json::to_string(&bridge_mount.source.to_string_lossy())?,
        );
        let mut script = tempfile::NamedTempFile::new()?;
        script.write_all(mapped_client.as_bytes())?;
        let mut child = std::process::Command::new("node")
            .arg(script.path())
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("launching projected bridge client")?;
        child
            .stdin
            .take()
            .context("projected bridge client stdin")?
            .write_all(stdin.as_bytes())?;
        let output = child.wait_with_output()?;
        self.commands.lock().unwrap().push(args.to_vec());
        if !output.status.success() {
            bail!(
                "projected bridge command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }
}

#[async_trait]
impl AttachedRuntimeExecutor for BridgeAttached {
    async fn resolve_image_compatibility_identity(
        &self,
        _engine: &str,
        _image: &str,
    ) -> Result<String> {
        Ok(IMAGE_A.to_string())
    }

    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        let runtime_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime")
            .context("runtime mount")?;
        let context = std::fs::read_to_string(runtime_mount.source.join("AGENTS.generated.md"))?;
        self.observations.lock().unwrap().context.push(context);
        let repo = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/workspace")
            .map(|mount| mount.source.clone())
            .context("workspace mount")?;
        let phase = {
            let mut phase = self.phase.lock().unwrap();
            let current = *phase;
            *phase += 1;
            current
        };
        match phase {
            0 => {
                self.run_client(
                    &request,
                    &[
                        "mission".into(),
                        "start".into(),
                        "--type".into(),
                        "software-dev".into(),
                        "--objective".into(),
                        "Exercise the attached everyday bridge end to end".into(),
                        "--runtime".into(),
                        "codex".into(),
                        "--image".into(),
                        IMAGE_A.into(),
                        "--json".into(),
                    ],
                    "",
                )?;
                let store = MissionStore::open(&repo).await?;
                let mission_id = store
                    .list_missions()
                    .await?
                    .into_iter()
                    .next()
                    .context("bridge-created mission")?;
                *self.mission_id.lock().unwrap() = Some(mission_id.clone());
                self.run_client(
                    &request,
                    &[
                        "mission".into(),
                        "plan".into(),
                        "propose".into(),
                        mission_id.to_string(),
                        "--file".into(),
                        "-".into(),
                    ],
                    &self.proposal,
                )?;
                let state = store.require_state(&mission_id).await?;
                let decision_id = lionclaw::model::next(&state)
                    .choices
                    .into_iter()
                    .find_map(|choice| match choice {
                        lionclaw::model::Choice::Decide {
                            id,
                            action: lionclaw::model::DecisionAction::Approve,
                        } => Some(id),
                        _ => None,
                    })
                    .context("plan approval choice")?;
                self.run_client(
                    &request,
                    &[
                        "mission".into(),
                        "decide".into(),
                        mission_id.to_string(),
                        decision_id,
                        "approve".into(),
                        "--justification".into(),
                        "deterministic attached bridge acceptance".into(),
                    ],
                    "",
                )?;
                for _ in 0..20 {
                    if lionclaw::model::next(&store.require_state(&mission_id).await?)
                        .choices
                        .iter()
                        .any(|choice| matches!(choice, lionclaw::model::Choice::Finish { .. }))
                    {
                        break;
                    }
                    self.run_client(
                        &request,
                        &[
                            "mission".into(),
                            "advance".into(),
                            mission_id.to_string(),
                            "--wait".into(),
                            "--json".into(),
                        ],
                        "",
                    )?;
                }
                let ready = store.require_state(&mission_id).await?;
                assert!(lionclaw::model::next(&ready)
                    .choices
                    .iter()
                    .any(|choice| matches!(choice, lionclaw::model::Choice::Finish { .. })));
                Ok(signal(9))
            }
            1 => {
                let mission_id = self
                    .mission_id
                    .lock()
                    .unwrap()
                    .clone()
                    .context("bridge mission id survived attached restart")?;
                self.run_client(
                    &request,
                    &[
                        "mission".into(),
                        "finish".into(),
                        mission_id.to_string(),
                        "--reason".into(),
                        "attached bridge acceptance completed".into(),
                    ],
                    "",
                )?;
                self.run_client(
                    &request,
                    &[
                        "mission".into(),
                        "report".into(),
                        mission_id.to_string(),
                        "--json".into(),
                    ],
                    "",
                )?;
                self.run_client(
                    &request,
                    &["mission".into(), "apply".into(), mission_id.to_string()],
                    "",
                )?;
                Ok(output(0))
            }
            _ => bail!("unexpected attached bridge execution phase {phase}"),
        }
    }
}

fn output(code: i32) -> ExecutionOutput {
    ExecutionOutput {
        exit_code: Some(code),
        ..Default::default()
    }
}

fn software_dev_proposal_json() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let mission_type = lionclaw::mission_type::load_mission_type(
        &root.join("mission-types/software-dev"),
        &lionclaw::authority::AuthorityCeiling::default(),
    )
    .unwrap();
    let plan = common::simple_plan();
    let mut team = mission_type.default_team.clone();
    team.revision = 1;
    team.task_assignments = std::collections::BTreeMap::from([(
        plan.tasks[0].id.clone(),
        lionclaw::model::RoleInstanceId::new("implementer")
            .unwrap()
            .into(),
    )]);
    team.judgment_assignments = std::collections::BTreeMap::from([(
        plan.assertions[0].id.clone(),
        vec![lionclaw::model::RoleInstanceId::new("reviewer").unwrap()],
    )]);
    serde_json::to_string(&lionclaw::model::MissionProposal {
        team: Some(team),
        plan: Some(lionclaw::model::PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan,
        }),
        oracles: Some(common::oracle_specs(&common::simple_plan())),
    })
    .unwrap()
}

fn software_dev_child_proposal_json() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let mission_type = lionclaw::mission_type::load_mission_type(
        &root.join("mission-types/software-dev"),
        &lionclaw::authority::AuthorityCeiling::default(),
    )
    .unwrap();
    let plan = common::simple_plan();
    let child_planner = common::role(
        "child-planner",
        lionclaw::model::OutputSemantics::ProposesPlan,
    );
    let child_worker = common::role(
        "child-worker",
        lionclaw::model::OutputSemantics::ProducesArtifact,
    );
    let child_reviewer = common::role(
        "child-reviewer",
        lionclaw::model::OutputSemantics::EmitsVerdict,
    );
    let child_team = lionclaw::model::TeamRevision {
        revision: 0,
        roles: std::collections::BTreeMap::from([
            (child_planner.id.clone(), child_planner),
            (child_worker.id.clone(), child_worker),
            (child_reviewer.id.clone(), child_reviewer),
        ]),
        planning_assignment: lionclaw::model::RoleInstanceId::new("child-planner").unwrap(),
        task_assignments: std::collections::BTreeMap::from([(
            plan.tasks[0].id.clone(),
            lionclaw::model::RoleInstanceId::new("child-worker")
                .unwrap()
                .into(),
        )]),
        judgment_assignments: std::collections::BTreeMap::from([(
            plan.assertions[0].id.clone(),
            vec![lionclaw::model::RoleInstanceId::new("child-reviewer").unwrap()],
        )]),
        gap_review_assignment: None,
        guidance: None,
    };
    let child = lionclaw::model::ChildMissionAssignment {
        objective: "produce the delegated report through an ordinary child mission".into(),
        output: lionclaw::model::OutputSemantics::ProducesArtifact,
        config: lionclaw::model::MissionConfig {
            stop: lionclaw::model::StopBar::Verified,
            ceilings: lionclaw::model::AuthorityCeilings {
                writes: true,
                ..Default::default()
            },
            runtime_ceilings: std::collections::BTreeSet::from(["codex".into()]),
            recovery: lionclaw::model::RecoveryConfig { max_attempts: 2 },
            execution: lionclaw::model::ExecutionPolicy {
                default_timeout_secs: 60,
                max_task_time_secs: 120,
                extension_step_secs: 30,
                effect_capacity: 1,
                max_child_depth: 3,
                max_descendants: 8,
                auto_continue_candidate: false,
                auto_continue_proof: false,
            },
            ..Default::default()
        },
        proposal: Box::new(lionclaw::model::MissionProposal {
            team: Some(child_team),
            plan: Some(lionclaw::model::PlanProposal {
                base_revision: 0,
                requirement_changes: Vec::new(),
                assertion_supersessions: Vec::new(),
                plan: plan.clone(),
            }),
            oracles: Some(common::oracle_specs(&plan)),
        }),
        deadline_secs: 120,
    };
    let mut parent_team = mission_type.default_team.clone();
    parent_team.revision = 1;
    parent_team.task_assignments = std::collections::BTreeMap::from([(
        plan.tasks[0].id.clone(),
        lionclaw::model::TaskAssignment::ChildMission {
            mission: Box::new(child),
        },
    )]);
    parent_team.judgment_assignments = std::collections::BTreeMap::from([(
        plan.assertions[0].id.clone(),
        vec![lionclaw::model::RoleInstanceId::new("reviewer").unwrap()],
    )]);
    serde_json::to_string(&lionclaw::model::MissionProposal {
        team: Some(parent_team),
        plan: Some(lionclaw::model::PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan.clone(),
        }),
        oracles: Some(common::oracle_specs(&plan)),
    })
    .unwrap()
}

fn signal(number: i32) -> ExecutionOutput {
    ExecutionOutput {
        exit_signal: Some(number),
        ..Default::default()
    }
}

fn profiles(home: &Path) -> RuntimeProfiles {
    profiles_with_image(home, IMAGE_A)
}

fn profiles_with_image(home: &Path, image: &str) -> RuntimeProfiles {
    RuntimeProfiles::from_toml(
        &format!(
            r#"
        [runtimes.fake]
        driver = "fake-terminal"
        command = "real-agent"
        native-resume = true
        auth = "fake-auth"
        skills-dir = ".agents/skills"
        model-network = {{ mode = "allow", destinations = [{{ host = "api.fake.example", ports = [443] }}] }}
        confinement = {{ backend = "podman", image = "{image}", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=64m"] }}

        [runtimes.codex]
        driver = "fake-terminal"
        command = "real-agent"
        native-resume = true
        auth = "fake-auth"
        skills-dir = ".agents/skills"
        model-network = {{ mode = "allow", destinations = [{{ host = "api.codex.example", ports = [443] }}] }}
        confinement = {{ backend = "podman", image = "{image}", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=64m"] }}

        [runtimes.opencode]
        driver = "fake-terminal"
        command = "real-agent"
        native-resume = true
        auth = "fake-auth"
        skills-dir = ".agents/skills"
        model-network = {{ mode = "allow", destinations = [{{ host = "api.opencode.example", ports = [443] }}] }}
        confinement = {{ backend = "podman", image = "{image}", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=64m"] }}
        "#
        ),
        home,
    )
    .expect("fake runtime profile")
}

fn transports(
    home: &Path,
    runtime_root: &Path,
    observations: Arc<Mutex<Observations>>,
    outputs: impl IntoIterator<Item = ExecutionOutput>,
) -> MissionTransports {
    transports_with_profiles(
        profiles(home),
        observations,
        outputs,
        [IMAGE_A.to_string()],
        runtime_root,
    )
}

fn transports_with_profiles(
    profiles: RuntimeProfiles,
    observations: Arc<Mutex<Observations>>,
    outputs: impl IntoIterator<Item = ExecutionOutput>,
    image_identities: impl IntoIterator<Item = String>,
    runtime_root: &Path,
) -> MissionTransports {
    let attached = Arc::new(FakeAttached::new(
        Arc::clone(&observations),
        outputs,
        image_identities,
    ));
    MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(FakeDriver {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::new([Arc::new(FakeAuth {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(MockOracleRunner::exiting(0)),
    )
    .with_attached_runtime(attached)
    .with_everyday_runtime_root(runtime_root.to_path_buf())
}

fn run_cli(repo: &Path) -> Cli {
    Cli::try_parse_from(["lionclaw", "run", "fake", "--repo", repo.to_str().unwrap()])
        .expect("lionclaw run fake parses")
}

fn new_run_cli(repo: &Path) -> Cli {
    Cli::try_parse_from([
        "lionclaw",
        "run",
        "fake",
        "--new",
        "--repo",
        repo.to_str().unwrap(),
    ])
    .expect("lionclaw run fake --new parses")
}

fn guide_cli(repo: &Path) -> Cli {
    Cli::try_parse_from([
        "lionclaw",
        "mission",
        "guide",
        "--repo",
        repo.to_str().unwrap(),
        "--json",
    ])
    .expect("lionclaw mission guide parses")
}

fn apply_cli(repo: &Path) -> Cli {
    Cli::try_parse_from([
        "lionclaw",
        "mission",
        "apply",
        "--repo",
        repo.to_str().unwrap(),
    ])
    .expect("lionclaw mission apply parses")
}

fn bridge_role_runner() -> Arc<MockRoleRunner> {
    Arc::new(MockRoleRunner::new(Box::new(|request| {
        use lionclaw::model::OutputSemantics;

        if request.role.output == OutputSemantics::EmitsGapVerdict {
            return Ok(lionclaw::testing::review_verdict(request, true, Vec::new()));
        }
        let (handoff, artifact) = match request.role.output {
            OutputSemantics::EmitsVerdict => (
                lionclaw::model::Handoff::Validate {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("bridge judgment"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| lionclaw::model::ValidationItem {
                            item_id,
                            passed: true,
                        })
                        .collect(),
                    passed: true,
                    request_attention: false,
                },
                None,
            ),
            OutputSemantics::ProducesReport => (
                lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("bridge-created report"),
                    request_attention: false,
                },
                None,
            ),
            OutputSemantics::ProducesArtifact => (
                lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("bridge-created artifact"),
                    request_attention: false,
                },
                Some(lionclaw::ports::CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    common::HEAD_SHA,
                )),
            ),
            OutputSemantics::ProposesPlan | OutputSemantics::EmitsGapVerdict => {
                unreachable!("attached acceptance does not dispatch planning")
            }
        };
        Ok(lionclaw::ports::RoleTurnOutcome {
            handoff: Some(handoff),
            artifact,
            prepared_inputs: Vec::new(),
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "bridge role completed".to_string(),
        })
    })))
}

#[tokio::test]
async fn everyday_run_reaches_validated_profile_auth_and_confinement() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));

    let code = cli::run_with_transports(
        run_cli(temp.path()),
        transports(
            temp.path(),
            runtime.path(),
            Arc::clone(&observations),
            [output(0)],
        ),
    )
    .await
    .expect("run succeeds at the transport boundary");

    assert_eq!(code, std::process::ExitCode::from(NONTERMINAL_EXIT));
    let observations = observations.lock().unwrap();
    assert!(observations.validations >= 1);
    assert_eq!(observations.auth_preparations, 1);
    assert_eq!(
        observations.image_resolutions,
        [(
            "podman".to_string(),
            IMAGE_A.to_string(),
            IMAGE_A.to_string()
        )]
    );
    assert_eq!(observations.requests.len(), 1);
    let request = &observations.requests[0];
    assert_eq!(request.plan.workspace_access.as_str(), "read-only");
    assert!(request.plan.network.allows("api.fake.example", 443));
    let resource_name = request
        .resource_name
        .as_deref()
        .expect("destination-scoped everyday network has an OCI resource owner");
    assert!(resource_name.starts_with("lionclaw-everyday-"));
    assert_eq!(resource_name.len(), "lionclaw-everyday-".len() + 32);
    assert!(request
        .plan
        .environment
        .contains(&("HOME".to_string(), "/runtime/home".to_string())));
    assert!(request.plan.environment.contains(&(
        "XDG_DATA_HOME".to_string(),
        "/runtime/home/.local/share".to_string()
    )));
    assert!(request.runtime_auth.is_some());
    assert!(request.plan.mounts.iter().any(|mount| {
        mount.target == "/runtime/home/.agents/skills/lionclaw"
            && mount.access == MountAccess::ReadOnly
    }));
    assert!(request
        .plan
        .mounts
        .iter()
        .any(|mount| { mount.target == "/scratch" && mount.access == MountAccess::ReadWrite }));
    assert_eq!(observations.bridge_socket_mounts, 1);
    assert!(observations.projected_clients[0].starts_with("#!/usr/bin/env node\n"));
    assert!(!observations.projected_clients[0]
        .contains(std::env::current_exe().unwrap().to_str().unwrap()));
    assert!(observations.context[0].contains("\"repository\": \"/workspace\""));
    assert!(observations.context[0].contains("\"runtime\": \"fake\""));
    assert!(observations.context[0].contains("\"mission\": null"));
}

#[tokio::test]
async fn attached_runtime_drives_full_mission_lifecycle_through_the_projected_bridge() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let commands = Arc::new(Mutex::new(Vec::new()));
    let mission_id = Arc::new(Mutex::new(None));
    let attached = Arc::new(BridgeAttached::new(
        Arc::clone(&observations),
        software_dev_proposal_json(),
        Arc::clone(&commands),
        Arc::clone(&mission_id),
    ));
    let role_runner = bridge_role_runner();
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .join("mission-types");
    let transports = MissionTransports::external(
        profiles(temp.path()),
        RuntimeDriverRegistry::new([Arc::new(FakeDriver {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::new([Arc::new(FakeAuth {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(MockOracleRunner::exiting(0)),
    )
    .with_attached_runtime(attached)
    .with_everyday_runtime_root(runtime.path().to_path_buf())
    .with_role_transport(role_runner, Arc::new(NoopEffectCleaner))
    .with_mission_types_root(source_root)
    .with_in_process_operator_bridge();

    let code = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .expect("attached bridge lifecycle succeeds");

    assert_eq!(code, std::process::ExitCode::SUCCESS);
    let mission_id = mission_id.lock().unwrap().clone().unwrap();
    let state = MissionStore::open(temp.path())
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    assert!(state.is_terminal());
    assert!(state.applied_result.is_some());
    let observations = observations.lock().unwrap();
    assert_eq!(observations.context.len(), 2);
    assert!(observations.context[0].contains("\"mission\": null"));
    assert!(observations.context[1].contains(mission_id.as_str()));
    assert!(observations.context[1].contains("\"kind\": \"finish\""));
    let commands = commands.lock().unwrap();
    for command in [
        "start", "propose", "decide", "advance", "finish", "report", "apply",
    ] {
        assert!(
            commands
                .iter()
                .any(|args| args.iter().any(|argument| argument == command)),
            "missing bridge command {command}: {commands:?}"
        );
    }
}

#[tokio::test]
async fn attached_runtime_authors_and_executes_child_mission_through_everyday_run() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let commands = Arc::new(Mutex::new(Vec::new()));
    let mission_id = Arc::new(Mutex::new(None));
    let attached = Arc::new(BridgeAttached::new(
        Arc::clone(&observations),
        software_dev_child_proposal_json(),
        Arc::clone(&commands),
        Arc::clone(&mission_id),
    ));
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .join("mission-types");
    let transports = MissionTransports::external(
        profiles(temp.path()),
        RuntimeDriverRegistry::new([Arc::new(FakeDriver {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::new([Arc::new(FakeAuth {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(MockOracleRunner::exiting(0)),
    )
    .with_attached_runtime(attached)
    .with_everyday_runtime_root(runtime.path().to_path_buf())
    .with_role_transport(bridge_role_runner(), Arc::new(NoopEffectCleaner))
    .with_mission_types_root(source_root)
    .with_in_process_operator_bridge();

    let code = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .expect("everyday child mission lifecycle succeeds");

    assert_eq!(code, std::process::ExitCode::SUCCESS);
    let parent_id = mission_id.lock().unwrap().clone().unwrap();
    let store = MissionStore::open(temp.path()).await.unwrap();
    let parent = store.require_state(&parent_id).await.unwrap();
    assert!(parent.is_terminal());
    assert_eq!(parent.child_mission_receipts.len(), 1);
    assert_eq!(parent.cleaned_child_missions.len(), 1);
    let child_id = parent
        .child_mission_receipts
        .values()
        .next()
        .unwrap()
        .child_mission_id
        .clone();
    let child = store.require_state(&child_id).await.unwrap();
    assert!(child.is_terminal());
    assert_eq!(child.lineage.as_ref().unwrap().parent_mission_id, parent_id);
    assert_eq!(store.list_missions().await.unwrap().len(), 2);
    let commands = commands.lock().unwrap();
    assert!(commands
        .iter()
        .any(|args| args.iter().any(|argument| argument == "propose")));
    assert!(commands
        .iter()
        .any(|args| args.iter().any(|argument| argument == "advance")));
}

#[tokio::test]
async fn ordinary_runtime_failure_is_not_retried_or_allowed_to_append_a_decision() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Keep the mission parked",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let store = MissionStore::open(temp.path()).await.unwrap();
    let before = store.load(&mission_id).await.unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));

    let code = cli::run_with_transports(
        run_cli(temp.path()),
        transports(
            temp.path(),
            runtime.path(),
            Arc::clone(&observations),
            [output(17), output(0)],
        ),
    )
    .await
    .expect("runtime failure is a reported outcome");

    assert_eq!(code, std::process::ExitCode::FAILURE);
    assert_eq!(observations.lock().unwrap().requests.len(), 1);
    assert_eq!(store.load(&mission_id).await.unwrap(), before);
}

#[tokio::test]
async fn runtime_signal_retries_are_bounded_and_cannot_append_a_decision() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Keep the mission parked",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let store = MissionStore::open(temp.path()).await.unwrap();
    let before = store.load(&mission_id).await.unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));

    let code = cli::run_with_transports(
        run_cli(temp.path()),
        transports(
            temp.path(),
            runtime.path(),
            Arc::clone(&observations),
            [signal(9), signal(9), signal(9), output(0)],
        ),
    )
    .await
    .expect("runtime crash is a reported outcome");

    assert_eq!(code, std::process::ExitCode::FAILURE);
    assert_eq!(observations.lock().unwrap().requests.len(), 3);
    assert_eq!(store.load(&mission_id).await.unwrap(), before);
}

#[tokio::test]
async fn restart_resumes_native_conversation_and_releases_a_settled_mission() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Observe current truth",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0), output(0)],
    );

    let first = cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    assert_eq!(first, std::process::ExitCode::from(NONTERMINAL_EXIT));
    harness
        .engine
        .abort(&mission_id, "test transition")
        .await
        .unwrap();
    let second = cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    assert_eq!(second, std::process::ExitCode::from(NONTERMINAL_EXIT));
    let next_mission = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Observe later truth",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let third = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();
    assert_eq!(third, std::process::ExitCode::from(NONTERMINAL_EXIT));

    let observations = observations.lock().unwrap();
    assert_eq!(observations.terminal_inputs.len(), 3);
    assert!(!observations.terminal_inputs[0].resume);
    assert!(observations.terminal_inputs[1].resume);
    assert!(observations.terminal_inputs[2].resume);
    assert!(observations.context[0].contains("\"kind\": \"propose_plan\""));
    assert!(observations.context[1].contains("\"mission\": null"));
    assert!(!observations.context[1].contains("\"kind\": \"aborted\""));
    assert!(observations.context[2].contains(next_mission.as_str()));
    assert!(observations.context[2].contains("\"kind\": \"propose_plan\""));
}

#[tokio::test]
async fn durable_binding_preserves_one_exact_live_mission() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let current = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Keep this mission current",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0)],
    );
    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    let other = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "A separately created mission",
            common::BASE_SHA,
        )
        .await
        .unwrap();

    cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();
    assert_eq!(
        cli::run_with_transports(guide_cli(temp.path()), MissionTransports::production())
            .await
            .unwrap(),
        std::process::ExitCode::SUCCESS
    );

    let observations = observations.lock().unwrap();
    assert!(observations.context[1].contains(current.as_str()));
    assert!(!observations.context[1].contains(other.as_str()));
    assert!(!observations.context[1].contains("\"kind\": \"ambiguous\""));
}

#[tokio::test]
async fn completed_binding_is_preserved_through_apply_then_released() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        common::review_runner(Vec::new()),
        MockOracleRunner::exiting(0),
    )
    .await;
    let current = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Finish and apply this mission",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0), output(0)],
    );
    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&current, common::proposal(0, common::simple_plan()))
        .await
        .unwrap();
    common::approve_plan(&harness.engine, &current).await;
    common::advance_to_finished(&harness.engine, &current).await;
    let later = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Wait until the current mission is applied",
            common::BASE_SHA,
        )
        .await
        .unwrap();

    let completed = cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    assert_eq!(completed, std::process::ExitCode::SUCCESS);
    assert_eq!(
        cli::run_with_transports(apply_cli(temp.path()), MissionTransports::production())
            .await
            .unwrap(),
        std::process::ExitCode::SUCCESS
    );
    let released = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();
    assert_eq!(released, std::process::ExitCode::from(NONTERMINAL_EXIT));

    let observations = observations.lock().unwrap();
    assert!(observations.context[1].contains(current.as_str()));
    assert!(observations.context[1].contains("\"kind\": \"apply\""));
    assert!(!observations.context[1].contains(later.as_str()));
    assert!(observations.context[2].contains(later.as_str()));
    assert!(!observations.context[2].contains(current.as_str()));
}

#[tokio::test]
async fn new_selection_releases_an_unapplied_result_without_destroying_it() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        common::review_runner(Vec::new()),
        MockOracleRunner::exiting(0),
    )
    .await;
    let completed = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Finish this mission but retain its unapplied result",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0)],
    );
    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&completed, common::proposal(0, common::simple_plan()))
        .await
        .unwrap();
    common::approve_plan(&harness.engine, &completed).await;
    common::advance_to_finished(&harness.engine, &completed).await;
    let later = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Start later everyday work",
            common::BASE_SHA,
        )
        .await
        .unwrap();

    let code = cli::run_with_transports(new_run_cli(temp.path()), transports)
        .await
        .unwrap();

    assert_eq!(code, std::process::ExitCode::from(NONTERMINAL_EXIT));
    let completed_state = lionclaw::model::fold(
        MissionStore::open(temp.path())
            .await
            .unwrap()
            .load(&completed)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(completed_state.is_terminal());
    assert!(lionclaw::model::next(&completed_state)
        .choices
        .iter()
        .any(|choice| matches!(choice, lionclaw::model::Choice::Apply { .. })));
    let observations = observations.lock().unwrap();
    assert!(observations.context[1].contains(later.as_str()));
    assert!(!observations.context[1].contains(completed.as_str()));
}

#[tokio::test]
async fn corrupt_current_mission_binding_fails_before_runtime_execution() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0)],
    );
    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    std::fs::write(
        temp.path().join(".lionclaw/everyday/current-mission"),
        b"not-a-mission",
    )
    .unwrap();

    let error = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .expect_err("corrupt binding must fail closed");

    assert!(
        format!("{error:#}").contains("current everyday mission identity is invalid"),
        "got {error:#}"
    );
    assert_eq!(observations.lock().unwrap().requests.len(), 1);
}

#[tokio::test]
async fn runtime_state_inside_the_judged_repository_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let runtime_root = temp.path().join(".runtime-state");

    let error = cli::run_with_transports(
        run_cli(temp.path()),
        transports(
            temp.path(),
            &runtime_root,
            Arc::clone(&observations),
            [output(0)],
        ),
    )
    .await
    .expect_err("writable runtime state must remain outside the judged tree");

    assert!(
        format!("{error:#}").contains("overlaps judged root"),
        "got {error:#}"
    );
    assert!(!runtime_root.exists());
    let observations = observations.lock().unwrap();
    assert_eq!(observations.auth_preparations, 0);
    assert!(observations.requests.is_empty());
}

#[tokio::test]
async fn mutable_image_tag_is_resolved_before_native_resume_and_execution() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mutable_ref = "localhost/lionclaw-runtime-dev:v1";
    let transports = transports_with_profiles(
        profiles_with_image(temp.path(), mutable_ref),
        Arc::clone(&observations),
        [output(0), output(0)],
        [IMAGE_A.to_string(), IMAGE_B.to_string()],
        runtime.path(),
    );

    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();

    let observations = observations.lock().unwrap();
    assert_eq!(
        observations.image_resolutions,
        [
            (
                "podman".to_string(),
                mutable_ref.to_string(),
                IMAGE_A.to_string()
            ),
            (
                "podman".to_string(),
                mutable_ref.to_string(),
                IMAGE_B.to_string()
            ),
        ]
    );
    assert_eq!(
        observations.requests[0]
            .plan
            .confinement
            .oci()
            .image
            .as_deref(),
        Some(IMAGE_A)
    );
    assert_eq!(
        observations.requests[1]
            .plan
            .confinement
            .oci()
            .image
            .as_deref(),
        Some(IMAGE_B)
    );
    assert!(!observations.terminal_inputs[0].resume);
    assert!(!observations.terminal_inputs[1].resume);
    assert_ne!(
        observations.terminal_inputs[0].session_id,
        observations.terminal_inputs[1].session_id
    );
}
