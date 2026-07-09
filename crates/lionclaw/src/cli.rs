//! The mission CLI surface. Each command is a per-mission stdio run: open
//! the store, fold, act, park or exit. Host-as-orchestrator: the human's
//! agent session invokes these as tools and reads `--json` output.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};

use crate::authority::AuthorityCeiling;
use crate::config::MissionRuntimeProfile;
use crate::engine::{AdvanceOutcome, Engine};
use crate::mission_type::{bundled_mission_types_dir, load_mission_type, Home};
use crate::model::{fold, MissionConfig, MissionId, MissionPhase};
use crate::oracle::OciOracleRunner;
use crate::ports::{Clock, SystemClock};
use crate::runner::OciRoleRunner;
use crate::store::MissionStore;
use crate::workspace;

#[derive(Parser)]
#[command(name = "lionclaw", about = "LionClaw mission engine")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Install the bundled mission types into `~/.lionclaw` (run once).
    Install(InstallArgs),
    /// Check the install: podman, git, and each installed mission type + image.
    Doctor,
    /// Mission engine commands.
    #[command(subcommand)]
    Mission(MissionCommand),
}

#[derive(Args)]
pub struct InstallArgs {
    /// Source directory of mission types to install (defaults to the ones
    /// bundled with this binary).
    #[arg(long)]
    pub from: Option<PathBuf>,
    /// Overwrite mission types already installed.
    #[arg(long)]
    pub force: bool,
}

#[derive(Subcommand)]
pub enum MissionCommand {
    /// Create a mission over a target repo and submit no plan yet.
    Start(StartArgs),
    /// Submit a plan (contract + task DAG) from a JSON file.
    SubmitPlan(SubmitPlanArgs),
    /// Amend a running mission's plan (add/supersede/cancel tasks, strengthen
    /// the contract) from an ops JSON file.
    Amend(AmendArgs),
    /// Drive a mission until it parks, finishes, or awaits input.
    Advance(AdvanceArgs),
    /// Show a mission's state (contract, phase, finish grade).
    Status(StatusArgs),
    /// Print a mission's event log.
    Log(LogArgs),
    /// List missions parked on open attention (durable interrupts).
    Inbox(InboxArgs),
    /// Approve the plan at the ratification gate.
    Ratify(RatifyArgs),
    /// Resolve an open attention item.
    Decide(DecideArgs),
    /// Inspect installed mission types.
    #[command(subcommand)]
    Type(TypeCommand),
    /// Drive the real stack end-to-end and assert the Slice-1 invariants
    /// (needs podman; model-auth-free).
    SelfTest(SelfTestArgs),
}

#[derive(Subcommand)]
pub enum TypeCommand {
    /// List installed mission types.
    List(TypeListArgs),
    /// Show one installed mission type (roles, oracles, stop bar, playbook).
    Show(TypeShowArgs),
    /// Validate a mission type directory (loader + moat) without installing it.
    Check(TypeCheckArgs),
}

#[derive(Args)]
pub struct TypeListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct TypeShowArgs {
    /// Installed mission type name.
    pub name: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct TypeCheckArgs {
    /// Mission type directory to validate.
    pub dir: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct StartArgs {
    /// Installed mission type name (see `mission type list`).
    #[arg(long = "type")]
    pub mission_type: String,
    /// Target repository the mission operates on.
    #[arg(long)]
    pub repo: PathBuf,
    /// The mission objective.
    #[arg(long)]
    pub objective: String,
    #[arg(long, default_value = "codex")]
    pub runtime: String,
    /// Skip the default-on ratification gate (auto-approve the plan).
    #[arg(long)]
    pub yes: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct InboxArgs {
    #[arg(long)]
    pub repo: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct RatifyArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
    #[arg(long, default_value = "approved")]
    pub justification: String,
}

#[derive(Args)]
pub struct DecideArgs {
    pub mission_id: String,
    /// The attention item id (see `mission status`/`inbox`).
    pub item: String,
    /// One of: ratify | retry | continue | abort.
    pub action: String,
    #[arg(long)]
    pub repo: PathBuf,
    #[arg(long, default_value = "")]
    pub justification: String,
}

#[derive(Args)]
pub struct SubmitPlanArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
    /// Plan JSON file ({ "assertions": [...], "tasks": [...] }).
    #[arg(long)]
    pub plan: PathBuf,
}

#[derive(Args)]
pub struct AmendArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
    /// Amendment ops JSON ({ "add": [...], "supersede": [...], "cancel": [...],
    /// "add_assertion": [...], "bind_oracle": [...] }).
    #[arg(long)]
    pub ops: PathBuf,
    /// The plan revision this amendment was authored against (see `status`).
    #[arg(long)]
    pub base_revision: u32,
    #[arg(long, default_value = "orchestrator")]
    pub actor: String,
    #[arg(long, default_value = "")]
    pub justification: String,
}

#[derive(Args)]
pub struct AdvanceArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct StatusArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct LogArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
}

#[derive(Args)]
pub struct SelfTestArgs {
    #[arg(long)]
    pub json: bool,
}

/// Run a command, returning the process exit code (so callers can gate on
/// e.g. `type check` without the command itself calling `process::exit`).
pub async fn run(cli: Cli) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cli.command {
        Command::Install(args) => cmd_install(args).await.map(|()| ExitCode::SUCCESS),
        Command::Doctor => cmd_doctor().await,
        Command::Mission(cmd) => run_mission(cmd).await,
    }
}

async fn run_mission(cmd: MissionCommand) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cmd {
        MissionCommand::Start(args) => cmd_start(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::SubmitPlan(args) => cmd_submit_plan(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Amend(args) => cmd_amend(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Advance(args) => cmd_advance(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Status(args) => cmd_status(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Log(args) => cmd_log(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Inbox(args) => cmd_inbox(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Ratify(args) => cmd_ratify(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Decide(args) => cmd_decide(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Type(cmd) => cmd_type(cmd).await,
        MissionCommand::SelfTest(args) => crate::selftest::run(args.json).await,
    }
}

fn runtime_profile(runtime: &str) -> Result<MissionRuntimeProfile> {
    match runtime {
        "codex" => Ok(MissionRuntimeProfile::codex_default()),
        other => bail!("unknown runtime '{other}' (only 'codex' is wired)"),
    }
}

/// Build an engine over an open store, a loaded mission type, and a runtime
/// profile (whose image the caller has already pinned).
#[allow(clippy::too_many_arguments)]
fn assemble_engine(
    store: MissionStore,
    repo: &Path,
    mission_type: crate::mission_type::MissionType,
    runtime: String,
    image_id: String,
    profile: MissionRuntimeProfile,
    ceiling: AuthorityCeiling,
) -> Result<Engine> {
    workspace::ensure_excluded(repo)?;
    let role_runner = Arc::new(OciRoleRunner::new(profile.clone(), ceiling));
    let oracle_runner = Arc::new(OciOracleRunner::new(profile));
    Ok(Engine::new(
        store,
        mission_type,
        runtime,
        image_id,
        role_runner,
        oracle_runner,
        Arc::new(SystemClock),
    ))
}

/// Open an engine to CREATE a mission from an installed mission type (by name).
/// The confinement image is resolved to a content id here — once, at start — so
/// a later rebuild of the tag cannot silently change the instrument. The engine
/// carries the runtime + image id it records on `MissionCreated`.
async fn open_engine_for_start(repo: &Path, type_name: &str, runtime: &str) -> Result<Engine> {
    let ceiling = AuthorityCeiling::default();
    let type_dir = Home::from_env()?.mission_type_dir(type_name);
    let mission_type = load_mission_type(&type_dir, &ceiling)
        .with_context(|| format!("mission type '{type_name}' (run `lionclaw install`?)"))?;
    let mut profile = runtime_profile(runtime)?;
    let engine = profile.confinement.oci().engine.clone();
    let image_id = lionclaw_confinement::resolve_oci_image_compatibility_identity(
        &engine,
        &mission_type.image,
    )
    .await
    .with_context(|| format!("resolving image '{}'", mission_type.image))?;
    profile.confinement.oci_mut().image = Some(image_id.clone());
    let store = MissionStore::open(repo).await?;
    let engine = assemble_engine(
        store,
        repo,
        mission_type,
        runtime.to_string(),
        image_id.clone(),
        profile,
        ceiling,
    )?;
    Ok(engine)
}

/// Open an engine for an EXISTING mission: resolve its recorded mission type (by
/// name, from the home), runtime, and pinned image id — so no `--type`/`--runtime`
/// is needed. `load_state` then verifies the pinned digest, fail-closed.
async fn open_engine_for_mission(repo: &Path, mission_id: &MissionId) -> Result<Engine> {
    let store = MissionStore::open(repo).await?;
    let state = store
        .load_state_snapshotted(mission_id)
        .await?
        .with_context(|| format!("mission {mission_id} not found"))?;
    let ceiling = AuthorityCeiling::default();
    let type_dir = Home::from_env()?.mission_type_dir(&state.mission_type.name);
    let mission_type = load_mission_type(&type_dir, &ceiling)
        .with_context(|| format!("mission type '{}'", state.mission_type.name))?;
    let mut profile = runtime_profile(&state.runtime)?;
    profile.confinement.oci_mut().image = Some(state.image_id.clone());
    let engine = assemble_engine(
        store,
        repo,
        mission_type,
        state.runtime.clone(),
        state.image_id.clone(),
        profile,
        ceiling,
    )?;
    // Verify the pinned mission-type digest before anything runs.
    engine.load_state(mission_id).await?;
    Ok(engine)
}

async fn cmd_start(args: StartArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    // Fail-closed: loading the mission type (and its moat check) happens before
    // any event is written.
    let engine = open_engine_for_start(&repo, &args.mission_type, &args.runtime).await?;
    let base_sha = workspace::head_sha(&repo).await?;
    let mission_id = engine
        .create_mission(
            &repo.to_string_lossy(),
            &args.objective,
            &base_sha,
            MissionConfig {
                // Default-on ratification gate; `--yes` auto-approves.
                ratification_gate: !args.yes,
                ..Default::default()
            },
        )
        .await?;
    if args.json {
        println!(
            "{}",
            serde_json::json!({ "mission_id": mission_id.as_str(), "base_sha": base_sha })
        );
    } else {
        println!("started mission {mission_id} at {base_sha}");
        println!(
            "submit a plan, then: lionclaw mission advance {mission_id} --repo {}",
            repo.display(),
        );
    }
    Ok(())
}

async fn cmd_inbox(args: InboxArgs) -> Result<()> {
    let store = MissionStore::open(&args.repo.canonicalize().context("repo path")?).await?;
    let mut parked = Vec::new();
    for summary in store.list_missions().await? {
        let events = store.load(&summary.mission_id).await?;
        let Some(state) = fold(events) else { continue };
        if !state.open_attention.is_empty() {
            parked.push((summary.mission_id, state));
        }
    }
    if args.json {
        let items: Vec<_> = parked
            .iter()
            .map(|(id, state)| {
                serde_json::json!({
                    "mission_id": id.as_str(),
                    "objective": state.objective,
                    "attention": state.open_attention.values().map(|a| {
                        serde_json::json!({ "id": a.id, "kind": format!("{:?}", a.kind).to_lowercase(), "report": a.report })
                    }).collect::<Vec<_>>(),
                })
            })
            .collect();
        println!("{}", serde_json::json!({ "parked": items }));
    } else if parked.is_empty() {
        println!("inbox empty: no missions awaiting attention");
    } else {
        for (id, state) in &parked {
            println!("{id}: {}", state.objective);
            for item in state.open_attention.values() {
                println!("  [{}] {}", item.id, item.report);
            }
        }
    }
    Ok(())
}

async fn cmd_ratify(args: RatifyArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    let store = MissionStore::open(&repo).await?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    // The ratification item id is stable ("ratify:mission").
    crate::engine::record_decision(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        "ratify:mission",
        crate::model::DecisionAction::Ratify,
        &args.justification,
        "cli",
    )
    .await?;
    println!("ratified mission {mission_id}");
    Ok(())
}

async fn cmd_decide(args: DecideArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    let store = MissionStore::open(&repo).await?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    let action = match args.action.as_str() {
        "ratify" => crate::model::DecisionAction::Ratify,
        "retry" => crate::model::DecisionAction::Retry,
        "continue" => crate::model::DecisionAction::Continue,
        "abort" => crate::model::DecisionAction::Abort,
        other => bail!("unknown action '{other}' (ratify|retry|continue|abort)"),
    };
    crate::engine::record_decision(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        &args.item,
        action,
        &args.justification,
        "cli",
    )
    .await?;
    println!(
        "recorded decision on '{}' for mission {mission_id}",
        args.item
    );
    Ok(())
}

async fn cmd_submit_plan(args: SubmitPlanArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    let engine = open_engine_for_mission(&repo, &mission_id).await?;
    let plan_text = std::fs::read_to_string(&args.plan)
        .with_context(|| format!("failed to read plan '{}'", args.plan.display()))?;
    let submission = serde_json::from_str(&plan_text).context("plan JSON is invalid")?;
    engine
        .submit_plan(&mission_id, submission)
        .await
        .context("plan rejected")?;
    println!("plan accepted for mission {mission_id}");
    Ok(())
}

async fn cmd_amend(args: AmendArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    let engine = open_engine_for_mission(&repo, &mission_id).await?;
    let ops_text = std::fs::read_to_string(&args.ops)
        .with_context(|| format!("failed to read ops '{}'", args.ops.display()))?;
    let ops = serde_json::from_str(&ops_text).context("amendment ops JSON is invalid")?;
    engine
        .amend_plan(
            &mission_id,
            ops,
            &args.actor,
            &args.justification,
            args.base_revision,
        )
        .await
        .context("amendment rejected")?;
    println!("amendment accepted for mission {mission_id}");
    Ok(())
}

async fn cmd_advance(args: AdvanceArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    let engine = open_engine_for_mission(&repo, &mission_id).await?;
    let outcome = engine.advance(&mission_id).await?;
    let state = engine.load_state(&mission_id).await?;
    report_state(
        &args.mission_id,
        &state.phase,
        &outcome,
        args.json,
        &engine,
        &mission_id,
    )
    .await
}

async fn cmd_status(args: StatusArgs) -> Result<()> {
    let store = MissionStore::open(&args.repo.canonicalize().context("repo path")?).await?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    let events = store.load(&mission_id).await?;
    let state = fold(events).with_context(|| format!("mission {mission_id} not found"))?;
    if args.json {
        let finish = match &state.phase {
            MissionPhase::Done { finish } => Some(format!("{finish:?}").to_lowercase()),
            _ => None,
        };
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id.as_str(),
                "phase": phase_slug(&state.phase),
                "finish": finish,
                "revision": state.revision,
                "current_sha": state.current_sha,
                "objective": state.objective,
                "contract": state.contract.iter().map(|(id, a)| {
                    serde_json::json!({
                        "id": id.as_str(),
                        "advisory": format!("{:?}", a.advisory).to_lowercase(),
                        "authoritative_pass": a.last_authoritative.as_ref().map(|v| v.passed()),
                    })
                }).collect::<Vec<_>>(),
            })
        );
    } else {
        println!(
            "mission {mission_id}: {} (revision {})",
            phase_slug(&state.phase),
            state.revision
        );
        println!("objective: {}", state.objective);
        for (id, assertion) in &state.contract {
            let auth = assertion
                .last_authoritative
                .as_ref()
                .map(|v| if v.passed() { "pass" } else { "fail" })
                .unwrap_or("—");
            println!("  {id}: authoritative={auth}");
        }
    }
    Ok(())
}

async fn cmd_log(args: LogArgs) -> Result<()> {
    let store = MissionStore::open(&args.repo.canonicalize().context("repo path")?).await?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    for envelope in store.load(&mission_id).await? {
        println!(
            "{:>4} {}",
            envelope.sequence_no,
            envelope.event.event_type()
        );
    }
    Ok(())
}

async fn cmd_install(args: InstallArgs) -> Result<()> {
    let home = Home::from_env()?;
    let source = match args.from {
        Some(dir) => dir,
        None => bundled_mission_types_dir()?,
    };
    let dest_dir = home.mission_types_dir();
    std::fs::create_dir_all(&dest_dir)
        .with_context(|| format!("creating '{}'", dest_dir.display()))?;

    let mut installed = Vec::new();
    for entry in
        std::fs::read_dir(&source).with_context(|| format!("reading '{}'", source.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let dest = dest_dir.join(&name);
        if dest.exists() {
            if !args.force {
                println!(
                    "skip {} (already installed; --force to overwrite)",
                    name.to_string_lossy()
                );
                continue;
            }
            std::fs::remove_dir_all(&dest)
                .with_context(|| format!("removing '{}'", dest.display()))?;
        }
        // Validate before installing: an invalid mission type never lands.
        load_mission_type(&entry.path(), &AuthorityCeiling::default())
            .with_context(|| format!("mission type '{}' is invalid", name.to_string_lossy()))?;
        copy_tree(&entry.path(), &dest)?;
        println!("installed {}", name.to_string_lossy());
        installed.push(name.to_string_lossy().into_owned());
    }
    if installed.is_empty() {
        println!("nothing to install (all mission types already present)");
    }
    println!("home: {}", home.root().display());
    Ok(())
}

/// Recursively copy a directory tree. `std::fs::copy` preserves Unix mode bits,
/// so oracle executables stay executable.
fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst).with_context(|| format!("creating '{}'", dst.display()))?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&from, &to)?;
        } else {
            std::fs::copy(&from, &to).with_context(|| format!("copying '{}'", from.display()))?;
        }
    }
    Ok(())
}

async fn cmd_doctor() -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    let mut ok = true;
    let mut check = |label: &str, pass: bool, detail: &str| {
        println!(
            "{} {label}{}",
            if pass { "PASS" } else { "FAIL" },
            if detail.is_empty() {
                String::new()
            } else {
                format!(" — {detail}")
            }
        );
        ok &= pass;
    };

    check("podman", command_ok("podman", &["--version"]).await, "");
    check("git", command_ok("git", &["--version"]).await, "");

    let home = Home::from_env()?;
    let types = home.installed_mission_types()?;
    if types.is_empty() {
        check(
            "mission types installed",
            false,
            "none — run `lionclaw install`",
        );
    }
    for name in &types {
        match load_mission_type(&home.mission_type_dir(name), &AuthorityCeiling::default()) {
            Ok(mt) => {
                let img = command_ok("podman", &["image", "exists", &mt.image]).await;
                check(
                    &format!("mission type '{name}'"),
                    img,
                    &if img {
                        String::new()
                    } else {
                        format!("image '{}' not present", mt.image)
                    },
                );
            }
            Err(e) => check(&format!("mission type '{name}'"), false, &e.to_string()),
        }
    }
    Ok(if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

async fn command_ok(program: &str, args: &[&str]) -> bool {
    tokio::process::Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn cmd_type(cmd: TypeCommand) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cmd {
        TypeCommand::List(args) => {
            let home = Home::from_env()?;
            let types = home.installed_mission_types()?;
            if args.json {
                println!("{}", serde_json::json!({ "mission_types": types }));
            } else if types.is_empty() {
                println!("no mission types installed (run `lionclaw install`)");
            } else {
                for name in types {
                    println!("{name}");
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        TypeCommand::Show(args) => {
            let home = Home::from_env()?;
            let dir = home.mission_type_dir(&args.name);
            show_mission_type(&dir, args.json)
        }
        TypeCommand::Check(args) => show_mission_type(&args.dir, args.json),
    }
}

/// Load a mission type from `dir` and print it; nonzero exit if it fails to
/// load (so a caller can gate on validity).
fn show_mission_type(dir: &Path, json: bool) -> Result<std::process::ExitCode> {
    match load_mission_type(dir, &AuthorityCeiling::default()) {
        Ok(mt) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "name": mt.name,
                        "stop": format!("{:?}", mt.stop).to_lowercase(),
                        "image": mt.image,
                        "roles": mt.roles.keys().map(|r| r.as_str()).collect::<Vec<_>>(),
                        "oracles": mt.oracles.keys().map(|o| o.as_str()).collect::<Vec<_>>(),
                        "playbook": mt.playbook,
                    })
                );
            } else {
                println!("mission type '{}' is valid", mt.name);
                println!("  stop:  {:?}", mt.stop);
                println!("  image: {}", mt.image);
                println!(
                    "  roles: {}",
                    mt.roles
                        .keys()
                        .map(|r| r.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                println!(
                    "  oracles: {}",
                    mt.oracles
                        .keys()
                        .map(|o| o.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if let Some(playbook) = &mt.playbook {
                    println!("\n--- playbook ---\n{playbook}");
                }
            }
            Ok(std::process::ExitCode::SUCCESS)
        }
        Err(err) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({ "ok": false, "error": err.to_string() })
                );
            } else {
                eprintln!("mission type invalid: {err}");
            }
            Ok(std::process::ExitCode::FAILURE)
        }
    }
}

async fn report_state(
    mission_id: &str,
    phase: &MissionPhase,
    outcome: &AdvanceOutcome,
    json: bool,
    engine: &Engine,
    id: &MissionId,
) -> Result<()> {
    let _ = engine;
    let _ = id;
    let finish = match phase {
        MissionPhase::Done { finish } => Some(format!("{finish:?}").to_lowercase()),
        _ => None,
    };
    if json {
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id,
                "phase": phase_slug(phase),
                "finish": finish,
                "outcome": outcome_slug(outcome),
            })
        );
    } else {
        match outcome {
            AdvanceOutcome::AwaitingPlan => println!("mission {mission_id}: awaiting a plan"),
            AdvanceOutcome::Parked { attention } => {
                println!(
                    "mission {mission_id}: parked ({} attention item(s))",
                    attention.len()
                );
                for item in attention {
                    println!("  [{}] {}", item.id, item.report);
                }
            }
            AdvanceOutcome::Busy => {
                println!("mission {mission_id}: effects in progress under another driver")
            }
            AdvanceOutcome::Terminal { phase } => {
                println!("mission {mission_id}: {}", phase_slug(phase));
            }
        }
    }
    Ok(())
}

fn phase_slug(phase: &MissionPhase) -> String {
    match phase {
        MissionPhase::Planning => "planning".to_string(),
        MissionPhase::Running => "running".to_string(),
        MissionPhase::AttentionNeeded => "attention_needed".to_string(),
        MissionPhase::Done { finish } => format!("done:{}", format!("{finish:?}").to_lowercase()),
        MissionPhase::Aborted { .. } => "aborted".to_string(),
    }
}

fn outcome_slug(outcome: &AdvanceOutcome) -> &'static str {
    match outcome {
        AdvanceOutcome::AwaitingPlan => "awaiting_plan",
        AdvanceOutcome::Parked { .. } => "parked",
        AdvanceOutcome::Busy => "busy",
        AdvanceOutcome::Terminal { .. } => "terminal",
    }
}
