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
use crate::model::{
    fold, short_hex, AttentionKind, EventEnvelope, FinishClass, MissionConfig, MissionId,
    MissionPhase,
};
use crate::oracle::OciOracleRunner;
use crate::ports::{Clock, EventSink, SystemClock};
use crate::runner::OciRoleRunner;
use crate::store::MissionStore;
use crate::workspace;

/// Streams committed events to stderr so a long `advance` is not silent. Stderr,
/// not stdout, so `--json` consumers reading stdout are unaffected.
struct StderrEventSink;

impl EventSink for StderrEventSink {
    fn emit(&self, event: &EventEnvelope) {
        eprintln!("  · {:>4}  {}", event.sequence_no, event.event.event_type());
    }
}

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
    /// The verifiable receipt: what was proven, by what, and what was NOT.
    Report(ReportArgs),
    /// Create a branch (`lionclaw/<id>`) at the mission's produced commit.
    Apply(ApplyArgs),
    /// Print a mission's event log.
    Log(LogArgs),
    /// List missions parked on open attention (durable interrupts).
    Inbox(InboxArgs),
    /// Approve the plan at the ratification gate.
    Ratify(RatifyArgs),
    /// Show the proposed contract awaiting ratification (assertion→oracle
    /// bindings and the verified/reviewed ceiling).
    Plan(PlanArgs),
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
    /// Target repository (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
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
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct RatifyArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long, default_value = "approved")]
    pub justification: String,
}

#[derive(Args)]
pub struct PlanArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct DecideArgs {
    pub mission_id: String,
    /// The attention item id (see `mission status`/`inbox`).
    pub item: String,
    /// One of: ratify | retry | continue | abort.
    pub action: String,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long, default_value = "")]
    pub justification: String,
}

#[derive(Args)]
pub struct SubmitPlanArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Plan JSON file ({ "assertions": [...], "tasks": [...] }); `-` reads stdin.
    #[arg(long)]
    pub plan: PathBuf,
}

#[derive(Args)]
pub struct AmendArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Amendment ops JSON ({ "add": [...], "supersede": [...], "cancel": [...],
    /// "add_assertion": [...], "bind_oracle": [...] }); `-` reads stdin.
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
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct StatusArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct ReportArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Include the base..head diff (text output only).
    #[arg(long)]
    pub patch: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct ApplyArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
    /// Overwrite the branch if it already exists.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args)]
pub struct LogArgs {
    /// Mission id (default: the sole live mission in this repo).
    pub mission_id: Option<String>,
    /// Target repo (default: the enclosing git worktree root).
    #[arg(long)]
    pub repo: Option<PathBuf>,
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
    // One error policy for every command: a `--json` command reports failure as
    // a structured `{"ok":false,"error":…}` envelope on stdout and exits 1; a
    // human command lets the error bubble to stderr. Success output is each
    // command's own concern.
    let json = cmd.is_json();
    match dispatch_mission(cmd).await {
        Ok(code) => Ok(code),
        Err(err) if json => {
            println!(
                "{}",
                serde_json::json!({ "ok": false, "error": err.to_string() })
            );
            Ok(std::process::ExitCode::FAILURE)
        }
        Err(err) => Err(err),
    }
}

impl MissionCommand {
    /// Whether this invocation asked for machine output (`--json`).
    fn is_json(&self) -> bool {
        match self {
            Self::Start(a) => a.json,
            Self::Status(a) => a.json,
            Self::Report(a) => a.json,
            Self::Plan(a) => a.json,
            Self::Inbox(a) => a.json,
            Self::Advance(a) => a.json,
            Self::SelfTest(a) => a.json,
            Self::Type(t) => t.is_json(),
            Self::SubmitPlan(_)
            | Self::Amend(_)
            | Self::Apply(_)
            | Self::Log(_)
            | Self::Ratify(_)
            | Self::Decide(_) => false,
        }
    }
}

impl TypeCommand {
    fn is_json(&self) -> bool {
        match self {
            Self::List(a) => a.json,
            Self::Show(a) => a.json,
            Self::Check(a) => a.json,
        }
    }
}

async fn dispatch_mission(cmd: MissionCommand) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cmd {
        MissionCommand::Start(args) => cmd_start(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::SubmitPlan(args) => cmd_submit_plan(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Amend(args) => cmd_amend(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Advance(args) => cmd_advance(args).await,
        MissionCommand::Status(args) => cmd_status(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Report(args) => cmd_report(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Apply(args) => cmd_apply(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Log(args) => cmd_log(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Inbox(args) => cmd_inbox(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Ratify(args) => cmd_ratify(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Plan(args) => cmd_plan(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Decide(args) => cmd_decide(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Type(cmd) => cmd_type(cmd).await,
        MissionCommand::SelfTest(args) => crate::selftest::run(args.json).await,
    }
}

fn runtime_profile(runtime: &str) -> Result<MissionRuntimeProfile> {
    match runtime {
        "codex" => Ok(MissionRuntimeProfile::codex_default()),
        "opencode" => Ok(MissionRuntimeProfile::opencode_default()),
        other => bail!("unknown runtime '{other}' (expected 'codex' or 'opencode')"),
    }
}

/// Build an engine over an open store, a loaded mission type, and a runtime
/// profile (whose image the caller has already pinned).
#[allow(clippy::too_many_arguments)]
async fn assemble_engine(
    store: MissionStore,
    repo: &Path,
    mission_type: crate::mission_type::MissionType,
    runtime: String,
    image_id: String,
    profile: MissionRuntimeProfile,
    ceiling: AuthorityCeiling,
) -> Result<Engine> {
    workspace::ensure_excluded(repo).await?;
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

/// Build an engine to CREATE a mission from an installed mission type (by name).
/// The confinement image is resolved to a content id here — once, at start — so
/// a later rebuild of the tag cannot silently change the instrument. The engine
/// carries the runtime + image id it records on `MissionCreated`.
async fn build_engine_for_start(
    store: MissionStore,
    repo: &Path,
    type_name: &str,
    runtime: &str,
) -> Result<Engine> {
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
    assemble_engine(
        store,
        repo,
        mission_type,
        runtime.to_string(),
        image_id,
        profile,
        ceiling,
    )
    .await
}

/// Build an engine for an EXISTING mission: resolve its recorded mission type (by
/// name, from the home), runtime, and pinned image id — so no `--type`/`--runtime`
/// is needed. `load_state` then verifies the pinned digest, fail-closed.
async fn build_engine_for_mission(
    store: MissionStore,
    repo: &Path,
    mission_id: &MissionId,
) -> Result<Engine> {
    let state = store.require_state(mission_id).await?;
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
    )
    .await?;
    // Verify the pinned mission-type digest before anything runs.
    engine.load_state(mission_id).await?;
    Ok(engine)
}

/// Resolve `--repo` (an explicit path is canonicalized; omitted ⇒ the enclosing
/// git worktree root) and open its store. The root is discovered *before* the
/// store is opened, so a default never creates a stray `.lionclaw/` in a
/// subdirectory (`MissionStore::open` creates unconditionally).
async fn open_store(explicit_repo: Option<PathBuf>) -> Result<(PathBuf, MissionStore)> {
    let repo = match explicit_repo {
        Some(path) => path.canonicalize().context("repo path")?,
        None => git_worktree_root().await?,
    };
    let store = MissionStore::open(&repo).await?;
    Ok((repo, store))
}

/// The top level of the git worktree containing the current directory.
async fn git_worktree_root() -> Result<PathBuf> {
    let out = tokio::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .await
        .context("failed to run git")?;
    if !out.status.success() {
        bail!("--repo not given and the current directory is not inside a git repository");
    }
    let root = String::from_utf8_lossy(&out.stdout).trim().to_string();
    PathBuf::from(root).canonicalize().context("repo path")
}

/// Resolve a mission id: an explicit id is parsed; omitted ⇒ the sole
/// non-terminal mission in this repo. Errors (never guesses) when zero or more
/// than one mission is live.
async fn resolve_mission_id(store: &MissionStore, explicit: Option<&str>) -> Result<MissionId> {
    if let Some(id) = explicit {
        return MissionId::parse(id).map_err(Into::into);
    }
    let mut all = Vec::new();
    let mut live = Vec::new();
    for mission_id in store.list_missions().await? {
        let Some(state) = fold(store.load(&mission_id).await?) else {
            continue;
        };
        if !state.phase.is_terminal() {
            live.push(mission_id.clone());
        }
        all.push(mission_id);
    }
    // Prefer the sole live mission; once every mission has finished, still default
    // to the sole mission — report/apply are post-completion commands. Refuse to
    // guess only when the choice is genuinely ambiguous.
    match (live.as_slice(), all.as_slice()) {
        ([only], _) => Ok(only.clone()),
        ([], [only]) => Ok(only.clone()),
        ([], []) => bail!("no mission in this repo; pass a mission id"),
        ([], _) => bail!("{} missions, none live; pass a mission id", all.len()),
        _ => bail!("{} live missions; pass a mission id to choose", live.len()),
    }
}

/// Read a JSON argument from a file, or from stdin when the path is `-`.
fn read_json_arg<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let text = if path == Path::new("-") {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read JSON from stdin")?;
        buf
    } else {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read '{}'", path.display()))?
    };
    serde_json::from_str(&text).context("invalid JSON")
}

async fn cmd_start(args: StartArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    // Fail-closed: loading the mission type (and its moat check) happens before
    // any event is written.
    let engine = build_engine_for_start(store, &repo, &args.mission_type, &args.runtime).await?;
    let base_sha = workspace::head_sha(&repo).await?;
    let mission_id = engine
        .create_mission(
            &repo.to_string_lossy(),
            &args.objective,
            &base_sha,
            MissionConfig {
                // Default-on ratification gate; `--yes` auto-approves.
                ratification_gate: !args.yes,
                // The honesty bar is the mission type's, not a hardcoded default.
                stop: engine.mission_type().stop,
                // The planning DAG the mission type ships (empty ⇒ awaits a
                // manually submitted plan).
                planning: engine.mission_type().planning.clone(),
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
    let (_repo, store) = open_store(args.repo).await?;
    let mut parked = Vec::new();
    for mission_id in store.list_missions().await? {
        let events = store.load(&mission_id).await?;
        let Some(state) = fold(events) else { continue };
        if !state.open_attention.is_empty() {
            parked.push((mission_id, state));
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
                        serde_json::json!({ "id": a.id, "kind": a.kind.slug(), "report": a.report })
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
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    // Resolve whichever ratification item is open — a proposed contract
    // (RatifyProposal) or a post-amendment re-ratification (Ratify) — so the
    // human never has to type the item id.
    let state = store.require_state(&mission_id).await?;
    let item = state
        .open_attention
        .values()
        .find(|a| {
            matches!(
                a.kind,
                AttentionKind::Ratify | AttentionKind::RatifyProposal
            )
        })
        .context("nothing is awaiting ratification for this mission")?;
    crate::engine::record_decision(
        &store,
        SystemClock.now_ms(),
        &mission_id,
        &item.id,
        crate::model::DecisionAction::Ratify,
        &args.justification,
        "cli",
    )
    .await?;
    println!("ratified mission {mission_id}");
    Ok(())
}

async fn cmd_plan(args: PlanArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let state = store.require_state(&mission_id).await?;
    let Some(proposal) = &state.proposal else {
        bail!("no proposal is awaiting ratification for mission {mission_id}");
    };
    // The verified/reviewed ceiling: a plan is verified-possible iff every
    // assertion binds an oracle.
    let ceiling = if proposal.all_assertions_bound() {
        "verified-possible"
    } else {
        "reviewed-only"
    };
    if args.json {
        let bindings: Vec<_> = proposal
            .assertions
            .iter()
            .map(|a| {
                serde_json::json!({
                    "id": a.id.as_str(),
                    "prose": a.prose,
                    "oracle": a.oracle.as_ref().map(|o| o.as_str()),
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id.as_str(),
                "ceiling": ceiling,
                "assertions": bindings,
                "tasks": proposal.tasks.len(),
            })
        );
    } else {
        println!("proposed contract for mission {mission_id} ({ceiling}):");
        for a in &proposal.assertions {
            let oracle = a
                .oracle
                .as_ref()
                .map_or("— no oracle (advisory)", |o| o.as_str());
            println!("  {} → {}\n    {}", a.id, oracle, a.prose);
        }
        println!(
            "  ({} tasks) — ratify to seed the contract and begin work",
            proposal.tasks.len()
        );
    }
    Ok(())
}

/// The branch name and target commit for `apply`, or an error when the mission
/// produced no commit (`current == base`) — never create an empty branch.
fn apply_target(mission_id: &MissionId, base_sha: &str, current_sha: &str) -> Result<String> {
    if current_sha == base_sha {
        bail!("mission {mission_id} produced no commit to apply");
    }
    Ok(format!("lionclaw/{mission_id}"))
}

async fn cmd_apply(args: ApplyArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let state = store.require_state(&mission_id).await?;
    let branch = apply_target(&mission_id, &state.base_sha, &state.current_sha)?;
    workspace::create_branch(&repo, &branch, &state.current_sha, args.force)
        .await
        .with_context(|| {
            format!("could not create branch '{branch}' (already exists? use --force)")
        })?;
    println!(
        "applied mission {mission_id} → branch {branch} ({})",
        short_hex(&state.current_sha)
    );
    Ok(())
}

async fn cmd_report(args: ReportArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let state = store.require_state(&mission_id).await?;

    let finish = state.phase.finish();
    // Per-assertion evidence: the oracle that judged it, its exit code, the
    // commit it judged, whether that verdict is fresh at the final head, and a
    // short excerpt of its output.
    let mut rows = Vec::new();
    for (aid, a) in &state.contract {
        let verdict = a.last_authoritative.as_ref().map(|v| {
            let (stdout, _stderr) = v.evidence();
            // The excerpt is only rendered in --json; don't resolve the blob for
            // the text receipt, which never prints it.
            let excerpt = args
                .json
                .then(|| store.blobs().resolve(stdout).ok())
                .flatten()
                .map(|s| s.chars().take(160).collect::<String>());
            serde_json::json!({
                "oracle": v.oracle().as_str(),
                "passed": v.passed(),
                "exit_code": v.exit_code(),
                "judged_sha": v.judged_sha(),
                "fresh": v.is_fresh_at(&state.current_sha),
                "evidence_excerpt": excerpt,
            })
        });
        rows.push((aid.as_str().to_string(), a.oracle.is_some(), verdict));
    }
    let uncovered: Vec<&str> = state
        .contract
        .iter()
        .filter(|(_, a)| a.oracle.is_none())
        .map(|(id, _)| id.as_str())
        .collect();

    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id.as_str(),
                "objective": state.objective,
                "mission_type": { "name": state.mission_type.name, "digest": state.mission_type.digest },
                "runtime": state.runtime,
                "image_id": state.image_id,
                "stop_bar": state.config.stop.slug(),
                "base_sha": state.base_sha,
                "current_sha": state.current_sha,
                "finish": finish.map(|f| f.slug()),
                "assertions": rows.iter().map(|(id, _, v)| serde_json::json!({ "id": id, "verdict": v })).collect::<Vec<_>>(),
                "not_covered_by_an_oracle": uncovered,
            })
        );
        return Ok(());
    }

    println!("Mission {mission_id} — {}", state.objective);
    println!(
        "  type:    {} @ {}",
        state.mission_type.name,
        short_hex(&state.mission_type.digest)
    );
    println!("  runtime: {}   image: {}", state.runtime, state.image_id);
    println!(
        "  commit:  {} → {}",
        short_hex(&state.base_sha),
        short_hex(&state.current_sha)
    );
    match finish {
        Some(FinishClass::Verified) => {
            println!(
                "  finish:  VERIFIED — a fresh oracle pass at the final commit for every assertion"
            )
        }
        Some(FinishClass::InternallyConsistent) => {
            println!("  finish:  INTERNALLY-CONSISTENT — no machine checked this; an agent said so")
        }
        Some(FinishClass::Unverified) => {
            println!("  finish:  UNVERIFIED — not proven at the final commit")
        }
        None => println!(
            "  finish:  (not finished; phase {})",
            phase_slug(&state.phase)
        ),
    }
    println!("  bar:     {:?}", state.config.stop);
    println!("\n  assertions:");
    for (id, has_oracle, verdict) in &rows {
        match verdict {
            Some(v) => {
                let passed = v["passed"].as_bool().unwrap_or(false);
                let fresh = v["fresh"].as_bool().unwrap_or(false);
                println!(
                    "    {id}: {} by {} (exit {}){}",
                    if passed { "PASS" } else { "FAIL" },
                    v["oracle"].as_str().unwrap_or("?"),
                    v["exit_code"],
                    if fresh {
                        ""
                    } else {
                        " [STALE — not at the final commit]"
                    },
                );
            }
            None if *has_oracle => println!("    {id}: (oracle owed, not yet run)"),
            None => println!("    {id}: advisory only — no oracle can prove this"),
        }
    }
    if !uncovered.is_empty() {
        println!(
            "\n  NOT covered by an oracle (agent judgement only): {}",
            uncovered.join(", ")
        );
    }
    if args.patch && state.current_sha != state.base_sha {
        let diff = workspace::diff(&repo, &state.base_sha, &state.current_sha).await?;
        println!(
            "\n--- diff {}..{} ---\n{diff}",
            state.base_sha, state.current_sha
        );
    }
    Ok(())
}

async fn cmd_decide(args: DecideArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
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
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let engine = build_engine_for_mission(store, &repo, &mission_id).await?;
    let submission = read_json_arg(&args.plan)?;
    engine
        .submit_plan(&mission_id, submission)
        .await
        .context("plan rejected")?;
    println!("plan accepted for mission {mission_id}");
    Ok(())
}

async fn cmd_amend(args: AmendArgs) -> Result<()> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let engine = build_engine_for_mission(store, &repo, &mission_id).await?;
    let ops = read_json_arg(&args.ops)?;
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

async fn cmd_advance(args: AdvanceArgs) -> Result<std::process::ExitCode> {
    let (repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    // Stream committed events to stderr so a run that blocks minutes per agent
    // turn is not silent (stdout stays clean for `--json`).
    let store = store.with_sink(Arc::new(StderrEventSink));
    let engine = build_engine_for_mission(store, &repo, &mission_id).await?;
    let outcome = engine.advance(&mission_id).await?;
    let state = engine.load_state(&mission_id).await?;
    print_advance_outcome(mission_id.as_str(), &state.phase, &outcome, args.json);
    // The exit code reflects the honesty bar: a mission that finished below the
    // stop bar its mission type declares exits nonzero, so a caller or CI can
    // gate on "actually verified" without parsing output.
    Ok(match &state.phase {
        MissionPhase::Done { finish } if !state.config.stop.satisfied_by(*finish) => {
            eprintln!(
                "finished {finish:?}, below the mission type's stop bar {:?}",
                state.config.stop
            );
            std::process::ExitCode::FAILURE
        }
        _ => std::process::ExitCode::SUCCESS,
    })
}

async fn cmd_status(args: StatusArgs) -> Result<()> {
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
    let events = store.load(&mission_id).await?;
    let state = fold(events).with_context(|| format!("mission {mission_id} not found"))?;
    if args.json {
        let finish = state.phase.finish().map(|f| f.slug());
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
                        "advisory": a.advisory.slug(),
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
    let (_repo, store) = open_store(args.repo).await?;
    let mission_id = resolve_mission_id(&store, args.mission_id.as_deref()).await?;
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
        let src_dir = entry.path();
        // Validate before installing (an invalid mission type never lands), and
        // key the destination by the manifest NAME — the identity a started
        // mission re-opens by — not the source directory basename, so a type
        // whose directory differs from its name still installs and starts
        // coherently.
        let name = load_mission_type(&src_dir, &AuthorityCeiling::default())
            .with_context(|| format!("mission type at '{}' is invalid", src_dir.display()))?
            .name;
        let dest = dest_dir.join(&name);
        if dest.exists() {
            if !args.force {
                println!("skip {name} (already installed; --force to overwrite)");
                continue;
            }
            std::fs::remove_dir_all(&dest)
                .with_context(|| format!("removing '{}'", dest.display()))?;
        }
        copy_tree(&src_dir, &dest)?;
        println!("installed {name}");
        installed.push(name);
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
                        "stop": mt.stop.slug(),
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
        // Bubble up — `run_mission` renders the `{"ok":false,…}` envelope for
        // `--json` and lets the human path print to stderr; either way exit 1.
        Err(err) => Err(anyhow::Error::from(err).context("mission type invalid")),
    }
}

fn print_advance_outcome(
    mission_id: &str,
    phase: &MissionPhase,
    outcome: &AdvanceOutcome,
    json: bool,
) {
    let finish = phase.finish().map(|f| f.slug());
    if json {
        println!(
            "{}",
            serde_json::json!({
                "mission_id": mission_id,
                "phase": phase_slug(phase),
                "finish": finish,
                "outcome": outcome.slug(),
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
}

/// The mission phase as a slug, carrying the finish grade for `Done`
/// (`done:verified`). The variant slugs live on the enums (one source).
fn phase_slug(phase: &MissionPhase) -> String {
    match phase {
        MissionPhase::Done { finish } => format!("done:{}", finish.slug()),
        other => other.slug().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mid() -> MissionId {
        MissionId::parse("mabc123def456").unwrap()
    }

    #[test]
    fn apply_refuses_a_mission_that_produced_no_commit() {
        // current == base ⇒ nothing to apply (never an empty branch).
        assert!(apply_target(&mid(), "base", "base").is_err());
        // a produced commit ⇒ the `lionclaw/<id>` branch target.
        assert_eq!(
            apply_target(&mid(), "base", "head").unwrap(),
            "lionclaw/mabc123def456"
        );
    }
}
