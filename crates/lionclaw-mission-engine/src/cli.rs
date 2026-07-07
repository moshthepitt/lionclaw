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
use crate::model::{fold, MissionConfig, MissionId, MissionPhase};
use crate::oracle::OciOracleRunner;
use crate::plugin::load_plugin;
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
    /// Mission engine commands.
    #[command(subcommand)]
    Mission(MissionCommand),
}

#[derive(Subcommand)]
pub enum MissionCommand {
    /// Create a mission over a target repo and submit no plan yet.
    Start(StartArgs),
    /// Submit a plan (contract + task DAG) from a JSON file.
    SubmitPlan(SubmitPlanArgs),
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
    /// Validate a plugin directory (loader + moat) without starting anything.
    Plugin(PluginArgs),
    /// Drive the real stack end-to-end and assert the Slice-1 invariants
    /// (needs podman; model-auth-free).
    SelfTest(SelfTestArgs),
}

#[derive(Args)]
pub struct StartArgs {
    /// Plugin directory (a domain of prose).
    #[arg(long)]
    pub plugin: PathBuf,
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
    #[arg(long)]
    pub plugin: PathBuf,
    /// Plan JSON file ({ "assertions": [...], "tasks": [...] }).
    #[arg(long)]
    pub plan: PathBuf,
    #[arg(long, default_value = "codex")]
    pub runtime: String,
}

#[derive(Args)]
pub struct AdvanceArgs {
    pub mission_id: String,
    #[arg(long)]
    pub repo: PathBuf,
    #[arg(long)]
    pub plugin: PathBuf,
    #[arg(long, default_value = "codex")]
    pub runtime: String,
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
pub struct PluginArgs {
    pub dir: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct SelfTestArgs {
    #[arg(long)]
    pub json: bool,
}

/// Run a command, returning the process exit code (so callers can gate on
/// e.g. `plugin check` without the command itself calling `process::exit`).
pub async fn run(cli: Cli) -> Result<std::process::ExitCode> {
    match cli.command {
        Command::Mission(cmd) => run_mission(cmd).await,
    }
}

async fn run_mission(cmd: MissionCommand) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    match cmd {
        MissionCommand::Start(args) => cmd_start(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::SubmitPlan(args) => cmd_submit_plan(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Advance(args) => cmd_advance(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Status(args) => cmd_status(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Log(args) => cmd_log(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Inbox(args) => cmd_inbox(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Ratify(args) => cmd_ratify(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Decide(args) => cmd_decide(args).await.map(|()| ExitCode::SUCCESS),
        MissionCommand::Plugin(args) => cmd_plugin(args).await,
        MissionCommand::SelfTest(args) => crate::selftest::run(args.json).await,
    }
}

fn runtime_profile(runtime: &str) -> Result<MissionRuntimeProfile> {
    match runtime {
        "codex" => Ok(MissionRuntimeProfile::codex_default()),
        other => bail!("unknown runtime '{other}' (only 'codex' is wired)"),
    }
}

async fn open_engine(repo: &Path, plugin_dir: &Path, runtime: &str) -> Result<Engine> {
    let ceiling = AuthorityCeiling::default();
    let plugin = load_plugin(plugin_dir, &ceiling)
        .with_context(|| format!("failed to load plugin '{}'", plugin_dir.display()))?;
    let store = MissionStore::open(repo).await?;
    workspace::ensure_excluded(repo)?;
    let profile = runtime_profile(runtime)?;
    let role_runner = Arc::new(OciRoleRunner::new(profile.clone(), ceiling));
    let oracle_runner = Arc::new(OciOracleRunner::new(profile));
    Ok(Engine::new(
        store,
        plugin,
        role_runner,
        oracle_runner,
        Arc::new(SystemClock),
    ))
}

async fn cmd_start(args: StartArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    // Fail-closed: loading the plugin (and its moat check) happens before any
    // event is written.
    let engine = open_engine(&repo, &args.plugin, &args.runtime).await?;
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
        println!("submit a plan, then: lionclaw mission advance {mission_id} --repo {} --plugin {}", repo.display(), args.plugin.display());
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
    println!("recorded decision on '{}' for mission {mission_id}", args.item);
    Ok(())
}

async fn cmd_submit_plan(args: SubmitPlanArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    let engine = open_engine(&repo, &args.plugin, &args.runtime).await?;
    let mission_id = MissionId::parse(&args.mission_id)?;
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

async fn cmd_advance(args: AdvanceArgs) -> Result<()> {
    let repo = args.repo.canonicalize().context("repo path")?;
    let engine = open_engine(&repo, &args.plugin, &args.runtime).await?;
    let mission_id = MissionId::parse(&args.mission_id)?;
    let outcome = engine.advance(&mission_id).await?;
    let state = engine.load_state(&mission_id).await?;
    report_state(&args.mission_id, &state.phase, &outcome, args.json, &engine, &mission_id).await
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
        println!("mission {mission_id}: {}", phase_slug(&state.phase));
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

async fn cmd_plugin(args: PluginArgs) -> Result<std::process::ExitCode> {
    match load_plugin(&args.dir, &AuthorityCeiling::default()) {
        Ok(plugin) => {
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "name": plugin.name,
                        "roles": plugin.roles.keys().map(|r| r.as_str()).collect::<Vec<_>>(),
                        "oracles": plugin.oracles.keys().map(|o| o.as_str()).collect::<Vec<_>>(),
                    })
                );
            } else {
                println!("plugin '{}' is valid", plugin.name);
                println!("  roles: {}", plugin.roles.len());
                println!("  oracles: {}", plugin.oracles.len());
            }
            Ok(std::process::ExitCode::SUCCESS)
        }
        Err(err) => {
            if args.json {
                println!("{}", serde_json::json!({ "ok": false, "error": err.to_string() }));
            } else {
                eprintln!("plugin invalid: {err}");
            }
            // Non-zero exit so a caller can gate on it.
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
                println!("mission {mission_id}: parked ({} attention item(s))", attention.len());
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
