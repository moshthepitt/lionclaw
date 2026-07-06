//! Manual smoke test for the confined oracle path (no LLM): snapshot a
//! commit, run the plugin's cargo-test oracle in a container, print the
//! evidence. Usage: cargo run --example oracle_smoke -- <repo> <plugin> <oracle> <sha>

use std::path::PathBuf;

use lionclaw_mission_engine::authority::AuthorityCeiling;
use lionclaw_mission_engine::config::MissionRuntimeProfile;
use lionclaw_mission_engine::model::{AssertionId, MissionId, OracleName};
use lionclaw_mission_engine::oracle::OciOracleRunner;
use lionclaw_mission_engine::plugin::load_plugin;
use lionclaw_mission_engine::ports::{OracleRunRequest, OracleRunner};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let repo = PathBuf::from(&args[1]).canonicalize()?;
    let plugin_dir = PathBuf::from(&args[2]);
    let oracle_name = OracleName::new(&args[3])?;
    let sha = args[4].clone();

    let plugin = load_plugin(&plugin_dir, &AuthorityCeiling::default())?;
    let oracle_path = plugin
        .oracles
        .get(&oracle_name)
        .expect("oracle exists")
        .clone();

    let state_dir = repo.join(".lionclaw");
    std::fs::create_dir_all(&state_dir)?;

    let runner = OciOracleRunner::new(MissionRuntimeProfile::codex_default());
    let outcome = runner
        .run(OracleRunRequest {
            mission_id: MissionId::from_digest_prefix("abcdef0123456789"),
            oracle: oracle_name,
            oracle_path,
            assertion_ids: vec![AssertionId::new("TESTS-PASS")?],
            judged_sha: sha,
            workspace_dir: repo,
            state_dir,
        })
        .await;

    match outcome {
        Ok(o) => {
            println!("exit_code = {:?}, signal = {:?}", o.exit_code, o.exit_signal);
            println!("duration_ms = {}", o.duration_ms);
            println!("--- stdout ---\n{}", String::from_utf8_lossy(&o.stdout));
            eprintln!("--- stderr ---\n{}", String::from_utf8_lossy(&o.stderr));
            std::process::exit(if o.exit_code == 0 && o.exit_signal.is_none() {
                0
            } else {
                2
            });
        }
        Err(e) => {
            eprintln!("oracle infra failure: {e}");
            std::process::exit(3);
        }
    }
}
