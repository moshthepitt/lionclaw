//! The real software-dev plugin loads; the moat rejects a plugin whose
//! verdict role would violate the honesty floor.

use std::path::PathBuf;

use lionclaw_mission_engine::authority::AuthorityCeiling;
use lionclaw_mission_engine::authority::MoatViolation;
use lionclaw_mission_engine::model::{OutputSemantics, StopBar};
use lionclaw_mission_engine::plugin::{load_plugin, PluginError};

fn repo_root() -> PathBuf {
    // <crate>/tests/plugin_loading.rs → repo root is three parents up from
    // the crate dir.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn software_dev_plugin_loads() {
    let plugin = load_plugin(
        &repo_root().join("plugins/software-dev"),
        &AuthorityCeiling::default(),
    )
    .expect("software-dev plugin loads");
    assert_eq!(plugin.name, "software-dev");
    assert_eq!(plugin.stop, StopBar::Verified);
    let implementer = plugin
        .roles
        .values()
        .find(|r| r.output == OutputSemantics::ProducesArtifact)
        .expect("has an implementer role");
    assert_eq!(implementer.runtime.as_deref(), Some("codex"));
    assert!(plugin.oracles.contains_key(
        &lionclaw_mission_engine::model::OracleName::new("cargo-test").expect("name")
    ));
}

#[test]
fn writable_judge_plugin_refuses_to_load() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/plugins/writable-judge");
    let err = load_plugin(&fixture, &AuthorityCeiling::default()).expect_err("must refuse");
    // A verdict role asking for secrets cannot satisfy the moat — a typed
    // moat violation, not a generic role error.
    assert!(
        matches!(
            err,
            PluginError::Moat {
                violation: MoatViolation::SecretsForJudge { .. },
                ..
            }
        ),
        "expected a typed moat violation, got {err:?}"
    );
}
