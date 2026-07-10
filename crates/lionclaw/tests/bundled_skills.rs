//! BUNDLED-SKILL-HANDOFF-TRUTH regression: every bundled mission skill
//! instruction contains the required handoff-file directive and no stale
//! `end_node` / unavailable completion-tool instructions or embedded handoff
//! schema literals.

use std::io;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn walk_skill_md(dir: &std::path::Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_skill_md(&path, out)?;
        } else if path.file_name().and_then(|n| n.to_str()) == Some("SKILL.md") {
            out.push(path);
        }
    }
    Ok(())
}

/// Every `mission-types/**/skills/*/SKILL.md` in the repo.
fn bundled_skill_files() -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    walk_skill_md(&repo_root().join("mission-types"), &mut files)?;
    files.sort();
    Ok(files)
}

#[test]
fn bundled_skill_set_is_non_empty_and_matches_software_dev() {
    let files = bundled_skill_files().expect("discover bundled skills");
    assert!(!files.is_empty(), "must find at least one bundled SKILL.md");

    let names: Vec<String> = files
        .iter()
        .map(|p| {
            p.parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    assert_eq!(
        names,
        vec!["scrutiny-validator", "user-testing-validator"],
        "the bundled software-dev skill set must match exactly"
    );
}

#[test]
fn every_bundled_skill_has_no_end_node_and_carries_the_handoff_directive() {
    let files = bundled_skill_files().expect("discover bundled skills");
    assert!(!files.is_empty());

    for path in &files {
        let text = std::fs::read_to_string(path).expect("read bundled skill");

        // Stale terms absent.
        assert!(
            !text.contains("end_node"),
            "{}: stale `end_node` directive must be absent",
            path.display()
        );
        assert!(
            !text.to_ascii_lowercase().contains("completion tool"),
            "{}: unavailable completion-tool instructions must be absent",
            path.display()
        );

        // Required handoff path present.
        assert!(
            text.contains("/mission/handoff/handoff.json"),
            "{}: required handoff path must be present",
            path.display()
        );

        // "as specified by the prompt" present.
        assert!(
            text.contains("as specified by the prompt"),
            "{}: 'as specified by the prompt' must be present",
            path.display()
        );

        // No embedded lionclaw.mission.*-handoff schema literal.
        assert!(
            !text.contains("lionclaw.mission.") && !text.contains("-handoff.v1"),
            "{}: no handoff schema literal may be embedded in a skill file",
            path.display()
        );
    }
}
