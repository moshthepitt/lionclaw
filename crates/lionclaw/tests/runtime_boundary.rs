use std::path::{Path, PathBuf};

fn rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn superseded_runtime_contract_has_no_source_residue() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rust_files(&workspace.join("crates"), &mut files);
    let this_file = workspace.join("crates/lionclaw/tests/runtime_boundary.rs");
    let forbidden = [
        ["Runtime", "TurnResult"].concat(),
        ["Hidden", "TurnSupport"].concat(),
        ["runtime", "_control"].concat(),
        ["Runtime", "Registry"].concat(),
        ["program", "_backed"].concat(),
        ["RuntimeCapability", "Request"].concat(),
        ["RuntimeCapability", "Result"].concat(),
    ];

    for file in files {
        if file == this_file {
            continue;
        }
        let source = std::fs::read_to_string(&file).expect("read Rust source");
        for obsolete in &forbidden {
            assert!(
                !source.contains(obsolete),
                "obsolete runtime surface {obsolete:?} remains in {}",
                file.display()
            );
        }
    }
}

#[test]
fn production_runtime_construction_does_not_branch_on_product_names() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let path = workspace.join("crates/lionclaw/src/runner/role_runner.rs");
    let source = std::fs::read_to_string(&path).expect("read production role runner");
    let forbidden = [
        ["match profile.", "driver"].concat(),
        ["driver.as_", "str()"].concat(),
        ["CODEX_RUNTIME_", "AUTH_KIND"].concat(),
        ["kind == ", "\"codex\""].concat(),
        ["driver == ", "\"codex\""].concat(),
        ["driver == ", "\"acp\""].concat(),
    ];

    for behavior_branch in forbidden {
        assert!(
            !source.contains(&behavior_branch),
            "runtime construction contains product-name behavior branch {behavior_branch:?}"
        );
    }
}
