use std::collections::BTreeSet;

#[test]
fn kernel_model_production_dependencies_are_pure_and_allowlisted() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let manifest: toml::Value = toml::from_str(&manifest).unwrap();
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .unwrap();
    let names = dependencies
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    assert_eq!(names, BTreeSet::from(["serde", "sha2", "thiserror"]));
    assert!(dependencies.values().all(|dependency| {
        dependency
            .get("default-features")
            .and_then(toml::Value::as_bool)
            == Some(false)
    }));
    assert!(manifest.get("build-dependencies").is_none());
    assert!(manifest.get("target").is_none());
    let crate_root = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
    assert!(crate_root.lines().any(|line| line.trim() == "#![no_std]"));
}
