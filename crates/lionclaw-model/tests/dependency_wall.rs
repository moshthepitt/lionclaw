use std::collections::BTreeSet;

#[test]
fn kernel_model_production_dependencies_are_pure_and_allowlisted() {
    let manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .unwrap();
    let manifest: toml::Value = toml::from_str(&manifest).unwrap();
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    assert_eq!(dependencies, BTreeSet::from(["serde", "sha2", "thiserror"]));
    assert!(manifest.get("build-dependencies").is_none());
    assert!(manifest.get("target").is_none());
}
