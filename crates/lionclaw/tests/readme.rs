//! The root README is the deliberately small, release-facing discovery path.

const README: &str = include_str!("../../../README.md");

#[test]
fn first_viewport_contains_only_the_five_discovery_elements() {
    let (viewport, advanced) = README
        .split_once("\n---\n")
        .expect("README must separate discovery from advanced signposts");

    assert!(viewport.contains("small trusted core and explicit local\nboundary"));
    assert!(viewport.contains("Supported platform: Linux x86_64."));
    assert!(viewport.contains("lionclaw-*-linux-x86_64.tar.gz"));
    assert!(viewport.contains("sha256sum -c"));
    assert!(viewport.contains("normal skill installation mechanism"));
    assert!(viewport.contains("From the extracted `lionclaw/` directory"));
    assert!(viewport.contains("\n./lionclaw doctor\n"));
    assert!(viewport.contains("\n./lionclaw run codex\n"));

    assert!(!viewport.contains("mission --help"));
    assert!(!viewport.contains("mission type --help"));
    assert!(!viewport.contains("run --help"));

    assert_eq!(README.matches("\n---\n").count(), 1);
    assert_eq!(viewport.matches("```text").count(), 1);
    assert_eq!(viewport.matches("```\n").count(), 1);

    assert_eq!(
        advanced.trim(),
        "For other runtimes, see `lionclaw run --help`. For mission operation, see\n\
         `lionclaw mission --help`. To author custom mission types, see\n\
         `lionclaw mission type --help`."
    );
}

#[test]
fn readme_stays_discovery_copy_not_a_reference_or_product_claim() {
    assert!(README.lines().count() <= 25, "README must remain short");

    let lowercase = README.to_ascii_lowercase();
    for excluded in [
        "architecture",
        "benchmark",
        "faster than",
        "commands:",
        "options:",
        "usage:",
    ] {
        assert!(
            !lowercase.contains(excluded),
            "README contains excluded reference or claim text: {excluded}"
        );
    }
}
