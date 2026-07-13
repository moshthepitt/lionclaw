use std::fmt::Write as _;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let root = manifest_dir.join("../../mission-types");
    println!("cargo:rerun-if-changed={}", root.display());

    let mut files = Vec::new();
    collect(&root, &root, &mut files);
    files.sort_by(|left, right| left.0.cmp(&right.0));

    let mut generated = String::from("static BUNDLED_FILES: &[BundledFile] = &[\n");
    for (relative, source, executable) in files {
        writeln!(
            generated,
            "    BundledFile {{ path: {relative:?}, bytes: include_bytes!({source:?}), executable: {executable} }},",
            source = source.to_string_lossy(),
        )
        .unwrap();
    }
    generated.push_str("];\n");

    let output =
        PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("bundled_mission_types.rs");
    fs::write(output, generated).unwrap();
}

fn collect(root: &Path, directory: &Path, files: &mut Vec<(String, PathBuf, bool)>) {
    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("reading '{}': {error}", directory.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("statting '{}': {error}", path.display()));
        if metadata.file_type().is_symlink() {
            panic!("bundled mission type contains symlink '{}'", path.display());
        }
        if metadata.is_dir() {
            collect(root, &path, files);
            continue;
        }
        if !metadata.is_file() {
            panic!(
                "bundled mission type entry '{}' is not a regular file",
                path.display()
            );
        }
        let relative = path
            .strip_prefix(root)
            .unwrap()
            .to_str()
            .unwrap_or_else(|| panic!("non-UTF-8 bundled path '{}'", path.display()))
            .replace(std::path::MAIN_SEPARATOR, "/");
        files.push((relative, path, metadata.permissions().mode() & 0o111 != 0));
    }
}
