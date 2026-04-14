use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftOutput {
    pub relative_path: String,
    pub size_bytes: u64,
    pub media_type: String,
}

pub fn list_outputs(root: &Path) -> Result<Vec<DraftOutput>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut outputs = Vec::new();
    collect_outputs(root, Path::new(""), &mut outputs)?;
    outputs.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(outputs)
}

pub fn remove_output(root: &Path, relative_path: &str) -> Result<()> {
    let path = resolve_output_path(root, relative_path)?;
    fs::remove_file(&path).with_context(|| format!("failed to delete {}", path.display()))
}

pub fn output_exists(root: &Path, relative_path: &str) -> Result<bool> {
    let relative = normalize_relative_path(relative_path)?;
    let path = root.join(&relative);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    if metadata.file_type().is_symlink() {
        bail!("draft path '{}' is a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("draft path '{}' is not a regular file", path.display());
    }
    Ok(true)
}

pub fn move_output(root: &Path, relative_path: &str, destination: &Path) -> Result<()> {
    let source = resolve_output_path(root, relative_path)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    match fs::rename(&source, destination) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            fs::copy(&source, destination).with_context(|| {
                format!(
                    "failed to copy {} to {}",
                    source.display(),
                    destination.display()
                )
            })?;
            fs::remove_file(&source)
                .with_context(|| format!("failed to delete {}", source.display()))?;
            let _ = rename_err;
            Ok(())
        }
    }
}

fn collect_outputs(
    root: &Path,
    relative_root: &Path,
    outputs: &mut Vec<DraftOutput>,
) -> Result<()> {
    let dir = if relative_root.as_os_str().is_empty() {
        root.to_path_buf()
    } else {
        root.join(relative_root)
    };

    for entry in fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("failed to iterate {}", dir.display()))?;
        let relative = relative_root.join(entry.file_name());
        let path = root.join(&relative);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_outputs(root, &relative, outputs)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        outputs.push(DraftOutput {
            relative_path: relative.to_string_lossy().to_string(),
            size_bytes: metadata.len(),
            media_type: media_type_for_path(&path),
        });
    }

    Ok(())
}

fn resolve_output_path(root: &Path, relative_path: &str) -> Result<PathBuf> {
    let relative = normalize_relative_path(relative_path)?;
    let path = root.join(&relative);
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("failed to stat {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("draft path '{}' is a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("draft path '{}' is not a regular file", path.display());
    }
    Ok(path)
}

fn normalize_relative_path(path: &str) -> Result<PathBuf> {
    let candidate = Path::new(path.trim());
    if candidate.as_os_str().is_empty() || candidate.is_absolute() {
        bail!("draft path '{}' is invalid", candidate.display());
    }
    let mut normalized = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("draft path '{}' is invalid", candidate.display())
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        bail!("draft path '{}' is invalid", candidate.display());
    }
    Ok(normalized)
}

fn media_type_for_path(path: &Path) -> String {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("md") => "text/markdown",
        Some("txt") => "text/plain",
        Some("csv") => "text/csv",
        Some("json") => "application/json",
        Some("html") => "text/html",
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        _ => "application/octet-stream",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::{list_outputs, move_output, output_exists, remove_output};

    #[test]
    fn lists_regular_files_recursively() {
        let temp_dir = tempdir().expect("temp dir");
        std::fs::write(temp_dir.path().join("report.md"), "# Report").expect("report");
        std::fs::create_dir_all(temp_dir.path().join("bundle")).expect("bundle");
        std::fs::write(temp_dir.path().join("bundle/chart.csv"), "x,y").expect("chart");

        let outputs = list_outputs(temp_dir.path()).expect("list");
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].relative_path, "bundle/chart.csv");
        assert_eq!(outputs[1].relative_path, "report.md");
    }

    #[test]
    fn skips_symlinked_files() {
        let temp_dir = tempdir().expect("temp dir");
        let outside_dir = tempdir().expect("outside dir");
        let outside = outside_dir.path().join("outside.txt");
        std::fs::write(&outside, "secret").expect("outside");
        symlink(&outside, temp_dir.path().join("report.txt")).expect("symlink");
        std::fs::write(temp_dir.path().join("notes.txt"), "visible").expect("notes");

        let outputs = list_outputs(temp_dir.path()).expect("list");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].relative_path, "notes.txt");
    }

    #[test]
    fn output_exists_returns_false_for_missing_file() {
        let temp_dir = tempdir().expect("temp dir");
        assert!(!output_exists(temp_dir.path(), "missing.txt").expect("missing file"));
    }

    #[test]
    fn remove_output_rejects_parent_escapes() {
        let temp_dir = tempdir().expect("temp dir");
        let err = remove_output(temp_dir.path(), "../secret").expect_err("invalid");
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn move_output_relocates_file() {
        let temp_dir = tempdir().expect("temp dir");
        std::fs::write(temp_dir.path().join("report.md"), "# Report").expect("report");
        let destination = temp_dir.path().join("kept/report.md");

        move_output(temp_dir.path(), "report.md", &destination).expect("move");

        assert!(!temp_dir.path().join("report.md").exists());
        assert_eq!(
            std::fs::read_to_string(destination).expect("read destination"),
            "# Report"
        );
    }
}
