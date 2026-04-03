use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Utc};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::Mutex};

pub const MEMORY_FILE: &str = "MEMORY.md";
pub const CONTINUITY_DIR: &str = "continuity";
const ACTIVE_FILE: &str = "ACTIVE.md";
const DAILY_DIR: &str = "daily";
const OPEN_LOOPS_DIR: &str = "open-loops";
const ARTIFACTS_DIR: &str = "artifacts";
const ROLLUPS_DIR: &str = "rollups";

const MEMORY_TEMPLATE: &str =
    "# Memory\n\nKeep durable facts, conventions, and important long-term context here.\n";
const ACTIVE_TEMPLATE: &str =
    "# Active Context\n\nNo active continuity signals have been recorded yet.\n";

#[derive(Debug, Clone)]
pub struct ContinuityLayout {
    workspace_root: PathBuf,
    daily_note_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Default)]
pub struct ActiveContinuitySnapshot {
    pub matters_today: Vec<String>,
    pub open_loops: Vec<String>,
    pub pending_approvals: Vec<String>,
    pub recent_outputs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ContinuityEvent {
    pub at: DateTime<Utc>,
    pub title: String,
    pub details: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ContinuityArtifact {
    pub at: DateTime<Utc>,
    pub slug: String,
    pub title: String,
    pub kind: String,
    pub summary: Option<String>,
    pub source: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityOpenLoop {
    pub title: String,
    pub relative_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityArtifactSummary {
    pub title: String,
    pub relative_path: String,
}

impl ContinuityLayout {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            daily_note_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn memory_path(&self) -> PathBuf {
        self.workspace_root.join(MEMORY_FILE)
    }

    pub fn continuity_dir(&self) -> PathBuf {
        self.workspace_root.join(CONTINUITY_DIR)
    }

    pub fn active_path(&self) -> PathBuf {
        self.continuity_dir().join(ACTIVE_FILE)
    }

    fn daily_dir(&self) -> PathBuf {
        self.continuity_dir().join(DAILY_DIR)
    }

    fn open_loops_dir(&self) -> PathBuf {
        self.continuity_dir().join(OPEN_LOOPS_DIR)
    }

    fn artifacts_dir(&self) -> PathBuf {
        self.continuity_dir().join(ARTIFACTS_DIR)
    }

    fn rollups_dir(&self) -> PathBuf {
        self.continuity_dir().join(ROLLUPS_DIR)
    }

    pub async fn ensure_base_layout(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.workspace_root)
            .await
            .with_context(|| format!("failed to create {}", self.workspace_root.display()))?;
        for dir in [
            self.continuity_dir(),
            self.daily_dir(),
            self.open_loops_dir(),
            self.artifacts_dir(),
            self.rollups_dir(),
        ] {
            tokio::fs::create_dir_all(&dir)
                .await
                .with_context(|| format!("failed to create {}", dir.display()))?;
        }

        ensure_file(self.memory_path(), MEMORY_TEMPLATE).await?;
        ensure_file(self.active_path(), ACTIVE_TEMPLATE).await?;
        Ok(())
    }

    pub async fn append_daily_event(&self, event: ContinuityEvent) -> Result<PathBuf> {
        let _guard = self.daily_note_lock.lock().await;
        let path = self.daily_note_path(event.at);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut entry = format!(
            "## {} UTC - {}\n",
            event.at.format("%H:%M"),
            event.title.trim()
        );
        for detail in event
            .details
            .into_iter()
            .filter(|line| !line.trim().is_empty())
        {
            entry.push_str(&format!("- {}\n", detail.trim()));
        }
        entry.push('\n');

        let exists = tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("failed to stat {}", path.display()))?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("failed to open {}", path.display()))?;
        if !exists {
            file.write_all(
                format!("# Daily Continuity {}\n\n", event.at.format("%Y-%m-%d")).as_bytes(),
            )
            .await
            .with_context(|| format!("failed to initialize {}", path.display()))?;
        }
        file.write_all(entry.as_bytes())
            .await
            .with_context(|| format!("failed to append {}", path.display()))?;
        file.flush()
            .await
            .with_context(|| format!("failed to flush {}", path.display()))?;
        Ok(path)
    }

    pub async fn record_artifact(&self, artifact: ContinuityArtifact) -> Result<PathBuf> {
        let path = self.artifact_path(artifact.at, &artifact.slug);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut content = format!("# {}\n\n", artifact.title.trim());
        content.push_str(&format!("- Kind: {}\n", artifact.kind.trim()));
        content.push_str(&format!("- Recorded: {} UTC\n", artifact.at.to_rfc3339()));
        if let Some(summary) = artifact
            .summary
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            content.push_str(&format!("- Summary: {}\n", summary));
        }
        if let Some(source) = artifact
            .source
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            content.push_str(&format!("- Source: {}\n", source));
        }
        content.push_str("\n## Body\n\n");
        content.push_str(artifact.body.trim());
        content.push('\n');

        tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    pub async fn write_active(&self, snapshot: &ActiveContinuitySnapshot) -> Result<()> {
        let mut sections = vec!["# Active Context".to_string()];
        if snapshot.matters_today.is_empty()
            && snapshot.open_loops.is_empty()
            && snapshot.pending_approvals.is_empty()
            && snapshot.recent_outputs.is_empty()
        {
            sections.push("\nNo active continuity signals have been recorded yet.".to_string());
        } else {
            push_list_section(&mut sections, "What Matters Today", &snapshot.matters_today);
            push_list_section(&mut sections, "Open Loops", &snapshot.open_loops);
            push_list_section(
                &mut sections,
                "Pending Approvals",
                &snapshot.pending_approvals,
            );
            push_list_section(&mut sections, "Recent Outputs", &snapshot.recent_outputs);
        }

        tokio::fs::write(self.active_path(), sections.join("\n\n") + "\n")
            .await
            .with_context(|| format!("failed to write {}", self.active_path().display()))?;
        Ok(())
    }

    pub async fn list_active_open_loops(&self) -> Result<Vec<ContinuityOpenLoop>> {
        let mut files = list_markdown_files(&self.open_loops_dir()).await?;
        files.sort();

        let mut loops = Vec::new();
        for path in files {
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            loops.push(ContinuityOpenLoop {
                title: extract_heading(&content).unwrap_or_else(|| stem_fallback(&path)),
                relative_path: relative_path(&self.workspace_root, &path),
            });
        }
        Ok(loops)
    }

    pub async fn list_recent_artifacts(
        &self,
        limit: usize,
    ) -> Result<Vec<ContinuityArtifactSummary>> {
        let mut files = list_markdown_files(&self.artifacts_dir()).await?;
        files.sort();
        files.reverse();

        let mut artifacts = Vec::new();
        for path in files.into_iter().take(limit) {
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            artifacts.push(ContinuityArtifactSummary {
                title: extract_heading(&content).unwrap_or_else(|| stem_fallback(&path)),
                relative_path: relative_path(&self.workspace_root, &path),
            });
        }
        Ok(artifacts)
    }

    fn daily_note_path(&self, at: DateTime<Utc>) -> PathBuf {
        self.daily_dir()
            .join(format!("{:04}", at.year()))
            .join(format!("{:02}", at.month()))
            .join(format!(
                "{:04}-{:02}-{:02}.md",
                at.year(),
                at.month(),
                at.day()
            ))
    }

    fn artifact_path(&self, at: DateTime<Utc>, slug: &str) -> PathBuf {
        self.artifacts_dir()
            .join(format!("{:04}", at.year()))
            .join(format!("{:02}", at.month()))
            .join(format!(
                "{:04}-{:02}-{:02}-{}.md",
                at.year(),
                at.month(),
                at.day(),
                sanitize_slug(slug)
            ))
    }
}

pub fn continuity_prompt_sections(workspace_root: &Path) -> Vec<(String, PathBuf)> {
    let layout = ContinuityLayout::new(workspace_root);
    vec![
        ("MEMORY.md".to_string(), layout.memory_path()),
        ("continuity/ACTIVE.md".to_string(), layout.active_path()),
    ]
}

async fn ensure_file(path: PathBuf, content: &str) -> Result<()> {
    match tokio::fs::try_exists(&path).await {
        Ok(true) => Ok(()),
        Ok(false) => tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display())),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

async fn list_markdown_files(root: &Path) -> Result<Vec<PathBuf>> {
    if !tokio::fs::try_exists(root)
        .await
        .with_context(|| format!("failed to stat {}", root.display()))?
    {
        return Ok(Vec::new());
    }

    let mut pending = VecDeque::from([root.to_path_buf()]);
    let mut files = Vec::new();
    while let Some(dir) = pending.pop_front() {
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .with_context(|| format!("failed to read {}", dir.display()))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .with_context(|| format!("failed to iterate {}", dir.display()))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .with_context(|| format!("failed to stat {}", path.display()))?;
            if file_type.is_dir() {
                pending.push_back(path);
            } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "md") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn push_list_section(sections: &mut Vec<String>, title: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }
    let mut body = format!("## {}", title);
    for item in items {
        body.push_str(&format!("\n- {}", item.trim()));
    }
    sections.push(body);
}

fn extract_heading(content: &str) -> Option<String> {
    content
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("# "))
        .map(|line| line.trim_start_matches("# ").trim().to_string())
        .filter(|line| !line.is_empty())
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

fn stem_fallback(path: &Path) -> String {
    path.file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("untitled")
        .replace('-', " ")
}

fn sanitize_slug(slug: &str) -> String {
    let mut normalized = slug
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while normalized.contains("--") {
        normalized = normalized.replace("--", "-");
    }
    normalized.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{
        continuity_prompt_sections, ActiveContinuitySnapshot, ContinuityArtifact, ContinuityEvent,
        ContinuityLayout,
    };

    #[tokio::test]
    async fn continuity_layout_bootstraps_expected_files() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));

        layout.ensure_base_layout().await.expect("bootstrap");

        assert!(layout.memory_path().exists());
        assert!(layout.active_path().exists());
        assert!(layout.continuity_dir().join("daily").exists());
        assert!(layout.continuity_dir().join("open-loops").exists());
        assert!(layout.continuity_dir().join("artifacts").exists());

        let prompt_sections = continuity_prompt_sections(layout.workspace_root());
        assert_eq!(prompt_sections.len(), 2);
    }

    #[tokio::test]
    async fn append_daily_event_and_artifact_are_human_readable() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let at = Utc.with_ymd_and_hms(2026, 4, 3, 9, 30, 0).unwrap();
        let daily_path = layout
            .append_daily_event(ContinuityEvent {
                at,
                title: "Scheduled brief completed".to_string(),
                details: vec!["artifact continuity/artifacts/...".to_string()],
            })
            .await
            .expect("append daily");
        let artifact_path = layout
            .record_artifact(ContinuityArtifact {
                at,
                slug: "daily-brief".to_string(),
                title: "Daily Brief".to_string(),
                kind: "scheduler_job_output".to_string(),
                summary: Some("repo summary".to_string()),
                source: Some("job:123".to_string()),
                body: "Everything looks stable.".to_string(),
            })
            .await
            .expect("record artifact");
        layout
            .write_active(&ActiveContinuitySnapshot {
                matters_today: vec!["CI is failing".to_string()],
                open_loops: vec!["Review release notes".to_string()],
                pending_approvals: vec!["terminal/alice".to_string()],
                recent_outputs: vec!["Daily Brief".to_string()],
            })
            .await
            .expect("write active");

        let daily_content = tokio::fs::read_to_string(&daily_path)
            .await
            .expect("read daily");
        let artifact_content = tokio::fs::read_to_string(&artifact_path)
            .await
            .expect("read artifact");
        let active_content = tokio::fs::read_to_string(layout.active_path())
            .await
            .expect("read active");

        assert!(daily_content.contains("Scheduled brief completed"));
        assert!(artifact_content.contains("# Daily Brief"));
        assert!(artifact_content.contains("scheduler_job_output"));
        assert!(active_content.contains("What Matters Today"));
        assert!(active_content.contains("Pending Approvals"));
    }

    #[tokio::test]
    async fn append_daily_event_preserves_concurrent_entries() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let at = Utc.with_ymd_and_hms(2026, 4, 3, 9, 30, 0).unwrap();
        let first = {
            let layout = layout.clone();
            tokio::spawn(async move {
                layout
                    .append_daily_event(ContinuityEvent {
                        at,
                        title: "First event".to_string(),
                        details: vec!["alpha".to_string()],
                    })
                    .await
                    .expect("append first")
            })
        };
        let second = {
            let layout = layout.clone();
            tokio::spawn(async move {
                layout
                    .append_daily_event(ContinuityEvent {
                        at,
                        title: "Second event".to_string(),
                        details: vec!["beta".to_string()],
                    })
                    .await
                    .expect("append second")
            })
        };

        let daily_path = first.await.expect("join first");
        second.await.expect("join second");

        let daily_content = tokio::fs::read_to_string(daily_path)
            .await
            .expect("read daily");
        assert_eq!(
            daily_content
                .matches("# Daily Continuity 2026-04-03")
                .count(),
            1
        );
        assert!(daily_content.contains("First event"));
        assert!(daily_content.contains("Second event"));
        assert!(daily_content.contains("alpha"));
        assert!(daily_content.contains("beta"));
    }
}
