use std::{
    collections::{BTreeSet, VecDeque},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, Utc};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::Mutex};

pub const MEMORY_FILE: &str = "MEMORY.md";
pub const CONTINUITY_DIR: &str = "continuity";
const ACTIVE_FILE: &str = "ACTIVE.md";
const DAILY_DIR: &str = "daily";
const OPEN_LOOPS_DIR: &str = "open-loops";
const ARTIFACTS_DIR: &str = "artifacts";
const ROLLUPS_DIR: &str = "rollups";
const PROPOSALS_DIR: &str = "proposals";
const MEMORY_PROPOSALS_DIR: &str = "memory";
const ARCHIVE_DIR: &str = "archive";
const MERGED_DIR: &str = "merged";
const REJECTED_DIR: &str = "rejected";

const MEMORY_TEMPLATE: &str = "# Memory\n\nKeep durable facts, conventions, and important long-term context here.\n\n## Entries\n";
const ACTIVE_TEMPLATE: &str =
    "# Active Context\n\nNo active continuity signals have been recorded yet.\n";

#[derive(Debug, Clone)]
pub struct ContinuityLayout {
    workspace_root: PathBuf,
    daily_note_lock: Arc<Mutex<()>>,
    proposal_lock: Arc<Mutex<()>>,
    open_loop_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Default)]
pub struct ActiveContinuitySnapshot {
    pub matters_today: Vec<String>,
    pub open_loops: Vec<String>,
    pub pending_approvals: Vec<String>,
    pub pending_proposals: Vec<String>,
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
pub struct ContinuityOpenLoopDraft {
    pub title: String,
    pub summary: String,
    pub next_step: String,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityOpenLoop {
    pub title: String,
    pub relative_path: String,
    pub summary: Option<String>,
    pub next_step: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityArtifactSummary {
    pub title: String,
    pub relative_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityMemoryProposalDraft {
    pub title: String,
    pub rationale: String,
    pub entries: Vec<String>,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityMemoryProposal {
    pub title: String,
    pub relative_path: String,
    pub rationale: Option<String>,
    pub entries: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuitySearchMatch {
    pub relative_path: String,
    pub title: String,
    pub snippet: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityStatus {
    pub memory_path: String,
    pub active_path: String,
    pub latest_daily_path: Option<String>,
    pub open_loops: Vec<ContinuityOpenLoop>,
    pub recent_artifacts: Vec<ContinuityArtifactSummary>,
    pub memory_proposals: Vec<ContinuityMemoryProposal>,
}

impl ContinuityLayout {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            daily_note_lock: Arc::new(Mutex::new(())),
            proposal_lock: Arc::new(Mutex::new(())),
            open_loop_lock: Arc::new(Mutex::new(())),
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

    fn open_loops_archive_dir(&self) -> PathBuf {
        self.open_loops_dir().join(ARCHIVE_DIR)
    }

    fn artifacts_dir(&self) -> PathBuf {
        self.continuity_dir().join(ARTIFACTS_DIR)
    }

    fn rollups_dir(&self) -> PathBuf {
        self.continuity_dir().join(ROLLUPS_DIR)
    }

    fn proposals_dir(&self) -> PathBuf {
        self.continuity_dir().join(PROPOSALS_DIR)
    }

    fn memory_proposals_dir(&self) -> PathBuf {
        self.proposals_dir().join(MEMORY_PROPOSALS_DIR)
    }

    fn archived_memory_proposals_dir(&self, status: &str) -> PathBuf {
        self.memory_proposals_dir().join(ARCHIVE_DIR).join(status)
    }

    pub async fn ensure_base_layout(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.workspace_root)
            .await
            .with_context(|| format!("failed to create {}", self.workspace_root.display()))?;
        for dir in [
            self.continuity_dir(),
            self.daily_dir(),
            self.open_loops_dir(),
            self.open_loops_archive_dir(),
            self.artifacts_dir(),
            self.rollups_dir(),
            self.memory_proposals_dir(),
            self.archived_memory_proposals_dir(MERGED_DIR),
            self.archived_memory_proposals_dir(REJECTED_DIR),
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
            .filter(|value| !value.is_empty())
        {
            content.push_str(&format!("- Summary: {}\n", summary));
        }
        if let Some(source) = artifact
            .source
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
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

    pub async fn upsert_open_loop(
        &self,
        loop_draft: &ContinuityOpenLoopDraft,
    ) -> Result<Option<PathBuf>> {
        let _guard = self.open_loop_lock.lock().await;
        let path = self.open_loop_path(&loop_draft.title);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        if let Ok(existing) = tokio::fs::read_to_string(&path).await {
            let same_title =
                extract_heading(&existing).unwrap_or_default().trim() == loop_draft.title.trim();
            let same_summary = metadata_value(&existing, "Summary")
                .unwrap_or_default()
                .trim()
                == loop_draft.summary.trim();
            let same_next_step = metadata_value(&existing, "Next Step")
                .unwrap_or_default()
                .trim()
                == loop_draft.next_step.trim();
            let same_source = metadata_value(&existing, "Source")
                .unwrap_or_default()
                .trim()
                == loop_draft.source.as_deref().unwrap_or_default().trim();
            if same_title && same_summary && same_next_step && same_source {
                return Ok(None);
            }
        }

        let updated_at = Utc::now();
        let mut content = format!("# {}\n\n", loop_draft.title.trim());
        content.push_str("- Status: open\n");
        content.push_str(&format!("- Updated: {} UTC\n", updated_at.to_rfc3339()));
        if let Some(source) = loop_draft
            .source
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            content.push_str(&format!("- Source: {}\n", source));
        }
        if !loop_draft.summary.trim().is_empty() {
            content.push_str(&format!("- Summary: {}\n", loop_draft.summary.trim()));
        }
        if !loop_draft.next_step.trim().is_empty() {
            content.push_str(&format!("- Next Step: {}\n", loop_draft.next_step.trim()));
        }
        content.push('\n');

        tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(Some(path))
    }

    pub async fn resolve_open_loop(&self, relative_path: &str) -> Result<PathBuf> {
        let _guard = self.open_loop_lock.lock().await;
        let source = self.resolve_relative_file(relative_path)?;
        ensure_path_is_under(&source, &self.open_loops_dir())?;
        let mut content = tokio::fs::read_to_string(&source)
            .await
            .with_context(|| format!("failed to read {}", source.display()))?;
        content = replace_or_insert_metadata(
            &content,
            "Status",
            "resolved".to_string(),
            "Updated",
            format!("{} UTC", Utc::now().to_rfc3339()),
        );

        let target = self.open_loops_archive_dir().join(
            source
                .file_name()
                .ok_or_else(|| anyhow!("invalid open loop path"))?,
        );
        tokio::fs::write(&source, &content)
            .await
            .with_context(|| format!("failed to update {}", source.display()))?;
        tokio::fs::rename(&source, &target)
            .await
            .with_context(|| format!("failed to archive {}", source.display()))?;
        Ok(target)
    }

    pub async fn record_memory_proposal(
        &self,
        proposal: &ContinuityMemoryProposalDraft,
    ) -> Result<Option<PathBuf>> {
        let entries = dedupe_lines(
            proposal
                .entries
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice(),
        );
        if entries.is_empty() {
            return Ok(None);
        }

        let _guard = self.proposal_lock.lock().await;
        let path = self.memory_proposal_path(&proposal.title);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut content = format!("# Memory Proposal: {}\n\n", proposal.title.trim());
        content.push_str("- Status: proposed\n");
        content.push_str(&format!("- Proposed: {} UTC\n", Utc::now().to_rfc3339()));
        if let Some(source) = proposal
            .source
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            content.push_str(&format!("- Source: {}\n", source));
        }
        if !proposal.rationale.trim().is_empty() {
            content.push_str(&format!("- Rationale: {}\n", proposal.rationale.trim()));
        }
        content.push_str("\n## Candidate Entries\n");
        for entry in &entries {
            content.push_str(&format!("- {}\n", entry));
        }
        content.push('\n');

        let existing = tokio::fs::read_to_string(&path).await.ok();
        if let Some(existing) = existing {
            let parsed = parse_memory_proposal(&self.workspace_root, &path, &existing);
            let same_title = parsed.title.trim() == proposal.title.trim();
            let same_rationale =
                parsed.rationale.unwrap_or_default().trim() == proposal.rationale.trim();
            let same_entries = parsed.entries == entries;
            if same_title && same_rationale && same_entries {
                return Ok(None);
            }
        }

        tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(Some(path))
    }

    pub async fn list_memory_proposals(
        &self,
        limit: usize,
    ) -> Result<Vec<ContinuityMemoryProposal>> {
        let mut files = list_markdown_files(&self.memory_proposals_dir()).await?;
        files.sort();
        files.retain(|path| is_direct_child(path, &self.memory_proposals_dir()));

        let mut proposals = Vec::new();
        for path in files.into_iter().take(limit) {
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            proposals.push(parse_memory_proposal(&self.workspace_root, &path, &content));
        }
        Ok(proposals)
    }

    pub async fn merge_memory_proposal(&self, relative_path: &str) -> Result<PathBuf> {
        let _guard = self.proposal_lock.lock().await;
        let path = self.resolve_relative_file(relative_path)?;
        ensure_path_is_under(&path, &self.memory_proposals_dir())?;
        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let proposal = parse_memory_proposal(&self.workspace_root, &path, &content);
        append_memory_entries(self.memory_path(), &proposal.entries).await?;
        let archived = self.archive_memory_proposal(&path, MERGED_DIR).await?;
        Ok(archived)
    }

    pub async fn reject_memory_proposal(&self, relative_path: &str) -> Result<PathBuf> {
        let _guard = self.proposal_lock.lock().await;
        let path = self.resolve_relative_file(relative_path)?;
        ensure_path_is_under(&path, &self.memory_proposals_dir())?;
        self.archive_memory_proposal(&path, REJECTED_DIR).await
    }

    pub async fn write_active(&self, snapshot: &ActiveContinuitySnapshot) -> Result<()> {
        let mut sections = vec!["# Active Context".to_string()];
        if snapshot.matters_today.is_empty()
            && snapshot.open_loops.is_empty()
            && snapshot.pending_approvals.is_empty()
            && snapshot.pending_proposals.is_empty()
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
            push_list_section(
                &mut sections,
                "Pending Proposals",
                &snapshot.pending_proposals,
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
        files.retain(|path| is_direct_child(path, &self.open_loops_dir()));

        let mut loops = Vec::new();
        for path in files {
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            let title = extract_heading(&content).unwrap_or_else(|| stem_fallback(&path));
            loops.push(ContinuityOpenLoop {
                title,
                relative_path: relative_path(&self.workspace_root, &path),
                summary: metadata_value(&content, "Summary"),
                next_step: metadata_value(&content, "Next Step"),
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

    pub async fn status(&self) -> Result<ContinuityStatus> {
        let latest_daily_path = self.latest_daily_note_path().await?;
        Ok(ContinuityStatus {
            memory_path: relative_path(&self.workspace_root, &self.memory_path()),
            active_path: relative_path(&self.workspace_root, &self.active_path()),
            latest_daily_path,
            open_loops: self.list_active_open_loops().await?,
            recent_artifacts: self.list_recent_artifacts(5).await?,
            memory_proposals: self.list_memory_proposals(20).await?,
        })
    }

    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<ContinuitySearchMatch>> {
        let needle = query.trim().to_lowercase();
        if needle.is_empty() {
            bail!("continuity search query cannot be empty");
        }

        let mut paths = vec![self.memory_path()];
        paths.extend(list_markdown_files(&self.continuity_dir()).await?);
        paths.sort();

        let mut matches = Vec::new();
        for path in paths {
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            if let Some(snippet) = search_snippet(&content, &needle) {
                matches.push(ContinuitySearchMatch {
                    title: extract_heading(&content).unwrap_or_else(|| stem_fallback(&path)),
                    relative_path: relative_path(&self.workspace_root, &path),
                    snippet,
                });
            }
            if matches.len() >= limit {
                break;
            }
        }
        Ok(matches)
    }

    pub async fn read_relative(&self, relative_path: &str) -> Result<String> {
        let path = self.resolve_relative_file(relative_path)?;
        tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))
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

    fn open_loop_path(&self, title: &str) -> PathBuf {
        self.open_loops_dir()
            .join(format!("{}.md", sanitize_slug(title)))
    }

    fn memory_proposal_path(&self, title: &str) -> PathBuf {
        self.memory_proposals_dir()
            .join(format!("{}.md", sanitize_slug(title)))
    }

    async fn archive_memory_proposal(&self, source: &Path, status: &str) -> Result<PathBuf> {
        let mut content = tokio::fs::read_to_string(source)
            .await
            .with_context(|| format!("failed to read {}", source.display()))?;
        content = replace_or_insert_metadata(
            &content,
            "Status",
            status.to_string(),
            "Proposed",
            format!("{} UTC", Utc::now().to_rfc3339()),
        );

        let target = self.archived_memory_proposals_dir(status).join(
            source
                .file_name()
                .ok_or_else(|| anyhow!("invalid proposal path"))?,
        );
        tokio::fs::write(source, &content)
            .await
            .with_context(|| format!("failed to update {}", source.display()))?;
        tokio::fs::rename(source, &target)
            .await
            .with_context(|| format!("failed to archive {}", source.display()))?;
        Ok(target)
    }

    async fn latest_daily_note_path(&self) -> Result<Option<String>> {
        let mut files = list_markdown_files(&self.daily_dir()).await?;
        files.sort();
        Ok(files
            .into_iter()
            .last()
            .map(|path| relative_path(&self.workspace_root, &path)))
    }

    fn resolve_relative_file(&self, relative_path: &str) -> Result<PathBuf> {
        let trimmed = relative_path.trim();
        if trimmed.is_empty() {
            bail!("continuity path cannot be empty");
        }
        let requested = Path::new(trimmed);
        if requested.is_absolute() {
            bail!("continuity path must be relative to assistant home");
        }
        for component in requested.components() {
            match component {
                Component::Normal(_) => {}
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    bail!("continuity path '{}' is invalid", trimmed);
                }
            }
        }
        Ok(self.workspace_root.join(requested))
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

async fn append_memory_entries(path: PathBuf, entries: &[String]) -> Result<()> {
    let existing = tokio::fs::read_to_string(&path)
        .await
        .unwrap_or_else(|_| MEMORY_TEMPLATE.to_string());
    let mut deduped_existing = BTreeSet::new();
    for line in existing.lines() {
        let trimmed = line.trim();
        if let Some(entry) = trimmed.strip_prefix("- ") {
            deduped_existing.insert(entry.trim().to_string());
        }
    }

    let new_entries = entries
        .iter()
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .filter(|entry| !deduped_existing.contains(*entry))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if new_entries.is_empty() {
        return Ok(());
    }

    let mut content = existing;
    if !content.contains("## Entries") {
        if !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str("\n## Entries\n");
    }
    if !content.ends_with('\n') {
        content.push('\n');
    }
    for entry in new_entries {
        content.push_str(&format!("- {}\n", entry));
    }
    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("failed to write {}", path.display()))
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

fn metadata_value(content: &str, key: &str) -> Option<String> {
    let prefix = format!("- {}:", key);
    content
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(&prefix))
        .map(str::trim)
        .map(ToString::to_string)
        .filter(|value| !value.is_empty())
}

fn bullet_section(content: &str, title: &str) -> Vec<String> {
    let marker = format!("## {}", title);
    let Some((_, tail)) = content.split_once(&marker) else {
        return Vec::new();
    };
    let mut values = Vec::new();
    for line in tail.lines().skip(1) {
        let trimmed = line.trim();
        if trimmed.starts_with("## ") {
            break;
        }
        if let Some(value) = trimmed.strip_prefix("- ") {
            let value = value.trim();
            if !value.is_empty() {
                values.push(value.to_string());
            }
        }
    }
    values
}

fn parse_memory_proposal(root: &Path, path: &Path, content: &str) -> ContinuityMemoryProposal {
    ContinuityMemoryProposal {
        title: extract_heading(content)
            .unwrap_or_else(|| stem_fallback(path))
            .trim_start_matches("Memory Proposal: ")
            .to_string(),
        relative_path: relative_path(root, path),
        rationale: metadata_value(content, "Rationale"),
        entries: bullet_section(content, "Candidate Entries"),
    }
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

fn dedupe_lines(values: &[&str]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_string()) {
            continue;
        }
        deduped.push(trimmed.to_string());
    }
    deduped
}

fn search_snippet(content: &str, needle: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.to_lowercase().contains(needle) && !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let lower = content.to_lowercase();
    let offset = lower.find(needle)?;
    let start = offset.saturating_sub(60);
    let end = (offset + needle.len() + 60).min(content.len());
    Some(
        content[start..end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn is_direct_child(path: &Path, root: &Path) -> bool {
    path.parent().is_some_and(|parent| parent == root)
}

fn ensure_path_is_under(path: &Path, root: &Path) -> Result<()> {
    let canonical_root = root
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", root.display()))?;
    let canonical_path = path
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", path.display()))?;
    if canonical_path == canonical_root || canonical_path.starts_with(&canonical_root) {
        Ok(())
    } else {
        bail!("path '{}' is outside '{}'", path.display(), root.display());
    }
}

fn replace_or_insert_metadata(
    content: &str,
    primary_key: &str,
    primary_value: String,
    secondary_key: &str,
    secondary_value: String,
) -> String {
    let mut lines = content.lines().map(ToString::to_string).collect::<Vec<_>>();
    let primary_prefix = format!("- {}:", primary_key);
    let secondary_prefix = format!("- {}:", secondary_key);
    let mut primary_found = false;
    let mut secondary_found = false;

    for line in &mut lines {
        let trimmed = line.trim();
        if trimmed.starts_with(&primary_prefix) {
            *line = format!("- {}: {}", primary_key, primary_value);
            primary_found = true;
        } else if trimmed.starts_with(&secondary_prefix) {
            *line = format!("- {}: {}", secondary_key, secondary_value);
            secondary_found = true;
        }
    }

    let insert_at = lines
        .iter()
        .position(|line| line.trim().starts_with("## "))
        .unwrap_or(lines.len());
    if !primary_found {
        lines.insert(insert_at, format!("- {}: {}", primary_key, primary_value));
    }
    if !secondary_found {
        let secondary_insert_at = lines
            .iter()
            .position(|line| line.trim().starts_with("## "))
            .unwrap_or(lines.len());
        lines.insert(
            secondary_insert_at + usize::from(primary_found),
            format!("- {}: {}", secondary_key, secondary_value),
        );
    }

    let mut rendered = lines.join("\n");
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    rendered
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{
        continuity_prompt_sections, ActiveContinuitySnapshot, ContinuityArtifact, ContinuityEvent,
        ContinuityLayout, ContinuityMemoryProposalDraft, ContinuityOpenLoopDraft,
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
        assert!(layout.continuity_dir().join("proposals/memory").exists());

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
                pending_proposals: vec!["Remember release checklist preference".to_string()],
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
        assert!(active_content.contains("Pending Proposals"));
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

    #[tokio::test]
    async fn memory_proposals_merge_and_archive_cleanly() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let proposal_path = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release Preferences".to_string(),
                rationale: "operator preference".to_string(),
                entries: vec![
                    "Prefers short release notes.".to_string(),
                    "Prefers short release notes.".to_string(),
                    "Ships from clean worktrees.".to_string(),
                ],
                source: Some("session:123".to_string()),
            })
            .await
            .expect("record proposal")
            .expect("proposal path");
        let relative = proposal_path
            .strip_prefix(layout.workspace_root())
            .expect("relative proposal")
            .to_string_lossy()
            .to_string();

        let merged_path = layout
            .merge_memory_proposal(&relative)
            .await
            .expect("merge proposal");
        let memory = tokio::fs::read_to_string(layout.memory_path())
            .await
            .expect("read memory");

        assert!(memory.contains("Prefers short release notes."));
        assert!(memory.contains("Ships from clean worktrees."));
        assert!(merged_path.exists());
        assert!(!proposal_path.exists());
    }

    #[tokio::test]
    async fn open_loops_status_and_search_are_visible() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");
        let loop_path = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Follow Up On Release Checklist".to_string(),
                summary: "Need to confirm final release steps".to_string(),
                next_step: "Draft the minimal checklist update".to_string(),
                source: Some("session:abc".to_string()),
            })
            .await
            .expect("upsert loop");
        assert!(loop_path.is_some());

        let proposals = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Rust Preferences".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Prefers Rust CLIs.".to_string()],
                source: None,
            })
            .await
            .expect("record proposal");
        assert!(proposals.is_some());

        let status = layout.status().await.expect("status");
        assert_eq!(status.open_loops.len(), 1);
        assert_eq!(status.memory_proposals.len(), 1);

        let matches = layout
            .search("release", 10)
            .await
            .expect("search continuity");
        assert!(!matches.is_empty());
        assert!(matches
            .iter()
            .any(|item| item.relative_path.contains("open-loops")));

        let loop_path = status.open_loops[0].relative_path.clone();
        let content = layout.read_relative(&loop_path).await.expect("read loop");
        assert!(content.contains("Follow Up On Release Checklist"));

        let archived = layout
            .resolve_open_loop(&loop_path)
            .await
            .expect("resolve loop");
        assert!(archived.exists());
        assert_eq!(
            layout
                .list_active_open_loops()
                .await
                .expect("list loops")
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn unchanged_proposals_and_loops_do_not_rewrite() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let first_proposal = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Stable Preference".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Prefers direct kernels.".to_string()],
                source: Some("session:1".to_string()),
            })
            .await
            .expect("record proposal");
        let second_proposal = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Stable Preference".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Prefers direct kernels.".to_string()],
                source: Some("session:1".to_string()),
            })
            .await
            .expect("record proposal");
        assert!(first_proposal.is_some());
        assert!(second_proposal.is_none());

        let first_loop = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Review Continuity Surface".to_string(),
                summary: "Need a final QA pass".to_string(),
                next_step: "Run cargo test".to_string(),
                source: Some("session:1".to_string()),
            })
            .await
            .expect("upsert loop");
        let second_loop = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Review Continuity Surface".to_string(),
                summary: "Need a final QA pass".to_string(),
                next_step: "Run cargo test".to_string(),
                source: Some("session:1".to_string()),
            })
            .await
            .expect("upsert loop");
        assert!(first_loop.is_some());
        assert!(second_loop.is_none());
    }
}
