use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Datelike, Utc};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::continuity_fs::ContinuityFs;
use super::continuity_index::{ContinuityIndexStore, ContinuityIndexedDocument};

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
    index_store: Option<ContinuityIndexStore>,
    fs: Arc<Mutex<Option<Arc<ContinuityFs>>>>,
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
pub struct ContinuityItemMetadata {
    pub title: String,
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
        Self::with_index_store(workspace_root, None)
    }

    pub fn with_index_store(
        workspace_root: impl Into<PathBuf>,
        index_store: Option<ContinuityIndexStore>,
    ) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            index_store,
            fs: Arc::new(Mutex::new(None)),
            daily_note_lock: Arc::new(Mutex::new(())),
            proposal_lock: Arc::new(Mutex::new(())),
            open_loop_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn memory_path(&self) -> PathBuf {
        self.workspace_root.join(self.memory_rel_path())
    }

    pub fn continuity_dir(&self) -> PathBuf {
        self.workspace_root.join(self.continuity_rel_dir())
    }

    pub fn active_path(&self) -> PathBuf {
        self.workspace_root.join(self.active_rel_path())
    }

    #[cfg(test)]
    fn daily_dir(&self) -> PathBuf {
        self.workspace_root.join(self.daily_rel_dir())
    }

    #[cfg(test)]
    fn open_loops_dir(&self) -> PathBuf {
        self.workspace_root.join(self.open_loops_rel_dir())
    }

    #[cfg(test)]
    fn artifacts_dir(&self) -> PathBuf {
        self.workspace_root.join(self.artifacts_rel_dir())
    }

    #[cfg(test)]
    fn memory_proposals_dir(&self) -> PathBuf {
        self.workspace_root.join(self.memory_proposals_rel_dir())
    }

    fn memory_rel_path(&self) -> PathBuf {
        PathBuf::from(MEMORY_FILE)
    }

    fn continuity_rel_dir(&self) -> PathBuf {
        PathBuf::from(CONTINUITY_DIR)
    }

    fn active_rel_path(&self) -> PathBuf {
        self.continuity_rel_dir().join(ACTIVE_FILE)
    }

    fn daily_rel_dir(&self) -> PathBuf {
        self.continuity_rel_dir().join(DAILY_DIR)
    }

    fn open_loops_rel_dir(&self) -> PathBuf {
        self.continuity_rel_dir().join(OPEN_LOOPS_DIR)
    }

    fn open_loops_archive_rel_dir(&self) -> PathBuf {
        self.open_loops_rel_dir().join(ARCHIVE_DIR)
    }

    fn artifacts_rel_dir(&self) -> PathBuf {
        self.continuity_rel_dir().join(ARTIFACTS_DIR)
    }

    fn rollups_rel_dir(&self) -> PathBuf {
        self.continuity_rel_dir().join(ROLLUPS_DIR)
    }

    fn proposals_rel_dir(&self) -> PathBuf {
        self.continuity_rel_dir().join(PROPOSALS_DIR)
    }

    fn memory_proposals_rel_dir(&self) -> PathBuf {
        self.proposals_rel_dir().join(MEMORY_PROPOSALS_DIR)
    }

    fn archived_memory_proposals_rel_dir(&self, status: &str) -> PathBuf {
        self.memory_proposals_rel_dir()
            .join(ARCHIVE_DIR)
            .join(status)
    }

    async fn fs(&self) -> Result<Arc<ContinuityFs>> {
        let mut guard = self.fs.lock().await;
        if guard.is_none() {
            *guard = Some(Arc::new(ContinuityFs::bootstrap(&self.workspace_root)?));
        }
        Ok(Arc::clone(
            guard.as_ref().expect("continuity fs initialized"),
        ))
    }

    pub async fn ensure_base_layout(&self) -> Result<()> {
        let fs = self.fs().await?;
        for dir in [
            self.continuity_rel_dir(),
            self.daily_rel_dir(),
            self.open_loops_rel_dir(),
            self.open_loops_archive_rel_dir(),
            self.artifacts_rel_dir(),
            self.rollups_rel_dir(),
            self.memory_proposals_rel_dir(),
            self.archived_memory_proposals_rel_dir(MERGED_DIR),
            self.archived_memory_proposals_rel_dir(REJECTED_DIR),
        ] {
            fs.create_dir_all(&dir)?;
        }

        fs.ensure_file(&self.memory_rel_path(), MEMORY_TEMPLATE)?;
        fs.ensure_file(&self.active_rel_path(), ACTIVE_TEMPLATE)?;
        self.rebuild_index().await?;
        Ok(())
    }

    pub async fn append_daily_event(&self, event: ContinuityEvent) -> Result<PathBuf> {
        let _guard = self.daily_note_lock.lock().await;
        let fs = self.fs().await?;
        let relative = self.daily_note_path(event.at);

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

        fs.append_string_with_header(
            &relative,
            Some(&format!(
                "# Daily Continuity {}\n\n",
                event.at.format("%Y-%m-%d")
            )),
            &entry,
        )?;
        self.sync_index_relative(&relative).await?;
        Ok(fs.absolute_path(&relative))
    }

    pub async fn record_artifact(&self, artifact: ContinuityArtifact) -> Result<PathBuf> {
        let fs = self.fs().await?;
        let relative = self.artifact_path(artifact.at, &artifact.slug);

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

        fs.write_string(&relative, &content)?;
        self.sync_index_relative(&relative).await?;
        Ok(fs.absolute_path(&relative))
    }

    pub async fn upsert_open_loop(
        &self,
        loop_draft: &ContinuityOpenLoopDraft,
    ) -> Result<Option<PathBuf>> {
        let _guard = self.open_loop_lock.lock().await;
        let fs = self.fs().await?;
        let relative = self.open_loop_path(&loop_draft.title);

        if let Ok(existing) = fs.read_to_string(&relative) {
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

        fs.write_string(&relative, &content)?;
        self.sync_index_relative(&relative).await?;
        Ok(Some(fs.absolute_path(&relative)))
    }

    pub async fn resolve_open_loop(&self, relative_path: &str) -> Result<PathBuf> {
        let _guard = self.open_loop_lock.lock().await;
        let fs = self.fs().await?;
        let source = self.resolve_relative_file(relative_path)?;
        ensure_relative_path_is_direct_child(&source, &self.open_loops_rel_dir())?;
        let mut content = fs.read_to_string(&source)?;
        content = replace_or_insert_metadata(
            &content,
            "Status",
            "resolved".to_string(),
            "Updated",
            format!("{} UTC", Utc::now().to_rfc3339()),
        );

        let target = self
            .open_loops_archive_rel_dir()
            .join(archive_file_name(&source)?);
        fs.write_string(&source, &content)?;
        fs.rename(&source, &target)?;
        self.remove_index_relative(&source).await?;
        self.sync_index_relative(&target).await?;
        Ok(fs.absolute_path(&target))
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
        let fs = self.fs().await?;
        let relative = self.memory_proposal_path(&proposal.title);

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

        let existing = fs.read_to_string(&relative).ok();
        if let Some(existing) = existing {
            let parsed = parse_memory_proposal(
                &self.workspace_root,
                &fs.absolute_path(&relative),
                &existing,
            );
            let same_title = parsed.title.trim() == proposal.title.trim();
            let same_rationale =
                parsed.rationale.unwrap_or_default().trim() == proposal.rationale.trim();
            let same_entries = parsed.entries == entries;
            if same_title && same_rationale && same_entries {
                return Ok(None);
            }
        }

        fs.write_string(&relative, &content)?;
        self.sync_index_relative(&relative).await?;
        Ok(Some(fs.absolute_path(&relative)))
    }

    pub async fn list_memory_proposals(
        &self,
        limit: usize,
    ) -> Result<Vec<ContinuityMemoryProposal>> {
        let fs = self.fs().await?;
        let mut files = fs.list_markdown_files(&self.memory_proposals_rel_dir())?;
        files.retain(|path| is_direct_child(path, &self.memory_proposals_rel_dir()));
        files = self.sort_relative_paths_by_modified_desc(files).await?;

        let mut proposals = Vec::new();
        for path in files.into_iter().take(limit) {
            let content = fs.read_to_string(&path)?;
            proposals.push(parse_memory_proposal(
                &self.workspace_root,
                &fs.absolute_path(&path),
                &content,
            ));
        }
        Ok(proposals)
    }

    pub async fn merge_memory_proposal(&self, relative_path: &str) -> Result<PathBuf> {
        let _guard = self.proposal_lock.lock().await;
        let fs = self.fs().await?;
        let path = self.resolve_relative_file(relative_path)?;
        ensure_relative_path_is_direct_child(&path, &self.memory_proposals_rel_dir())?;
        let content = fs.read_to_string(&path)?;
        let proposal =
            parse_memory_proposal(&self.workspace_root, &fs.absolute_path(&path), &content);
        self.append_memory_entries(&proposal.entries).await?;
        self.sync_index_relative(&self.memory_rel_path()).await?;
        let archived = self.archive_memory_proposal(&path, MERGED_DIR).await?;
        Ok(archived)
    }

    pub async fn reject_memory_proposal(&self, relative_path: &str) -> Result<PathBuf> {
        let _guard = self.proposal_lock.lock().await;
        let path = self.resolve_relative_file(relative_path)?;
        ensure_relative_path_is_direct_child(&path, &self.memory_proposals_rel_dir())?;
        self.archive_memory_proposal(&path, REJECTED_DIR).await
    }

    pub async fn write_active(&self, snapshot: &ActiveContinuitySnapshot) -> Result<()> {
        let fs = self.fs().await?;
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

        fs.write_string(&self.active_rel_path(), &(sections.join("\n\n") + "\n"))?;
        self.sync_index_relative(&self.active_rel_path()).await?;
        Ok(())
    }

    pub async fn list_active_open_loops(&self) -> Result<Vec<ContinuityOpenLoop>> {
        let fs = self.fs().await?;
        let mut files = fs.list_markdown_files(&self.open_loops_rel_dir())?;
        files.retain(|path| is_direct_child(path, &self.open_loops_rel_dir()));
        files = self.sort_relative_paths_by_modified_desc(files).await?;

        let mut loops = Vec::new();
        for path in files {
            let content = fs.read_to_string(&path)?;
            let title = extract_heading(&content).unwrap_or_else(|| stem_fallback(&path));
            loops.push(ContinuityOpenLoop {
                title,
                relative_path: path.to_string_lossy().to_string(),
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
        let fs = self.fs().await?;
        let mut files = fs.list_markdown_files(&self.artifacts_rel_dir())?;
        files = self.sort_relative_paths_by_modified_desc(files).await?;

        let mut artifacts = Vec::new();
        for path in files.into_iter().take(limit) {
            let content = fs.read_to_string(&path)?;
            artifacts.push(ContinuityArtifactSummary {
                title: extract_heading(&content).unwrap_or_else(|| stem_fallback(&path)),
                relative_path: path.to_string_lossy().to_string(),
            });
        }
        Ok(artifacts)
    }

    pub async fn status(&self) -> Result<ContinuityStatus> {
        let latest_daily_path = self.latest_daily_note_path().await?;
        Ok(ContinuityStatus {
            memory_path: self.memory_rel_path().to_string_lossy().to_string(),
            active_path: self.active_rel_path().to_string_lossy().to_string(),
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

        if let Some(index_store) = &self.index_store {
            self.rebuild_index().await?;
            let indexed = index_store.search(&needle, limit).await?;
            if !indexed.is_empty() {
                return Ok(indexed
                    .into_iter()
                    .map(|item| ContinuitySearchMatch {
                        relative_path: item.relative_path,
                        title: item.title,
                        snippet: item.snippet,
                    })
                    .collect());
            }
        }

        let fs = self.fs().await?;
        let mut paths = vec![self.memory_rel_path()];
        paths.extend(fs.list_markdown_files(&self.continuity_rel_dir())?);
        paths = self.sort_relative_paths_by_modified_desc(paths).await?;

        let mut matches = Vec::new();
        for path in paths {
            let content = fs.read_to_string(&path)?;
            if let Some(snippet) = search_snippet(&content, &needle) {
                matches.push(ContinuitySearchMatch {
                    title: extract_heading(&content).unwrap_or_else(|| stem_fallback(&path)),
                    relative_path: path.to_string_lossy().to_string(),
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
        let fs = self.fs().await?;
        let path = self.resolve_canonical_read_path(relative_path)?;
        fs.read_to_string(&path)
    }

    pub async fn memory_proposal_metadata(
        &self,
        relative_path: &str,
    ) -> Result<ContinuityItemMetadata> {
        let fs = self.fs().await?;
        let path = self.resolve_relative_file(relative_path)?;
        ensure_relative_path_is_direct_child(&path, &self.memory_proposals_rel_dir())?;
        let content = fs.read_to_string(&path)?;
        let proposal =
            parse_memory_proposal(&self.workspace_root, &fs.absolute_path(&path), &content);
        Ok(ContinuityItemMetadata {
            title: proposal.title,
        })
    }

    pub async fn open_loop_metadata(&self, relative_path: &str) -> Result<ContinuityItemMetadata> {
        let fs = self.fs().await?;
        let path = self.resolve_relative_file(relative_path)?;
        ensure_relative_path_is_direct_child(&path, &self.open_loops_rel_dir())?;
        let content = fs.read_to_string(&path)?;
        Ok(ContinuityItemMetadata {
            title: extract_heading(&content).unwrap_or_else(|| stem_fallback(&path)),
        })
    }

    fn daily_note_path(&self, at: DateTime<Utc>) -> PathBuf {
        self.daily_rel_dir()
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
        self.artifacts_rel_dir()
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
        self.open_loops_rel_dir().join(title_file_name(title))
    }

    fn memory_proposal_path(&self, title: &str) -> PathBuf {
        self.memory_proposals_rel_dir().join(title_file_name(title))
    }

    async fn archive_memory_proposal(&self, source: &Path, status: &str) -> Result<PathBuf> {
        let fs = self.fs().await?;
        let mut content = fs.read_to_string(source)?;
        content = replace_or_insert_metadata(
            &content,
            "Status",
            status.to_string(),
            "Proposed",
            format!("{} UTC", Utc::now().to_rfc3339()),
        );

        let target = self
            .archived_memory_proposals_rel_dir(status)
            .join(archive_file_name(source)?);
        fs.write_string(source, &content)?;
        fs.rename(source, &target)?;
        self.remove_index_relative(source).await?;
        self.sync_index_relative(&target).await?;
        Ok(fs.absolute_path(&target))
    }

    async fn latest_daily_note_path(&self) -> Result<Option<String>> {
        let fs = self.fs().await?;
        let mut files = fs.list_markdown_files(&self.daily_rel_dir())?;
        files = self.sort_relative_paths_by_modified_desc(files).await?;
        Ok(files
            .into_iter()
            .next()
            .map(|path| path.to_string_lossy().to_string()))
    }

    async fn rebuild_index(&self) -> Result<()> {
        let Some(index_store) = &self.index_store else {
            return Ok(());
        };
        let fs = self.fs().await?;
        let mut paths = vec![self.memory_rel_path()];
        paths.extend(fs.list_markdown_files(&self.continuity_rel_dir())?);
        let mut documents = Vec::new();
        for path in paths {
            documents.push(self.read_index_document(&path).await?);
        }
        index_store.replace_all(&documents).await
    }

    async fn sync_index_relative(&self, path: &Path) -> Result<()> {
        let Some(index_store) = &self.index_store else {
            return Ok(());
        };
        let fs = self.fs().await?;
        if let Err(err) = fs.read_to_string(path) {
            if err.downcast_ref::<std::io::Error>().is_some()
                || err.downcast_ref::<rustix::io::Errno>().is_some()
            {
                self.remove_index_relative(path).await?;
                return Ok(());
            }
            return Err(err);
        }
        let document = self.read_index_document(path).await?;
        index_store.upsert(&document).await
    }

    async fn remove_index_relative(&self, path: &Path) -> Result<()> {
        let Some(index_store) = &self.index_store else {
            return Ok(());
        };
        index_store.remove(&path.to_string_lossy()).await
    }

    async fn read_index_document(&self, path: &Path) -> Result<ContinuityIndexedDocument> {
        let fs = self.fs().await?;
        let body = fs.read_to_string(path)?;
        let title = extract_heading(&body).unwrap_or_else(|| stem_fallback(path));
        Ok(ContinuityIndexedDocument {
            relative_path: path.to_string_lossy().to_string(),
            title,
            body,
            updated_at_ms: fs.modified_at_ms(path)?,
        })
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
        let mut normalized = PathBuf::new();
        for component in requested.components() {
            match component {
                Component::Normal(value) => normalized.push(value),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    bail!("continuity path '{}' is invalid", trimmed);
                }
            }
        }
        if normalized.as_os_str().is_empty() {
            bail!("continuity path '{}' is invalid", trimmed);
        }
        Ok(normalized)
    }

    fn resolve_canonical_read_path(&self, relative_path: &str) -> Result<PathBuf> {
        let path = self.resolve_relative_file(relative_path)?;
        if path == self.memory_rel_path() || path.starts_with(self.continuity_rel_dir()) {
            Ok(path)
        } else {
            bail!(
                "continuity path '{}' is outside canonical continuity files",
                relative_path
            );
        }
    }

    async fn append_memory_entries(&self, entries: &[String]) -> Result<()> {
        let fs = self.fs().await?;
        let path = self.memory_rel_path();
        let existing = fs
            .read_to_string(&path)
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
        fs.write_string(&path, &content)
    }
}

impl ContinuityLayout {
    pub async fn read_prompt_sections(&self) -> Result<Vec<(String, String)>> {
        let fs = self.fs().await?;
        let mut sections = Vec::new();
        for (label, path) in [
            ("MEMORY.md".to_string(), self.memory_rel_path()),
            ("continuity/ACTIVE.md".to_string(), self.active_rel_path()),
        ] {
            if let Some(content) = fs.read_to_string_if_exists(&path)? {
                sections.push((label, content));
            }
        }
        Ok(sections)
    }

    async fn sort_relative_paths_by_modified_desc(
        &self,
        paths: Vec<PathBuf>,
    ) -> Result<Vec<PathBuf>> {
        let fs = self.fs().await?;
        let mut dated = Vec::with_capacity(paths.len());
        for path in paths {
            dated.push((fs.modified_at_ms(&path)?, path));
        }
        dated.sort_by(|left, right| right.cmp(left));
        Ok(dated.into_iter().map(|(_, path)| path).collect())
    }
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

pub(crate) fn normalized_title_key(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn title_file_name(title: &str) -> String {
    let normalized = normalized_title_key(title);
    let slug = sanitize_slug(&normalized);
    let slug = if slug.is_empty() {
        "item"
    } else {
        slug.as_str()
    };
    let key = Uuid::new_v5(
        &Uuid::from_u128(0x5f026ae9551b4d3ea511f0f2d74cf241),
        normalized.as_bytes(),
    );
    format!("{}--{}.md", slug, key)
}

fn archive_file_name(source: &Path) -> Result<String> {
    let stem = source
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("continuity archive path '{}' is invalid", source.display()))?;
    Ok(format!("{}--{}.md", stem, Uuid::new_v4()))
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

fn ensure_relative_path_is_direct_child(path: &Path, root: &Path) -> Result<()> {
    if is_direct_child(path, root) {
        Ok(())
    } else {
        bail!(
            "path '{}' is not an active child of '{}'",
            path.display(),
            root.display()
        );
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
    use tempfile::tempdir;

    use super::{
        title_file_name, ActiveContinuitySnapshot, ContinuityArtifact, ContinuityEvent,
        ContinuityLayout, ContinuityMemoryProposalDraft, ContinuityOpenLoopDraft, MERGED_DIR,
    };
    use crate::kernel::{continuity_index::ContinuityIndexStore, db::Db};

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

        let prompt_sections = layout
            .read_prompt_sections()
            .await
            .expect("prompt sections");
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
    async fn repeated_same_title_archives_preserve_history() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let merged_archive_dir = layout
            .workspace_root()
            .join(layout.archived_memory_proposals_rel_dir(MERGED_DIR));
        let loop_archive_dir = layout
            .workspace_root()
            .join(layout.open_loops_archive_rel_dir());

        let first_proposal = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release Preferences".to_string(),
                rationale: "first".to_string(),
                entries: vec!["Keep releases small.".to_string()],
                source: Some("session:first".to_string()),
            })
            .await
            .expect("record first proposal")
            .expect("first proposal path");
        let first_proposal_relative = first_proposal
            .strip_prefix(layout.workspace_root())
            .expect("first proposal relative")
            .to_string_lossy()
            .to_string();
        let first_merged = layout
            .merge_memory_proposal(&first_proposal_relative)
            .await
            .expect("merge first proposal");

        let second_proposal = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release Preferences".to_string(),
                rationale: "second".to_string(),
                entries: vec!["Verify release notes before tagging.".to_string()],
                source: Some("session:second".to_string()),
            })
            .await
            .expect("record second proposal")
            .expect("second proposal path");
        let second_proposal_relative = second_proposal
            .strip_prefix(layout.workspace_root())
            .expect("second proposal relative")
            .to_string_lossy()
            .to_string();
        let second_merged = layout
            .merge_memory_proposal(&second_proposal_relative)
            .await
            .expect("merge second proposal");

        assert_ne!(first_merged, second_merged);
        assert_eq!(
            std::fs::read_dir(&merged_archive_dir)
                .expect("read merged archive dir")
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "md"))
                .count(),
            2
        );

        let first_loop = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Follow Up On Release Checklist".to_string(),
                summary: "first summary".to_string(),
                next_step: "first next step".to_string(),
                source: Some("session:first".to_string()),
            })
            .await
            .expect("upsert first loop")
            .expect("first loop path");
        let first_loop_relative = first_loop
            .strip_prefix(layout.workspace_root())
            .expect("first loop relative")
            .to_string_lossy()
            .to_string();
        let first_resolved = layout
            .resolve_open_loop(&first_loop_relative)
            .await
            .expect("resolve first loop");

        let second_loop = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Follow Up On Release Checklist".to_string(),
                summary: "second summary".to_string(),
                next_step: "second next step".to_string(),
                source: Some("session:second".to_string()),
            })
            .await
            .expect("upsert second loop")
            .expect("second loop path");
        let second_loop_relative = second_loop
            .strip_prefix(layout.workspace_root())
            .expect("second loop relative")
            .to_string_lossy()
            .to_string();
        let second_resolved = layout
            .resolve_open_loop(&second_loop_relative)
            .await
            .expect("resolve second loop");

        assert_ne!(first_resolved, second_resolved);
        assert_eq!(
            std::fs::read_dir(&loop_archive_dir)
                .expect("read loop archive dir")
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "md"))
                .count(),
            2
        );
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

    #[tokio::test]
    async fn archived_items_are_history_only_and_same_title_items_can_reappear() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let proposal_path = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release Preferences".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Keep releases concise.".to_string()],
                source: Some("session:1".to_string()),
            })
            .await
            .expect("record proposal")
            .expect("proposal path");
        let proposal_relative = proposal_path
            .strip_prefix(layout.workspace_root())
            .expect("proposal relative")
            .to_string_lossy()
            .to_string();
        let archived_proposal = layout
            .merge_memory_proposal(&proposal_relative)
            .await
            .expect("merge proposal");
        let recreated_proposal = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release Preferences".to_string(),
                rationale: "fresh preference".to_string(),
                entries: vec!["Keep releases even shorter.".to_string()],
                source: Some("session:2".to_string()),
            })
            .await
            .expect("recreate proposal")
            .expect("recreated proposal path");
        assert_ne!(
            archived_proposal.file_name(),
            recreated_proposal.file_name()
        );
        assert_eq!(
            recreated_proposal
                .file_name()
                .and_then(|value| value.to_str()),
            Some(title_file_name("Release Preferences").as_str()),
            "same title should map back to the canonical active filename"
        );

        let loop_path = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Follow Up On Release Checklist".to_string(),
                summary: "Need the final review.".to_string(),
                next_step: "Review release checklist".to_string(),
                source: Some("session:1".to_string()),
            })
            .await
            .expect("record open loop")
            .expect("open loop path");
        let loop_relative = loop_path
            .strip_prefix(layout.workspace_root())
            .expect("loop relative")
            .to_string_lossy()
            .to_string();
        let archived_loop = layout
            .resolve_open_loop(&loop_relative)
            .await
            .expect("resolve loop");
        let recreated_loop = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Follow Up On Release Checklist".to_string(),
                summary: "Need a second pass.".to_string(),
                next_step: "Run the release review again".to_string(),
                source: Some("session:2".to_string()),
            })
            .await
            .expect("recreate open loop")
            .expect("recreated loop path");
        assert_ne!(archived_loop.file_name(), recreated_loop.file_name());
        assert_eq!(
            recreated_loop.file_name().and_then(|value| value.to_str()),
            Some(title_file_name("Follow Up On Release Checklist").as_str())
        );
    }

    #[tokio::test]
    async fn different_titles_with_same_slug_get_distinct_canonical_files() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let first = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release/Checklist".to_string(),
                rationale: "first".to_string(),
                entries: vec!["First entry.".to_string()],
                source: None,
            })
            .await
            .expect("record first")
            .expect("first path");
        let second = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release Checklist".to_string(),
                rationale: "second".to_string(),
                entries: vec!["Second entry.".to_string()],
                source: None,
            })
            .await
            .expect("record second")
            .expect("second path");

        assert_ne!(first, second);
        assert!(first
            .file_name()
            .expect("first file")
            .to_string_lossy()
            .starts_with("release-checklist--"));
        assert!(second
            .file_name()
            .expect("second file")
            .to_string_lossy()
            .starts_with("release-checklist--"));
        assert_ne!(first.file_name(), second.file_name());
    }

    #[tokio::test]
    async fn indexed_search_tracks_archived_and_merged_files() {
        let temp_dir = tempdir().expect("temp dir");
        let db = Db::connect_file(&temp_dir.path().join("lionclaw.db"))
            .await
            .expect("db connect");
        let layout = ContinuityLayout::with_index_store(
            temp_dir.path().join("workspace"),
            Some(ContinuityIndexStore::new(db.pool())),
        );
        layout.ensure_base_layout().await.expect("bootstrap");

        let proposal_path = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Release Preference".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Prefer brief release notes.".to_string()],
                source: None,
            })
            .await
            .expect("record proposal")
            .expect("proposal path");
        let open_loop_path = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Review Continuity Search".to_string(),
                summary: "Need to verify indexed continuity search".to_string(),
                next_step: "Search for continuity".to_string(),
                source: None,
            })
            .await
            .expect("upsert loop")
            .expect("loop path");

        assert!(!layout
            .search("indexed continuity", 10)
            .await
            .expect("search")
            .is_empty());

        let proposal_relative = proposal_path
            .strip_prefix(layout.workspace_root())
            .expect("proposal relative")
            .to_string_lossy()
            .to_string();
        layout
            .merge_memory_proposal(&proposal_relative)
            .await
            .expect("merge proposal");
        let loop_relative = open_loop_path
            .strip_prefix(layout.workspace_root())
            .expect("loop relative")
            .to_string_lossy()
            .to_string();
        layout
            .resolve_open_loop(&loop_relative)
            .await
            .expect("resolve loop");

        let memory_hits = layout
            .search("brief release notes", 10)
            .await
            .expect("search memory");
        assert!(memory_hits
            .iter()
            .any(|item| item.relative_path == "MEMORY.md"));
        let loop_hits = layout
            .search("indexed continuity search", 10)
            .await
            .expect("search loop");
        assert!(loop_hits
            .iter()
            .any(|item| item.relative_path.contains("open-loops/archive")));
    }

    #[tokio::test]
    async fn indexed_search_reflects_manual_file_edits_without_restart() {
        let temp_dir = tempdir().expect("temp dir");
        let db = Db::connect_file(&temp_dir.path().join("lionclaw.db"))
            .await
            .expect("db connect");
        let layout = ContinuityLayout::with_index_store(
            temp_dir.path().join("workspace"),
            Some(ContinuityIndexStore::new(db.pool())),
        );
        layout.ensure_base_layout().await.expect("bootstrap");

        tokio::fs::write(
            layout.memory_path(),
            "# Memory\n\n## Entries\n- Prefer concise reviews.\n",
        )
        .await
        .expect("write memory v1");
        let first_hits = layout
            .search("concise reviews", 10)
            .await
            .expect("search first edit");
        assert!(first_hits
            .iter()
            .any(|item| item.relative_path == "MEMORY.md"));

        tokio::fs::write(
            layout.memory_path(),
            "# Memory\n\n## Entries\n- Prefer exhaustive reviews.\n",
        )
        .await
        .expect("write memory v2");
        assert!(layout
            .search("concise reviews", 10)
            .await
            .expect("search stale phrase")
            .is_empty());
        let second_hits = layout
            .search("exhaustive reviews", 10)
            .await
            .expect("search second edit");
        assert!(second_hits
            .iter()
            .any(|item| item.relative_path == "MEMORY.md"));
    }

    #[tokio::test]
    async fn archived_actions_are_rejected_for_active_only_surfaces() {
        let temp_dir = tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let proposal_path = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Archive Me".to_string(),
                rationale: "proposal".to_string(),
                entries: vec!["entry".to_string()],
                source: None,
            })
            .await
            .expect("record proposal")
            .expect("proposal path");
        let proposal_relative = proposal_path
            .strip_prefix(layout.workspace_root())
            .expect("proposal relative")
            .to_string_lossy()
            .to_string();
        let archived_proposal = layout
            .merge_memory_proposal(&proposal_relative)
            .await
            .expect("merge proposal");
        let archived_proposal_relative = archived_proposal
            .strip_prefix(layout.workspace_root())
            .expect("archived proposal relative")
            .to_string_lossy()
            .to_string();

        let err = layout
            .merge_memory_proposal(&archived_proposal_relative)
            .await
            .expect_err("archived proposal merge should fail");
        assert!(err.to_string().contains("not an active child"));

        let loop_path = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Archive Loop".to_string(),
                summary: "summary".to_string(),
                next_step: "next".to_string(),
                source: None,
            })
            .await
            .expect("record loop")
            .expect("loop path");
        let loop_relative = loop_path
            .strip_prefix(layout.workspace_root())
            .expect("loop relative")
            .to_string_lossy()
            .to_string();
        let archived_loop = layout
            .resolve_open_loop(&loop_relative)
            .await
            .expect("resolve loop");
        let archived_loop_relative = archived_loop
            .strip_prefix(layout.workspace_root())
            .expect("archived loop relative")
            .to_string_lossy()
            .to_string();

        let err = layout
            .resolve_open_loop(&archived_loop_relative)
            .await
            .expect_err("archived loop resolve should fail");
        assert!(err.to_string().contains("not an active child"));
    }

    #[tokio::test]
    async fn relative_paths_normalize_curdir_components_for_active_actions() {
        let temp_dir = tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let proposal_path = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Normalized Proposal".to_string(),
                rationale: "proposal".to_string(),
                entries: vec!["entry".to_string()],
                source: None,
            })
            .await
            .expect("record proposal")
            .expect("proposal path");
        let proposal_relative = format!(
            "./{}",
            proposal_path
                .strip_prefix(layout.workspace_root())
                .expect("proposal relative")
                .to_string_lossy()
        );
        layout
            .merge_memory_proposal(&proposal_relative)
            .await
            .expect("merge normalized proposal path");

        let loop_path = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Normalized Loop".to_string(),
                summary: "summary".to_string(),
                next_step: "next".to_string(),
                source: None,
            })
            .await
            .expect("record loop")
            .expect("loop path");
        let loop_relative = format!(
            "./{}",
            loop_path
                .strip_prefix(layout.workspace_root())
                .expect("loop relative")
                .to_string_lossy()
        );
        layout
            .resolve_open_loop(&loop_relative)
            .await
            .expect("resolve normalized loop path");

        let memory = layout
            .read_relative("./MEMORY.md")
            .await
            .expect("read normalized memory path");
        assert!(memory.contains("entry"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_prompt_sections_surfaces_non_missing_read_failures() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let outside = temp_dir.path().join("outside-active.md");
        std::fs::write(&outside, "outside\n").expect("write outside file");
        std::fs::remove_file(layout.active_path()).expect("remove active");
        symlink(&outside, layout.active_path()).expect("symlink active");

        let err = layout
            .read_prompt_sections()
            .await
            .expect_err("symlinked active should fail");
        let message = err.to_string();
        assert!(message.contains("failed to open") || message.contains("not a regular file"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_paths_reject_symlinked_roots() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let layout = ContinuityLayout::new(temp_dir.path().join("workspace"));
        layout.ensure_base_layout().await.expect("bootstrap");

        let outside_daily = temp_dir.path().join("outside-daily");
        let outside_artifacts = temp_dir.path().join("outside-artifacts");
        let outside_loops = temp_dir.path().join("outside-loops");
        let outside_proposals = temp_dir.path().join("outside-proposals");
        let outside_memory = temp_dir.path().join("outside-memory.md");
        let outside_active = temp_dir.path().join("outside-active.md");
        std::fs::create_dir_all(&outside_daily).expect("create outside daily");
        std::fs::create_dir_all(&outside_artifacts).expect("create outside artifacts");
        std::fs::create_dir_all(&outside_loops).expect("create outside loops");
        std::fs::create_dir_all(&outside_proposals).expect("create outside proposals");
        std::fs::write(&outside_memory, "outside memory\n").expect("write outside memory");
        std::fs::write(&outside_active, "outside active\n").expect("write outside active");

        let daily_dir = layout.daily_dir();
        std::fs::remove_dir_all(&daily_dir).expect("remove daily dir");
        symlink(&outside_daily, &daily_dir).expect("symlink daily dir");
        let err = layout
            .append_daily_event(ContinuityEvent {
                at: Utc::now(),
                title: "escape".to_string(),
                details: Vec::new(),
            })
            .await
            .expect_err("daily append should fail");
        assert!(err.to_string().contains("failed to open"));

        let artifacts_dir = layout.artifacts_dir();
        std::fs::remove_dir_all(&artifacts_dir).expect("remove artifacts dir");
        symlink(&outside_artifacts, &artifacts_dir).expect("symlink artifacts dir");
        let err = layout
            .record_artifact(ContinuityArtifact {
                at: Utc::now(),
                slug: "escape".to_string(),
                title: "Escape".to_string(),
                kind: "test".to_string(),
                summary: None,
                source: None,
                body: "body".to_string(),
            })
            .await
            .expect_err("artifact write should fail");
        assert!(err.to_string().contains("failed to open"));

        let loops_dir = layout.open_loops_dir();
        std::fs::remove_dir_all(&loops_dir).expect("remove loops dir");
        symlink(&outside_loops, &loops_dir).expect("symlink loops dir");
        let err = layout
            .upsert_open_loop(&ContinuityOpenLoopDraft {
                title: "Escape Loop".to_string(),
                summary: "summary".to_string(),
                next_step: "next".to_string(),
                source: None,
            })
            .await
            .expect_err("open loop write should fail");
        assert!(err.to_string().contains("failed to open"));

        let proposals_dir = layout.memory_proposals_dir();
        std::fs::remove_dir_all(&proposals_dir).expect("remove proposals dir");
        symlink(&outside_proposals, &proposals_dir).expect("symlink proposals dir");
        let err = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Escape Proposal".to_string(),
                rationale: "rationale".to_string(),
                entries: vec!["entry".to_string()],
                source: None,
            })
            .await
            .expect_err("proposal write should fail");
        assert!(err.to_string().contains("failed to open"));

        std::fs::remove_file(&proposals_dir).expect("remove proposal symlink");
        std::fs::create_dir_all(&proposals_dir).expect("restore proposal dir");
        let proposal_path = layout
            .record_memory_proposal(&ContinuityMemoryProposalDraft {
                title: "Memory Escape".to_string(),
                rationale: "rationale".to_string(),
                entries: vec!["entry".to_string()],
                source: None,
            })
            .await
            .expect("record proposal")
            .expect("proposal path");

        std::fs::remove_file(layout.memory_path()).expect("remove memory");
        symlink(&outside_memory, layout.memory_path()).expect("symlink memory");
        let relative = proposal_path
            .strip_prefix(layout.workspace_root())
            .expect("proposal relative")
            .to_string_lossy()
            .to_string();
        let archived = layout
            .merge_memory_proposal(&relative)
            .await
            .expect("memory merge should replace symlink leaf");
        assert!(archived.exists());
        assert_eq!(
            std::fs::read_to_string(&outside_memory).expect("read outside memory"),
            "outside memory\n"
        );
        assert!(!std::fs::symlink_metadata(layout.memory_path())
            .expect("memory metadata")
            .file_type()
            .is_symlink());

        std::fs::remove_file(layout.active_path()).expect("remove active");
        symlink(&outside_active, layout.active_path()).expect("symlink active");
        layout
            .write_active(&ActiveContinuitySnapshot::default())
            .await
            .expect("active write should replace symlink leaf");
        assert_eq!(
            std::fs::read_to_string(&outside_active).expect("read outside active"),
            "outside active\n"
        );
        assert!(!std::fs::symlink_metadata(layout.active_path())
            .expect("active metadata")
            .file_type()
            .is_symlink());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bootstrap_rejects_symlinked_workspace_root() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let outside = temp_dir.path().join("outside-workspace");
        std::fs::create_dir_all(&outside).expect("create outside workspace");
        let workspace = temp_dir.path().join("workspace");
        symlink(&outside, &workspace).expect("symlink workspace");

        let layout = ContinuityLayout::new(&workspace);
        let err = layout
            .ensure_base_layout()
            .await
            .expect_err("symlinked workspace root should fail");
        assert!(err.to_string().contains("failed to open"));
    }
}
