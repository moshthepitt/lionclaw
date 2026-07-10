//! The global install home: `~/.lionclaw`. Mission types are installed here
//! (`~/.lionclaw/mission-types/<name>/`) and resolved by name — so a mission
//! command never needs a `--type <path>` once the type is installed. Mission
//! *state* (the event store, blobs, attempt dirs) stays repo-local; only the
//! installation is global.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

/// The global LionClaw home. `$LIONCLAW_HOME`, else `$HOME/.lionclaw`. There is
/// no relative fallback: a home we can't locate is a hard error, never a stray
/// `.lionclaw` in the cwd.
pub struct Home {
    root: PathBuf,
}

impl Home {
    pub fn from_env() -> Result<Self> {
        // An exported-but-empty value (`LIONCLAW_HOME=`, or an empty `HOME` in a
        // minimal container/CI) must not become a relative root — that would put
        // a stray `.lionclaw` in the cwd. Treat empty as unset and fall through.
        let non_empty = |v: std::ffi::OsString| (!v.is_empty()).then_some(v);
        let root = std::env::var_os("LIONCLAW_HOME")
            .and_then(non_empty)
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .and_then(non_empty)
                    .map(|h| PathBuf::from(h).join(".lionclaw"))
            })
            .ok_or_else(|| anyhow!("neither LIONCLAW_HOME nor HOME is set"))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where installed mission types live.
    pub fn mission_types_dir(&self) -> PathBuf {
        self.root.join("mission-types")
    }

    /// The install directory of one mission type (may not exist).
    pub fn mission_type_dir(&self, name: &str) -> PathBuf {
        self.mission_types_dir().join(name)
    }

    /// User-configured runtime profiles. Missing means built-in defaults.
    pub fn runtimes_file(&self) -> PathBuf {
        self.root.join("runtimes.toml")
    }

    /// Installed mission-type names, sorted. Empty (not an error) before the
    /// first `install`.
    pub fn installed_mission_types(&self) -> Result<Vec<String>> {
        let dir = self.mission_types_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut names = Vec::new();
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("reading '{}'", dir.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        Ok(names)
    }
}

/// Resolve the directory of mission types bundled with this binary — the source
/// `install` copies from. Next to the executable in a release
/// (`<exe_dir>/mission-types`), or the repo's `mission-types/` in a dev build.
/// Follows the same exe-relative-then-source-tree pattern the kernel used for
/// bundled assets.
pub fn bundled_mission_types_dir() -> Result<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("mission-types");
            if candidate.is_dir() {
                return Ok(candidate);
            }
        }
    }
    let dev = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../mission-types");
    if dev.is_dir() {
        return Ok(dev);
    }
    Err(anyhow!(
        "no bundled mission types found (looked next to the binary and in the source tree); \
         pass `--from <dir>`"
    ))
}
