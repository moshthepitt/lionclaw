use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

use super::manifest::is_path_safe_name;
use super::Home;

/// One explicit mission-type reference: either a catalog name or a filesystem
/// path. Bare names never consult the current directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissionTypeLocator {
    Named(String),
    Path(PathBuf),
}

impl MissionTypeLocator {
    pub fn parse(raw: &str) -> Result<Self> {
        if path_shaped(raw) {
            return Ok(Self::Path(PathBuf::from(raw)));
        }
        if !is_path_safe_name(raw) {
            bail!("mission type name '{raw}' is not path-safe");
        }
        Ok(Self::Named(raw.to_string()))
    }

    pub fn resolve(&self) -> Result<PathBuf> {
        match self {
            Self::Named(name) => Ok(Home::from_env()?.mission_type_dir(name)),
            Self::Path(path) => Ok(path.clone()),
        }
    }
}

fn path_shaped(raw: &str) -> bool {
    let path = Path::new(raw);
    path.is_absolute()
        || raw == "."
        || raw == ".."
        || raw.starts_with("./")
        || raw.starts_with("../")
        || raw.contains('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_names_and_paths_are_unambiguous() {
        assert_eq!(
            MissionTypeLocator::parse("software-dev").unwrap(),
            MissionTypeLocator::Named("software-dev".to_string())
        );
        for path in [
            ".",
            "..",
            "./local",
            "../local",
            "types/local",
            "/opt/local",
        ] {
            assert!(matches!(
                MissionTypeLocator::parse(path).unwrap(),
                MissionTypeLocator::Path(_)
            ));
        }
    }
}
