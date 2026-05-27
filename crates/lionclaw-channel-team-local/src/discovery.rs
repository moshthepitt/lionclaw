use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::protocol::DeliveryRoute;

const PROJECT_DIR: &str = ".lionclaw";
const PROJECT_FILE: &str = "project.toml";
const INSTANCES_DIR: &str = "instances";
const HOME_ID_FILE: &str = "config/home-id";
const OPERATOR_CONFIG_FILE: &str = "config/lionclaw.toml";

#[derive(Debug, Clone)]
pub struct ProjectDiscovery {
    pub project_root: PathBuf,
    pub self_instance: String,
    members_by_home_id: BTreeMap<String, ProjectMember>,
    home_id_by_name: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ProjectMember {
    pub name: String,
    pub home: PathBuf,
    pub home_id: String,
    pub base_url: String,
}

#[derive(Debug, Default, Deserialize)]
struct OperatorConfigFile {
    #[serde(default)]
    daemon: DaemonConfigFile,
}

#[derive(Debug, Deserialize)]
struct DaemonConfigFile {
    #[serde(default = "default_bind")]
    bind: String,
}

impl Default for DaemonConfigFile {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

impl ProjectDiscovery {
    pub fn discover(home: &Path) -> Result<Self> {
        let home = fs::canonicalize(home)
            .with_context(|| format!("failed to resolve {}", home.display()))?;
        let (project_root, self_instance) = project_parts_from_home(&home)?;
        let instances_dir = project_root.join(PROJECT_DIR).join(INSTANCES_DIR);
        let mut members_by_home_id: BTreeMap<String, ProjectMember> = BTreeMap::new();
        let mut home_id_by_name: BTreeMap<String, String> = BTreeMap::new();

        let mut entries = fs::read_dir(&instances_dir)
            .with_context(|| format!("failed to read {}", instances_dir.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("failed to iterate {}", instances_dir.display()))?;
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let name = entry
                .file_name()
                .to_str()
                .ok_or_else(|| anyhow!("invalid instance name under {}", instances_dir.display()))?
                .to_string();
            if name.starts_with('.') {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())
                .with_context(|| format!("failed to stat {}", entry.path().display()))?;
            if metadata.file_type().is_symlink() {
                bail!(
                    "instance home {} must not be a symlink",
                    entry.path().display()
                );
            }
            if !metadata.is_dir() {
                continue;
            }

            let home = fs::canonicalize(entry.path())
                .with_context(|| format!("failed to resolve {}", entry.path().display()))?;
            let home_id = read_home_id(&home)?;
            let base_url = read_base_url(&home)?;
            if let Some(existing) = members_by_home_id.get(&home_id) {
                bail!(
                    "project instances '{}' and '{}' share home id '{}'",
                    existing.name,
                    name,
                    home_id
                );
            }
            home_id_by_name.insert(name.clone(), home_id.clone());
            members_by_home_id.insert(
                home_id.clone(),
                ProjectMember {
                    name,
                    home,
                    home_id,
                    base_url,
                },
            );
        }

        Ok(Self {
            project_root,
            self_instance,
            members_by_home_id,
            home_id_by_name,
        })
    }

    pub fn member_for_route(&self, route: &DeliveryRoute) -> Option<&ProjectMember> {
        match route {
            DeliveryRoute::HomeId(home_id) => self.members_by_home_id.get(home_id),
            DeliveryRoute::InstanceName(name) => self
                .home_id_by_name
                .get(name)
                .and_then(|home_id| self.members_by_home_id.get(home_id)),
        }
    }
}

fn project_parts_from_home(home: &Path) -> Result<(PathBuf, String)> {
    let instance_name = home
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("invalid LionClaw instance home '{}'", home.display()))?
        .to_string();
    let instances_dir = home
        .parent()
        .ok_or_else(|| anyhow!("instance home '{}' has no parent", home.display()))?;
    if instances_dir.file_name().and_then(|value| value.to_str()) != Some(INSTANCES_DIR) {
        bail!("team-local requires a project instance home under .lionclaw/instances");
    }
    let project_dir = instances_dir.parent().ok_or_else(|| {
        anyhow!(
            "instances directory '{}' has no parent",
            instances_dir.display()
        )
    })?;
    if project_dir.file_name().and_then(|value| value.to_str()) != Some(PROJECT_DIR) {
        bail!("team-local requires a project instance home under .lionclaw/instances");
    }
    let project_root = project_dir
        .parent()
        .ok_or_else(|| {
            anyhow!(
                "project metadata directory '{}' has no parent",
                project_dir.display()
            )
        })?
        .to_path_buf();
    let project_file = project_dir.join(PROJECT_FILE);
    let metadata = fs::symlink_metadata(&project_file)
        .with_context(|| format!("failed to stat {}", project_file.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "project config {} must be a regular file",
            project_file.display()
        );
    }
    Ok((project_root, instance_name))
}

fn read_home_id(home: &Path) -> Result<String> {
    let path = home.join(HOME_ID_FILE);
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("failed to stat {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("home id file {} must be a regular file", path.display());
    }
    let home_id = fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?
        .trim()
        .to_string();
    if home_id.is_empty() {
        bail!("home id file {} is empty", path.display());
    }
    Ok(home_id)
}

fn read_base_url(home: &Path) -> Result<String> {
    let path = home.join(OPERATOR_CONFIG_FILE);
    let config = match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                bail!("operator config {} must be a regular file", path.display());
            }
            let content = fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            toml::from_str::<OperatorConfigFile>(&content)
                .with_context(|| format!("failed to parse {}", path.display()))?
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => OperatorConfigFile::default(),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    Ok(bind_to_base_url(&config.daemon.bind))
}

fn bind_to_base_url(bind: &str) -> String {
    let bind = bind.trim();
    if bind.starts_with("http://") || bind.starts_with("https://") {
        bind.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", bind.trim_end_matches('/'))
    }
}

fn default_bind() -> String {
    "127.0.0.1:8979".to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::ProjectDiscovery;
    use crate::protocol::DeliveryRoute;

    #[test]
    fn discovers_project_members() {
        let temp_dir = tempdir().expect("temp dir");
        let instances = temp_dir.path().join(".lionclaw/instances");
        fs::create_dir_all(instances.join("main/config")).expect("main config");
        fs::create_dir_all(instances.join("reviewer/config")).expect("reviewer config");
        fs::write(
            temp_dir.path().join(".lionclaw/project.toml"),
            "version = 1\n",
        )
        .expect("project");
        fs::write(instances.join("main/config/home-id"), "home-main\n").expect("home id");
        fs::write(instances.join("reviewer/config/home-id"), "home-reviewer\n").expect("home id");
        fs::write(
            instances.join("reviewer/config/lionclaw.toml"),
            "[daemon]\nbind = \"127.0.0.1:9988\"\n",
        )
        .expect("config");

        let discovery = ProjectDiscovery::discover(&instances.join("main")).expect("discovery");
        let member = discovery
            .member_for_route(&DeliveryRoute::HomeId("home-reviewer".to_string()))
            .expect("member");

        assert_eq!(discovery.self_instance, "main");
        assert_eq!(member.name, "reviewer");
        assert_eq!(member.base_url, "http://127.0.0.1:9988");
    }

    #[test]
    fn rejects_duplicate_project_home_ids() {
        let temp_dir = tempdir().expect("temp dir");
        let instances = temp_dir.path().join(".lionclaw/instances");
        fs::create_dir_all(instances.join("main/config")).expect("main config");
        fs::create_dir_all(instances.join("reviewer/config")).expect("reviewer config");
        fs::write(
            temp_dir.path().join(".lionclaw/project.toml"),
            "version = 1\n",
        )
        .expect("project");
        fs::write(instances.join("main/config/home-id"), "same-home\n").expect("home id");
        fs::write(instances.join("reviewer/config/home-id"), "same-home\n").expect("home id");

        let err = ProjectDiscovery::discover(&instances.join("main"))
            .expect_err("duplicate home ids should fail");

        assert!(err.to_string().contains("share home id 'same-home'"));
    }
}
