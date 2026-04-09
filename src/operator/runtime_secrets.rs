use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use dotenvy::from_path_iter;

use crate::home::LionClawHome;

pub async fn load_runtime_secrets(home: &LionClawHome) -> Result<BTreeMap<String, String>> {
    let path = home.runtime_secrets_env_path();
    if !tokio::fs::try_exists(&path)
        .await
        .with_context(|| format!("failed to stat {}", path.display()))?
    {
        return Ok(BTreeMap::new());
    }

    let iter =
        from_path_iter(&path).with_context(|| format!("failed to parse {}", path.display()))?;
    let mut secrets = BTreeMap::new();

    for entry in iter {
        let (key, value) = entry.with_context(|| format!("failed to parse {}", path.display()))?;
        if value.is_empty() {
            return Err(anyhow!(
                "runtime secret '{}' in '{}' cannot be empty",
                key,
                path.display()
            ));
        }
        if secrets.insert(key.clone(), value).is_some() {
            return Err(anyhow!(
                "runtime secret '{}' is defined more than once in '{}'",
                key,
                path.display()
            ));
        }
    }

    Ok(secrets)
}

#[cfg(test)]
mod tests {
    use super::load_runtime_secrets;
    use crate::home::LionClawHome;

    #[tokio::test]
    async fn missing_runtime_secrets_file_loads_empty_map() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");

        let secrets = load_runtime_secrets(&home).await.expect("load secrets");
        assert!(secrets.is_empty());
    }

    #[tokio::test]
    async fn runtime_secrets_file_loads_dotenv_values() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        tokio::fs::write(
            home.runtime_secrets_env_path(),
            "GITHUB_TOKEN=ghp_test\nOPENAI_API_KEY=sk-test\n",
        )
        .await
        .expect("write env");

        let secrets = load_runtime_secrets(&home).await.expect("load secrets");
        assert_eq!(secrets["GITHUB_TOKEN"], "ghp_test");
        assert_eq!(secrets["OPENAI_API_KEY"], "sk-test");
    }

    #[tokio::test]
    async fn runtime_secrets_file_rejects_duplicate_keys() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        tokio::fs::write(
            home.runtime_secrets_env_path(),
            "GITHUB_TOKEN=one\nGITHUB_TOKEN=two\n",
        )
        .await
        .expect("write env");

        let err = load_runtime_secrets(&home)
            .await
            .expect_err("duplicate key should fail");
        assert!(err.to_string().contains("defined more than once"));
    }
}
