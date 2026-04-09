use std::{collections::BTreeMap, path::Path};

use anyhow::{anyhow, Context, Result};

use crate::home::LionClawHome;

pub async fn load_runtime_secrets(home: &LionClawHome) -> Result<BTreeMap<String, String>> {
    let path = home.runtime_secrets_env_path();
    if !tokio::fs::try_exists(&path)
        .await
        .with_context(|| format!("failed to stat {}", path.display()))?
    {
        return Ok(BTreeMap::new());
    }

    let content = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_runtime_secrets(&content, &path)
}

fn parse_runtime_secrets(content: &str, path: &Path) -> Result<BTreeMap<String, String>> {
    let mut secrets = BTreeMap::new();

    for (line_number, line) in content.lines().enumerate() {
        let Some((key, value)) = parse_runtime_secret_line(line).with_context(|| {
            format!(
                "failed to parse {} line {}",
                path.display(),
                line_number + 1
            )
        })?
        else {
            continue;
        };
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

fn parse_runtime_secret_line(line: &str) -> Result<Option<(String, String)>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }

    let declaration = trimmed
        .strip_prefix("export ")
        .map(str::trim_start)
        .unwrap_or(trimmed);
    let (key, raw_value) = declaration
        .split_once('=')
        .ok_or_else(|| anyhow!("expected KEY=VALUE"))?;
    let key = key.trim();
    if !is_valid_env_name(key) {
        return Err(anyhow!(
            "runtime secret key '{}' must be a valid environment variable name",
            key
        ));
    }

    Ok(Some((
        key.to_string(),
        parse_runtime_secret_value(raw_value)?,
    )))
}

fn is_valid_env_name(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn parse_runtime_secret_value(raw: &str) -> Result<String> {
    let raw = raw.trim_start();
    if raw.is_empty() {
        return Ok(String::new());
    }

    if let Some(single_quoted) = raw.strip_prefix('\'') {
        let end = single_quoted
            .find('\'')
            .ok_or_else(|| anyhow!("unterminated single-quoted value"))?;
        let value = &single_quoted[..end];
        let trailing = single_quoted[end + 1..].trim_start();
        if !trailing.is_empty() && !trailing.starts_with('#') {
            return Err(anyhow!("unexpected trailing content after quoted value"));
        }
        return Ok(value.to_string());
    }

    if let Some(double_quoted) = raw.strip_prefix('"') {
        return parse_double_quoted_value(double_quoted);
    }

    Ok(strip_unquoted_comment(raw).trim_end().to_string())
}

fn parse_double_quoted_value(raw: &str) -> Result<String> {
    let mut value = String::new();
    let mut escaped = false;

    for (index, ch) in raw.char_indices() {
        if escaped {
            match ch {
                '\\' | '"' | '\'' | '$' => value.push(ch),
                'n' => value.push('\n'),
                'r' => value.push('\r'),
                't' => value.push('\t'),
                _ => return Err(anyhow!("unsupported escape sequence in quoted value")),
            }
            escaped = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '"' => {
                let trailing = raw[index + 1..].trim_start();
                if !trailing.is_empty() && !trailing.starts_with('#') {
                    return Err(anyhow!("unexpected trailing content after quoted value"));
                }
                return Ok(value);
            }
            _ => value.push(ch),
        }
    }

    if escaped {
        return Err(anyhow!("unterminated escape sequence in quoted value"));
    }

    Err(anyhow!("unterminated double-quoted value"))
}

fn strip_unquoted_comment(raw: &str) -> &str {
    let mut previous_was_whitespace = false;

    for (index, ch) in raw.char_indices() {
        if ch == '#' && previous_was_whitespace {
            return raw[..index].trim_end();
        }
        previous_was_whitespace = ch.is_whitespace();
    }

    raw
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{load_runtime_secrets, parse_runtime_secrets};
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
            "# comment\nGITHUB_TOKEN=ghp_test\nOPENAI_API_KEY=\"sk-test\"\n",
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

    #[test]
    fn parser_keeps_dollar_values_literal() {
        let secrets = parse_runtime_secrets(
            "GITHUB_TOKEN=$FROM_ENV\nOPENAI_API_KEY='sk-test#$literal'\n",
            Path::new("/tmp/runtime-secrets.env"),
        )
        .expect("parse secrets");

        assert_eq!(secrets["GITHUB_TOKEN"], "$FROM_ENV");
        assert_eq!(secrets["OPENAI_API_KEY"], "sk-test#$literal");
    }
}
