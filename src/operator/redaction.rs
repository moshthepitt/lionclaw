use std::{collections::BTreeSet, path::Path};

use anyhow::{bail, Result};

use crate::{
    home::LionClawHome,
    operator::{
        channel_env::load_channel_env, channel_metadata::validate_channel_id,
        private_paths::read_private_dir_file_paths,
    },
};

pub(crate) struct SecretRedactor {
    values: Vec<String>,
}

impl SecretRedactor {
    pub(crate) fn from_home(home: &LionClawHome) -> Result<Self> {
        let mut values = BTreeSet::new();
        for path in
            read_private_dir_file_paths(home, &home.channel_env_dir(), "channel env directory")?
        {
            let Some(channel_id) = channel_id_from_env_path(&path)? else {
                continue;
            };
            let env = load_channel_env(home, &channel_id)?;
            for value in env.values() {
                insert_redaction_value(&mut values, value);
            }
        }

        Ok(Self::from_values(values))
    }

    pub(crate) fn redact(&self, text: &str) -> String {
        let mut out = text.to_string();
        for value in &self.values {
            out = out.replace(value, "[REDACTED]");
        }
        out
    }

    fn from_values<I>(values: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let mut values: Vec<_> = values
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect();
        values.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        values.dedup();
        Self { values }
    }
}

fn channel_id_from_env_path(path: &Path) -> Result<Option<String>> {
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        bail!(
            "channel env path {} has a non-UTF-8 file name",
            path.display()
        );
    };
    let Some(channel_id) = file_name.strip_suffix(".env") else {
        return Ok(None);
    };
    validate_channel_id(channel_id)?;
    Ok(Some(channel_id.to_string()))
}

fn insert_redaction_value(values: &mut BTreeSet<String>, value: &str) {
    if value.is_empty() {
        return;
    }
    values.insert(value.to_string());
    for line in value.lines() {
        if !line.is_empty() {
            values.insert(line.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{insert_redaction_value, SecretRedactor};
    use crate::{
        home::LionClawHome,
        operator::channel_env::{save_channel_env, ChannelEnv},
    };

    #[test]
    fn redacts_values_from_private_channel_env_files() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut env = ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        save_channel_env(&home, "telegram", &env).expect("save channel env");

        let redactor = SecretRedactor::from_home(&home).expect("redactor");

        assert_eq!(
            redactor.redact("boot secret-token done"),
            "boot [REDACTED] done"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_channel_env_entries() {
        use std::{fs, os::unix::fs::symlink};

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.channel_env_dir()).expect("channel env dir");
        let outside = temp_dir.path().join("outside.env");
        fs::write(&outside, "TOKEN=secret\n").expect("outside env");
        symlink(outside, home.channel_env_path("telegram")).expect("symlink env");

        let err = match SecretRedactor::from_home(&home) {
            Ok(_) => panic!("symlink should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("must not be a symlink"));
    }

    #[test]
    fn redacts_longest_values_first() {
        let redactor = SecretRedactor::from_values([
            "abc".to_string(),
            "abc123".to_string(),
            "abc".to_string(),
        ]);

        assert_eq!(redactor.redact("abc123 abc"), "[REDACTED] [REDACTED]");
    }

    #[test]
    fn ignores_empty_values() {
        let redactor = SecretRedactor::from_values(["".to_string()]);

        assert_eq!(redactor.redact("abc"), "abc");
    }

    #[test]
    fn includes_multiline_secret_fragments() {
        let mut values = BTreeSet::new();
        insert_redaction_value(&mut values, "alpha\nbeta");
        let redactor = SecretRedactor::from_values(values);

        assert_eq!(
            redactor.redact("alpha then beta then alpha\nbeta"),
            "[REDACTED] then [REDACTED] then [REDACTED]"
        );
    }
}
