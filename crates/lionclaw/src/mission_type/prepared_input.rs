use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use crate::model::InputName;

use super::{has_shebang, is_executable, PreparedInput, MAX_PREPARED_INPUT_CONTENT_BYTES};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PreparedInputContractError {
    #[error("prepared input '{input}' is invalid: {detail}")]
    Invalid { input: String, detail: String },
    #[error("io error at '{path}': {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

pub(crate) fn validate_prepared_inputs(
    inputs: &BTreeMap<InputName, PreparedInput>,
) -> Result<(), PreparedInputContractError> {
    let mut environment_owners = BTreeMap::<String, InputName>::new();
    for (name, input) in inputs {
        let invalid = |detail: String| PreparedInputContractError::Invalid {
            input: name.to_string(),
            detail,
        };
        if name != &input.name {
            return Err(invalid(format!(
                "map key '{name}' does not match input definition '{}'",
                input.name
            )));
        }
        if input.key_files.is_empty() {
            return Err(invalid(
                "key-files must contain at least one workspace path".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        for path in &input.key_files {
            if !safe_relative_path(path) {
                return Err(invalid(format!(
                    "key-file '{}' must be a non-empty relative path without traversal",
                    path.display()
                )));
            }
            if !seen.insert(path) {
                return Err(invalid(format!(
                    "key-file '{}' is declared more than once",
                    path.display()
                )));
            }
        }
        for variable in input.environment.keys() {
            if !valid_environment_name(variable) {
                return Err(invalid(format!("environment key '{variable}' is invalid")));
            }
            if let Some(owner) = environment_owners.insert(variable.clone(), name.clone()) {
                return Err(invalid(format!(
                    "environment key '{variable}' is already provided by input '{owner}'"
                )));
            }
        }

        let metadata = std::fs::symlink_metadata(&input.program).map_err(|source| {
            PreparedInputContractError::Io {
                path: input.program.clone(),
                source,
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(invalid(
                "program must be a regular file (no symlinks)".to_string(),
            ));
        }
        if metadata.len() > MAX_PREPARED_INPUT_CONTENT_BYTES {
            return Err(invalid(format!(
                "program exceeds the {MAX_PREPARED_INPUT_CONTENT_BYTES} byte limit"
            )));
        }
        if !is_executable(&input.program) || !has_shebang(&input.program) {
            return Err(invalid(
                "program must be executable and start with a #! shebang".to_string(),
            ));
        }
    }
    Ok(())
}

fn safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn valid_environment_name(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
