//! Mission-local command oracle declarations and their canonical identity.

use serde::{Deserialize, Serialize};

use super::digest::CanonicalDigest;
use super::{
    validate_environment_entry, AuthorityCeilings, AuthorityGrants, ConfinementResources,
    ExecutionPolicy,
};
use crate::prelude::*;

pub const MAX_ORACLE_ARG_COUNT: usize = 128;
pub const MAX_ORACLE_ARG_BYTES: usize = 16 * 1024;
pub const MAX_ORACLE_ARGV_BYTES: usize = 64 * 1024;
pub const MAX_ORACLE_CWD_BYTES: usize = 4 * 1024;
pub const MAX_ORACLE_ENVIRONMENT_ENTRIES: usize = 128;
pub const MAX_ORACLE_ENVIRONMENT_BYTES: usize = 64 * 1024;

const SHELL_EXECUTABLES: &[&str] = &["ash", "bash", "dash", "fish", "ksh", "sh", "zsh"];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WorkspaceRelativeDir(String);

impl WorkspaceRelativeDir {
    pub fn new(raw: impl Into<String>) -> Result<Self, OracleSpecError> {
        let raw = raw.into();
        if raw == "." {
            return Ok(Self(raw));
        }
        if raw.is_empty()
            || raw.len() > MAX_ORACLE_CWD_BYTES
            || raw.starts_with('/')
            || raw.ends_with('/')
            || raw.contains('\\')
            || raw.contains('\0')
            || raw
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
        {
            return Err(OracleSpecError::InvalidWorkingDirectory(raw));
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for WorkspaceRelativeDir {
    type Error = OracleSpecError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::new(raw)
    }
}

impl From<WorkspaceRelativeDir> for String {
    fn from(path: WorkspaceRelativeDir) -> Self {
        path.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandOracle {
    pub argv: Vec<String>,
    pub cwd: WorkspaceRelativeDir,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    pub timeout_secs: u64,
    #[serde(default)]
    pub grants: AuthorityGrants,
    #[serde(default, skip_serializing_if = "ConfinementResources::is_empty")]
    pub resources: ConfinementResources,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OracleSpec {
    Command(CommandOracle),
}

impl OracleSpec {
    pub fn digest(&self) -> String {
        let mut digest = CanonicalDigest::new("lionclaw.oracle-spec.v1");
        match self {
            Self::Command(command) => {
                digest.str("type", "command");
                digest.sequence("argv", command.argv.iter());
                digest.str("cwd", command.cwd.as_str());
                digest.map("environment", command.environment.iter());
                digest.u64("timeout_secs", command.timeout_secs);
                digest.bool("grants.secrets", command.grants.secrets);
                command
                    .grants
                    .network
                    .feed_digest(&mut digest, "grants.network");
                digest.bool("grants.install", command.grants.install);
                digest.bool("grants.writes", command.grants.writes);
                digest.set("grants.devices", command.grants.devices.iter());
                digest.set(
                    "grants.inputs",
                    command.grants.inputs.iter().map(ToString::to_string),
                );
                digest.set("resources.tmpfs", command.resources.tmpfs.iter());
            }
        }
        digest.finish()
    }

    pub fn validate(
        &self,
        ceilings: &AuthorityCeilings,
        resource_ceilings: &ConfinementResources,
        execution: &ExecutionPolicy,
    ) -> Result<(), OracleSpecError> {
        match self {
            Self::Command(command) => command.validate(ceilings, resource_ceilings, execution),
        }
    }

    pub fn as_command(&self) -> &CommandOracle {
        match self {
            Self::Command(command) => command,
        }
    }
}

impl CommandOracle {
    fn validate(
        &self,
        ceilings: &AuthorityCeilings,
        resource_ceilings: &ConfinementResources,
        execution: &ExecutionPolicy,
    ) -> Result<(), OracleSpecError> {
        if self.argv.is_empty() {
            return Err(OracleSpecError::EmptyArgv);
        }
        if self.argv.len() > MAX_ORACLE_ARG_COUNT {
            return Err(OracleSpecError::TooManyArguments(self.argv.len()));
        }
        let executable = self.argv[0].rsplit('/').next().unwrap_or_default();
        if SHELL_EXECUTABLES.contains(&executable) {
            return Err(OracleSpecError::ShellExecutable(executable.to_string()));
        }
        let mut argv_bytes = 0usize;
        for (index, argument) in self.argv.iter().enumerate() {
            if argument.is_empty() && index == 0 {
                return Err(OracleSpecError::EmptyExecutable);
            }
            if argument.contains('\0') {
                return Err(OracleSpecError::ArgumentContainsNul(index));
            }
            if argument.len() > MAX_ORACLE_ARG_BYTES {
                return Err(OracleSpecError::ArgumentTooLarge(index));
            }
            argv_bytes = argv_bytes.saturating_add(argument.len());
        }
        if argv_bytes > MAX_ORACLE_ARGV_BYTES {
            return Err(OracleSpecError::ArgvTooLarge(argv_bytes));
        }
        if self.environment.len() > MAX_ORACLE_ENVIRONMENT_ENTRIES {
            return Err(OracleSpecError::TooManyEnvironmentEntries(
                self.environment.len(),
            ));
        }
        let mut environment_bytes = 0usize;
        for (name, value) in &self.environment {
            validate_environment_entry(name, value).map_err(OracleSpecError::InvalidEnvironment)?;
            environment_bytes = environment_bytes
                .saturating_add(name.len())
                .saturating_add(value.len());
        }
        if environment_bytes > MAX_ORACLE_ENVIRONMENT_BYTES {
            return Err(OracleSpecError::EnvironmentTooLarge(environment_bytes));
        }
        if self.timeout_secs == 0 || self.timeout_secs > execution.max_task_time_secs {
            return Err(OracleSpecError::InvalidTimeout {
                requested: self.timeout_secs,
                maximum: execution.max_task_time_secs,
            });
        }
        if !self.grants.within(ceilings) {
            return Err(OracleSpecError::AuthorityExceedsCeilings);
        }
        if self.grants.secrets || self.grants.install || self.grants.writes {
            return Err(OracleSpecError::AuthorityViolatesProofFloor);
        }
        self.resources
            .within(resource_ceilings)
            .map_err(OracleSpecError::ResourcesExceedCeilings)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OracleSpecError {
    #[error("oracle working directory '{0}' must be a clean workspace-relative directory")]
    InvalidWorkingDirectory(String),
    #[error("oracle argv must contain an executable")]
    EmptyArgv,
    #[error("oracle executable must not be empty")]
    EmptyExecutable,
    #[error("command oracle executable '{0}' is a shell; shell execution is not permitted")]
    ShellExecutable(String),
    #[error("oracle argv contains {0} arguments; the limit is {MAX_ORACLE_ARG_COUNT}")]
    TooManyArguments(usize),
    #[error("oracle argument {0} contains NUL")]
    ArgumentContainsNul(usize),
    #[error("oracle argument {0} exceeds {MAX_ORACLE_ARG_BYTES} bytes")]
    ArgumentTooLarge(usize),
    #[error("oracle argv is {0} bytes; the limit is {MAX_ORACLE_ARGV_BYTES}")]
    ArgvTooLarge(usize),
    #[error(
        "oracle environment contains {0} entries; the limit is {MAX_ORACLE_ENVIRONMENT_ENTRIES}"
    )]
    TooManyEnvironmentEntries(usize),
    #[error("oracle environment is invalid: {0}")]
    InvalidEnvironment(String),
    #[error("oracle environment is {0} bytes; the limit is {MAX_ORACLE_ENVIRONMENT_BYTES}")]
    EnvironmentTooLarge(usize),
    #[error("oracle timeout {requested}s must be between 1s and the {maximum}s mission ceiling")]
    InvalidTimeout { requested: u64, maximum: u64 },
    #[error("oracle authority exceeds mission ceilings")]
    AuthorityExceedsCeilings,
    #[error("command oracles may not request secrets, install, or writes")]
    AuthorityViolatesProofFloor,
    #[error("oracle resources exceed mission ceilings: {0}")]
    ResourcesExceedCeilings(String),
}
