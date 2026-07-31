//! Mission-local command oracle declarations and their canonical identity.

use serde::{Deserialize, Serialize};

use super::digest::CanonicalDigest;
use super::{
    validate_environment_entry, AuthorityCeilings, AuthorityGrants, ConfinementResources,
    ExecutionPolicy, NetworkGrant,
};
use crate::prelude::*;

pub const MAX_ORACLE_ARG_COUNT: usize = 128;
pub const MAX_ORACLE_ARG_BYTES: usize = 16 * 1024;
pub const MAX_ORACLE_ARGV_BYTES: usize = 64 * 1024;
pub const MAX_ORACLE_CWD_BYTES: usize = 4 * 1024;
pub const MAX_ORACLE_ENVIRONMENT_ENTRIES: usize = 128;
pub const MAX_ORACLE_ENVIRONMENT_BYTES: usize = 64 * 1024;
pub const MAX_EXTERNAL_ORACLE_DRIVER_ID_BYTES: usize = 128;
pub const MAX_EXTERNAL_ORACLE_REQUEST_FIELDS: usize = 64;
pub const MAX_EXTERNAL_ORACLE_REQUEST_KEY_BYTES: usize = 128;
pub const MAX_EXTERNAL_ORACLE_REQUEST_VALUE_BYTES: usize = 8 * 1024;
pub const MAX_EXTERNAL_ORACLE_REQUEST_BYTES: usize = 64 * 1024;

const SHELL_EXECUTABLES: &[&str] = &["ash", "bash", "dash", "fish", "ksh", "sh", "zsh"];
const DEFAULT_EXTERNAL_POLL_SECS: u64 = 30;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ExternalOracleDriverId(String);

impl ExternalOracleDriverId {
    pub fn new(raw: impl Into<String>) -> Result<Self, OracleSpecError> {
        let raw = raw.into();
        let mut chars = raw.chars();
        let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
        if !first_ok
            || raw.len() > MAX_EXTERNAL_ORACLE_DRIVER_ID_BYTES
            || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(OracleSpecError::InvalidExternalDriverId(raw));
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ExternalOracleDriverId {
    type Error = OracleSpecError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::new(raw)
    }
}

impl From<ExternalOracleDriverId> for String {
    fn from(driver: ExternalOracleDriverId) -> Self {
        driver.0
    }
}

impl core::fmt::Display for ExternalOracleDriverId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalOracleDriverAuthIdentity {
    pub kind: String,
    pub config_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalOracleDriverIdentity {
    pub driver: ExternalOracleDriverId,
    pub image_id: String,
    #[serde(default, skip_serializing_if = "NetworkGrant::is_denied")]
    pub network: NetworkGrant,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ExternalOracleDriverAuthIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalOracle {
    pub driver: ExternalOracleDriverId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver_identity: Option<ExternalOracleDriverIdentity>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request: BTreeMap<String, String>,
    pub timeout_secs: u64,
    #[serde(default = "default_external_poll_secs")]
    pub poll_secs: u64,
}

const fn default_external_poll_secs() -> u64 {
    DEFAULT_EXTERNAL_POLL_SECS
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OracleSpec {
    Command(CommandOracle),
    External(ExternalOracle),
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
            Self::External(external) => {
                digest.str("type", "external");
                digest.str("driver", external.driver.as_str());
                feed_external_driver_identity(
                    &mut digest,
                    "driver_identity",
                    external.driver_identity.as_ref(),
                );
                digest.map("request", external.request.iter());
                digest.u64("timeout_secs", external.timeout_secs);
                digest.u64("poll_secs", external.poll_secs);
            }
        }
        digest.finish()
    }

    pub fn request_digest(&self) -> Option<String> {
        let Self::External(external) = self else {
            return None;
        };
        let mut digest = CanonicalDigest::new("lionclaw.external-oracle-request.v1");
        digest.str("driver", external.driver.as_str());
        feed_external_driver_identity(
            &mut digest,
            "driver_identity",
            external.driver_identity.as_ref(),
        );
        digest.map("request", external.request.iter());
        Some(digest.finish())
    }

    pub fn validate(
        &self,
        ceilings: &AuthorityCeilings,
        resource_ceilings: &ConfinementResources,
        execution: &ExecutionPolicy,
    ) -> Result<(), OracleSpecError> {
        match self {
            Self::Command(command) => command.validate(ceilings, resource_ceilings, execution),
            Self::External(external) => external.validate(ceilings, execution),
        }
    }

    pub fn timeout_secs(&self) -> u64 {
        match self {
            Self::Command(command) => command.timeout_secs,
            Self::External(external) => external.timeout_secs,
        }
    }

    pub fn as_command(&self) -> &CommandOracle {
        match self {
            Self::Command(command) => command,
            Self::External(_) => panic!("external oracle is not a command oracle"),
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

fn feed_external_driver_identity(
    digest: &mut CanonicalDigest,
    prefix: &str,
    identity: Option<&ExternalOracleDriverIdentity>,
) {
    let Some(identity) = identity else {
        digest.str(&format!("{prefix}.state"), "unresolved");
        return;
    };
    digest.str(&format!("{prefix}.state"), "resolved");
    digest.str(&format!("{prefix}.driver"), identity.driver.as_str());
    digest.str(&format!("{prefix}.image_id"), &identity.image_id);
    identity
        .network
        .feed_digest(digest, &format!("{prefix}.network"));
    match &identity.auth {
        Some(auth) => {
            digest.str(&format!("{prefix}.auth.state"), "configured");
            digest.str(&format!("{prefix}.auth.kind"), &auth.kind);
            digest.str(&format!("{prefix}.auth.config_digest"), &auth.config_digest);
        }
        None => digest.str(&format!("{prefix}.auth.state"), "none"),
    }
}

impl ExternalOracle {
    fn validate(
        &self,
        ceilings: &AuthorityCeilings,
        execution: &ExecutionPolicy,
    ) -> Result<(), OracleSpecError> {
        if self.timeout_secs == 0 || self.timeout_secs > execution.max_task_time_secs {
            return Err(OracleSpecError::InvalidTimeout {
                requested: self.timeout_secs,
                maximum: execution.max_task_time_secs,
            });
        }
        if self.poll_secs == 0 || self.poll_secs > self.timeout_secs {
            return Err(OracleSpecError::InvalidExternalPoll {
                requested: self.poll_secs,
                timeout: self.timeout_secs,
            });
        }
        self.validate_driver_identity(ceilings)?;
        validate_external_request(&self.request)
    }

    fn validate_driver_identity(
        &self,
        ceilings: &AuthorityCeilings,
    ) -> Result<(), OracleSpecError> {
        let identity = self
            .driver_identity
            .as_ref()
            .ok_or(OracleSpecError::UnresolvedExternalDriverAuthority)?;
        if identity.driver != self.driver {
            return Err(OracleSpecError::ExternalDriverAuthorityMismatch);
        }
        if !valid_external_driver_content_id(&identity.image_id) {
            return Err(OracleSpecError::InvalidExternalDriverContentIdentity);
        }
        if !identity.network.within(&ceilings.network) {
            return Err(OracleSpecError::AuthorityExceedsCeilings);
        }
        if let Some(auth) = &identity.auth {
            if !valid_external_request_key(&auth.kind)
                || !valid_external_driver_auth_digest(&auth.config_digest)
            {
                return Err(OracleSpecError::InvalidExternalDriverAuthIdentity);
            }
        }
        Ok(())
    }
}

fn valid_external_driver_content_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.contains('\0') && value.trim() == value
}

fn valid_external_driver_auth_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_external_request(request: &BTreeMap<String, String>) -> Result<(), OracleSpecError> {
    if request.len() > MAX_EXTERNAL_ORACLE_REQUEST_FIELDS {
        return Err(OracleSpecError::TooManyExternalRequestFields(request.len()));
    }
    let mut total = 0usize;
    for (key, value) in request {
        if !valid_external_request_key(key) {
            return Err(OracleSpecError::InvalidExternalRequestKey(key.clone()));
        }
        if credential_like_key(key) {
            return Err(OracleSpecError::ExternalRequestContainsCredential(
                key.clone(),
            ));
        }
        if value.contains('\0') {
            return Err(OracleSpecError::ExternalRequestValueContainsNul(
                key.clone(),
            ));
        }
        if value.len() > MAX_EXTERNAL_ORACLE_REQUEST_VALUE_BYTES {
            return Err(OracleSpecError::ExternalRequestFieldTooLarge(key.clone()));
        }
        total = total.saturating_add(key.len()).saturating_add(value.len());
    }
    if total > MAX_EXTERNAL_ORACLE_REQUEST_BYTES {
        return Err(OracleSpecError::ExternalRequestTooLarge(total));
    }
    Ok(())
}

fn valid_external_request_key(key: &str) -> bool {
    let mut chars = key.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
    first_ok
        && key.len() <= MAX_EXTERNAL_ORACLE_REQUEST_KEY_BYTES
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

fn credential_like_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '.'], "_");
    normalized == "secret"
        || normalized == "secrets"
        || normalized == "credential"
        || normalized == "credentials"
        || normalized == "password"
        || normalized == "api_key"
        || normalized == "apikey"
        || normalized == "bearer"
        || normalized == "auth_token"
        || normalized == "access_token"
        || normalized == "refresh_token"
        || normalized.ends_with("_secret")
        || normalized.ends_with("_token")
        || normalized.ends_with("_password")
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
    #[error("external oracle driver id '{0}' must be an installed driver identity, not a path or command")]
    InvalidExternalDriverId(String),
    #[error("external oracle driver authority was not resolved at mission admission")]
    UnresolvedExternalDriverAuthority,
    #[error("external oracle resolved driver authority does not match the requested driver")]
    ExternalDriverAuthorityMismatch,
    #[error("external oracle resolved driver content identity is invalid")]
    InvalidExternalDriverContentIdentity,
    #[error("external oracle resolved driver auth identity is invalid")]
    InvalidExternalDriverAuthIdentity,
    #[error("external oracle request contains {0} fields; the limit is {MAX_EXTERNAL_ORACLE_REQUEST_FIELDS}")]
    TooManyExternalRequestFields(usize),
    #[error("external oracle request key '{0}' is invalid")]
    InvalidExternalRequestKey(String),
    #[error("external oracle request key '{0}' names credential material")]
    ExternalRequestContainsCredential(String),
    #[error("external oracle request value for '{0}' contains NUL")]
    ExternalRequestValueContainsNul(String),
    #[error("external oracle request field '{0}' exceeds {MAX_EXTERNAL_ORACLE_REQUEST_VALUE_BYTES} bytes")]
    ExternalRequestFieldTooLarge(String),
    #[error(
        "external oracle request is {0} bytes; the limit is {MAX_EXTERNAL_ORACLE_REQUEST_BYTES}"
    )]
    ExternalRequestTooLarge(usize),
    #[error(
        "external oracle poll interval {requested}s must be between 1s and timeout {timeout}s"
    )]
    InvalidExternalPoll { requested: u64, timeout: u64 },
}
