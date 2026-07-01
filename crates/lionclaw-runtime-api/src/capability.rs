use std::str::FromStr;

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Any,
    FsRead,
    FsWrite,
    NetEgress,
    SecretRequest,
    ChannelSend,
    SchedulerRun,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "*",
            Self::FsRead => "fs.read",
            Self::FsWrite => "fs.write",
            Self::NetEgress => "net.egress",
            Self::SecretRequest => "secret.request",
            Self::ChannelSend => "channel.send",
            Self::SchedulerRun => "scheduler.run",
        }
    }
}

impl FromStr for Capability {
    type Err = String;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        match raw.trim() {
            "*" => Ok(Self::Any),
            "fs.read" => Ok(Self::FsRead),
            "fs.write" => Ok(Self::FsWrite),
            "net.egress" => Ok(Self::NetEgress),
            "secret.request" => Ok(Self::SecretRequest),
            "channel.send" => Ok(Self::ChannelSend),
            "scheduler.run" => Ok(Self::SchedulerRun),
            other => Err(format!("unsupported capability '{other}'")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeCapabilityRequest {
    pub request_id: String,
    pub skill_id: String,
    pub capability: Capability,
    pub scope: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct RuntimeCapabilityResult {
    pub request_id: String,
    pub allowed: bool,
    pub reason: Option<String>,
    pub output: Value,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeTurnResult {
    pub capability_requests: Vec<RuntimeCapabilityRequest>,
}
