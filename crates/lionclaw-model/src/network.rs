//! Canonical destination-scoped network authority.

use core::net::IpAddr;

use serde::{Deserialize, Serialize};

use super::digest::CanonicalDigest;
use crate::prelude::*;

pub const MAX_DESTINATION_HOST_BYTES: usize = 253;
pub const MAX_DESTINATION_PORTS: usize = 32;
pub const MAX_NETWORK_DESTINATIONS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "UncheckedDestination", into = "UncheckedDestination")]
pub struct Destination {
    host: String,
    ports: BTreeSet<u16>,
}

impl Destination {
    pub fn new(host: impl Into<String>, ports: BTreeSet<u16>) -> Result<Self, NetworkGrantError> {
        let host = canonical_host(host.into())?;
        validate_ports(&ports)?;
        Ok(Self { host, ports })
    }

    pub fn single(host: impl Into<String>, port: u16) -> Result<Self, NetworkGrantError> {
        Self::new(host, BTreeSet::from([port]))
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn ports(&self) -> &BTreeSet<u16> {
        &self.ports
    }

    pub fn contains(&self, host: &str, port: u16) -> bool {
        canonical_host(host.to_string())
            .is_ok_and(|host| host == self.host && self.ports.contains(&port))
    }

    pub(crate) fn feed_digest(&self, digest: &mut CanonicalDigest, prefix: &str) {
        digest.str(&format!("{prefix}.host"), &self.host);
        digest.set(
            &format!("{prefix}.ports"),
            self.ports.iter().map(u16::to_string),
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedDestination {
    host: String,
    ports: BTreeSet<u16>,
}

impl TryFrom<UncheckedDestination> for Destination {
    type Error = NetworkGrantError;

    fn try_from(value: UncheckedDestination) -> Result<Self, Self::Error> {
        Destination::new(value.host, value.ports)
    }
}

impl From<Destination> for UncheckedDestination {
    fn from(value: Destination) -> Self {
        Self {
            host: value.host,
            ports: value.ports,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum NetworkGrant {
    #[default]
    Deny,
    Allow {
        #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
        destinations: BTreeSet<Destination>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
enum UncheckedNetworkGrant {
    Deny,
    Allow {
        #[serde(default)]
        destinations: BTreeSet<Destination>,
    },
}

impl<'de> Deserialize<'de> for NetworkGrant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        UncheckedNetworkGrant::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

impl TryFrom<UncheckedNetworkGrant> for NetworkGrant {
    type Error = NetworkGrantError;

    fn try_from(value: UncheckedNetworkGrant) -> Result<Self, Self::Error> {
        match value {
            UncheckedNetworkGrant::Deny => Ok(NetworkGrant::Deny),
            UncheckedNetworkGrant::Allow { destinations } => NetworkGrant::allow(destinations),
        }
    }
}

impl NetworkGrant {
    pub fn allow(destinations: BTreeSet<Destination>) -> Result<Self, NetworkGrantError> {
        validate_destinations(&destinations)?;
        Ok(Self::Allow { destinations })
    }

    pub fn allow_single(host: impl Into<String>, port: u16) -> Result<Self, NetworkGrantError> {
        Self::allow(BTreeSet::from([Destination::single(host, port)?]))
    }

    pub fn destinations(&self) -> Option<&BTreeSet<Destination>> {
        match self {
            Self::Deny => None,
            Self::Allow { destinations } => Some(destinations),
        }
    }

    pub fn is_denied(&self) -> bool {
        self.destinations().is_none_or(BTreeSet::is_empty)
    }

    pub fn allows(&self, host: &str, port: u16) -> bool {
        self.destinations()
            .into_iter()
            .flatten()
            .any(|destination| destination.contains(host, port))
    }

    pub fn within(&self, ceiling: &Self) -> bool {
        match (self, ceiling) {
            (Self::Deny, _) => true,
            (
                Self::Allow { destinations },
                Self::Allow {
                    destinations: ceiling,
                },
            ) => destinations.iter().all(|requested| {
                ceiling.iter().any(|allowed| {
                    requested.host == allowed.host && requested.ports.is_subset(&allowed.ports)
                })
            }),
            (Self::Allow { .. }, Self::Deny) => false,
        }
    }

    pub fn union(&self, other: &Self) -> Result<Self, NetworkGrantError> {
        let destinations = self
            .destinations()
            .into_iter()
            .flatten()
            .chain(other.destinations().into_iter().flatten())
            .cloned()
            .collect::<BTreeSet<_>>();
        if destinations.is_empty() {
            return Ok(Self::Deny);
        }
        Self::allow(merge_destinations(destinations))
    }

    pub(crate) fn feed_digest(&self, digest: &mut CanonicalDigest, prefix: &str) {
        match self {
            Self::Deny => digest.str(&format!("{prefix}.mode"), "deny"),
            Self::Allow { destinations } => {
                digest.str(&format!("{prefix}.mode"), "allow");
                for (index, destination) in destinations.iter().enumerate() {
                    destination.feed_digest(digest, &format!("{prefix}.destinations.{index}"));
                }
            }
        }
    }
}

fn merge_destinations(destinations: BTreeSet<Destination>) -> BTreeSet<Destination> {
    let mut by_host = BTreeMap::<String, BTreeSet<u16>>::new();
    for destination in destinations {
        by_host
            .entry(destination.host)
            .or_default()
            .extend(destination.ports);
    }
    by_host
        .into_iter()
        .map(|(host, ports)| Destination { host, ports })
        .collect()
}

fn validate_destinations(destinations: &BTreeSet<Destination>) -> Result<(), NetworkGrantError> {
    if destinations.is_empty() {
        return Err(NetworkGrantError::EmptyAllowlist);
    }
    if destinations.len() > MAX_NETWORK_DESTINATIONS {
        return Err(NetworkGrantError::TooManyDestinations(destinations.len()));
    }
    Ok(())
}

fn validate_ports(ports: &BTreeSet<u16>) -> Result<(), NetworkGrantError> {
    if ports.is_empty() {
        return Err(NetworkGrantError::EmptyPorts);
    }
    if ports.len() > MAX_DESTINATION_PORTS {
        return Err(NetworkGrantError::TooManyPorts(ports.len()));
    }
    if ports.contains(&0) {
        return Err(NetworkGrantError::InvalidPort(0));
    }
    Ok(())
}

pub fn canonical_host(raw: String) -> Result<String, NetworkGrantError> {
    if raw.is_empty()
        || raw.len() > MAX_DESTINATION_HOST_BYTES
        || raw.contains('\0')
        || raw.contains('/')
        || raw.contains('\\')
        || raw.trim() != raw
    {
        return Err(NetworkGrantError::InvalidHost(raw));
    }
    let bracketed_ip = raw
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .is_some_and(|value| value.parse::<IpAddr>().is_ok());
    if raw.parse::<IpAddr>().is_ok() || bracketed_ip {
        return Err(NetworkGrantError::IpLiteral(raw));
    }
    if raw.contains(':') {
        return Err(NetworkGrantError::InvalidHost(raw));
    }
    let host = raw.to_ascii_lowercase();
    let labels = host.split('.').collect::<Vec<_>>();
    if labels.is_empty()
        || labels.iter().any(|label| {
            label.is_empty() || label.len() > 63 || label.starts_with('-') || label.ends_with('-')
        })
    {
        return Err(NetworkGrantError::InvalidHost(raw));
    }
    for label in &labels {
        if !label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(NetworkGrantError::InvalidHost(raw));
        }
    }
    Ok(host)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetworkGrantError {
    #[error("network destination host '{0}' is invalid")]
    InvalidHost(String),
    #[error("network destination host '{0}' is an IP literal; declare a DNS host")]
    IpLiteral(String),
    #[error("network destination must declare at least one port")]
    EmptyPorts,
    #[error("network destination declares {0} ports; limit is {MAX_DESTINATION_PORTS}")]
    TooManyPorts(usize),
    #[error("network destination port {0} is invalid")]
    InvalidPort(u16),
    #[error("network allow grant must declare at least one destination")]
    EmptyAllowlist,
    #[error("network allow grant declares {0} destinations; limit is {MAX_NETWORK_DESTINATIONS}")]
    TooManyDestinations(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_canonicalizes_case_and_rejects_ip_literals() {
        let destination = Destination::single("API.OpenAI.Com", 443).expect("destination");
        assert_eq!(destination.host(), "api.openai.com");
        assert!(destination.contains("api.openai.com", 443));
        assert!(destination.contains("API.OPENAI.COM", 443));

        assert!(matches!(
            Destination::single("127.0.0.1", 443),
            Err(NetworkGrantError::IpLiteral(_))
        ));
        assert!(matches!(
            Destination::single("::1", 443),
            Err(NetworkGrantError::IpLiteral(_))
        ));
    }

    #[test]
    fn grant_subset_uses_one_destination_predicate() {
        let ceiling = NetworkGrant::allow(BTreeSet::from([Destination::new(
            "example.com",
            BTreeSet::from([443, 8443]),
        )
        .unwrap()]))
        .unwrap();
        let granted = NetworkGrant::allow_single("example.com", 443).unwrap();
        let wrong_port = NetworkGrant::allow_single("example.com", 80).unwrap();
        let wrong_host = NetworkGrant::allow_single("other.example.com", 443).unwrap();

        assert!(granted.within(&ceiling));
        assert!(!wrong_port.within(&ceiling));
        assert!(!wrong_host.within(&ceiling));
        assert!(granted.allows("example.com", 443));
        assert!(!granted.allows("example.com", 80));
        assert!(!granted.allows("127.0.0.1", 443));
    }

    #[test]
    fn grant_deserialization_uses_constructor_validation() {
        let empty_allow = r#"{"mode":"allow","destinations":[]}"#;
        let err = serde_json::from_str::<NetworkGrant>(empty_allow)
            .expect_err("empty allowlist must not deserialize");
        assert!(err.to_string().contains("at least one destination"));

        let destinations = (0..=MAX_NETWORK_DESTINATIONS)
            .map(|index| format!(r#"{{"host":"host-{index}.example.com","ports":[443]}}"#))
            .collect::<Vec<_>>()
            .join(",");
        let too_many = format!(r#"{{"mode":"allow","destinations":[{destinations}]}}"#);
        let err = serde_json::from_str::<NetworkGrant>(&too_many)
            .expect_err("oversized allowlist must not deserialize");
        assert!(err.to_string().contains("limit"));
    }
}
