//! A strict YAML-subset frontmatter parser for role files. Deliberately not
//! a general YAML parser (that dependency is archived): it accepts exactly
//! the flat scalar keys a role declares and rejects everything else, so a
//! typo can never silently widen authority.
//!
//! ```text
//! ---
//! output: produces-artifact
//! runtime: codex
//! network: [api.example.com:443]
//! secrets: false
//! skills: []          # optional mission skill aliases
//! ---
//! <prompt body>
//! ```

use crate::model::{
    ConfinementResources, Destination, NetworkGrant, OutputSemantics, MAX_EXECUTION_DURATION_SECS,
};

#[derive(Debug)]
pub struct RoleFrontmatter {
    pub output: OutputSemantics,
    pub network: NetworkGrant,
    pub secrets: bool,
    pub install: Option<bool>,
    pub writes: Option<bool>,
    pub devices: Vec<String>,
    pub inputs: Vec<String>,
    pub resources: ConfinementResources,
    pub runtime: Option<String>,
    pub timeout_secs: Option<u64>,
    pub skills: Vec<String>,
    pub prompt_body: String,
}

pub fn parse_role_file(text: &str) -> Result<RoleFrontmatter, String> {
    let (frontmatter, body) = split_frontmatter(text)?;
    let mut output: Option<OutputSemantics> = None;
    let mut network = NetworkGrant::Deny;
    let mut secrets = false;
    let mut install = None;
    let mut writes = None;
    let mut devices = Vec::new();
    let mut inputs = Vec::new();
    let mut tmpfs = Vec::new();
    let mut runtime: Option<String> = None;
    let mut timeout_secs: Option<u64> = None;
    let mut skills: Vec<String> = Vec::new();
    let mut seen = std::collections::BTreeSet::new();

    for (lineno, raw_line) in frontmatter.lines().enumerate() {
        let line = raw_line.trim_end();
        if line.trim().is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once(':')
            .ok_or_else(|| format!("line {}: expected 'key: value'", lineno + 1))?;
        let key = key.trim();
        let value = value.trim();
        if !seen.insert(key.to_string()) {
            return Err(format!("duplicate key '{key}'"));
        }
        match key {
            "output" => output = Some(parse_output(value)?),
            "network" => network = parse_network_grant(value)?,
            "secrets" => secrets = parse_bool(key, value)?,
            "install" => install = Some(parse_bool(key, value)?),
            "writes" => writes = Some(parse_bool(key, value)?),
            "devices" => devices = parse_string_list(value)?,
            "inputs" => inputs = parse_string_list(value)?,
            "tmpfs" => tmpfs = parse_string_list(value)?,
            "runtime" => runtime = Some(parse_scalar(key, value)?),
            "timeout-secs" => {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|_| "timeout-secs must be a positive integer".to_string())?;
                if parsed == 0 || parsed > MAX_EXECUTION_DURATION_SECS {
                    return Err(format!(
                        "timeout-secs must be between 1 and {MAX_EXECUTION_DURATION_SECS}"
                    ));
                }
                timeout_secs = Some(parsed);
            }
            "skills" => skills = parse_string_list(value)?,
            other => return Err(format!("unknown key '{other}'")),
        }
    }

    let output = output.ok_or("missing required key 'output'")?;
    let prompt_body = body.trim();
    if prompt_body.is_empty() {
        return Err("role prompt body is empty".to_string());
    }
    Ok(RoleFrontmatter {
        output,
        network,
        secrets,
        install,
        writes,
        devices,
        inputs,
        resources: ConfinementResources { tmpfs },
        runtime,
        timeout_secs,
        skills,
        prompt_body: prompt_body.to_string(),
    })
}

fn split_frontmatter(text: &str) -> Result<(&str, &str), String> {
    let rest = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
        .ok_or("role file must start with a '---' frontmatter fence")?;
    // Find the closing fence at the start of a line.
    let mut search_start = 0;
    loop {
        let idx = rest[search_start..]
            .find("---")
            .map(|i| search_start + i)
            .ok_or("role file has no closing '---' fence")?;
        let at_line_start = idx == 0 || rest.as_bytes()[idx - 1] == b'\n';
        let after = &rest[idx + 3..];
        let fence_line_ends =
            after.is_empty() || after.starts_with('\n') || after.starts_with("\r\n");
        if at_line_start && fence_line_ends {
            let frontmatter = &rest[..idx];
            let body = after
                .strip_prefix('\n')
                .or_else(|| after.strip_prefix("\r\n"))
                .unwrap_or(after);
            return Ok((frontmatter, body));
        }
        search_start = idx + 3;
    }
}

fn parse_output(value: &str) -> Result<OutputSemantics, String> {
    match value {
        "produces-report" => Ok(OutputSemantics::ProducesReport),
        "produces-artifact" => Ok(OutputSemantics::ProducesArtifact),
        "emits-verdict" => Ok(OutputSemantics::EmitsVerdict),
        "emits-gap-verdict" => Ok(OutputSemantics::EmitsGapVerdict),
        "proposes-plan" => Ok(OutputSemantics::ProposesPlan),
        other => Err(format!(
            "output must be one of produces-report|produces-artifact|emits-verdict|emits-gap-verdict|proposes-plan, \
             got '{other}'"
        )),
    }
}

fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!("{key} must be true or false, got '{other}'")),
    }
}

fn parse_network_grant(value: &str) -> Result<NetworkGrant, String> {
    let destinations = parse_string_list(value)?
        .into_iter()
        .map(|item| parse_destination_item(&item))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if destinations.is_empty() {
        return Ok(NetworkGrant::Deny);
    }
    NetworkGrant::allow(destinations).map_err(|error| error.to_string())
}

fn parse_destination_item(item: &str) -> Result<Destination, String> {
    let (host, port) = item
        .rsplit_once(':')
        .ok_or_else(|| format!("network destination '{item}' must be host:port"))?;
    if host.contains(':') {
        return Err(format!(
            "network destination '{item}' must use a DNS host and one declared port"
        ));
    }
    let port = port
        .parse::<u16>()
        .map_err(|_| format!("network destination '{item}' has invalid port"))?;
    Destination::single(host, port).map_err(|error| error.to_string())
}

fn parse_scalar(key: &str, value: &str) -> Result<String, String> {
    let value = value.trim_matches('"').trim();
    if value.is_empty() {
        return Err(format!("{key} must not be empty"));
    }
    if value.contains(['[', ']', '{', '}', ':']) {
        return Err(format!("{key} must be a plain scalar"));
    }
    Ok(value.to_string())
}

fn parse_string_list(value: &str) -> Result<Vec<String>, String> {
    let inner = value
        .strip_prefix('[')
        .and_then(|v| v.strip_suffix(']'))
        .ok_or("value must be an inline list like [a, b]")?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut items = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    for (index, character) in inner.char_indices() {
        match character {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                items.push(parse_list_item(&inner[start..index])?);
                start = index + 1;
            }
            _ => {}
        }
    }
    if in_quotes {
        return Err("list has an unterminated quoted entry".to_string());
    }
    items.push(parse_list_item(&inner[start..])?);
    Ok(items)
}

fn parse_list_item(raw: &str) -> Result<String, String> {
    let item = raw.trim().trim_matches('"').trim();
    if item.is_empty() {
        Err("list has an empty entry".to_string())
    } else {
        Ok(item.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_frontmatter() {
        let text = "---\noutput: emits-verdict\nruntime: codex\nnetwork: [api.example.com:443]\nsecrets: false\nskills: [rust, git]\n---\nJudge the work.\n";
        let fm = parse_role_file(text).expect("parse");
        assert_eq!(fm.output, OutputSemantics::EmitsVerdict);
        assert_eq!(fm.runtime.as_deref(), Some("codex"));
        assert!(fm.network.allows("api.example.com", 443));
        assert!(!fm.secrets);
        assert_eq!(fm.skills, vec!["rust", "git"]);
        assert_eq!(fm.prompt_body, "Judge the work.");
    }

    #[test]
    fn minimal_frontmatter_defaults() {
        let text = "---\noutput: produces-artifact\n---\nDo it.";
        let fm = parse_role_file(text).expect("parse");
        assert_eq!(fm.output, OutputSemantics::ProducesArtifact);
        assert!(fm.network.is_denied());
        assert!(!fm.secrets);
        assert!(fm.runtime.is_none());
        assert!(fm.skills.is_empty());
    }

    #[test]
    fn rejects_unknown_key() {
        let text = "---\noutput: produces-report\nwritable: true\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("unknown key"));
    }

    #[test]
    fn rejects_duplicate_key() {
        let text = "---\noutput: produces-report\noutput: emits-verdict\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("duplicate"));
    }

    #[test]
    fn rejects_missing_output() {
        let text = "---\nnetwork: [api.example.com:443]\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("output"));
    }

    #[test]
    fn rejects_missing_fence() {
        assert!(parse_role_file("output: produces-report\n\nbody").is_err());
        assert!(parse_role_file("---\noutput: produces-report\nbody").is_err());
    }

    #[test]
    fn rejects_empty_body() {
        let text = "---\noutput: produces-report\n---\n   \n";
        assert!(parse_role_file(text).unwrap_err().contains("empty"));
    }

    #[test]
    fn rejects_bad_bool() {
        let text = "---\noutput: produces-report\nnetwork: yes\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("inline list"));
    }

    #[test]
    fn timeout_must_fit_the_effect_deadline_representation() {
        let first_overflow = MAX_EXECUTION_DURATION_SECS + 1;
        let rejected =
            format!("---\noutput: produces-report\ntimeout-secs: {first_overflow}\n---\nx");
        assert!(parse_role_file(&rejected).is_err());

        let maximum = MAX_EXECUTION_DURATION_SECS;
        let accepted = format!("---\noutput: produces-report\ntimeout-secs: {maximum}\n---\nx");
        assert_eq!(
            parse_role_file(&accepted).unwrap().timeout_secs,
            Some(maximum)
        );
    }

    #[test]
    fn parses_quoted_tmpfs_entries_with_commas() {
        let text =
            "---\noutput: produces-report\ntmpfs: [\"/tmp:rw,size=1g\", \"/cache:rw,size=64m\"]\n---\nx";
        let fm = parse_role_file(text).expect("parse");
        assert_eq!(
            fm.resources.tmpfs,
            vec![
                "/tmp:rw,size=1g".to_string(),
                "/cache:rw,size=64m".to_string()
            ]
        );
    }
}
