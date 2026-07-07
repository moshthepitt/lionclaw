//! A strict YAML-subset frontmatter parser for role files. Deliberately not
//! a general YAML parser (that dependency is archived): it accepts exactly
//! the flat scalar keys a role declares and rejects everything else, so a
//! typo can never silently widen authority.
//!
//! ```text
//! ---
//! output: produces-artifact
//! runtime: codex
//! network: false
//! secrets: false
//! skills: [rust, git]
//! ---
//! <prompt body>
//! ```

use crate::model::OutputSemantics;

#[derive(Debug)]
pub struct RoleFrontmatter {
    pub output: OutputSemantics,
    pub network: bool,
    pub secrets: bool,
    pub runtime: Option<String>,
    pub skills: Vec<String>,
    pub prompt_body: String,
}

pub fn parse_role_file(text: &str) -> Result<RoleFrontmatter, String> {
    let (frontmatter, body) = split_frontmatter(text)?;
    let mut output: Option<OutputSemantics> = None;
    let mut network = false;
    let mut secrets = false;
    let mut runtime: Option<String> = None;
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
            "network" => network = parse_bool(key, value)?,
            "secrets" => secrets = parse_bool(key, value)?,
            "runtime" => runtime = Some(parse_scalar(key, value)?),
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
        runtime,
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
        "plans" => Ok(OutputSemantics::Plans),
        "produces-artifact" => Ok(OutputSemantics::ProducesArtifact),
        "emits-verdict" => Ok(OutputSemantics::EmitsVerdict),
        "egresses" => Ok(OutputSemantics::Egresses),
        other => Err(format!(
            "output must be one of plans|produces-artifact|emits-verdict|egresses, got '{other}'"
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
        .ok_or("skills must be an inline list like [a, b]")?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|item| {
            let item = item.trim().trim_matches('"').trim();
            if item.is_empty() {
                Err("skills list has an empty entry".to_string())
            } else {
                Ok(item.to_string())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_frontmatter() {
        let text = "---\noutput: emits-verdict\nruntime: codex\nnetwork: true\nsecrets: false\nskills: [rust, git]\n---\nJudge the work.\n";
        let fm = parse_role_file(text).expect("parse");
        assert_eq!(fm.output, OutputSemantics::EmitsVerdict);
        assert_eq!(fm.runtime.as_deref(), Some("codex"));
        assert!(fm.network);
        assert!(!fm.secrets);
        assert_eq!(fm.skills, vec!["rust", "git"]);
        assert_eq!(fm.prompt_body, "Judge the work.");
    }

    #[test]
    fn minimal_frontmatter_defaults() {
        let text = "---\noutput: produces-artifact\n---\nDo it.";
        let fm = parse_role_file(text).expect("parse");
        assert_eq!(fm.output, OutputSemantics::ProducesArtifact);
        assert!(!fm.network);
        assert!(!fm.secrets);
        assert!(fm.runtime.is_none());
        assert!(fm.skills.is_empty());
    }

    #[test]
    fn rejects_unknown_key() {
        let text = "---\noutput: plans\nwritable: true\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("unknown key"));
    }

    #[test]
    fn rejects_duplicate_key() {
        let text = "---\noutput: plans\noutput: emits-verdict\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("duplicate"));
    }

    #[test]
    fn rejects_missing_output() {
        let text = "---\nnetwork: true\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("output"));
    }

    #[test]
    fn rejects_missing_fence() {
        assert!(parse_role_file("output: plans\n\nbody").is_err());
        assert!(parse_role_file("---\noutput: plans\nbody").is_err());
    }

    #[test]
    fn rejects_empty_body() {
        let text = "---\noutput: plans\n---\n   \n";
        assert!(parse_role_file(text).unwrap_err().contains("empty"));
    }

    #[test]
    fn rejects_bad_bool() {
        let text = "---\noutput: plans\nnetwork: yes\n---\nx";
        assert!(parse_role_file(text).unwrap_err().contains("true or false"));
    }
}
