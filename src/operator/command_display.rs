use std::path::PathBuf;

use crate::home::LionClawHome;

pub(crate) fn lionclaw_home_command_prefix(home: &LionClawHome) -> String {
    format!(
        "lionclaw --home {}",
        shell_quote_arg(&absolute_home_path(home).display().to_string())
    )
}

pub(crate) fn shell_quote_arg(value: &str) -> String {
    if !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '='))
    {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

fn absolute_home_path(home: &LionClawHome) -> PathBuf {
    let root = home.root();
    if root.is_absolute() {
        return root;
    }

    std::env::current_dir()
        .map(|cwd| cwd.join(&root))
        .unwrap_or(root)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{lionclaw_home_command_prefix, shell_quote_arg};
    use crate::home::LionClawHome;

    #[test]
    fn shell_quote_preserves_simple_arguments() {
        assert_eq!(shell_quote_arg("/tmp/lionclaw-home"), "/tmp/lionclaw-home");
    }

    #[test]
    fn shell_quote_handles_spaces_and_single_quotes() {
        assert_eq!(
            shell_quote_arg("/tmp/reviewer's home"),
            "'/tmp/reviewer'\\''s home'"
        );
    }

    #[test]
    fn home_command_prefix_targets_absolute_selected_home() {
        let home = LionClawHome::new(PathBuf::from("/tmp/reviewer home"));

        assert_eq!(
            lionclaw_home_command_prefix(&home),
            "lionclaw --home '/tmp/reviewer home'"
        );
    }
}
