#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassifiedInput {
    Empty,
    Prompt(String),
    LionClawControl(LionClawControlInput),
    RuntimeControl(RuntimeControlCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LionClawControlInput {
    pub command_name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeControlCommand {
    pub raw: String,
    pub command_name: String,
    pub arguments: String,
}

pub fn classify_input(raw: &str) -> ClassifiedInput {
    let input = raw.trim_end_matches(['\r', '\n']);
    if input.trim().is_empty() {
        return ClassifiedInput::Empty;
    }

    if input.starts_with(char::is_whitespace) {
        return ClassifiedInput::Prompt(input.to_string());
    }

    if !input.starts_with('/') {
        return ClassifiedInput::Prompt(input.to_string());
    }

    let (command_token, arguments) = split_command(input);
    if is_path_like_slash_token(command_token) {
        return ClassifiedInput::Prompt(input.to_string());
    }

    if command_token == "/lionclaw" {
        return ClassifiedInput::LionClawControl(LionClawControlInput {
            command_name: first_argument(arguments).to_string(),
            arguments: remaining_arguments(arguments).to_string(),
        });
    }

    let command_name = command_token.trim_start_matches('/');
    if command_name.is_empty() {
        return ClassifiedInput::Prompt(input.to_string());
    }

    ClassifiedInput::RuntimeControl(RuntimeControlCommand {
        raw: input.to_string(),
        command_name: command_name.to_string(),
        arguments: arguments.to_string(),
    })
}

fn split_command(input: &str) -> (&str, &str) {
    match input.split_once(char::is_whitespace) {
        Some((command, rest)) => (command, rest.trim_start()),
        None => (input, ""),
    }
}

fn first_argument(input: &str) -> &str {
    match input.split_once(char::is_whitespace) {
        Some((first, _)) => first,
        None => input,
    }
}

fn remaining_arguments(input: &str) -> &str {
    match input.split_once(char::is_whitespace) {
        Some((_, rest)) => rest.trim_start(),
        None => "",
    }
}

fn is_path_like_slash_token(command_token: &str) -> bool {
    let Some(rest) = command_token.strip_prefix('/') else {
        return false;
    };
    rest.contains('/')
}

#[cfg(test)]
mod tests {
    use super::{classify_input, ClassifiedInput, LionClawControlInput, RuntimeControlCommand};

    #[test]
    fn classifies_empty_input() {
        assert_eq!(classify_input(""), ClassifiedInput::Empty);
        assert_eq!(classify_input(" \n"), ClassifiedInput::Empty);
    }

    #[test]
    fn classifies_normal_prompt() {
        assert_eq!(
            classify_input("fix the test"),
            ClassifiedInput::Prompt("fix the test".to_string())
        );
    }

    #[test]
    fn classifies_lionclaw_control() {
        assert_eq!(
            classify_input("/lionclaw reset"),
            ClassifiedInput::LionClawControl(LionClawControlInput {
                command_name: "reset".to_string(),
                arguments: String::new(),
            })
        );
        assert_eq!(
            classify_input("/lionclaw retry now"),
            ClassifiedInput::LionClawControl(LionClawControlInput {
                command_name: "retry".to_string(),
                arguments: "now".to_string(),
            })
        );
    }

    #[test]
    fn classifies_runtime_control_without_knowing_runtime_commands() {
        for command in ["/compact", "/rename project", "/model", "/permissions"] {
            assert!(matches!(
                classify_input(command),
                ClassifiedInput::RuntimeControl(_)
            ));
        }

        assert_eq!(
            classify_input("/rename project"),
            ClassifiedInput::RuntimeControl(RuntimeControlCommand {
                raw: "/rename project".to_string(),
                command_name: "rename".to_string(),
                arguments: "project".to_string(),
            })
        );
    }

    #[test]
    fn classifies_path_like_slash_input_as_prompt() {
        assert_eq!(
            classify_input("/"),
            ClassifiedInput::Prompt("/".to_string())
        );
        assert_eq!(
            classify_input("/home/user/file.rs fix this"),
            ClassifiedInput::Prompt("/home/user/file.rs fix this".to_string())
        );
    }

    #[test]
    fn classifies_leading_space_slash_input_as_prompt() {
        assert_eq!(
            classify_input(" /compact"),
            ClassifiedInput::Prompt(" /compact".to_string())
        );
    }
}
