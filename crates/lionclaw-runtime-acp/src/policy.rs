use serde_json::{json, Value};

pub(crate) fn acp_error_response(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

pub(crate) fn acp_permission_denial(params: Option<&Value>) -> Value {
    let reject_option = params
        .and_then(|params| params.get("options"))
        .and_then(Value::as_array)
        .and_then(|options| {
            denial_option_id(options, "reject_once")
                .or_else(|| denial_option_id(options, "reject_always"))
        });

    match reject_option {
        Some(option_id) => json!({
            "outcome": {
                "outcome": "selected",
                "optionId": option_id,
            },
        }),
        None => json!({
            "outcome": {
                "outcome": "cancelled",
            },
        }),
    }
}

fn denial_option_id<'a>(options: &'a [Value], kind: &str) -> Option<&'a str> {
    options.iter().find_map(|option| {
        if option.get("kind").and_then(Value::as_str) != Some(kind) {
            return None;
        }
        option
            .get("optionId")
            .or_else(|| option.get("id"))
            .and_then(Value::as_str)
            .filter(|option_id| !option_id.is_empty())
    })
}
