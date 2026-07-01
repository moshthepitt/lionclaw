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
            options.iter().find_map(|option| {
                let kind = option
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let name = option
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if kind.contains("reject")
                    || kind.contains("deny")
                    || name.to_ascii_lowercase().contains("reject")
                    || name.to_ascii_lowercase().contains("deny")
                {
                    option
                        .get("optionId")
                        .or_else(|| option.get("id"))
                        .and_then(Value::as_str)
                } else {
                    None
                }
            })
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
