use std::path::{Path, PathBuf};

use lionclaw_runtime_api::{safe_relative_path, RuntimeExecutionContext};
use serde_json::Value;

use super::event_mapping::app_server_event_payload;

pub(crate) const CODEX_GENERATED_IMAGES_NATIVE_HOME_DIR: &str = ".codex/generated_images";
const CODEX_GENERATED_IMAGES_RUNTIME_DIR: &str = "/runtime/home/.codex/generated_images";

pub(crate) fn codex_generated_image_payload<'a>(
    method: Option<&'a str>,
    message: &'a Value,
) -> Option<&'a Value> {
    if let Some(("image_generation_end", payload)) = app_server_event_payload(method, message) {
        if codex_generated_image_is_publishable(method, payload) {
            return Some(payload);
        }
    }

    let item = message.get("item").unwrap_or(message);
    let item_type = item.get("type").and_then(Value::as_str);
    if matches!(method, Some("item/completed" | "item/updated"))
        && matches!(item_type, Some("imageGeneration" | "image_generation_call"))
        && codex_generated_image_is_publishable(method, item)
    {
        return Some(item);
    }

    if method.is_none() && message.get("type").and_then(Value::as_str) == Some("response_item") {
        let payload = message.get("payload")?;
        if payload.get("type").and_then(Value::as_str) == Some("image_generation_call")
            && codex_generated_image_is_publishable(method, payload)
        {
            return Some(payload);
        }
    }

    None
}

fn codex_generated_image_is_publishable(method: Option<&str>, payload: &Value) -> bool {
    if codex_generated_image_has_saved_path(payload) {
        return true;
    }

    match payload
        .get("status")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|status| !status.is_empty())
    {
        Some(status) => matches!(
            status.to_ascii_lowercase().as_str(),
            "completed" | "complete" | "succeeded" | "success" | "done"
        ),
        None => {
            method == Some("item/completed")
                || payload.get("type").and_then(Value::as_str) == Some("image_generation_end")
        }
    }
}

pub(crate) fn codex_generated_image_call_id(payload: &Value) -> Option<String> {
    payload
        .get("call_id")
        .or_else(|| payload.get("callId"))
        .or_else(|| payload.get("id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn codex_generated_image_has_saved_path(payload: &Value) -> bool {
    codex_generated_image_saved_path(payload).is_some()
}

pub(crate) fn codex_generated_image_saved_path(payload: &Value) -> Option<&str> {
    payload
        .get("saved_path")
        .or_else(|| payload.get("savedPath"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(crate) fn codex_default_generated_image_path(
    thread_id: &str,
    filename: &str,
    runtime_state_root: &Path,
    runtime_context: Option<&RuntimeExecutionContext>,
) -> PathBuf {
    let runtime_path = PathBuf::from(CODEX_GENERATED_IMAGES_RUNTIME_DIR)
        .join(thread_id)
        .join(filename);
    if let Some(path) =
        runtime_context.and_then(|context| context.host_path_for_runtime_path(&runtime_path))
    {
        return path;
    }
    runtime_state_root
        .join("home")
        .join(".codex")
        .join("generated_images")
        .join(thread_id)
        .join(filename)
}

pub(crate) fn codex_generated_image_path(
    raw: &str,
    runtime_state_root: &Path,
    runtime_context: Option<&RuntimeExecutionContext>,
) -> Option<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        if let Some(context) = runtime_context {
            return context.host_path_for_runtime_path(path);
        }
        if let Ok(container_relative) = path.strip_prefix("/runtime") {
            return runtime_state_child_path(runtime_state_root, container_relative);
        }
        return None;
    }
    let safe_path = safe_relative_path(path)?;
    if safe_path.as_os_str().is_empty() {
        return None;
    }
    if let Some(context) = runtime_context {
        return context.host_path_for_runtime_path(Path::new("/runtime").join(&safe_path));
    }
    Some(runtime_state_root.join(safe_path))
}

fn runtime_state_child_path(runtime_state_root: &Path, relative_path: &Path) -> Option<PathBuf> {
    let safe_path = safe_relative_path(relative_path)?;
    if safe_path.as_os_str().is_empty() {
        return None;
    }
    Some(runtime_state_root.join(safe_path))
}
