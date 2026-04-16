use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::body::Body;
use http_body_util::BodyExt;
use hyper::{Request, StatusCode, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use serde::de::DeserializeOwned;
use tokio::{
    net::{lookup_host, TcpStream},
    time::sleep,
};

use crate::contracts::DaemonInfoResponse;

#[derive(Debug, Clone)]
pub(crate) enum DaemonClassification {
    Absent,
    SameHome,
    SameHomeDifferentConfig,
    SameHomeDifferentProject,
    ForeignHome(DaemonInfoResponse),
    IncompatibleLionClaw,
    UnknownListener,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct HealthResponse {
    service: String,
}

#[derive(Debug)]
enum ProbeJsonResult<T> {
    Ok(T),
    Status,
    InvalidBody,
    Transport,
}

pub(crate) async fn classify_daemon(
    bind_addr: &str,
    expected_home_id: &str,
    expected_project_scope: &str,
    expected_config_fingerprint: &str,
) -> Result<DaemonClassification> {
    if !listener_is_present(bind_addr).await {
        return Ok(DaemonClassification::Absent);
    }

    match get_json::<DaemonInfoResponse>(bind_addr, "/v0/daemon/info").await? {
        ProbeJsonResult::Ok(info) => {
            if info.home_id == expected_home_id {
                if info.project_scope != expected_project_scope {
                    Ok(DaemonClassification::SameHomeDifferentProject)
                } else if info.config_fingerprint == expected_config_fingerprint {
                    Ok(DaemonClassification::SameHome)
                } else {
                    Ok(DaemonClassification::SameHomeDifferentConfig)
                }
            } else {
                Ok(DaemonClassification::ForeignHome(info))
            }
        }
        ProbeJsonResult::Status | ProbeJsonResult::InvalidBody | ProbeJsonResult::Transport => {
            match get_json::<HealthResponse>(bind_addr, "/health").await? {
                ProbeJsonResult::Ok(health) if health.service == "lionclawd" => {
                    Ok(DaemonClassification::IncompatibleLionClaw)
                }
                ProbeJsonResult::Ok(_)
                | ProbeJsonResult::Status
                | ProbeJsonResult::InvalidBody
                | ProbeJsonResult::Transport => Ok(DaemonClassification::UnknownListener),
            }
        }
    }
}

pub(crate) async fn wait_for_same_home_daemon(
    bind_addr: &str,
    expected_home_id: &str,
    expected_project_scope: &str,
    expected_config_fingerprint: &str,
    timeout_duration: Duration,
) -> Result<DaemonClassification> {
    let deadline = Instant::now() + timeout_duration;
    loop {
        let classification = classify_daemon(
            bind_addr,
            expected_home_id,
            expected_project_scope,
            expected_config_fingerprint,
        )
        .await?;
        if matches!(classification, DaemonClassification::SameHome) {
            return Ok(classification);
        }
        if Instant::now() >= deadline {
            return Ok(classification);
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn listener_is_present(bind_addr: &str) -> bool {
    let addrs = match lookup_host(bind_addr).await {
        Ok(values) => values.collect::<Vec<_>>(),
        Err(_) => return false,
    };

    for addr in addrs {
        if matches!(
            tokio::time::timeout(Duration::from_millis(250), TcpStream::connect(addr)).await,
            Ok(Ok(_))
        ) {
            return true;
        }
    }

    false
}

async fn get_json<T: DeserializeOwned>(bind_addr: &str, path: &str) -> Result<ProbeJsonResult<T>> {
    let uri: Uri = format!("http://{bind_addr}{path}")
        .parse()
        .with_context(|| format!("invalid daemon probe URI for '{bind_addr}{path}'"))?;
    let client = Client::builder(TokioExecutor::new()).build(HttpConnector::new());
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .context("failed to build daemon probe request")?;
    let response =
        match tokio::time::timeout(Duration::from_millis(500), client.request(request)).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) | Err(_) => return Ok(ProbeJsonResult::Transport),
        };
    if response.status() != StatusCode::OK {
        return Ok(ProbeJsonResult::Status);
    }

    let body = match response.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Ok(ProbeJsonResult::Transport),
    };
    match serde_json::from_slice::<T>(&body) {
        Ok(value) => Ok(ProbeJsonResult::Ok(value)),
        Err(_) => Ok(ProbeJsonResult::InvalidBody),
    }
}
