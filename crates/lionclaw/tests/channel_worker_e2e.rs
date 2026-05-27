mod common;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use common::TestHome;
use lionclaw::{
    api::build_router,
    contracts::{
        ChannelHealthCheck, ChannelHealthReportRequest, ChannelHealthReportResponse,
        ChannelHealthStatus, ChannelInboundOutcome, ChannelInboundRequest, ChannelInboundResponse,
        ChannelOutboxDeliveryView, ChannelOutboxPullRequest, ChannelOutboxPullResponse,
        ChannelOutboxReportOutcomeDto, ChannelOutboxReportRequest, ChannelOutboxReportResponse,
        ChannelPairingApproveRequest, ChannelSessionBinding, ChannelStreamAckRequest,
        ChannelStreamAckResponse, ChannelStreamEventView, ChannelStreamPullRequest,
        ChannelStreamPullResponse, ChannelStreamStartMode, ChannelTrigger, DaemonInfoResponse,
        StreamEventKindDto, StreamLaneDto, TrustTier,
    },
    kernel::{Kernel, KernelOptions},
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

const CHANNEL_ID: &str = "fixture-loopback";
const PEER_ID: &str = "peer-a";
const WORKER_ID: &str = "fixture-worker";

#[tokio::test]
async fn test_only_channel_fixture_exercises_worker_http_contract() -> Result<()> {
    let env = TestHome::new().await;
    env.install_channel_fixture("loopback-worker-fixture", CHANNEL_ID)
        .await;

    let kernel = env
        .kernel_with_options(KernelOptions {
            default_runtime_id: Some("mock".to_string()),
            runtime_root: Some(env.home().runtime_dir()),
            ..KernelOptions::default()
        })
        .await;
    let (base_url, server) = spawn_test_api(&env, kernel).await?;
    let client = reqwest::Client::new();

    let pending: ChannelInboundResponse = post_json(
        &client,
        &base_url,
        "/v0/channels/inbound",
        &inbound_request("pairing-1", "hello before approval"),
    )
    .await?;
    assert_eq!(pending.outcome, ChannelInboundOutcome::PendingApproval);

    let pairing_code = pending
        .pairing_code
        .clone()
        .context("pending approval should return pairing code")?;
    post_json::<_, serde_json::Value>(
        &client,
        &base_url,
        "/v0/channels/pairing/approve",
        &ChannelPairingApproveRequest {
            channel_id: CHANNEL_ID.to_string(),
            pairing_id: None,
            pairing_code: Some(pairing_code),
            routing_profile: None,
            trust_tier: Some(TrustTier::Main),
            label: Some("Loopback peer".to_string()),
        },
    )
    .await?;

    let queued: ChannelInboundResponse = post_json(
        &client,
        &base_url,
        "/v0/channels/inbound",
        &inbound_request("message-1", "worker e2e prompt"),
    )
    .await?;
    assert_eq!(queued.outcome, ChannelInboundOutcome::Queued);
    let turn_id = queued
        .turn_id
        .context("queued inbound should start a turn")?;

    let events = wait_for_turn_done(&client, &base_url, turn_id).await?;
    let through_sequence = events
        .iter()
        .map(|event| event.sequence)
        .max()
        .context("stream should include events to ack")?;
    let ack: ChannelStreamAckResponse = post_json(
        &client,
        &base_url,
        "/v0/channels/stream/ack",
        &ChannelStreamAckRequest {
            channel_id: CHANNEL_ID.to_string(),
            consumer_id: WORKER_ID.to_string(),
            through_sequence,
        },
    )
    .await?;
    assert!(ack.acknowledged);

    let delivery = pull_delivery(&client, &base_url).await?;
    assert_eq!(delivery.channel_id, CHANNEL_ID);
    assert_eq!(delivery.conversation_ref, PEER_ID);
    assert_eq!(delivery.turn_id, Some(turn_id));
    assert!(delivery.content.text.contains("[mock]"));
    assert!(delivery.content.text.contains("worker e2e prompt"));
    assert!(delivery.content.attachments.is_empty());

    let report: ChannelOutboxReportResponse = post_json(
        &client,
        &base_url,
        "/v0/channels/outbox/report",
        &ChannelOutboxReportRequest {
            delivery_id: delivery.delivery_id,
            attempt_id: delivery.attempt_id,
            channel_id: CHANNEL_ID.to_string(),
            worker_id: WORKER_ID.to_string(),
            outcome: ChannelOutboxReportOutcomeDto::Delivered,
            provider_receipt: Some(serde_json::json!({"message_ref": "fixture-message-1"})),
            error_code: None,
            error_text: None,
        },
    )
    .await?;
    assert!(report.accepted);

    let empty: ChannelOutboxPullResponse = post_json(
        &client,
        &base_url,
        "/v0/channels/outbox/pull",
        &outbox_pull(),
    )
    .await?;
    assert!(empty.deliveries.is_empty());

    let health: ChannelHealthReportResponse = post_json(
        &client,
        &base_url,
        "/v0/channels/health/report",
        &ChannelHealthReportRequest {
            channel_id: CHANNEL_ID.to_string(),
            reporter_id: WORKER_ID.to_string(),
            status: ChannelHealthStatus::Ok,
            checks: vec![ChannelHealthCheck {
                code: "fixture.loopback".to_string(),
                status: ChannelHealthStatus::Ok,
                message: "loopback worker reached LionClaw API".to_string(),
                details: serde_json::json!({"deliveries": 1}),
            }],
            observed_at: Utc::now(),
        },
    )
    .await?;
    assert!(health.accepted);

    server.abort();
    Ok(())
}

async fn spawn_test_api(
    env: &TestHome,
    kernel: Kernel,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind test API")?;
    let bind_addr = listener.local_addr().context("read test API addr")?;
    let app = build_router(
        Arc::new(kernel),
        DaemonInfoResponse {
            daemon: "lionclawd".to_string(),
            status: "ok".to_string(),
            home_id: "test-home".to_string(),
            home_root: env.home().root().to_string_lossy().into_owned(),
            bind_addr: bind_addr.to_string(),
            project_scope: "test-project".to_string(),
            daemon_fingerprint: "test-fingerprint".to_string(),
        },
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve test LionClaw API");
    });
    Ok((format!("http://{bind_addr}"), server))
}

fn inbound_request(event_id: &str, text: &str) -> ChannelInboundRequest {
    ChannelInboundRequest {
        channel_id: CHANNEL_ID.to_string(),
        event_id: event_id.to_string(),
        sender_ref: PEER_ID.to_string(),
        conversation_ref: PEER_ID.to_string(),
        thread_ref: None,
        message_ref: Some(event_id.to_string()),
        text: Some(text.to_string()),
        attachments: Vec::new(),
        reply_to_ref: None,
        trigger: ChannelTrigger::Dm,
        session_binding: ChannelSessionBinding::Grant,
        received_at: None,
        provider_metadata: serde_json::json!({}),
    }
}

fn outbox_pull() -> ChannelOutboxPullRequest {
    ChannelOutboxPullRequest {
        channel_id: CHANNEL_ID.to_string(),
        worker_id: WORKER_ID.to_string(),
        conversation_ref: None,
        thread_ref: None,
        limit: Some(10),
        lease_ms: Some(120_000),
    }
}

async fn wait_for_turn_done(
    client: &reqwest::Client,
    base_url: &str,
    turn_id: Uuid,
) -> Result<Vec<ChannelStreamEventView>> {
    for _ in 0..60 {
        let stream: ChannelStreamPullResponse = post_json(
            client,
            base_url,
            "/v0/channels/stream/pull",
            &ChannelStreamPullRequest {
                channel_id: CHANNEL_ID.to_string(),
                consumer_id: WORKER_ID.to_string(),
                start_mode: Some(ChannelStreamStartMode::Resume),
                start_after_sequence: None,
                limit: Some(50),
                wait_ms: Some(50),
            },
        )
        .await?;
        let saw_answer = stream.events.iter().any(|event| {
            event.turn_id == Some(turn_id)
                && event.kind == StreamEventKindDto::MessageDelta
                && event.lane == Some(StreamLaneDto::Answer)
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("worker e2e prompt"))
        });
        let saw_done = stream
            .events
            .iter()
            .any(|event| event.turn_id == Some(turn_id) && event.kind == StreamEventKindDto::Done);
        if saw_answer && saw_done {
            return Ok(stream.events);
        }
        sleep(Duration::from_millis(25)).await;
    }
    bail!("timed out waiting for channel stream completion");
}

async fn pull_delivery(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<ChannelOutboxDeliveryView> {
    for _ in 0..20 {
        let outbox: ChannelOutboxPullResponse =
            post_json(client, base_url, "/v0/channels/outbox/pull", &outbox_pull()).await?;
        if let Some(delivery) = outbox.deliveries.into_iter().next() {
            return Ok(delivery);
        }
        sleep(Duration::from_millis(25)).await;
    }
    bail!("timed out waiting for outbox delivery");
}

async fn post_json<T, R>(
    client: &reqwest::Client,
    base_url: &str,
    path: &str,
    payload: &T,
) -> Result<R>
where
    T: Serialize + ?Sized,
    R: DeserializeOwned,
{
    let response = client
        .post(format!("{base_url}{path}"))
        .json(payload)
        .send()
        .await
        .with_context(|| format!("POST {path}"))?
        .error_for_status()
        .with_context(|| format!("POST {path} returned error status"))?;
    response
        .json()
        .await
        .with_context(|| format!("decode POST {path} response"))
}
