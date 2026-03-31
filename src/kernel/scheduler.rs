use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use tokio::time::sleep;
use uuid::Uuid;

use crate::contracts::{JobTickResponse, SessionHistoryPolicy, SessionOpenRequest, TrustTier};

use super::{
    core::Kernel,
    error::KernelError,
    jobs::{
        ClaimedSchedulerJob, SchedulerJobDeliveryStatus, SchedulerJobRecord, SchedulerJobRunRecord,
        SchedulerJobRunStatus, SchedulerJobTriggerKind,
    },
};

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub tick_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
pub struct SchedulerEngine {
    config: SchedulerConfig,
}

impl SchedulerEngine {
    pub fn new(config: SchedulerConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    pub async fn tick(&self, kernel: &Kernel) -> Result<JobTickResponse, KernelError> {
        let owner = format!("scheduler:{}", Uuid::new_v4());
        let lease_ttl = self.config.tick_interval.max(Duration::from_secs(60));
        if !kernel
            .job_store()
            .try_acquire_tick_lease(&owner, lease_ttl)
            .await
            .map_err(internal)?
        {
            return Ok(JobTickResponse { claimed_runs: 0 });
        }

        let tick_result = async {
            let mut claimed_runs = 0usize;
            while let Some(claimed_job) = self.claim_next_due_job(kernel).await? {
                kernel
                    .audit_log()
                    .append(
                        "job.run.claimed",
                        None,
                        Some("scheduler".to_string()),
                        json!({
                            "job_id": claimed_job.job.job_id,
                            "run_id": claimed_job.run.run_id,
                            "trigger_kind": claimed_job.run.trigger_kind.as_str(),
                        }),
                    )
                    .await
                    .map_err(internal)?;
                claimed_runs += 1;

                if let Err(err) = self.run_claimed_job(kernel, claimed_job).await {
                    let _ = kernel
                        .audit_log()
                        .append(
                            "job.run.execution_failed",
                            None,
                            Some("scheduler".to_string()),
                            json!({"error": err.to_string()}),
                        )
                        .await;
                }
            }

            Ok(JobTickResponse { claimed_runs })
        }
        .await;

        let release_result = kernel.job_store().release_tick_lease(&owner).await;
        release_result.map_err(internal)?;
        tick_result
    }

    pub async fn run_loop(&self, kernel: Arc<Kernel>) {
        loop {
            if let Err(err) = self.tick(&kernel).await {
                let _ = kernel
                    .audit_log()
                    .append(
                        "scheduler.tick.failed",
                        None,
                        Some("kernel".to_string()),
                        json!({"error": err.to_string()}),
                    )
                    .await;
            }
            sleep(self.config.tick_interval).await;
        }
    }

    pub async fn run_claimed_job(
        &self,
        kernel: &Kernel,
        claimed: ClaimedSchedulerJob,
    ) -> Result<(SchedulerJobRecord, SchedulerJobRunRecord), KernelError> {
        let job = claimed.job;
        let mut current_run = claimed.run;
        loop {
            let opened = kernel
                .open_session(SessionOpenRequest {
                    channel_id: "scheduler".to_string(),
                    peer_id: format!("job:{}", job.job_id),
                    trust_tier: TrustTier::Main,
                    history_policy: Some(SessionHistoryPolicy::Conservative),
                })
                .await?;
            let turn_id = Uuid::new_v4();
            let turn_result = kernel
                .execute_scheduled_job_turn(opened.session_id, turn_id, &job)
                .await;

            match turn_result {
                Ok(response) => {
                    let delivery_status = self
                        .deliver_job_result(kernel, &job, &response.assistant_text)
                        .await;
                    let updated_job = kernel
                        .job_store()
                        .complete_run_success(
                            current_run.run_id,
                            response.session_id,
                            response.turn_id,
                            delivery_status,
                        )
                        .await
                        .map_err(internal)?
                        .ok_or_else(|| {
                            KernelError::NotFound(
                                "scheduled job disappeared during completion".to_string(),
                            )
                        })?;
                    let final_run =
                        self.latest_job_run(kernel, job.job_id)
                            .await?
                            .ok_or_else(|| {
                                KernelError::NotFound("scheduled job run disappeared".to_string())
                            })?;
                    kernel
                        .audit_log()
                        .append(
                            "job.run.completed",
                            Some(response.session_id),
                            Some("scheduler".to_string()),
                            json!({
                                "job_id": job.job_id,
                                "run_id": final_run.run_id,
                                "turn_id": response.turn_id,
                                "delivery_status": delivery_status.as_str(),
                            }),
                        )
                        .await
                        .map_err(internal)?;
                    return Ok((updated_job, final_run));
                }
                Err(err) => {
                    if current_run.attempt_no <= job.retry_attempts {
                        current_run = kernel
                            .job_store()
                            .begin_retry_run(current_run.run_id, Utc::now())
                            .await
                            .map_err(internal)?
                            .ok_or_else(|| {
                                KernelError::Conflict(
                                    "scheduled retry could not be started".to_string(),
                                )
                            })?;
                        kernel
                            .audit_log()
                            .append(
                                "job.run.retry",
                                Some(opened.session_id),
                                Some("scheduler".to_string()),
                                json!({
                                    "job_id": job.job_id,
                                    "run_id": current_run.run_id,
                                    "attempt_no": current_run.attempt_no,
                                }),
                            )
                            .await
                            .map_err(internal)?;
                        continue;
                    }

                    let failure_summary = format!("Scheduled job '{}' failed: {}", job.name, err);
                    let delivery_status = self
                        .deliver_job_result(kernel, &job, &failure_summary)
                        .await;
                    let updated_job = kernel
                        .job_store()
                        .complete_run_failure(
                            current_run.run_id,
                            Some(opened.session_id),
                            Some(turn_id),
                            &err.to_string(),
                            SchedulerJobRunStatus::DeadLetter,
                            delivery_status,
                        )
                        .await
                        .map_err(internal)?
                        .ok_or_else(|| {
                            KernelError::NotFound(
                                "scheduled job disappeared during failure completion".to_string(),
                            )
                        })?;
                    let final_run =
                        self.latest_job_run(kernel, job.job_id)
                            .await?
                            .ok_or_else(|| {
                                KernelError::NotFound("scheduled job run disappeared".to_string())
                            })?;
                    kernel
                        .audit_log()
                        .append(
                            "job.run.failed",
                            Some(opened.session_id),
                            Some("scheduler".to_string()),
                            json!({
                                "job_id": job.job_id,
                                "run_id": final_run.run_id,
                                "error": err.to_string(),
                                "delivery_status": delivery_status.as_str(),
                            }),
                        )
                        .await
                        .map_err(internal)?;
                    return Ok((updated_job, final_run));
                }
            }
        }
    }

    async fn claim_next_due_job(
        &self,
        kernel: &Kernel,
    ) -> Result<Option<ClaimedSchedulerJob>, KernelError> {
        Ok(kernel
            .job_store()
            .claim_due_jobs(Utc::now(), 1, SchedulerJobTriggerKind::Schedule)
            .await
            .map_err(internal)?
            .into_iter()
            .next())
    }

    async fn latest_job_run(
        &self,
        kernel: &Kernel,
        job_id: Uuid,
    ) -> Result<Option<SchedulerJobRunRecord>, KernelError> {
        Ok(kernel
            .job_store()
            .list_runs(job_id, 1)
            .await
            .map_err(internal)?
            .into_iter()
            .next())
    }

    async fn deliver_job_result(
        &self,
        kernel: &Kernel,
        job: &SchedulerJobRecord,
        content: &str,
    ) -> SchedulerJobDeliveryStatus {
        let Some(delivery) = &job.delivery else {
            return SchedulerJobDeliveryStatus::NotRequested;
        };
        let text = if content.trim().is_empty() {
            format!(
                "Scheduled job '{}' completed with no assistant output.",
                job.name
            )
        } else {
            content.to_string()
        };
        match kernel
            .emit_channel_message(&delivery.channel_id, &delivery.peer_id, None, None, &text)
            .await
        {
            Ok(_) => SchedulerJobDeliveryStatus::Delivered,
            Err(_) => SchedulerJobDeliveryStatus::Failed,
        }
    }
}

fn internal(err: anyhow::Error) -> KernelError {
    KernelError::Internal(err.to_string())
}
