use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use chrono::Utc;
use serde_json::json;
use tokio::{sync::oneshot, time::sleep};
use uuid::Uuid;

use crate::contracts::{JobTickResponse, SessionHistoryPolicy, SessionOpenRequest, TrustTier};

use super::{
    core::Kernel,
    error::KernelError,
    jobs::{
        ClaimedSchedulerJob, SchedulerJobDeliveryStatus, SchedulerJobRecord, SchedulerJobRunRecord,
        SchedulerJobRunStatus,
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

        let renewal_handle = self.spawn_lease_renewal(kernel, owner.clone(), lease_ttl);
        let tick_result = async {
            let mut claimed_runs = 0usize;
            while let Some(next_due_job) = self.peek_next_due_job(kernel).await? {
                if let Err(err) = kernel
                    .validate_runtime_launch_prerequisites(&next_due_job.runtime_id)
                    .await
                {
                    let Some(claimed_job) =
                        self.claim_and_record_due_job(kernel, &next_due_job).await?
                    else {
                        continue;
                    };
                    claimed_runs += 1;
                    self.fail_claimed_job_preflight(kernel, claimed_job, &err)
                        .await?;
                    continue;
                }
                let Some(claimed_job) =
                    self.claim_and_record_due_job(kernel, &next_due_job).await?
                else {
                    continue;
                };
                claimed_runs += 1;

                self.run_claimed_job(kernel, claimed_job).await?;
            }

            Ok(JobTickResponse { claimed_runs })
        }
        .await;

        let renewal_result = renewal_handle.stop().await;
        let release_result = kernel.job_store().release_tick_lease(&owner).await;
        release_result.map_err(internal)?;
        renewal_result?;
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
            let attempt_result = self.run_job_attempt(kernel, &job, &current_run).await;
            match attempt_result {
                Ok(AttemptOutcome::Retry(next_run)) => {
                    current_run = next_run;
                }
                Ok(AttemptOutcome::Finished(result)) => return Ok(*result),
                Err(err) => {
                    let _ = kernel
                        .job_store()
                        .interrupt_run(
                            current_run.run_id,
                            &format!("scheduled job execution failed unexpectedly: {err}"),
                        )
                        .await;
                    let _ = kernel
                        .audit_log()
                        .append(
                            "job.run.execution_failed",
                            None,
                            Some("scheduler".to_string()),
                            json!({
                                "job_id": job.job_id,
                                "run_id": current_run.run_id,
                                "error": err.to_string(),
                            }),
                        )
                        .await;
                    return Err(err);
                }
            }
        }
    }

    async fn peek_next_due_job(
        &self,
        kernel: &Kernel,
    ) -> Result<Option<SchedulerJobRecord>, KernelError> {
        kernel
            .job_store()
            .peek_next_due_job(Utc::now())
            .await
            .map_err(internal)
    }

    async fn claim_due_job(
        &self,
        kernel: &Kernel,
        job: &SchedulerJobRecord,
    ) -> Result<Option<ClaimedSchedulerJob>, KernelError> {
        kernel
            .job_store()
            .claim_scheduled_run(job.job_id, job.next_run_at, Utc::now())
            .await
            .map_err(internal)
    }

    async fn claim_and_record_due_job(
        &self,
        kernel: &Kernel,
        job: &SchedulerJobRecord,
    ) -> Result<Option<ClaimedSchedulerJob>, KernelError> {
        let Some(claimed_job) = self.claim_due_job(kernel, job).await? else {
            return Ok(None);
        };
        let _ = kernel
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
            .await;
        Ok(Some(claimed_job))
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

    async fn fail_claimed_job_preflight(
        &self,
        kernel: &Kernel,
        claimed: ClaimedSchedulerJob,
        error: &KernelError,
    ) -> Result<(), KernelError> {
        let ClaimedSchedulerJob { job, run } = claimed;
        let error_text = error.to_string();
        let failure_summary = format!("Scheduled job '{}' failed: {}", job.name, error_text);
        let delivery_status = self
            .deliver_job_result(kernel, &job, &failure_summary)
            .await;
        let final_status = if matches!(job.schedule, super::jobs::JobSchedule::Once { .. }) {
            SchedulerJobRunStatus::DeadLetter
        } else {
            SchedulerJobRunStatus::Failed
        };
        let _updated_job = kernel
            .job_store()
            .complete_run_failure(
                run.run_id,
                None,
                None,
                &error_text,
                final_status,
                delivery_status,
            )
            .await
            .map_err(internal)?
            .ok_or_else(|| {
                KernelError::NotFound(
                    "scheduled job disappeared during preflight failure".to_string(),
                )
            })?;
        let final_run = kernel
            .job_store()
            .get_run(run.run_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| KernelError::NotFound("scheduled job run disappeared".to_string()))?;
        let _ = kernel
            .audit_log()
            .append(
                "job.run.failed",
                None,
                Some("scheduler".to_string()),
                json!({
                    "job_id": job.job_id,
                    "run_id": final_run.run_id,
                    "error": error_text.clone(),
                    "delivery_status": delivery_status.as_str(),
                    "failure_phase": "preflight",
                }),
            )
            .await;
        let _ = kernel
            .record_scheduler_continuity_failure(&job, &final_run, &error_text)
            .await;
        Ok(())
    }

    async fn run_job_attempt(
        &self,
        kernel: &Kernel,
        job: &SchedulerJobRecord,
        current_run: &SchedulerJobRunRecord,
    ) -> Result<AttemptOutcome, KernelError> {
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
            .execute_scheduled_job_turn(opened.session_id, turn_id, job)
            .await;

        match turn_result {
            Ok(response) => {
                let delivery_status = self
                    .deliver_job_result(kernel, job, &response.assistant_text)
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
                let final_run = kernel
                    .job_store()
                    .get_run(current_run.run_id)
                    .await
                    .map_err(internal)?
                    .ok_or_else(|| {
                        KernelError::NotFound("scheduled job run disappeared".to_string())
                    })?;
                let _ = kernel
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
                    .await;
                let _ = kernel
                    .record_scheduler_continuity_success(job, &final_run, &response.assistant_text)
                    .await;
                Ok(AttemptOutcome::Finished(Box::new((updated_job, final_run))))
            }
            Err(err) => {
                if current_run.attempt_no <= job.retry_attempts {
                    let next_run = kernel
                        .job_store()
                        .begin_retry_run(current_run.run_id, Utc::now())
                        .await
                        .map_err(internal)?
                        .ok_or_else(|| {
                            KernelError::Conflict(
                                "scheduled retry could not be started".to_string(),
                            )
                        })?;
                    let _ = kernel
                        .audit_log()
                        .append(
                            "job.run.retry",
                            Some(opened.session_id),
                            Some("scheduler".to_string()),
                            json!({
                                "job_id": job.job_id,
                                "run_id": next_run.run_id,
                                "attempt_no": next_run.attempt_no,
                            }),
                        )
                        .await;
                    return Ok(AttemptOutcome::Retry(next_run));
                }

                let failure_summary = format!("Scheduled job '{}' failed: {}", job.name, err);
                let delivery_status = self.deliver_job_result(kernel, job, &failure_summary).await;
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
                let final_run = kernel
                    .job_store()
                    .get_run(current_run.run_id)
                    .await
                    .map_err(internal)?
                    .ok_or_else(|| {
                        KernelError::NotFound("scheduled job run disappeared".to_string())
                    })?;
                let _ = kernel
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
                    .await;
                let _ = kernel
                    .record_scheduler_continuity_failure(job, &final_run, &err.to_string())
                    .await;
                Ok(AttemptOutcome::Finished(Box::new((updated_job, final_run))))
            }
        }
    }

    fn spawn_lease_renewal(
        &self,
        kernel: &Kernel,
        owner: String,
        lease_ttl: Duration,
    ) -> TickLeaseRenewal {
        let jobs = kernel.job_store().clone();
        let renew_every = (lease_ttl / 2).max(Duration::from_secs(1));
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    _ = sleep(renew_every) => {
                        let renewed = jobs.renew_tick_lease(&owner, lease_ttl).await?;
                        if !renewed {
                            return Err(anyhow!("scheduler tick lease lost during execution"));
                        }
                    }
                }
            }
        });
        TickLeaseRenewal {
            stop_tx: Some(stop_tx),
            handle,
        }
    }
}

struct TickLeaseRenewal {
    stop_tx: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

enum AttemptOutcome {
    Retry(SchedulerJobRunRecord),
    Finished(Box<(SchedulerJobRecord, SchedulerJobRunRecord)>),
}

impl TickLeaseRenewal {
    async fn stop(mut self) -> Result<(), KernelError> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        let renewal_result = self.handle.await.map_err(|err| internal(err.into()))?;
        renewal_result.map_err(internal)?;
        Ok(())
    }
}

fn internal(err: anyhow::Error) -> KernelError {
    KernelError::Internal(err.to_string())
}
