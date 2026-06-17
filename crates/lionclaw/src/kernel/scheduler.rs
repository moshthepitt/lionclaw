use std::{future::Future, sync::Arc, time::Duration};

use anyhow::anyhow;
use chrono::Utc;
use serde_json::json;
use tokio::{sync::oneshot, time::sleep};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::contracts::{JobTickResponse, SessionHistoryPolicy, SessionOpenRequest, TrustTier};

use super::{
    channel_outbox::ChannelDeliveryRoute,
    core::{Kernel, SchedulerLeaseReconciliation},
    error::KernelError,
    jobs::{
        ClaimedSchedulerJob, SchedulerJobDeliveryStatus, SchedulerJobRecord, SchedulerJobRunRecord,
        SchedulerJobRunStatus, SchedulerJobTriggerKind,
    },
    runtime::ExecutionPlanPurpose,
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
        let tick = Box::pin(self.with_scheduler_lease(
            kernel,
            SchedulerLeaseMode::Tick,
            |lease_owner| async move {
                if kernel
                    .reconcile_stale_scheduler_runs_after_scheduler_lease(&lease_owner)
                    .await?
                    == SchedulerLeaseReconciliation::LeaseLost
                {
                    return Ok(JobTickResponse { claimed_runs: 0 });
                }
                kernel.recover_pending_scheduler_deliveries().await?;
                let mut claimed_runs = 0usize;
                while let Some(claimed_job) = self.claim_next_due_job(kernel, &lease_owner).await? {
                    claimed_runs += 1;
                    self.run_claimed_job(kernel, claimed_job).await?;
                }

                Ok(JobTickResponse { claimed_runs })
            },
        ))
        .await?;

        Ok(tick.unwrap_or(JobTickResponse { claimed_runs: 0 }))
    }

    pub async fn run_manual_job(
        &self,
        kernel: &Kernel,
        job_id: Uuid,
        job_already_running: bool,
    ) -> Result<(SchedulerJobRecord, SchedulerJobRunRecord), KernelError> {
        Box::pin(self.with_scheduler_lease(
            kernel,
            SchedulerLeaseMode::Execution,
            |lease_owner| async move {
                if kernel
                    .reconcile_stale_scheduler_runs_after_scheduler_lease(&lease_owner)
                    .await?
                    == SchedulerLeaseReconciliation::LeaseLost
                {
                    return Err(KernelError::Conflict(
                        "scheduler lease lost before manual run".to_string(),
                    ));
                }
                let claimed = kernel
                    .job_store()
                    .claim_manual_run_with_scheduler_lease(job_id, Utc::now(), &lease_owner)
                    .await
                    .map_err(internal)?
                    .ok_or_else(|| {
                        if job_already_running {
                            KernelError::Conflict("job is already running".to_string())
                        } else {
                            KernelError::Conflict(
                                "job could not be claimed for manual run".to_string(),
                            )
                        }
                    })?;
                self.run_claimed_job(kernel, claimed).await
            },
        ))
        .await?
        .ok_or_else(|| KernelError::Conflict("scheduler is already running".to_string()))
    }

    pub async fn run_loop(&self, kernel: Arc<Kernel>) {
        loop {
            if let Err(err) = Box::pin(self.tick(&kernel)).await {
                if let Err(audit_err) = kernel
                    .audit_log()
                    .append(
                        "scheduler.tick.failed",
                        None,
                        Some("kernel".to_string()),
                        json!({"error": err.to_string()}),
                    )
                    .await
                {
                    warn!(
                        ?audit_err,
                        ?err,
                        "failed to append scheduler tick failure audit event"
                    );
                }
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
            if let Err(err) = kernel
                .validate_runtime_launch_prerequisites_for_purpose(
                    &job.runtime_id,
                    ExecutionPlanPurpose::Interactive,
                )
                .await
            {
                match self
                    .handle_failed_attempt(
                        kernel,
                        &job,
                        &current_run,
                        AttemptFailureContext {
                            session_id: None,
                            turn_id: None,
                            failure_phase: Some("preflight"),
                        },
                        &err,
                    )
                    .await?
                {
                    AttemptOutcome::Retry(next_run) => {
                        current_run = next_run;
                        continue;
                    }
                    AttemptOutcome::Finished(result) => return Ok(*result),
                }
            }

            let attempt_result = self.run_job_attempt(kernel, &job, &current_run).await;
            match attempt_result {
                Ok(AttemptOutcome::Retry(next_run)) => {
                    current_run = next_run;
                }
                Ok(AttemptOutcome::Finished(result)) => return Ok(*result),
                Err(err) => {
                    if let Err(interrupt_err) = kernel
                        .job_store()
                        .interrupt_run(
                            current_run.run_id,
                            &format!("scheduled job execution failed unexpectedly: {err}"),
                        )
                        .await
                    {
                        warn!(?interrupt_err, run_id = %current_run.run_id, "failed to interrupt failed scheduled job run");
                    }
                    if let Err(audit_err) = kernel
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
                        .await
                    {
                        warn!(?audit_err, run_id = %current_run.run_id, "failed to append scheduled job execution failure audit event");
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn claim_next_due_job(
        &self,
        kernel: &Kernel,
        lease_owner: &str,
    ) -> Result<Option<ClaimedSchedulerJob>, KernelError> {
        let Some(claimed_job) = kernel
            .job_store()
            .claim_due_jobs_with_scheduler_lease(
                Utc::now(),
                1,
                SchedulerJobTriggerKind::Schedule,
                lease_owner,
            )
            .await
            .map_err(internal)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        if let Err(err) = kernel
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
        {
            warn!(?err, job_id = %claimed_job.job.job_id, run_id = %claimed_job.run.run_id, "failed to append scheduled job claim audit event");
        }
        Ok(Some(claimed_job))
    }

    async fn deliver_job_result(
        &self,
        kernel: &Kernel,
        job: &SchedulerJobRecord,
        run_id: Uuid,
        session_id: Option<Uuid>,
        turn_id: Option<Uuid>,
        content: &str,
    ) -> SchedulerJobDeliveryStatus {
        let Some(delivery) = &job.delivery else {
            return SchedulerJobDeliveryStatus::NotRequested;
        };
        match kernel
            .enqueue_scheduler_run_delivery(
                job,
                run_id,
                session_id,
                turn_id,
                ChannelDeliveryRoute {
                    channel_id: &delivery.channel_id,
                    conversation_ref: &delivery.conversation_ref,
                    thread_ref: delivery.thread_ref.as_deref(),
                    reply_to_ref: delivery.reply_to_ref.as_deref(),
                },
                content,
            )
            .await
        {
            Ok(_) => SchedulerJobDeliveryStatus::Pending,
            Err(_) => SchedulerJobDeliveryStatus::Failed,
        }
    }

    fn initial_delivery_status(job: &SchedulerJobRecord) -> SchedulerJobDeliveryStatus {
        if job.delivery.is_some() {
            SchedulerJobDeliveryStatus::Pending
        } else {
            SchedulerJobDeliveryStatus::NotRequested
        }
    }

    async fn deliver_completed_job_result(
        &self,
        kernel: &Kernel,
        job: &SchedulerJobRecord,
        run_id: Uuid,
        session_id: Option<Uuid>,
        turn_id: Option<Uuid>,
        content: &str,
    ) -> Result<SchedulerJobDeliveryStatus, KernelError> {
        let delivery_status = self
            .deliver_job_result(kernel, job, run_id, session_id, turn_id, content)
            .await;
        if delivery_status == SchedulerJobDeliveryStatus::Failed {
            let updated = kernel
                .job_store()
                .update_run_delivery_status(run_id, delivery_status)
                .await
                .map_err(internal)?;
            if !updated {
                warn!(%run_id, "failed to update completed scheduler run delivery status");
            }
        }
        Ok(delivery_status)
    }

    async fn finish_current_run_snapshot(
        &self,
        kernel: &Kernel,
        job_id: Uuid,
        final_run: SchedulerJobRunRecord,
    ) -> Result<Option<AttemptOutcome>, KernelError> {
        let Some(updated_job) = kernel.job_store().get_job(job_id).await.map_err(internal)? else {
            return Ok(None);
        };
        if final_run.status == SchedulerJobRunStatus::Running
            && updated_job.running_run_id == Some(final_run.run_id)
        {
            return Ok(None);
        }

        Ok(Some(AttemptOutcome::Finished(Box::new((
            updated_job,
            final_run,
        )))))
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
            .execute_scheduled_job_turn(current_run.run_id, opened.session_id, turn_id, job)
            .await;

        match turn_result {
            Ok(response) => {
                let initial_delivery_status = Self::initial_delivery_status(job);
                let updated_job = kernel
                    .job_store()
                    .complete_run_success(
                        current_run.run_id,
                        response.session_id,
                        response.turn_id,
                        initial_delivery_status,
                    )
                    .await
                    .map_err(internal)?
                    .ok_or_else(|| {
                        KernelError::NotFound(
                            "scheduled job disappeared during completion".to_string(),
                        )
                    })?;
                let delivery_status = self
                    .deliver_completed_job_result(
                        kernel,
                        job,
                        current_run.run_id,
                        Some(response.session_id),
                        Some(response.turn_id),
                        &response.assistant_text,
                    )
                    .await?;
                let final_run = kernel
                    .job_store()
                    .get_run(current_run.run_id)
                    .await
                    .map_err(internal)?
                    .ok_or_else(|| {
                        KernelError::NotFound("scheduled job run disappeared".to_string())
                    })?;
                if let Err(err) = kernel
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
                {
                    warn!(?err, job_id = %job.job_id, run_id = %final_run.run_id, "failed to append scheduled job completion audit event");
                }
                if let Err(err) = kernel
                    .record_scheduler_continuity_success(job, &final_run, &response.assistant_text)
                    .await
                {
                    warn!(?err, job_id = %job.job_id, run_id = %final_run.run_id, "failed to record scheduled job success continuity");
                }
                Ok(AttemptOutcome::Finished(Box::new((updated_job, final_run))))
            }
            Err(err) => {
                let run_still_owns_turn = kernel
                    .job_store()
                    .running_run_owns_turn(current_run.run_id, opened.session_id, turn_id)
                    .await
                    .map_err(internal)?;
                let (session_id, turn_id) = if run_still_owns_turn {
                    (Some(opened.session_id), Some(turn_id))
                } else {
                    let maybe_run = kernel
                        .job_store()
                        .get_run(current_run.run_id)
                        .await
                        .map_err(internal)?;
                    match maybe_run {
                        Some(run)
                            if run.status != SchedulerJobRunStatus::Running
                                || run.session_id.is_some()
                                || run.turn_id.is_some() =>
                        {
                            if let Some(outcome) = self
                                .finish_current_run_snapshot(kernel, job.job_id, run)
                                .await?
                            {
                                return Ok(outcome);
                            }
                            return Err(KernelError::Conflict(
                                "scheduled job run attachment no longer matches current turn"
                                    .to_string(),
                            ));
                        }
                        Some(_) => (None, None),
                        None => (Some(opened.session_id), Some(turn_id)),
                    }
                };
                self.handle_failed_attempt(
                    kernel,
                    job,
                    current_run,
                    AttemptFailureContext {
                        session_id,
                        turn_id,
                        failure_phase: None,
                    },
                    &err,
                )
                .await
            }
        }
    }

    async fn handle_failed_attempt(
        &self,
        kernel: &Kernel,
        job: &SchedulerJobRecord,
        current_run: &SchedulerJobRunRecord,
        context: AttemptFailureContext,
        err: &KernelError,
    ) -> Result<AttemptOutcome, KernelError> {
        if current_run.attempt_no <= job.retry_attempts {
            let next_run = kernel
                .job_store()
                .begin_retry_run(current_run.run_id, Utc::now())
                .await
                .map_err(internal)?
                .ok_or_else(|| {
                    KernelError::Conflict("scheduled retry could not be started".to_string())
                })?;
            if let Err(err) = kernel
                .audit_log()
                .append(
                    "job.run.retry",
                    context.session_id,
                    Some("scheduler".to_string()),
                    json!({
                        "job_id": job.job_id,
                        "run_id": next_run.run_id,
                        "attempt_no": next_run.attempt_no,
                    }),
                )
                .await
            {
                warn!(?err, job_id = %job.job_id, run_id = %next_run.run_id, "failed to append scheduled job retry audit event");
            }
            return Ok(AttemptOutcome::Retry(next_run));
        }

        let error_text = err.to_string();
        let failure_summary = format!("Scheduled job '{}' failed: {}", job.name, error_text);
        let initial_delivery_status = Self::initial_delivery_status(job);
        let updated_job = kernel
            .job_store()
            .complete_run_failure(
                current_run.run_id,
                context.session_id,
                context.turn_id,
                &error_text,
                SchedulerJobRunStatus::DeadLetter,
                initial_delivery_status,
            )
            .await
            .map_err(internal)?
            .ok_or_else(|| {
                KernelError::NotFound(
                    "scheduled job disappeared during failure completion".to_string(),
                )
            })?;
        let delivery_status = self
            .deliver_completed_job_result(
                kernel,
                job,
                current_run.run_id,
                context.session_id,
                context.turn_id,
                &failure_summary,
            )
            .await?;
        let final_run = kernel
            .job_store()
            .get_run(current_run.run_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| KernelError::NotFound("scheduled job run disappeared".to_string()))?;
        let mut audit_details = json!({
            "job_id": job.job_id,
            "run_id": final_run.run_id,
            "error": error_text,
            "delivery_status": delivery_status.as_str(),
        });
        if let Some(failure_phase) = context.failure_phase {
            if let Some(details) = audit_details.as_object_mut() {
                details.insert("failure_phase".to_string(), json!(failure_phase));
            }
        }
        if let Err(err) = kernel
            .audit_log()
            .append(
                "job.run.failed",
                context.session_id,
                Some("scheduler".to_string()),
                audit_details,
            )
            .await
        {
            warn!(?err, job_id = %job.job_id, run_id = %final_run.run_id, "failed to append scheduled job failure audit event");
        }
        if let Err(err) = kernel
            .record_scheduler_continuity_failure(job, &final_run, &error_text)
            .await
        {
            warn!(?err, job_id = %job.job_id, run_id = %final_run.run_id, "failed to record scheduled job failure continuity");
        }
        Ok(AttemptOutcome::Finished(Box::new((updated_job, final_run))))
    }

    async fn with_scheduler_lease<F, Fut, T>(
        &self,
        kernel: &Kernel,
        mode: SchedulerLeaseMode,
        work: F,
    ) -> Result<Option<T>, KernelError>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = Result<T, KernelError>>,
    {
        let owner = format!("{}:{}", mode.owner_prefix(), Uuid::new_v4());
        let lease_ttl = self.config.tick_interval.max(Duration::from_secs(60));
        let acquired = match mode {
            SchedulerLeaseMode::Tick => {
                kernel
                    .job_store()
                    .try_acquire_tick_lease(&owner, lease_ttl)
                    .await
            }
            SchedulerLeaseMode::Execution => {
                kernel
                    .job_store()
                    .try_acquire_execution_lease(&owner, lease_ttl)
                    .await
            }
        }
        .map_err(internal)?;
        if !acquired {
            return Ok(None);
        }

        let renewal_handle = self.spawn_lease_renewal(kernel, owner.clone(), lease_ttl, mode);
        let work_result = work(owner.clone()).await;
        let renewal_result = renewal_handle.stop().await;
        let release_result = match mode {
            SchedulerLeaseMode::Tick => kernel.job_store().release_tick_lease(&owner).await,
            SchedulerLeaseMode::Execution => {
                kernel.job_store().release_execution_lease(&owner).await
            }
        };
        release_result.map_err(internal)?;
        renewal_result?;
        work_result.map(Some)
    }

    fn spawn_lease_renewal(
        &self,
        kernel: &Kernel,
        owner: String,
        lease_ttl: Duration,
        mode: SchedulerLeaseMode,
    ) -> SchedulerLeaseRenewal {
        let jobs = kernel.job_store().clone();
        let renew_every = (lease_ttl / 2).max(Duration::from_secs(1));
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return Ok(()),
                    _ = sleep(renew_every) => {
                        let renewed = match mode {
                            SchedulerLeaseMode::Tick => jobs.renew_tick_lease(&owner, lease_ttl).await,
                            SchedulerLeaseMode::Execution => jobs.renew_execution_lease(&owner, lease_ttl).await,
                        }?;
                        if !renewed {
                            return Err(anyhow!(mode.lost_message()));
                        }
                    }
                }
            }
        });
        SchedulerLeaseRenewal {
            stop_tx: Some(stop_tx),
            handle,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SchedulerLeaseMode {
    Tick,
    Execution,
}

impl SchedulerLeaseMode {
    fn owner_prefix(self) -> &'static str {
        match self {
            Self::Tick => "scheduler",
            Self::Execution => "manual",
        }
    }

    fn lost_message(self) -> &'static str {
        match self {
            Self::Tick => "scheduler tick lease lost during execution",
            Self::Execution => "scheduler execution lease lost during execution",
        }
    }
}

struct SchedulerLeaseRenewal {
    stop_tx: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

enum AttemptOutcome {
    Retry(SchedulerJobRunRecord),
    Finished(Box<(SchedulerJobRecord, SchedulerJobRunRecord)>),
}

struct AttemptFailureContext {
    session_id: Option<Uuid>,
    turn_id: Option<Uuid>,
    failure_phase: Option<&'static str>,
}

impl SchedulerLeaseRenewal {
    async fn stop(mut self) -> Result<(), KernelError> {
        if let Some(stop_tx) = self.stop_tx.take() {
            if stop_tx.send(()).is_err() {
                debug!("scheduler lease renewal task already stopped");
            }
        }
        let renewal_result = self.handle.await.map_err(|err| internal(err.into()))?;
        renewal_result.map_err(internal)?;
        Ok(())
    }
}

fn internal(err: anyhow::Error) -> KernelError {
    KernelError::Internal(format!("{err:#}"))
}
