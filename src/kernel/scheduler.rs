use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub tick_interval: Duration,
    pub max_concurrent_runs: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(30),
            max_concurrent_runs: 1,
        }
    }
}

#[derive(Clone)]
pub struct SchedulerRuntime {
    config: SchedulerConfig,
    semaphore: Arc<Semaphore>,
}

impl SchedulerRuntime {
    pub fn new(config: SchedulerConfig) -> Self {
        let permits = config.max_concurrent_runs.max(1);
        Self {
            config,
            semaphore: Arc::new(Semaphore::new(permits)),
        }
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    pub fn semaphore(&self) -> &Arc<Semaphore> {
        &self.semaphore
    }
}
