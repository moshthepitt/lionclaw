use lionclaw_model::{AppliedRuntimeConfiguration, RuntimeUsage};

/// The complete generic result of one role turn.
///
/// Runtime permissions are deliberately absent: confinement and native
/// session support are declared by the selected profile before launch.
#[derive(Debug, Clone, Default)]
pub struct TurnResult {
    pub configuration: AppliedRuntimeConfiguration,
    pub runtime_usage: RuntimeUsage,
    pub final_response: String,
}

impl TurnResult {
    pub fn projected(mut self) -> Self {
        self.configuration = self.configuration.projected();
        self.runtime_usage = self.runtime_usage.projected();
        self.final_response = crate::bounded_text(&self.final_response);
        self
    }
}
