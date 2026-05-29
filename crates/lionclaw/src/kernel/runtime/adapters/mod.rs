mod codex;
mod mock;
mod opencode;
mod state_file;

pub use codex::{CodexRuntimeAdapter, CodexRuntimeConfig};
pub use mock::MockRuntimeAdapter;
pub use opencode::{OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig};

#[derive(Debug, Default)]
struct TerminalTranscriptTarget {
    id: Option<String>,
    exported: bool,
    resumable: bool,
}

impl TerminalTranscriptTarget {
    fn is_empty(&self) -> bool {
        self.id.is_none()
    }

    fn choose_if_empty(&mut self, id: &str) -> bool {
        if self.id.is_some() {
            return false;
        }
        self.id = Some(id.to_string());
        true
    }

    fn record_export(&mut self, id: &str, resumable: bool) {
        if self.id.as_deref() == Some(id) {
            self.exported = true;
            self.resumable = resumable;
        }
    }

    fn resumable(&self) -> bool {
        self.exported && self.resumable
    }
}
