use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Default, Clone, Copy)]
pub struct AppendStatsSnapshot {
    pub append_seq: u64,
    pub last_append_ms: u64,
    pub last_append_rows: u32,
    pub pending_rows_before_append: u32,
    pub last_append_finished_ts_ms: u64,
}

#[derive(Debug, Default)]
pub struct Diagnostics {
    append_stats: Mutex<HashMap<&'static str, AppendStatsSnapshot>>,
}

impl Diagnostics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_append(&self, service: &'static str, stats: AppendStatsSnapshot) {
        if let Ok(mut guard) = self.append_stats.lock() {
            guard.insert(service, stats);
        }
    }

    pub fn append_snapshot(&self, service: &'static str) -> AppendStatsSnapshot {
        self.append_stats
            .lock()
            .ok()
            .and_then(|guard| guard.get(service).copied())
            .unwrap_or_default()
    }
}
