use serde::Serialize;
use std::time::{Duration, Instant};

pub(crate) const SCAN_INTERVAL: Duration = Duration::from_secs(10);
pub(crate) const POSITION_INTERVAL: Duration = Duration::from_secs(5);

/// Process-local observations. Wall-clock timestamps label cycles; durations use Instant.
#[derive(Clone, Default, Serialize)]
pub struct RuntimeMetrics {
    pub scan: CycleMetrics,
    pub position_management: CycleMetrics,
}

#[derive(Clone, Default, Serialize)]
pub struct CycleMetrics {
    pub running: bool,
    pub runs: u64,
    pub failed_runs: u64,
    pub slow_runs: u64,
    pub last_started_at: Option<i64>,
    pub last_completed_at: Option<i64>,
    pub last_duration_ms: Option<u64>,
    pub max_duration_ms: u64,
    pub last_start_interval_ms: Option<u64>,
    #[serde(skip)]
    previous_start: Option<Instant>,
}

fn milliseconds(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

impl CycleMetrics {
    pub(crate) fn start(&mut self, at: i64, now: Instant) {
        self.last_start_interval_ms = self
            .previous_start
            .map(|previous| milliseconds(now.saturating_duration_since(previous)));
        self.previous_start = Some(now);
        self.last_started_at = Some(at);
        self.running = true;
    }

    pub(crate) fn finish(&mut self, at: i64, elapsed: Duration, target: Duration, failed: bool) {
        self.running = false;
        self.runs += 1;
        self.failed_runs += u64::from(failed);
        self.slow_runs += u64::from(elapsed > target);
        self.last_completed_at = Some(at);
        let elapsed_ms = milliseconds(elapsed);
        self.last_duration_ms = Some(elapsed_ms);
        self.max_duration_ms = self.max_duration_ms.max(elapsed_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycles_track_in_progress_failures_overruns_and_actual_intervals() {
        let mut metrics = CycleMetrics::default();
        let start = Instant::now();
        assert!(metrics.last_started_at.is_none());
        metrics.start(1000, start);
        assert!(metrics.running);
        assert!(metrics.last_start_interval_ms.is_none());
        metrics.finish(7000, Duration::from_secs(6), POSITION_INTERVAL, true);
        assert!(!metrics.running);
        assert_eq!(metrics.runs, 1);
        assert_eq!(metrics.failed_runs, 1);
        assert_eq!(metrics.slow_runs, 1);
        assert_eq!(metrics.last_duration_ms, Some(6000));
        metrics.start(11_000, start + Duration::from_secs(10));
        assert_eq!(metrics.last_start_interval_ms, Some(10_000));
        metrics.finish(12_000, Duration::from_secs(1), POSITION_INTERVAL, false);
        assert_eq!(metrics.runs, 2);
        assert_eq!(metrics.failed_runs, 1);
        assert_eq!(metrics.slow_runs, 1);
        assert_eq!(metrics.last_started_at, Some(11_000));
        assert_eq!(metrics.last_completed_at, Some(12_000));
        assert_eq!(metrics.last_duration_ms, Some(1000));
        assert_eq!(metrics.max_duration_ms, 6000);
        let serialized = serde_json::to_value(metrics).unwrap();
        assert!(serialized.get("previous_start").is_none());
    }
}
