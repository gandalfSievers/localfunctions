use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;

/// Per-function invocation metrics.
#[derive(Debug, Clone, Serialize)]
pub struct FunctionMetrics {
    pub invocation_count: u64,
    pub error_count: u64,
    pub cold_start_count: u64,
    pub avg_duration_ms: f64,
    pub p50_duration_ms: f64,
    pub p95_duration_ms: f64,
    pub p99_duration_ms: f64,
}

/// Internal mutable state for a single function.
#[derive(Debug)]
struct FunctionMetricsState {
    invocation_count: u64,
    error_count: u64,
    cold_start_count: u64,
    /// All recorded durations, kept for percentile calculation.
    durations: Vec<Duration>,
}

impl FunctionMetricsState {
    fn new() -> Self {
        Self {
            invocation_count: 0,
            error_count: 0,
            cold_start_count: 0,
            durations: Vec::new(),
        }
    }

    fn snapshot(&self) -> FunctionMetrics {
        let avg = if self.durations.is_empty() {
            0.0
        } else {
            let sum: f64 = self.durations.iter().map(|d| d.as_secs_f64() * 1000.0).sum();
            sum / self.durations.len() as f64
        };

        let mut sorted: Vec<f64> = self
            .durations
            .iter()
            .map(|d| d.as_secs_f64() * 1000.0)
            .collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        FunctionMetrics {
            invocation_count: self.invocation_count,
            error_count: self.error_count,
            cold_start_count: self.cold_start_count,
            avg_duration_ms: avg,
            p50_duration_ms: percentile(&sorted, 50.0),
            p95_duration_ms: percentile(&sorted, 95.0),
            p99_duration_ms: percentile(&sorted, 99.0),
        }
    }
}

/// Compute a percentile from a sorted slice using nearest-rank method.
fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

/// Thread-safe metrics collector for all functions.
///
/// Uses a single `Mutex<HashMap>` which is sufficient for a local development
/// tool — contention is negligible at local invocation rates.
#[derive(Debug)]
pub struct MetricsCollector {
    state: Mutex<HashMap<String, FunctionMetricsState>>,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Record a completed invocation.
    pub fn record_invocation(
        &self,
        function_name: &str,
        duration: Duration,
        is_error: bool,
        is_cold_start: bool,
    ) {
        let mut state = self.state.lock().unwrap();
        let entry = state
            .entry(function_name.to_string())
            .or_insert_with(FunctionMetricsState::new);

        entry.invocation_count += 1;
        entry.durations.push(duration);
        if is_error {
            entry.error_count += 1;
        }
        if is_cold_start {
            entry.cold_start_count += 1;
        }
    }

    /// Return a snapshot of all function metrics.
    pub fn snapshot(&self) -> HashMap<String, FunctionMetrics> {
        let state = self.state.lock().unwrap();
        state.iter().map(|(k, v)| (k.clone(), v.snapshot())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_metrics_snapshot() {
        let collector = MetricsCollector::new();
        let snap = collector.snapshot();
        assert!(snap.is_empty());
    }

    #[test]
    fn record_single_invocation() {
        let collector = MetricsCollector::new();
        collector.record_invocation("my-func", Duration::from_millis(100), false, true);

        let snap = collector.snapshot();
        let m = snap.get("my-func").unwrap();
        assert_eq!(m.invocation_count, 1);
        assert_eq!(m.error_count, 0);
        assert_eq!(m.cold_start_count, 1);
        assert!((m.avg_duration_ms - 100.0).abs() < 1.0);
        assert!((m.p50_duration_ms - 100.0).abs() < 1.0);
        assert!((m.p95_duration_ms - 100.0).abs() < 1.0);
        assert!((m.p99_duration_ms - 100.0).abs() < 1.0);
    }

    #[test]
    fn record_error_invocation() {
        let collector = MetricsCollector::new();
        collector.record_invocation("err-func", Duration::from_millis(50), true, false);

        let snap = collector.snapshot();
        let m = snap.get("err-func").unwrap();
        assert_eq!(m.invocation_count, 1);
        assert_eq!(m.error_count, 1);
        assert_eq!(m.cold_start_count, 0);
    }

    #[test]
    fn percentile_calculations() {
        let collector = MetricsCollector::new();
        // Record 100 invocations with durations 1..=100 ms
        for i in 1..=100 {
            collector.record_invocation(
                "perc-func",
                Duration::from_millis(i),
                false,
                false,
            );
        }

        let snap = collector.snapshot();
        let m = snap.get("perc-func").unwrap();
        assert_eq!(m.invocation_count, 100);

        // avg should be ~50.5
        assert!((m.avg_duration_ms - 50.5).abs() < 1.0);

        // p50 = 50th value
        assert!((m.p50_duration_ms - 50.0).abs() < 1.0);

        // p95 = 95th value
        assert!((m.p95_duration_ms - 95.0).abs() < 1.0);

        // p99 = 99th value
        assert!((m.p99_duration_ms - 99.0).abs() < 1.0);
    }

    #[test]
    fn multiple_functions_tracked_independently() {
        let collector = MetricsCollector::new();
        collector.record_invocation("func-a", Duration::from_millis(10), false, true);
        collector.record_invocation("func-b", Duration::from_millis(20), true, false);
        collector.record_invocation("func-a", Duration::from_millis(30), false, false);

        let snap = collector.snapshot();
        assert_eq!(snap.len(), 2);

        let a = snap.get("func-a").unwrap();
        assert_eq!(a.invocation_count, 2);
        assert_eq!(a.error_count, 0);
        assert_eq!(a.cold_start_count, 1);

        let b = snap.get("func-b").unwrap();
        assert_eq!(b.invocation_count, 1);
        assert_eq!(b.error_count, 1);
        assert_eq!(b.cold_start_count, 0);
    }

    #[test]
    fn percentile_of_empty_returns_zero() {
        assert_eq!(percentile(&[], 50.0), 0.0);
    }

    #[test]
    fn percentile_of_single_value() {
        assert_eq!(percentile(&[42.0], 50.0), 42.0);
        assert_eq!(percentile(&[42.0], 99.0), 42.0);
    }

    #[test]
    fn concurrent_recording() {
        use std::sync::Arc;
        let collector = Arc::new(MetricsCollector::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let c = collector.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..100 {
                    c.record_invocation(
                        "conc-func",
                        Duration::from_millis(i * 100 + j),
                        j % 10 == 0,
                        j == 0,
                    );
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let snap = collector.snapshot();
        let m = snap.get("conc-func").unwrap();
        assert_eq!(m.invocation_count, 1000);
        // Each thread has j==0 once → 10 cold starts
        assert_eq!(m.cold_start_count, 10);
        // Each thread has j % 10 == 0 ten times → 100 errors
        assert_eq!(m.error_count, 100);
    }
}
