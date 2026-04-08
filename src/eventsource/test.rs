use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use tokio::sync::watch;

use super::*;
use super::test_helpers::test_state;

// ---------------------------------------------------------------------------
// Test poller implementation
// ---------------------------------------------------------------------------

/// A minimal poller that increments a counter each poll cycle and exits on
/// shutdown.
struct TestPoller {
    name: String,
    started: Arc<AtomicBool>,
    poll_count: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl EventSourcePoller for TestPoller {
    fn description(&self) -> String {
        self.name.clone()
    }

    async fn run(&self, _state: AppState, mut shutdown_rx: watch::Receiver<bool>) {
        self.started.store(true, Ordering::SeqCst);
        loop {
            self.poll_count.fetch_add(1, Ordering::SeqCst);
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn manager_new_starts_empty() {
    let mgr = EventSourceManager::new();
    assert_eq!(mgr.poller_count(), 0);
    assert_eq!(mgr.sns_handle_count(), 0);
}

#[test]
fn manager_add_sns_handle() {
    let mut mgr = EventSourceManager::new();
    mgr.add_sns_handle(SnsSubscriptionHandle {
        function_name: "my-func".into(),
        topic_arn: "arn:aws:sns:us-east-1:000000000000:my-topic".into(),
        subscription_arn: None,
        sns_endpoint_override: None,
        region: "us-east-1".into(),
    });
    assert_eq!(mgr.sns_handle_count(), 1);
}

#[tokio::test]
async fn manager_starts_and_stops_pollers() {
    let state = test_state();
    let started = Arc::new(AtomicBool::new(false));
    let poll_count = Arc::new(AtomicU32::new(0));

    let poller = TestPoller {
        name: "test-poller".into(),
        started: started.clone(),
        poll_count: poll_count.clone(),
    };

    let mut mgr = EventSourceManager::new();
    mgr.add_poller(Box::new(poller));
    mgr.start(state);

    assert_eq!(mgr.poller_count(), 1);

    // Give the poller time to start and run a few cycles.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(started.load(Ordering::SeqCst));
    assert!(poll_count.load(Ordering::SeqCst) > 0);

    // Shutdown should complete without hanging.
    mgr.shutdown().await;
    assert_eq!(mgr.poller_count(), 0);
}

#[tokio::test]
async fn manager_multiple_pollers() {
    let state = test_state();
    let started_a = Arc::new(AtomicBool::new(false));
    let started_b = Arc::new(AtomicBool::new(false));

    let poller_a = TestPoller {
        name: "poller-a".into(),
        started: started_a.clone(),
        poll_count: Arc::new(AtomicU32::new(0)),
    };
    let poller_b = TestPoller {
        name: "poller-b".into(),
        started: started_b.clone(),
        poll_count: Arc::new(AtomicU32::new(0)),
    };

    let mut mgr = EventSourceManager::new();
    mgr.add_poller(Box::new(poller_a));
    mgr.add_poller(Box::new(poller_b));
    mgr.start(state);

    assert_eq!(mgr.poller_count(), 2);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(started_a.load(Ordering::SeqCst));
    assert!(started_b.load(Ordering::SeqCst));

    mgr.shutdown().await;
}

#[tokio::test]
async fn manager_shutdown_clears_sns_handles() {
    let mut mgr = EventSourceManager::new();
    mgr.add_sns_handle(SnsSubscriptionHandle {
        function_name: "func-a".into(),
        topic_arn: "arn:aws:sns:us-east-1:000000000000:topic-a".into(),
        subscription_arn: None,
        sns_endpoint_override: None,
        region: "us-east-1".into(),
    });
    mgr.add_sns_handle(SnsSubscriptionHandle {
        function_name: "func-b".into(),
        topic_arn: "arn:aws:sns:us-east-1:000000000000:topic-b".into(),
        subscription_arn: None,
        sns_endpoint_override: None,
        region: "us-east-1".into(),
    });
    assert_eq!(mgr.sns_handle_count(), 2);

    mgr.shutdown().await;
    assert_eq!(mgr.sns_handle_count(), 0);
}

#[tokio::test]
async fn manager_start_with_no_pollers() {
    let state = test_state();
    let mut mgr = EventSourceManager::new();
    mgr.start(state);
    assert_eq!(mgr.poller_count(), 0);
    mgr.shutdown().await;
}

#[test]
fn poller_description() {
    let poller = TestPoller {
        name: "sqs-poller-orders".into(),
        started: Arc::new(AtomicBool::new(false)),
        poll_count: Arc::new(AtomicU32::new(0)),
    };
    assert_eq!(poller.description(), "sqs-poller-orders");
}
