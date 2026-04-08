use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use tokio::sync::watch;

use super::*;

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
// Helper to create a minimal AppState for tests
// ---------------------------------------------------------------------------

fn test_state() -> AppState {
    use crate::config::{Config, LogFormat};
    use crate::container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig};
    use crate::extensions::ExtensionRegistry;
    use crate::function::FunctionsConfig;
    use crate::metrics::MetricsCollector;
    use crate::runtime::RuntimeBridge;
    use std::collections::HashMap;

    let config = Config {
        host: "127.0.0.1".parse().unwrap(),
        port: 9600,
        runtime_port: 9601,
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        functions_file: "./functions.json".into(),
        log_level: "info".into(),
        shutdown_timeout: 30,
        container_idle_timeout: 300,
        max_containers: 20,
        docker_network: "localfunctions".into(),
        max_body_size: 6 * 1024 * 1024,
        log_format: LogFormat::Text,
        pull_images: false,
        init_timeout: 10,
        container_acquire_timeout: 10,
        forward_aws_credentials: true,
        mount_aws_credentials: false,
        max_async_body_size: 256 * 1024,
        hot_reload: false,
        hot_reload_debounce_ms: 500,
        domain: None,
        callback_url: "http://0.0.0.0:9600".to_string(),
    };
    let docker = bollard::Docker::connect_with_local_defaults().unwrap();
    let functions = FunctionsConfig {
        functions: HashMap::new(),
        runtime_images: HashMap::new(),
        event_source_mappings: Vec::new(),
        sns_subscriptions: Vec::new(),
    };
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let extension_registry = Arc::new(ExtensionRegistry::new(shutdown_rx.clone()));
    let runtime_bridge = Arc::new(RuntimeBridge::new(
        HashMap::new(),
        HashMap::new(),
        shutdown_rx,
    ));
    let container_registry = Arc::new(ContainerRegistry::new(docker.clone()));
    let container_manager = Arc::new(ContainerManager::new(
        docker.clone(),
        HashMap::new(),
        "localfunctions".into(),
        9601,
        "us-east-1".into(),
        container_registry.clone(),
        20,
        CredentialForwardingConfig::default(),
    ));
    AppState {
        config: Arc::new(config),
        container_registry,
        container_manager,
        docker,
        functions: Arc::new(functions),
        shutting_down: Arc::new(AtomicBool::new(false)),
        runtime_bridge,
        metrics: Arc::new(MetricsCollector::new()),
        extension_registry,
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
        endpoint_url: "http://localhost:9600/sns/my-func".into(),
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
        endpoint_url: "http://localhost/sns/func-a".into(),
    });
    mgr.add_sns_handle(SnsSubscriptionHandle {
        function_name: "func-b".into(),
        topic_arn: "arn:aws:sns:us-east-1:000000000000:topic-b".into(),
        endpoint_url: "http://localhost/sns/func-b".into(),
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
