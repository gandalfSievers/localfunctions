use std::collections::HashMap;
use std::time::Duration;

use bollard::container::{RemoveContainerOptions, StopContainerOptions};
use bollard::Docker;
use bollard::network::{CreateNetworkOptions, InspectNetworkOptions};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::types::{FunctionConfig, ServiceError};

/// Manages the Docker network used for communication between Lambda containers
/// and the Runtime API.
#[allow(dead_code)]
pub struct DockerNetwork {
    docker: Docker,
    network_name: String,
}

#[allow(dead_code)]
impl DockerNetwork {
    /// Create a new DockerNetwork manager. Does not create the network yet;
    /// call [`ensure_created`] to create it.
    pub fn new(docker: Docker, network_name: String) -> Self {
        Self {
            docker,
            network_name,
        }
    }

    /// Ensure the Docker network exists. Creates it if missing, reuses if
    /// already present.
    pub async fn ensure_created(&self) -> Result<(), ServiceError> {
        match self
            .docker
            .inspect_network(
                &self.network_name,
                None::<InspectNetworkOptions<String>>,
            )
            .await
        {
            Ok(_) => {
                info!(network = %self.network_name, "Docker network already exists, reusing");
                Ok(())
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                debug!(network = %self.network_name, "Creating Docker network");
                self.docker
                    .create_network(CreateNetworkOptions {
                        name: self.network_name.clone(),
                        driver: "bridge".to_string(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| {
                        ServiceError::ServiceException(format!(
                            "Failed to create Docker network '{}': {}",
                            self.network_name, e
                        ))
                    })?;
                info!(network = %self.network_name, "Docker network created");
                Ok(())
            }
            Err(e) => Err(ServiceError::ServiceException(format!(
                "Failed to inspect Docker network '{}': {}",
                self.network_name, e
            ))),
        }
    }

    /// Remove the Docker network. Should be called after all containers are
    /// stopped.
    pub async fn remove(&self) -> Result<(), ServiceError> {
        debug!(network = %self.network_name, "Removing Docker network");
        match self.docker.remove_network(&self.network_name).await {
            Ok(_) => {
                info!(network = %self.network_name, "Docker network removed");
                Ok(())
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                warn!(network = %self.network_name, "Docker network already removed");
                Ok(())
            }
            Err(e) => Err(ServiceError::ServiceException(format!(
                "Failed to remove Docker network '{}': {}",
                self.network_name, e
            ))),
        }
    }

    /// Return the network name.
    pub fn name(&self) -> &str {
        &self.network_name
    }
}

// ---------------------------------------------------------------------------
// ContainerRegistry
// ---------------------------------------------------------------------------

/// Metadata for a tracked container.
#[derive(Debug, Clone)]
struct RegisteredContainer {
    container_id: String,
    function_name: String,
}

/// Thread-safe registry of all running function containers.
///
/// Used during shutdown to stop and remove every container that was started by
/// this process.
#[allow(dead_code)]
pub struct ContainerRegistry {
    docker: Docker,
    containers: RwLock<HashMap<String, RegisteredContainer>>,
}

#[allow(dead_code)]
impl ContainerRegistry {
    /// Create a new, empty registry.
    pub fn new(docker: Docker) -> Self {
        Self {
            docker,
            containers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a running container so it will be cleaned up on shutdown.
    pub async fn register(&self, container_id: String, function_name: String) {
        let mut map = self.containers.write().await;
        debug!(container_id = %container_id, function = %function_name, "registered container");
        map.insert(
            container_id.clone(),
            RegisteredContainer {
                container_id,
                function_name,
            },
        );
    }

    /// Remove a container from the registry (e.g. after it exits normally).
    pub async fn deregister(&self, container_id: &str) {
        let mut map = self.containers.write().await;
        if map.remove(container_id).is_some() {
            debug!(container_id = %container_id, "deregistered container");
        }
    }

    /// Return the number of tracked containers.
    pub async fn count(&self) -> usize {
        self.containers.read().await.len()
    }

    /// Stop and remove all tracked containers.
    ///
    /// Each container is given `timeout` to stop gracefully; after that Docker
    /// sends SIGKILL. Containers are stopped concurrently.
    pub async fn shutdown_all(&self, timeout: Duration) {
        let map = self.containers.write().await;
        let count = map.len();
        if count == 0 {
            info!("no containers to clean up");
            return;
        }

        info!(count, "stopping all containers");

        let timeout_secs = timeout.as_secs().try_into().unwrap_or(i64::MAX);
        let mut handles = Vec::with_capacity(count);

        for entry in map.values() {
            let docker = self.docker.clone();
            let id = entry.container_id.clone();
            let name = entry.function_name.clone();

            handles.push(tokio::spawn(async move {
                debug!(container_id = %id, function = %name, "stopping container");

                // Stop — sends SIGTERM, waits `timeout`, then SIGKILL
                if let Err(e) = docker
                    .stop_container(
                        &id,
                        Some(StopContainerOptions { t: timeout_secs }),
                    )
                    .await
                {
                    // 304 = container already stopped, 404 = already removed
                    if !is_benign_docker_error(&e) {
                        error!(container_id = %id, %e, "failed to stop container");
                    }
                }

                // Remove — force flag ensures removal even if stop failed
                if let Err(e) = docker
                    .remove_container(
                        &id,
                        Some(RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await
                {
                    if !is_benign_docker_error(&e) {
                        error!(container_id = %id, %e, "failed to remove container");
                    }
                }

                info!(container_id = %id, function = %name, "container cleaned up");
            }));
        }

        // Wait for all stop+remove tasks to finish
        for handle in handles {
            if let Err(e) = handle.await {
                error!(%e, "container cleanup task panicked");
            }
        }

        info!(count, "all containers cleaned up");
    }
}

/// Returns true for Docker errors that indicate the container is already
/// stopped or removed (304 Not Modified, 404 Not Found).
fn is_benign_docker_error(e: &bollard::errors::Error) -> bool {
    matches!(
        e,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 304 | 404,
            ..
        }
    )
}

/// Build the complete set of environment variables to inject into a Lambda
/// function container.
///
/// System variables (AWS_LAMBDA_FUNCTION_NAME, _HANDLER, etc.) are set first,
/// then user-configured variables are merged in. User variables do **not**
/// override the system variables.
pub fn lambda_env_vars(
    function: &FunctionConfig,
    runtime_port: u16,
    region: &str,
) -> Vec<String> {
    let runtime_api = runtime_api_endpoint(runtime_port);

    // System variables required by the AWS Lambda execution environment
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert(
        "AWS_LAMBDA_FUNCTION_NAME".into(),
        function.name.clone(),
    );
    vars.insert("_HANDLER".into(), function.handler.clone());
    vars.insert(
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE".into(),
        function.memory_size.to_string(),
    );
    vars.insert("AWS_LAMBDA_RUNTIME_API".into(), runtime_api);
    vars.insert("AWS_REGION".into(), region.to_string());
    vars.insert("AWS_DEFAULT_REGION".into(), region.to_string());
    vars.insert(
        "AWS_LAMBDA_FUNCTION_VERSION".into(),
        "$LATEST".into(),
    );
    vars.insert(
        "AWS_LAMBDA_LOG_GROUP_NAME".into(),
        format!("/aws/lambda/{}", function.name),
    );
    vars.insert(
        "AWS_LAMBDA_LOG_STREAM_NAME".into(),
        "localfunctions/latest".into(),
    );

    // Merge user-configured environment variables without overriding system vars
    for (key, value) in &function.environment {
        vars.entry(key.clone()).or_insert_with(|| value.clone());
    }

    // Convert to Docker's "KEY=VALUE" format
    vars.into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect()
}

/// Build the `AWS_LAMBDA_RUNTIME_API` endpoint value that containers should
/// use to reach the Runtime API.
///
/// On macOS and Windows, containers reach the host via `host.docker.internal`.
/// On Linux, the host IP on the Docker bridge network is used instead.
pub fn runtime_api_endpoint(runtime_port: u16) -> String {
    if cfg!(target_os = "macos") || cfg!(target_os = "windows") {
        format!("host.docker.internal:{}", runtime_port)
    } else {
        // On Linux the host gateway is typically 172.17.0.1, but Docker
        // Desktop for Linux also supports host.docker.internal. We use the
        // special hostname which Docker resolves automatically.
        format!("host.docker.internal:{}", runtime_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_api_endpoint_contains_port() {
        let endpoint = runtime_api_endpoint(9601);
        assert_eq!(endpoint, "host.docker.internal:9601");
    }

    #[test]
    fn runtime_api_endpoint_custom_port() {
        let endpoint = runtime_api_endpoint(3000);
        assert_eq!(endpoint, "host.docker.internal:3000");
    }

    // -- lambda_env_vars ------------------------------------------------

    fn make_test_function() -> FunctionConfig {
        FunctionConfig {
            name: "my-func".into(),
            runtime: "python3.12".into(),
            handler: "main.handler".into(),
            code_path: std::path::PathBuf::from("/tmp/code"),
            timeout: 30,
            memory_size: 256,
            environment: HashMap::new(),
            image: None,
        }
    }

    #[test]
    fn lambda_env_vars_contains_function_name() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_NAME=my-func".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_handler() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"_HANDLER=main.handler".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_memory_size() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_MEMORY_SIZE=256".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_runtime_api() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"AWS_LAMBDA_RUNTIME_API=host.docker.internal:9601".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_region() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "eu-west-1");
        assert!(vars.contains(&"AWS_REGION=eu-west-1".to_string()));
        assert!(vars.contains(&"AWS_DEFAULT_REGION=eu-west-1".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_version() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_VERSION=$LATEST".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_log_group() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"AWS_LAMBDA_LOG_GROUP_NAME=/aws/lambda/my-func".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_log_stream() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"AWS_LAMBDA_LOG_STREAM_NAME=localfunctions/latest".to_string()));
    }

    #[test]
    fn lambda_env_vars_includes_user_vars() {
        let mut func = make_test_function();
        func.environment.insert("TABLE_NAME".into(), "my-table".into());
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(&"TABLE_NAME=my-table".to_string()));
    }

    #[test]
    fn lambda_env_vars_user_vars_do_not_override_system_vars() {
        let mut func = make_test_function();
        func.environment.insert("AWS_LAMBDA_FUNCTION_NAME".into(), "hacked".into());
        func.environment.insert("_HANDLER".into(), "evil.handler".into());
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        // System values must win
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_NAME=my-func".to_string()));
        assert!(vars.contains(&"_HANDLER=main.handler".to_string()));
        assert!(!vars.iter().any(|v| v.contains("hacked")));
    }

    #[test]
    fn lambda_env_vars_custom_port() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 3000, "us-east-1");
        assert!(vars.contains(&"AWS_LAMBDA_RUNTIME_API=host.docker.internal:3000".to_string()));
    }

    #[test]
    fn lambda_env_vars_count() {
        // 9 system vars with no user vars
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert_eq!(vars.len(), 9);
    }

    #[test]
    fn docker_network_name() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let network = DockerNetwork::new(docker, "test-network".to_string());
        assert_eq!(network.name(), "test-network");
    }

    #[test]
    fn docker_network_default_name_from_config() {
        // Verifies the default network name matches what Config produces
        let docker = Docker::connect_with_local_defaults().unwrap();
        let network = DockerNetwork::new(docker, "localfunctions".to_string());
        assert_eq!(network.name(), "localfunctions");
    }

    #[tokio::test]
    async fn container_registry_starts_empty() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = ContainerRegistry::new(docker);
        assert_eq!(registry.count().await, 0);
    }

    #[tokio::test]
    async fn container_registry_register_and_deregister() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = ContainerRegistry::new(docker);

        registry
            .register("abc123".into(), "my-func".into())
            .await;
        assert_eq!(registry.count().await, 1);

        registry
            .register("def456".into(), "other-func".into())
            .await;
        assert_eq!(registry.count().await, 2);

        registry.deregister("abc123").await;
        assert_eq!(registry.count().await, 1);

        registry.deregister("def456").await;
        assert_eq!(registry.count().await, 0);
    }

    #[tokio::test]
    async fn container_registry_deregister_nonexistent_is_noop() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = ContainerRegistry::new(docker);
        registry.deregister("nonexistent").await;
        assert_eq!(registry.count().await, 0);
    }

    #[tokio::test]
    async fn container_registry_shutdown_all_empty_is_noop() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = ContainerRegistry::new(docker);
        // Should complete without error even with no containers
        registry
            .shutdown_all(Duration::from_secs(5))
            .await;
    }

    #[test]
    fn is_benign_docker_error_matches_404() {
        let err = bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            message: "not found".into(),
        };
        assert!(is_benign_docker_error(&err));
    }

    #[test]
    fn is_benign_docker_error_matches_304() {
        let err = bollard::errors::Error::DockerResponseServerError {
            status_code: 304,
            message: "not modified".into(),
        };
        assert!(is_benign_docker_error(&err));
    }

    #[test]
    fn is_benign_docker_error_rejects_500() {
        let err = bollard::errors::Error::DockerResponseServerError {
            status_code: 500,
            message: "internal error".into(),
        };
        assert!(!is_benign_docker_error(&err));
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn network_create_and_teardown() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let network_name = format!("localfunctions-test-{}", uuid::Uuid::new_v4());
        let net = DockerNetwork::new(docker.clone(), network_name.clone());

        // Create network
        net.ensure_created().await.unwrap();

        // Verify it exists
        let info = docker
            .inspect_network(&network_name, None::<InspectNetworkOptions<String>>)
            .await
            .unwrap();
        assert_eq!(info.name.as_deref(), Some(network_name.as_str()));

        // Calling ensure_created again should succeed (reuse)
        net.ensure_created().await.unwrap();

        // Remove network
        net.remove().await.unwrap();

        // Verify it's gone
        let result = docker
            .inspect_network(&network_name, None::<InspectNetworkOptions<String>>)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn remove_nonexistent_network_succeeds() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let net = DockerNetwork::new(docker, "nonexistent-network-12345".to_string());

        // Should not error when network doesn't exist
        net.remove().await.unwrap();
    }
}
