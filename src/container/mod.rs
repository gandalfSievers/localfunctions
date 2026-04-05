use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bollard::container::{
    Config as ContainerConfig, CreateContainerOptions, ListContainersOptions,
    RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::Docker;
use bollard::network::{CreateNetworkOptions, InspectNetworkOptions};
use futures_util::TryStreamExt;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::types::{ContainerState, FunctionConfig, ServiceError};

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

    /// Stop and remove a single container by ID.
    ///
    /// Issues a Docker stop with the given grace period, then force-removes the
    /// container and deregisters it from tracking.
    pub async fn stop_and_remove(&self, container_id: &str, timeout: Duration) {
        let timeout_secs = timeout.as_secs().try_into().unwrap_or(i64::MAX);

        if let Err(e) = self
            .docker
            .stop_container(container_id, Some(StopContainerOptions { t: timeout_secs }))
            .await
        {
            if !is_benign_docker_error(&e) {
                error!(container_id = %container_id, %e, "failed to stop container");
            }
        }

        if let Err(e) = self
            .docker
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
        {
            if !is_benign_docker_error(&e) {
                error!(container_id = %container_id, %e, "failed to remove container");
            }
        }

        self.deregister(container_id).await;
        info!(container_id = %container_id, "container stopped and removed");
    }

    /// Stop and remove all containers for a given function.
    ///
    /// Returns the container IDs that were cleaned up.
    pub async fn stop_and_remove_by_function(
        &self,
        function_name: &str,
        timeout: Duration,
    ) -> Vec<String> {
        let timeout_secs = timeout.as_secs().try_into().unwrap_or(i64::MAX);
        let mut removed = Vec::new();

        // Collect matching container IDs under a read lock.
        let ids: Vec<String> = {
            let map = self.containers.read().await;
            map.values()
                .filter(|c| c.function_name == function_name)
                .map(|c| c.container_id.clone())
                .collect()
        };

        if ids.is_empty() {
            return removed;
        }

        info!(function = %function_name, count = ids.len(), "stopping containers for failed function");

        let mut handles = Vec::with_capacity(ids.len());
        for id in ids {
            let docker = self.docker.clone();
            let cid = id.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) = docker
                    .stop_container(&cid, Some(StopContainerOptions { t: timeout_secs }))
                    .await
                {
                    if !is_benign_docker_error(&e) {
                        error!(container_id = %cid, %e, "failed to stop container");
                    }
                }
                if let Err(e) = docker
                    .remove_container(
                        &cid,
                        Some(RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await
                {
                    if !is_benign_docker_error(&e) {
                        error!(container_id = %cid, %e, "failed to remove container");
                    }
                }
                cid
            }));
        }

        for handle in handles {
            match handle.await {
                Ok(cid) => {
                    // Deregister from tracking
                    self.containers.write().await.remove(&cid);
                    info!(container_id = %cid, function = %function_name, "container stopped and removed (init error)");
                    removed.push(cid);
                }
                Err(e) => {
                    error!(%e, "container cleanup task panicked");
                }
            }
        }

        removed
    }

    /// Detect and remove orphan containers from previous runs.
    ///
    /// Queries Docker for containers with the `managed-by=localfunctions` label
    /// and stops/removes them. This ensures no leftover containers consume
    /// resources after an unclean shutdown.
    pub async fn cleanup_orphans(&self) {
        let mut filters = HashMap::new();
        filters.insert("label", vec!["managed-by=localfunctions"]);

        let opts = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        let containers = match self.docker.list_containers(Some(opts)).await {
            Ok(c) => c,
            Err(e) => {
                error!(%e, "failed to list orphan containers");
                return;
            }
        };

        if containers.is_empty() {
            info!("no orphan containers found");
            return;
        }

        info!(count = containers.len(), "orphan containers detected, cleaning up");

        let mut handles = Vec::with_capacity(containers.len());
        for container in &containers {
            let id = match &container.id {
                Some(id) => id.clone(),
                None => continue,
            };
            let names = container
                .names
                .as_ref()
                .map(|n| n.join(", "))
                .unwrap_or_default();

            info!(container_id = %id, names = %names, "cleaning up orphan container");

            let docker = self.docker.clone();
            handles.push(tokio::spawn(async move {
                // Stop first (may already be stopped — that's fine)
                if let Err(e) = docker
                    .stop_container(&id, Some(StopContainerOptions { t: 5 }))
                    .await
                {
                    if !is_benign_docker_error(&e) {
                        warn!(container_id = %id, %e, "failed to stop orphan container");
                    }
                }

                // Force-remove
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
                        error!(container_id = %id, %e, "failed to remove orphan container");
                    }
                }
            }));
        }

        for handle in handles {
            if let Err(e) = handle.await {
                error!(%e, "orphan cleanup task panicked");
            }
        }

        info!(count = containers.len(), "orphan container cleanup complete");
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

// ---------------------------------------------------------------------------
// ContainerManager
// ---------------------------------------------------------------------------

/// Metadata for a managed container with lifecycle state tracking.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ManagedContainer {
    pub container_id: String,
    pub function_name: String,
    pub state: ContainerState,
    pub image: String,
    pub last_used: Instant,
}

/// Creates and manages Docker containers for Lambda function execution.
///
/// Handles image resolution (from `runtime_images` mapping or custom image),
/// lazy image pulling, container creation with correct mounts/env/limits,
/// and container lifecycle (start, stop, remove).
#[allow(dead_code)]
pub struct ContainerManager {
    docker: Docker,
    runtime_images: HashMap<String, String>,
    network_name: String,
    runtime_port: u16,
    region: String,
    registry: Arc<ContainerRegistry>,
    /// Tracks state of containers managed by this instance.
    containers: RwLock<HashMap<String, ManagedContainer>>,
}

#[allow(dead_code)]
impl ContainerManager {
    /// Create a new `ContainerManager`.
    ///
    /// - `runtime_images`: mapping from runtime name (e.g. `"python3.12"`) to
    ///   Docker image (e.g. `"public.ecr.aws/lambda/python:3.12"`).
    /// - `network_name`: the Docker network to attach containers to.
    /// - `registry`: shared `ContainerRegistry` for shutdown tracking.
    pub fn new(
        docker: Docker,
        runtime_images: HashMap<String, String>,
        network_name: String,
        runtime_port: u16,
        region: String,
        registry: Arc<ContainerRegistry>,
    ) -> Self {
        Self {
            docker,
            runtime_images,
            network_name,
            runtime_port,
            region,
            registry,
            containers: RwLock::new(HashMap::new()),
        }
    }

    /// Resolve the Docker image for a function.
    ///
    /// If the function has a custom `image` field, use that. Otherwise look up
    /// the runtime in the `runtime_images` map.
    pub fn resolve_image(&self, function: &FunctionConfig) -> Result<String, ServiceError> {
        if let Some(ref image) = function.image {
            return Ok(image.clone());
        }

        self.runtime_images
            .get(&function.runtime)
            .cloned()
            .ok_or_else(|| {
                ServiceError::InvalidRuntime(format!(
                    "no image configured for runtime '{}' — add it to runtime_images or set a \
                     custom image on the function",
                    function.runtime
                ))
            })
    }

    /// Pull a Docker image if it is not already present locally.
    ///
    /// This is a lazy pull: if the image already exists, this is a no-op.
    pub async fn ensure_image(&self, image: &str) -> Result<(), ServiceError> {
        // Check if image exists locally
        match self.docker.inspect_image(image).await {
            Ok(_) => {
                debug!(image = %image, "image already present locally");
                return Ok(());
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                // Image not found locally — pull it
            }
            Err(e) => {
                return Err(ServiceError::ServiceException(format!(
                    "failed to inspect image '{}': {}",
                    image, e
                )));
            }
        }

        info!(image = %image, "pulling image (not found locally)");

        let opts = CreateImageOptions {
            from_image: image,
            ..Default::default()
        };

        self.docker
            .create_image(Some(opts), None, None)
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| {
                ServiceError::ServiceException(format!("failed to pull image '{}': {}", image, e))
            })?;

        info!(image = %image, "image pulled successfully");
        Ok(())
    }

    /// Create and start a container for the given function.
    ///
    /// This method:
    /// 1. Resolves the Docker image (custom or from runtime_images)
    /// 2. Pulls the image lazily if not present
    /// 3. Creates the container with correct env vars, mounts, memory limits,
    ///    network, and labels
    /// 4. Starts the container
    /// 5. Registers it in the ContainerRegistry and tracks its state
    ///
    /// Returns the container ID on success.
    pub async fn create_and_start(
        &self,
        function: &FunctionConfig,
    ) -> Result<String, ServiceError> {
        let image = self.resolve_image(function)?;

        // Lazy pull
        self.ensure_image(&image).await?;

        // Build environment variables
        let env = lambda_env_vars(function, self.runtime_port, &self.region);

        // Code path mount: read-only at /var/task
        let code_path = function
            .code_path
            .to_str()
            .ok_or_else(|| {
                ServiceError::ServiceException(format!(
                    "code_path contains invalid UTF-8: {:?}",
                    function.code_path
                ))
            })?
            .to_string();

        let binds = vec![format!("{}:/var/task:ro", code_path)];

        // Memory limit in bytes (memory_size is in MB)
        let memory = (function.memory_size as i64) * 1024 * 1024;

        // Labels for identification
        let mut labels = HashMap::new();
        labels.insert("managed-by".to_string(), "localfunctions".to_string());
        labels.insert(
            "localfunctions.function".to_string(),
            function.name.clone(),
        );

        let host_config = HostConfig {
            binds: Some(binds),
            memory: Some(memory),
            network_mode: Some(self.network_name.clone()),
            extra_hosts: Some(container_extra_hosts()),
            // Explicitly no privileged mode, no port bindings
            privileged: Some(false),
            publish_all_ports: Some(false),
            ..Default::default()
        };

        let container_config: ContainerConfig<String> = ContainerConfig {
            image: Some(image.clone()),
            env: Some(env),
            labels: Some(labels),
            host_config: Some(host_config),
            ..Default::default()
        };

        // Track as Starting
        let container_name = format!("localfunctions-{}-{}", function.name, uuid_short());

        debug!(
            function = %function.name,
            image = %image,
            container_name = %container_name,
            "creating container"
        );

        let create_opts = CreateContainerOptions {
            name: container_name.as_str(),
            platform: None,
        };

        let response = self
            .docker
            .create_container(Some(create_opts), container_config)
            .await
            .map_err(|e| {
                ServiceError::ServiceException(format!(
                    "failed to create container for '{}': {}",
                    function.name, e
                ))
            })?;

        let container_id = response.id;

        // Insert as Starting state
        {
            let mut containers = self.containers.write().await;
            containers.insert(
                container_id.clone(),
                ManagedContainer {
                    container_id: container_id.clone(),
                    function_name: function.name.clone(),
                    state: ContainerState::Starting,
                    image: image.clone(),
                    last_used: Instant::now(),
                },
            );
        }

        // Start the container
        if let Err(e) = self
            .docker
            .start_container(&container_id, None::<StartContainerOptions<String>>)
            .await
        {
            // Clean up on failure
            self.set_state(&container_id, ContainerState::Stopping).await;
            let _ = self
                .docker
                .remove_container(
                    &container_id,
                    Some(RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await;
            self.containers.write().await.remove(&container_id);
            return Err(ServiceError::ServiceException(format!(
                "failed to start container for '{}': {}",
                function.name, e
            )));
        }

        // Register in the global registry for shutdown cleanup
        self.registry
            .register(container_id.clone(), function.name.clone())
            .await;

        // Transition to Idle
        self.set_state(&container_id, ContainerState::Idle).await;

        info!(
            container_id = %container_id,
            function = %function.name,
            image = %image,
            "container started"
        );

        Ok(container_id)
    }

    /// Update the state of a managed container.
    pub async fn set_state(&self, container_id: &str, state: ContainerState) {
        let mut containers = self.containers.write().await;
        if let Some(entry) = containers.get_mut(container_id) {
            debug!(
                container_id = %container_id,
                function = %entry.function_name,
                old_state = ?entry.state,
                new_state = ?state,
                "container state transition"
            );
            entry.state = state;
        }
    }

    /// Get the current state of a managed container.
    pub async fn get_state(&self, container_id: &str) -> Option<ContainerState> {
        self.containers
            .read()
            .await
            .get(container_id)
            .map(|c| c.state)
    }

    /// Stop and remove a single container.
    ///
    /// Transitions the container through Stopping state, stops it with the
    /// given timeout, force-removes it, and deregisters it from both the
    /// local tracking and the global registry.
    pub async fn stop_and_remove(
        &self,
        container_id: &str,
        timeout: Duration,
    ) -> Result<(), ServiceError> {
        self.set_state(container_id, ContainerState::Stopping).await;

        let timeout_secs = timeout.as_secs().try_into().unwrap_or(i64::MAX);

        if let Err(e) = self
            .docker
            .stop_container(
                container_id,
                Some(StopContainerOptions { t: timeout_secs }),
            )
            .await
        {
            if !is_benign_docker_error(&e) {
                error!(container_id = %container_id, %e, "failed to stop container");
            }
        }

        if let Err(e) = self
            .docker
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
        {
            if !is_benign_docker_error(&e) {
                error!(container_id = %container_id, %e, "failed to remove container");
                return Err(ServiceError::ServiceException(format!(
                    "failed to remove container '{}': {}",
                    container_id, e
                )));
            }
        }

        // Deregister from both local and global tracking
        self.containers.write().await.remove(container_id);
        self.registry.deregister(container_id).await;

        info!(container_id = %container_id, "container stopped and removed");
        Ok(())
    }

    /// Return the number of containers currently managed.
    pub async fn count(&self) -> usize {
        self.containers.read().await.len()
    }

    /// Get a snapshot of all managed containers.
    pub async fn list(&self) -> Vec<ManagedContainer> {
        self.containers.read().await.values().cloned().collect()
    }

    /// Atomically claim an idle container for a function and mark it Busy.
    ///
    /// Uses FIFO selection (oldest `last_used` first) so that containers idle
    /// the longest are reused first, giving the reaper a consistent window to
    /// reap the most recently used ones.
    ///
    /// Returns the container ID if an idle container was found.
    pub async fn claim_idle_container(&self, function_name: &str) -> Option<String> {
        let mut containers = self.containers.write().await;
        let oldest_idle = containers
            .values()
            .filter(|c| c.function_name == function_name && c.state == ContainerState::Idle)
            .min_by_key(|c| c.last_used)
            .map(|c| c.container_id.clone());

        if let Some(ref id) = oldest_idle {
            if let Some(entry) = containers.get_mut(id) {
                debug!(
                    container_id = %id,
                    function = %function_name,
                    "reusing warm container"
                );
                entry.state = ContainerState::Busy;
            }
        }
        oldest_idle
    }

    /// Release a container back to the idle pool after an invocation completes.
    ///
    /// Transitions the container to Idle and updates `last_used` so the reaper
    /// knows when it became idle.
    pub async fn release_container(&self, container_id: &str) {
        let mut containers = self.containers.write().await;
        if let Some(entry) = containers.get_mut(container_id) {
            debug!(
                container_id = %container_id,
                function = %entry.function_name,
                "container released to idle pool"
            );
            entry.state = ContainerState::Idle;
            entry.last_used = Instant::now();
        }
    }

    /// Reap idle containers that have exceeded `idle_timeout`.
    ///
    /// Scans all managed containers for those in Idle state whose `last_used`
    /// timestamp is older than the timeout, then stops and removes them.
    pub async fn reap_idle_containers(&self, idle_timeout: Duration) {
        let now = Instant::now();
        let expired: Vec<(String, String)> = {
            let containers = self.containers.read().await;
            containers
                .values()
                .filter(|c| {
                    c.state == ContainerState::Idle
                        && now.duration_since(c.last_used) > idle_timeout
                })
                .map(|c| (c.container_id.clone(), c.function_name.clone()))
                .collect()
        };

        if expired.is_empty() {
            return;
        }

        info!(count = expired.len(), "reaping idle containers");

        for (container_id, function_name) in expired {
            debug!(
                container_id = %container_id,
                function = %function_name,
                "reaping idle container"
            );
            if let Err(e) = self.stop_and_remove(&container_id, Duration::from_secs(5)).await {
                error!(container_id = %container_id, %e, "failed to reap idle container");
            }
        }
    }

    /// Remove all internal tracking entries for a function.
    ///
    /// Used after init errors where containers are killed via the registry.
    pub async fn deregister_by_function(&self, function_name: &str) {
        let mut containers = self.containers.write().await;
        containers.retain(|_, c| c.function_name != function_name);
    }

    /// Insert a container directly into tracking (for testing).
    #[doc(hidden)]
    pub async fn insert_test_container(
        &self,
        container_id: String,
        function_name: String,
        state: ContainerState,
    ) {
        let mut containers = self.containers.write().await;
        containers.insert(
            container_id.clone(),
            ManagedContainer {
                container_id,
                function_name,
                state,
                image: "test:latest".into(),
                last_used: Instant::now(),
            },
        );
    }
}

/// Generate a short unique suffix for container names.
fn uuid_short() -> String {
    let id = uuid::Uuid::new_v4();
    id.to_string()[..8].to_string()
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

/// Build the `extra_hosts` entries for a Lambda container.
///
/// Returns `["host.docker.internal:host-gateway"]` so that containers can
/// resolve `host.docker.internal` to the host machine on all platforms.
/// On macOS/Windows Docker Desktop this mapping is added automatically, but
/// on Linux the explicit `host-gateway` directive is required.  Including it
/// unconditionally is harmless and keeps the behaviour consistent.
///
/// This allows Lambda functions to reach other local* services (e.g.
/// localObjectStorage, localSecrets) running on the host via
/// `host.docker.internal:<port>`.  When other services run as Docker
/// containers on the same user-defined bridge network, Docker's embedded DNS
/// resolves their container names automatically — no extra config needed.
pub fn container_extra_hosts() -> Vec<String> {
    vec!["host.docker.internal:host-gateway".to_string()]
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

    // -- uuid_short --------------------------------------------------------

    #[test]
    fn uuid_short_is_8_chars() {
        let s = uuid_short();
        assert_eq!(s.len(), 8);
    }

    #[test]
    fn uuid_short_is_unique() {
        let a = uuid_short();
        let b = uuid_short();
        assert_ne!(a, b);
    }

    // -- ContainerManager: resolve_image -----------------------------------

    fn make_runtime_images() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(
            "python3.12".to_string(),
            "public.ecr.aws/lambda/python:3.12".to_string(),
        );
        m.insert(
            "nodejs20.x".to_string(),
            "public.ecr.aws/lambda/nodejs:20".to_string(),
        );
        m
    }

    fn make_container_manager() -> ContainerManager {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        ContainerManager::new(
            docker,
            make_runtime_images(),
            "test-network".to_string(),
            9601,
            "us-east-1".to_string(),
            registry,
        )
    }

    #[test]
    fn resolve_image_from_runtime_images() {
        let mgr = make_container_manager();
        let func = make_test_function(); // runtime = "python3.12"
        let image = mgr.resolve_image(&func).unwrap();
        assert_eq!(image, "public.ecr.aws/lambda/python:3.12");
    }

    #[test]
    fn resolve_image_from_runtime_images_nodejs() {
        let mgr = make_container_manager();
        let mut func = make_test_function();
        func.runtime = "nodejs20.x".into();
        let image = mgr.resolve_image(&func).unwrap();
        assert_eq!(image, "public.ecr.aws/lambda/nodejs:20");
    }

    #[test]
    fn resolve_image_custom_image_overrides_runtime() {
        let mgr = make_container_manager();
        let mut func = make_test_function();
        func.image = Some("my-custom:latest".into());
        let image = mgr.resolve_image(&func).unwrap();
        assert_eq!(image, "my-custom:latest");
    }

    #[test]
    fn resolve_image_custom_image_for_custom_runtime() {
        let mgr = make_container_manager();
        let mut func = make_test_function();
        func.runtime = "custom".into();
        func.image = Some("my-custom-runtime:v1".into());
        let image = mgr.resolve_image(&func).unwrap();
        assert_eq!(image, "my-custom-runtime:v1");
    }

    #[test]
    fn resolve_image_unknown_runtime_returns_error() {
        let mgr = make_container_manager();
        let mut func = make_test_function();
        func.runtime = "ruby3.2".into();
        let err = mgr.resolve_image(&func).unwrap_err();
        assert!(matches!(err, ServiceError::InvalidRuntime(_)));
        assert!(err.to_string().contains("ruby3.2"));
    }

    #[test]
    fn resolve_image_empty_runtime_images() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let mgr = ContainerManager::new(
            docker,
            HashMap::new(),
            "net".into(),
            9601,
            "us-east-1".into(),
            registry,
        );
        let func = make_test_function();
        let err = mgr.resolve_image(&func).unwrap_err();
        assert!(matches!(err, ServiceError::InvalidRuntime(_)));
    }

    // -- ContainerManager: state tracking ----------------------------------

    #[tokio::test]
    async fn container_manager_starts_empty() {
        let mgr = make_container_manager();
        assert_eq!(mgr.count().await, 0);
        assert!(mgr.list().await.is_empty());
    }

    #[tokio::test]
    async fn container_manager_get_state_nonexistent_returns_none() {
        let mgr = make_container_manager();
        assert_eq!(mgr.get_state("nonexistent").await, None);
    }

    #[tokio::test]
    async fn container_manager_set_and_get_state() {
        let mgr = make_container_manager();

        // Manually insert a container for testing state transitions
        {
            let mut containers = mgr.containers.write().await;
            containers.insert(
                "test-id".to_string(),
                ManagedContainer {
                    container_id: "test-id".to_string(),
                    function_name: "test-func".to_string(),
                    state: ContainerState::Starting,
                    image: "test:latest".to_string(),
                    last_used: Instant::now(),
                },
            );
        }

        assert_eq!(
            mgr.get_state("test-id").await,
            Some(ContainerState::Starting)
        );

        mgr.set_state("test-id", ContainerState::Idle).await;
        assert_eq!(
            mgr.get_state("test-id").await,
            Some(ContainerState::Idle)
        );

        mgr.set_state("test-id", ContainerState::Busy).await;
        assert_eq!(
            mgr.get_state("test-id").await,
            Some(ContainerState::Busy)
        );

        mgr.set_state("test-id", ContainerState::Stopping).await;
        assert_eq!(
            mgr.get_state("test-id").await,
            Some(ContainerState::Stopping)
        );
    }

    #[tokio::test]
    async fn container_manager_set_state_nonexistent_is_noop() {
        let mgr = make_container_manager();
        // Should not panic
        mgr.set_state("nonexistent", ContainerState::Idle).await;
        assert_eq!(mgr.get_state("nonexistent").await, None);
    }

    #[tokio::test]
    async fn container_manager_list_returns_all() {
        let mgr = make_container_manager();

        {
            let mut containers = mgr.containers.write().await;
            containers.insert(
                "id-1".to_string(),
                ManagedContainer {
                    container_id: "id-1".to_string(),
                    function_name: "func-a".to_string(),
                    state: ContainerState::Idle,
                    image: "img-a:latest".to_string(),
                    last_used: Instant::now(),
                },
            );
            containers.insert(
                "id-2".to_string(),
                ManagedContainer {
                    container_id: "id-2".to_string(),
                    function_name: "func-b".to_string(),
                    state: ContainerState::Busy,
                    image: "img-b:latest".to_string(),
                    last_used: Instant::now(),
                },
            );
        }

        assert_eq!(mgr.count().await, 2);
        let list = mgr.list().await;
        assert_eq!(list.len(), 2);
        let names: Vec<&str> = list.iter().map(|c| c.function_name.as_str()).collect();
        assert!(names.contains(&"func-a"));
        assert!(names.contains(&"func-b"));
    }

    // -- ManagedContainer ---------------------------------------------------

    #[test]
    fn managed_container_clone() {
        let mc = ManagedContainer {
            container_id: "abc".into(),
            function_name: "f".into(),
            state: ContainerState::Idle,
            image: "img:latest".into(),
            last_used: Instant::now(),
        };
        let mc2 = mc.clone();
        assert_eq!(mc2.container_id, "abc");
        assert_eq!(mc2.state, ContainerState::Idle);
    }

    // -- Warm container pool ------------------------------------------------

    #[tokio::test]
    async fn claim_idle_container_returns_none_when_empty() {
        let mgr = make_container_manager();
        assert!(mgr.claim_idle_container("test-func").await.is_none());
    }

    #[tokio::test]
    async fn claim_idle_container_returns_idle_container() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Idle)
            .await;

        let claimed = mgr.claim_idle_container("test-func").await;
        assert_eq!(claimed, Some("ctr-1".to_string()));

        // Container should now be Busy
        assert_eq!(mgr.get_state("ctr-1").await, Some(ContainerState::Busy));
    }

    #[tokio::test]
    async fn claim_idle_container_skips_busy_containers() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Busy)
            .await;

        assert!(mgr.claim_idle_container("test-func").await.is_none());
    }

    #[tokio::test]
    async fn claim_idle_container_skips_wrong_function() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "other-func".into(), ContainerState::Idle)
            .await;

        assert!(mgr.claim_idle_container("test-func").await.is_none());
    }

    #[tokio::test]
    async fn claim_idle_container_fifo_selects_oldest() {
        let mgr = make_container_manager();

        // Insert two idle containers with different last_used times
        {
            let mut containers = mgr.containers.write().await;
            containers.insert(
                "ctr-old".to_string(),
                ManagedContainer {
                    container_id: "ctr-old".into(),
                    function_name: "test-func".into(),
                    state: ContainerState::Idle,
                    image: "test:latest".into(),
                    last_used: Instant::now() - Duration::from_secs(60),
                },
            );
            containers.insert(
                "ctr-new".to_string(),
                ManagedContainer {
                    container_id: "ctr-new".into(),
                    function_name: "test-func".into(),
                    state: ContainerState::Idle,
                    image: "test:latest".into(),
                    last_used: Instant::now(),
                },
            );
        }

        // FIFO should pick the oldest
        let claimed = mgr.claim_idle_container("test-func").await;
        assert_eq!(claimed, Some("ctr-old".to_string()));
        assert_eq!(mgr.get_state("ctr-old").await, Some(ContainerState::Busy));
        assert_eq!(mgr.get_state("ctr-new").await, Some(ContainerState::Idle));
    }

    #[tokio::test]
    async fn claim_idle_container_second_claim_gets_next() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Idle)
            .await;
        mgr.insert_test_container("ctr-2".into(), "test-func".into(), ContainerState::Idle)
            .await;

        let first = mgr.claim_idle_container("test-func").await;
        assert!(first.is_some());

        let second = mgr.claim_idle_container("test-func").await;
        assert!(second.is_some());
        assert_ne!(first, second);

        // Third claim should fail — both are busy
        assert!(mgr.claim_idle_container("test-func").await.is_none());
    }

    #[tokio::test]
    async fn release_container_transitions_to_idle() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Busy)
            .await;

        mgr.release_container("ctr-1").await;

        assert_eq!(mgr.get_state("ctr-1").await, Some(ContainerState::Idle));
    }

    #[tokio::test]
    async fn release_container_updates_last_used() {
        let mgr = make_container_manager();
        let old_time = Instant::now() - Duration::from_secs(600);
        {
            let mut containers = mgr.containers.write().await;
            containers.insert(
                "ctr-1".to_string(),
                ManagedContainer {
                    container_id: "ctr-1".into(),
                    function_name: "test-func".into(),
                    state: ContainerState::Busy,
                    image: "test:latest".into(),
                    last_used: old_time,
                },
            );
        }

        mgr.release_container("ctr-1").await;

        let containers = mgr.containers.read().await;
        let c = containers.get("ctr-1").unwrap();
        assert!(c.last_used > old_time);
    }

    #[tokio::test]
    async fn release_nonexistent_container_is_noop() {
        let mgr = make_container_manager();
        // Should not panic
        mgr.release_container("nonexistent").await;
    }

    #[tokio::test]
    async fn reap_idle_containers_removes_expired() {
        let mgr = make_container_manager();
        let expired_time = Instant::now() - Duration::from_secs(600);
        {
            let mut containers = mgr.containers.write().await;
            containers.insert(
                "ctr-expired".to_string(),
                ManagedContainer {
                    container_id: "ctr-expired".into(),
                    function_name: "test-func".into(),
                    state: ContainerState::Idle,
                    image: "test:latest".into(),
                    last_used: expired_time,
                },
            );
            containers.insert(
                "ctr-fresh".to_string(),
                ManagedContainer {
                    container_id: "ctr-fresh".into(),
                    function_name: "test-func".into(),
                    state: ContainerState::Idle,
                    image: "test:latest".into(),
                    last_used: Instant::now(),
                },
            );
        }

        mgr.reap_idle_containers(Duration::from_secs(300)).await;

        // Expired container should be removed from tracking
        assert!(mgr.get_state("ctr-expired").await.is_none());
        // Fresh container should remain
        assert_eq!(
            mgr.get_state("ctr-fresh").await,
            Some(ContainerState::Idle)
        );
    }

    #[tokio::test]
    async fn reap_idle_containers_skips_busy_containers() {
        let mgr = make_container_manager();
        let expired_time = Instant::now() - Duration::from_secs(600);
        {
            let mut containers = mgr.containers.write().await;
            containers.insert(
                "ctr-busy".to_string(),
                ManagedContainer {
                    container_id: "ctr-busy".into(),
                    function_name: "test-func".into(),
                    state: ContainerState::Busy,
                    image: "test:latest".into(),
                    last_used: expired_time,
                },
            );
        }

        mgr.reap_idle_containers(Duration::from_secs(300)).await;

        // Busy container should NOT be reaped even if last_used is old
        assert_eq!(
            mgr.get_state("ctr-busy").await,
            Some(ContainerState::Busy)
        );
    }

    #[tokio::test]
    async fn reap_idle_containers_noop_when_none_expired() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Idle)
            .await;

        mgr.reap_idle_containers(Duration::from_secs(300)).await;

        // Container should still be present (was just created, not expired)
        assert_eq!(mgr.get_state("ctr-1").await, Some(ContainerState::Idle));
    }

    #[tokio::test]
    async fn deregister_by_function_removes_matching() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "func-a".into(), ContainerState::Idle)
            .await;
        mgr.insert_test_container("ctr-2".into(), "func-a".into(), ContainerState::Busy)
            .await;
        mgr.insert_test_container("ctr-3".into(), "func-b".into(), ContainerState::Idle)
            .await;

        mgr.deregister_by_function("func-a").await;

        assert!(mgr.get_state("ctr-1").await.is_none());
        assert!(mgr.get_state("ctr-2").await.is_none());
        assert_eq!(mgr.get_state("ctr-3").await, Some(ContainerState::Idle));
    }

    #[tokio::test]
    async fn warm_container_lifecycle_claim_release_reuse() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Idle)
            .await;

        // Claim
        let claimed = mgr.claim_idle_container("test-func").await;
        assert_eq!(claimed, Some("ctr-1".to_string()));
        assert_eq!(mgr.get_state("ctr-1").await, Some(ContainerState::Busy));

        // No more idle containers
        assert!(mgr.claim_idle_container("test-func").await.is_none());

        // Release
        mgr.release_container("ctr-1").await;
        assert_eq!(mgr.get_state("ctr-1").await, Some(ContainerState::Idle));

        // Can claim again
        let reclaimed = mgr.claim_idle_container("test-func").await;
        assert_eq!(reclaimed, Some("ctr-1".to_string()));
    }

    // -- container_extra_hosts -----------------------------------------------

    #[test]
    fn container_extra_hosts_contains_host_gateway() {
        let hosts = container_extra_hosts();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0], "host.docker.internal:host-gateway");
    }

    #[test]
    fn lambda_env_vars_passes_through_endpoint_url_vars() {
        let mut func = make_test_function();
        func.environment.insert(
            "AWS_ENDPOINT_URL_S3".into(),
            "http://host.docker.internal:9090".into(),
        );
        func.environment.insert(
            "AWS_ENDPOINT_URL_SECRETS_MANAGER".into(),
            "http://host.docker.internal:9091".into(),
        );
        let vars = lambda_env_vars(&func, 9601, "us-east-1");
        assert!(vars.contains(
            &"AWS_ENDPOINT_URL_S3=http://host.docker.internal:9090".to_string()
        ));
        assert!(vars.contains(
            &"AWS_ENDPOINT_URL_SECRETS_MANAGER=http://host.docker.internal:9091".to_string()
        ));
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

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn container_manager_create_start_stop_lifecycle() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let network_name = format!("lf-test-{}", uuid_short());

        // Create network for the test
        let net = DockerNetwork::new(docker.clone(), network_name.clone());
        net.ensure_created().await.unwrap();

        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let mut runtime_images = HashMap::new();
        runtime_images.insert(
            "python3.12".to_string(),
            "public.ecr.aws/lambda/python:3.12".to_string(),
        );

        let mgr = ContainerManager::new(
            docker.clone(),
            runtime_images,
            network_name.clone(),
            9601,
            "us-east-1".to_string(),
            registry.clone(),
        );

        // Create a temp directory for code
        let code_dir = tempfile::tempdir().unwrap();

        let func = FunctionConfig {
            name: "test-func".into(),
            runtime: "python3.12".into(),
            handler: "main.handler".into(),
            code_path: code_dir.path().to_path_buf(),
            timeout: 30,
            memory_size: 128,
            environment: HashMap::from([("MY_VAR".into(), "my_value".into())]),
            image: None,
        };

        // Create and start
        let container_id = mgr.create_and_start(&func).await.unwrap();
        assert!(!container_id.is_empty());

        // Verify state is Idle after creation
        assert_eq!(
            mgr.get_state(&container_id).await,
            Some(ContainerState::Idle)
        );
        assert_eq!(mgr.count().await, 1);

        // Verify it's registered in the global registry
        assert_eq!(registry.count().await, 1);

        // Inspect container to verify configuration
        let info = docker.inspect_container(&container_id, None).await.unwrap();

        // Verify labels
        let labels = info.config.as_ref().unwrap().labels.as_ref().unwrap();
        assert_eq!(labels.get("managed-by"), Some(&"localfunctions".to_string()));
        assert_eq!(
            labels.get("localfunctions.function"),
            Some(&"test-func".to_string())
        );

        // Verify host config
        let host_config = info.host_config.as_ref().unwrap();
        assert_eq!(host_config.privileged, Some(false));

        // Verify memory limit (128 MB)
        assert_eq!(host_config.memory, Some(128 * 1024 * 1024));

        // Verify network
        assert_eq!(
            host_config.network_mode,
            Some(network_name.clone())
        );

        // Verify volume mount
        let binds = host_config.binds.as_ref().unwrap();
        assert_eq!(binds.len(), 1);
        assert!(binds[0].ends_with(":/var/task:ro"));

        // Verify env vars
        let env = info.config.as_ref().unwrap().env.as_ref().unwrap();
        assert!(env.iter().any(|e| e == "AWS_LAMBDA_FUNCTION_NAME=test-func"));
        assert!(env.iter().any(|e| e == "_HANDLER=main.handler"));
        assert!(env.iter().any(|e| e == "MY_VAR=my_value"));

        // Stop and remove
        mgr.stop_and_remove(&container_id, Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(mgr.count().await, 0);
        assert_eq!(mgr.get_state(&container_id).await, None);
        assert_eq!(registry.count().await, 0);

        // Clean up network
        net.remove().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn container_manager_ensure_image_pulls_lazily() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let mgr = ContainerManager::new(
            docker,
            HashMap::new(),
            "test".into(),
            9601,
            "us-east-1".into(),
            registry,
        );

        // Use a small, commonly available image
        // This test verifies that ensure_image does not fail for a valid image
        mgr.ensure_image("public.ecr.aws/lambda/python:3.12")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn cleanup_orphans_completes_with_no_orphans() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = ContainerRegistry::new(docker);
        // Should complete without error when there are no orphan containers
        registry.cleanup_orphans().await;
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn cleanup_orphans_removes_labeled_containers() {
        let docker = Docker::connect_with_local_defaults().unwrap();

        // Create a container with the managed-by=localfunctions label
        let mut labels = HashMap::new();
        labels.insert("managed-by", "localfunctions");
        labels.insert("localfunctions.function", "orphan-test");

        let config = ContainerConfig {
            image: Some("hello-world:latest"),
            labels: Some(labels),
            ..Default::default()
        };

        let container = docker
            .create_container(
                Some(CreateContainerOptions {
                    name: "localfunctions-orphan-test",
                    ..Default::default()
                }),
                config,
            )
            .await
            .unwrap();

        let container_id = container.id.clone();

        // Verify the container exists
        let info = docker.inspect_container(&container_id, None).await;
        assert!(info.is_ok());

        // Run cleanup
        let registry = ContainerRegistry::new(docker.clone());
        registry.cleanup_orphans().await;

        // Verify the container was removed
        let info = docker.inspect_container(&container_id, None).await;
        assert!(info.is_err(), "orphan container should have been removed");
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn cleanup_orphans_does_not_remove_unrelated_containers() {
        let docker = Docker::connect_with_local_defaults().unwrap();

        // Create a container WITHOUT the managed-by=localfunctions label
        let mut labels = HashMap::new();
        labels.insert("managed-by", "something-else");

        let config = ContainerConfig {
            image: Some("hello-world:latest"),
            labels: Some(labels),
            ..Default::default()
        };

        let container = docker
            .create_container(
                Some(CreateContainerOptions {
                    name: "unrelated-container-test",
                    ..Default::default()
                }),
                config,
            )
            .await
            .unwrap();

        let container_id = container.id.clone();

        // Run cleanup
        let registry = ContainerRegistry::new(docker.clone());
        registry.cleanup_orphans().await;

        // Verify the unrelated container still exists
        let info = docker.inspect_container(&container_id, None).await;
        assert!(info.is_ok(), "unrelated container should NOT have been removed");

        // Clean up manually
        let _ = docker
            .remove_container(
                &container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn container_manager_stop_nonexistent_container_handles_gracefully() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let mgr = ContainerManager::new(
            docker,
            HashMap::new(),
            "test".into(),
            9601,
            "us-east-1".into(),
            registry,
        );

        // Stopping a nonexistent container should succeed (benign errors are swallowed)
        let result = mgr
            .stop_and_remove("nonexistent-container-id", Duration::from_secs(1))
            .await;
        assert!(result.is_ok());
    }
}
