use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bollard::container::{
    Config as ContainerConfig, CreateContainerOptions, ListContainersOptions, LogOutput,
    LogsOptions, RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
    WaitContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::system::EventsOptions;
use bollard::Docker;
use bollard::network::{CreateNetworkOptions, InspectNetworkOptions};
use futures_util::{StreamExt, TryStreamExt};
use tokio::sync::{watch, RwLock, Semaphore};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::runtime::RuntimeBridge;
use crate::types::{ContainerState, FunctionConfig, ServiceError};

/// Configuration for host AWS credential forwarding into function containers.
#[derive(Debug, Clone)]
pub struct CredentialForwardingConfig {
    /// Forward host AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
    /// AWS_SESSION_TOKEN (if set) as environment variables.
    pub forward_env: bool,
    /// Mount the host ~/.aws directory read-only at /root/.aws inside
    /// containers and forward AWS_PROFILE if set.
    pub mount_aws_dir: bool,
}

impl Default for CredentialForwardingConfig {
    fn default() -> Self {
        Self {
            forward_env: true,
            mount_aws_dir: false,
        }
    }
}

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
    /// Limits the total number of active containers. Each container holds one
    /// permit; when all permits are taken, new container creation blocks until
    /// a permit is released (container removed).
    container_semaphore: Arc<Semaphore>,
    /// Controls whether and how host AWS credentials are forwarded to
    /// function containers.
    credential_config: CredentialForwardingConfig,
}

#[allow(dead_code)]
impl ContainerManager {
    /// Create a new `ContainerManager`.
    ///
    /// - `runtime_images`: mapping from runtime name (e.g. `"python3.12"`) to
    ///   Docker image (e.g. `"public.ecr.aws/lambda/python:3.12"`).
    /// - `network_name`: the Docker network to attach containers to.
    /// - `registry`: shared `ContainerRegistry` for shutdown tracking.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        docker: Docker,
        runtime_images: HashMap<String, String>,
        network_name: String,
        runtime_port: u16,
        region: String,
        registry: Arc<ContainerRegistry>,
        max_containers: usize,
        credential_config: CredentialForwardingConfig,
    ) -> Self {
        Self {
            docker,
            runtime_images,
            network_name,
            runtime_port,
            region,
            registry,
            containers: RwLock::new(HashMap::new()),
            container_semaphore: Arc::new(Semaphore::new(max_containers)),
            credential_config,
        }
    }

    /// Try to acquire a container slot within the given timeout.
    ///
    /// Returns `true` if a slot was acquired, `false` if the timeout expired
    /// (all `max_containers` slots are in use). The caller must ensure a
    /// corresponding `release_container_slot()` call when the container is
    /// eventually removed.
    pub async fn acquire_container_slot(&self, timeout: Duration) -> bool {
        match tokio::time::timeout(
            timeout,
            self.container_semaphore.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => {
                // Deliberately forget the permit so it is not auto-released
                // when dropped. We manage release manually via
                // release_container_slot().
                permit.forget();
                true
            }
            _ => false,
        }
    }

    /// Release a container slot back to the pool, allowing a new container to
    /// be created.
    pub fn release_container_slot(&self) {
        self.container_semaphore.add_permits(1);
    }

    /// Return the number of available container slots.
    pub fn available_container_slots(&self) -> usize {
        self.container_semaphore.available_permits()
    }

    /// Count the number of active (Starting or Busy) containers for a specific function.
    pub async fn count_active_by_function(&self, function_name: &str) -> usize {
        let containers = self.containers.read().await;
        containers
            .values()
            .filter(|c| {
                c.function_name == function_name
                    && matches!(c.state, ContainerState::Starting | ContainerState::Busy)
            })
            .count()
    }

    /// Resolve the Docker image for a function.
    ///
    /// Priority: `image_uri` > `image` > `runtime_images` map lookup.
    /// `image_uri` is used for fully-packaged container image functions.
    /// `image` is for custom runtime base images (code still mounted).
    pub fn resolve_image(&self, function: &FunctionConfig) -> Result<String, ServiceError> {
        if let Some(ref image_uri) = function.image_uri {
            return Ok(image_uri.clone());
        }

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
    /// When `platform` is provided (e.g. `"linux/arm64"`), the pull request
    /// includes it so the correct architecture variant is fetched.
    pub async fn ensure_image(&self, image: &str, platform: Option<&str>) -> Result<(), ServiceError> {
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
            platform: platform.unwrap_or_default(),
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

        // Resolve target platform from the function's architecture setting
        let platform = match function.architecture.as_str() {
            "arm64" => "linux/arm64",
            _ => "linux/amd64",
        };

        // Lazy pull (with platform for multi-arch manifests)
        self.ensure_image(&image, Some(platform)).await?;

        // Build environment variables (with optional host credential forwarding)
        let env = lambda_env_vars(
            function,
            self.runtime_port,
            &self.region,
            &self.credential_config,
        );

        // Code path mount: read-only at /var/task (skipped for image_uri functions
        // since the image already contains the function code)
        let mut binds = Vec::new();
        if function.image_uri.is_none() {
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
            binds.push(format!("{}:/var/task:ro", code_path));
        }

        // Mount Lambda Layers at /opt (read-only)
        // When a single layer is configured, mount it directly at /opt.
        // When multiple layers are configured, create a merged directory so
        // later layers take precedence for conflicting files, matching AWS
        // Lambda behavior.
        if !function.layers.is_empty() {
            let opt_path = if function.layers.len() == 1 {
                function.layers[0].to_str().ok_or_else(|| {
                    ServiceError::ServiceException(format!(
                        "layer path contains invalid UTF-8: {:?}",
                        function.layers[0]
                    ))
                })?.to_string()
            } else {
                // Merge multiple layers into a temporary directory
                let merged = merge_layers(&function.name, &function.layers)?;
                merged.to_str().ok_or_else(|| {
                    ServiceError::ServiceException(
                        "merged layer path contains invalid UTF-8".to_string(),
                    )
                })?.to_string()
            };
            binds.push(format!("{}:/opt:ro", opt_path));
        }

        // Optionally mount host ~/.aws directory read-only
        if self.credential_config.mount_aws_dir {
            if let Some(aws_dir) = host_aws_config_dir() {
                binds.push(format!("{}:/root/.aws:ro", aws_dir));
            }
        }

        // Memory limit in bytes (memory_size is in MB)
        let memory = (function.memory_size as i64) * 1024 * 1024;

        // CPU allocation proportional to memory (matching AWS Lambda behavior)
        let (cpu_period, cpu_quota, cpu_shares) = cpu_constraints(function.memory_size);

        // Labels for identification
        let mut labels = HashMap::new();
        labels.insert("managed-by".to_string(), "localfunctions".to_string());
        labels.insert(
            "localfunctions.function".to_string(),
            function.name.clone(),
        );

        // Ephemeral /tmp as a size-limited tmpfs (matches AWS Lambda behavior)
        let tmpfs_size_bytes = (function.ephemeral_storage_mb as i64) * 1024 * 1024;
        let tmpfs = HashMap::from([(
            "/tmp".to_string(),
            format!("size={}", tmpfs_size_bytes),
        )]);

        let host_config = HostConfig {
            binds: Some(binds),
            memory: Some(memory),
            cpu_period: Some(cpu_period),
            cpu_quota: Some(cpu_quota),
            cpu_shares: Some(cpu_shares),
            network_mode: Some(self.network_name.clone()),
            extra_hosts: Some(container_extra_hosts()),
            tmpfs: Some(tmpfs),
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
            platform: Some(platform),
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

        // Release the container slot so new containers can be created.
        self.release_container_slot();

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

    /// Retrieve the combined stdout and stderr output from a container.
    ///
    /// Returns at most the last `max_bytes` bytes of output. Used to collect
    /// logs for the `X-Amz-Log-Result` response header (LogType: Tail).
    /// Returns an empty string if logs cannot be retrieved.
    pub async fn get_container_logs(&self, container_id: &str, max_bytes: usize) -> String {
        let opts = LogsOptions::<String> {
            stdout: true,
            stderr: true,
            tail: "all".to_string(),
            ..Default::default()
        };

        let mut stream = self.docker.logs(container_id, Some(opts));
        let mut output = String::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(log) => {
                    let line = match log {
                        LogOutput::StdOut { message } | LogOutput::StdErr { message } => {
                            String::from_utf8_lossy(&message).into_owned()
                        }
                        _ => continue,
                    };
                    output.push_str(&line);
                }
                Err(_) => break,
            }
        }

        // Keep only the last max_bytes bytes.
        if output.len() > max_bytes {
            // Find a valid char boundary at or after the cut point.
            let start = output.len() - max_bytes;
            let start = output.ceil_char_boundary(start);
            output = output[start..].to_string();
        }
        output
    }

    /// Retrieve the stderr output from a container (last `tail` lines).
    ///
    /// Returns an empty string if logs cannot be retrieved (e.g. container
    /// already removed).
    pub async fn get_container_stderr(&self, container_id: &str, tail: &str) -> String {
        let opts = LogsOptions::<String> {
            stdout: false,
            stderr: true,
            tail: tail.to_string(),
            ..Default::default()
        };

        let mut stream = self.docker.logs(container_id, Some(opts));
        let mut output = String::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(log) => output.push_str(&log.to_string()),
                Err(_) => break,
            }
        }
        output
    }

    /// Stream stdout and stderr from a container in real-time.
    ///
    /// Spawns a background task that follows the container's log output and
    /// emits each line via `tracing::info!`, prefixed with the function name
    /// and request ID. The returned `JoinHandle` can be aborted to stop
    /// streaming (e.g. when the invocation completes).
    ///
    /// Log lines are emitted at INFO level so they respect the configured log
    /// verbosity. Stderr lines are tagged with `stream="stderr"` and stdout
    /// with `stream="stdout"`.
    pub fn stream_container_logs(
        &self,
        container_id: &str,
        function_name: &str,
        request_id: &str,
    ) -> tokio::task::JoinHandle<()> {
        let opts = LogsOptions::<String> {
            follow: true,
            stdout: true,
            stderr: true,
            since: 0,
            timestamps: false,
            tail: "0".to_string(),
            ..Default::default()
        };

        let mut stream = self.docker.logs(container_id, Some(opts));
        let function_name = function_name.to_string();
        let request_id = request_id.to_string();

        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(log) => {
                        let (stream_name, line) = match log {
                            LogOutput::StdOut { message } => {
                                ("stdout", String::from_utf8_lossy(&message).into_owned())
                            }
                            LogOutput::StdErr { message } => {
                                ("stderr", String::from_utf8_lossy(&message).into_owned())
                            }
                            _ => continue,
                        };
                        // Trim trailing newline to avoid double-spacing in log output.
                        let line = line.trim_end_matches('\n');
                        if !line.is_empty() {
                            info!(
                                function = %function_name,
                                request_id = %request_id,
                                stream = stream_name,
                                "{}",
                                line
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            function = %function_name,
                            request_id = %request_id,
                            error = %e,
                            "container log stream ended"
                        );
                        break;
                    }
                }
            }
        })
    }

    /// Inspect a container and return its exit code, if available.
    ///
    /// Returns `None` if the container is still running or cannot be inspected.
    pub async fn get_container_exit_code(&self, container_id: &str) -> Option<i64> {
        match self.docker.inspect_container(container_id, None).await {
            Ok(info) => info.state.and_then(|s| s.exit_code),
            Err(_) => None,
        }
    }

    /// Wait for a container to exit. Returns the exit status code.
    ///
    /// This blocks until the container process terminates. Used to detect
    /// bootstrap failures where the container exits before calling `/next`.
    pub async fn wait_for_exit(&self, container_id: &str) -> Option<i64> {
        let mut stream = self.docker.wait_container(
            container_id,
            None::<WaitContainerOptions<String>>,
        );
        match stream.next().await {
            Some(Ok(response)) => Some(response.status_code),
            _ => None,
        }
    }

    /// Remove all internal tracking entries for a function.
    ///
    /// Used after init errors where containers are killed via the registry.
    pub async fn deregister_by_function(&self, function_name: &str) {
        let mut containers = self.containers.write().await;
        let before = containers.len();
        containers.retain(|_, c| c.function_name != function_name);
        let removed = before - containers.len();
        // Release container slots for each removed container.
        for _ in 0..removed {
            self.release_container_slot();
        }
    }

    /// Mark a container as failed and remove it from the managed pool.
    ///
    /// Returns the function name if the container was found and marked,
    /// `None` if the container was not tracked.
    pub async fn mark_container_failed(&self, container_id: &str) -> Option<String> {
        let mut containers = self.containers.write().await;
        if let Some(entry) = containers.get_mut(container_id) {
            // Skip containers already in Stopping or Failed state
            if entry.state == ContainerState::Stopping || entry.state == ContainerState::Failed {
                return None;
            }
            let function_name = entry.function_name.clone();
            // Remove from tracking so it's not claimed again
            containers.remove(container_id);
            // Release the container slot so new containers can be created.
            self.release_container_slot();
            Some(function_name)
        } else {
            None
        }
    }

    /// Insert a container directly into tracking (for testing).
    ///
    /// Also consumes a container slot to keep the semaphore consistent.
    #[doc(hidden)]
    pub async fn insert_test_container(
        &self,
        container_id: String,
        function_name: String,
        state: ContainerState,
    ) {
        // Consume a semaphore permit to keep slot accounting consistent.
        let permit = self.container_semaphore.clone().acquire_owned().await.unwrap();
        permit.forget();

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

/// Monitor Docker events for container die/stop events and handle crashes.
///
/// Subscribes to the Docker event stream filtered to containers with the
/// `managed-by=localfunctions` label. When a container dies or is killed
/// unexpectedly:
/// 1. Logs the crash at WARN level with container ID and function name
/// 2. Marks the container as Failed and removes it from the pool
/// 3. Fails any in-flight invocations with a 502-compatible ServiceException
/// 4. Cleans up the Docker container (stop + force-remove)
/// 5. Deregisters from the container registry
///
/// Runs until the shutdown signal fires.
pub async fn monitor_container_events(
    docker: Docker,
    container_manager: Arc<ContainerManager>,
    container_registry: Arc<ContainerRegistry>,
    runtime_bridge: Arc<RuntimeBridge>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut filters = HashMap::new();
    filters.insert("type".to_string(), vec!["container".to_string()]);
    filters.insert(
        "event".to_string(),
        vec!["die".to_string(), "oom".to_string()],
    );
    filters.insert(
        "label".to_string(),
        vec!["managed-by=localfunctions".to_string()],
    );

    let options = EventsOptions {
        filters,
        ..Default::default()
    };

    let mut event_stream = docker.events(Some(options));

    loop {
        tokio::select! {
            event = event_stream.next() => {
                match event {
                    Some(Ok(msg)) => {
                        handle_container_event(
                            &msg,
                            &container_manager,
                            &container_registry,
                            &runtime_bridge,
                        ).await;
                    }
                    Some(Err(e)) => {
                        error!(%e, "Docker event stream error");
                        // Brief pause before retrying to avoid tight error loops
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    None => {
                        warn!("Docker event stream ended unexpectedly");
                        break;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                info!("container event monitor shutting down");
                break;
            }
        }
    }
}

/// Handle a single Docker container event (die/oom).
async fn handle_container_event(
    msg: &bollard::models::EventMessage,
    container_manager: &Arc<ContainerManager>,
    container_registry: &Arc<ContainerRegistry>,
    runtime_bridge: &Arc<RuntimeBridge>,
) {
    let action = match &msg.action {
        Some(a) => a.clone(),
        None => return,
    };

    let actor = match &msg.actor {
        Some(a) => a,
        None => return,
    };

    let container_id = match &actor.id {
        Some(id) => id.clone(),
        None => return,
    };

    let function_name = actor
        .attributes
        .as_ref()
        .and_then(|attrs| attrs.get("localfunctions.function").cloned())
        .unwrap_or_else(|| "unknown".to_string());

    // Mark container as failed and remove from pool.
    // If mark_container_failed returns None, the container was already
    // stopping/failed or not tracked — skip further processing.
    let tracked_function = container_manager.mark_container_failed(&container_id).await;
    if tracked_function.is_none() {
        return;
    }

    let exit_code = actor
        .attributes
        .as_ref()
        .and_then(|attrs| attrs.get("exitCode"))
        .and_then(|c| c.parse::<i64>().ok());

    warn!(
        container_id = %container_id,
        function = %function_name,
        action = %action,
        exit_code = ?exit_code,
        "container crashed"
    );

    // Fail any in-flight invocations assigned to this container.
    let failed_count = runtime_bridge
        .fail_container_invocations(&container_id)
        .await;
    if failed_count > 0 {
        warn!(
            container_id = %container_id,
            function = %function_name,
            failed_count,
            "failed in-flight invocations due to container crash"
        );
    }

    // Clean up the Docker container (force-remove).
    container_registry.stop_and_remove(&container_id, Duration::from_secs(2)).await;
}

/// Merge multiple layer directories into a single temporary directory.
///
/// Files from each layer are copied in order so that later layers take
/// precedence for conflicting paths, matching AWS Lambda behaviour.
/// The merged directory is placed under the system temp dir and must be
/// cleaned up by the caller (or on container removal).
fn merge_layers(function_name: &str, layers: &[PathBuf]) -> Result<PathBuf, ServiceError> {
    let merged_dir = std::env::temp_dir().join(format!("localfunctions-layers-{}-{}", function_name, &uuid::Uuid::new_v4().to_string()[..8]));
    std::fs::create_dir_all(&merged_dir).map_err(|e| {
        ServiceError::ServiceException(format!(
            "failed to create merged layers directory: {}", e
        ))
    })?;

    for layer_path in layers {
        copy_dir_recursive(layer_path, &merged_dir).map_err(|e| {
            ServiceError::ServiceException(format!(
                "failed to merge layer '{}': {}", layer_path.display(), e
            ))
        })?;
    }

    debug!(
        function = %function_name,
        merged_dir = %merged_dir.display(),
        layer_count = layers.len(),
        "merged layers into temp directory"
    );

    Ok(merged_dir)
}

/// Recursively copy the contents of `src` into `dst`, overwriting existing files.
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            std::fs::create_dir_all(&dst_path)?;
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
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
/// Priority order (highest to lowest):
/// 1. System variables (AWS_LAMBDA_FUNCTION_NAME, _HANDLER, etc.)
/// 2. Per-function environment variables from functions.json
/// 3. Forwarded host AWS credentials (when enabled)
///
/// Per-function environment variable overrides take precedence over forwarded
/// host credentials, allowing functions to use their own credentials.
pub fn lambda_env_vars(
    function: &FunctionConfig,
    runtime_port: u16,
    region: &str,
    credential_config: &CredentialForwardingConfig,
) -> Vec<String> {
    let runtime_api = runtime_api_endpoint(runtime_port);

    // System variables required by the AWS Lambda execution environment
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert(
        "AWS_LAMBDA_FUNCTION_NAME".into(),
        function.name.clone(),
    );
    // For image_uri functions, only set _HANDLER if the user explicitly provided one.
    // This allows the container's ENTRYPOINT/CMD to be respected as-is.
    if function.image_uri.is_none() || !function.handler.is_empty() {
        vars.insert("_HANDLER".into(), function.handler.clone());
    }
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

    // Set an initial _X_AMZN_TRACE_ID so the variable exists from container
    // start.  Per-invocation trace IDs are delivered via the
    // Lambda-Runtime-Trace-Id header on /next, and the Lambda runtime client
    // updates this env var automatically for each invocation.
    vars.insert(
        "_X_AMZN_TRACE_ID".into(),
        "Root=1-00000000-000000000000000000000000;Parent=0000000000000000;Sampled=0".into(),
    );

    // Merge user-configured environment variables without overriding system vars.
    // User vars are inserted before host credentials so they take precedence
    // over forwarded credentials.
    for (key, value) in &function.environment {
        vars.entry(key.clone()).or_insert_with(|| value.clone());
    }

    // Forward host AWS credentials when enabled. These are inserted with
    // or_insert so per-function overrides (already in vars) take precedence.
    if credential_config.forward_env {
        for key in &[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            if let Ok(val) = std::env::var(key) {
                vars.entry((*key).to_string())
                    .or_insert(val);
            }
        }
    }

    // When mounting ~/.aws, forward AWS_PROFILE so the SDK inside the
    // container uses the same profile as the host.
    if credential_config.mount_aws_dir {
        if let Ok(val) = std::env::var("AWS_PROFILE") {
            vars.entry("AWS_PROFILE".to_string()).or_insert(val);
        }
    }

    // Convert to Docker's "KEY=VALUE" format
    vars.into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect()
}

/// Return the host ~/.aws directory path if it exists.
fn host_aws_config_dir() -> Option<String> {
    std::env::var("HOME")
        .ok()
        .map(|h| std::path::PathBuf::from(h).join(".aws"))
        .filter(|p| p.is_dir())
        .and_then(|p| p.to_str().map(String::from))
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

/// Compute Docker CPU constraints proportional to the function's memory size.
///
/// AWS Lambda allocates CPU power proportionally to configured memory. At
/// **1769 MB** a function receives the equivalent of **1 full vCPU**. This
/// function mirrors that mapping using Docker's `cpu_period` / `cpu_quota` /
/// `cpu_shares` settings.
///
/// Returns `(cpu_period, cpu_quota, cpu_shares)` where:
/// - `cpu_period` is fixed at 100 000 µs (Docker default).
/// - `cpu_quota` is `memory_size / 1769 * cpu_period`, clamped to a minimum of
///   2 000 µs (~2 % of a CPU) so that very-low-memory functions are not
///   completely starved.
/// - `cpu_shares` scales from the Docker baseline of 1024 at 1 vCPU, with a
///   minimum of 2 (Docker minimum).
pub fn cpu_constraints(memory_size_mb: u64) -> (i64, i64, i64) {
    const CPU_PERIOD: i64 = 100_000; // microseconds
    const FULL_VCPU_MB: f64 = 1769.0;
    const MIN_QUOTA: f64 = 2_000.0;
    const MIN_SHARES: f64 = 2.0;

    let vcpu_fraction = memory_size_mb as f64 / FULL_VCPU_MB;
    let cpu_quota = (vcpu_fraction * CPU_PERIOD as f64).round().max(MIN_QUOTA) as i64;
    let cpu_shares = (vcpu_fraction * 1024.0).round().max(MIN_SHARES) as i64;

    (CPU_PERIOD, cpu_quota, cpu_shares)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

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
            ephemeral_storage_mb: 512,
            environment: HashMap::new(),
            image: None,
            image_uri: None,
            reserved_concurrent_executions: None,
            architecture: "x86_64".into(),
            layers: vec![],
        }
    }

    #[test]
    fn lambda_env_vars_contains_function_name() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_NAME=my-func".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_handler() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"_HANDLER=main.handler".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_memory_size() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_MEMORY_SIZE=256".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_runtime_api() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_LAMBDA_RUNTIME_API=host.docker.internal:9601".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_region() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "eu-west-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_REGION=eu-west-1".to_string()));
        assert!(vars.contains(&"AWS_DEFAULT_REGION=eu-west-1".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_version() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_VERSION=$LATEST".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_log_group() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_LAMBDA_LOG_GROUP_NAME=/aws/lambda/my-func".to_string()));
    }

    #[test]
    fn lambda_env_vars_contains_log_stream() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_LAMBDA_LOG_STREAM_NAME=localfunctions/latest".to_string()));
    }

    #[test]
    fn lambda_env_vars_includes_user_vars() {
        let mut func = make_test_function();
        func.environment.insert("TABLE_NAME".into(), "my-table".into());
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"TABLE_NAME=my-table".to_string()));
    }

    #[test]
    fn lambda_env_vars_user_vars_do_not_override_system_vars() {
        let mut func = make_test_function();
        func.environment.insert("AWS_LAMBDA_FUNCTION_NAME".into(), "hacked".into());
        func.environment.insert("_HANDLER".into(), "evil.handler".into());
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        // System values must win
        assert!(vars.contains(&"AWS_LAMBDA_FUNCTION_NAME=my-func".to_string()));
        assert!(vars.contains(&"_HANDLER=main.handler".to_string()));
        assert!(!vars.iter().any(|v| v.contains("hacked")));
    }

    #[test]
    fn lambda_env_vars_custom_port() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 3000, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"AWS_LAMBDA_RUNTIME_API=host.docker.internal:3000".to_string()));
    }

    #[test]
    #[serial]
    fn lambda_env_vars_count() {
        // 10 system vars with no user vars (and no host credentials)
        // (includes _X_AMZN_TRACE_ID placeholder)
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_SESSION_TOKEN");
        let func = make_test_function();
        let no_creds = CredentialForwardingConfig {
            forward_env: false,
            mount_aws_dir: false,
        };
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &no_creds);
        assert_eq!(vars.len(), 10);
    }

    #[test]
    fn lambda_env_vars_contains_trace_id_placeholder() {
        let func = make_test_function();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        let trace_var = vars.iter().find(|v| v.starts_with("_X_AMZN_TRACE_ID="));
        assert!(trace_var.is_some(), "expected _X_AMZN_TRACE_ID env var");
        let value = trace_var.unwrap().strip_prefix("_X_AMZN_TRACE_ID=").unwrap();
        assert!(value.starts_with("Root=1-"), "trace placeholder: {}", value);
        assert!(value.contains(";Parent="), "trace placeholder: {}", value);
        assert!(value.contains(";Sampled="), "trace placeholder: {}", value);
    }

    #[test]
    fn lambda_env_vars_image_uri_without_handler_skips_handler() {
        let mut func = make_test_function();
        func.image_uri = Some("my-lambda:latest".into());
        func.handler = String::new();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(!vars.iter().any(|v| v.starts_with("_HANDLER=")));
    }

    #[test]
    fn lambda_env_vars_image_uri_with_handler_sets_handler() {
        let mut func = make_test_function();
        func.image_uri = Some("my-lambda:latest".into());
        func.handler = "app.handler".into();
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(&"_HANDLER=app.handler".to_string()));
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
            20,
            CredentialForwardingConfig::default(),
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
    fn resolve_image_from_image_uri() {
        let mgr = make_container_manager();
        let mut func = make_test_function();
        func.image_uri = Some("my-lambda:latest".into());
        let image = mgr.resolve_image(&func).unwrap();
        assert_eq!(image, "my-lambda:latest");
    }

    #[test]
    fn resolve_image_image_uri_takes_priority_over_image() {
        let mgr = make_container_manager();
        let mut func = make_test_function();
        func.image = Some("custom-base:v1".into());
        func.image_uri = Some("my-lambda:latest".into());
        let image = mgr.resolve_image(&func).unwrap();
        assert_eq!(image, "my-lambda:latest");
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
            20,
            CredentialForwardingConfig::default(),
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

    // -- count_active_by_function -------------------------------------------

    #[tokio::test]
    async fn count_active_by_function_empty() {
        let mgr = make_container_manager();
        assert_eq!(mgr.count_active_by_function("test-func").await, 0);
    }

    #[tokio::test]
    async fn count_active_by_function_counts_busy_and_starting() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Busy)
            .await;
        mgr.insert_test_container("ctr-2".into(), "test-func".into(), ContainerState::Starting)
            .await;
        mgr.insert_test_container("ctr-3".into(), "test-func".into(), ContainerState::Idle)
            .await;
        mgr.insert_test_container("ctr-4".into(), "other-func".into(), ContainerState::Busy)
            .await;
        assert_eq!(mgr.count_active_by_function("test-func").await, 2);
    }

    #[tokio::test]
    async fn count_active_by_function_excludes_stopping_and_failed() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Stopping)
            .await;
        mgr.insert_test_container("ctr-2".into(), "test-func".into(), ContainerState::Failed)
            .await;
        mgr.insert_test_container("ctr-3".into(), "test-func".into(), ContainerState::Busy)
            .await;
        assert_eq!(mgr.count_active_by_function("test-func").await, 1);
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

    // -- mark_container_failed -------------------------------------------------

    #[tokio::test]
    async fn mark_container_failed_returns_function_name() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Busy)
            .await;

        let result = mgr.mark_container_failed("ctr-1").await;
        assert_eq!(result, Some("test-func".to_string()));

        // Container should be removed from tracking
        assert!(mgr.get_state("ctr-1").await.is_none());
    }

    #[tokio::test]
    async fn mark_container_failed_skips_stopping_container() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Stopping)
            .await;

        let result = mgr.mark_container_failed("ctr-1").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn mark_container_failed_returns_none_for_unknown() {
        let mgr = make_container_manager();
        let result = mgr.mark_container_failed("nonexistent").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn mark_container_failed_removes_from_pool() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Idle)
            .await;

        mgr.mark_container_failed("ctr-1").await;

        // Should not be claimable
        assert!(mgr.claim_idle_container("test-func").await.is_none());
        assert_eq!(mgr.count().await, 0);
    }

    #[tokio::test]
    async fn mark_container_failed_does_not_affect_other_containers() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-1".into(), "test-func".into(), ContainerState::Busy)
            .await;
        mgr.insert_test_container("ctr-2".into(), "test-func".into(), ContainerState::Idle)
            .await;

        mgr.mark_container_failed("ctr-1").await;

        // ctr-2 should still be claimable
        assert_eq!(
            mgr.claim_idle_container("test-func").await,
            Some("ctr-2".to_string())
        );
    }

    // -- handle_container_event ------------------------------------------------

    #[tokio::test]
    async fn handle_container_event_processes_die_event() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-abc".into(), "my-func".into(), ContainerState::Busy)
            .await;

        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let bridge = Arc::new(RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx));

        let mut attributes = HashMap::new();
        attributes.insert("localfunctions.function".to_string(), "my-func".to_string());
        attributes.insert("exitCode".to_string(), "1".to_string());

        let msg = bollard::models::EventMessage {
            action: Some("die".to_string()),
            actor: Some(bollard::models::EventActor {
                id: Some("ctr-abc".to_string()),
                attributes: Some(attributes),
            }),
            ..Default::default()
        };

        handle_container_event(&msg, &Arc::new(mgr), &registry, &bridge).await;

        // Container should be removed from tracking (mark_container_failed removes it)
        // We can't check registry since ctr-abc was never a real Docker container,
        // but we can verify the manager no longer tracks it.
    }

    #[tokio::test]
    async fn handle_container_event_ignores_already_stopping() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-abc".into(), "my-func".into(), ContainerState::Stopping)
            .await;

        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let bridge = Arc::new(RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx));

        let mut attributes = HashMap::new();
        attributes.insert("localfunctions.function".to_string(), "my-func".to_string());

        let msg = bollard::models::EventMessage {
            action: Some("die".to_string()),
            actor: Some(bollard::models::EventActor {
                id: Some("ctr-abc".to_string()),
                attributes: Some(attributes),
            }),
            ..Default::default()
        };

        handle_container_event(&msg, &Arc::new(mgr), &registry, &bridge).await;

        // Should not have processed (Stopping state is skipped)
    }

    #[tokio::test]
    async fn handle_container_event_fails_inflight_invocations() {
        let mgr = make_container_manager();
        mgr.insert_test_container("ctr-abc".into(), "my-func".into(), ContainerState::Busy)
            .await;

        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let bridge = Arc::new(RuntimeBridge::new(HashMap::new(), HashMap::new(), shutdown_rx));

        // Store a pending invocation for this container
        let (tx, rx) = tokio::sync::oneshot::channel();
        let request_id = uuid::Uuid::new_v4();
        bridge
            .store_pending(request_id, "my-func".into(), Some("ctr-abc".into()), tx)
            .await;

        let mut attributes = HashMap::new();
        attributes.insert("localfunctions.function".to_string(), "my-func".to_string());

        let msg = bollard::models::EventMessage {
            action: Some("die".to_string()),
            actor: Some(bollard::models::EventActor {
                id: Some("ctr-abc".to_string()),
                attributes: Some(attributes),
            }),
            ..Default::default()
        };

        handle_container_event(&msg, &Arc::new(mgr), &registry, &bridge).await;

        // The sender is dropped on crash, so the receiver gets RecvError.
        // This mirrors the invoke handler's Ok(Err(_)) branch → 502 BAD_GATEWAY.
        assert!(rx.await.is_err());
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
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &CredentialForwardingConfig::default());
        assert!(vars.contains(
            &"AWS_ENDPOINT_URL_S3=http://host.docker.internal:9090".to_string()
        ));
        assert!(vars.contains(
            &"AWS_ENDPOINT_URL_SECRETS_MANAGER=http://host.docker.internal:9091".to_string()
        ));
    }

    // -- Credential forwarding tests ----------------------------------------

    #[test]
    #[serial]
    fn credential_forwarding_injects_host_aws_env_vars() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        std::env::set_var("AWS_SESSION_TOKEN", "FwoGZXIvYXdzEBY_session_token");

        let func = make_test_function();
        let config = CredentialForwardingConfig {
            forward_env: true,
            mount_aws_dir: false,
        };
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &config);

        assert!(vars.contains(&"AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE".to_string()));
        assert!(vars.contains(&"AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()));
        assert!(vars.contains(&"AWS_SESSION_TOKEN=FwoGZXIvYXdzEBY_session_token".to_string()));

        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_SESSION_TOKEN");
    }

    #[test]
    #[serial]
    fn credential_forwarding_disabled_does_not_inject() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "secret");

        let func = make_test_function();
        let config = CredentialForwardingConfig {
            forward_env: false,
            mount_aws_dir: false,
        };
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &config);

        assert!(!vars.iter().any(|v| v.starts_with("AWS_ACCESS_KEY_ID=")));
        assert!(!vars.iter().any(|v| v.starts_with("AWS_SECRET_ACCESS_KEY=")));

        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    #[test]
    #[serial]
    fn credential_forwarding_skips_unset_session_token() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIATEST");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "secret");
        std::env::remove_var("AWS_SESSION_TOKEN");

        let func = make_test_function();
        let config = CredentialForwardingConfig {
            forward_env: true,
            mount_aws_dir: false,
        };
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &config);

        assert!(vars.contains(&"AWS_ACCESS_KEY_ID=AKIATEST".to_string()));
        assert!(vars.contains(&"AWS_SECRET_ACCESS_KEY=secret".to_string()));
        assert!(!vars.iter().any(|v| v.starts_with("AWS_SESSION_TOKEN=")));

        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    #[test]
    #[serial]
    fn per_function_env_overrides_forwarded_credentials() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "HOST_KEY");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "HOST_SECRET");

        let mut func = make_test_function();
        func.environment.insert("AWS_ACCESS_KEY_ID".into(), "FUNC_KEY".into());
        func.environment.insert("AWS_SECRET_ACCESS_KEY".into(), "FUNC_SECRET".into());

        let config = CredentialForwardingConfig {
            forward_env: true,
            mount_aws_dir: false,
        };
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &config);

        // Per-function values must win over host credentials
        assert!(vars.contains(&"AWS_ACCESS_KEY_ID=FUNC_KEY".to_string()));
        assert!(vars.contains(&"AWS_SECRET_ACCESS_KEY=FUNC_SECRET".to_string()));
        assert!(!vars.iter().any(|v| v.contains("HOST_KEY")));
        assert!(!vars.iter().any(|v| v.contains("HOST_SECRET")));

        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    #[test]
    #[serial]
    fn aws_profile_forwarded_when_mount_enabled() {
        std::env::set_var("AWS_PROFILE", "my-dev-profile");

        let func = make_test_function();
        let config = CredentialForwardingConfig {
            forward_env: false,
            mount_aws_dir: true,
        };
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &config);

        assert!(vars.contains(&"AWS_PROFILE=my-dev-profile".to_string()));

        std::env::remove_var("AWS_PROFILE");
    }

    #[test]
    #[serial]
    fn aws_profile_not_forwarded_when_mount_disabled() {
        std::env::set_var("AWS_PROFILE", "my-dev-profile");

        let func = make_test_function();
        let config = CredentialForwardingConfig {
            forward_env: true,
            mount_aws_dir: false,
        };
        let vars = lambda_env_vars(&func, 9601, "us-east-1", &config);

        assert!(!vars.iter().any(|v| v.starts_with("AWS_PROFILE=")));

        std::env::remove_var("AWS_PROFILE");
    }

    #[test]
    fn credential_forwarding_config_default() {
        let config = CredentialForwardingConfig::default();
        assert!(config.forward_env);
        assert!(!config.mount_aws_dir);
    }

    #[test]
    #[serial]
    fn credentials_not_exposed_in_debug_output() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/secret");

        let config = CredentialForwardingConfig {
            forward_env: true,
            mount_aws_dir: false,
        };
        // The config struct itself should not contain credentials
        let debug_str = format!("{:?}", config);
        assert!(!debug_str.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug_str.contains("wJalrXUtnFEMI/secret"));

        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    // -- Container slot / semaphore tests -----------------------------------

    #[tokio::test]
    async fn container_slots_default_available() {
        let mgr = make_container_manager();
        assert_eq!(mgr.available_container_slots(), 20);
    }

    #[tokio::test]
    async fn acquire_container_slot_succeeds_when_available() {
        let mgr = make_container_manager();
        assert!(mgr.acquire_container_slot(Duration::from_millis(10)).await);
        assert_eq!(mgr.available_container_slots(), 19);
    }

    #[tokio::test]
    async fn release_container_slot_restores_permit() {
        let mgr = make_container_manager();
        assert!(mgr.acquire_container_slot(Duration::from_millis(10)).await);
        assert_eq!(mgr.available_container_slots(), 19);
        mgr.release_container_slot();
        assert_eq!(mgr.available_container_slots(), 20);
    }

    #[tokio::test]
    async fn acquire_slot_times_out_when_all_taken() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let mgr = ContainerManager::new(
            docker,
            HashMap::new(),
            "test".into(),
            9601,
            "us-east-1".into(),
            registry,
            2, // only 2 slots
            CredentialForwardingConfig::default(),
        );

        // Take both slots
        assert!(mgr.acquire_container_slot(Duration::from_millis(10)).await);
        assert!(mgr.acquire_container_slot(Duration::from_millis(10)).await);
        assert_eq!(mgr.available_container_slots(), 0);

        // Third should timeout
        assert!(!mgr.acquire_container_slot(Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn insert_test_container_consumes_slot() {
        let mgr = make_container_manager();
        let initial = mgr.available_container_slots();
        mgr.insert_test_container(
            "c1".into(),
            "func-a".into(),
            ContainerState::Idle,
        )
        .await;
        assert_eq!(mgr.available_container_slots(), initial - 1);
    }

    #[tokio::test]
    async fn mark_container_failed_releases_slot() {
        let mgr = make_container_manager();
        let initial = mgr.available_container_slots();
        mgr.insert_test_container(
            "c1".into(),
            "func-a".into(),
            ContainerState::Busy,
        )
        .await;
        assert_eq!(mgr.available_container_slots(), initial - 1);

        mgr.mark_container_failed("c1").await;
        assert_eq!(mgr.available_container_slots(), initial);
    }

    #[tokio::test]
    async fn deregister_by_function_releases_slots() {
        let mgr = make_container_manager();
        let initial = mgr.available_container_slots();
        mgr.insert_test_container("c1".into(), "func-a".into(), ContainerState::Idle).await;
        mgr.insert_test_container("c2".into(), "func-a".into(), ContainerState::Busy).await;
        mgr.insert_test_container("c3".into(), "func-b".into(), ContainerState::Idle).await;
        assert_eq!(mgr.available_container_slots(), initial - 3);

        // Deregister func-a (2 containers)
        mgr.deregister_by_function("func-a").await;
        assert_eq!(mgr.available_container_slots(), initial - 1);
        assert_eq!(mgr.count().await, 1);
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
            20,
            CredentialForwardingConfig::default(),
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
            ephemeral_storage_mb: 512,
            environment: HashMap::from([("MY_VAR".into(), "my_value".into())]),
            image: None,
            image_uri: None,
            reserved_concurrent_executions: None,
            architecture: "x86_64".into(),
            layers: vec![],
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

        // Verify tmpfs mount for /tmp with size limit
        let tmpfs = host_config.tmpfs.as_ref().unwrap();
        assert!(tmpfs.contains_key("/tmp"));
        assert_eq!(tmpfs["/tmp"], format!("size={}", 512 * 1024 * 1024));

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
            20,
            CredentialForwardingConfig::default(),
        );

        // Use a small, commonly available image
        // This test verifies that ensure_image does not fail for a valid image
        mgr.ensure_image("public.ecr.aws/lambda/python:3.12", None)
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
            20,
            CredentialForwardingConfig::default(),
        );

        // Stopping a nonexistent container should succeed (benign errors are swallowed)
        let result = mgr
            .stop_and_remove("nonexistent-container-id", Duration::from_secs(1))
            .await;
        assert!(result.is_ok());
    }

    // -- stream_container_logs ----------------------------------------------

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn stream_container_logs_nonexistent_container_completes_gracefully() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let mgr = ContainerManager::new(
            docker,
            HashMap::new(),
            "test".into(),
            9601,
            "us-east-1".into(),
            registry,
            20,
            CredentialForwardingConfig::default(),
        );

        // Streaming from a nonexistent container should complete quickly without panicking.
        let request_id = uuid::Uuid::new_v4().to_string();
        let handle = mgr.stream_container_logs(
            "nonexistent-container-id",
            "test-func",
            &request_id,
        );

        // The task should finish on its own (stream error → break).
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            handle,
        )
        .await;
        assert!(result.is_ok(), "stream task should complete when container doesn't exist");
    }

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn stream_container_logs_can_be_aborted() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let registry = Arc::new(ContainerRegistry::new(docker.clone()));
        let mgr = ContainerManager::new(
            docker,
            HashMap::new(),
            "test".into(),
            9601,
            "us-east-1".into(),
            registry,
            20,
            CredentialForwardingConfig::default(),
        );

        let request_id = uuid::Uuid::new_v4().to_string();
        let handle = mgr.stream_container_logs(
            "nonexistent-container-id",
            "test-func",
            &request_id,
        );

        // Aborting should not panic.
        handle.abort();
    }

    // -- cpu_constraints ---------------------------------------------------

    #[test]
    fn cpu_constraints_1769mb_yields_one_vcpu() {
        let (period, quota, shares) = cpu_constraints(1769);
        assert_eq!(period, 100_000);
        assert_eq!(quota, 100_000); // exactly 1 vCPU
        assert_eq!(shares, 1024);
    }

    #[test]
    fn cpu_constraints_128mb_default() {
        let (period, quota, shares) = cpu_constraints(128);
        assert_eq!(period, 100_000);
        // 128 / 1769 ≈ 0.0724 → quota ≈ 7235
        assert!(quota > 2_000 && quota < 10_000, "quota was {quota}");
        assert!(shares > 2 && shares < 100, "shares was {shares}");
    }

    #[test]
    fn cpu_constraints_3008mb_multi_vcpu() {
        let (_period, quota, shares) = cpu_constraints(3008);
        // 3008 / 1769 ≈ 1.70 → quota ≈ 170_040
        assert!(quota > 100_000, "quota was {quota}");
        assert!(shares > 1024, "shares was {shares}");
    }

    #[test]
    fn cpu_constraints_very_small_memory_hits_minimum() {
        let (_period, quota, shares) = cpu_constraints(1);
        // 1 / 1769 ≈ 0.00057 → raw quota ≈ 57 < 2000 → clamped
        assert_eq!(quota, 2_000);
        assert_eq!(shares, 2);
    }

    #[test]
    fn cpu_constraints_10240mb_max_lambda() {
        let (_period, quota, shares) = cpu_constraints(10240);
        // 10240 / 1769 ≈ 5.79 → ~6 vCPUs
        assert!(quota > 500_000, "quota was {quota}");
        assert!(shares > 5000, "shares was {shares}");
    }

    // -- Layer merging --------------------------------------------------------

    #[test]
    fn merge_layers_single_layer() {
        let tmp = tempfile::tempdir().unwrap();
        let layer1 = tmp.path().join("layer1");
        std::fs::create_dir_all(layer1.join("python")).unwrap();
        std::fs::write(layer1.join("python/utils.py"), "# utils").unwrap();

        let merged = merge_layers("test-fn", &[layer1]).unwrap();
        assert!(merged.join("python/utils.py").exists());
        let content = std::fs::read_to_string(merged.join("python/utils.py")).unwrap();
        assert_eq!(content, "# utils");

        // Cleanup
        std::fs::remove_dir_all(&merged).ok();
    }

    #[test]
    fn merge_layers_later_layer_takes_precedence() {
        let tmp = tempfile::tempdir().unwrap();

        let layer1 = tmp.path().join("layer1");
        std::fs::create_dir_all(layer1.join("python")).unwrap();
        std::fs::write(layer1.join("python/utils.py"), "layer1 content").unwrap();
        std::fs::write(layer1.join("python/shared.py"), "shared from layer1").unwrap();

        let layer2 = tmp.path().join("layer2");
        std::fs::create_dir_all(layer2.join("python")).unwrap();
        std::fs::write(layer2.join("python/utils.py"), "layer2 content").unwrap();

        let merged = merge_layers("test-fn", &[layer1, layer2]).unwrap();

        // layer2 should overwrite layer1's utils.py
        let utils = std::fs::read_to_string(merged.join("python/utils.py")).unwrap();
        assert_eq!(utils, "layer2 content");

        // layer1's shared.py should still be present
        let shared = std::fs::read_to_string(merged.join("python/shared.py")).unwrap();
        assert_eq!(shared, "shared from layer1");

        // Cleanup
        std::fs::remove_dir_all(&merged).ok();
    }

    #[test]
    fn copy_dir_recursive_preserves_structure() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(src.join("a/b")).unwrap();
        std::fs::write(src.join("a/b/file.txt"), "deep").unwrap();
        std::fs::write(src.join("top.txt"), "top").unwrap();

        let dst = tmp.path().join("dst");
        std::fs::create_dir_all(&dst).unwrap();
        copy_dir_recursive(&src, &dst).unwrap();

        assert_eq!(std::fs::read_to_string(dst.join("a/b/file.txt")).unwrap(), "deep");
        assert_eq!(std::fs::read_to_string(dst.join("top.txt")).unwrap(), "top");
    }
}
