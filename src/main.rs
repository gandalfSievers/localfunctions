mod api;
mod config;
mod container;
mod extensions;
mod function;
mod metrics;
mod runtime;
mod server;
mod types;
mod watcher;

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bollard::Docker;
use futures_util::TryStreamExt;
use tracing::{error, info, warn};

use container::{ContainerManager, ContainerRegistry, CredentialForwardingConfig, DockerNetwork, monitor_container_events};
use runtime::RuntimeBridge;
use server::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = config::Config::from_env()?;

    let env_filter = tracing_subscriber::EnvFilter::new(&config.log_level);

    match config.log_format {
        config::LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .with_target(true)
                .init();
        }
        config::LogFormat::Text => {
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .init();
        }
    }

    info!(
        host = %config.host,
        port = config.port,
        runtime_port = config.runtime_port,
        "starting localfunctions"
    );

    let docker = Docker::connect_with_local_defaults()
        .map_err(|e| anyhow::anyhow!("Failed to connect to Docker: {}", e))?;

    // Clean up any orphan containers from previous runs before proceeding.
    let orphan_registry = ContainerRegistry::new(docker.clone());
    orphan_registry.cleanup_orphans().await;

    let network = DockerNetwork::new(docker.clone(), config.docker_network.clone());
    network.ensure_created().await.map_err(|e| {
        error!(%e, "Docker network setup failed");
        anyhow::anyhow!("{}", e)
    })?;

    info!(
        network = %config.docker_network,
        runtime_api = %format!("{{function}}.runtime.local:{}", config.runtime_port),
        "Docker network ready"
    );

    // Load and validate function definitions — fail fast if configuration is invalid
    let functions_config = function::load_functions_config(
        &config.functions_file,
        &std::env::current_dir()?,
    )
    .map_err(|e| {
        error!("{}", e);
        anyhow::anyhow!("Failed to load functions configuration")
    })?;

    info!(count = functions_config.functions.len(), "functions loaded");

    // Verify that all required Docker images are available locally.
    // If pull_images is enabled, missing images are pulled automatically.
    verify_runtime_images(&docker, &functions_config, config.pull_images).await?;

    // Create per-function invocation channels for the runtime bridge.
    let mut invocation_receivers = std::collections::HashMap::new();
    let mut invocation_senders = std::collections::HashMap::new();
    for name in functions_config.functions.keys() {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        invocation_senders.insert(name.clone(), tx);
        invocation_receivers.insert(name.clone(), rx);
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let reaper_shutdown_rx = shutdown_rx.clone();
    let event_monitor_shutdown_rx = shutdown_rx.clone();
    let watcher_shutdown_rx = shutdown_rx.clone();
    let extension_registry = Arc::new(extensions::ExtensionRegistry::new(shutdown_rx.clone()));
    let runtime_bridge = Arc::new(RuntimeBridge::new(invocation_senders, invocation_receivers, shutdown_rx));

    let shutdown_timeout = Duration::from_secs(config.shutdown_timeout);
    let container_idle_timeout = Duration::from_secs(config.container_idle_timeout);
    let container_registry = Arc::new(ContainerRegistry::new(docker.clone()));

    let credential_config = CredentialForwardingConfig {
        forward_env: config.forward_aws_credentials,
        mount_aws_dir: config.mount_aws_credentials,
    };
    let container_manager = Arc::new(ContainerManager::new(
        docker.clone(),
        functions_config.runtime_images.clone(),
        config.docker_network.clone(),
        config.runtime_port,
        config.region.clone(),
        container_registry.clone(),
        config.max_containers,
        credential_config,
    ));

    let state = AppState {
        config: Arc::new(config),
        docker,
        functions: Arc::new(functions_config),
        container_registry: container_registry.clone(),
        container_manager: container_manager.clone(),
        shutting_down: Arc::new(AtomicBool::new(false)),
        runtime_bridge,
        metrics: Arc::new(metrics::MetricsCollector::new()),
        extension_registry,
    };

    // Spawn background idle container reaper (30-second interval).
    {
        let reaper_manager = container_manager.clone();
        let mut shutdown_rx = reaper_shutdown_rx;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            // First tick completes immediately — skip it.
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        reaper_manager.reap_idle_containers(container_idle_timeout).await;
                    }
                    _ = shutdown_rx.changed() => {
                        info!("idle container reaper shutting down");
                        break;
                    }
                }
            }
        });
    }

    // Spawn Docker event monitor for container crash detection.
    {
        let event_docker = state.docker.clone();
        let event_manager = container_manager.clone();
        let event_registry = container_registry.clone();
        let event_bridge = state.runtime_bridge.clone();
        let event_shutdown_rx = event_monitor_shutdown_rx;
        tokio::spawn(async move {
            monitor_container_events(
                event_docker,
                event_manager,
                event_registry,
                event_bridge,
                event_shutdown_rx,
            )
            .await;
        });
    }

    // Spawn file watcher for hot reload (if enabled).
    if state.config.hot_reload {
        let watcher_functions = state.functions.clone();
        let watcher_registry = container_registry.clone();
        let debounce_ms = state.config.hot_reload_debounce_ms;
        tokio::spawn(async move {
            watcher::watch_code_paths(
                watcher_functions,
                watcher_registry,
                debounce_ms,
                watcher_shutdown_rx,
            )
            .await;
        });
    } else {
        info!("hot reload disabled");
    }

    // Start both API servers (blocks until shutdown signal)
    server::start(state).await?;

    // --- Graceful shutdown sequence ---

    info!("graceful shutdown started");

    // Wake any long-polling runtime handlers so they can exit cleanly.
    let _ = shutdown_tx.send(true);

    // Step 1: Wait for in-flight invocations to complete (up to timeout),
    //         then forcibly stop and remove all containers.
    info!(
        timeout_secs = shutdown_timeout.as_secs(),
        "waiting for in-flight invocations to complete"
    );
    container_registry.shutdown_all(shutdown_timeout).await;

    // Step 2: Remove the Docker network after all containers are gone.
    info!("removing Docker network");
    if let Err(e) = network.remove().await {
        error!(%e, "failed to remove Docker network during shutdown");
    }

    info!("shutdown complete");

    Ok(())
}

/// Collect all unique Docker images required by the configured functions and
/// verify that each one is available locally. When `pull_images` is true,
/// missing images are pulled automatically; otherwise the server fails to start
/// with a clear error naming the missing images.
async fn verify_runtime_images(
    docker: &Docker,
    functions_config: &function::FunctionsConfig,
    pull_images: bool,
) -> Result<()> {
    // Collect unique images: per-function custom images and runtime_images values.
    let mut images: HashSet<String> = HashSet::new();
    let mut image_to_functions: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for (name, func) in &functions_config.functions {
        let image = if let Some(ref img) = func.image_uri {
            img.clone()
        } else if let Some(ref img) = func.image {
            img.clone()
        } else if let Some(img) = functions_config.runtime_images.get(&func.runtime) {
            img.clone()
        } else {
            // This is already validated by load_functions_config, skip.
            continue;
        };
        images.insert(image.clone());
        image_to_functions
            .entry(image)
            .or_default()
            .push(name.clone());
    }

    if images.is_empty() {
        return Ok(());
    }

    let mut missing: Vec<(String, Vec<String>)> = Vec::new();

    for image in &images {
        match docker.inspect_image(image).await {
            Ok(_) => {
                info!(image = %image, "runtime image available");
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                if pull_images {
                    info!(image = %image, "pulling missing runtime image");
                    let opts = bollard::image::CreateImageOptions {
                        from_image: image.as_str(),
                        ..Default::default()
                    };
                    match docker
                        .create_image(Some(opts), None, None)
                        .try_collect::<Vec<_>>()
                        .await
                    {
                        Ok(_) => {
                            info!(image = %image, "runtime image pulled successfully");
                        }
                        Err(e) => {
                            let funcs = image_to_functions
                                .get(image)
                                .map(|v| v.join(", "))
                                .unwrap_or_default();
                            error!(
                                image = %image,
                                functions = %funcs,
                                error = %e,
                                "failed to pull runtime image"
                            );
                            missing.push((image.clone(), image_to_functions.get(image).cloned().unwrap_or_default()));
                        }
                    }
                } else {
                    let funcs = image_to_functions
                        .get(image)
                        .map(|v| v.join(", "))
                        .unwrap_or_default();
                    error!(
                        image = %image,
                        functions = %funcs,
                        "runtime image not found locally (set LOCAL_LAMBDA_PULL_IMAGES=true to pull automatically)"
                    );
                    missing.push((image.clone(), image_to_functions.get(image).cloned().unwrap_or_default()));
                }
            }
            Err(e) => {
                warn!(image = %image, error = %e, "failed to inspect runtime image");
                missing.push((image.clone(), image_to_functions.get(image).cloned().unwrap_or_default()));
            }
        }
    }

    if !missing.is_empty() {
        let details: Vec<String> = missing
            .iter()
            .map(|(img, funcs)| format!("  - {} (used by: {})", img, funcs.join(", ")))
            .collect();
        let msg = format!(
            "missing {} runtime image(s):\n{}",
            missing.len(),
            details.join("\n")
        );
        error!("{}", msg);
        return Err(anyhow::anyhow!("{}", msg));
    }

    Ok(())
}
