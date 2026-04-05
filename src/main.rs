mod api;
mod config;
mod container;
mod function;
mod server;
mod types;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bollard::Docker;
use tracing::{error, info};

use container::{ContainerRegistry, DockerNetwork};
use server::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = config::Config::from_env()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.log_level.clone().into()),
        )
        .init();

    info!(
        host = %config.host,
        port = config.port,
        runtime_port = config.runtime_port,
        "starting localfunctions"
    );

    let docker = Docker::connect_with_local_defaults()
        .map_err(|e| anyhow::anyhow!("Failed to connect to Docker: {}", e))?;

    let network = DockerNetwork::new(docker.clone(), config.docker_network.clone());
    network.ensure_created().await.map_err(|e| {
        error!(%e, "Docker network setup failed");
        anyhow::anyhow!("{}", e)
    })?;

    info!(
        network = %config.docker_network,
        runtime_api = %container::runtime_api_endpoint(config.runtime_port),
        "Docker network ready"
    );

    // Load function definitions
    let functions_config = function::load_functions_config(
        &config.functions_file,
        &std::env::current_dir()?,
    )
    .unwrap_or_else(|e| {
        tracing::warn!(%e, "Failed to load functions config, starting with empty config");
        function::FunctionsConfig {
            functions: Default::default(),
            runtime_images: Default::default(),
        }
    });

    info!(count = functions_config.functions.len(), "functions loaded");

    let shutdown_timeout = Duration::from_secs(config.shutdown_timeout);
    let container_registry = Arc::new(ContainerRegistry::new(docker.clone()));

    let state = AppState {
        config: Arc::new(config),
        docker,
        functions: Arc::new(functions_config),
        container_registry: container_registry.clone(),
        shutting_down: Arc::new(AtomicBool::new(false)),
    };

    // Start both API servers (blocks until shutdown signal)
    server::start(state).await?;

    // --- Graceful shutdown sequence ---

    info!("graceful shutdown started");

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
