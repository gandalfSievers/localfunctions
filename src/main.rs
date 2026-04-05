mod api;
mod config;
mod container;
mod function;
mod server;
mod types;

use std::sync::Arc;

use anyhow::Result;
use bollard::Docker;
use tracing::{error, info};

use container::DockerNetwork;
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

    let state = AppState {
        config: Arc::new(config),
        docker,
        functions: Arc::new(functions_config),
    };

    // Start both API servers (blocks until shutdown signal)
    server::start(state.clone()).await?;

    // Shutdown: remove the Docker network
    if let Err(e) = network.remove().await {
        error!(%e, "Failed to remove Docker network during shutdown");
    }

    Ok(())
}
