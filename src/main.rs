mod api;
mod config;
mod container;
mod function;
mod server;
mod types;

use anyhow::Result;
use bollard::Docker;
use tracing::{error, info};

use container::DockerNetwork;

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

    let network = DockerNetwork::new(docker, config.docker_network.clone());
    network.ensure_created().await.map_err(|e| {
        error!(%e, "Docker network setup failed");
        anyhow::anyhow!("{}", e)
    })?;

    info!(
        network = %config.docker_network,
        runtime_api = %container::runtime_api_endpoint(config.runtime_port),
        "Docker network ready"
    );

    // TODO: start Runtime API server, load functions, serve requests

    // Shutdown: remove the Docker network
    if let Err(e) = network.remove().await {
        error!(%e, "Failed to remove Docker network during shutdown");
    }

    Ok(())
}
