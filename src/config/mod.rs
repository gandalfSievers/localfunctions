use std::net::IpAddr;
use std::path::PathBuf;

use serde::Deserialize;

/// Service-level configuration loaded from environment variables.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    pub runtime_port: u16,
    pub region: String,
    pub account_id: String,
    pub functions_file: PathBuf,
    pub log_level: String,
    pub shutdown_timeout: u64,
    pub container_idle_timeout: u64,
    pub max_containers: usize,
    pub docker_network: String,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let host = parse_env("LOCAL_LAMBDA_HOST", "0.0.0.0")?;
        let port = parse_env("LOCAL_LAMBDA_PORT", "9600")?;
        let runtime_port = parse_env("LOCAL_LAMBDA_RUNTIME_PORT", "9601")?;
        let region = parse_env::<String>("LOCAL_LAMBDA_REGION", "us-east-1")?;
        let account_id = parse_env::<String>("LOCAL_LAMBDA_ACCOUNT_ID", "000000000000")?;
        let functions_file = parse_env::<PathBuf>("LOCAL_LAMBDA_FUNCTIONS_FILE", "./functions.json")?;
        let log_level = parse_env::<String>("LOCAL_LAMBDA_LOG_LEVEL", "info")?;
        let shutdown_timeout = parse_env("LOCAL_LAMBDA_SHUTDOWN_TIMEOUT", "30")?;
        let container_idle_timeout = parse_env("LOCAL_LAMBDA_CONTAINER_IDLE_TIMEOUT", "300")?;
        let max_containers = parse_env("LOCAL_LAMBDA_MAX_CONTAINERS", "20")?;
        let docker_network =
            parse_env::<String>("LOCAL_LAMBDA_DOCKER_NETWORK", "localfunctions")?;

        Ok(Config {
            host,
            port,
            runtime_port,
            region,
            account_id,
            functions_file,
            log_level,
            shutdown_timeout,
            container_idle_timeout,
            max_containers,
            docker_network,
        })
    }
}

fn parse_env<T>(key: &str, default: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = std::env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse::<T>()
        .map_err(|e| ConfigError::InvalidValue(key.to_string(), e.to_string()))
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid value for {0}: {1}")]
    InvalidValue(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        // Clear any env vars that might interfere
        for key in &[
            "LOCAL_LAMBDA_HOST",
            "LOCAL_LAMBDA_PORT",
            "LOCAL_LAMBDA_RUNTIME_PORT",
            "LOCAL_LAMBDA_REGION",
            "LOCAL_LAMBDA_ACCOUNT_ID",
            "LOCAL_LAMBDA_FUNCTIONS_FILE",
            "LOCAL_LAMBDA_LOG_LEVEL",
            "LOCAL_LAMBDA_SHUTDOWN_TIMEOUT",
            "LOCAL_LAMBDA_CONTAINER_IDLE_TIMEOUT",
            "LOCAL_LAMBDA_MAX_CONTAINERS",
            "LOCAL_LAMBDA_DOCKER_NETWORK",
        ] {
            std::env::remove_var(key);
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.host, "0.0.0.0".parse::<IpAddr>().unwrap());
        assert_eq!(config.port, 9600);
        assert_eq!(config.runtime_port, 9601);
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.account_id, "000000000000");
        assert_eq!(config.functions_file, PathBuf::from("./functions.json"));
        assert_eq!(config.log_level, "info");
        assert_eq!(config.shutdown_timeout, 30);
        assert_eq!(config.container_idle_timeout, 300);
        assert_eq!(config.max_containers, 20);
        assert_eq!(config.docker_network, "localfunctions");
    }

    #[test]
    fn test_custom_env_values() {
        std::env::set_var("LOCAL_LAMBDA_PORT", "8080");
        std::env::set_var("LOCAL_LAMBDA_REGION", "eu-west-1");

        let config = Config::from_env().unwrap();
        assert_eq!(config.port, 8080);
        assert_eq!(config.region, "eu-west-1");

        std::env::remove_var("LOCAL_LAMBDA_PORT");
        std::env::remove_var("LOCAL_LAMBDA_REGION");
    }

    #[test]
    fn test_invalid_port() {
        std::env::set_var("LOCAL_LAMBDA_PORT", "not_a_number");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOCAL_LAMBDA_PORT");
    }

    #[test]
    fn test_invalid_host() {
        std::env::set_var("LOCAL_LAMBDA_HOST", "not_an_ip");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOCAL_LAMBDA_HOST");
    }
}
