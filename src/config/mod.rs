use std::net::IpAddr;
use std::path::PathBuf;

/// Service-level configuration loaded from environment variables.
#[derive(Debug, Clone)]
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
    /// Maximum request body size in bytes for the Invoke API (default 6 MB).
    pub max_body_size: usize,
    /// Log output format: "json" for structured JSON, "text" for human-readable.
    pub log_format: LogFormat,
    /// Whether to automatically pull missing Docker images on startup.
    pub pull_images: bool,
    /// Maximum time in seconds to wait for a container's bootstrap process to
    /// call the Runtime API `/next` endpoint (default 10s).
    pub init_timeout: u64,
}

/// Controls the format of log output.
#[derive(Debug, Clone, PartialEq)]
pub enum LogFormat {
    Json,
    Text,
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(LogFormat::Json),
            "text" | "plain" => Ok(LogFormat::Text),
            other => Err(format!("unknown log format '{}', expected 'json' or 'text'", other)),
        }
    }
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::Json => write!(f, "json"),
            LogFormat::Text => write!(f, "text"),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let host = parse_env("LOCAL_LAMBDA_HOST", "0.0.0.0")?;
        let port = parse_env("LOCAL_LAMBDA_PORT", "9600")?;
        let runtime_port = parse_env("LOCAL_LAMBDA_RUNTIME_PORT", "9601")?;
        let region = parse_env::<String>("LOCAL_LAMBDA_REGION", "us-east-1")?;
        let account_id = parse_env::<String>("LOCAL_LAMBDA_ACCOUNT_ID", "000000000000")?;
        let functions_file =
            parse_env::<PathBuf>("LOCAL_LAMBDA_FUNCTIONS_FILE", "./functions.json")?;
        let log_level = parse_env_with_alias::<String>("LOCAL_LAMBDA_LOG_LEVEL", "LOG_LEVEL", "info")?;
        let log_format = parse_env_with_alias::<LogFormat>("LOCAL_LAMBDA_LOG_FORMAT", "LOG_FORMAT", "json")?;
        let shutdown_timeout = parse_env("LOCAL_LAMBDA_SHUTDOWN_TIMEOUT", "30")?;
        let container_idle_timeout = parse_env("LOCAL_LAMBDA_CONTAINER_IDLE_TIMEOUT", "300")?;
        let max_containers = parse_env("LOCAL_LAMBDA_MAX_CONTAINERS", "20")?;
        let docker_network = parse_env::<String>("LOCAL_LAMBDA_DOCKER_NETWORK", "localfunctions")?;
        let max_body_size = parse_env("LOCAL_LAMBDA_MAX_BODY_SIZE", "6291456")?; // 6 MB
        let pull_images = parse_env_with_alias::<bool>(
            "LOCAL_LAMBDA_PULL_IMAGES",
            "LOCALFUNCTIONS_PULL_IMAGES",
            "false",
        )?;
        let init_timeout = parse_env("LOCAL_LAMBDA_INIT_TIMEOUT", "10")?;

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
            max_body_size,
            log_format,
            pull_images,
            init_timeout,
        })
    }
}

/// Like `parse_env`, but checks a short alias first (e.g. `LOG_LEVEL` before
/// `LOCAL_LAMBDA_LOG_LEVEL`). The primary key takes precedence over the alias.
fn parse_env_with_alias<T>(primary: &str, alias: &str, default: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = std::env::var(primary)
        .or_else(|_| std::env::var(alias))
        .unwrap_or_else(|_| default.to_string());
    raw.parse::<T>()
        .map_err(|e| ConfigError::InvalidValue(primary.to_string(), e.to_string()))
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
    use serial_test::serial;

    #[test]
    #[serial]
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
            "LOG_LEVEL",
            "LOCAL_LAMBDA_LOG_FORMAT",
            "LOG_FORMAT",
            "LOCAL_LAMBDA_SHUTDOWN_TIMEOUT",
            "LOCAL_LAMBDA_CONTAINER_IDLE_TIMEOUT",
            "LOCAL_LAMBDA_MAX_CONTAINERS",
            "LOCAL_LAMBDA_DOCKER_NETWORK",
            "LOCAL_LAMBDA_MAX_BODY_SIZE",
            "LOCAL_LAMBDA_PULL_IMAGES",
            "LOCALFUNCTIONS_PULL_IMAGES",
            "LOCAL_LAMBDA_INIT_TIMEOUT",
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
        assert_eq!(config.max_body_size, 6 * 1024 * 1024);
        assert_eq!(config.log_format, LogFormat::Json);
        assert!(!config.pull_images);
        assert_eq!(config.init_timeout, 10);
    }

    #[test]
    #[serial]
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
    #[serial]
    fn test_invalid_port() {
        std::env::set_var("LOCAL_LAMBDA_PORT", "not_a_number");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOCAL_LAMBDA_PORT");
    }

    #[test]
    #[serial]
    fn test_invalid_host() {
        std::env::set_var("LOCAL_LAMBDA_HOST", "not_an_ip");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOCAL_LAMBDA_HOST");
    }

    #[test]
    #[serial]
    fn test_invalid_value_error_is_descriptive() {
        std::env::set_var("LOCAL_LAMBDA_PORT", "abc");
        let err = Config::from_env().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("LOCAL_LAMBDA_PORT"), "error should name the variable");
        assert!(msg.contains("abc") || msg.contains("invalid"), "error should describe the problem");
        std::env::remove_var("LOCAL_LAMBDA_PORT");
    }

    #[test]
    #[serial]
    fn test_env_var_overrides_dotenv() {
        // dotenvy merges .env into the environment without overwriting existing vars.
        // Verify that a pre-set env var is preserved after from_env().
        std::env::set_var("LOCAL_LAMBDA_PORT", "7777");
        let config = Config::from_env().unwrap();
        assert_eq!(config.port, 7777);
        std::env::remove_var("LOCAL_LAMBDA_PORT");
    }

    #[test]
    #[serial]
    fn test_invalid_shutdown_timeout() {
        std::env::set_var("LOCAL_LAMBDA_SHUTDOWN_TIMEOUT", "-5");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOCAL_LAMBDA_SHUTDOWN_TIMEOUT");
    }

    #[test]
    #[serial]
    fn test_log_format_json_default() {
        std::env::remove_var("LOCAL_LAMBDA_LOG_FORMAT");
        std::env::remove_var("LOG_FORMAT");
        let config = Config::from_env().unwrap();
        assert_eq!(config.log_format, LogFormat::Json);
    }

    #[test]
    #[serial]
    fn test_log_format_text() {
        std::env::set_var("LOG_FORMAT", "text");
        std::env::remove_var("LOCAL_LAMBDA_LOG_FORMAT");
        let config = Config::from_env().unwrap();
        assert_eq!(config.log_format, LogFormat::Text);
        std::env::remove_var("LOG_FORMAT");
    }

    #[test]
    #[serial]
    fn test_log_format_primary_overrides_alias() {
        std::env::set_var("LOCAL_LAMBDA_LOG_FORMAT", "json");
        std::env::set_var("LOG_FORMAT", "text");
        let config = Config::from_env().unwrap();
        assert_eq!(config.log_format, LogFormat::Json);
        std::env::remove_var("LOCAL_LAMBDA_LOG_FORMAT");
        std::env::remove_var("LOG_FORMAT");
    }

    #[test]
    #[serial]
    fn test_log_format_invalid() {
        std::env::set_var("LOG_FORMAT", "xml");
        std::env::remove_var("LOCAL_LAMBDA_LOG_FORMAT");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOG_FORMAT");
    }

    #[test]
    #[serial]
    fn test_log_level_alias() {
        std::env::set_var("LOG_LEVEL", "debug");
        std::env::remove_var("LOCAL_LAMBDA_LOG_LEVEL");
        let config = Config::from_env().unwrap();
        assert_eq!(config.log_level, "debug");
        std::env::remove_var("LOG_LEVEL");
    }

    #[test]
    #[serial]
    fn test_log_level_primary_overrides_alias() {
        std::env::set_var("LOCAL_LAMBDA_LOG_LEVEL", "warn");
        std::env::set_var("LOG_LEVEL", "debug");
        let config = Config::from_env().unwrap();
        assert_eq!(config.log_level, "warn");
        std::env::remove_var("LOCAL_LAMBDA_LOG_LEVEL");
        std::env::remove_var("LOG_LEVEL");
    }

    #[test]
    fn test_log_format_parse() {
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert_eq!("TEXT".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert_eq!("plain".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert!("xml".parse::<LogFormat>().is_err());
    }

    #[test]
    fn test_log_format_display() {
        assert_eq!(LogFormat::Json.to_string(), "json");
        assert_eq!(LogFormat::Text.to_string(), "text");
    }

    #[test]
    #[serial]
    fn test_pull_images_default_false() {
        std::env::remove_var("LOCAL_LAMBDA_PULL_IMAGES");
        std::env::remove_var("LOCALFUNCTIONS_PULL_IMAGES");
        let config = Config::from_env().unwrap();
        assert!(!config.pull_images);
    }

    #[test]
    #[serial]
    fn test_pull_images_enabled_via_primary() {
        std::env::set_var("LOCAL_LAMBDA_PULL_IMAGES", "true");
        std::env::remove_var("LOCALFUNCTIONS_PULL_IMAGES");
        let config = Config::from_env().unwrap();
        assert!(config.pull_images);
        std::env::remove_var("LOCAL_LAMBDA_PULL_IMAGES");
    }

    #[test]
    #[serial]
    fn test_pull_images_enabled_via_alias() {
        std::env::remove_var("LOCAL_LAMBDA_PULL_IMAGES");
        std::env::set_var("LOCALFUNCTIONS_PULL_IMAGES", "true");
        let config = Config::from_env().unwrap();
        assert!(config.pull_images);
        std::env::remove_var("LOCALFUNCTIONS_PULL_IMAGES");
    }

    #[test]
    #[serial]
    fn test_pull_images_primary_overrides_alias() {
        std::env::set_var("LOCAL_LAMBDA_PULL_IMAGES", "false");
        std::env::set_var("LOCALFUNCTIONS_PULL_IMAGES", "true");
        let config = Config::from_env().unwrap();
        assert!(!config.pull_images);
        std::env::remove_var("LOCAL_LAMBDA_PULL_IMAGES");
        std::env::remove_var("LOCALFUNCTIONS_PULL_IMAGES");
    }

    #[test]
    #[serial]
    fn test_pull_images_invalid_value() {
        std::env::set_var("LOCAL_LAMBDA_PULL_IMAGES", "yes");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOCAL_LAMBDA_PULL_IMAGES");
    }

    #[test]
    #[serial]
    fn test_init_timeout_default() {
        std::env::remove_var("LOCAL_LAMBDA_INIT_TIMEOUT");
        let config = Config::from_env().unwrap();
        assert_eq!(config.init_timeout, 10);
    }

    #[test]
    #[serial]
    fn test_init_timeout_custom() {
        std::env::set_var("LOCAL_LAMBDA_INIT_TIMEOUT", "30");
        let config = Config::from_env().unwrap();
        assert_eq!(config.init_timeout, 30);
        std::env::remove_var("LOCAL_LAMBDA_INIT_TIMEOUT");
    }

    #[test]
    #[serial]
    fn test_init_timeout_invalid() {
        std::env::set_var("LOCAL_LAMBDA_INIT_TIMEOUT", "abc");
        let result = Config::from_env();
        assert!(result.is_err());
        std::env::remove_var("LOCAL_LAMBDA_INIT_TIMEOUT");
    }
}
