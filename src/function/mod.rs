use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::warn;

use crate::types::{FunctionConfig, Invocation, ServiceError};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum FunctionConfigError {
    #[error("failed to read functions file '{path}': {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to parse functions file: {0}")]
    ParseJson(#[from] serde_json::Error),

    #[error("invalid function name '{name}': {reason}")]
    InvalidFunctionName { name: String, reason: String },

    #[error("function '{name}': invalid timeout {value} — must be between 1 and 900 seconds")]
    InvalidTimeout { name: String, value: u64 },

    #[error("function '{name}': invalid memory_size {value} — must be a positive integer")]
    InvalidMemorySize { name: String, value: u64 },

    #[error("function '{name}': invalid ephemeral_storage_mb {value} — must be between 512 and 10240")]
    InvalidEphemeralStorage { name: String, value: u64 },

    #[error("function '{name}': code_path '{path}' is invalid — directory traversal is not allowed")]
    DirectoryTraversal { name: String, path: String },

    #[error("function '{name}': runtime 'custom' requires an 'image' field")]
    MissingImageForCustomRuntime { name: String },

    #[error("function '{name}': unknown runtime '{value}' — expected one of: {expected}")]
    UnknownRuntime {
        name: String,
        value: String,
        expected: String,
    },

    #[error("function '{name}': invalid handler '{value}' — {reason}")]
    InvalidHandler {
        name: String,
        value: String,
        reason: String,
    },

    #[error("function '{name}': code_path '{path}' does not exist or is not readable")]
    CodePathNotFound { name: String, path: String },

    #[error("function '{name}': invalid environment variable key '{key}' — must start with a letter and contain only alphanumeric characters and underscores")]
    InvalidEnvironmentKey { name: String, key: String },

    #[error("function '{name}': image_uri functions must not specify code_path")]
    ImageUriWithCodePath { name: String },

    #[error("function '{name}': must specify either image_uri or both runtime and code_path")]
    MissingRuntimeOrImageUri { name: String },

    #[error("function '{name}': invalid reserved_concurrent_executions {value} — must be between 1 and 1000")]
    InvalidReservedConcurrency { name: String, value: u64 },

    #[error("function '{name}': environment variables total size {actual_size} bytes exceeds the 4096-byte limit")]
    EnvironmentTooLarge { name: String, actual_size: usize },

    #[error("function '{name}': invalid architecture '{value}' — must be 'x86_64' or 'arm64'")]
    InvalidArchitecture { name: String, value: String },

    #[error("function '{name}': layer path '{path}' is invalid — directory traversal is not allowed")]
    LayerDirectoryTraversal { name: String, path: String },

    #[error("function '{name}': invalid max_retry_attempts {value} — must be between 0 and 2")]
    InvalidMaxRetryAttempts { name: String, value: u32 },

    #[error("function '{name}': layer path '{path}' does not exist or is not a directory")]
    LayerPathNotFound { name: String, path: String },

    #[error("function '{name}': on_success destination '{destination}' does not refer to a known function")]
    InvalidOnSuccessDestination { name: String, destination: String },

    #[error("function '{name}': on_failure destination '{destination}' does not refer to a known function")]
    InvalidOnFailureDestination { name: String, destination: String },

    #[error("configuration validation failed with {count} error(s):\n{details}")]
    ValidationErrors { count: usize, details: String },
}

// ---------------------------------------------------------------------------
// Raw deserialization structs (serde)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RawFunctionsFile {
    functions: HashMap<String, RawFunctionEntry>,
    #[serde(default)]
    runtime_images: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RawFunctionEntry {
    runtime: Option<String>,
    handler: Option<String>,
    code_path: Option<String>,
    timeout: Option<u64>,
    memory_size: Option<u64>,
    ephemeral_storage_mb: Option<u64>,
    #[serde(default)]
    environment: HashMap<String, String>,
    image: Option<String>,
    image_uri: Option<String>,
    reserved_concurrent_executions: Option<u64>,
    architecture: Option<String>,
    #[serde(default)]
    layers: Vec<String>,
    #[serde(default)]
    function_url_enabled: bool,
    max_retry_attempts: Option<u32>,
    on_success: Option<String>,
    on_failure: Option<String>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Result of parsing a functions configuration file.
#[derive(Debug)]
#[allow(dead_code)]
pub struct FunctionsConfig {
    pub functions: HashMap<String, FunctionConfig>,
    pub runtime_images: HashMap<String, String>,
}

/// Parse and validate a functions.json file.
///
/// `base_dir` is the directory against which relative `code_path` values are
/// resolved (typically the directory containing the config file).
#[allow(dead_code)]
pub fn load_functions_config(
    path: &Path,
    base_dir: &Path,
) -> Result<FunctionsConfig, FunctionConfigError> {
    let content = fs::read_to_string(path).map_err(|e| FunctionConfigError::ReadFile {
        path: path.display().to_string(),
        source: e,
    })?;

    parse_functions_config(&content, base_dir)
}

/// Known runtime prefixes that localfunctions supports.
const KNOWN_RUNTIMES: &[&str] = &[
    "python3.",
    "nodejs",
    "java",
    "dotnet",
    "ruby",
    "provided",
    "custom",
];

/// Check whether a runtime string matches one of the known runtime prefixes.
fn is_known_runtime(runtime: &str) -> bool {
    KNOWN_RUNTIMES
        .iter()
        .any(|prefix| runtime.starts_with(prefix))
}

/// Parse and validate functions configuration from a JSON string.
///
/// All functions are validated and errors are collected. If any validation
/// errors are found, they are returned together in a single
/// `ValidationErrors` variant so the developer can fix them all at once.
#[allow(dead_code)]
pub fn parse_functions_config(
    json: &str,
    base_dir: &Path,
) -> Result<FunctionsConfig, FunctionConfigError> {
    let raw: RawFunctionsFile = serde_json::from_str(json)?;

    let mut functions = HashMap::new();
    let mut errors: Vec<FunctionConfigError> = Vec::new();

    // Sort keys for deterministic error ordering
    let mut names: Vec<String> = raw.functions.keys().cloned().collect();
    names.sort();

    for name in &names {
        let entry = &raw.functions[name];

        // Validate function name
        if let Err(e) = validate_function_name(name) {
            errors.push(e);
            // Skip further validation for this function since the name is invalid
            continue;
        }

        let timeout = entry.timeout.unwrap_or(30);
        let memory_size = entry.memory_size.unwrap_or(128);
        let ephemeral_storage_mb = entry.ephemeral_storage_mb.unwrap_or(512);

        let is_image_uri = entry.image_uri.is_some();

        // Image-URI functions: code_path must not be specified
        if is_image_uri && entry.code_path.is_some() {
            errors.push(FunctionConfigError::ImageUriWithCodePath {
                name: name.clone(),
            });
        }

        // Non-image-URI functions: runtime and code_path are required
        if !is_image_uri && (entry.runtime.is_none() || entry.code_path.is_none()) {
            errors.push(FunctionConfigError::MissingRuntimeOrImageUri {
                name: name.clone(),
            });
            continue;
        }

        // Resolve runtime: image_uri functions default to "provided.al2023"
        let runtime = entry
            .runtime
            .clone()
            .unwrap_or_else(|| "provided.al2023".to_string());

        // Resolve handler: image_uri functions without explicit handler use ""
        let handler = entry.handler.clone().unwrap_or_default();

        // Validate runtime (skip for image_uri functions without explicit runtime)
        if entry.runtime.is_some() && !is_known_runtime(&runtime) {
            errors.push(FunctionConfigError::UnknownRuntime {
                name: name.clone(),
                value: runtime.clone(),
                expected: KNOWN_RUNTIMES.join(", "),
            });
        }

        // Validate handler format: required for non-image-uri functions,
        // optional for image-uri functions (only validated if explicitly set)
        if !is_image_uri || entry.handler.is_some() {
            if let Err(e) = validate_handler(name, &handler, &runtime) {
                errors.push(e);
            }
        }

        // Validate timeout
        if let Err(e) = validate_timeout(name, timeout) {
            errors.push(e);
        }

        // Validate memory size
        if let Err(e) = validate_memory_size(name, memory_size) {
            errors.push(e);
        }

        // Validate ephemeral storage
        if let Err(e) = validate_ephemeral_storage(name, ephemeral_storage_mb) {
            errors.push(e);
        }

        // Validate code_path (only for non-image-uri functions)
        let code_path = if let Some(ref raw_code_path) = entry.code_path {
            match canonicalize_code_path(name, raw_code_path, base_dir) {
                Ok(p) => {
                    // Validate code_path existence
                    if !p.exists() {
                        errors.push(FunctionConfigError::CodePathNotFound {
                            name: name.clone(),
                            path: raw_code_path.clone(),
                        });
                    }
                    p
                }
                Err(e) => {
                    errors.push(e);
                    // Use a placeholder so we can continue validation
                    base_dir.join(raw_code_path)
                }
            }
        } else {
            // image_uri functions have no code_path
            PathBuf::new()
        };

        // Validate custom runtime requires image (only for non-image-uri)
        if !is_image_uri && runtime == "custom" && entry.image.is_none() {
            errors.push(FunctionConfigError::MissingImageForCustomRuntime {
                name: name.clone(),
            });
        }

        // Validate reserved_concurrent_executions
        if let Some(rce) = entry.reserved_concurrent_executions {
            if rce == 0 || rce > 1000 {
                errors.push(FunctionConfigError::InvalidReservedConcurrency {
                    name: name.clone(),
                    value: rce,
                });
            }
        }

        // Validate architecture (default to host architecture)
        let architecture = entry
            .architecture
            .clone()
            .unwrap_or_else(|| default_architecture().to_string());
        if let Err(e) = validate_architecture(name, &architecture) {
            errors.push(e);
        }

        // Validate environment variable keys
        for key in entry.environment.keys() {
            if let Err(e) = validate_env_key(name, key) {
                errors.push(e);
            }
        }

        // Validate total environment variable size (4KB limit)
        if let Err(e) = validate_env_total_size(name, &entry.environment, &handler, memory_size, is_image_uri) {
            errors.push(e);
        }

        // Validate and canonicalize layer paths
        let mut layers = Vec::new();
        for raw_layer_path in &entry.layers {
            match canonicalize_layer_path(name, raw_layer_path, base_dir) {
                Ok(p) => {
                    if !p.exists() || !p.is_dir() {
                        errors.push(FunctionConfigError::LayerPathNotFound {
                            name: name.clone(),
                            path: raw_layer_path.clone(),
                        });
                    }
                    layers.push(p);
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        // Validate max_retry_attempts
        let max_retry_attempts = entry.max_retry_attempts.unwrap_or(2);
        if max_retry_attempts > 2 {
            errors.push(FunctionConfigError::InvalidMaxRetryAttempts {
                name: name.clone(),
                value: max_retry_attempts,
            });
        }

        // Only build the config if there are no errors for this function
        // (we still continue to validate other functions)
        let config = FunctionConfig {
            name: name.clone(),
            runtime,
            handler,
            code_path,
            timeout,
            memory_size,
            ephemeral_storage_mb,
            environment: entry.environment.clone(),
            image: entry.image.clone(),
            image_uri: entry.image_uri.clone(),
            reserved_concurrent_executions: entry.reserved_concurrent_executions,
            architecture,
            layers,
            function_url_enabled: entry.function_url_enabled,
            max_retry_attempts,
            on_success: entry.on_success.clone(),
            on_failure: entry.on_failure.clone(),
        };

        functions.insert(name.clone(), config);
    }

    // Validate destination references after all functions are loaded so that
    // forward references work (e.g. function A's on_success points to B which
    // is defined later in the file).
    for name in &names {
        if let Some(cfg) = functions.get(name) {
            if let Some(ref dest) = cfg.on_success {
                if !functions.contains_key(dest) {
                    errors.push(FunctionConfigError::InvalidOnSuccessDestination {
                        name: name.clone(),
                        destination: dest.clone(),
                    });
                }
            }
            if let Some(ref dest) = cfg.on_failure {
                if !functions.contains_key(dest) {
                    errors.push(FunctionConfigError::InvalidOnFailureDestination {
                        name: name.clone(),
                        destination: dest.clone(),
                    });
                }
            }
        }
    }

    if !errors.is_empty() {
        let count = errors.len();
        let details = errors
            .iter()
            .enumerate()
            .map(|(i, e)| format!("  {}. {}", i + 1, e))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(FunctionConfigError::ValidationErrors { count, details });
    }

    Ok(FunctionsConfig {
        functions,
        runtime_images: raw.runtime_images,
    })
}

// ---------------------------------------------------------------------------
// FunctionManager
// ---------------------------------------------------------------------------

/// Central manager that holds loaded function definitions, provides lookup and
/// listing, and maintains per-function invocation channels for routing payloads
/// to containers.
#[allow(dead_code)]
pub struct FunctionManager {
    functions: HashMap<String, FunctionConfig>,
    runtime_images: HashMap<String, String>,
    /// Per-function sender for routing invocations to container workers.
    invocation_txs: HashMap<String, mpsc::Sender<Invocation>>,
    region: String,
    account_id: String,
}

#[allow(dead_code)]
impl FunctionManager {
    /// Create a new `FunctionManager` from a parsed `FunctionsConfig`.
    ///
    /// For each function an mpsc channel is created (with `buffer_size` capacity)
    /// whose receiver should be consumed by the container/worker layer.
    pub fn new(
        config: FunctionsConfig,
        region: String,
        account_id: String,
        buffer_size: usize,
    ) -> (Self, HashMap<String, mpsc::Receiver<Invocation>>) {
        let mut invocation_txs = HashMap::new();
        let mut invocation_rxs = HashMap::new();

        for name in config.functions.keys() {
            let (tx, rx) = mpsc::channel(buffer_size);
            invocation_txs.insert(name.clone(), tx);
            invocation_rxs.insert(name.clone(), rx);
        }

        let manager = Self {
            functions: config.functions,
            runtime_images: config.runtime_images,
            invocation_txs,
            region,
            account_id,
        };

        (manager, invocation_rxs)
    }

    /// Look up a function by name, returning its configuration.
    ///
    /// Returns `ServiceError::ResourceNotFound` if no function with the given
    /// name is configured.
    pub fn get_function(&self, name: &str) -> Result<&FunctionConfig, ServiceError> {
        self.functions
            .get(name)
            .ok_or_else(|| ServiceError::ResourceNotFound(name.to_string()))
    }

    /// Return all configured functions as a slice-like iterator.
    pub fn list_functions(&self) -> Vec<&FunctionConfig> {
        self.functions.values().collect()
    }

    /// Get the invocation sender for a function so callers can route payloads.
    ///
    /// Returns `ServiceError::ResourceNotFound` if the function does not exist.
    pub fn invocation_tx(&self, name: &str) -> Result<&mpsc::Sender<Invocation>, ServiceError> {
        self.invocation_txs
            .get(name)
            .ok_or_else(|| ServiceError::ResourceNotFound(name.to_string()))
    }

    /// Generate an AWS-format ARN for the given function.
    ///
    /// Format: `arn:aws:lambda:{region}:{account_id}:function:{function_name}`
    pub fn arn(&self, function_name: &str) -> Result<String, ServiceError> {
        if !self.functions.contains_key(function_name) {
            return Err(ServiceError::ResourceNotFound(function_name.to_string()));
        }
        Ok(format!(
            "arn:aws:lambda:{}:{}:function:{}",
            self.region, self.account_id, function_name
        ))
    }

    /// Return the number of configured functions.
    pub fn function_count(&self) -> usize {
        self.functions.len()
    }
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Validate handler format based on the runtime.
///
/// For non-custom runtimes, the handler must be in `module.function` format
/// (i.e., contain at least one dot). For `custom` runtimes, any non-empty
/// handler is accepted.
fn validate_handler(
    name: &str,
    handler: &str,
    runtime: &str,
) -> Result<(), FunctionConfigError> {
    if handler.is_empty() {
        return Err(FunctionConfigError::InvalidHandler {
            name: name.to_string(),
            value: handler.to_string(),
            reason: "must not be empty".to_string(),
        });
    }
    if runtime != "custom" && !handler.contains('.') {
        return Err(FunctionConfigError::InvalidHandler {
            name: name.to_string(),
            value: handler.to_string(),
            reason: format!(
                "handler for runtime '{}' must be in 'module.function' format (e.g., 'main.handler')",
                runtime
            ),
        });
    }
    Ok(())
}

/// Validate an environment variable key.
///
/// Keys must start with a letter and contain only alphanumeric characters and
/// underscores (matching the POSIX convention for environment variable names).
fn validate_env_key(name: &str, key: &str) -> Result<(), FunctionConfigError> {
    if key.is_empty() {
        return Err(FunctionConfigError::InvalidEnvironmentKey {
            name: name.to_string(),
            key: key.to_string(),
        });
    }
    let first = key.chars().next().unwrap();
    if !first.is_ascii_alphabetic() {
        return Err(FunctionConfigError::InvalidEnvironmentKey {
            name: name.to_string(),
            key: key.to_string(),
        });
    }
    if !key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(FunctionConfigError::InvalidEnvironmentKey {
            name: name.to_string(),
            key: key.to_string(),
        });
    }
    Ok(())
}

/// Maximum total size of environment variables per function (4KB, matching AWS Lambda).
const ENV_VARS_MAX_SIZE: usize = 4096;

/// Warning threshold for environment variable total size (3.5KB).
const ENV_VARS_WARN_SIZE: usize = 3584;

/// Calculate the total serialized size of environment variables.
///
/// The size is computed as the sum of (key.len() + value.len()) for each entry,
/// which matches how AWS Lambda measures the limit.
fn calculate_env_size(vars: &HashMap<String, String>) -> usize {
    vars.iter().map(|(k, v)| k.len() + v.len()).sum()
}

/// Build the set of system-injected environment variables that AWS Lambda
/// injects into every function, so we can include them in the size calculation.
fn system_env_vars(name: &str, handler: &str, memory_size: u64, is_image_uri: bool) -> HashMap<String, String> {
    let mut vars = HashMap::new();
    vars.insert("AWS_LAMBDA_FUNCTION_NAME".into(), name.to_string());
    if !is_image_uri || !handler.is_empty() {
        vars.insert("_HANDLER".into(), handler.to_string());
    }
    vars.insert(
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE".into(),
        memory_size.to_string(),
    );
    // Use a representative value for the runtime API endpoint
    vars.insert("AWS_LAMBDA_RUNTIME_API".into(), "host.docker.internal:9601".into());
    vars.insert("AWS_REGION".into(), "us-east-1".into());
    vars.insert("AWS_DEFAULT_REGION".into(), "us-east-1".into());
    vars.insert("AWS_LAMBDA_FUNCTION_VERSION".into(), "$LATEST".into());
    vars.insert(
        "AWS_LAMBDA_LOG_GROUP_NAME".into(),
        format!("/aws/lambda/{}", name),
    );
    vars.insert("AWS_LAMBDA_LOG_STREAM_NAME".into(), "localfunctions/latest".into());
    vars
}

/// Validate total environment variable size (keys + values) including system-injected vars.
///
/// AWS Lambda enforces a 4KB limit. This validation catches the error locally.
/// A warning is emitted at 3.5KB to alert developers approaching the limit.
fn validate_env_total_size(
    name: &str,
    user_env: &HashMap<String, String>,
    handler: &str,
    memory_size: u64,
    is_image_uri: bool,
) -> Result<(), FunctionConfigError> {
    let mut combined = system_env_vars(name, handler, memory_size, is_image_uri);
    // User vars that don't collide with system vars
    for (k, v) in user_env {
        combined.entry(k.clone()).or_insert_with(|| v.clone());
    }

    let total_size = calculate_env_size(&combined);

    if total_size > ENV_VARS_MAX_SIZE {
        return Err(FunctionConfigError::EnvironmentTooLarge {
            name: name.to_string(),
            actual_size: total_size,
        });
    }
    if total_size > ENV_VARS_WARN_SIZE {
        warn!(
            function = name,
            size = total_size,
            limit = ENV_VARS_MAX_SIZE,
            "environment variables approaching 4KB limit ({total_size}/{ENV_VARS_MAX_SIZE} bytes)"
        );
    }
    Ok(())
}

/// Validate a function name: alphanumeric, hyphens, underscores only, max 64 chars.
///
/// Returns `Ok(())` if valid, or an error describing the violation.
pub fn validate_function_name(name: &str) -> Result<(), FunctionConfigError> {
    if name.is_empty() {
        return Err(FunctionConfigError::InvalidFunctionName {
            name: name.to_string(),
            reason: "must not be empty".to_string(),
        });
    }
    if name.len() > 64 {
        return Err(FunctionConfigError::InvalidFunctionName {
            name: name.to_string(),
            reason: "must be at most 64 characters".to_string(),
        });
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(FunctionConfigError::InvalidFunctionName {
            name: name.to_string(),
            reason: "may only contain alphanumeric characters, hyphens, and underscores"
                .to_string(),
        });
    }
    Ok(())
}

fn validate_timeout(name: &str, timeout: u64) -> Result<(), FunctionConfigError> {
    if !(1..=900).contains(&timeout) {
        return Err(FunctionConfigError::InvalidTimeout {
            name: name.to_string(),
            value: timeout,
        });
    }
    Ok(())
}

fn validate_memory_size(name: &str, memory_size: u64) -> Result<(), FunctionConfigError> {
    if memory_size == 0 {
        return Err(FunctionConfigError::InvalidMemorySize {
            name: name.to_string(),
            value: memory_size,
        });
    }
    Ok(())
}

/// Valid architecture values for Lambda functions.
const VALID_ARCHITECTURES: &[&str] = &["x86_64", "arm64"];

fn validate_architecture(name: &str, architecture: &str) -> Result<(), FunctionConfigError> {
    if !VALID_ARCHITECTURES.contains(&architecture) {
        return Err(FunctionConfigError::InvalidArchitecture {
            name: name.to_string(),
            value: architecture.to_string(),
        });
    }
    Ok(())
}

fn validate_ephemeral_storage(name: &str, ephemeral_storage_mb: u64) -> Result<(), FunctionConfigError> {
    if !(512..=10240).contains(&ephemeral_storage_mb) {
        return Err(FunctionConfigError::InvalidEphemeralStorage {
            name: name.to_string(),
            value: ephemeral_storage_mb,
        });
    }
    Ok(())
}

/// Return the default architecture based on the host platform.
fn default_architecture() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        "x86_64"
    }
}

/// Resolve and validate a code_path, preventing directory traversal.
fn canonicalize_code_path(
    name: &str,
    code_path: &str,
    base_dir: &Path,
) -> Result<PathBuf, FunctionConfigError> {
    // Check for obvious traversal patterns before resolving
    let normalized = if Path::new(code_path).is_absolute() {
        PathBuf::from(code_path)
    } else {
        base_dir.join(code_path)
    };

    // Normalize the path by resolving `.` and `..` components lexically
    let mut result = PathBuf::new();
    for component in normalized.components() {
        match component {
            std::path::Component::ParentDir => {
                if !result.pop() {
                    return Err(FunctionConfigError::DirectoryTraversal {
                        name: name.to_string(),
                        path: code_path.to_string(),
                    });
                }
            }
            other => result.push(other),
        }
    }

    // After normalization, the resolved path must still be under base_dir
    if !result.starts_with(base_dir) {
        return Err(FunctionConfigError::DirectoryTraversal {
            name: name.to_string(),
            path: code_path.to_string(),
        });
    }

    Ok(result)
}

/// Resolve and validate a layer path, preventing directory traversal.
fn canonicalize_layer_path(
    name: &str,
    layer_path: &str,
    base_dir: &Path,
) -> Result<PathBuf, FunctionConfigError> {
    let normalized = if Path::new(layer_path).is_absolute() {
        PathBuf::from(layer_path)
    } else {
        base_dir.join(layer_path)
    };

    let mut result = PathBuf::new();
    for component in normalized.components() {
        match component {
            std::path::Component::ParentDir => {
                if !result.pop() {
                    return Err(FunctionConfigError::LayerDirectoryTraversal {
                        name: name.to_string(),
                        path: layer_path.to_string(),
                    });
                }
            }
            other => result.push(other),
        }
    }

    if !result.starts_with(base_dir) {
        return Err(FunctionConfigError::LayerDirectoryTraversal {
            name: name.to_string(),
            path: layer_path.to_string(),
        });
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Create a temp directory with the given subdirectories for use as code paths.
    fn setup_dirs(subdirs: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for sub in subdirs {
            std::fs::create_dir_all(dir.path().join(sub)).unwrap();
        }
        dir
    }

    /// Helper: assert an error is `ValidationErrors` and its message contains the given text.
    fn assert_validation_error_contains(err: &FunctionConfigError, expected: &str) {
        match err {
            FunctionConfigError::ValidationErrors { details, .. } => {
                assert!(
                    details.contains(expected),
                    "expected error details to contain {:?}, got:\n{}",
                    expected,
                    details
                );
            }
            other => {
                panic!(
                    "expected ValidationErrors, got: {}",
                    other
                );
            }
        }
    }

    // -- Valid config --------------------------------------------------------

    #[test]
    fn parse_valid_full_config() {
        let dir = setup_dirs(&["functions/my-python-func", "functions/my-node-func"]);
        let json = r#"{
            "functions": {
                "my-python-func": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./functions/my-python-func",
                    "timeout": 30,
                    "memory_size": 256,
                    "environment": {
                        "TABLE_NAME": "my-table"
                    }
                },
                "my-node-func": {
                    "runtime": "nodejs20.x",
                    "handler": "index.handler",
                    "code_path": "./functions/my-node-func",
                    "timeout": 10
                }
            },
            "runtime_images": {
                "python3.12": "public.ecr.aws/lambda/python:3.12",
                "nodejs20.x": "public.ecr.aws/lambda/nodejs:20"
            }
        }"#;

        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions.len(), 2);
        assert_eq!(config.runtime_images.len(), 2);

        let py = &config.functions["my-python-func"];
        assert_eq!(py.runtime, "python3.12");
        assert_eq!(py.handler, "main.handler");
        assert_eq!(py.timeout, 30);
        assert_eq!(py.memory_size, 256);
        assert_eq!(py.environment.get("TABLE_NAME").unwrap(), "my-table");

        let node = &config.functions["my-node-func"];
        assert_eq!(node.timeout, 10);
        assert_eq!(node.memory_size, 128); // default
        assert!(node.environment.is_empty());
    }

    #[test]
    fn parse_minimal_config() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;

        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["f1"];
        assert_eq!(f.timeout, 30);
        assert_eq!(f.memory_size, 128);
        assert!(f.environment.is_empty());
        assert!(f.image.is_none());
        assert!(config.runtime_images.is_empty());
    }

    #[test]
    fn parse_custom_runtime_with_image() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "custom-func": {
                    "runtime": "custom",
                    "handler": "bootstrap",
                    "code_path": "./code",
                    "image": "my-custom:latest"
                }
            }
        }"#;

        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["custom-func"];
        assert_eq!(f.runtime, "custom");
        assert_eq!(f.image, Some("my-custom:latest".to_string()));
    }

    // -- Missing required fields (serde-level errors, not wrapped) -----------

    #[test]
    fn error_missing_runtime() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        // Without image_uri, runtime and code_path are required together
        assert!(matches!(err, FunctionConfigError::ValidationErrors { .. }));
        assert!(err.to_string().contains("runtime"));
    }

    #[test]
    fn error_missing_handler() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "code_path": "./code"
                }
            }
        }"#;
        // Handler is optional — when omitted, it defaults to empty string.
        // For non-image-uri functions with a standard runtime, handler
        // validation catches empty handlers.
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ValidationErrors { .. }));
    }

    #[test]
    fn error_missing_code_path() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        // Without image_uri, code_path is required
        assert!(matches!(err, FunctionConfigError::ValidationErrors { .. }));
    }

    #[test]
    fn error_missing_functions_key() {
        let dir = setup_dirs(&[]);
        let json = r#"{ "runtime_images": {} }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ParseJson(_)));
    }

    // -- Invalid function names ----------------------------------------------

    #[test]
    fn error_function_name_with_spaces() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "bad name": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "bad name");
    }

    #[test]
    fn error_function_name_with_special_chars() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "func@#$": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "func@#$");
    }

    #[test]
    fn error_function_name_too_long() {
        let dir = setup_dirs(&["code"]);
        let long_name = "a".repeat(65);
        let json = format!(
            "{{\n\"functions\": {{\n\"{}\": {{\n\"runtime\": \"python3.12\",\n\"handler\": \"m.h\",\n\"code_path\": \"./code\"\n}}\n}}\n}}",
            long_name
        );
        let err = parse_functions_config(&json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "64");
    }

    #[test]
    fn valid_function_name_64_chars() {
        let dir = setup_dirs(&["code"]);
        let name = "a".repeat(64);
        let json = format!(
            "{{\n\"functions\": {{\n\"{}\": {{\n\"runtime\": \"python3.12\",\n\"handler\": \"m.h\",\n\"code_path\": \"./code\"\n}}\n}}\n}}",
            name
        );
        let config = parse_functions_config(&json, dir.path()).unwrap();
        assert!(config.functions.contains_key(&name));
    }

    #[test]
    fn valid_function_name_with_hyphens_and_underscores() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "my-func_v2": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert!(config.functions.contains_key("my-func_v2"));
    }

    // -- Directory traversal -------------------------------------------------

    #[test]
    fn error_directory_traversal_dotdot() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "../../../etc/passwd"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "directory traversal");
    }

    #[test]
    fn error_directory_traversal_embedded() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./functions/../../secret"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "directory traversal");
    }

    #[test]
    fn valid_code_path_stays_within_base() {
        let dir = setup_dirs(&["functions/code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./functions/../functions/code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["f1"];
        assert!(f.code_path.starts_with(dir.path()));
    }

    // -- Timeout boundary values ---------------------------------------------

    #[test]
    fn error_timeout_zero() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "timeout": 0
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "invalid timeout");
    }

    #[test]
    fn error_timeout_901() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "timeout": 901
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "invalid timeout");
    }

    #[test]
    fn valid_timeout_1() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "timeout": 1
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].timeout, 1);
    }

    #[test]
    fn valid_timeout_900() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "timeout": 900
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].timeout, 900);
    }

    // -- Memory size ---------------------------------------------------------

    #[test]
    fn error_memory_size_zero() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "memory_size": 0
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "invalid memory_size");
    }

    // -- Ephemeral storage validation -----------------------------------------

    #[test]
    fn ephemeral_storage_default_512() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].ephemeral_storage_mb, 512);
    }

    #[test]
    fn ephemeral_storage_custom_value() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "ephemeral_storage_mb": 1024
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].ephemeral_storage_mb, 1024);
    }

    #[test]
    fn ephemeral_storage_max_value() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "ephemeral_storage_mb": 10240
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].ephemeral_storage_mb, 10240);
    }

    #[test]
    fn error_ephemeral_storage_below_minimum() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "ephemeral_storage_mb": 256
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "invalid ephemeral_storage_mb");
    }

    #[test]
    fn error_ephemeral_storage_above_maximum() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "ephemeral_storage_mb": 10241
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "invalid ephemeral_storage_mb");
    }

    #[test]
    fn error_ephemeral_storage_zero() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "ephemeral_storage_mb": 0
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "invalid ephemeral_storage_mb");
    }

    // -- Custom runtime requires image ---------------------------------------

    #[test]
    fn error_custom_runtime_without_image() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "custom",
                    "handler": "bootstrap",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "requires an 'image' field");
    }

    // -- image_uri functions ---------------------------------------------------

    #[test]
    fn image_uri_function_minimal_config() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "my-image-func": {
                    "image_uri": "my-lambda:latest"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["my-image-func"];
        assert_eq!(f.image_uri, Some("my-lambda:latest".into()));
        assert_eq!(f.runtime, "provided.al2023");
        assert!(f.handler.is_empty());
        assert_eq!(f.timeout, 30);
        assert_eq!(f.memory_size, 128);
    }

    #[test]
    fn image_uri_function_with_handler_override() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "my-image-func": {
                    "image_uri": "my-lambda:latest",
                    "handler": "app.handler"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["my-image-func"];
        assert_eq!(f.image_uri, Some("my-lambda:latest".into()));
        assert_eq!(f.handler, "app.handler");
    }

    #[test]
    fn image_uri_function_with_all_optional_fields() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "my-image-func": {
                    "image_uri": "my-lambda:latest",
                    "timeout": 60,
                    "memory_size": 512,
                    "ephemeral_storage_mb": 1024,
                    "environment": { "MY_VAR": "hello" }
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["my-image-func"];
        assert_eq!(f.image_uri, Some("my-lambda:latest".into()));
        assert_eq!(f.timeout, 60);
        assert_eq!(f.memory_size, 512);
        assert_eq!(f.ephemeral_storage_mb, 1024);
        assert_eq!(f.environment.get("MY_VAR"), Some(&"hello".to_string()));
    }

    #[test]
    fn error_image_uri_with_code_path() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "image_uri": "my-lambda:latest",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "must not specify code_path");
    }

    #[test]
    fn error_missing_runtime_and_image_uri() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "must specify either image_uri");
    }

    #[test]
    fn image_uri_function_with_explicit_runtime() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "my-image-func": {
                    "image_uri": "my-lambda:latest",
                    "runtime": "python3.12"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["my-image-func"];
        assert_eq!(f.runtime, "python3.12");
        assert_eq!(f.image_uri, Some("my-lambda:latest".into()));
    }

    // -- Unknown runtime -----------------------------------------------------

    #[test]
    fn error_unknown_runtime() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "cobol42",
                    "handler": "m.h",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "unknown runtime");
        assert_validation_error_contains(&err, "cobol42");
    }

    // -- Handler format validation -------------------------------------------

    #[test]
    fn error_handler_missing_dot_for_python() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "module.function");
    }

    #[test]
    fn valid_handler_custom_runtime_no_dot() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "custom",
                    "handler": "bootstrap",
                    "code_path": "./code",
                    "image": "my-image:latest"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].handler, "bootstrap");
    }

    // -- Code path existence -------------------------------------------------

    #[test]
    fn error_code_path_not_found() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./nonexistent"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "does not exist");
    }

    // -- Environment variable key validation ---------------------------------

    #[test]
    fn error_env_key_starts_with_number() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "environment": {
                        "1BAD_KEY": "value"
                    }
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "1BAD_KEY");
    }

    #[test]
    fn error_env_key_with_hyphen() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "environment": {
                        "BAD-KEY": "value"
                    }
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "BAD-KEY");
    }

    #[test]
    fn valid_env_keys() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "environment": {
                        "TABLE_NAME": "t",
                        "AWS_REGION": "us-east-1",
                        "Foo123": "bar"
                    }
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].environment.len(), 3);
    }

    // -- Environment variable total size validation --------------------------

    #[test]
    fn error_env_vars_exceed_4kb() {
        let dir = setup_dirs(&["code"]);
        // Create a large value that will push total over 4096 bytes
        // (system vars take up some space, so we need to fill the rest)
        let big_value = "x".repeat(4000);
        let json = format!(
            r#"{{
            "functions": {{
                "f1": {{
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "environment": {{
                        "BIG_VAR": "{big_value}"
                    }}
                }}
            }}
        }}"#
        );
        let err = parse_functions_config(&json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "environment variables total size");
        assert_validation_error_contains(&err, "exceeds the 4096-byte limit");
    }

    #[test]
    fn env_vars_within_limit_succeeds() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "environment": {
                        "SMALL_VAR": "hello"
                    }
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].environment["SMALL_VAR"], "hello");
    }

    #[test]
    fn env_size_includes_system_vars() {
        // System vars should be counted toward the limit.
        // Even with no user vars, system vars exist and are counted.
        let sys = system_env_vars("test-func", "m.h", 128, false);
        let size = calculate_env_size(&sys);
        assert!(size > 0, "system env vars should have non-zero size");
        // Verify the size is reasonable (system vars should be a few hundred bytes)
        assert!(size < 1000, "system env vars alone should be under 1KB, got {size}");
    }

    #[test]
    fn env_size_calculation_correct() {
        let mut vars = HashMap::new();
        vars.insert("KEY".to_string(), "VAL".to_string());
        // KEY=3 + VAL=3 = 6
        assert_eq!(calculate_env_size(&vars), 6);

        vars.insert("ANOTHER".to_string(), "VALUE".to_string());
        // 6 + ANOTHER=7 + VALUE=5 = 18
        assert_eq!(calculate_env_size(&vars), 18);
    }

    #[test]
    fn env_vars_warning_threshold() {
        // Verify constants are correct
        assert_eq!(ENV_VARS_MAX_SIZE, 4096);
        assert_eq!(ENV_VARS_WARN_SIZE, 3584); // 3.5 * 1024
    }

    // -- Multiple errors collected -------------------------------------------

    #[test]
    fn multiple_errors_collected() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./nonexistent",
                    "timeout": 0,
                    "memory_size": 0
                },
                "f2": {
                    "runtime": "cobol42",
                    "handler": "nope",
                    "code_path": "./also-missing"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        match &err {
            FunctionConfigError::ValidationErrors { count, details } => {
                // f1: code_path not found + timeout + memory_size = 3
                // f2: unknown runtime + handler no dot + code_path not found = 3
                // Total >= 6
                assert!(
                    *count >= 6,
                    "expected at least 6 errors, got {}: {}",
                    count,
                    details
                );
            }
            other => panic!("expected ValidationErrors, got: {}", other),
        }
    }

    #[test]
    fn multiple_functions_all_errors_reported() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "timeout": 0
                },
                "f2": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "timeout": 999
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        // Both functions' errors should be present
        assert!(msg.contains("f1"), "should mention f1 in: {}", msg);
        assert!(msg.contains("f2"), "should mention f2 in: {}", msg);
    }

    // -- Malformed JSON ------------------------------------------------------

    #[test]
    fn error_malformed_json() {
        let dir = setup_dirs(&[]);
        let json = "{ not valid json }";
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ParseJson(_)));
    }

    // -- load_functions_config (file-based) -----------------------------------

    #[test]
    fn error_missing_file() {
        let dir = setup_dirs(&[]);
        let err = load_functions_config(Path::new("/nonexistent/functions.json"), dir.path()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ReadFile { .. }));
    }

    #[test]
    fn load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("functions.json");
        let code_dir = dir.path().join("code");
        std::fs::create_dir(&code_dir).unwrap();

        std::fs::write(
            &file,
            r#"{
                "functions": {
                    "f1": {
                        "runtime": "python3.12",
                        "handler": "main.handler",
                        "code_path": "./code"
                    }
                }
            }"#,
        )
        .unwrap();

        let config = load_functions_config(&file, dir.path()).unwrap();
        assert_eq!(config.functions.len(), 1);
        assert!(config.functions["f1"].code_path.starts_with(dir.path()));
    }

    // -- runtime_images map --------------------------------------------------

    // -- FunctionManager -----------------------------------------------------

    fn test_functions_config() -> FunctionsConfig {
        let dir = setup_dirs(&["functions/py", "functions/node"]);
        let json = r#"{
            "functions": {
                "my-python-func": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./functions/py",
                    "timeout": 30,
                    "memory_size": 256
                },
                "my-node-func": {
                    "runtime": "nodejs20.x",
                    "handler": "index.handler",
                    "code_path": "./functions/node"
                }
            },
            "runtime_images": {
                "python3.12": "public.ecr.aws/lambda/python:3.12"
            }
        }"#;
        // Note: dir is kept alive because the temp dir stays in scope
        // within the calling test function's stack. We use dir.path() here
        // but the paths resolve to real temp dirs.
        parse_functions_config(json, dir.path()).unwrap()
    }

    #[test]
    fn function_manager_loads_all_functions() {
        let config = test_functions_config();
        let (mgr, _rxs) = FunctionManager::new(config, "us-east-1".into(), "123456789012".into(), 10);
        assert_eq!(mgr.function_count(), 2);
    }

    #[test]
    fn function_manager_get_existing_function() {
        let config = test_functions_config();
        let (mgr, _rxs) = FunctionManager::new(config, "us-east-1".into(), "123456789012".into(), 10);

        let f = mgr.get_function("my-python-func").unwrap();
        assert_eq!(f.name, "my-python-func");
        assert_eq!(f.runtime, "python3.12");
        assert_eq!(f.handler, "main.handler");
    }

    #[test]
    fn function_manager_get_missing_function_returns_error() {
        let config = test_functions_config();
        let (mgr, _rxs) = FunctionManager::new(config, "us-east-1".into(), "123456789012".into(), 10);

        let err = mgr.get_function("nonexistent").unwrap_err();
        assert!(matches!(err, ServiceError::ResourceNotFound(_)));
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn function_manager_list_returns_all_functions() {
        let config = test_functions_config();
        let (mgr, _rxs) = FunctionManager::new(config, "us-east-1".into(), "123456789012".into(), 10);

        let list = mgr.list_functions();
        assert_eq!(list.len(), 2);

        let names: Vec<&str> = list.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"my-python-func"));
        assert!(names.contains(&"my-node-func"));
    }

    #[test]
    fn function_manager_arn_generation() {
        let config = test_functions_config();
        let (mgr, _rxs) = FunctionManager::new(config, "us-west-2".into(), "123456789012".into(), 10);

        let arn = mgr.arn("my-python-func").unwrap();
        assert_eq!(arn, "arn:aws:lambda:us-west-2:123456789012:function:my-python-func");
    }

    #[test]
    fn function_manager_arn_missing_function_returns_error() {
        let config = test_functions_config();
        let (mgr, _rxs) = FunctionManager::new(config, "us-east-1".into(), "123456789012".into(), 10);

        let err = mgr.arn("nonexistent").unwrap_err();
        assert!(matches!(err, ServiceError::ResourceNotFound(_)));
    }

    #[test]
    fn function_manager_invocation_channels_created() {
        let config = test_functions_config();
        let (mgr, rxs) = FunctionManager::new(config, "us-east-1".into(), "123456789012".into(), 10);

        // Each function should have a tx and rx
        assert!(mgr.invocation_tx("my-python-func").is_ok());
        assert!(mgr.invocation_tx("my-node-func").is_ok());
        assert!(mgr.invocation_tx("nonexistent").is_err());

        assert!(rxs.contains_key("my-python-func"));
        assert!(rxs.contains_key("my-node-func"));
        assert_eq!(rxs.len(), 2);
    }

    #[test]
    fn function_manager_empty_config() {
        let config = FunctionsConfig {
            functions: HashMap::new(),
            runtime_images: HashMap::new(),
        };
        let (mgr, rxs) = FunctionManager::new(config, "us-east-1".into(), "000000000000".into(), 10);

        assert_eq!(mgr.function_count(), 0);
        assert!(mgr.list_functions().is_empty());
        assert!(rxs.is_empty());
        assert!(mgr.get_function("any").is_err());
    }

    // -- runtime_images map --------------------------------------------------

    #[test]
    fn runtime_images_parsed() {
        let dir = setup_dirs(&[]);
        let json = r#"{
            "functions": {},
            "runtime_images": {
                "python3.12": "public.ecr.aws/lambda/python:3.12",
                "nodejs20.x": "public.ecr.aws/lambda/nodejs:20"
            }
        }"#;

        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.runtime_images.len(), 2);
        assert_eq!(
            config.runtime_images["python3.12"],
            "public.ecr.aws/lambda/python:3.12"
        );
    }

    // -- reserved_concurrent_executions --------------------------------------

    #[test]
    fn reserved_concurrent_executions_valid() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "reserved_concurrent_executions": 10
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].reserved_concurrent_executions, Some(10));
    }

    #[test]
    fn reserved_concurrent_executions_omitted_means_none() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].reserved_concurrent_executions, None);
    }

    #[test]
    fn reserved_concurrent_executions_zero_invalid() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "reserved_concurrent_executions": 0
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("reserved_concurrent_executions"), "error: {msg}");
    }

    #[test]
    fn reserved_concurrent_executions_over_1000_invalid() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "m.h",
                    "code_path": "./code",
                    "reserved_concurrent_executions": 1001
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("reserved_concurrent_executions"), "error: {msg}");
    }

    // -- Known runtimes accepted ---------------------------------------------

    #[test]
    fn all_known_runtimes_accepted() {
        let dir = setup_dirs(&["code"]);
        let runtimes = vec![
            "python3.12", "nodejs20.x", "java21", "dotnet8",
            "ruby3.3", "provided.al2023", "custom",
        ];
        for rt in runtimes {
            let handler = if rt == "custom" { "bootstrap" } else { "m.h" };
            let image = if rt == "custom" {
                r#", "image": "img:latest""#
            } else {
                ""
            };
            let json = format!(
                r#"{{
                    "functions": {{
                        "f1": {{
                            "runtime": "{}",
                            "handler": "{}",
                            "code_path": "./code"{}
                        }}
                    }}
                }}"#,
                rt, handler, image
            );
            let result = parse_functions_config(&json, dir.path());
            assert!(result.is_ok(), "runtime '{}' should be accepted, got: {:?}", rt, result.err());
        }
    }

    // -- architecture --------------------------------------------------------

    #[test]
    fn architecture_defaults_to_host() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].architecture, default_architecture());
    }

    #[test]
    fn architecture_arm64_accepted() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "architecture": "arm64"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].architecture, "arm64");
    }

    #[test]
    fn architecture_x86_64_accepted() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "architecture": "x86_64"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].architecture, "x86_64");
    }

    #[test]
    fn architecture_invalid_rejected() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "architecture": "mips"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid architecture"), "error: {msg}");
        assert!(msg.contains("mips"), "error: {msg}");
    }

    #[test]
    fn architecture_empty_string_rejected() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "architecture": ""
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid architecture"), "error: {msg}");
    }

    // -- Layers ---------------------------------------------------------------

    #[test]
    fn parse_config_with_layers() {
        let dir = setup_dirs(&["code", "layers/common", "layers/utils"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "layers": ["./layers/common", "./layers/utils"]
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["f1"];
        assert_eq!(f.layers.len(), 2);
        assert!(f.layers[0].ends_with("layers/common"));
        assert!(f.layers[1].ends_with("layers/utils"));
    }

    #[test]
    fn parse_config_without_layers() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["f1"];
        assert!(f.layers.is_empty());
    }

    #[test]
    fn parse_config_layer_directory_traversal() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "layers": ["../../etc/secret"]
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "directory traversal");
    }

    #[test]
    fn parse_config_layer_path_not_found() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "layers": ["./nonexistent-layer"]
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "does not exist");
    }

    #[test]
    fn parse_config_layer_is_file_not_directory() {
        let dir = setup_dirs(&["code"]);
        // Create a file (not a directory) at the layer path
        std::fs::write(dir.path().join("not-a-dir"), "hello").unwrap();
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "layers": ["./not-a-dir"]
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        assert_validation_error_contains(&err, "does not exist");
    }

    #[test]
    fn parse_config_empty_layers_array() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "layers": []
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let f = &config.functions["f1"];
        assert!(f.layers.is_empty());
    }

    #[test]
    fn parse_config_max_retry_attempts_defaults_to_2() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].max_retry_attempts, 2);
    }

    #[test]
    fn parse_config_max_retry_attempts_zero() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "max_retry_attempts": 0
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(config.functions["f1"].max_retry_attempts, 0);
    }

    #[test]
    fn parse_config_max_retry_attempts_valid_values() {
        let dir = setup_dirs(&["code"]);
        for value in [0, 1, 2] {
            let json = format!(
                r#"{{
                    "functions": {{
                        "f1": {{
                            "runtime": "python3.12",
                            "handler": "main.handler",
                            "code_path": "./code",
                            "max_retry_attempts": {}
                        }}
                    }}
                }}"#,
                value
            );
            let config = parse_functions_config(&json, dir.path()).unwrap();
            assert_eq!(config.functions["f1"].max_retry_attempts, value);
        }
    }

    #[test]
    fn parse_config_max_retry_attempts_invalid_above_2() {
        let dir = setup_dirs(&["code"]);
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "max_retry_attempts": 3
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("max_retry_attempts"), "error should mention the field: {msg}");
        assert!(msg.contains("3"), "error should contain the invalid value: {msg}");
    }

    // -- Destination validation -----------------------------------------------

    #[test]
    fn on_success_destination_valid_function() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("code")).unwrap();
        let json = r#"{
            "functions": {
                "producer": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "on_success": "consumer"
                },
                "consumer": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(
            config.functions["producer"].on_success,
            Some("consumer".into())
        );
        assert!(config.functions["consumer"].on_success.is_none());
    }

    #[test]
    fn on_failure_destination_valid_function() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("code")).unwrap();
        let json = r#"{
            "functions": {
                "producer": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "on_failure": "dlq-handler"
                },
                "dlq-handler": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert_eq!(
            config.functions["producer"].on_failure,
            Some("dlq-handler".into())
        );
    }

    #[test]
    fn on_success_destination_unknown_function_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("code")).unwrap();
        let json = r#"{
            "functions": {
                "producer": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "on_success": "nonexistent"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("on_success"), "error should mention on_success: {msg}");
        assert!(msg.contains("nonexistent"), "error should mention the destination: {msg}");
    }

    #[test]
    fn on_failure_destination_unknown_function_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("code")).unwrap();
        let json = r#"{
            "functions": {
                "producer": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "on_failure": "nonexistent"
                }
            }
        }"#;
        let err = parse_functions_config(json, dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("on_failure"), "error should mention on_failure: {msg}");
        assert!(msg.contains("nonexistent"), "error should mention the destination: {msg}");
    }

    #[test]
    fn both_destinations_configured() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("code")).unwrap();
        let json = r#"{
            "functions": {
                "processor": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code",
                    "on_success": "success-fn",
                    "on_failure": "failure-fn"
                },
                "success-fn": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                },
                "failure-fn": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        let proc = &config.functions["processor"];
        assert_eq!(proc.on_success, Some("success-fn".into()));
        assert_eq!(proc.on_failure, Some("failure-fn".into()));
    }

    #[test]
    fn destinations_default_to_none() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("code")).unwrap();
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, dir.path()).unwrap();
        assert!(config.functions["f1"].on_success.is_none());
        assert!(config.functions["f1"].on_failure.is_none());
    }
}
