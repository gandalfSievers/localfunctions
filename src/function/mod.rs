use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tokio::sync::mpsc;

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

    #[error("function '{name}': code_path '{path}' is invalid — directory traversal is not allowed")]
    DirectoryTraversal { name: String, path: String },

    #[error("function '{name}': runtime 'custom' requires an 'image' field")]
    MissingImageForCustomRuntime { name: String },
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
    runtime: String,
    handler: String,
    code_path: String,
    timeout: Option<u64>,
    memory_size: Option<u64>,
    #[serde(default)]
    environment: HashMap<String, String>,
    image: Option<String>,
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

/// Parse and validate functions configuration from a JSON string.
#[allow(dead_code)]
pub fn parse_functions_config(
    json: &str,
    base_dir: &Path,
) -> Result<FunctionsConfig, FunctionConfigError> {
    let raw: RawFunctionsFile = serde_json::from_str(json)?;

    let mut functions = HashMap::new();

    for (name, entry) in raw.functions {
        validate_function_name(&name)?;
        validate_timeout(&name, entry.timeout.unwrap_or(30))?;
        validate_memory_size(&name, entry.memory_size.unwrap_or(128))?;

        let code_path = canonicalize_code_path(&name, &entry.code_path, base_dir)?;

        if entry.runtime == "custom" && entry.image.is_none() {
            return Err(FunctionConfigError::MissingImageForCustomRuntime {
                name: name.clone(),
            });
        }

        let config = FunctionConfig {
            name: name.clone(),
            runtime: entry.runtime,
            handler: entry.handler,
            code_path,
            timeout: entry.timeout.unwrap_or(30),
            memory_size: entry.memory_size.unwrap_or(128),
            environment: entry.environment,
            image: entry.image,
        };

        functions.insert(name, config);
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn base() -> PathBuf {
        PathBuf::from("/project")
    }

    // -- Valid config --------------------------------------------------------

    #[test]
    fn parse_valid_full_config() {
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

        let config = parse_functions_config(json, &base()).unwrap();
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
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;

        let config = parse_functions_config(json, &base()).unwrap();
        let f = &config.functions["f1"];
        assert_eq!(f.timeout, 30);
        assert_eq!(f.memory_size, 128);
        assert!(f.environment.is_empty());
        assert!(f.image.is_none());
        assert!(config.runtime_images.is_empty());
    }

    #[test]
    fn parse_custom_runtime_with_image() {
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

        let config = parse_functions_config(json, &base()).unwrap();
        let f = &config.functions["custom-func"];
        assert_eq!(f.runtime, "custom");
        assert_eq!(f.image, Some("my-custom:latest".to_string()));
    }

    // -- Missing required fields ---------------------------------------------

    #[test]
    fn error_missing_runtime() {
        let json = r#"{
            "functions": {
                "f1": {
                    "handler": "main.handler",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ParseJson(_)));
        assert!(err.to_string().contains("runtime"));
    }

    #[test]
    fn error_missing_handler() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ParseJson(_)));
        assert!(err.to_string().contains("handler"));
    }

    #[test]
    fn error_missing_code_path() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "main.handler"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ParseJson(_)));
        assert!(err.to_string().contains("code_path"));
    }

    #[test]
    fn error_missing_functions_key() {
        let json = r#"{ "runtime_images": {} }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ParseJson(_)));
    }

    // -- Invalid function names ----------------------------------------------

    #[test]
    fn error_function_name_with_spaces() {
        let json = r#"{
            "functions": {
                "bad name": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(
            err,
            FunctionConfigError::InvalidFunctionName { .. }
        ));
        assert!(err.to_string().contains("bad name"));
    }

    #[test]
    fn error_function_name_with_special_chars() {
        let json = r#"{
            "functions": {
                "func@#$": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(
            err,
            FunctionConfigError::InvalidFunctionName { .. }
        ));
    }

    #[test]
    fn error_function_name_too_long() {
        let long_name = "a".repeat(65);
        let json = format!(
            "{{\n\"functions\": {{\n\"{}\": {{\n\"runtime\": \"python3.12\",\n\"handler\": \"h\",\n\"code_path\": \"./code\"\n}}\n}}\n}}",
            long_name
        );
        let err = parse_functions_config(&json, &base()).unwrap_err();
        assert!(matches!(
            err,
            FunctionConfigError::InvalidFunctionName { .. }
        ));
        assert!(err.to_string().contains("64"));
    }

    #[test]
    fn valid_function_name_64_chars() {
        let name = "a".repeat(64);
        let json = format!(
            "{{\n\"functions\": {{\n\"{}\": {{\n\"runtime\": \"python3.12\",\n\"handler\": \"h\",\n\"code_path\": \"./code\"\n}}\n}}\n}}",
            name
        );
        let config = parse_functions_config(&json, &base()).unwrap();
        assert!(config.functions.contains_key(&name));
    }

    #[test]
    fn valid_function_name_with_hyphens_and_underscores() {
        let json = r#"{
            "functions": {
                "my-func_v2": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code"
                }
            }
        }"#;
        let config = parse_functions_config(json, &base()).unwrap();
        assert!(config.functions.contains_key("my-func_v2"));
    }

    // -- Directory traversal -------------------------------------------------

    #[test]
    fn error_directory_traversal_dotdot() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "../../../etc/passwd"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::DirectoryTraversal { .. }));
    }

    #[test]
    fn error_directory_traversal_embedded() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./functions/../../secret"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::DirectoryTraversal { .. }));
    }

    #[test]
    fn valid_code_path_stays_within_base() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./functions/../functions/code"
                }
            }
        }"#;
        let config = parse_functions_config(json, &base()).unwrap();
        let f = &config.functions["f1"];
        assert!(f.code_path.starts_with("/project"));
    }

    // -- Timeout boundary values ---------------------------------------------

    #[test]
    fn error_timeout_zero() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code",
                    "timeout": 0
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::InvalidTimeout { .. }));
    }

    #[test]
    fn error_timeout_901() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code",
                    "timeout": 901
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::InvalidTimeout { .. }));
    }

    #[test]
    fn valid_timeout_1() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code",
                    "timeout": 1
                }
            }
        }"#;
        let config = parse_functions_config(json, &base()).unwrap();
        assert_eq!(config.functions["f1"].timeout, 1);
    }

    #[test]
    fn valid_timeout_900() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code",
                    "timeout": 900
                }
            }
        }"#;
        let config = parse_functions_config(json, &base()).unwrap();
        assert_eq!(config.functions["f1"].timeout, 900);
    }

    // -- Memory size ---------------------------------------------------------

    #[test]
    fn error_memory_size_zero() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "python3.12",
                    "handler": "h",
                    "code_path": "./code",
                    "memory_size": 0
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(
            err,
            FunctionConfigError::InvalidMemorySize { .. }
        ));
    }

    // -- Custom runtime requires image ---------------------------------------

    #[test]
    fn error_custom_runtime_without_image() {
        let json = r#"{
            "functions": {
                "f1": {
                    "runtime": "custom",
                    "handler": "bootstrap",
                    "code_path": "./code"
                }
            }
        }"#;
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(
            err,
            FunctionConfigError::MissingImageForCustomRuntime { .. }
        ));
    }

    // -- Malformed JSON ------------------------------------------------------

    #[test]
    fn error_malformed_json() {
        let json = "{ not valid json }";
        let err = parse_functions_config(json, &base()).unwrap_err();
        assert!(matches!(err, FunctionConfigError::ParseJson(_)));
    }

    // -- load_functions_config (file-based) -----------------------------------

    #[test]
    fn error_missing_file() {
        let err = load_functions_config(Path::new("/nonexistent/functions.json"), &base()).unwrap_err();
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
        parse_functions_config(json, &base()).unwrap()
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
        let json = r#"{
            "functions": {},
            "runtime_images": {
                "python3.12": "public.ecr.aws/lambda/python:3.12",
                "nodejs20.x": "public.ecr.aws/lambda/nodejs:20"
            }
        }"#;

        let config = parse_functions_config(json, &base()).unwrap();
        assert_eq!(config.runtime_images.len(), 2);
        assert_eq!(
            config.runtime_images["python3.12"],
            "public.ecr.aws/lambda/python:3.12"
        );
    }
}
