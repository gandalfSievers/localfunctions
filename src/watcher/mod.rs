use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::RecursiveMode;
use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::container::ContainerRegistry;
use crate::function::FunctionsConfig;

/// Watches function code directories for changes and recycles containers when
/// code is modified.
///
/// Each function's `code_path` is watched recursively. When a change is
/// detected, all warm containers for that function are stopped and removed so
/// the next invocation picks up updated code.
pub async fn watch_code_paths(
    functions: Arc<FunctionsConfig>,
    container_registry: Arc<ContainerRegistry>,
    debounce_ms: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // Build a map from watched directory -> function name(s).
    let mut path_to_functions: HashMap<PathBuf, Vec<String>> = HashMap::new();
    for (name, func) in &functions.functions {
        // image_uri functions have no local code_path to watch.
        if func.image_uri.is_some() {
            continue;
        }
        let code_path = &func.code_path;
        if !code_path.exists() {
            warn!(
                function = %name,
                path = %code_path.display(),
                "code_path does not exist, skipping file watch"
            );
            continue;
        }
        path_to_functions
            .entry(code_path.clone())
            .or_default()
            .push(name.clone());
    }

    if path_to_functions.is_empty() {
        info!("no code paths to watch for hot reload");
        return;
    }

    // Channel for receiving debounced events from the synchronous notify watcher.
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(256);

    let debounce_duration = Duration::from_millis(debounce_ms);

    // The notify debouncer runs on a background thread; we forward events into
    // our async channel.
    let mut debouncer = match new_debouncer(debounce_duration, move |result| {
        if let Ok(events) = result {
            // Best-effort send — if the channel is full we drop events rather
            // than blocking the filesystem notification thread.
            let _ = event_tx.blocking_send(events);
        }
    }) {
        Ok(d) => d,
        Err(e) => {
            error!(%e, "failed to create file watcher for hot reload");
            return;
        }
    };

    // Watch each unique code_path directory.
    for path in path_to_functions.keys() {
        if let Err(e) = debouncer.watcher().watch(path, RecursiveMode::Recursive) {
            error!(
                path = %path.display(),
                %e,
                "failed to watch code path"
            );
        } else {
            let functions: Vec<&str> = path_to_functions[path]
                .iter()
                .map(|s| s.as_str())
                .collect();
            info!(
                path = %path.display(),
                functions = ?functions,
                "watching code path for changes"
            );
        }
    }

    let stop_timeout = Duration::from_secs(5);

    loop {
        tokio::select! {
            Some(events) = event_rx.recv() => {
                // Collect the set of affected function names from the changed
                // paths. A single batch of debounced events may touch multiple
                // directories / functions.
                let mut affected: HashMap<String, PathBuf> = HashMap::new();
                for event in &events {
                    if event.kind != DebouncedEventKind::Any {
                        continue;
                    }
                    // Walk up from the changed file to find the watched root.
                    for (watched_path, func_names) in &path_to_functions {
                        if event.path.starts_with(watched_path) {
                            for name in func_names {
                                affected.entry(name.clone()).or_insert_with(|| watched_path.clone());
                            }
                        }
                    }
                }

                for (function_name, code_path) in &affected {
                    info!(
                        function = %function_name,
                        code_path = %code_path.display(),
                        "code change detected, recycling containers"
                    );
                    let removed = container_registry
                        .stop_and_remove_by_function(function_name, stop_timeout)
                        .await;
                    if removed.is_empty() {
                        info!(
                            function = %function_name,
                            "no warm containers to recycle"
                        );
                    } else {
                        info!(
                            function = %function_name,
                            count = removed.len(),
                            "recycled containers for code change"
                        );
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                info!("file watcher shutting down");
                break;
            }
        }
    }
}

/// Returns the set of unique code_path directories that would be watched,
/// useful for testing and diagnostics.
#[allow(dead_code)]
pub fn watched_paths(functions: &FunctionsConfig) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = functions
        .functions
        .values()
        .filter(|f| f.image_uri.is_none() && f.code_path.exists())
        .map(|f| f.code_path.clone())
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FunctionConfig;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn make_function(name: &str, code_path: PathBuf) -> FunctionConfig {
        FunctionConfig {
            name: name.to_string(),
            runtime: "python3.12".to_string(),
            handler: "handler.handler".to_string(),
            code_path,
            timeout: 30,
            memory_size: 128,
            ephemeral_storage_mb: 512,
            environment: HashMap::new(),
            image: None,
            image_uri: None,
            reserved_concurrent_executions: None,
            architecture: "x86_64".to_string(),
            layers: Vec::new(),
            function_url_enabled: false,
            payload_format_version: "2.0".to_string(),
            max_retry_attempts: 0,
            on_success: None,
            on_failure: None,
        }
    }

    #[test]
    fn watched_paths_returns_existing_directories() {
        let tmp = TempDir::new().unwrap();
        let dir_a = tmp.path().join("func_a");
        let dir_b = tmp.path().join("func_b");
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();

        let mut functions = HashMap::new();
        functions.insert("func-a".into(), make_function("func-a", dir_a.clone()));
        functions.insert("func-b".into(), make_function("func-b", dir_b.clone()));

        let config = FunctionsConfig {
            functions,
            runtime_images: HashMap::new(),
            event_source_mappings: Vec::new(),
            sns_subscriptions: Vec::new(),
        };

        let paths = watched_paths(&config);
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&dir_a));
        assert!(paths.contains(&dir_b));
    }

    #[test]
    fn watched_paths_skips_nonexistent_directories() {
        let tmp = TempDir::new().unwrap();
        let existing = tmp.path().join("exists");
        let missing = tmp.path().join("missing");
        std::fs::create_dir_all(&existing).unwrap();

        let mut functions = HashMap::new();
        functions.insert("exists".into(), make_function("exists", existing.clone()));
        functions.insert("missing".into(), make_function("missing", missing));

        let config = FunctionsConfig {
            functions,
            runtime_images: HashMap::new(),
            event_source_mappings: Vec::new(),
            sns_subscriptions: Vec::new(),
        };

        let paths = watched_paths(&config);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], existing);
    }

    #[test]
    fn watched_paths_skips_image_uri_functions() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("func");
        std::fs::create_dir_all(&dir).unwrap();

        let mut func = make_function("img-func", dir);
        func.image_uri = Some("my-image:latest".to_string());

        let mut functions = HashMap::new();
        functions.insert("img-func".into(), func);

        let config = FunctionsConfig {
            functions,
            runtime_images: HashMap::new(),
            event_source_mappings: Vec::new(),
            sns_subscriptions: Vec::new(),
        };

        let paths = watched_paths(&config);
        assert!(paths.is_empty());
    }

    #[test]
    fn watched_paths_deduplicates_shared_code_path() {
        let tmp = TempDir::new().unwrap();
        let shared_dir = tmp.path().join("shared");
        std::fs::create_dir_all(&shared_dir).unwrap();

        let mut functions = HashMap::new();
        functions.insert(
            "func-a".into(),
            make_function("func-a", shared_dir.clone()),
        );
        functions.insert(
            "func-b".into(),
            make_function("func-b", shared_dir.clone()),
        );

        let config = FunctionsConfig {
            functions,
            runtime_images: HashMap::new(),
            event_source_mappings: Vec::new(),
            sns_subscriptions: Vec::new(),
        };

        let paths = watched_paths(&config);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], shared_dir);
    }
}
