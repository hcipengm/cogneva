// ---------------------------------------------------------------------------
// Hot-reload watcher
// ---------------------------------------------------------------------------

use crate::config_loader::AppConfig;
use cog_core::SFResult;
use notify::Watcher;
use std::{collections::HashSet, path::PathBuf};
use tokio::sync::watch;

/// Watches configuration files for changes and broadcasts updated [`AppConfig`] values.
/// Uses the `notify` crate for cross-platform file-system events and
/// [`tokio::sync::watch`] for cheap multi-consumer broadcasts.
#[derive(Debug)]
pub struct ConfigWatcher {
    rx: watch::Receiver<AppConfig>,
}

impl ConfigWatcher {
    /// Start watching the given config paths.
    /// On creation the current [`config_loader::load()`] result is sent as the initial
    /// value.  Every time one of the watched files is modified the config is
    /// reloaded and subscribers receive the new value.
    /// The returned [`notify::RecommendedWatcher`] must be kept alive; dropping
    /// it stops the background watcher thread.
    pub fn new(paths: Vec<PathBuf>) -> SFResult<(Self, notify::RecommendedWatcher)> {
        let initial = crate::config_loader::load();
        let (tx, rx) = watch::channel(initial);

        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(event) => {
                    if event.kind.is_modify() || event.kind.is_create() {
                        let new_config = crate::config_loader::load();
                        if tx.send(new_config).is_err() {
                            tracing::debug!("ConfigWatcher: all subscribers dropped");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Config watcher error: {}", e);
                }
            },
        )
        .map_err(|e| cog_core::SFError::Config(format!("notify error: {e}")))?;

        // Watch parent directories rather than the files themselves. Besides
        // allowing a config file that does not exist at startup to be created
        // later, this also keeps the watch alive when Kubernetes updates a
        // ConfigMap by replacing its `..data` symlink.
        let mut watched_dirs = HashSet::new();
        for path in &paths {
            let dir = if path.is_dir() {
                path.clone()
            } else {
                path.parent()
                    .unwrap_or_else(|| std::path::Path::new("."))
                    .into()
            };

            if !watched_dirs.insert(dir.clone()) {
                continue;
            }
            watcher
                .watch(&dir, notify::RecursiveMode::NonRecursive)
                .map_err(|e| cog_core::SFError::Config(format!("watch error: {e}")))?;
        }

        Ok((Self { rx }, watcher))
    }

    /// Convenience constructor that watches the standard config locations:
    /// 1. `$COGNEVA_CONFIG_PATH` (default `/etc/cogneva/cogneva.json`, same as the loader)
    /// 2. `cogneva.{env}.json`
    pub fn watch_default() -> SFResult<(Self, notify::RecommendedWatcher)> {
        let base_path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| crate::config_loader::DEFAULT_CONFIG_PATH.into());
        let env = std::env::var("COGNEVA_ENV").unwrap_or_else(|_| "development".into());
        let env_path = base_path.replace(".json", &format!(".{}.json", env));

        let mut paths = vec![PathBuf::from(base_path)];
        if env_path != paths[0].to_string_lossy() {
            paths.push(PathBuf::from(env_path));
        }
        Self::new(paths)
    }

    /// Subscribe to config changes.
    pub fn subscribe(&self) -> watch::Receiver<AppConfig> {
        self.rx.clone()
    }

    /// Get a clone of the current config value.
    #[allow(dead_code)]
    pub fn current(&self) -> AppConfig {
        self.rx.borrow().clone()
    }
}

#[cfg(test)]
mod watcher_tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn watcher_reloads_on_file_change() {
        let _lock = crate::config_loader::ENV_LOCK.lock().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("cogneva.json");

        // Write initial config
        {
            let mut file = std::fs::File::create(&config_path).unwrap();
            file.write_all(
                br#"{"app": {"name": "test-v1", "version": "1.0.0", "log_level": "info", "data_dir": "/tmp", "config_dir": "/tmp", "app_dir": "/tmp"}}"#,
            )
            .unwrap();
        }

        // Point config_loader::load() at our temp file so reloads pick it up.
        let _g1 = crate::config_loader::EnvGuard::set(
            "COGNEVA_CONFIG_PATH",
            &config_path.to_string_lossy(),
        );
        // Clear any env vars that could pollute config_loader::load() from parallel tests.
        let _g2 = crate::config_loader::EnvGuard::remove("COGNEVA_APP_NAME");

        let (watcher, _notify_watcher) = ConfigWatcher::new(vec![config_path.clone()]).unwrap();
        let mut sub = watcher.subscribe();

        // Initial value
        assert_eq!(sub.borrow().app.name, "test-v1");

        // Wait a bit for the watcher to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Modify the file
        {
            let mut file = std::fs::File::create(&config_path).unwrap();
            file.write_all(
                br#"{"app": {"name": "test-v2", "version": "1.0.0", "log_level": "info", "data_dir": "/tmp", "config_dir": "/tmp", "app_dir": "/tmp"}}"#,
            )
            .unwrap();
        }

        // Wait for the notify event + reload
        tokio::time::timeout(Duration::from_secs(5), sub.changed())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(sub.borrow().app.name, "test-v2");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn watcher_reloads_when_missing_config_is_created() {
        let _lock = crate::config_loader::ENV_LOCK.lock().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("cogneva.json");
        let _config_path = crate::config_loader::EnvGuard::set(
            "COGNEVA_CONFIG_PATH",
            &config_path.to_string_lossy(),
        );
        let _app_name = crate::config_loader::EnvGuard::remove("COGNEVA_APP_NAME");

        let (watcher, _notify_watcher) = ConfigWatcher::new(vec![config_path.clone()]).unwrap();
        let mut sub = watcher.subscribe();
        assert_ne!(sub.borrow().app.name, "created-config");

        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(
            &config_path,
            br#"{"app": {"name": "created-config", "version": "1.0.0", "log_level": "info", "data_dir": "/tmp", "config_dir": "/tmp", "app_dir": "/tmp"}}"#,
        )
        .unwrap();

        tokio::time::timeout(Duration::from_secs(5), sub.changed())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(sub.borrow().app.name, "created-config");
    }
}
