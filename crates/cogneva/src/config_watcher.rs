// ---------------------------------------------------------------------------
// Hot-reload watcher
// ---------------------------------------------------------------------------

use crate::config_loader::AppConfig;
use cog_core::SFResult;
use notify::Watcher;
use std::path::PathBuf;
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
    /// On creation the current [`crate::config_loader::load`] result is sent as the initial
    /// value.  Every time one of the watched files is modified the config is
    /// reloaded and subscribers receive the new value.
    /// A reload that cannot be read or parsed is *not* published — the last
    /// known-good value stands and the failure is logged — so a partially
    /// written file never reaches subscribers as a degraded configuration.
    /// The returned [`notify::RecommendedWatcher`] must be kept alive; dropping
    /// it stops the background watcher thread.
    pub fn new(paths: Vec<PathBuf>) -> SFResult<(Self, notify::RecommendedWatcher)> {
        let initial = crate::config_loader::load();
        let (tx, rx) = watch::channel(initial);

        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(event) => {
                    if event.kind.is_modify() || event.kind.is_create() {
                        // A modify event fires the moment a file is truncated —
                        // before the writer has put anything back — and configmap
                        // volumes swap a symlink underneath the watched path. A
                        // failed read must not be published: `load()` would hand
                        // back the default config, silently stripping the running
                        // app of its entire configuration. Keep the last
                        // known-good value and say so instead.
                        match crate::config_loader::try_load() {
                            Ok(new_config) => {
                                if tx.send(new_config).is_err() {
                                    tracing::debug!("ConfigWatcher: all subscribers dropped");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    paths = ?event.paths,
                                    "ConfigWatcher: reload failed, keeping the last known-good configuration: {e}"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Config watcher error: {}", e);
                }
            },
        )
        .map_err(|e| cog_core::SFError::Config(format!("notify error: {e}")))?;

        for path in &paths {
            // Skip files that don't exist yet — their creation is caught by the
            // parent-directory watch; a hard error here would kill the watcher.
            if !path.exists() {
                continue;
            }
            watcher
                .watch(path, notify::RecursiveMode::NonRecursive)
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
        // K8s configmap volumes swap a `..data` symlink on update — the watch on
        // the file itself dies with the old inode. Watching the parent directory
        // catches the swap (create/rename events) so hot reload actually fires.
        let mut dirs: Vec<PathBuf> = Vec::new();
        for p in &paths {
            if let Some(parent) = p.parent() {
                if !dirs.iter().any(|d| d == parent) {
                    dirs.push(parent.to_path_buf());
                }
            }
        }
        paths.extend(dirs);
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

    /// A modify event fires while the file is still truncated — that is exactly
    /// what a `File::create` + `write_all` save looks like from the watcher's
    /// side. Reloading then fails, and publishing the default config it would
    /// fall back to strips the running app of its whole configuration.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn a_partially_written_config_is_never_published() {
        let _lock = crate::config_loader::ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("cogneva.json");
        std::fs::write(
            &config_path,
            br#"{"app": {"name": "test-v1", "version": "1.0.0", "log_level": "info", "data_dir": "/tmp", "config_dir": "/tmp", "app_dir": "/tmp"}}"#,
        )
        .unwrap();

        let _g1 = crate::config_loader::EnvGuard::set(
            "COGNEVA_CONFIG_PATH",
            &config_path.to_string_lossy(),
        );
        let _g2 = crate::config_loader::EnvGuard::remove("COGNEVA_APP_NAME");

        let (watcher, _notify_watcher) = ConfigWatcher::new(vec![config_path.clone()]).unwrap();
        let mut sub = watcher.subscribe();
        assert_eq!(sub.borrow().app.name, "test-v1");
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The truncated state a writer leaves behind mid-save.
        std::fs::File::create(&config_path).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(
            sub.borrow().app.name,
            "test-v1",
            "a truncated config must not be published as a degraded default"
        );
        assert!(
            !matches!(sub.has_changed(), Ok(true)),
            "no notification may be pending for a reload that failed"
        );

        // The watcher is still live: the next complete write still lands.
        std::fs::write(
            &config_path,
            br#"{"app": {"name": "test-v2", "version": "1.0.0", "log_level": "info", "data_dir": "/tmp", "config_dir": "/tmp", "app_dir": "/tmp"}}"#,
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), sub.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sub.borrow().app.name, "test-v2");
    }
}
