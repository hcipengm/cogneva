//! Infrastructure assembly — config, PostgreSQL pools, Redis, auth, prompt manager.

use crate::config_loader::AppConfig;
use crate::{config_watcher, daemon, pidfile, platform};
use tracing::{info, warn};

/// Load configuration and apply backward-compatibility fixes.
pub fn load_and_normalize_config() -> AppConfig {
    let mut config = crate::config_loader::load();

    // Backward compatibility: promote legacy `nats_url` string into `nats.urls`.
    if config.core.dag_executor.nats.urls == ["nats://127.0.0.1:4222"] {
        if let Some(ref old_url) = config.core.dag_executor.nats_url {
            config.core.dag_executor.nats.urls = vec![old_url.clone()];
        }
    }
    config
}

/// Daemon control + PID file.
pub fn init_daemon_and_pidfile() -> (daemon::DaemonControl, Option<pidfile::PidFile>) {
    let daemon = daemon::DaemonControl::new();
    let pid_path = std::env::var("COGNEVA_PID_FILE").unwrap_or_else(|_| {
        platform::PlatformPaths::pid_file()
            .to_string_lossy()
            .into_owned()
    });
    let pid_file = match pidfile::PidFile::new(&pid_path) {
        Ok(pid) => {
            info!("PID file created: {}", pid_path);
            Some(pid)
        }
        Err(e) => {
            warn!("Failed to create PID file at {}: {}", pid_path, e);
            None
        }
    };
    (daemon, pid_file)
}

/// Config hot-reload watcher.
pub fn init_config_watcher() -> (
    Option<config_watcher::ConfigWatcher>,
    Option<notify::RecommendedWatcher>,
) {
    match config_watcher::ConfigWatcher::watch_default() {
        Ok((watcher, notify)) => {
            info!("ConfigWatcher started");
            (Some(watcher), Some(notify))
        }
        Err(e) => {
            warn!("ConfigWatcher failed: {}", e);
            (None, None)
        }
    }
}
