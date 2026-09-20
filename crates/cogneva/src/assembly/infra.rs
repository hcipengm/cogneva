//! Infrastructure assembly — config, PostgreSQL pools, Redis, auth, prompt manager.

use std::path::Path;

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
///
/// The platform default for the PID file is the FHS runtime directory, and the
/// documented graceful reload reaches the process through it. That directory is
/// root-owned while the process runs as a plain user, so under the container
/// deployment the write fails on every start even though the handle it provides
/// is just as valid there. Fall back rather than give up, and name the path that
/// actually got used. An explicitly configured path is not negotiated with: an
/// unreachable path there is a configuration error, and falling back would hide
/// it behind an operator who trusts the configured location.
pub fn init_daemon_and_pidfile() -> (daemon::DaemonControl, Option<pidfile::PidFile>) {
    let daemon = daemon::DaemonControl::new();
    let pid_file = match std::env::var("COGNEVA_PID_FILE") {
        Ok(path) => match pidfile::PidFile::new(&path) {
            Ok(pid) => Some(pid),
            Err(e) => {
                warn!("Failed to create PID file at {}: {}", path, e);
                None
            }
        },
        Err(_) => create_pid_file(
            &platform::PlatformPaths::pid_file(),
            &platform::PlatformPaths::tmp_dir().join("cogneva.pid"),
        ),
    };
    if let Some(pid) = &pid_file {
        info!("PID file created: {}", pid.path().display());
    }
    (daemon, pid_file)
}

/// Write the PID file, preferring `preferred` and falling back to `fallback`.
/// The returned guard knows the path it actually landed on.
fn create_pid_file(preferred: &Path, fallback: &Path) -> Option<pidfile::PidFile> {
    match pidfile::PidFile::new(preferred) {
        Ok(pid) => Some(pid),
        Err(e) => match pidfile::PidFile::new(fallback) {
            Ok(pid) => {
                info!(
                    preferred = %preferred.display(),
                    error = %e,
                    "preferred PID file path is not writable in this deployment form; \
                     using the per-process temp dir instead"
                );
                Some(pid)
            }
            Err(e2) => {
                warn!(
                    preferred = %preferred.display(),
                    fallback = %fallback.display(),
                    "failed to create PID file: {e}; fallback also failed: {e2}"
                );
                None
            }
        },
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A path that cannot be written to no matter which user runs the test.
    /// Permission bits are not usable for this: the assertion would depend on
    /// whether the suite happens to run as root, which bypasses them.
    fn unusable_path(dir: &Path) -> std::path::PathBuf {
        let blocker = dir.join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();
        blocker.join("cogneva.pid")
    }

    #[test]
    fn pid_file_prefers_the_platform_path_when_it_is_writable() {
        let tmp = tempfile::tempdir().unwrap();
        let preferred = tmp.path().join("run/cogneva.pid");
        let fallback = tmp.path().join("tmp/cogneva.pid");

        let pid = create_pid_file(&preferred, &fallback).expect("preferred path should work");

        assert_eq!(pid.path(), preferred.as_path());
        assert!(preferred.exists());
        assert!(!fallback.exists(), "写进去了就不该再动退路");
    }

    #[test]
    fn pid_file_falls_back_and_reports_where_it_landed() {
        let tmp = tempfile::tempdir().unwrap();
        let preferred = unusable_path(tmp.path());
        let fallback = tmp.path().join("tmp/cogneva.pid");

        let pid = create_pid_file(&preferred, &fallback).expect("fallback should work");

        assert_eq!(pid.path(), fallback.as_path(), "必须报出真实落点");
        assert!(fallback.exists());
    }

    #[test]
    fn pid_file_gives_up_when_neither_path_works() {
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();

        assert!(create_pid_file(&blocker.join("a.pid"), &blocker.join("b.pid")).is_none());
    }
}
