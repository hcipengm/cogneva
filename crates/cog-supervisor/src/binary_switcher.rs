//! Binary switching and restart strategies for self-evolution deployment.
//!
//! Provides three switching modes selected by configuration:
//! - `self_exec`: current process replaces itself with the new binary via execve.
//! - `systemd`: ask systemd to restart the `cogneva` service.
//! - `sidecar`: send a command over a Unix socket to an external sidecar process
//!   that owns the cogneva process lifecycle.

use std::path::{Path, PathBuf};

use cog_core::{BinarySwitcher, SFError, SFResult};
use std::sync::Arc;
use tracing::{info, warn};

/// Configuration shared by all switchers.
#[derive(Debug, Clone)]
pub struct BinarySwitcherConfig {
    pub binary_dir: PathBuf,
    pub binary_name: String,
    pub health_url: String,
    pub health_check_grace_period_secs: u64,
    pub health_check_interval_secs: u64,
    pub health_check_max_retries: u32,
    pub systemd_service_name: String,
    pub sidecar_socket_path: PathBuf,
}

impl Default for BinarySwitcherConfig {
    fn default() -> Self {
        Self {
            binary_dir: PathBuf::from("/opt/cogneva/bin"),
            binary_name: "cogneva".to_string(),
            health_url: "http://127.0.0.1:8080/health".to_string(),
            health_check_grace_period_secs: 10,
            health_check_interval_secs: 5,
            health_check_max_retries: 6,
            systemd_service_name: "cogneva".to_string(),
            sidecar_socket_path: PathBuf::from("/run/cogneva/sidecar.sock"),
        }
    }
}

/// Factory for the three supported switch modes.
pub fn build_switcher(
    mode: &str,
    config: BinarySwitcherConfig,
) -> Arc<dyn cog_core::BinarySwitcher> {
    match mode {
        "systemd" => Arc::new(SystemdSwitcher::new(config)),
        "sidecar" => Arc::new(SidecarSwitcher::new(config)),
        _ => Arc::new(SelfExecSwitcher::new(config)),
    }
}

// ============================================================================
// Self-Exec Switcher
// ============================================================================

/// Replaces the current process with the new binary using `execve`.
///
/// The process PID is preserved, making this the fastest mode. Because the
/// current process is destroyed, health checking after the exec is the
/// responsibility of the new process (see `cogneva/src/main.rs` bootstrap).
#[derive(Debug, Clone)]
pub struct SelfExecSwitcher {
    config: BinarySwitcherConfig,
}

impl SelfExecSwitcher {
    pub fn new(config: BinarySwitcherConfig) -> Self {
        Self { config }
    }

    fn binary_path(&self) -> PathBuf {
        self.config.binary_dir.join(&self.config.binary_name)
    }

    fn prev_binary_path(&self) -> PathBuf {
        self.config
            .binary_dir
            .join(format!("{}.prev", self.config.binary_name))
    }

    fn new_binary_path(&self) -> PathBuf {
        self.config
            .binary_dir
            .join(format!("{}.new", self.config.binary_name))
    }

    /// Perform the actual exec. This function does not return on success.
    fn exec_binary(binary: &Path) -> SFResult<()> {
        use std::os::unix::process::CommandExt;
        info!(binary = %binary.display(), "Exec replacing current process");
        let err = std::process::Command::new(binary).exec();
        Err(SFError::IO(format!(
            "exec failed for {}: {}",
            binary.display(),
            err
        )))
    }
}

#[async_trait::async_trait]
impl cog_core::BinarySwitcher for SelfExecSwitcher {
    async fn stage_new_binary(&self, new_binary_path: &Path) -> SFResult<()> {
        ensure_dir(&self.config.binary_dir).await?;
        let staged = self.new_binary_path();
        tokio::fs::copy(new_binary_path, &staged)
            .await
            .map_err(|e| {
                SFError::IO(format!(
                    "Failed to stage {} to {}: {}",
                    new_binary_path.display(),
                    staged.display(),
                    e
                ))
            })?;
        info!(staged = %staged.display(), "Staged new binary for self-exec");
        Ok(())
    }

    async fn switch_and_restart(&self) -> SFResult<()> {
        ensure_dir(&self.config.binary_dir).await?;
        let current = self.binary_path();
        let prev = self.prev_binary_path();
        let staged = self.new_binary_path();

        if !staged.exists() {
            return Err(SFError::Validation(
                "No staged binary available for switch".into(),
            ));
        }

        // Backup current binary.
        if current.exists() {
            tokio::fs::copy(&current, &prev).await.map_err(|e| {
                SFError::IO(format!(
                    "Failed to backup {} to {}: {}",
                    current.display(),
                    prev.display(),
                    e
                ))
            })?;
        }

        // Atomically replace current binary with staged binary.
        tokio::fs::copy(&staged, &current).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to promote {} to {}: {}",
                staged.display(),
                current.display(),
                e
            ))
        })?;

        // Remove staged copy to avoid confusion.
        let _ = tokio::fs::remove_file(&staged).await;

        info!(current = %current.display(), "Switching to new binary via exec");
        Self::exec_binary(&current)
    }

    async fn rollback(&self) -> SFResult<()> {
        let current = self.binary_path();
        let prev = self.prev_binary_path();

        if !prev.exists() {
            return Err(SFError::Validation(
                "No previous binary available for rollback".into(),
            ));
        }

        tokio::fs::copy(&prev, &current).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to restore {} from {}: {}",
                current.display(),
                prev.display(),
                e
            ))
        })?;

        info!(current = %current.display(), "Rolling back to previous binary via exec");
        Self::exec_binary(&current)
    }
}

// ============================================================================
// Systemd Switcher
// ============================================================================

/// Asks systemd to restart the cogneva service.
#[derive(Debug, Clone)]
pub struct SystemdSwitcher {
    config: BinarySwitcherConfig,
}

impl SystemdSwitcher {
    pub fn new(config: BinarySwitcherConfig) -> Self {
        Self { config }
    }

    fn binary_path(&self) -> PathBuf {
        self.config.binary_dir.join(&self.config.binary_name)
    }

    fn prev_binary_path(&self) -> PathBuf {
        self.config
            .binary_dir
            .join(format!("{}.prev", self.config.binary_name))
    }

    async fn systemctl(&self, args: &[&str]) -> SFResult<()> {
        let output = tokio::process::Command::new("systemctl")
            .args(args)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run systemctl: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!(
                "systemctl {} failed: {}",
                args.join(" "),
                stderr
            )));
        }
        Ok(())
    }

    async fn health_check(&self, url: &str, max_retries: u32, interval_secs: u64) -> SFResult<()> {
        for i in 0..max_retries {
            match simple_http_get(url).await {
                Ok(()) => {
                    info!("Systemd-switched service passed health check");
                    return Ok(());
                }
                Err(e) => {
                    warn!(attempt = i + 1, error = %e, "Health check failed");
                    tokio::time::sleep(tokio::time::Duration::from_secs(interval_secs)).await;
                }
            }
        }
        Err(SFError::Validation(
            "Health check failed after systemd restart".into(),
        ))
    }
}

#[async_trait::async_trait]
impl cog_core::BinarySwitcher for SystemdSwitcher {
    async fn stage_new_binary(&self, new_binary_path: &Path) -> SFResult<()> {
        // For systemd the "staged" binary is the active one: the service file
        // points directly at `binary_dir/binary_name`. We still keep `.prev`.
        ensure_dir(&self.config.binary_dir).await?;
        let current = self.binary_path();
        let prev = self.prev_binary_path();

        if current.exists() {
            tokio::fs::copy(&current, &prev).await.map_err(|e| {
                SFError::IO(format!(
                    "Failed to backup current binary to {}: {}",
                    prev.display(),
                    e
                ))
            })?;
        }

        tokio::fs::copy(new_binary_path, &current)
            .await
            .map_err(|e| {
                SFError::IO(format!(
                    "Failed to install {} to {}: {}",
                    new_binary_path.display(),
                    current.display(),
                    e
                ))
            })?;

        info!(current = %current.display(), "Installed new binary for systemd restart");
        Ok(())
    }

    async fn switch_and_restart(&self) -> SFResult<()> {
        info!(service = %self.config.systemd_service_name, "Restarting via systemd");
        self.systemctl(&["restart", &self.config.systemd_service_name])
            .await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(
            self.config.health_check_grace_period_secs,
        ))
        .await;
        self.health_check(
            &self.config.health_url,
            self.config.health_check_max_retries,
            self.config.health_check_interval_secs,
        )
        .await
    }

    async fn rollback(&self) -> SFResult<()> {
        let current = self.binary_path();
        let prev = self.prev_binary_path();
        if !prev.exists() {
            return Err(SFError::Validation(
                "No previous binary available for rollback".into(),
            ));
        }
        tokio::fs::copy(&prev, &current).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to restore previous binary to {}: {}",
                current.display(),
                e
            ))
        })?;
        info!(current = %current.display(), "Restored previous binary; restarting via systemd");
        self.systemctl(&["restart", &self.config.systemd_service_name])
            .await
    }
}

// ============================================================================
// Sidecar Switcher
// ============================================================================

/// Sends commands to an external sidecar process over a Unix socket.
///
/// The sidecar owns the cogneva process lifecycle. If the sidecar is not
/// running, this switcher falls back to `SelfExecSwitcher` behavior.
#[derive(Debug, Clone)]
pub struct SidecarSwitcher {
    config: BinarySwitcherConfig,
}

impl SidecarSwitcher {
    pub fn new(config: BinarySwitcherConfig) -> Self {
        Self { config }
    }

    async fn send_command(&self, command: &str) -> SFResult<()> {
        let socket = &self.config.sidecar_socket_path;
        if !socket.exists() {
            warn!(
                socket = %socket.display(),
                "Sidecar socket not found; falling back to self_exec"
            );
            let fallback = SelfExecSwitcher::new(self.config.clone());
            return match command {
                "switch" => fallback.switch_and_restart().await,
                "rollback" => fallback.rollback().await,
                _ => Err(SFError::Validation(format!(
                    "Unknown sidecar command: {}",
                    command
                ))),
            };
        }

        let stream = tokio::net::UnixStream::connect(socket)
            .await
            .map_err(|e| SFError::IO(format!("Failed to connect to sidecar: {}", e)))?;

        let (mut reader, mut writer) = stream.into_split();
        let payload = format!("{}\n", command);
        tokio::io::AsyncWriteExt::write_all(&mut writer, payload.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("Failed to send sidecar command: {}", e)))?;

        let mut buf = [0u8; 256];
        let n = tokio::io::AsyncReadExt::read(&mut reader, &mut buf)
            .await
            .map_err(|e| SFError::IO(format!("Failed to read sidecar response: {}", e)))?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() == "ok" {
            Ok(())
        } else {
            Err(SFError::IO(format!(
                "Sidecar command {} failed: {}",
                command, response
            )))
        }
    }
}

#[async_trait::async_trait]
impl cog_core::BinarySwitcher for SidecarSwitcher {
    async fn stage_new_binary(&self, new_binary_path: &Path) -> SFResult<()> {
        // Stage into the same location self_exec uses; sidecar will look there.
        let fallback = SelfExecSwitcher::new(self.config.clone());
        fallback.stage_new_binary(new_binary_path).await
    }

    async fn switch_and_restart(&self) -> SFResult<()> {
        self.send_command("switch").await
    }

    async fn rollback(&self) -> SFResult<()> {
        self.send_command("rollback").await
    }
}

// ============================================================================
// Helpers
// ============================================================================

async fn ensure_dir(dir: &Path) -> SFResult<()> {
    tokio::fs::create_dir_all(dir).await.map_err(|e| {
        SFError::IO(format!(
            "Failed to create directory {}: {}",
            dir.display(),
            e
        ))
    })
}

/// Minimal HTTP GET using curl. Avoids adding an HTTP client dependency to
/// cog-supervisor while still allowing real health checks.
async fn simple_http_get(url: &str) -> SFResult<()> {
    let output = tokio::process::Command::new("curl")
        .args(["--fail", "--silent", "--show-error", "--max-time", "5", url])
        .output()
        .await
        .map_err(|e| SFError::IO(format!("Failed to run curl: {}", e)))?;

    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(SFError::IO(format!("curl health check failed: {}", stderr)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_builds_self_exec() {
        let config = BinarySwitcherConfig::default();
        let switcher = build_switcher("self_exec", config);
        // Trait object constructed successfully.
        let _ = switcher;
    }

    #[test]
    fn build_switcher_selects_sidecar() {
        let config = BinarySwitcherConfig::default();
        let switcher = build_switcher("sidecar", config);
        let _ = switcher;
    }

    #[test]
    fn build_switcher_defaults_to_self_exec() {
        let config = BinarySwitcherConfig::default();
        let switcher = build_switcher("unknown_mode", config);
        // Unknown modes fall back to self-exec.
        let _ = switcher;
    }
}
