/// Cross-platform daemon / service manager integration.
/// | Platform | Protocol                          |
/// |----------|-----------------------------------|
/// | Linux    | systemd `sd_notify`               |
/// | Windows  | Windows Service control handler   |
/// | macOS    | launchd (no in-app code required) |
/// Use [`DaemonControl::ready`] after initialization is complete and
/// [`DaemonControl::stopping`] when shutdown begins.
pub struct DaemonControl;

impl DaemonControl {
    pub fn new() -> Self {
        Self
    }

    /// Notify the service manager that the daemon is ready.
    pub fn ready(&self) {
        #[cfg(unix)]
        {
            match sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
                Ok(()) => tracing::debug!("sent systemd ready notification"),
                Err(e) => tracing::debug!("sd_notify ready failed: {}", e),
            }
        }
    }

    /// Notify the service manager that the daemon is stopping.
    pub fn stopping(&self) {
        #[cfg(unix)]
        {
            match sd_notify::notify(true, &[sd_notify::NotifyState::Stopping]) {
                Ok(()) => tracing::debug!("sent systemd stopping notification"),
                Err(e) => tracing::debug!("sd_notify stopping failed: {}", e),
            }
        }
    }

    /// Notify the service manager that the daemon is reloading configuration.
    #[allow(dead_code)]
    pub fn reloading(&self) {
        #[cfg(unix)]
        {
            match sd_notify::notify(true, &[sd_notify::NotifyState::Reloading]) {
                Ok(()) => tracing::debug!("sent systemd reloading notification"),
                Err(e) => tracing::debug!("sd_notify reloading failed: {}", e),
            }
        }
    }

    /// Send a custom status text to the service manager.
    #[allow(dead_code)]
    pub fn status(&self, message: &str) {
        #[cfg(unix)]
        {
            match sd_notify::notify(true, &[sd_notify::NotifyState::Status(message)]) {
                Ok(()) => tracing::debug!("sent systemd status notification"),
                Err(e) => tracing::debug!("sd_notify status failed: {}", e),
            }
        }
    }
}

impl Default for DaemonControl {
    fn default() -> Self {
        Self::new()
    }
}
