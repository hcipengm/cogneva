#![allow(dead_code)]
use std::path::PathBuf;

/// Cross-platform directory layout for Cogneva.
/// | Platform | data_dir        | config_dir               | log_dir               | pid_dir               |
/// |----------|-----------------|--------------------------|-----------------------|-----------------------|
/// | Linux    | /var/lib/cogneva-data| /etc/cogneva          | /var/log/cogneva   | /run/cogneva       |
/// | macOS    | ~/Library/Application Support/sf-data | ~/Library/Preferences/cogneva | ~/Library/Logs/cogneva | ~/Library/Run/cogneva |
/// | Windows  | %PROGRAMDATA%\sf-data | %PROGRAMDATA%\cogneva | %PROGRAMDATA%\cogneva\logs | %PROGRAMDATA%\cogneva |
pub struct PlatformPaths;

impl PlatformPaths {
    /// Base directory for persistent data (vectors, memory, wiki, raw logs).
    pub fn data_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from("/var/lib/cogneva-data")
        }
        #[cfg(target_os = "macos")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from("~/Library/Application Support"))
                .join("sf-data")
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join("sf-data")
        }
    }

    /// Configuration directory.
    pub fn config_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from("/etc/cogneva")
        }
        #[cfg(target_os = "macos")]
        {
            dirs::config_dir()
                .unwrap_or_else(|| PathBuf::from("~/Library/Preferences"))
                .join("cogneva")
        }
        #[cfg(target_os = "windows")]
        {
            dirs::config_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join("cogneva")
        }
    }

    /// Application binary / installation directory.
    pub fn app_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from("/opt/cogneva")
        }
        #[cfg(target_os = "macos")]
        {
            PathBuf::from("/Applications/cogneva")
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join("cogneva")
        }
    }

    /// Log directory.
    pub fn log_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from("/var/log/cogneva")
        }
        #[cfg(target_os = "macos")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from("~/Library/Logs"))
                .join("cogneva")
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join("cogneva")
                .join("logs")
        }
    }

    /// PID file directory.
    pub fn pid_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from("/run/cogneva")
        }
        #[cfg(target_os = "macos")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from("~/Library/Application Support"))
                .join("cogneva")
                .join("run")
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join("cogneva")
                .join("run")
        }
    }

    /// Temporary directory for tier-migration scratch files.
    pub fn tmp_dir() -> PathBuf {
        std::env::temp_dir().join("cogneva")
    }

    /// Default PID file path.
    pub fn pid_file() -> PathBuf {
        Self::pid_dir().join("cogneva.pid")
    }

    /// Default log file path.
    pub fn log_file() -> PathBuf {
        Self::log_dir().join("cogneva.log")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_paths_are_not_empty() {
        assert!(!PlatformPaths::data_dir().as_os_str().is_empty());
        assert!(!PlatformPaths::config_dir().as_os_str().is_empty());
        assert!(!PlatformPaths::app_dir().as_os_str().is_empty());
        assert!(!PlatformPaths::log_dir().as_os_str().is_empty());
        assert!(!PlatformPaths::pid_dir().as_os_str().is_empty());
    }

    #[test]
    fn test_pid_file_contains_pid_filename() {
        let pid = PlatformPaths::pid_file();
        assert!(pid.to_string_lossy().contains("cogneva.pid"));
    }
}
