#![allow(dead_code)]
use std::path::{Path, PathBuf};

/// 应用自己那一级目录的名字。macOS / Windows 的配置、日志、pid、临时目录都挂在
/// 它下面：一个词，四处各写一遍时，改其中一处只会挪走一半的东西。
const APP_DIR_NAME: &str = "cogneva";

/// macOS / Windows 的数据根默认名（Linux 的数据根由 `cog_core::paths` 声明）。
const DATA_DIR_NAME: &str = "sf-data";

/// macOS / Windows 的 pid 目录在应用目录下的那一级。
const RUN_SUBDIR: &str = "run";

/// pid 文件名。首选目录与临时回落目录落的是同一个名字。
const PID_FILE_NAME: &str = "cogneva.pid";

/// 日志文件名。
const LOG_FILE_NAME: &str = "cogneva.log";

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
            std::path::PathBuf::from(cog_core::paths::data_dir())
        }
        #[cfg(target_os = "macos")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from("~/Library/Application Support"))
                .join(DATA_DIR_NAME)
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join(DATA_DIR_NAME)
        }
    }

    /// Configuration directory.
    pub fn config_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            std::path::PathBuf::from(cog_core::paths::CONFIG_DIR)
        }
        #[cfg(target_os = "macos")]
        {
            dirs::config_dir()
                .unwrap_or_else(|| PathBuf::from("~/Library/Preferences"))
                .join(APP_DIR_NAME)
        }
        #[cfg(target_os = "windows")]
        {
            dirs::config_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join(APP_DIR_NAME)
        }
    }

    /// Application binary / installation directory.
    pub fn app_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            PathBuf::from(cog_core::paths::APP_DIR)
        }
        #[cfg(target_os = "macos")]
        {
            PathBuf::from("/Applications/cogneva")
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join(APP_DIR_NAME)
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
                .join(APP_DIR_NAME)
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join(APP_DIR_NAME)
                .join("logs")
        }
    }

    /// PID file directory.
    pub fn pid_dir() -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            std::path::PathBuf::from(cog_core::paths::RUN_DIR)
        }
        #[cfg(target_os = "macos")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from("~/Library/Application Support"))
                .join(APP_DIR_NAME)
                .join(RUN_SUBDIR)
        }
        #[cfg(target_os = "windows")]
        {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join(APP_DIR_NAME)
                .join(RUN_SUBDIR)
        }
    }

    /// Temporary directory for tier-migration scratch files.
    pub fn tmp_dir() -> PathBuf {
        std::env::temp_dir().join(APP_DIR_NAME)
    }

    /// Default PID file path.
    pub fn pid_file() -> PathBuf {
        Self::pid_file_in(&Self::pid_dir())
    }

    /// The pid file's path under a given directory.
    ///
    /// The preferred directory and the temporary fallback differ, the file name
    /// does not: written twice, a rename would leave half the processes writing
    /// a pid file nobody looks for.
    pub fn pid_file_in(dir: &Path) -> PathBuf {
        dir.join(PID_FILE_NAME)
    }

    /// Default log file path.
    pub fn log_file() -> PathBuf {
        Self::log_dir().join(LOG_FILE_NAME)
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
