#![allow(dead_code)]
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use cog_core::{SFError, SFResult};

/// Manages a PID file for daemon processes.
/// On creation, writes the current process PID to the file.
/// On drop (unless explicitly released), removes the file.
#[derive(Debug)]
pub struct PidFile {
    path: PathBuf,
    released: bool,
}

impl PidFile {
    /// Create a new PID file at `path` containing the current process ID.
    /// If the file already exists, its contents are overwritten.
    pub fn new(path: impl AsRef<Path>) -> SFResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| SFError::Agent(format!("failed to create pidfile dir: {}", e)))?;
        }
        let mut file = fs::File::create(path)
            .map_err(|e| SFError::Agent(format!("failed to create pidfile: {}", e)))?;
        writeln!(file, "{}", std::process::id())
            .map_err(|e| SFError::Agent(format!("failed to write pidfile: {}", e)))?;
        Ok(Self {
            path: path.to_path_buf(),
            released: false,
        })
    }

    /// Read the PID from an existing pidfile.
    pub fn read_pid(path: impl AsRef<Path>) -> SFResult<u32> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| SFError::Agent(format!("failed to read pidfile: {}", e)))?;
        content
            .trim()
            .parse()
            .map_err(|e| SFError::Agent(format!("invalid pidfile content: {}", e)))
    }

    /// Check whether a process with the PID stored in `path` is still running.
    #[cfg(unix)]
    pub fn is_running(path: impl AsRef<Path>) -> bool {
        match Self::read_pid(path) {
            Ok(pid) => std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Check whether a process with the PID stored in `path` is still running.
    #[cfg(windows)]
    pub fn is_running(path: impl AsRef<Path>) -> bool {
        match Self::read_pid(path) {
            Ok(pid) => std::process::Command::new("tasklist")
                .args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).contains(&pid.to_string()))
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Return the path of the pidfile.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Explicitly remove the pidfile and mark this guard as released.
    pub fn release(mut self) -> SFResult<()> {
        self.remove_file()?;
        self.released = true;
        Ok(())
    }

    fn remove_file(&self) -> SFResult<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)
                .map_err(|e| SFError::Agent(format!("failed to remove pidfile: {}", e)))?;
        }
        Ok(())
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.remove_file();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pidfile_write_and_read() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_path_buf();
        // Remove the temp file so PidFile can create it
        drop(tmpfile);

        let pidfile = PidFile::new(&path).unwrap();
        let pid = PidFile::read_pid(&path).unwrap();
        assert_eq!(pid, std::process::id());

        pidfile.release().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_pidfile_auto_remove_on_drop() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_path_buf();
        drop(tmpfile);

        {
            let _pidfile = PidFile::new(&path).unwrap();
            assert!(path.exists());
        }

        assert!(!path.exists());
    }

    #[test]
    fn test_pidfile_release_prevents_auto_remove() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_path_buf();
        drop(tmpfile);

        let pidfile = PidFile::new(&path).unwrap();
        pidfile.release().unwrap();
        assert!(!path.exists());

        // After release, dropping should not panic or error
        // (path already removed, released flag set)
    }

    #[test]
    fn test_pidfile_is_running_current_process() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_path_buf();
        drop(tmpfile);

        let _pidfile = PidFile::new(&path).unwrap();
        assert!(PidFile::is_running(&path));
    }

    #[test]
    fn test_pidfile_is_running_nonexistent() {
        assert!(!PidFile::is_running("/nonexistent/pidfile.pid"));
    }
}
