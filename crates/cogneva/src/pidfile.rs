use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use cog_core::{SFError, SFResult};

/// Owns the process's PID file.
///
/// The file is the handle an operator uses to reach this process from outside
/// it — the documented graceful reload sends SIGHUP to the PID in the file — so
/// the only thing this type has to get right is that the file exists at a path
/// that can be named, and that it goes away when the process does.
///
/// On drop the file is removed.
#[derive(Debug)]
pub struct PidFile {
    path: PathBuf,
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
        })
    }

    /// Return the path of the pidfile.
    pub fn path(&self) -> &Path {
        &self.path
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
        let _ = self.remove_file();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pidfile_auto_remove_on_drop() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_path_buf();
        drop(tmpfile);

        {
            let _pidfile = PidFile::new(&path).unwrap();
            assert!(path.exists());
            assert_eq!(
                fs::read_to_string(&path).unwrap().trim(),
                std::process::id().to_string()
            );
        }

        assert!(!path.exists());
    }

    #[test]
    fn test_pidfile_overwrites_an_existing_file() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_path_buf();
        fs::write(&path, "stale\n").unwrap();

        let pidfile = PidFile::new(&path).unwrap();
        assert_eq!(pidfile.path(), path.as_path());
        assert_eq!(
            fs::read_to_string(&path).unwrap().trim(),
            std::process::id().to_string()
        );
    }
}
