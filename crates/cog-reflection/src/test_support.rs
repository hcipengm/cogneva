//! Helpers shared by this crate's tests.

use std::path::{Path, PathBuf};

/// Write `script` as an executable at `dir/name` and return its path.
///
/// The script is staged in a temporary name and renamed into place. Writing the
/// final path directly leaves a window where it is open for writing, and a
/// sibling test that forks during that window hands the descriptor to its
/// child; exec'ing a path that anyone still holds open for writing fails with
/// `ETXTBSY`. After a rename the visible path is only ever readable, so the
/// race disappears instead of being narrowed.
pub fn write_executable(dir: &Path, name: &str, script: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    let staged = dir.join(format!(".{name}.staged"));
    std::fs::write(&staged, script).unwrap();
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&staged, &path).unwrap();
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_script_lands_executable_with_no_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_executable(dir.path(), "fake-kubectl", "#!/bin/sh\nexit 0\n");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "#!/bin/sh\nexit 0\n"
        );
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o111, 0o111, "must be executable, mode {mode:o}");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n.to_string_lossy() != "fake-kubectl")
            .collect();
        assert!(
            leftovers.is_empty(),
            "staging file left behind: {leftovers:?}"
        );
    }
}
