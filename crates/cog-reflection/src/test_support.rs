//! Helpers shared by this crate's tests.

use std::path::{Path, PathBuf};

/// Write `script` as an executable at `dir/name` and return its path.
///
/// The bytes go to a staging file that is never executed, and the destination
/// is then produced by `cp` — a child process, not a descriptor of this one.
///
/// Exec'ing a file fails with `ETXTBSY` while *any* process still holds it open
/// for writing, and a descriptor names an inode rather than a path, so renaming
/// a staged file into place does not remove the hazard: a sibling test that
/// forks while the staging write is still open hands its child a duplicate of
/// that descriptor, and once the staging file has been renamed the duplicate
/// refers to exactly the file about to be exec'd. It lives until that child
/// execs — the descriptor is close-on-exec, but only from that moment on — and
/// that window has been observed losing the race.
///
/// Handing the copy to a child process means the destination inode was never
/// written by a process that can fork: `cp` has no children, and by the time it
/// exits no descriptor to the destination exists anywhere, so a concurrently
/// forking sibling has nothing to inherit.
pub fn write_executable(dir: &Path, name: &str, script: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    let staged = dir.join(format!(".{name}.staged"));
    // A duplicate of *this* descriptor escaping into a forked child is
    // harmless: the staging path is never exec'd.
    std::fs::write(&staged, script).unwrap();
    // Unlink rather than let `cp` truncate: truncating a file something is
    // currently executing fails with ETXTBSY inside `cp` itself.
    let _ = std::fs::remove_file(&path);
    let status = std::process::Command::new("cp")
        .arg(&staged)
        .arg(&path)
        .status()
        .expect("cp must be runnable to install a test executable");
    assert!(status.success(), "cp {staged:?} -> {path:?}: {status}");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::remove_file(&staged).unwrap();
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

    /// The property the staging dance exists for: the path that will be exec'd
    /// must not be an inode this process wrote. A descriptor on the inode that
    /// the path names is what a concurrently forking sibling hands to its
    /// child, and any such child keeps the exec failing until it execs itself.
    ///
    /// The staging inode is pinned up front so the two designs differ
    /// observably: writing the destination through a descriptor of ours (and
    /// renaming it into place) reuses that inode, copying it from a child
    /// process does not.
    #[test]
    fn the_path_that_gets_exec_d_is_not_an_inode_this_process_wrote() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let staged = dir.path().join(".fake-kubectl.staged");
        std::fs::write(&staged, "junk").unwrap();
        let written = std::fs::metadata(&staged).unwrap().ino();

        let path = write_executable(dir.path(), "fake-kubectl", "#!/bin/sh\nexit 0\n");

        assert_ne!(
            std::fs::metadata(&path).unwrap().ino(),
            written,
            "the executable must not be the inode this process held open for writing"
        );
    }

    /// The kernel behaviour the two tests above are about, reproduced without
    /// relying on a sibling test racing us: a descriptor held open in a
    /// separate process is enough to fail the exec, and closing it is enough to
    /// let the exec through again.
    #[test]
    fn an_exec_fails_while_another_process_holds_the_file_open_for_writing() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held-open");
        let ready = dir.path().join("ready");
        let release = dir.path().join("release");
        std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();

        // A shell, not a bare `sleep`: a compound command cannot be optimized
        // into an exec, so this process is guaranteed to stay alive holding the
        // append descriptor until the release file appears. The loop is capped
        // in the script itself so a failing assertion above cannot leave an
        // orphan polling a file that will never be written.
        let holder = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "exec 3>>'{path}'; : > '{ready}'; n=0; \
                 while [ ! -e '{release}' ] && [ $n -lt 600 ]; do sleep 0.05; n=$((n+1)); done",
                path = path.display(),
                ready = ready.display(),
                release = release.display(),
            ))
            .spawn()
            .unwrap();

        assert!(
            wait_until(|| ready.exists()),
            "the holder never opened the file for writing"
        );
        let err = std::process::Command::new(&path)
            .status()
            .expect_err("exec must fail while another process holds the file open for writing");
        assert_eq!(err.raw_os_error(), Some(26), "expected ETXTBSY: {err}");

        std::fs::write(&release, "").unwrap();
        let mut holder = holder;
        holder.wait().unwrap();
        assert!(
            wait_until(|| std::process::Command::new(&path)
                .status()
                .is_ok_and(|s| s.success())),
            "the exec must succeed again once the writer is gone"
        );
    }

    /// Bounded poll, so a holder that never opens or never exits fails the test
    /// instead of hanging it.
    fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if cond() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        cond()
    }
}
