//! How much of a generated unified diff actually matches the tree it targets.
//!
//! A generated change is accepted or rejected as a whole, so the rejection
//! reason names one hunk and says nothing about the rest. That makes
//! "generation quality" unreadable: a diff with one bad hunk out of three and a
//! diff with three bad hunks out of three both surface as a single rejected
//! artifact, and neither is aggregatable across rounds.
//!
//! This module answers the finer question: of the hunks this artifact claims to
//! carry, how many find their context in the target tree. It exists so that
//! number can be reported, trended, and alerted on instead of only inferred
//! from the fact that some step downstream refused the patch.
//!
//! The oracle is `git apply --check`, deliberately: whatever decides whether a
//! change lands has to be the same thing that decides what this reading means,
//! or the reading can be green while the gate is red.

use std::path::Path;

/// Per-artifact fidelity against the tree the artifact was generated for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiffFidelity {
    pub files_total: u64,
    pub files_faithful: u64,
    pub hunks_total: u64,
    pub hunks_faithful: u64,
}

impl DiffFidelity {
    /// The diff decomposed into no file section, so nothing was measured. An
    /// artifact that never reached the gate is missing from the reading, not
    /// present in it as a zero.
    pub fn is_empty(&self) -> bool {
        self.files_total == 0
    }
}

/// Measure how much of `diff` matches the tree at `workdir`.
///
/// `whole_patch_ok` is the caller's own `git apply --check` verdict on the full
/// diff. When it passed, every hunk found its context and no further invocation
/// is needed; when it failed, each file and each hunk is checked on its own so
/// the failure can be attributed instead of merely observed.
pub async fn measure(workdir: &Path, diff: &str, whole_patch_ok: bool) -> DiffFidelity {
    let entries = cog_core::diff_file_entries(diff);

    let total = DiffFidelity {
        files_total: entries.len() as u64,
        files_faithful: 0,
        hunks_total: entries.iter().map(|f| f.hunks.len() as u64).sum(),
        hunks_faithful: 0,
    };

    if total.is_empty() {
        return total;
    }

    if whole_patch_ok {
        return DiffFidelity {
            files_faithful: total.files_total,
            hunks_faithful: total.hunks_total,
            ..total
        };
    }

    let mut files_faithful = 0;
    let mut hunks_faithful = 0;
    for entry in &entries {
        // A section that only renames or changes mode carries no hunk to check
        // one at a time, so its whole body of work is its header.
        if entry.hunks.is_empty() {
            if apply_check(workdir, &entry.narrow(&[])).await {
                files_faithful += 1;
            }
            continue;
        }

        let all: Vec<usize> = (0..entry.hunks.len()).collect();
        if apply_check(workdir, &entry.narrow(&all)).await {
            files_faithful += 1;
        }
        for i in 0..entry.hunks.len() {
            if apply_check(workdir, &entry.narrow(&[i])).await {
                hunks_faithful += 1;
            }
        }
    }

    DiffFidelity {
        files_faithful,
        hunks_faithful,
        ..total
    }
}

async fn apply_check(workdir: &Path, patch: &str) -> bool {
    use tokio::io::AsyncWriteExt;

    let mut child = match tokio::process::Command::new("git")
        .arg("apply")
        .arg("--check")
        .current_dir(workdir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            tracing::warn!(error = %e, "git apply unavailable; treating hunk as unfaithful");
            return false;
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        if stdin.write_all(patch.as_bytes()).await.is_err() {
            return false;
        }
    }

    matches!(child.wait().await, Ok(status) if status.success())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_diff_measures_nothing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let measured = rt.block_on(measure(Path::new("/nonexistent"), "", false));
        assert!(measured.is_empty());
        assert_eq!(measured.hunks_total, 0);
    }

    /// The shape this reading exists for: an artifact carrying three hunks of
    /// which only one finds its context. The whole-patch check fails, and the
    /// per-hunk checks have to say *which* one is good — a reading that only
    /// reproduced "the patch failed" would not be worth recording.
    #[test]
    fn attributes_a_partially_faithful_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        run_git(root, &["init", "-q"]);
        std::fs::write(root.join("one.txt"), "alpha\nbeta\ngamma\n").unwrap();
        run_git(root, &["add", "-A"]);
        run_git(
            root,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                "base",
            ],
        );

        // Hunk 1 rewrites a line that exists (faithful); hunks 2 and 3 claim
        // context the file does not have (unfaithful).
        let diff = "\
--- a/one.txt
+++ b/one.txt
@@ -1,3 +1,3 @@
 alpha
-beta
+BETA
 gamma
@@ -20,3 +20,3 @@
 absent-a
-absent-b
+ABSENT-B
 absent-c
@@ -40,2 +40,2 @@
 more-absent
-more
+MORE
";

        let rt = tokio::runtime::Runtime::new().unwrap();
        let measured = rt.block_on(measure(root, diff, false));

        assert_eq!(measured.files_total, 1);
        assert_eq!(
            measured.files_faithful, 0,
            "the file as a whole does not apply"
        );
        assert_eq!(measured.hunks_total, 3);
        assert_eq!(measured.hunks_faithful, 1, "only the first hunk matches");
    }

    /// Hunks that are individually faithful but mutually exclusive: both rewrite
    /// the same line, so each applies to the untouched tree and the pair does
    /// not. The reading is per-hunk, so it reports two — and the whole-artifact
    /// verdict this sits beside is what says the pair cannot land together.
    #[test]
    fn counts_hunks_each_against_the_untouched_tree() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        run_git(root, &["init", "-q"]);
        std::fs::write(root.join("one.txt"), "alpha\nbeta\ngamma\n").unwrap();
        run_git(root, &["add", "-A"]);
        run_git(
            root,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                "base",
            ],
        );

        let diff = "\
--- a/one.txt
+++ b/one.txt
@@ -1,3 +1,3 @@
 alpha
-beta
+BETA
 gamma
@@ -1,3 +1,3 @@
 alpha
-beta
+BETAA
 gamma
";

        let rt = tokio::runtime::Runtime::new().unwrap();
        let measured = rt.block_on(measure(root, diff, false));

        assert_eq!(measured.hunks_total, 2);
        assert_eq!(measured.hunks_faithful, 2);
    }

    #[test]
    fn a_whole_artifact_that_applies_reports_every_hunk_faithful() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        run_git(root, &["init", "-q"]);
        std::fs::write(root.join("one.txt"), "alpha\nbeta\ngamma\n").unwrap();
        run_git(root, &["add", "-A"]);
        run_git(
            root,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                "base",
            ],
        );

        let diff = "\
--- a/one.txt
+++ b/one.txt
@@ -1,3 +1,3 @@
 alpha
-beta
+BETA
 gamma
";

        let rt = tokio::runtime::Runtime::new().unwrap();
        let measured = rt.block_on(measure(root, diff, true));

        assert_eq!(measured.files_total, 1);
        assert_eq!(measured.files_faithful, 1);
        assert_eq!(measured.hunks_total, 1);
        assert_eq!(measured.hunks_faithful, 1);
    }

    /// A rename carries no hunk, so nothing about it can be checked a hunk at a
    /// time; it is still counted, because `git apply --check` decides it and an
    /// artifact made only of renames is not an empty artifact.
    #[test]
    fn a_section_without_hunks_is_measured_by_its_header_alone() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        run_git(root, &["init", "-q"]);
        std::fs::write(root.join("old.txt"), "alpha\n").unwrap();
        run_git(root, &["add", "-A"]);
        run_git(
            root,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                "base",
            ],
        );

        let diff = "\
diff --git a/old.txt b/new.txt
similarity index 100%
rename from old.txt
rename to new.txt
";

        let rt = tokio::runtime::Runtime::new().unwrap();
        let measured = rt.block_on(measure(root, diff, false));

        assert_eq!(measured.files_total, 1);
        assert_eq!(measured.files_faithful, 1);
        assert_eq!(measured.hunks_total, 0);
        assert_eq!(measured.hunks_faithful, 0);
    }

    fn run_git(dir: &Path, args: &[&str]) {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
