//! The act of dropping what [`crate::contract::build_cache_reclaim`] plans.
//!
//! The rules that decide *what* to drop stay with the types they are about, in
//! the contract half, because both processes that own a cargo cache read them
//! and neither can depend on the other. What is here is the deletion and the one
//! rule that binds it:
//!
//! **Nothing is deleted while a build can be running.** Not because of a
//! clock -- a file's age says nothing about whether a build is reading it --
//! but because the build gate is the fact that says whether one is. The
//! caller holds a slot for the duration of the pass; a file deleted under a
//! running build would fail that build for a reason that has nothing to do
//! with it, which is how a host problem gets recorded as a change's fault.

use std::path::{Path, PathBuf};

pub use crate::contract::build_cache_reclaim::{
    plan_reclaim, PlannedGroup, ReclaimPlan, DISCARDABLE_LEAVES, FLOOR_FILES_PER_LAYER,
    RESULT_LEAVES,
};

/// What a pass removed, and what it could not.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReclaimOutcome {
    /// Names unlinked, which is more than the number of files when a file had
    /// more than one.
    pub deleted_names: usize,
    /// Files whose every name is gone, so their bytes are back.
    pub freed_files: usize,
    /// Bytes those files held. Counted here rather than subtracted from a total
    /// seen earlier, so a pass cannot report freeing what it did not remove.
    pub freed_bytes: u64,
    /// Names the plan named that are still there, with why. A failed delete is
    /// not an error to retry silently: it is the difference between a cap that
    /// holds and one that does not, so it is reported on its own.
    pub failures: Vec<(PathBuf, String)>,
}

/// Delete the files a plan names, under `root`.
///
/// Refuses any path outside `root`. The plan comes from a walk of `root`, so a
/// path outside it is either a bug or a path that changed underneath the pass,
/// and deleting outside the cache is the one mistake here that cannot be walked
/// back. A directory named by a plan is a failure rather than a removal: a plan
/// holds files.
pub fn apply_reclaim(root: &Path, plan: &ReclaimPlan) -> ReclaimOutcome {
    let mut outcome = ReclaimOutcome::default();
    for group in &plan.delete {
        let mut every_name_gone = true;
        for path in &group.paths {
            if !path.starts_with(root) {
                outcome.failures.push((
                    path.clone(),
                    format!("outside the cache root {}", root.display()),
                ));
                every_name_gone = false;
                continue;
            }
            match std::fs::remove_file(path) {
                Ok(()) => outcome.deleted_names += 1,
                Err(e) => {
                    outcome.failures.push((path.clone(), e.to_string()));
                    every_name_gone = false;
                }
            }
        }
        // Charged what the plan said the file held, not what it holds now: a
        // file that grew between the walk and the delete must not make the pass
        // look like it freed more than the plan asked for. And charged only for
        // a file whose every name is gone, since a name removed while another
        // still stands for the same bytes frees nothing.
        if every_name_gone {
            outcome.freed_files += 1;
            outcome.freed_bytes = outcome.freed_bytes.saturating_add(group.len);
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A path outside the cache root is refused rather than deleted, and counted
    /// as a failure rather than as work done.
    #[test]
    fn a_path_outside_the_cache_root_is_never_deleted() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let victim = outside.path().join("important");
        std::fs::write(&victim, b"keep me").unwrap();

        let plan = ReclaimPlan {
            delete: vec![PlannedGroup {
                paths: vec![victim.clone()],
                len: 7,
            }],
            delete_bytes: 7,
            unreachable_bytes: 0,
        };
        let outcome = apply_reclaim(root.path(), &plan);

        assert_eq!(outcome.deleted_names, 0);
        assert_eq!(outcome.freed_bytes, 0);
        assert_eq!(outcome.failures.len(), 1);
        assert!(victim.exists(), "a path outside the cache must survive");
    }

    /// Removing every name of a file frees its bytes; removing some of them
    /// frees nothing and is reported as a failure, since a cap that silently
    /// fails to come down is the state this whole module exists to avoid.
    #[test]
    fn a_file_frees_its_bytes_only_once_every_name_is_gone() {
        let root = tempfile::tempdir().unwrap();
        let counted = root.path().join("release/deps/lib.rlib");
        let uplifted = root.path().join("release/lib.rlib");
        std::fs::create_dir_all(counted.parent().unwrap()).unwrap();
        std::fs::create_dir_all(uplifted.parent().unwrap()).unwrap();
        std::fs::write(&counted, vec![b'x'; 64]).unwrap();
        std::fs::hard_link(&counted, &uplifted).unwrap();
        let missing = root.path().join("release/deps/gone-later");

        let plan = ReclaimPlan {
            delete: vec![
                PlannedGroup {
                    paths: vec![counted.clone(), uplifted.clone()],
                    len: 64,
                },
                PlannedGroup {
                    paths: vec![missing.clone()],
                    len: 7,
                },
            ],
            delete_bytes: 71,
            unreachable_bytes: 0,
        };
        let outcome = apply_reclaim(root.path(), &plan);

        assert_eq!(outcome.deleted_names, 2);
        assert_eq!(outcome.freed_files, 1);
        assert_eq!(outcome.freed_bytes, 64, "both names, one file's bytes");
        assert_eq!(outcome.failures.len(), 1, "{outcome:?}");
        assert_eq!(outcome.failures[0].0, missing);
        assert!(!counted.exists() && !uplifted.exists());
    }

    /// Half a link removed is not half a file freed: the bytes are still there
    /// under the name that is left, so nothing is charged for it.
    #[test]
    fn a_partly_removed_link_frees_nothing() {
        let root = tempfile::tempdir().unwrap();
        let counted = root.path().join("release/deps/lib.rlib");
        let uplifted = root.path().join("release/lib.rlib");
        std::fs::create_dir_all(counted.parent().unwrap()).unwrap();
        std::fs::create_dir_all(uplifted.parent().unwrap()).unwrap();
        std::fs::write(&counted, vec![b'x'; 64]).unwrap();
        std::fs::hard_link(&counted, &uplifted).unwrap();

        // The second name is gone by the time the pass runs.
        std::fs::remove_file(&uplifted).unwrap();
        let plan = ReclaimPlan {
            delete: vec![PlannedGroup {
                paths: vec![counted.clone(), uplifted.clone()],
                len: 64,
            }],
            delete_bytes: 64,
            unreachable_bytes: 0,
        };
        let outcome = apply_reclaim(root.path(), &plan);

        assert_eq!(outcome.deleted_names, 1);
        assert_eq!(outcome.freed_files, 0);
        assert_eq!(outcome.freed_bytes, 0);
        assert_eq!(outcome.failures.len(), 1, "{outcome:?}");
    }
}
