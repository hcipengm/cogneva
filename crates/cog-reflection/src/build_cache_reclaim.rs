//! What to drop from a build cache that has outgrown its cap, and the act of
//! dropping it.
//!
//! The cache only ever grows, so a cap needs something that removes bytes. What
//! makes that safe to do at all is that cargo's cache is rebuildable: every file
//! in it is a compiled result or a scratch file whose absence costs the next
//! build time and never correctness. What makes it worth doing carefully is the
//! other half of the same fact: dropping the wrong files costs a cold rebuild of
//! everything, which is the outcome the cache exists to avoid.
//!
//! Four rules, in the order they bind:
//!
//! 1. **Layers that are only speed go first.** `tmp`, `incremental` and `build`
//!    are scratch space, incremental state cargo is designed to discard, and
//!    re-runnable build scripts. `deps` and `.fingerprint` are the compiled
//!    results: losing one of those is a recompile of that unit, and losing all
//!    of them is the cold rebuild. The order within this group is the cost of
//!    losing the layer, which is why it is a fixed list and not the alphabet.
//! 2. **Every layer keeps one file** ([`FLOOR_FILES_PER_LAYER`]), the newest it
//!    has. At this granularity that floor is not a cost floor — one artifact
//!    out of thousands saves nothing worth measuring — it is a visibility floor:
//!    a layer that empties is indistinguishable from a layer the walk could not
//!    see, and the layer series is how a reader knows a layer exists at all.
//!    The newest is the only recency cargo leaves behind: it writes an artifact
//!    when it builds it and does not touch it when it reuses it.
//! 3. **A file goes with all of its names or not at all.** cargo hardlinks what
//!    it lifts out of `deps`, and removing one name of such a file frees nothing
//!    — the bytes stay on the volume under the other name, and the build that
//!    looked at the removed name reads the same either way. So the unit of a
//!    plan is every name of one file, and the bytes it frees are that file's
//!    bytes counted once. A file with a name outside the measured tree is left
//!    alone for the same reason: its bytes cannot be freed from here.
//! 4. **Nothing is deleted while a build can be running.** Not because of a
//!    clock — a file's age says nothing about whether a build is reading it —
//!    but because the build gate is the fact that says whether one is. The
//!    caller holds a slot for the duration of the pass; a file deleted under a
//!    running build would fail that build for a reason that has nothing to do
//!    with it, which is how a host problem gets recorded as a change's fault.
//!
//! What is *not* here is any notion of which workspace a file belongs to. In
//! this deployment every workspace builds into one `CARGO_TARGET_DIR`, and
//! cargo's paths carry no workspace identity, so there is no reading that could
//! tell a resident workspace's artifacts from a retired one's. Retaining by
//! worktree — the shape a cap over a per-worktree cache would take — has nothing
//! to select on here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use cog_core::fs_size::{FileEntry, FileId};

/// Layers whose loss costs only time, in the order they are given up.
///
/// Compared on the last path component, so it applies to every cargo profile:
/// `debug/incremental` and `release/incremental` are the same kind of thing.
/// Anything not listed is treated as a compiled result.
pub const DISCARDABLE_LEAVES: &[&str] = &["tmp", "incremental", "build"];

/// Files kept in every layer, whatever the cap.
///
/// One, and it is the newest file the layer has. See the module docs: this is
/// what keeps a layer from reading as absent, not what keeps a build warm.
pub const FLOOR_FILES_PER_LAYER: usize = 1;

/// One file a plan would delete, by every name it has.
///
/// A group rather than a path: the names share one file, so they go together
/// (rule 3) and `len` — the bytes the volume gets back — is counted once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedGroup {
    /// Every name of the file, in path order.
    pub paths: Vec<PathBuf>,
    /// The bytes the file holds, as measured by the walk the plan came from.
    pub len: u64,
}

/// What a pass would delete, and what it cannot reach.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReclaimPlan {
    /// Files to delete, in the order they were chosen.
    pub delete: Vec<PlannedGroup>,
    /// Bytes those files hold, as measured by the walk the plan came from.
    pub delete_bytes: u64,
    /// Bytes still above the cap once everything eligible is gone. Non-zero
    /// means the cap cannot be reached by deleting what this plan may delete,
    /// which is a state a reader has to be able to tell from a cache that is
    /// merely over its cap and about to be fixed. The floor is the usual reason
    /// and not the only one: bytes held by a file with a name outside the cache
    /// are equally out of reach.
    pub unreachable_bytes: u64,
}

/// Choose what to drop so that `total` falls to `cap`.
///
/// Pure: the same entries, total and cap give the same plan, so what a pass
/// would do can be read without doing it. `total` is passed rather than summed
/// here because the caller measured it in the same walk and it is the number it
/// published; a plan computed against a different sum would be enforcing a
/// figure nobody can see.
pub fn plan_reclaim(files: &[FileEntry], total: u64, cap: u64) -> ReclaimPlan {
    let mut plan = ReclaimPlan::default();
    let excess = total.saturating_sub(cap);
    if excess == 0 {
        return plan;
    }

    // One group per file. Files whose identity the filesystem did not report
    // cannot be grouped, so each is its own: the plan treats them the way it
    // treats a file with one name, which is all that can be known about them.
    let mut grouped: BTreeMap<FileId, Vec<&FileEntry>> = BTreeMap::new();
    let mut unidentified: Vec<Vec<&FileEntry>> = Vec::new();
    for file in files {
        match file.id {
            Some(id) => grouped.entry(id).or_default().push(file),
            None => unidentified.push(vec![file]),
        }
    }
    let mut groups: Vec<Vec<&FileEntry>> = grouped.into_values().chain(unidentified).collect();

    // The floor: in every layer, the newest file stays.
    let mut floors: BTreeSet<&Path> = BTreeSet::new();
    if FLOOR_FILES_PER_LAYER > 0 {
        let mut by_layer: BTreeMap<&str, Vec<&FileEntry>> = BTreeMap::new();
        for file in files {
            by_layer.entry(file.layer.as_str()).or_default().push(file);
        }
        for members in by_layer.values() {
            if let Some(held) = members.iter().max_by(|a, b| {
                a.modified
                    .cmp(&b.modified)
                    .then_with(|| a.path.cmp(&b.path))
            }) {
                floors.insert(held.path.as_path());
            }
        }
    }

    groups.retain(|group| {
        // A name the walk did not see keeps the bytes alive, so removing the
        // names it did see would free nothing.
        let links = group
            .iter()
            .filter_map(|file| file.links)
            .max()
            .unwrap_or(1);
        links <= group.len() as u64
            && group
                .iter()
                .all(|file| !floors.contains(file.path.as_path()))
    });
    // Ordered by where a group's bytes are counted, cheapest layer first, then
    // by layer and path: two passes over one tree choose the same files.
    groups.sort_by(|a, b| group_key(a).cmp(&group_key(b)));

    let mut freed = 0u64;
    let mut eligible_total = 0u64;
    for group in groups {
        let len = group.iter().map(|file| file.len).max().unwrap_or(0);
        eligible_total = eligible_total.saturating_add(len);
        if freed >= excess {
            continue;
        }
        let mut paths: Vec<PathBuf> = group.iter().map(|file| file.path.clone()).collect();
        paths.sort();
        plan.delete.push(PlannedGroup { paths, len });
        freed = freed.saturating_add(len);
    }

    plan.delete_bytes = freed;
    // What the eligible files could not cover. Computed from the groups rather
    // than as `excess - freed` because `freed` stops the moment the cap is
    // reached, and past that point the remainder is not unreachable, it is
    // deliberately left alone.
    plan.unreachable_bytes = excess.saturating_sub(eligible_total);
    plan
}

/// Where a group sits in the order: the layer its counted name is in, then the
/// layer name, then the path.
fn group_key<'a>(group: &[&'a FileEntry]) -> ((usize, usize), &'a str, &'a Path) {
    let counted = counted_name(group);
    (
        layer_rank(&counted.layer),
        counted.layer.as_str(),
        counted.path.as_path(),
    )
}

/// The name of a file its bytes are counted under, which is the one the walk
/// marked as such, or the first name if none did.
fn counted_name<'a>(group: &[&'a FileEntry]) -> &'a FileEntry {
    group
        .iter()
        .find(|file| !file.alias)
        .copied()
        .unwrap_or(group[0])
}

/// Which group a layer belongs to: discardable first, in the declared order.
fn layer_rank(layer: &str) -> (usize, usize) {
    let leaf = layer.rsplit('/').next().unwrap_or(layer);
    match DISCARDABLE_LEAVES.iter().position(|d| *d == leaf) {
        Some(index) => (0, index),
        None => (1, 0),
    }
}

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
    use std::time::{Duration, UNIX_EPOCH};

    fn entry(path: &str, layer: &str, len: u64, age_secs: u64) -> FileEntry {
        FileEntry {
            path: PathBuf::from(path),
            layer: layer.to_string(),
            len,
            modified: Some(UNIX_EPOCH + Duration::from_secs(10_000 - age_secs)),
            id: None,
            links: None,
            alias: false,
        }
    }

    fn total(files: &[FileEntry]) -> u64 {
        files.iter().map(|f| f.len).sum()
    }

    fn planned(plan: &ReclaimPlan) -> Vec<PathBuf> {
        plan.delete
            .iter()
            .flat_map(|group| group.paths.clone())
            .collect()
    }

    /// Nothing is planned under the cap, and nothing is reported as unreachable
    /// either: a cache inside its cap is not a cache that failed to shrink.
    #[test]
    fn a_cache_under_its_cap_plans_nothing() {
        let files = vec![entry("/c/debug/deps/a.rlib", "debug/deps", 100, 0)];
        let plan = plan_reclaim(&files, 100, 100);
        assert_eq!(plan, ReclaimPlan::default());
    }

    /// The discardable layers go before the compiled results, even when the
    /// compiled results are larger: dropping one `deps` file would free the cap
    /// faster, and cost a recompile to do it.
    #[test]
    fn speed_layers_are_given_up_before_compiled_results() {
        // Two files per layer, the newer one named `keep`: a layer's floor is
        // its newest file, so what it can pay is everything besides that.
        let files = vec![
            entry("/c/tmp/keep", "tmp", 100, 0),
            entry("/c/tmp/drop", "tmp", 100, 5),
            entry("/c/release/incremental/keep", "release/incremental", 100, 0),
            entry("/c/release/incremental/drop", "release/incremental", 100, 5),
            entry("/c/release/deps/keep.rlib", "release/deps", 900, 0),
            entry("/c/release/deps/drop.rlib", "release/deps", 900, 5),
        ];
        let held = total(&files);

        // 100 over: scratch space pays, and nothing else is touched.
        let plan = plan_reclaim(&files, held, held - 100);
        assert_eq!(planned(&plan), vec![PathBuf::from("/c/tmp/drop")]);
        assert_eq!(plan.delete_bytes, 100);
        assert_eq!(plan.unreachable_bytes, 0);

        // 200 over: still nothing from the compiled results, although a single
        // `deps` file would cover the whole excess on its own.
        let plan = plan_reclaim(&files, held, held - 200);
        assert_eq!(
            planned(&plan),
            vec![
                PathBuf::from("/c/tmp/drop"),
                PathBuf::from("/c/release/incremental/drop"),
            ]
        );
        assert_eq!(plan.delete_bytes, 200);

        // 1100 over: the discardable layers are exhausted, so compiled results
        // start going: one recompile is cheaper than a build failing on a full
        // disk. Even then the newer of the two is kept.
        let plan = plan_reclaim(&files, held, held - 1100);
        assert_eq!(
            planned(&plan),
            vec![
                PathBuf::from("/c/tmp/drop"),
                PathBuf::from("/c/release/incremental/drop"),
                PathBuf::from("/c/release/deps/drop.rlib"),
            ]
        );
        assert_eq!(plan.delete_bytes, 1100);
        assert!(!planned(&plan).contains(&PathBuf::from("/c/release/deps/keep.rlib")));
    }

    /// A layer is only reached once the one before it has given everything it
    /// may: `tmp` is scratch space, `incremental` is state cargo discards on its
    /// own, `build` re-runs, and only then do compiled results start going.
    #[test]
    fn the_discardable_layers_are_given_up_in_the_order_of_what_they_cost() {
        let files = vec![
            entry("/c/tmp/keep", "tmp", 100, 0),
            entry("/c/tmp/drop", "tmp", 100, 5),
            entry("/c/debug/incremental/keep", "debug/incremental", 100, 0),
            entry("/c/debug/incremental/drop", "debug/incremental", 100, 5),
            entry("/c/release/build/keep", "release/build", 100, 0),
            entry("/c/release/build/drop", "release/build", 100, 5),
        ];
        let held = total(&files);

        let plan = plan_reclaim(&files, held, held - 100);
        assert_eq!(planned(&plan), vec![PathBuf::from("/c/tmp/drop")]);

        let plan = plan_reclaim(&files, held, held - 200);
        assert_eq!(
            planned(&plan),
            vec![
                PathBuf::from("/c/tmp/drop"),
                PathBuf::from("/c/debug/incremental/drop"),
            ]
        );

        let plan = plan_reclaim(&files, held, held - 300);
        assert_eq!(
            planned(&plan),
            vec![
                PathBuf::from("/c/tmp/drop"),
                PathBuf::from("/c/debug/incremental/drop"),
                PathBuf::from("/c/release/build/drop"),
            ]
        );
        assert_eq!(plan.delete_bytes, 300);
        assert_eq!(plan.unreachable_bytes, 0);
    }

    /// Past the discardable layers the plan keeps going into the compiled
    /// results: an enforced cap is worth a recompile, and a build that fails on
    /// a full disk is not.
    #[test]
    fn the_compiled_results_are_given_up_when_they_have_to_be() {
        let files = vec![
            entry("/c/release/deps/a.rlib", "release/deps", 400, 5),
            entry("/c/release/deps/b.rlib", "release/deps", 400, 1),
            entry("/c/release/.fingerprint/f", "release/.fingerprint", 200, 2),
        ];
        // 1000 total, cap 100: 900 has to go. The fingerprint layer holds one
        // file, which is its floor, so nothing there is eligible; `deps` can
        // give up one of its two, and that is all the cache may lose.
        let plan = plan_reclaim(&files, total(&files), 100);
        assert_eq!(
            planned(&plan),
            vec![PathBuf::from("/c/release/deps/a.rlib")]
        );
        assert_eq!(plan.delete_bytes, 400);
        assert_eq!(plan.unreachable_bytes, 500, "{plan:?}");

        // With more to give, both layers contribute, compiled results included.
        let files = vec![
            entry("/c/release/deps/a.rlib", "release/deps", 400, 5),
            entry("/c/release/deps/b.rlib", "release/deps", 400, 1),
            entry("/c/release/.fingerprint/f1", "release/.fingerprint", 100, 2),
            entry("/c/release/.fingerprint/f2", "release/.fingerprint", 100, 3),
        ];
        let plan = plan_reclaim(&files, total(&files), 100);
        assert_eq!(
            planned(&plan),
            vec![
                PathBuf::from("/c/release/.fingerprint/f2"),
                PathBuf::from("/c/release/deps/a.rlib"),
            ]
        );
        assert_eq!(plan.delete_bytes, 500);
    }

    /// Every layer keeps its newest file, and a layer that has only that file is
    /// left alone entirely. What this protects is the reading, not the warmth:
    /// a layer that empties reads as a layer that vanished.
    #[test]
    fn every_layer_keeps_its_newest_file() {
        let files = vec![
            entry("/c/release/deps/old.rlib", "release/deps", 500, 100),
            entry("/c/release/deps/new.rlib", "release/deps", 10, 1),
            entry("/c/tmp/only", "tmp", 500, 50),
        ];
        // Cap far below what is there: the plan takes everything eligible, and
        // what the floor keeps is reported as unreachable rather than deleted.
        let plan = plan_reclaim(&files, total(&files), 1);
        assert_eq!(
            planned(&plan),
            vec![PathBuf::from("/c/release/deps/old.rlib")]
        );
        assert_eq!(plan.delete_bytes, 500);
        // 1010 bytes held, cap 1: only the non-floor file is reachable, so the
        // remaining 509 bytes are the cap being unreachable, not a plan that
        // stopped early.
        assert_eq!(plan.unreachable_bytes, 1010 - 1 - 500);
    }

    /// A file whose age the filesystem does not report is never the floor
    /// holder: the floor is defined as the newest, and an unknown age cannot be
    /// read as the newest of anything. So it is the one that goes.
    #[test]
    fn a_file_with_no_timestamp_cannot_hold_the_floor() {
        let mut unknown = entry("/c/tmp/no-time", "tmp", 10, 0);
        unknown.modified = None;
        let files = vec![unknown, entry("/c/tmp/known", "tmp", 20, 5)];

        let plan = plan_reclaim(&files, total(&files), 1);
        assert_eq!(planned(&plan), vec![PathBuf::from("/c/tmp/no-time")]);
        // 30 held against a cap of 1: 29 over, and only the 10 the plan may
        // delete are reachable.
        assert_eq!(plan.delete_bytes, 10);
        assert_eq!(plan.unreachable_bytes, 19);
    }

    /// The plan is a function of the tree, not of the clock: two passes over the
    /// same entries choose the same files, including when two of them are the
    /// same age and the tie has to be broken by something stable.
    #[test]
    fn the_plan_does_not_depend_on_when_it_is_made() {
        let files = vec![
            entry("/c/release/deps/a.rlib", "release/deps", 100, 0),
            entry("/c/release/deps/b.rlib", "release/deps", 100, 0),
            entry("/c/tmp/c", "tmp", 100, 0),
        ];
        let held = total(&files);
        let first = plan_reclaim(&files, held, held - 150);
        let second = plan_reclaim(&files, held, held - 150);
        assert_eq!(first, second);
        // `tmp` holds a single file, which is its floor, so the only layer that
        // can pay is `deps`: one of the two, whichever the tie-break keeps.
        assert_eq!(
            planned(&first),
            vec![PathBuf::from("/c/release/deps/a.rlib")]
        );
        assert_eq!(first.delete_bytes, 100);
        assert_eq!(first.unreachable_bytes, 50);
    }

    /// Every name of a file goes into one group, and the bytes that come back
    /// are that file's bytes: a name removed on its own frees nothing, so a plan
    /// that named one would report a cap it never reached.
    #[test]
    fn a_files_names_are_deleted_together_and_its_bytes_counted_once() {
        let id = FileId { dev: 1, ino: 7 };
        let mut in_deps = entry("/c/release/deps/lib.rlib", "release/deps", 900, 5);
        in_deps.id = Some(id);
        in_deps.links = Some(2);
        let mut uplifted = entry("/c/release/lib.rlib", "release", 900, 5);
        uplifted.id = Some(id);
        uplifted.links = Some(2);
        uplifted.alias = true;
        let mut disposable = entry("/c/release/deps/a.rlib", "release/deps", 900, 1);
        disposable.id = Some(FileId { dev: 1, ino: 8 });
        disposable.links = Some(1);
        let files = vec![
            in_deps,
            uplifted,
            disposable,
            // Newest in `release/deps`, so the floor there falls on it rather
            // than on the linked file.
            entry("/c/release/deps/newer.rlib", "release/deps", 20, 0),
            // Keeps `release`'s floor off the linked file, and stands in for the
            // files a profile directory holds besides the links.
            entry("/c/release/other", "release", 10, 0),
        ];
        // What the walk counted: the linked file once, plus the other three.
        let held = files
            .iter()
            .filter(|f| !f.alias)
            .map(|f| f.len)
            .sum::<u64>();
        assert_eq!(held, 1830);

        let plan = plan_reclaim(&files, held, 120);
        let linked = plan
            .delete
            .iter()
            .find(|group| group.paths.len() == 2)
            .expect("the linked file has to be planned as one group");
        assert_eq!(
            linked.paths,
            vec![
                PathBuf::from("/c/release/deps/lib.rlib"),
                PathBuf::from("/c/release/lib.rlib"),
            ],
            "both names, since one alone frees nothing"
        );
        assert_eq!(linked.len, 900, "counted once, not twice");
        assert_eq!(plan.delete_bytes, 1800);
        assert_eq!(plan.unreachable_bytes, 0);
    }

    /// A file with a name the walk did not see is left alone: its bytes stay on
    /// the volume however many of its names are removed from here, so a plan
    /// that counted those bytes would report a cap it did not reach.
    #[test]
    fn a_file_with_a_name_outside_the_walk_is_not_offered() {
        let mut shared = entry("/c/release/deps/lib.rlib", "release/deps", 900, 5);
        shared.id = Some(FileId { dev: 1, ino: 9 });
        shared.links = Some(2);
        let files = vec![shared];

        let plan = plan_reclaim(&files, 900, 0);
        assert!(plan.delete.is_empty(), "{plan:?}");
        assert_eq!(plan.unreachable_bytes, 900);
    }

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
