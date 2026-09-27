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
//! 1. **The order is what losing a thing costs, at every level.** The only
//!    files a later build reads are the compiled results, so everything else
//!    goes first: `tmp`, `incremental` and `build` are scratch space,
//!    incremental state cargo is designed to discard, and re-runnable build
//!    scripts, and `examples` holds linked binaries, which a link recreates.
//!    Then the compiled results themselves, `deps` before `.fingerprint`: an
//!    artifact is worth nothing to the next build without the fingerprint that
//!    records how it was made, while a fingerprint without its artifact is a
//!    rounding error on the volume. Within a layer the same question is
//!    answered the same way, by `artifact_rank`. Every way of ordering in
//!    here is a fixed list of costs, never the alphabet -- but a plan must also
//!    be a function of the tree alone, so the path is what decides between two
//!    files that nothing else separates.
//! 2. **Every layer keeps one file** ([`FLOOR_FILES_PER_LAYER`]), the newest it
//!    has. At this granularity that floor is not a cost floor — one artifact
//!    out of thousands saves nothing worth measuring — it is a visibility floor:
//!    a layer that empties is indistinguishable from a layer the walk could not
//!    see, and the layer series is how a reader knows a layer exists at all.
//!    The newest is the only recency cargo leaves behind: it writes an artifact
//!    when it builds it and does not touch it when it reuses it. That same
//!    reading separates two files of one kind in one layer, where the older one
//!    goes first: the bytes an old configuration left behind are ones no later
//!    build has read, and they are only ever found by age. What it cannot see
//!    is that a generation's low-level crates are the ones a partial rebuild
//!    reads, so a pass deep enough to eat into the freshest generation gives up
//!    its leaves before its tips; no reading in the tree names a dependent.
//! 3. **A file goes with all of its names or not at all.** cargo hardlinks what
//!    it lifts out of `deps`, and removing one name of such a file frees nothing
//!    — the bytes stay on the volume under the other name, and the build that
//!    looked at the removed name reads the same either way. So the unit of a
//!    plan is every name of one file, and the bytes it frees are that file's
//!    bytes counted once. The same fact places such a file in the order below:
//!    one file is one thing, so it is priced by the most expensive of its names
//!    that a list knows, and only a file no name places keeps the unknown rank.
//!    Which of its names the walk counted its bytes under is a fact about the
//!    alphabet, and the alphabet cannot price anything here. A file with a name
//!    outside the measured tree is left alone for the same reason: its bytes
//!    cannot be freed from here.
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
use std::time::SystemTime;

use cog_core::fs_size::{FileEntry, FileId};

/// Layers whose loss costs only time, in the order they are given up.
///
/// Compared on the last path component, so it applies to every cargo profile:
/// `debug/incremental` and `release/incremental` are the same kind of thing.
/// `examples` holds linked binaries and their sidecars, which a link and a
/// re-run of the example recreate -- nothing in there is an input to anything.
/// Anything not listed here or in [`RESULT_LEAVES`] is treated as a compiled
/// result that no later build reads either, which is a layer this deployment
/// has not seen; it goes last. A name in no list is an absence of a reading
/// rather than a reading, so it prices a file only when nothing else does --
/// see `group_key`.
pub const DISCARDABLE_LEAVES: &[&str] = &["tmp", "incremental", "build", "examples"];

/// Layers holding the compiled results, in the order they are given up.
///
/// `deps` is every artifact cargo built, `deps` and the sidecars and linked
/// outputs among them; `.fingerprint` is how each of those was made, which is
/// what cargo compares to decide whether to reuse it. So the artifacts go
/// first, and only once they are gone does their record follow: deleting a
/// fingerprint while its artifact stays forfeits the artifact, and the whole
/// layer weighs less than one of the artifacts in the one before it.
pub const RESULT_LEAVES: &[&str] = &["deps", ".fingerprint"];

/// Files kept in every layer, whatever the cap.
///
/// One, and it is the newest file the layer has. See the module docs: this is
/// what keeps a layer from reading as absent, not what keeps a build warm.
pub const FLOOR_FILES_PER_LAYER: usize = 1;

/// The rank of a layer neither list places, which is the last of them.
///
/// Named rather than written as a literal because the question "is this layer
/// placed at all" is asked in more than one place, and a second `2` there would
/// be a second answer to it.
const UNPLACED: usize = 2;

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
    // Ordered by what losing each group costs, cheapest first: the layer that
    // places it, then the layer, then the kind of file, then the oldest of its
    // kind -- and the path last, so that two passes over one tree choose the
    // same files.
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

/// Where a group sits in the order: the layer that places it, the layer name,
/// what kind of file it is, how old it is, and the path.
///
/// Read over every name the group holds rather than over the one its bytes are
/// counted under. The names are one file -- same inode, so same size, same age,
/// same extension -- which leaves the layer as the only thing they can disagree
/// about, and cargo makes them disagree: it hardlinks what it lifts out of
/// `deps` into the profile root, and which of those two names the walk counts
/// the bytes under comes down to the alphabet. `cogneva` sorts before `deps`, so
/// its bytes are counted in the profile root, while `libcog_extension.rlib`
/// sorts after it and is counted in `deps` -- two linked binaries, one shape,
/// priced apart by the first letter of a name.
///
/// What losing a file costs is a property of the file, so the most expensive
/// name any list places decides, and a name no list places prices nothing: a
/// layer this module has never heard of is an absence of a reading, and letting
/// it outrank a layer that is known would price a file by what is not known
/// about it. A group no name places keeps the unknown rank, which is last.
fn group_key<'a>(
    group: &[&'a FileEntry],
) -> ((usize, usize), &'a str, usize, Option<SystemTime>, &'a Path) {
    group
        .iter()
        .filter(|file| placed(&file.layer))
        .map(|file| name_key(file))
        .max()
        .or_else(|| group.iter().map(|file| name_key(file)).max())
        .expect("a group is the names of one file, so at least one name")
}

/// Where one name sits in the order, on its own.
fn name_key(file: &FileEntry) -> ((usize, usize), &str, usize, Option<SystemTime>, &Path) {
    (
        layer_rank(&file.layer),
        file.layer.as_str(),
        artifact_rank(&file.path),
        file.modified,
        file.path.as_path(),
    )
}

/// Whether either list has anything to say about this layer. Everything they do
/// not is what [`layer_rank`] answers with its last bucket.
fn placed(layer: &str) -> bool {
    layer_rank(layer).0 < UNPLACED
}

/// What kind of file this is, in the order it is given up.
///
/// The layer list answers how much losing a layer costs; this answers the same
/// question one level down, and its answer comes from the same place: only the
/// compiled results are read by a later build.
///
/// - A dep-info sidecar (`.d`) names the inputs one unit was built from. It is
///   written by the same compile as that unit's artifact, and it is not what a
///   later build reads to decide freshness -- cargo keeps its own copy of that
///   inside the fingerprint directory -- so losing one costs nothing that was
///   not already paid for by losing its artifact.
/// - A linked output has no extension: a binary, a test executable, an
///   example. Compiling is over by then; recreating it is a link.
/// - Everything else is a compiled result, which other units read: losing one
///   costs its own compile and the recompile of everything downstream.
///
/// Misreading a compiled result as a linked output would spend what the cache
/// exists for, so the fallback is the last class rather than the middle one.
fn artifact_rank(path: &Path) -> usize {
    match path.extension() {
        Some(ext) if ext == "d" => 0,
        None => 1,
        Some(_) => 2,
    }
}

/// Which group a layer belongs to: what costs only time first, then the
/// compiled results, then whatever this list has never heard of.
fn layer_rank(layer: &str) -> (usize, usize) {
    let leaf = layer.rsplit('/').next().unwrap_or(layer);
    if let Some(index) = DISCARDABLE_LEAVES.iter().position(|d| *d == leaf) {
        return (0, index);
    }
    match RESULT_LEAVES.iter().position(|d| *d == leaf) {
        Some(index) => (1, index),
        None => (UNPLACED, 0),
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

        // With more to give, both layers contribute, compiled results included:
        // the artifacts first, and only then the record of how they were made.
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
                PathBuf::from("/c/release/deps/a.rlib"),
                PathBuf::from("/c/release/.fingerprint/f2"),
            ]
        );
        assert_eq!(plan.delete_bytes, 500);
    }

    /// An artifact is worth nothing to the next build without the fingerprint
    /// that says how it was made, and the whole fingerprint layer weighs less
    /// than one artifact does -- so the fingerprints are the last thing in the
    /// cache to go, although their layer name sorts before `deps`.
    #[test]
    fn the_fingerprints_follow_the_artifacts_they_describe() {
        let files = vec![
            entry("/c/debug/.fingerprint/a-lib", "debug/.fingerprint", 10, 5),
            entry("/c/debug/.fingerprint/b-lib", "debug/.fingerprint", 10, 1),
            entry("/c/debug/deps/old.rlib", "debug/deps", 100, 5),
            entry("/c/debug/deps/new.rlib", "debug/deps", 100, 0),
        ];
        // 220 held, 100 over, which the one old artifact covers on its own. The
        // layer that comes first in path order is the fingerprint layer, and
        // nothing in it is touched.
        let plan = plan_reclaim(&files, total(&files), 120);
        assert_eq!(
            planned(&plan),
            vec![PathBuf::from("/c/debug/deps/old.rlib")]
        );
        assert_eq!(plan.delete_bytes, 100);
        assert_eq!(plan.unreachable_bytes, 0);
    }

    /// Inside one layer the kinds are separated the same way: the sidecar and
    /// the linked output go before the compiled result, whatever their names
    /// sort like. A pass that has to take bytes out of the compiled results
    /// takes the ones a link and a re-run recreate first.
    #[test]
    fn the_sidecar_and_the_linked_output_go_before_the_compiled_result() {
        let files = vec![
            entry("/c/debug/deps/a.rlib", "debug/deps", 100, 5),
            entry("/c/debug/deps/z-1234", "debug/deps", 100, 5),
            entry("/c/debug/deps/z.d", "debug/deps", 100, 5),
            // The newest in the layer, so the floor falls here and the compiled
            // result above is the one the plan may choose between.
            entry("/c/debug/deps/zzz.rlib", "debug/deps", 100, 0),
        ];
        // 200 over, which two of the three eligible files cover. In path order
        // the compiled result comes first; here it is the one left standing.
        let plan = plan_reclaim(&files, total(&files), 200);
        assert_eq!(
            planned(&plan),
            vec![
                PathBuf::from("/c/debug/deps/z.d"),
                PathBuf::from("/c/debug/deps/z-1234"),
            ]
        );
        assert!(!planned(&plan).contains(&PathBuf::from("/c/debug/deps/a.rlib")));
    }

    /// Two files of one kind in one layer are separated by nothing but their
    /// age, and the older one goes: an artifact a later build reuses is one
    /// cargo does not touch, so age is the only shape a stale generation has
    /// from here.
    #[test]
    fn the_older_of_two_artifacts_of_one_kind_goes_first() {
        let files = vec![
            entry("/c/release/deps/z.rlib", "release/deps", 100, 90),
            entry("/c/release/deps/m.rlib", "release/deps", 100, 5),
            entry("/c/release/deps/a.rlib", "release/deps", 100, 1),
        ];
        // 100 over, which one file covers: the oldest, not the one whose name
        // sorts first, and not the newest, which is the floor here.
        let plan = plan_reclaim(&files, total(&files), 200);
        assert_eq!(
            planned(&plan),
            vec![PathBuf::from("/c/release/deps/z.rlib")]
        );
        assert_eq!(plan.delete_bytes, 100);
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

    /// A linked binary is priced by the name the lists place it in, not by the
    /// name the walk counted its bytes under.
    ///
    /// cargo lifts what it links out of `deps` into the profile root, and the
    /// walk counts a file's bytes under the first of its names in path order:
    /// `debug/cogneva` sorts before `debug/deps/cogneva-1a2b` while
    /// `debug/libcog_extension.rlib` sorts after `debug/deps/libcog_...rlib`, so
    /// the alphabet decides which layer one linked binary is counted in. A plan
    /// that read only that name would put the profile root's copies after the
    /// fingerprints -- spending the whole cache to keep a link's worth of bytes.
    #[test]
    fn a_linked_binary_is_priced_by_the_name_the_lists_place() {
        let id = FileId { dev: 1, ino: 11 };
        let mut at_root = entry("/c/debug/cogneva", "debug", 900, 9);
        at_root.id = Some(id);
        at_root.links = Some(2);
        let mut in_deps = entry("/c/debug/deps/cogneva-1a2b", "debug/deps", 900, 9);
        in_deps.id = Some(id);
        in_deps.links = Some(2);
        in_deps.alias = true;
        assert!(
            at_root.path < in_deps.path,
            "the fixture has to be the shape where the profile root owns the \
             bytes, which is the shape the alphabet produces"
        );

        let files = vec![
            at_root,
            in_deps,
            // A compiled result, which costs a compile to lose, and a newer one
            // that carries the floor so the layers below are about the plan.
            entry("/c/debug/deps/aaa.rlib", "debug/deps", 100, 4),
            entry("/c/debug/deps/bbb.rlib", "debug/deps", 100, 3),
            // Keeps the profile root's floor off the linked file, standing in
            // for the lock files a real one holds.
            entry("/c/debug/.cargo-lock", "debug", 0, 0),
            // Two fingerprints, since a layer's newest file is its floor and one
            // alone would be out of the plan entirely. Losing either of them is
            // what makes the rest of the cache unusable.
            entry(
                "/c/debug/.fingerprint/aaa-1a2b",
                "debug/.fingerprint",
                400,
                6,
            ),
            entry(
                "/c/debug/.fingerprint/zzz-9f8e",
                "debug/.fingerprint",
                400,
                7,
            ),
        ];
        // What the walk counted: the linked file once, plus the other five --
        // one of which is the empty lock file, so it adds nothing to the total.
        let held = 900 + 100 + 100 + 400 + 400;
        assert_eq!(
            held,
            files
                .iter()
                .filter(|f| !f.alias)
                .map(|f| f.len)
                .sum::<u64>()
        );

        // 900 over, which the linked binary covers on its own. Priced last, the
        // plan would instead take `aaa`'s recompile and a fingerprint -- and
        // still have to take the binary, spending 1400 bytes of cache on a 900
        // byte excess and forfeiting a layer the next build reads.
        let plan = plan_reclaim(&files, held, held - 900);
        assert_eq!(plan.delete_bytes, 900, "{plan:?}");
        assert_eq!(plan.unreachable_bytes, 0);
        assert_eq!(
            planned(&plan),
            vec![
                PathBuf::from("/c/debug/cogneva"),
                PathBuf::from("/c/debug/deps/cogneva-1a2b"),
            ],
            "both names go, and nothing else does: a link is cheaper than a \
             recompile and cheaper than the record of one"
        );
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
