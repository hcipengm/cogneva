//! A cache trimmed to its cap must still be a cache.
//!
//! The cap exists so that a shared `CARGO_TARGET_DIR` cannot fill its volume and
//! take every build down with it. The failure that would make it worse than the
//! problem is removing bytes in a way that turns the next build into a cold
//! one, or that leaves cargo unable to build at all -- so both halves are
//! asserted here against real cargo rather than against a plan: the occupancy
//! falls under the cap, and what the cap did not require stays exactly where it
//! was.
//!
//! Three passes, because "trimmed" covers three outcomes that must not be read
//! as one:
//!
//! - a pass that pays from the layers whose loss costs time leaves the workspace
//!   up to date, which is the whole point of ordering the layers that way;
//! - a pass that has to pay out of the compiled results takes the bytes whose
//!   loss costs a link before the ones whose loss costs a compile, so the
//!   dependency the next build would otherwise recompile is still there;
//! - a cap the cache cannot be brought under is reported as unmet, and the
//!   workspace still builds and runs afterwards.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use cog_core::build_gate::BuildGate;
use cog_core::config::BuildGateConfig;
use cog_core::fs_size;
use cog_core::observability::Observable;
use cog_reflection::build_cache_readings::{
    BuildCacheReadings, BUILD_TARGET_OVER_CAP_METRIC, BUILD_TARGET_UNMET_METRIC, CACHE_LAYER_DEPTH,
    OUTCOME_LABEL, OUTCOME_RECLAIMED,
};
use cog_reflection::build_cache_reclaim::{plan_reclaim, DISCARDABLE_LEAVES, RESULT_LEAVES};

/// A two-crate workspace with one path dependency: enough for cargo to hold a
/// compiled result it can reuse and an incremental state it can keep, and small
/// enough that a test can build it from cold in about a second.
///
/// The names are not decoration. Under a plan that falls back to the alphabet,
/// a cache whose artifacts sort before the ones that depend on them is the
/// shape that loses the reusable half, so the dependency here is named to sort
/// first and the binary that uses it to sort last.
fn write_fixture(root: &Path) {
    std::fs::create_dir_all(root.join("lib/src")).unwrap();
    std::fs::create_dir_all(root.join("app/src")).unwrap();
    std::fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"lib\", \"app\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    std::fs::write(
        root.join("lib/Cargo.toml"),
        "[package]\nname = \"aardvark-lib\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    std::fs::write(
        root.join("lib/src/lib.rs"),
        "pub fn add(a: u64, b: u64) -> u64 {\n    a + b\n}\n",
    )
    .unwrap();
    std::fs::write(
        root.join("app/Cargo.toml"),
        "[package]\nname = \"zebra-app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
         [dependencies]\naardvark-lib = { path = \"../lib\" }\n",
    )
    .unwrap();
    std::fs::write(
        root.join("app/src/main.rs"),
        "fn main() {\n    println!(\"{}\", aardvark_lib::add(1, 2));\n}\n",
    )
    .unwrap();
}

/// The layer a planned path sits in, by its last component, so a claim about
/// where a plan took its bytes from can be read off the walk.
fn layer_of(files: &[fs_size::FileEntry], path: &Path) -> String {
    let entry = files
        .iter()
        .find(|f| f.path == path)
        .expect("a plan may only name files the walk saw");
    entry
        .layer
        .rsplit('/')
        .next()
        .unwrap_or(entry.layer.as_str())
        .to_string()
}

/// Build the fixture with cargo, in a target directory and an environment of the
/// test's choosing, and return what cargo said.
///
/// Both streams are returned because the lines this test reads are cargo's
/// progress lines ("Compiling", "Fresh"), and those go to stderr: a reader that
/// kept only stdout would see an empty run and conclude nothing was built.
///
/// `CARGO_INCREMENTAL` is pinned rather than inherited: the fixture has to have
/// a speed layer for the layer ordering to be about anything, and whether the
/// outer run enabled incremental compilation is not something this test should
/// depend on.
fn build(target: &Path, workspace: &Path, args: &[&str]) -> String {
    let out = Command::new(env!("CARGO"))
        .args(args)
        .current_dir(workspace)
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "1")
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .unwrap_or_else(|e| panic!("cargo {args:?}: {e}"));
    assert!(
        out.status.success(),
        "cargo {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A build gate in force over its own slot directory, so the pass holds the slot
/// the way the watcher does.
fn gate(lock_dir: &Path) -> Arc<BuildGate> {
    Arc::new(BuildGate::new(&BuildGateConfig {
        enabled: true,
        max_concurrent: 1,
        wait_secs: 0,
        lock_dir: lock_dir.display().to_string(),
    }))
}

/// Build the fixture from cold, then read the cache the way the watcher does.
fn warm_cache(root: &Path) -> (PathBuf, Vec<fs_size::FileEntry>, u64) {
    write_fixture(root);
    let target = root.join("target");
    let cold = build(&target, root, &["build", "-v"]);
    assert!(
        cold.contains("Compiling"),
        "a cold build has to compile something: {cold}"
    );
    let files = fs_size::dir_files(&target, CACHE_LAYER_DEPTH, &[]).unwrap();
    let total = fs_size::counted_bytes(&files);
    assert_eq!(
        total,
        fs_size::dir_size_bytes(&target, &[]).unwrap(),
        "the walk the cap is judged against and the one the reading comes from \
         must be the same tree"
    );
    // cargo hardlinks what it lifts out of `deps`, so the fixture has to have
    // such a link for the accounting below to be about anything.
    assert!(
        files.iter().any(|f| f.alias),
        "the fixture holds no hardlinked artifact: {files:?}"
    );
    assert!(total < files.iter().map(|f| f.len).sum::<u64>());
    (target, files, total)
}

/// A pass whose excess one file covers takes that file from a layer whose loss
/// costs time, and the workspace is still up to date afterwards.
#[tokio::test]
async fn a_pass_that_pays_from_the_speed_layers_leaves_the_workspace_up_to_date() {
    let root = tempfile::tempdir().unwrap();
    let (target, files, total) = warm_cache(root.path());

    // One byte over: the plan owes one byte, and where it takes it from is the
    // whole question this test asks. One file with bytes in it covers that, so
    // exactly one of them is named -- cargo leaves zero-length files (lock
    // files) behind, and naming one of those costs the plan a choice without
    // buying it a byte.
    let plan = plan_reclaim(&files, total, total - 1);
    assert_eq!(
        plan.delete.iter().filter(|group| group.len > 0).count(),
        1,
        "{plan:?}"
    );
    let removed = plan.delete[0].paths.clone();
    for path in &removed {
        let entry = files
            .iter()
            .find(|f| f.path == *path)
            .expect("a plan may only name files the walk saw");
        let leaf = entry
            .layer
            .rsplit('/')
            .next()
            .unwrap_or(entry.layer.as_str());
        assert!(
            DISCARDABLE_LEAVES.contains(&leaf),
            "the file given up came from layer {} ({}), which costs a recompile \
             to lose; the layers that only cost time come first",
            entry.layer,
            path.display()
        );
    }

    let lock = tempfile::tempdir().unwrap();
    let readings = BuildCacheReadings::new(&target).with_cap(total - 1, 300);
    readings.enforce_cap(&files, Some(&gate(lock.path()))).await;

    for path in &removed {
        assert!(
            !path.exists(),
            "the pass has to have removed {}",
            path.display()
        );
    }
    let after = fs_size::dir_size_bytes(&target, &[]).unwrap();
    assert!(after < total, "{after} is still over the cap");
    assert_eq!(
        after,
        total - plan.delete_bytes,
        "the cap's excess is all that may go: a pass that removes more than it \
         was asked for spends the cache it exists to protect"
    );
    let metrics = readings.collect_metrics("").await.unwrap();
    let outcome = |name: &str, wanted: Option<&str>| {
        metrics
            .iter()
            .find(|m| {
                m.name == name
                    && wanted
                        .is_none_or(|w| m.labels.get(OUTCOME_LABEL).map(String::as_str) == Some(w))
            })
            .map(|m| m.value)
    };
    assert_eq!(
        outcome(BUILD_TARGET_OVER_CAP_METRIC, Some(OUTCOME_RECLAIMED)),
        Some(1.0),
        "reaching the cap has to be readable as reaching it"
    );
    assert_eq!(outcome(BUILD_TARGET_UNMET_METRIC, None), Some(0.0));

    // The claim the layer ordering is for: nothing the trim removed was
    // something the next build needed.
    let warm = build(&target, root.path(), &["build", "-v"]);
    assert_eq!(
        warm.matches("Compiling").count(),
        0,
        "trimming the speed layers must not cost a recompile: {warm}"
    );
}

/// A cap below what the per-layer floor keeps is a cap that cannot hold. The
/// pass still does everything it may, and the disk and the reading agree about
/// what is left over -- a cap that quietly stopped being enforced is the state
/// this whole surface exists to make visible.
///
/// This is the shape a small cap has on a real cache, not a corner case: one
/// file per layer stays whatever the cap says, and in a workspace whose newest
/// compiled result is also its largest the floor is most of the cache. A cap
/// under it is therefore a cap that cannot be met, and it has to read that way
/// instead of reading as a cache that was brought down to it.
#[tokio::test]
async fn a_cap_under_the_floor_is_reported_and_the_cache_still_builds() {
    let root = tempfile::tempdir().unwrap();
    let (target, files, total) = warm_cache(root.path());

    let cap = total / 4;
    let plan = plan_reclaim(&files, total, cap);
    assert!(plan.unreachable_bytes > 0, "{plan:?}");

    let lock = tempfile::tempdir().unwrap();
    let readings = BuildCacheReadings::new(&target).with_cap(cap, 300);
    readings.enforce_cap(&files, Some(&gate(lock.path()))).await;

    // Everything the plan may remove went, so what is on disk is the cap plus
    // the part of it that cannot be reached.
    let after = fs_size::dir_size_bytes(&target, &[]).unwrap();
    assert_eq!(after, cap + plan.unreachable_bytes, "{plan:?}");
    let metrics = readings.collect_metrics("").await.unwrap();
    let outcome = |name: &str, wanted: Option<&str>| {
        metrics
            .iter()
            .find(|m| {
                m.name == name
                    && wanted
                        .is_none_or(|w| m.labels.get(OUTCOME_LABEL).map(String::as_str) == Some(w))
            })
            .map(|m| m.value)
    };
    assert_eq!(
        outcome(BUILD_TARGET_UNMET_METRIC, None),
        Some(plan.unreachable_bytes as f64),
        "what stayed above the cap is the reading a reader has to be able to act on"
    );
    assert_eq!(
        outcome(BUILD_TARGET_OVER_CAP_METRIC, Some(OUTCOME_RECLAIMED)),
        Some(0.0)
    );

    // Still a cache root, and still a workspace: cargo heals whatever the pass
    // took and the program runs. What it costs in rebuilds is not asserted
    // here: a cap this far below what the floor keeps is not a cache being
    // trimmed, it is a cache being emptied.
    let run = build(&target, root.path(), &["run", "-q"]);
    assert!(run.contains('3'), "{run}");
}

/// A pass that has to reach past the layers whose loss costs time still takes
/// the cheapest thing the compiled results hold: a sidecar, whose absence costs
/// nothing at all, and the workspace the pass leaves behind is still up to date.
///
/// This is the shape of a real pass, and the reason it is asserted against
/// cargo rather than against a plan: a plan that reached for the alphabet
/// instead would take the fingerprint layer first -- its name sorts ahead of
/// `deps` -- and a workspace whose fingerprints are gone is dirty whatever its
/// compiled results still hold.
#[tokio::test]
async fn a_pass_that_reaches_the_compiled_results_still_leaves_the_workspace_warm() {
    let root = tempfile::tempdir().unwrap();
    let (target, files, total) = warm_cache(root.path());

    // The cut this test is about is the shallowest one that reaches the
    // compiled results: everything the layers whose loss costs time can pay,
    // and then one byte. Both halves are read off the plan rather than assumed
    // -- which is also where the floor drops out of the arithmetic, since a
    // group the floor keeps is one the plan never names.
    let drained = plan_reclaim(&files, total, 0);
    let among_results = |path: &Path| RESULT_LEAVES.contains(&layer_of(&files, path).as_str());
    assert!(
        drained
            .delete
            .iter()
            .any(|group| group.paths.iter().any(|p| among_results(p))),
        "the fixture has to have compiled results the plan may reach: {drained:?}"
    );
    let payable: u64 = drained
        .delete
        .iter()
        .take_while(|group| !group.paths.iter().any(|p| among_results(p)))
        .map(|group| group.len)
        .sum();

    let cap = total - payable - 1;
    let plan = plan_reclaim(&files, total, cap);

    let lock = tempfile::tempdir().unwrap();
    let readings = BuildCacheReadings::new(&target).with_cap(cap, 300);
    readings.enforce_cap(&files, Some(&gate(lock.path()))).await;
    let after = fs_size::dir_size_bytes(&target, &[]).unwrap();
    assert_eq!(
        after,
        total - plan.delete_bytes,
        "the cap's excess is all that may go"
    );
    assert!(after <= cap, "{after} is still over the cap {cap}");

    // What the ordering is for, read off the tool that would have to do the
    // work: the pass reached the compiled results and cost the workspace
    // nothing, because what it took first was the one kind of file there whose
    // absence is not a rebuild.
    let warm = build(&target, root.path(), &["build", "-v"]);
    assert_eq!(
        warm.matches("Compiling").count(),
        0,
        "reaching the compiled results cost a rebuild, which is the outcome the \
         ordering exists to prevent: {warm}"
    );

    // Asserted last so that a plan which never reached the compiled results
    // fails above, on what it cost, rather than here on what it was about.
    let planned: Vec<String> = plan
        .delete
        .iter()
        .flat_map(|group| group.paths.iter())
        .map(|path| layer_of(&files, path))
        .collect();
    assert!(
        planned.iter().any(|layer| layer == "deps"),
        "the cut has to land in the compiled results, or nothing above was \
         about them: {planned:?}"
    );
}
