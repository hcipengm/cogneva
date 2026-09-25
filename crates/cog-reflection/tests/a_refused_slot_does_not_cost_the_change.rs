//! A build that never got a slot must not cost the change its commit.
//!
//! The build slot is taken *before* the commit for exactly this reason. Taken
//! after it, a host that stayed busy for the whole wait budget would land in the
//! rollback arm: the change would be destroyed, and it would be recorded as a
//! change that failed to compile. It did not fail to compile -- it never
//! started, and the reason belongs to the host, not to the code.
//!
//! Two places have to say that, and both are asserted here: the tree (a commit
//! that should still be there, or one that is gone) and the record of what the
//! build cost, where a refusal counted under the failures would put the host's
//! load on the change's account and a duration published for it would be the
//! length of a wait rather than of a build.
//!
//! One test for both, because the gate is process-wide: the slot the harness
//! installs is the only one this binary has, so a second test holding it would
//! make whichever ran second refuse for the wrong reason.

use std::path::Path;
use std::process::Command;

use cog_core::observability::Observable;
use cog_core::types::task::EvolutionIntent;
use cog_reflection::evolution_build_readings::{
    EvolutionBuildReadings, BUILD_LAST_SECONDS_METRIC, BUILD_OUTCOMES_TOTAL_METRIC,
};

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[tokio::test]
async fn a_refused_slot_leaves_the_tree_uncommitted() {
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q"]);
    git(
        repo.path(),
        &["config", "user.email", "gates@example.invalid"],
    );
    git(repo.path(), &["config", "user.name", "gates"]);
    std::fs::write(repo.path().join("a.txt"), "one\n").unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-q", "-m", "base"]);

    // The change, applied and sitting in the tree, which is the state the
    // deployer is handed.
    std::fs::write(repo.path().join("b.txt"), "change\n").unwrap();

    let lock_dir = tempfile::tempdir().unwrap();
    let gate = cog_core::build_gate::install(&cog_core::config::BuildGateConfig {
        enabled: true,
        max_concurrent: 1,
        wait_secs: 0,
        lock_dir: lock_dir.path().to_string_lossy().into_owned(),
    });
    let held = gate
        .try_acquire("test holds the only slot")
        .await
        .expect("an empty gate admits the first build");
    assert!(
        held.held(),
        "the slot has to be really taken, or the refusal below means nothing"
    );

    let readings = std::sync::Arc::new(EvolutionBuildReadings::new());
    let deployer = cog_reflection::EvolutionDeployer::new(repo.path(), repo.path(), repo.path())
        .with_build_readings(readings.clone());
    let err = deployer
        .commit_and_build_in("change-1", repo.path(), Some(EvolutionIntent::CiFix))
        .await
        .expect_err("a gate at its bound with no wait budget must refuse");
    assert!(
        err.is_build_slot_refused(),
        "the caller has to be able to tell a refused slot from a failed build: {err}"
    );

    assert_eq!(
        git(repo.path(), &["rev-list", "--count", "HEAD"]),
        "1",
        "a refusal is not a reason to commit"
    );
    let status = git(repo.path(), &["status", "--porcelain"]);
    assert!(
        status.contains("b.txt"),
        "the change is untouched and unjudged: {status}"
    );

    let metrics = readings.collect_metrics("").await.unwrap();
    let value = |name: &str, outcome: &str| -> Option<f64> {
        metrics
            .iter()
            .find(|m| {
                m.name == name
                    && m.labels.get("intent").map(String::as_str) == Some("ci_fix")
                    && m.labels.get("outcome").map(String::as_str) == Some(outcome)
            })
            .map(|m| m.value)
    };
    assert_eq!(
        value(BUILD_OUTCOMES_TOTAL_METRIC, "unstarted"),
        Some(1.0),
        "the refusal is not on the reading"
    );
    for outcome in ["built", "failed", "timed_out"] {
        assert_eq!(
            value(BUILD_OUTCOMES_TOTAL_METRIC, outcome),
            Some(0.0),
            "a build that never started was counted as {outcome}"
        );
    }
    assert!(
        !metrics.iter().any(|m| m.name == BUILD_LAST_SECONDS_METRIC
            && m.labels.get("intent").map(String::as_str) == Some("ci_fix")),
        "a wait that ended in a refusal is not a build duration"
    );
}
