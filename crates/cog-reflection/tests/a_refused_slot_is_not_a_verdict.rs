//! Verification that never got a slot must not read as a change that failed.
//!
//! The verification path runs the heaviest build on this host, so it takes a
//! build slot too. What it must not do is turn a refusal into a judgement: the
//! caller retires a change on `Ok(verdict: Refused(..))` and keeps it on `Err`,
//! so a refusal that came back as a verdict would spend the change -- and the
//! generation that produced it -- on the host simply being busy. Nothing about
//! the change was judged, and nothing about it failed.
//!
//! Asserted through the tree, because the damage is a git state: a workspace
//! left holding a change that was never verified.

use std::path::Path;
use std::process::Command;

use cog_core::config::BuildGateConfig;
use cog_reflection::types::{EvolutionKind, EvolutionResult, EvolutionStatus};

/// Formatted to begin with, so the formatting gate passes and the run reaches
/// the test step where the slot is taken. If it were unformatted the refusal
/// below would come from the wrong gate and prove nothing.
const SEED: &str = "pub fn answer() -> i32 {\n    41\n}\n";

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
async fn a_refused_slot_keeps_the_change_alive_and_the_tree_clean() {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    git(
        root.path(),
        &["config", "user.email", "gates@example.invalid"],
    );
    git(root.path(), &["config", "user.name", "gates"]);
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname = \"slot-probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(root.path().join("src")).unwrap();
    std::fs::write(root.path().join("src/lib.rs"), SEED).unwrap();
    git(root.path(), &["add", "-A"]);
    git(root.path(), &["commit", "-q", "-m", "seed"]);

    let change = EvolutionResult {
        kind: EvolutionKind::CodeChange,
        artifact_id: "slot-1".into(),
        description: "修正这个取值的计算".into(),
        content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                  --- a/src/lib.rs\n\
                  +++ b/src/lib.rs\n\
                  @@ -1,3 +1,3 @@\n\
                  \x20pub fn answer() -> i32 {\n\
                  -    41\n\
                  +    42\n\
                  \x20}\n"
            .into(),
        status: EvolutionStatus::CompileChecked,
        created_at: chrono::Utc::now(),
        eval_summary: None,
    };

    let lock_dir = tempfile::tempdir().unwrap();
    let gate = cog_core::build_gate::install(&BuildGateConfig {
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

    let pipeline =
        cog_reflection::ChangePipeline::new(root.path(), root.path().join("changes"), false);
    let err = pipeline
        .apply_and_test_in(&change, root.path())
        .await
        .expect_err("a gate at its bound with no wait budget must refuse");
    assert!(
        err.is_build_slot_refused(),
        "the caller has to be able to tell a refused slot from a failed test run: {err}"
    );

    assert_eq!(
        std::fs::read_to_string(root.path().join("src/lib.rs")).unwrap(),
        SEED,
        "a change that was never verified must not be left in the workspace for the next run to judge"
    );
    assert_eq!(
        git(root.path(), &["rev-list", "--count", "HEAD"]),
        "1",
        "a refusal is not a reason to commit"
    );
}
