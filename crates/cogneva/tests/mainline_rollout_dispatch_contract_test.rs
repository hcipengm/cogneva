//! principle: feedback_a_counted_call_site_going_red_asks_for_a_signature_not_a_loosened_assertion
//! The deployer decides which revision to roll at the top of a round, then
//! spends a cold release build getting there. Nothing between those two moments
//! errors when the upstream moves past that revision — the rollout simply
//! happens, the deployments converge onto a revision upstream has left, and the
//! next round pays a second rollout Job and a second set of workload restarts
//! to reach a tip that was already known.
//!
//! Every reading of a wasted rollout is silent: the extra Job looks like a Job,
//! the second restart looks like a restart, and the round that skipped nothing
//! looks exactly like the round that never asked. So the links are asserted
//! here, over the sources: that a dispatch cannot be reached without passing the
//! guard, and that the guard's outcome is a reading in both of its branches —
//! absent, the "nothing was superseded" case and the "no guard is wired" case
//! are the same series.

use std::path::PathBuf;

const DEPLOYER: &str = "crates/cog-reflection/src/mainline_deployer.rs";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// Everything up to the first test-only item: a test may name anything it
/// likes, and a fixture that dispatches a Job is not a dispatch site.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// Assembled at compile time so this file never contains the text it forbids.
const DISPATCH: &str = concat!("self.dispatch_", "job(");
const GUARD_CALL: &str = concat!("self.roll_", "out(");
const GUARD_DEF: &str = "async fn roll_out(";

#[test]
fn no_rollout_is_dispatched_without_passing_the_guard() {
    let source = read(DEPLOYER);
    let production = production_source(&source);

    let sites: Vec<usize> = production.match_indices(DISPATCH).map(|(i, _)| i).collect();
    assert_eq!(
        sites.len(),
        1,
        "the deployer dispatches a rollout job from {} places. A site outside the guard rolls a \
         revision the upstream may already have passed, and nothing about that round reads \
         differently from a correct one",
        sites.len()
    );

    let guard = production
        .find(GUARD_DEF)
        .expect("the guard is gone; every dispatch site is then unguarded and silent about it");
    let end_of_guard = production[guard..]
        .match_indices("\n    async fn ")
        .map(|(i, _)| guard + i)
        .next()
        .unwrap_or(production.len());
    assert!(
        guard < sites[0] && sites[0] < end_of_guard,
        "the only dispatch site is not inside the guard's own body"
    );
}

#[test]
fn every_path_that_starts_a_rollout_asks_the_guard() {
    let source = read(DEPLOYER);
    let production = production_source(&source);
    // Four paths reach a dispatch: the registry fast path, the build path, the
    // re-dispatch an external apply forces, and the rollback a withdrawal
    // dispatches to the pin's landing revision. A deleted call site changes no
    // behaviour that any other reading can see; an added one is a new answer to
    // "which revision goes on the cluster", so it is counted here on purpose.
    let guarded = production.matches(GUARD_CALL).count();
    assert_eq!(
        guarded, 4,
        "{guarded} of the deployer's dispatch paths ask the guard. Adding or removing one is a \
         change to which revisions get rolled — and to nothing else this file can observe"
    );
}

#[test]
fn only_the_withdrawal_skips_the_ci_verdict() {
    let source = read(DEPLOYER);
    let production = production_source(&source);
    // The guard takes a `withdrawal` flag that skips the CI gate: that gate asks
    // whether the revision being rolled has a failing check, and an operator's
    // withdrawal is not an answer to that question. Every other path has to ask
    // it. The flag is a bare `true`/`false` at a call site, so moving it — or
    // copying a call and leaving it `true` — would roll red revisions with
    // nothing in this file, or any series, reading differently.
    let skipping = production
        .match_indices(GUARD_CALL)
        .filter(|(i, _)| {
            let tail = &production[*i..];
            let args = &tail[..tail.find(')').unwrap_or(tail.len())];
            args.trim_end().ends_with("true")
        })
        .count();
    assert_eq!(
        skipping, 1,
        "{skipping} of the deployer's dispatch paths skip the CI verdict. Only a withdrawal is \
         entitled to: it rolls a revision that was already delivered, whose check verdict is \
         whatever it was then and will never be re-asked"
    );
}

#[test]
fn both_answers_the_guard_gives_are_published() {
    let source = read(DEPLOYER);
    let production = production_source(&source);
    assert!(
        production.contains(concat!("MAINLINE_SUPERSEDED_", "ROLLOUT_TOTAL")),
        "the guard records no reading of its own; a rollout it skipped and one it never \
         considered are then the same round"
    );
    for branch in [
        concat!("record_supersession_", "reading(true)"),
        concat!("record_supersession_", "reading(false)"),
    ] {
        assert!(
            production.contains(branch),
            "{branch} is missing: the guard has to publish the rounds it let through as well as \
             the ones it stopped, or a deleted call site leaves no series at all"
        );
    }
}
