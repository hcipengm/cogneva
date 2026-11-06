//! Whose red is it: the change's, or the tree it landed on.
//!
//! A landed change is judged by the CI of the whole base branch, so a check that
//! was already failing on the commit it was replayed onto fails here too. The
//! landing channel reverts a landed change whose CI is red, which makes this
//! question expensive to get wrong in the direction of "the change did it": the
//! revert deletes work that did nothing wrong, and the tree it restores fails
//! the same checks.
//!
//! Two readings are not evidence that *this* change failed anything, and both
//! are ordinary rather than rare:
//!
//! 1. **A check that never reached a conclusion.** `cancelled` -- a workflow
//!    superseded by a newer push, or a runner taken away -- and `stale` say
//!    nothing about the code. Measured 2026-11-06: a change whose only
//!    non-passing check was a cancelled macOS bootstrap was reverted sixteen
//!    minutes after it landed, while every check that actually ran passed.
//! 2. **A comparison made against a tree that is still being read.** Failing
//!    checks are compared by name against the commit the change was replayed
//!    onto, and that commit's checks conclude on their own schedule. A check can
//!    fail here before it is recorded as failing there, and the change is then
//!    convicted of a red the previous commit had. Measured the same day: two
//!    landings went that way, one of them because the parent commit's identical
//!    `Entry Scripts Lint` failure concluded four minutes after the revert.
//!
//! So the answer is three-valued, and "the tree has not finished being read" is
//! its own answer rather than a conviction.

use cog_core::contract::ci::ci_conclusion_passes;

use crate::provider::CiCheckReadings;

/// Whether a red verdict on a landed revision belongs to the change or to the
/// tree it was replayed onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attribution {
    /// Every check this revision failed was already failing on the commit it
    /// was replayed onto. Reverting would restore a tree that fails the same
    /// way, so the change keeps its place and stays watched.
    Inherited,
    /// At least one check failed here that the tree it landed on had concluded
    /// as passing.
    Own,
    /// The question cannot be settled from this reading: nothing here names a
    /// failure of the code, or the tree it landed on has checks still running.
    Unanswerable,
}

/// A conclusion that says nothing about the code.
///
/// Deliberately not part of [`ci_conclusion_passes`]: that predicate answers
/// "may this revision be rolled out", where an unclear conclusion must stay on
/// the safe side. Here the question is "did this change break something", and a
/// job that never ran has no answer to give either way.
fn concludes_nothing_about_the_code(conclusion: &str) -> bool {
    matches!(conclusion, "cancelled" | "stale")
}

/// The checks that concluded as a failure of the code.
fn definitive_failures(readings: &CiCheckReadings) -> Vec<&str> {
    readings
        .concluded
        .iter()
        .filter(|(_, conclusion)| {
            !ci_conclusion_passes(conclusion) && !concludes_nothing_about_the_code(conclusion)
        })
        .map(|(name, _)| name.as_str())
        .collect()
}

/// Attribute a red verdict on `child` between the change and the tree it was
/// replayed onto.
///
/// `parent` is the same reading for the commit the change landed on, and `None`
/// when it could not be read at all.
pub fn attribute(child: &CiCheckReadings, parent: Option<&CiCheckReadings>) -> Attribution {
    let here = definitive_failures(child);
    if here.is_empty() {
        // Every non-passing check either passed by the platform's rules or
        // never reached a conclusion, so nothing here names a failure of the
        // change. The red verdict came from elsewhere and this reading cannot
        // answer for it.
        return Attribution::Unanswerable;
    }
    let Some(parent) = parent else {
        // No reading of the tree it was replayed onto. An unreadable
        // attribution is not an exoneration, so the change keeps answering for
        // the red, which is what every caller did before this existed.
        return Attribution::Own;
    };
    if parent.pending {
        // The tree is still being read. A check that failed here may yet be
        // recorded as failing there, and if it is, the red is the tree's.
        return Attribution::Unanswerable;
    }
    if here
        .iter()
        .all(|name| definitive_failures(parent).contains(name))
    {
        Attribution::Inherited
    } else {
        Attribution::Own
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readings(pending: bool, checks: &[(&str, &str)]) -> CiCheckReadings {
        CiCheckReadings {
            concluded: checks
                .iter()
                .map(|(name, conclusion)| (name.to_string(), conclusion.to_string()))
                .collect(),
            pending,
        }
    }

    #[test]
    fn a_red_built_only_from_cancellations_convicts_nobody() {
        // 2026-11-06 05:07: landed with every check green but one, whose job was
        // cancelled. It was reverted sixteen minutes later.
        let child = readings(
            false,
            &[("Test", "success"), ("Bootstrap (macos)", "cancelled")],
        );
        let parent = readings(
            false,
            &[("Test", "success"), ("Bootstrap (macos)", "success")],
        );
        assert_eq!(attribute(&child, Some(&parent)), Attribution::Unanswerable);
    }

    #[test]
    fn a_stale_check_is_not_a_failure_of_the_code() {
        let child = readings(false, &[("Test", "stale")]);
        let parent = readings(false, &[("Test", "success")]);
        assert_eq!(attribute(&child, Some(&parent)), Attribution::Unanswerable);
    }

    #[test]
    fn a_definitive_failure_still_convicts_beside_a_cancelled_one() {
        let child = readings(
            false,
            &[("Test", "failure"), ("Bootstrap (macos)", "cancelled")],
        );
        let parent = readings(
            false,
            &[("Test", "success"), ("Bootstrap (macos)", "success")],
        );
        assert_eq!(attribute(&child, Some(&parent)), Attribution::Own);
    }

    #[test]
    fn a_comparison_against_a_tree_still_being_read_waits() {
        // 2026-11-06 08:31: the parent's identical `Entry Scripts Lint` failure
        // concluded four minutes after this change was reverted for it.
        let child = readings(
            false,
            &[("Test", "failure"), ("Entry Scripts Lint", "failure")],
        );
        let parent = readings(true, &[("Test", "failure")]);
        assert_eq!(attribute(&child, Some(&parent)), Attribution::Unanswerable);
    }

    #[test]
    fn a_red_the_tree_already_had_is_inherited() {
        let child = readings(false, &[("Test", "failure"), ("Code Coverage", "failure")]);
        let parent = readings(
            false,
            &[
                ("Test", "failure"),
                ("Code Coverage", "failure"),
                ("deploy", "failure"),
            ],
        );
        assert_eq!(attribute(&child, Some(&parent)), Attribution::Inherited);
    }

    #[test]
    fn one_check_the_tree_had_green_is_the_changes_own() {
        let child = readings(false, &[("Test", "failure"), ("Web Build", "failure")]);
        let parent = readings(false, &[("Test", "failure"), ("Web Build", "success")]);
        assert_eq!(attribute(&child, Some(&parent)), Attribution::Own);
    }

    #[test]
    fn a_tree_that_could_not_be_read_keeps_the_change_answering() {
        // The fallback is the behaviour before attribution existed: an
        // unreadable other side is not an exoneration.
        let child = readings(false, &[("Test", "failure")]);
        assert_eq!(attribute(&child, None), Attribution::Own);
    }
}
