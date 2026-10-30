//! Gate: the judge's kind vocabulary is one closed set, and every member of it
//! answers which way its check points.
//!
//! The vocabulary used to live in a hand-kept table beside the code, and the
//! table was a restatement of three things the code already knew: which kinds the
//! judge answers, which names it refuses, and which way each one points. Anything
//! restated drifts, and this one drifted in the expensive direction -- a kind the
//! intake accepts but the judge has no predicate for is accepted, stored, and
//! then answered by the judge *before it ever reads the scope*. The row reads as
//! outstanding work for good, and nothing anywhere says so.
//!
//! The vocabulary now lives where it is used: `JUDGEABLE_KINDS` and
//! `RETIRED_KINDS` next to the dispatch arms, and `kind_direction` next to the
//! constants that decide the answer. What is left to gate is the seam between
//! them -- a kind added to either list without a direction, a kind in both, a
//! direction that no longer matches the predicate answering it. Those are exactly
//! the edits that happen when someone adds a kind and forgets the other half.
//!
//! The intake is the other side and is not reachable from here: it lives in the
//! private repository that feeds the account. A kind the intake accepts and this
//! does not name is a row nothing will ever measure.

use std::collections::BTreeSet;

use cog_orchestrator::change_measure::{
    kind_direction, KindDirection, ABSENCE_KINDS, EXISTS_KIND, GONE_KIND, GREP_KIND,
    JUDGEABLE_KINDS, RETIRED_KINDS,
};

fn as_set(items: impl IntoIterator<Item = &'static str>) -> BTreeSet<&'static str> {
    items.into_iter().collect()
}

fn kinds_with_direction(want: KindDirection) -> BTreeSet<&'static str> {
    JUDGEABLE_KINDS
        .iter()
        .chain(RETIRED_KINDS.iter())
        .copied()
        .filter(|kind| kind_direction(kind) == Some(want))
        .collect()
}

/// Anti-vacuity: the checks below compare sets, and a vocabulary that emptied out
/// would make every one of them pass by comparing two empty sets.
#[test]
fn the_vocabulary_is_not_empty() {
    assert!(
        JUDGEABLE_KINDS.len() >= 6,
        "the judge publishes {} kind(s); the vocabulary moved or shrank and the \
         checks below are comparing nothing",
        JUDGEABLE_KINDS.len()
    );
    assert!(!RETIRED_KINDS.is_empty(), "no retired name is listed");
}

/// A name in both lists would be answered and refused at once, and whichever
/// helper a caller used would decide which -- with `kind_direction` returning
/// the first match, the ambiguity would be invisible.
#[test]
fn no_kind_is_both_answered_and_retired() {
    let judgeable = as_set(JUDGEABLE_KINDS);
    let retired = as_set(RETIRED_KINDS);
    let both: Vec<&&str> = judgeable.intersection(&retired).collect();
    assert!(
        both.is_empty(),
        "these kinds are in JUDGEABLE_KINDS and RETIRED_KINDS at once: {both:?} -- a \
         declaration under one would be measured by one reader and refused by another"
    );
    for name in JUDGEABLE_KINDS.iter().chain(RETIRED_KINDS.iter()) {
        let seen = JUDGEABLE_KINDS.iter().filter(|k| *k == name).count()
            + RETIRED_KINDS.iter().filter(|k| *k == name).count();
        assert_eq!(seen, 1, "`{name}` is listed {seen} times");
    }
}

/// Every kind on either list has to answer which way it points. This is the arm
/// that catches a kind added to `JUDGEABLE_KINDS` and nowhere else: it would be
/// measurable by name and would carry no criterion, so a row could arrive under
/// it and nothing would say what it asks for.
#[test]
fn every_listed_kind_answers_which_way_it_points() {
    let mut unanswered = Vec::new();
    for kind in JUDGEABLE_KINDS.iter().chain(RETIRED_KINDS.iter()) {
        if kind_direction(kind).is_none() {
            unanswered.push(*kind);
        }
    }
    assert!(
        unanswered.is_empty(),
        "these kinds are in the vocabulary but `kind_direction` does not answer for \
         them, so a declaration under one states no direction: {unanswered:?}"
    );
}

/// The direction column, tied to the constants that decide it. Direction is what
/// the demand points at, so the two present kinds and the four absent ones are
/// named by the code rather than restated.
#[test]
fn the_direction_sets_match_the_constants_that_decide_them() {
    assert_eq!(
        kinds_with_direction(KindDirection::Present),
        as_set([GREP_KIND, EXISTS_KIND]),
        "`present` is the expectation that a literal or a path is there -- exactly \
         the two kinds the judge answers with `judge_grep` and `judge_exists`"
    );

    let mut absent = as_set(ABSENCE_KINDS);
    absent.insert(GONE_KIND);
    assert_eq!(
        kinds_with_direction(KindDirection::Absent),
        absent,
        "`absent` is the expectation that something is gone; the three registry kinds \
         and `gone` are the ones answered by the absence predicate"
    );

    assert_eq!(
        kinds_with_direction(KindDirection::Ambiguous),
        as_set(RETIRED_KINDS),
        "a name that reads both ways cannot be a criterion, so the ambiguous kinds are \
         exactly the retired ones -- a new ambiguous name is a new retired name"
    );
}

/// Direction and state are not independent: a name the judge answers cannot be
/// one whose direction was never decided, and a retired name is one whose
/// direction was never decided by construction.
#[test]
fn no_answered_kind_is_ambiguous() {
    let bad: Vec<&str> = JUDGEABLE_KINDS
        .iter()
        .copied()
        .filter(|kind| kind_direction(kind) == Some(KindDirection::Ambiguous))
        .collect();
    assert!(
        bad.is_empty(),
        "these kinds are answered by the judge and ambiguous at once -- it can answer \
         them, yet the vocabulary says nobody can tell which way they point: {bad:?}"
    );
    let bad: Vec<&str> = RETIRED_KINDS
        .iter()
        .copied()
        .filter(|kind| kind_direction(kind) != Some(KindDirection::Ambiguous))
        .collect();
    assert!(
        bad.is_empty(),
        "these retired names point somewhere definite, so they could have been \
         criteria: {bad:?}"
    );
}
