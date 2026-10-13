//! Gate: the judge's kind vocabulary is one written-down list, and the code is
//! bound to it in both directions.
//!
//! The vocabulary used to live in two places at once, in two languages: this
//! crate's judge dispatch arms, and the private intake that mints declarations.
//! They agreed by inspection, and inspection is exactly what fails here -- a
//! kind the intake accepts but the judge has no predicate for is accepted,
//! stored, and then answered by the judge *before it ever reads the scope*. The
//! row reads as outstanding work for good, and nothing anywhere says so.
//!
//! So the list is a file, and this test is what keeps the code on it:
//!
//!   * file -> code: every judgeable row names a kind the judge publishes, and
//!     the retired rows are the ones the judge refuses.
//!   * code -> file: every kind the judge publishes appears as a judgeable row,
//!     and the names it refuses appear as retired rows. Together with the first
//!     direction that makes the two sets equal rather than merely overlapping,
//!     which matters: a file that listed only some of the judge's kinds would
//!     read as complete while the rest of the vocabulary lived in nobody's
//!     written-down place.
//!
//! The `direction` column is checked against the constants rather than against
//! a behaviour, because direction is a property of the demand and not of any one
//! look: `gone` and the three absence kinds are one predicate split by *what the
//! work behind them is*, not by how they are answered. Tying the two columns to
//! `GREP_KIND`/`EXISTS_KIND` and to `ABSENCE_KINDS`/`GONE_KIND` is what keeps the
//! column a reading rather than a note nobody checks.
//!
//! The third side -- the intake -- is not reachable from here: it lives in the
//! private repository that feeds the account. That gap is named on the file
//! itself rather than papered over.

use std::collections::BTreeSet;
use std::path::PathBuf;

use cog_orchestrator::change_measure::{
    ABSENCE_KINDS, EXISTS_KIND, GONE_KIND, GREP_KIND, JUDGEABLE_KINDS, RETIRED_KINDS,
};

/// The vocabulary's written-down form, relative to this crate.
const VOCABULARY_FILE: &str = "contracts/change-kinds.tsv";

/// Direction: the check demands the artifact be there.
const PRESENT: &str = "present";
/// Direction: the check demands it be gone.
const ABSENT: &str = "absent";
/// Direction: the name reads both ways, which is why it is not a criterion.
const AMBIGUOUS: &str = "ambiguous";

const DIRECTIONS: [&str; 3] = [PRESENT, ABSENT, AMBIGUOUS];

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    kind: String,
    direction: String,
    state: String,
}

fn vocabulary_text() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(VOCABULARY_FILE);
    std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "the kind vocabulary is missing at {}: {e}. It is the one written-down \
             place the judge's kinds are listed, and this gate reads it as the \
             contract -- without it the check below would pass by finding nothing",
            path.display()
        )
    })
}

/// The rows, with the `#` line comments and blank lines skipped.
fn rows() -> Vec<Row> {
    vocabulary_text()
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            assert_eq!(
                fields.len(),
                3,
                "a vocabulary row is `kind<TAB>direction<TAB>state`; this one has \
                 {} field(s): {line:?}",
                fields.len()
            );
            let (kind, direction, state) = (fields[0].trim(), fields[1].trim(), fields[2].trim());
            for (column, value) in [("kind", kind), ("direction", direction), ("state", state)] {
                assert!(!value.is_empty(), "{column} is empty on this row: {line:?}");
            }
            assert!(
                DIRECTIONS.contains(&direction),
                "`{direction}` is not a direction; the set is {DIRECTIONS:?}. A fourth \
                 one is a decision about what this vocabulary means, so it belongs in \
                 the code that reads the column, not in a row here"
            );
            assert!(
                matches!(state, "judgeable" | "retired"),
                "`{state}` is not a state; it is `judgeable` or `retired`"
            );
            Row {
                kind: kind.to_string(),
                direction: direction.to_string(),
                state: state.to_string(),
            }
        })
        .collect()
}

fn kinds_with_state(state: &str) -> BTreeSet<String> {
    rows()
        .into_iter()
        .filter(|r| r.state == state)
        .map(|r| r.kind)
        .collect()
}

fn kinds_with_direction(direction: &str) -> BTreeSet<String> {
    rows()
        .into_iter()
        .filter(|r| r.direction == direction)
        .map(|r| r.kind)
        .collect()
}

fn as_set(items: impl IntoIterator<Item = &'static str>) -> BTreeSet<String> {
    items.into_iter().map(str::to_string).collect()
}

/// A row repeated under two states, or a kind written twice, would let one of
/// them be edited while the other keeps the set looking whole.
#[test]
fn no_kind_is_listed_twice() {
    let all = rows();
    let unique: BTreeSet<&str> = all.iter().map(|r| r.kind.as_str()).collect();
    assert_eq!(
        all.len(),
        unique.len(),
        "a kind is listed more than once in {VOCABULARY_FILE}; the later row silently \
         wins in every reader that builds a map from it, and the two readers here \
         disagree about which"
    );
}

/// Anti-vacuity: the two checks below compare sets, and a file that parsed to
/// nothing would make both of them pass by comparing two empty sets.
#[test]
fn the_vocabulary_is_not_empty() {
    let all = rows();
    assert!(
        all.len() >= 8,
        "only {} row(s) parsed out of {VOCABULARY_FILE}; the separator, the comment \
         prefix, or the file itself moved and the checks below are now comparing \
         nothing",
        all.len()
    );
}

/// File -> code: the rows this file calls judgeable are the kinds the judge
/// publishes, and the retired rows are the names it refuses.
#[test]
fn every_row_names_a_kind_the_code_carries() {
    let judgeable = as_set(JUDGEABLE_KINDS);
    let retired = as_set(RETIRED_KINDS);
    let listed = kinds_with_state("judgeable");
    let listed_retired = kinds_with_state("retired");

    let unknown: Vec<&String> = listed.difference(&judgeable).collect();
    assert!(
        unknown.is_empty(),
        "these rows are filed as judgeable but no constant carries them, so a \
         declaration under one would be stored and never measured: {unknown:?}"
    );
    let unknown_retired: Vec<&String> = listed_retired.difference(&retired).collect();
    assert!(
        unknown_retired.is_empty(),
        "these rows are filed as retired but the judge does not refuse them, so the \
         file is claiming a name is dead while the code answers it: {unknown_retired:?}"
    );
}

/// Code -> file: every kind the judge publishes is on the list. Without this
/// direction the file could list one kind out of six and still read as the
/// vocabulary.
#[test]
fn every_published_kind_is_on_the_list() {
    let listed = kinds_with_state("judgeable");
    let missing: Vec<&str> = JUDGEABLE_KINDS
        .iter()
        .filter(|kind| !listed.contains(**kind))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "these kinds are judgeable in the code but absent from {VOCABULARY_FILE}, so \
         the written-down vocabulary and the judge have already drifted: {missing:?}"
    );

    let listed_retired = kinds_with_state("retired");
    let missing_retired: Vec<&str> = RETIRED_KINDS
        .iter()
        .filter(|kind| !listed_retired.contains(**kind))
        .copied()
        .collect();
    assert!(
        missing_retired.is_empty(),
        "these kinds are refused by the judge but absent from {VOCABULARY_FILE}: \
         {missing_retired:?} -- a row already in the account under one of them has to \
         stay nameable in order to be retired"
    );
}

/// The `direction` column, tied to the constants that decide it. Direction is
/// what the demand points at, so the two present kinds and the four absent ones
/// are named by the code and compared here rather than restated.
#[test]
fn the_direction_column_matches_the_constants_that_decide_it() {
    assert_eq!(
        kinds_with_direction(PRESENT),
        as_set([GREP_KIND, EXISTS_KIND]),
        "`present` is the expectation that a literal or a path is there -- exactly \
         the two kinds the judge answers with `judge_grep` and `judge_exists`"
    );

    let mut absent = as_set(ABSENCE_KINDS);
    absent.insert(GONE_KIND.to_string());
    assert_eq!(
        kinds_with_direction(ABSENT),
        absent,
        "`absent` is the expectation that something is gone; the three registry kinds \
         and `gone` are the ones answered by the absence predicate"
    );

    assert_eq!(
        kinds_with_direction(AMBIGUOUS),
        as_set(RETIRED_KINDS),
        "a name that reads both ways cannot be a criterion, so the ambiguous rows are \
         exactly the retired ones -- a new ambiguous name is a new retired name"
    );
}

/// Direction and state are not independent: a judgement that points nowhere is
/// not a criterion, and a retired name is one whose direction was never decided.
#[test]
fn no_judgeable_row_is_filed_as_ambiguous() {
    let bad: Vec<String> = rows()
        .into_iter()
        .filter(|r| r.state == "judgeable" && r.direction == AMBIGUOUS)
        .map(|r| r.kind)
        .collect();
    assert!(
        bad.is_empty(),
        "these rows are judgeable and ambiguous at once -- the judge can answer them, \
         yet the file says nobody can tell which way they point: {bad:?}"
    );
}
