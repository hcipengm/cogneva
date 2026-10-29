//! The verdict a census gives a series that no face carries.
//!
//! Two censuses ask this, on two different populations, and both need the same
//! three answers and the same lookup.
//!
//! The reader-face census walks `cog_core::metric_names::ALL` -- the closed set
//! the recording macros generate -- and asks whether a shipped rule or a panel
//! reads each name. The scrape census walks the names a build can *render*:
//! the ones producers hand to an exposition through an `Observable`, which the
//! store never holds and which therefore appear in the closed set not at all.
//! Different populations, and the same question about each member: something
//! carries it, or the reason it is carried by nothing is written down.
//!
//! The three verdicts mean the same thing on both sides. `Elsewhere` names the
//! series the same fact is read from, which makes leaving this name unread a
//! decision instead of an oversight. `NotAReading` says the series does not
//! carry a fact anything could act on. `Gap` records a fact that nothing states
//! and nothing reports: a debt, counted by the census's own `GAPS_AT_*` so the
//! next one cannot arrive unnoticed.
//!
//! The reader is a series name rather than prose on purpose. The reader-face
//! census filed two entries as `Elsewhere` with a reason saying a rule read one
//! of their operands, and that rule reads neither -- the two names occurred
//! nowhere in it. A reason is a sentence a gate can only count words in, so a
//! true exemption and a false one look the same; a series name is looked up,
//! and the lookup is what separates them.
//!
//! Included with `#[path]` rather than through `common/mod.rs`, like the other
//! helpers here: a test that wants this does not have to compile the harness
//! and mocks alongside it. Kept in one file rather than copied into each
//! census, because a second copy of the verdict type is a second answer to
//! "what does `Elsewhere` claim" -- the drift these contracts exist to
//! prevent, in the thing that does the preventing.

use std::collections::BTreeSet;

pub enum Unread {
    /// The same fact is read from these series, each by a face the census
    /// walks.
    Elsewhere {
        readers: &'static [&'static str],
        reason: &'static str,
    },
    /// No face can state it: the series carries no measurement -- a label, a
    /// timestamp, or a copy of something published where it is read.
    NotAReading(&'static str),
    /// Nothing states it. A debt, counted by the census's `GAPS_AT_*`.
    ///
    /// The scrape census stopped constructing this variant on 2026-10-29, when
    /// its last entry left: the startup phase timing got a panel. *That* unit
    /// is the one clippy calls this variant dead in, and clippy judges each
    /// compilation unit separately -- the sibling census sharing this file
    /// still files its own debts with it, and one file shared by two units is
    /// exactly what makes the same variant live in one and dead in the other.
    /// Keeping it is what makes the ratchet a ratchet: the next series
    /// published with no reader is recorded by writing one of these, and
    /// writing one is what pushes the pinned count above its constant. A
    /// census that had to reintroduce the variant to say "this one is owed"
    /// would instead have a comment saying so, which is the shape the debt
    /// took before the count existed.
    #[allow(dead_code)]
    Gap(&'static str),
}

/// Entries an `Elsewhere` reader claim cannot be resolved against.
///
/// A reader has to be a name some face reads -- the same closure the census
/// walks. That is the whole claim the entry makes, and it is the one the
/// reader-face census shipped two false entries under: a reason saying the
/// floor rule read one of the sample log's operands, where the rule reads
/// neither.
pub fn unresolved_readers(read: &BTreeSet<String>, name: &str, readers: &[&str]) -> Vec<String> {
    readers
        .iter()
        .filter(|reader| !read.contains(**reader))
        .map(|reader| format!("  {name} -> {reader}"))
        .collect()
}
