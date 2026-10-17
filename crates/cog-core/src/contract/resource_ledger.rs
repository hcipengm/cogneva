//!Contract for the resource ledger's event half: the moments a bound moved and
//!the asks a gate turned away.
//!
//!The resource account is split by what a cell holds, and the split is the whole
//!design. Values live where they are produced -- what we asked for is a line in
//!the chart, what the kernel runs with is a drop-in on the host, what the cluster
//!admits is a projection of the first -- so a table of current values would be a
//!second definition of a fact that already has a home. Events are the other
//!half: a bound the control law moved at runtime left no commit behind, and a
//!refusal exists only as a fact with an identity and an instant.
//!
//![`ResourceEvent`] therefore never carries "the current bound". It carries what
//!happened, who decided it, which reading the decision was made on, which bound
//!moved from what to what, and how it turned out. Any single row answers those
//!questions on its own, which is the only test this table has to pass: a reader
//!that had to look at another row to know what a row means would be reading a
//!projection, not an account.
//!
//!Identity is content. The store's primary key is the tuple of kind, instant,
//!subject and actor, and a repeated report of the same event collapses onto one
//!row with a larger [`ResourceEvent::count`]. Reporters retry; a reporter that
//!retries after a restart must not be able to write an event twice, and no
//!consumer should have to remember a cursor to make that true.
//!
//!Two shapes are deliberately absent. There is no state machine: these rows are
//!append-only and the only column a consumer may move is the fold marker. And
//!there is no "unmeasured" variant of [`ResourceEventKind`] -- an event that did
//!not happen has no row, and a reader that wants to know whether anything was
//!watched reads the pending count and the loop's own liveness, not a sentinel
//!event.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// The two things this account records, and no third.
///
/// They are not two views of one fact: a bound change is a decision the platform
/// made about the host, and an admission refusal is a decision the platform made
/// about a piece of work. A reader that summed them would answer "something
/// happened", which is exactly the reading that cannot be acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceEventKind {
    /// The enforced bound moved. `subject` names the bound, `from_value` and
    /// `to_value` are the two ends.
    BoundChange,
    /// A piece of work asked for a slot and was turned away. `subject` names
    /// the asker alongside the wall that refused it.
    AdmissionRefusal,
}

impl ResourceEventKind {
    /// The word this kind is written and read as, in the table and in a label.
    /// One spelling: a reading that labelled the same event two ways would
    /// split its own series.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BoundChange => "bound_change",
            Self::AdmissionRefusal => "admission_refusal",
        }
    }

    /// Every kind, for a reader that must publish a fixed series set rather than
    /// only the kinds that happen to have occurred.
    pub const ALL: [Self; 2] = [Self::BoundChange, Self::AdmissionRefusal];

    /// Reads back what [`Self::as_str`] writes, answering `None` for a word this
    /// build does not know. Unknown must stay representable: a row written by a
    /// newer build is not a row to drop, and it is not one to guess at either.
    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_str() == word)
    }
}

/// How an event ended.
///
/// `Deferred` exists because the control law has one: a bound that may not rise
/// on a busy host is not a bound that refused to change, and writing it down as
/// `Applied` would claim a move that did not happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceEventOutcome {
    /// The decision took effect.
    Applied,
    /// The ask was turned away.
    Refused,
    /// The decision was made and deliberately not carried out yet.
    Deferred,
}

impl ResourceEventOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Refused => "refused",
            Self::Deferred => "deferred",
        }
    }

    pub const ALL: [Self; 3] = [Self::Applied, Self::Refused, Self::Deferred];

    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.as_str() == word)
    }
}

/// One event, as it is written and as it is read back.
///
/// `occurred_at` is when the event happened, not when a reporter got round to
/// it: the account's order is the order things happened in, and a reporter that
/// was delayed must not be able to move an event later in that order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceEvent {
    pub kind: ResourceEventKind,
    pub occurred_at: DateTime<Utc>,
    /// Which bound moved, or which asker and wall. Identity, not a label: the
    /// same work refused by two walls is two events with the wall in the
    /// subject, not one event counted twice.
    pub subject: String,
    /// The process that made the decision -- a pod name, or the controller on
    /// this host. The reading that matters for attribution.
    pub actor: String,
    /// The name of the reading the decision was made on. Every bound in this
    /// account is supposed to be derived from something; this is where that
    /// something is named, so "derived from what" is a column and not folklore.
    pub reading: String,
    pub reading_value: Option<f64>,
    pub from_value: Option<i64>,
    pub to_value: Option<i64>,
    pub outcome: ResourceEventOutcome,
    /// Whatever else the producer knew and a reader would otherwise have to
    /// reconstruct from a log line that has rotated away.
    pub detail: serde_json::Value,
    /// How many times the same event was reported. One for an event reported
    /// once; a retried report adds to this rather than writing a second row.
    pub count: i64,
}

impl ResourceEvent {
    /// What the store compares rows on. Stated once, here, so that the table's
    /// primary key and anything that dedupes in memory before writing cannot
    /// disagree about what "the same event" means.
    pub fn identity(&self) -> (&'static str, DateTime<Utc>, &str, &str) {
        (
            self.kind.as_str(),
            self.occurred_at,
            self.subject.as_str(),
            self.actor.as_str(),
        )
    }
}

/// A row as it is read back, with the two things the table adds to an event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceEventRow {
    pub event: ResourceEvent,
    pub reported_at: DateTime<Utc>,
    /// When a consumer last folded this row. `None` means no consumer has read
    /// it yet -- which is what makes "nobody is consuming this account" a
    /// question the table itself can answer.
    pub folded_at: Option<DateTime<Utc>>,
}

/// Durable storage for the resource account's events.
///
/// Append-only, and the only column that moves is the fold marker. There is no
/// call that sets a bound here: the account records what happened to a bound,
/// never what the bound is.
#[async_trait]
pub trait ResourceEventStore: Send + Sync {
    /// Append events, collapsing a repeated report of the same event onto its
    /// existing row (the count grows) rather than writing a second one.
    ///
    /// Returns the number of rows that were new. A store that cannot tell the
    /// two apart would make a retried report invisible, which is the fault this
    /// identity exists to prevent.
    async fn append_events(&self, events: &[ResourceEvent]) -> SFResult<usize>;

    /// Take up to `limit` unfolded events and mark them folded, in one step, so
    /// that two consumers cannot both take the same row and neither can leave a
    /// row marked as read that it never saw.
    async fn fold_events(&self, limit: usize) -> SFResult<Vec<ResourceEventRow>>;

    /// How many events have been written and not yet folded.
    async fn pending_events(&self) -> SFResult<u64>;

    /// When the oldest unfolded event happened, if any are unfolded.
    ///
    /// The count alone cannot tell "events are arriving" from "nothing has
    /// consumed this account for a day"; the age can, and it is the only
    /// reading that says which of the two a pending count is.
    async fn oldest_unfolded_at(&self) -> SFResult<Option<DateTime<Utc>>>;

    /// How many events of each `(kind, outcome)` the account holds, folded or
    /// not. Fixed over the closed sets rather than over what happens to have
    /// occurred: a series that appears only after its first event cannot be
    /// used to say "none yet".
    async fn census(&self) -> SFResult<Vec<(ResourceEventKind, ResourceEventOutcome, u64)>>;

    /// The most recent `limit` events, newest first, for a reader that has to
    /// look at what actually happened rather than at totals.
    async fn recent(&self, limit: usize) -> SFResult<Vec<ResourceEventRow>>;
}
