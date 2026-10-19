//!Where the meta-learning engine's evidence lives when the process does not.
//!
//!The engine accumulates one counter pair per decision, grouped by what the
//!decision was applied to. Those counters were kept in a file the engine
//!rewrote whole on every record, and this module is the port that replaces it:
//!the engine hands over one increment, the store applies it, and the engine
//!reads the whole table back when it starts.
//!
//!The record is deliberately plain -- three strings and two numbers. What a
//!group is and what a decision means belongs to the engine and stays there; a
//!store that knew about decision categories would be a second place that has to
//!be changed when the engine grows one, and the two would drift.
//!
//!Increments rather than totals, and that is the whole point of moving: a
//!caller that wrote totals would have to read the current value first, and the
//!gap between the read and the write is where a second writer's increment goes
//!missing. An increment applied by the database is the only shape that stays
//!correct when two processes count into the same cell.

use async_trait::async_trait;

use crate::SFResult;

/// One cell of the evidence: how often a decision was taken for a group, and
/// how often that was a success.
///
/// The same type carries both directions, and the difference is in the verb:
/// [`DecisionStatsStore::accumulate`] adds these numbers to the cell, while
/// [`DecisionStatsStore::load_all`] returns the cell's totals. A separate pair
/// of types would not make that distinction any safer -- the numbers have the
/// same shape either way -- and would be two things to keep in step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionStatCell {
    pub category: String,
    pub group_key: String,
    pub decision: String,
    /// How many times the decision was taken. Always at least one in an
    /// accumulation: a call that adds nothing is not the record of a decision.
    pub attempts: i64,
    /// How many of those attempts succeeded. Never more than `attempts`.
    pub successes: i64,
}

/// The ports a decision-stats store has to serve.
#[async_trait]
pub trait DecisionStatsStore: Send + Sync {
    /// Add one decision's outcome to its cell, creating the cell if this is the
    /// first decision in its group.
    ///
    /// The addition happens inside the store's own statement. A store that read
    /// the cell and wrote the sum back would be exactly the file this replaces,
    /// with a smaller window.
    async fn accumulate(&self, delta: &DecisionStatCell) -> SFResult<()>;

    /// Raise a cell to at least these totals, and never lower it.
    ///
    /// This is how evidence that was recorded before the table existed -- or
    /// during a moment the table was unreachable -- reaches it. The difference
    /// from [`Self::accumulate`] is what makes the call safe to repeat: a total
    /// that is already at or below the stored one changes nothing, so a process
    /// that starts twice, or two processes that start at once, cannot count the
    /// same evidence twice. Assignment would be the third wrong answer here:
    /// writing these totals over a larger stored count would erase decisions
    /// another process recorded.
    async fn hold_at_least(&self, totals: &DecisionStatCell) -> SFResult<()>;

    /// Every cell the store holds, for a process hydrating its counters at
    /// start.
    ///
    /// An empty answer is an ordinary one -- a fresh deployment has recorded no
    /// decisions yet -- and is not distinguished from "the table is not
    /// reachable": that is an error, and a reader that folded the two together
    /// would start from zero without saying so.
    async fn load_all(&self) -> SFResult<Vec<DecisionStatCell>>;
}
