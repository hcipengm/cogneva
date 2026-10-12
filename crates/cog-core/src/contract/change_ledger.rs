//!Contract for the change ledger: what is still outstanding, and when it was
//!last measured against the tree.
//!
//!A change is identified by the artifact it names, byte for byte -- the same
//!string its check evaluates. That identity is the point of the table: two rows
//!that name different artifacts cannot merge, and one row whose check is
//!re-pointed at something else is a different change, not the same change with
//!a new state.
//!
//!The contract is deliberately lopsided. There is exactly one way to move a
//!row's state, and it takes a [`ChangeMeasurement`] -- the result of looking at
//!the tree. There is no call that sets a state or stamps `landed_at` on a
//!change's own behalf, because a ledger the judged party can write its own
//!completion into answers a different question than the one it is read for.
//!The residency requirement follows from the same place: the writer is a single
//!lease-elected loop, so there is one account rather than one per process.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// What is outstanding about one named artifact.
///
/// The three parts are stored and compared separately rather than as one
/// `kind:scope:literal` string. A literal is often a fragment of source code and
/// routinely contains colons, so the flattened form cannot be split back into
/// its parts -- it is a rendering for humans, not the key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChangeIdentity {
    /// The kind of check: `grep`, `absent`, `exists`, ...
    pub kind: String,
    /// What the check looks in: a path or a directory.
    pub scope: String,
    /// The literal the check matches, byte for byte.
    pub literal: String,
}

impl ChangeIdentity {
    pub fn new(
        kind: impl Into<String>,
        scope: impl Into<String>,
        literal: impl Into<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            scope: scope.into(),
            literal: literal.into(),
        }
    }

    /// The flattened `kind:scope:literal` form, for display and for matching the
    /// repository views.
    ///
    /// Not reversible: a literal containing a colon renders the same as a
    /// different split of the same characters. Anything that needs to key or
    /// look up a change uses the parts.
    pub fn as_key(&self) -> String {
        format!("{}:{}:{}", self.kind, self.scope, self.literal)
    }
}

impl std::fmt::Display for ChangeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_key())
    }
}

/// Where a change is, as of the last time something looked.
///
/// The set is closed: a state outside it is a caller error rather than a new
/// category, and the storage layer refuses to store one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeState {
    /// The artifact and the documents disagree about it.
    Conflict,
    /// Wanted, not there yet.
    Pending,
    /// Partly there: some of what the change asks for exists.
    Partial,
    /// Nothing has looked, or what it found does not settle the question.
    Undecided,
    /// A measurement found the artifact as the change asks for it.
    Landed,
}

impl ChangeState {
    /// Every state, in the order the repository views group them.
    pub const ALL: [ChangeState; 5] = [
        ChangeState::Conflict,
        ChangeState::Pending,
        ChangeState::Partial,
        ChangeState::Undecided,
        ChangeState::Landed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeState::Conflict => "conflict",
            ChangeState::Pending => "pending",
            ChangeState::Partial => "partial",
            ChangeState::Undecided => "undecided",
            ChangeState::Landed => "landed",
        }
    }
}

impl std::str::FromStr for ChangeState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|state| state.as_str() == s)
            .ok_or_else(|| format!("unknown change state: {s}"))
    }
}

/// One document that asks for a change, and what that document said about it.
///
/// Kept beside the row rather than folded into its state: what a document says
/// is a claim about the document, and the state is what a measurement of the
/// tree found. Collapsing the two loses the disagreement between them, which is
/// the reading the ledger exists to produce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSource {
    pub doc: String,
    /// The document's own verdict, when it gave one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verdict: Option<String>,
}

/// One row of the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeLedgerRow {
    pub identity: ChangeIdentity,
    pub state: ChangeState,
    /// What the change asks for, in one line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The change itself: what to do.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assertion: Option<String>,
    pub sources: Vec<ChangeSource>,
    /// When the row entered the account.
    pub first_seen_at: DateTime<Utc>,
    /// When it was last measured. `None` while nothing has measured it: a row
    /// that nothing has looked at is not the same as one measured a while ago,
    /// and a reader that cannot tell them apart reports the second as current.
    pub measured_at: Option<DateTime<Utc>>,
    /// When a measurement last found the change satisfied. Set and cleared by
    /// measurement, so `state == Landed` and this being present agree by
    /// construction.
    pub landed_at: Option<DateTime<Utc>>,
}

/// The result of looking at the tree for one change.
///
/// Carries no timestamp: the store stamps both times from its own clock as it
/// writes, so the moment a row is recorded as landed is the moment the
/// measurement was applied rather than a time the caller chose. A caller-set
/// time can be backdated or reused, and a ledger whose completion times are
/// chosen by the side being judged has no independent reading in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeMeasurement {
    pub identity: ChangeIdentity,
    pub state: ChangeState,
    /// What the measurement saw, kept with the row so a state that disagrees
    /// with the tree can be argued with rather than merely disbelieved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation: Option<String>,
}

impl ChangeMeasurement {
    pub fn new(identity: ChangeIdentity, state: ChangeState) -> Self {
        Self {
            identity,
            state,
            observation: None,
        }
    }
}

/// Durable storage for the change ledger.
///
/// The only way to move a row's state is [`Self::apply_measurement`]. Rows enter
/// through the reading pipeline (a document read into the account), which is a
/// separate surface: a reading records what a document asks for, a measurement
/// records what the tree shows, and only the second one decides whether a change
/// is done.
#[async_trait]
pub trait ChangeLedgerStore: Send + Sync {
    /// Add or refresh the rows a document reading contributes: the identity, the
    /// text of the change, and the source it came from.
    ///
    /// Never writes state, `measured_at` or `landed_at` -- a reading is not a
    /// measurement, and a change cannot become landed by being described as
    /// done. Rows the reading does not mention are left alone: the ledger
    /// outlives any one reading of the corpus, and a reading that omitted a row
    /// because its document was dropped from the corpus is not a statement that
    /// the change is gone.
    async fn record_readings(&self, readings: &[ChangeReading]) -> SFResult<usize>;

    /// Apply one measurement: move the row's state, stamp `measured_at`, and
    /// stamp or clear `landed_at` to match.
    ///
    /// A measurement of an identity the ledger does not have yet creates the
    /// row, because the account is defined by what has been measured rather than
    /// by what has been read about.
    async fn apply_measurement(&self, measurement: &ChangeMeasurement) -> SFResult<()>;

    /// The rows that are not `landed`, stalest measurement first.
    ///
    /// `None` in `measured_at` sorts first: nothing has looked at those, which
    /// is less known than a stale measurement, not more current.
    async fn outstanding(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>>;

    /// One row, by the identity that names it.
    async fn get(&self, identity: &ChangeIdentity) -> SFResult<Option<ChangeLedgerRow>>;

    /// How many rows the ledger holds, by state.
    ///
    /// Answers with every state present, including the zeroes: a reader
    /// comparing two censuses needs to see a state that emptied, which is
    /// exactly the reading an absent key would hide.
    async fn census(&self) -> SFResult<Vec<(ChangeState, u64)>>;
}

/// What one document reading contributes to the account.
///
/// A reading names a change and the text of it; it does not carry a state. The
/// kinds are those a check can take, so the identity of a row is the same
/// string the measurement will later evaluate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeReading {
    pub identity: ChangeIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assertion: Option<String>,
    pub source: ChangeSource,
}
