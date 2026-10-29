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
    /// A look found the identity itself is one the account should not be
    /// holding, so there is nothing left to ask of the code tree. A second
    /// terminal state beside [`ChangeState::Landed`], and like it a property of
    /// the `(identity, criterion revision)` pair rather than of the artifact: a
    /// later look at a different corpus can move the row back out, which is why
    /// this is a state and not a deletion.
    Retired,
}

impl ChangeState {
    /// Every state, in the order the repository views group them.
    ///
    /// The two terminal states come last, side by side, because that is how a
    /// reader asks about them: `landed` and `retired` are the two ways a row
    /// stops being outstanding, and the four before them are the ways it is not.
    pub const ALL: [ChangeState; 6] = [
        ChangeState::Conflict,
        ChangeState::Pending,
        ChangeState::Partial,
        ChangeState::Undecided,
        ChangeState::Landed,
        ChangeState::Retired,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeState::Conflict => "conflict",
            ChangeState::Pending => "pending",
            ChangeState::Partial => "partial",
            ChangeState::Undecided => "undecided",
            ChangeState::Landed => "landed",
            ChangeState::Retired => "retired",
        }
    }

    /// Whether a row in this state is still outstanding work.
    ///
    /// Both terminal states read false, and both for the same reason: a row
    /// whose question has been answered -- by the artifact being there, or by the
    /// identity turning out not to belong -- is not work anybody still owes. A
    /// reader that asked "is it landed" to answer this would count every retired
    /// row as outstanding.
    pub fn is_outstanding(self) -> bool {
        !matches!(self, ChangeState::Landed | ChangeState::Retired)
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

/// Why a change was retired: the closed set of answers to "why does the account
/// not hold this identity".
///
/// A state that could not say why would be indistinguishable from a deletion --
/// the row would be gone from the outstanding set with nothing to argue with, and
/// the same three rows coming back would look like three new changes. Keeping the
/// reason closed is what lets a reader ask "how much of the account was retired
/// because it named a fixture" and get an answer that a free-text note could not
/// give.
///
/// The set is closed for the same reason [`ChangeState`]'s is: a value outside it
/// is a caller error rather than a new category, and the storage refuses to store
/// one. Adding a reason is a variant here, a value in the column's closed set, and
/// a migration -- which is what keeps the set closed rather than merely written
/// down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetireReason {
    /// A later declaration names the same change under a different identity --
    /// a rename, or a check re-pointed at the artifact it was always about. The
    /// change still exists; this row's identity is the one left behind, and
    /// retiring it is the merge that the two identities would otherwise never
    /// get, since the ledger keys them apart on purpose.
    Superseded,
    /// The identity names a test fixture rather than a change to the artifact.
    Fixture,
    /// The scope is outside the tree a criterion can read, so the row's check
    /// could only ever come back unsettled and no measurement will ever move it.
    OutOfScope,
    /// The artifact left the tree the judge reads, by a decision taken outside
    /// it: the work was real and was measured here, and the surface it was
    /// measured on is now kept somewhere this account cannot read.
    ///
    /// Distinct from [`RetireReason::OutOfScope`], which names a scope this
    /// account was never able to read. A reader asking whether the row can come
    /// back -- the one question the two answers differ on -- needs the
    /// difference: an `out_of_scope` row returns as soon as a tree is mounted
    /// for it, while this one returns only if the surface itself comes back
    /// into the measured tree.
    SurfaceMoved,
    /// Another account has answered this row's question, on a surface this
    /// account's checks cannot search.
    ///
    /// The row was minted rightly: at the time, nothing enforced the rule, and
    /// the platform's own account said so. A gate was then written, and it is a
    /// test in a tree this account does not measure -- the host keeps its own
    /// carriers and reports them as a column of its own account, which is the
    /// column this reason is produced from. Its identity here is a search of one
    /// scope of one tree, so the answer can never arrive as a measurement, and
    /// the row would stay outstanding while reading exactly like a rule nobody
    /// has written a check for.
    ///
    /// Distinct from [`RetireReason::OutOfScope`], whose row names a scope this
    /// account cannot read: this row's scope is read on every pass. Distinct from
    /// [`RetireReason::SurfaceMoved`], whose row was measured here before its
    /// surface left: this one was never measurable here at all. The question the
    /// three answers differ on is whether the row can come back, and for this one
    /// it can: the other account dropping the gate is what brings it back, and
    /// that is a fact readable from that account on any pass.
    AnsweredElsewhere,
}

impl RetireReason {
    /// Every reason, in the order the readings group them.
    pub const ALL: [RetireReason; 5] = [
        RetireReason::Superseded,
        RetireReason::Fixture,
        RetireReason::OutOfScope,
        RetireReason::SurfaceMoved,
        RetireReason::AnsweredElsewhere,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RetireReason::Superseded => "superseded",
            RetireReason::Fixture => "fixture",
            RetireReason::OutOfScope => "out_of_scope",
            RetireReason::SurfaceMoved => "surface_moved",
            RetireReason::AnsweredElsewhere => "answered_elsewhere",
        }
    }
}

impl std::str::FromStr for RetireReason {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|reason| reason.as_str() == s)
            .ok_or_else(|| format!("unknown retire reason: {s}"))
    }
}

/// Why an outstanding change is not being worked on.
///
/// A state answers whether the tree satisfies the demand; it cannot answer "what
/// is this row waiting for". The two are orthogonal -- a row can be outstanding
/// and blocked at the same time -- so the wait is a flag beside the state, and
/// this is the closed set of answers it can carry.
///
/// Closed for a reason a free-text reason could not give: the column is grouped
/// and counted, and a reason per row would produce as many categories as there
/// are rows. The table refuses a value outside the set, which is what makes the
/// set real rather than merely written down here.
///
/// Every word below is also spelled by queries -- the stamp that blocks a row
/// on a pool, the clear that takes that block back, the reads that count what is
/// waiting. A query that spells one of them itself agrees with this enum only
/// until someone renames a variant, and then it matches nothing and answers
/// exactly like a table with no blocked rows: "nothing is waiting on a pool"
/// while the rows sit there. `as_str` is the only place these words are decided;
/// the queries fill them from `ALL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedReason {
    /// No configured upstream can serve: the pool is down, and nothing about
    /// this row is what holds it.
    PoolUnavailable,
    /// Waiting on a reading only a run can produce -- a measurement that has
    /// not come back yet, not one this pass could take itself.
    UpstreamReading,
    /// Waiting on a decision this platform does not make.
    OwnerDecision,
    /// Waiting on an input from outside the account.
    ExternalInput,
    /// Waiting on another change to land first.
    Dependency,
}

impl BlockedReason {
    /// Every reason, in the order the readings group them.
    pub const ALL: [BlockedReason; 5] = [
        BlockedReason::PoolUnavailable,
        BlockedReason::UpstreamReading,
        BlockedReason::OwnerDecision,
        BlockedReason::ExternalInput,
        BlockedReason::Dependency,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            BlockedReason::PoolUnavailable => "pool_unavailable",
            BlockedReason::UpstreamReading => "upstream_reading",
            BlockedReason::OwnerDecision => "owner_decision",
            BlockedReason::ExternalInput => "external_input",
            BlockedReason::Dependency => "dependency",
        }
    }
}

impl std::str::FromStr for BlockedReason {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|reason| reason.as_str() == s)
            .ok_or_else(|| format!("unknown blocked reason: {s}"))
    }
}

/// How urgent the document that asked for a change said that change was.
///
/// Only a document can answer this. Nothing measures urgency: there is no tree
/// that says one change matters more than another, so a band computed from
/// whatever happens to be stuck at the moment would be a stored answer to a
/// question whose input keeps moving -- written once, never revisited, and read
/// afterwards as if it still held. The band therefore rides on the source, and
/// a row's band is the highest one any document still asking for it stated.
///
/// `Unstated` is a member of the set rather than an absence from it. A band
/// that fell back to something when no document said would put every
/// unprioritised change in one band, and a reader could no longer tell "the
/// document said P0" from "no document said" -- which is the reading the column
/// exists for. It ranks below every band, so the maximum over a row's sources
/// is the band to act on and a row nobody prioritised stays unstated. That is
/// why the order is written out rather than derived: `P0` is the highest, and a
/// derived `Ord` would take the position of the variant in this list for its
/// rank and answer `P3`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ChangePriority {
    /// No document that asks for this change said how urgent it was.
    #[default]
    Unstated,
    P0,
    P1,
    P2,
    P3,
}

impl ChangePriority {
    /// Every band: `unstated` first, then `P0` through `P3` by name.
    ///
    /// Not by rank -- `P0` outranks `P3`, so a rank-ordered list would read
    /// `unstated, p3, p2, p1, p0`. This is the order a reading prints, the same
    /// one the projection's census uses, so two readings of the same account are
    /// compared line by line.
    pub const ALL: [ChangePriority; 5] = [
        ChangePriority::Unstated,
        ChangePriority::P0,
        ChangePriority::P1,
        ChangePriority::P2,
        ChangePriority::P3,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangePriority::Unstated => "unstated",
            ChangePriority::P0 => "p0",
            ChangePriority::P1 => "p1",
            ChangePriority::P2 => "p2",
            ChangePriority::P3 => "p3",
        }
    }

    /// Rank, with `P0` highest and `Unstated` below every band.
    ///
    /// Public because it is the number a store's own ordering column holds: the
    /// ledger keeps the rank as a generated column so a bounded read can be
    /// ordered before it is cut, and a count taken per rank has to be read back
    /// as a band.
    ///
    /// The same five numbers are spelled in the migration that generates that
    /// column, because a generated expression cannot call into Rust. That copy
    /// cannot promote work nobody prioritised: a band the SQL does not know
    /// ranks zero, the same answer as `unstated`, so a drift between the two
    /// hides urgency rather than inventing it -- and the live counts are what
    /// make it visible if it ever happens.
    pub const fn rank(self) -> u8 {
        match self {
            ChangePriority::P0 => 4,
            ChangePriority::P1 => 3,
            ChangePriority::P2 => 2,
            ChangePriority::P3 => 1,
            ChangePriority::Unstated => 0,
        }
    }
}

impl Ord for ChangePriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl PartialOrd for ChangePriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::str::FromStr for ChangePriority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|band| band.as_str() == s)
            .ok_or_else(|| format!("unknown change priority: {s}"))
    }
}

/// Which face of the programme a change belongs to, when the document asking
/// for it named one.
///
/// The other half of the ordering key. A band says how soon within a face; the
/// block says which face, and without it "how much of this face is done" is a
/// substring search over provenance text rather than a count. It rides on the
/// source for the same reason the band does: a grouping is something a document
/// states about what it is asking for, not something a tree can measure.
///
/// A name rather than a variant list. The faces are declared in the roster and
/// the account keeps what the documents name; a closed set here would put the
/// list of faces in this type, in the SQL fold and in two CHECK constraints, and
/// every declared face would then have to be added to all of them. Those copies
/// drift in a way that does not fail loudly -- a name the fold does not know is
/// filed as `none`, which is the same value the account uses for "nobody named
/// a face", so the new face's work reads as work nobody asked for. The set the
/// census prints is measured from the rows instead, so a face appears when work
/// is filed under it and needs no code change to be born.
///
/// It is still a shape, not free text: lowercase ASCII, digits and underscores,
/// starting with a letter. The value travels through SQL, JSONB and metric
/// labels, and a value that needs quoting differs among the three.
///
/// `none` is a member rather than an absence, and so is the case where the
/// documents disagree. The account cannot pick between two faces a set of
/// documents names, and guessing one would silently file work under a heading
/// nobody chose; the count of rows in that state is published beside the
/// per-face census, so the ambiguity is readable instead of hidden.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ChangeBlock(std::borrow::Cow<'static, str>);

impl Default for ChangeBlock {
    fn default() -> Self {
        Self::NONE
    }
}

impl ChangeBlock {
    /// No document that asks for this change named a face, or its documents
    /// named different ones.
    pub const NONE: ChangeBlock = ChangeBlock(std::borrow::Cow::Borrowed("none"));

    /// The literal stored in the account, and the one a reading prints.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this names a face at all. `none` is the answer to "no document
    /// said" and to "the documents disagree", so it cannot be counted as one.
    pub fn is_named(&self) -> bool {
        self.as_str() != Self::NONE.as_str()
    }

    /// The shape, written once as text so that the copies of this rule living
    /// where this crate cannot reach them -- the SQL function, the intake and
    /// the projection -- can be compared against it instead of against a
    /// transcription. [`Self::named`] below checks the same shape byte by byte
    /// rather than by running a regular expression, so the two have to stay in
    /// step: a test pins both against the same sample names, and the SQL on the
    /// other side of a database is compared against this string.
    pub const SHAPE: &str = "^[a-z][a-z0-9_]{0,63}$";

    /// A face name, if it has the shape a column value and a label value can
    /// carry. The empty string, `-` and `none` all mean "no face".
    pub fn named(name: &str) -> Result<Self, String> {
        let trimmed = name.trim();
        if trimmed.is_empty() || trimmed == "-" || trimmed == Self::NONE.as_str() {
            return Ok(Self::NONE);
        }
        let ok = trimmed.len() <= 64
            && trimmed.as_bytes()[0].is_ascii_lowercase()
            && trimmed
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !ok {
            return Err(format!(
                "a change block is a lowercase ascii name (a-z, 0-9, _), got {name:?}"
            ));
        }
        Ok(ChangeBlock(std::borrow::Cow::Owned(trimmed.to_string())))
    }
}

impl std::str::FromStr for ChangeBlock {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::named(s)
    }
}

impl TryFrom<String> for ChangeBlock {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::named(&value)
    }
}

impl From<ChangeBlock> for String {
    fn from(block: ChangeBlock) -> String {
        block.0.into_owned()
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
    /// The band this document asked for, or [`ChangePriority::Unstated`].
    ///
    /// Defaulted rather than required so a source written before the column
    /// existed reads back as unstated instead of failing to parse: those rows
    /// were never asked the question, and refusing to read them would lose an
    /// answer the account already has. Written out rather than skipped when
    /// unstated, so the two renderings of a source -- this one and the
    /// `jsonb_build_object` the store builds -- carry the same keys.
    #[serde(default)]
    pub priority: ChangePriority,
    /// The face this document files the change under, or [`ChangeBlock::NONE`].
    ///
    /// Defaulted for the same reason the band is: a source written before the
    /// column existed was never asked, and refusing to read it would lose the
    /// answer. Written out rather than skipped so this rendering and the store's
    /// `jsonb_build_object` carry the same keys.
    #[serde(default)]
    pub block: ChangeBlock,
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
    /// The revision that measurement read, when it named one. `None` wherever
    /// `measured_at` is `None`, and also for a measurement taken by a process
    /// with no revision to name.
    pub measured_rev: Option<String>,
    /// The revision that delivered this change: the commit whose
    /// [`LANDING_TRAILER`] names the task this row was handed over under.
    ///
    /// A different fact from `landed_at`, and kept apart on purpose. `landed` is
    /// an answer about the tree -- the row's check measured true -- while this is
    /// an answer about delivery: a diff reached the line. A change can be
    /// delivered and never measure landed (the commit went in and the property
    /// it was meant to establish did not), and a row can measure landed without
    /// being delivered (the tree already satisfied it). Neither is evidence for
    /// the other, so neither may be written by the other's reader.
    ///
    /// Set once, by the reader that finds the trailer, and never cleared: a
    /// change that was delivered was delivered, even if a later revision took
    /// the diff back out. That the tree no longer carries it is what `state` and
    /// the reopen record are for.
    pub delivered_rev: Option<String>,
    /// When a measurement last found the change satisfied. Set and cleared by
    /// measurement, so `state == Landed` and this being present agree by
    /// construction.
    pub landed_at: Option<DateTime<Utc>>,
    /// Why the row is `undecided`, when it is. `None` in every other state.
    ///
    /// A row is `undecided` only for a reason that is a property of the row
    /// itself rather than of the look, so this is the reason the row will never
    /// settle by retrying. Kept as a name from the judge's closed set, as text:
    /// the vocabulary belongs to the judge, and threading the judge's type
    /// through here would make the storage contract depend on the judge's module.
    /// A value outside the set is refused by the column's own constraint.
    pub unsettled_reason: Option<String>,
    /// Why the change was retired, when it was. `None` in every other state:
    /// the reason is part of the retirement rather than a field every row
    /// carries, and a reason left beside a row that is not retired would read as
    /// one. Set and cleared with [`ChangeLedgerRow::retired_at`].
    pub retire_reason: Option<RetireReason>,
    /// When a measurement last found the change retired. Set and cleared by
    /// measurement, so `state == Retired` and this being present agree by
    /// construction, the same way `landed_at` agrees with `Landed`.
    pub retired_at: Option<DateTime<Utc>>,
}

impl ChangeLedgerRow {
    /// The band to act on: the highest any document still asking for this
    /// change stated, or [`ChangePriority::Unstated`] when none of them said.
    ///
    /// Derived from the sources rather than handed in by a caller. A band a
    /// writer supplies would be a second answer to a question the sources
    /// already answer, and the two would drift the first time a declaration was
    /// corrected -- the reading path updates the source, and nothing would
    /// update the copy.
    ///
    /// The ledger table derives the same value from the same citations, as a
    /// generated column, for one reason reading cannot serve: the bounded reads
    /// cut a slice before the caller holds the rows, so they have to be ordered
    /// by the band. Being generated, that copy is recomputed from the citations
    /// by the database on every write that touches them, so it cannot disagree
    /// with this method the way a maintained column could.
    pub fn priority(&self) -> ChangePriority {
        self.sources
            .iter()
            .map(|source| source.priority)
            .max()
            .unwrap_or_default()
    }

    /// The face to file this change under: the one its documents name, or
    /// [`ChangeBlock::NONE`] when none of them name one or they disagree.
    ///
    /// Derived from the sources for the same reason the band is: a copy a
    /// writer has to keep in step with the citations goes stale the first time
    /// a document is corrected. Disagreement is folded to `none` here and
    /// counted by [`Self::block_conflict`] rather than decided -- picking one
    /// document's face over another's would file the row under a heading half
    /// the people asking for it never chose.
    pub fn block(&self) -> ChangeBlock {
        let mut named = self
            .sources
            .iter()
            .map(|source| &source.block)
            .filter(|block| block.is_named());
        match (named.next(), named.next()) {
            (Some(only), None) => only.clone(),
            _ => ChangeBlock::NONE,
        }
    }

    /// Whether the documents asking for this change name more than one face.
    ///
    /// Separated from [`Self::block`] because both answers fold to `none`, and a
    /// reader that could not tell "nobody named a face" from "two documents
    /// named different faces" would read a disagreement as silence.
    pub fn block_conflict(&self) -> bool {
        let mut distinct = self
            .sources
            .iter()
            .map(|source| source.block.as_str())
            .filter(|name| *name != ChangeBlock::NONE.as_str())
            .collect::<Vec<_>>();
        distinct.sort_unstable();
        distinct.dedup();
        distinct.len() > 1
    }

    /// Whether this change is standing in front of everyone else.
    ///
    /// A commit delivered it and the account still owes it: the line is
    /// carrying a change whose claim is not true of the tree. Everything
    /// measured on that tree stands on it, which is the one blocking relation
    /// the account can measure about itself -- the holder is named by the
    /// delivery, and the held are the rows measured at that same revision.
    ///
    /// Deliberately a method over the row's own facts rather than a stored
    /// band: the moment the claim becomes true the state moves and this answers
    /// false, with nothing left behind to go stale.
    pub fn holds_the_line(&self) -> bool {
        self.delivered_rev.is_some() && self.state.is_outstanding()
    }
}

/// The result of looking at the tree for one change.
///
/// Carries no timestamp: the store stamps both times from its own clock as it
/// writes, so the moment a row is recorded as landed is the moment the
/// measurement was applied rather than a time the caller chose. A caller-set
/// time can be backdated or reused, and a ledger whose completion times are
/// chosen by the side being judged has no independent reading in it.
///
/// The revision is the caller's, by contrast, and has to be: it is not a time but
/// the thing that was looked at, and only the look knows which tree it opened.
/// Nothing invents one when the look had none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeMeasurement {
    pub identity: ChangeIdentity,
    pub state: ChangeState,
    /// Why, when the state is [`ChangeState::Retired`]. `None` for every other
    /// state, and required for that one: a retirement that did not say why would
    /// be a deletion wearing a state's clothes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retire_reason: Option<RetireReason>,
    /// Why, when the state is [`ChangeState::Undecided`]. `None` for every other
    /// state, and required for that one: a row retracted from the outstanding set
    /// without a reason to argue with would read as answered. A name from the
    /// judge's closed set, as text for the same reason [`ChangeLedgerRow`] keeps
    /// it as text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsettled_reason: Option<String>,
    /// What the measurement saw, kept with the row so a state that disagrees
    /// with the tree can be argued with rather than merely disbelieved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation: Option<String>,
    /// The revision the tree was read at, or `None` when the look had no
    /// revision to name.
    ///
    /// Kept beside the state because a state is a claim about a tree and the tree
    /// moves: without this, "landed" says the artifact is in the project and
    /// cannot say whether that was true of this mainline or of one that has since
    /// dropped it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measured_rev: Option<String>,
    /// What the same check answered at the revision before [`Self::measured_rev`],
    /// or why it could not be asked there.
    ///
    /// A landed row says the artifact is in this tree. It does not say whether it
    /// got there now or was already there when the demand was written down, and
    /// those two read the same on every other column. Kept as the answer's own
    /// words (`satisfied`, `absent`, `unsettled:<reason>`, `unreadable`) rather
    /// than as a verdict, so the comparison is made by whichever reader needs it
    /// and the storage does not carry this module's vocabulary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_observation: Option<String>,
}

impl ChangeMeasurement {
    /// A measurement of a non-retired state.
    ///
    /// Panics on [`ChangeState::Retired`] rather than building a retirement with
    /// no reason: retire through [`ChangeMeasurement::retired`], which cannot be
    /// called without one. The alternative -- letting this build a reason-less
    /// retirement and rejecting it later in the store -- would make the invariant
    /// a property of one storage backend instead of of the value, and the two
    /// would drift the moment a second backend appeared.
    pub fn new(identity: ChangeIdentity, state: ChangeState) -> Self {
        assert_ne!(
            state,
            ChangeState::Retired,
            "a retirement must be built with ChangeMeasurement::retired so it carries a reason"
        );
        assert_ne!(
            state,
            ChangeState::Undecided,
            "an undecided measurement must be built with ChangeMeasurement::undecided so it carries a reason"
        );
        Self {
            identity,
            state,
            retire_reason: None,
            unsettled_reason: None,
            observation: None,
            measured_rev: None,
            parent_observation: None,
        }
    }

    /// A measurement that could not settle the row because of a property of the
    /// row itself, with the reason it could not.
    ///
    /// The reason is an argument rather than a field a caller may leave empty, so
    /// an undecided measurement with no reason is not a value that can be built --
    /// the same shape [`ChangeMeasurement::retired`] has, and for the same
    /// reason: a row moved out of the outstanding set with nothing to say why
    /// would read as answered.
    pub fn undecided(identity: ChangeIdentity, reason: impl Into<String>) -> Self {
        Self {
            identity,
            state: ChangeState::Undecided,
            retire_reason: None,
            unsettled_reason: Some(reason.into()),
            observation: None,
            measured_rev: None,
            parent_observation: None,
        }
    }

    /// A measurement that found the identity is one the account should not hold,
    /// with the reason it should not.
    ///
    /// The reason is an argument to this constructor rather than a field a caller
    /// may leave empty, so a retirement without one is not a value that can be
    /// built -- the closed set of reasons is a required part of the state, not a
    /// label hung on it afterwards.
    pub fn retired(identity: ChangeIdentity, reason: RetireReason) -> Self {
        Self {
            identity,
            state: ChangeState::Retired,
            retire_reason: Some(reason),
            unsettled_reason: None,
            observation: None,
            measured_rev: None,
            parent_observation: None,
        }
    }

    /// The same measurement, naming the revision it was taken against.
    pub fn at_revision(mut self, revision: impl Into<String>) -> Self {
        self.measured_rev = Some(revision.into());
        self
    }

    /// The same measurement, carrying what the same check answered one revision
    /// earlier.
    ///
    /// Only a landed row carries this: for a row that is still outstanding the
    /// answer is `absent` by definition, and for an undecided or retired row
    /// there is no answer to compare.
    pub fn with_parent_observation(mut self, observation: impl Into<String>) -> Self {
        self.parent_observation = Some(observation.into());
        self
    }
}

/// One row the account had called done, whose check a later pass found no longer
/// holds.
///
/// Not a state and not a measurement. The state moves through
/// [`ChangeLedgerStore::apply_measurement`] like every other, and this is the
/// record of what that move took away: the account clears `landed_at` on the way
/// out of `landed`, so without a row here "the ledger had this as done and was
/// wrong" is indistinguishable from "nothing ever measured it" -- and only the
/// first of those is a fact about the tree having moved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeReopen {
    pub identity: ChangeIdentity,
    /// When the row had landed, as the account held it just before the pass
    /// moved it.
    ///
    /// `None` when `landed_at` was already missing on a row whose state said
    /// `landed`. That is a row disagreeing with itself, and it is recorded rather
    /// than skipped: the disagreement is one of the two things this row exists to
    /// make readable, and a call that refused to write it would hide it at the
    /// one moment it is visible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub was_landed_at: Option<DateTime<Utc>>,
    /// The revision the pass read the tree at, empty when it had none to name.
    /// Beside the identity for the same reason a measurement carries one: the
    /// tree moves, and "this stopped being true" without a revision is a claim
    /// nothing can be re-read against.
    pub measured_rev: String,
    /// Whether the row's scope was still in the tree this pass read.
    ///
    /// Two stories arrive as one reopen and a reader cannot tell them apart
    /// afterwards: a claim stops holding because the surface it lived in was
    /// removed (a decision took a demand off the tree), or because the criterion
    /// stopped being true inside a surface that is still there (a regression).
    /// The pass asks the tree this question already -- it is what decides whether
    /// an absent scope retires the row -- and this keeps the answer.
    ///
    /// `None` is "the tree could not be asked", which is neither of the two:
    /// the look failed for a reason that says nothing about the scope. Rows
    /// recorded before the column existed read the same way, and both are
    /// reported as unrecorded rather than folded into either answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_present: Option<bool>,
}

/// One move the account recorded for one identity.
///
/// The stored row says where a change is now; this says how it got there. Both
/// are needed to answer "why does the account hold this row at all", and the
/// answer is not recoverable from the row: a row that moved `undecided` ->
/// `landed` and one that never left `landed` read identically once the earlier
/// steps are gone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeLedgerEvent {
    /// When the move was recorded, not when the tree was read. A pass that
    /// landed late records the instant it was written; `measured_rev` is what
    /// ties the move back to a tree.
    pub observed_at: DateTime<Utc>,
    /// The state the row held before. `None` is the account's word for
    /// "entered": there was no earlier state to leave.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_state: Option<ChangeState>,
    pub to_state: ChangeState,
    /// The revision the measurement behind the move read, when it named one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measured_rev: Option<String>,
    /// Why the row was retired, on the moves that retired it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retire_reason: Option<RetireReason>,
    /// Why the move left the row unsettled, on the moves that could not settle
    /// it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsettled_reason: Option<String>,
}

/// Durable storage for the change ledger.
///
/// The only way to move a row's state is [`Self::apply_measurement`]. Rows enter
/// through the reading pipeline (a document read into the account), which is a
/// separate surface: a reading records what a document asks for, a measurement
/// records what the tree shows, and only the second one decides whether a change
/// is done.
///
/// The reading pipeline has two steps, and the storage keeps them apart. A
/// reading can be queued with [`Self::declare`] by anyone who has news for the
/// ledger; it enters the account only when the ledger's own writer folds it with
/// [`Self::fold_declarations`]. The queue is durable rather than in-process
/// because the two steps happen at different times and often in different
/// processes, and a handover that only ever existed in memory is one the ledger
/// cannot be shown to have received.
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
    ///
    /// `first_seen_rev` is the tree revision in hand the pass folds the reading,
    /// stamped onto a row the first time it enters and never moved, the same rule
    /// `first_seen_at` follows. It is not a verdict -- it says which tree the
    /// account was reading, not what the tree said -- so it does not violate the
    /// line this method draws between a reading and a measurement. `None` when the
    /// pass could not resolve a revision, which is kept rather than filled: a row
    /// that entered while the tree was unreadable has no entry revision.
    async fn record_readings(
        &self,
        readings: &[ChangeReading],
        first_seen_rev: Option<&str>,
    ) -> SFResult<usize>;

    /// Queue a reading to be folded into the account: what a document asks for,
    /// waiting for the ledger's own writer to pick it up.
    ///
    /// This is the intake, and it is a separate door from [`Self::record_readings`]
    /// on purpose. Writing the account directly is the resident writer's job; a
    /// session, a script or an extraction batch that has news for the ledger
    /// queues it here instead. Queuing *is* telling: a change nobody queued is a
    /// change the ledger was never told about, so there is no second channel
    /// through which the handover could happen and not be recorded.
    ///
    /// Takes the same shape a reading does, because that is what it becomes --
    /// one document's naming of one change, with no state. A declaration cannot
    /// move a row's state any more than a reading can: the fold writes through
    /// [`Self::record_readings`], which by contract never touches `state`,
    /// `measured_at` or `landed_at`. The side that declares a change therefore
    /// has no way to declare it done.
    ///
    /// Idempotent per document and identity: queuing the same change from the
    /// same document twice leaves one declaration, so a declaring script that is
    /// re-run after a failure does not queue the same work twice. A declaration
    /// already queued is left exactly as it is -- correcting one means
    /// withdrawing it and declaring again, for the same reason a reading is
    /// withdrawn rather than edited. Answers how many declarations were newly
    /// queued; the ones already there are not counted.
    async fn declare(&self, readings: &[ChangeReading]) -> SFResult<usize>;

    /// Fold up to `limit` unfolded declarations into the account, and answer how
    /// many were folded.
    ///
    /// The fold is the ledger's only way in, and it adds nothing of its own:
    /// every declaration becomes exactly one [`Self::record_readings`] call, so
    /// a state still comes only from a measurement. `first_seen_rev` is carried
    /// through to that call as the revision the folded rows entered at.
    ///
    /// Declarations are marked folded *after* their reading is written. A fold
    /// interrupted in between leaves them unfolded and the next pass repeats the
    /// work, which costs nothing because the reading path is an idempotent
    /// upsert. Marking first would instead drop the reading, and a change that
    /// was declared but never entered the account is the failure the intake
    /// exists to make impossible.
    async fn fold_declarations(
        &self,
        limit: usize,
        first_seen_rev: Option<&str>,
    ) -> SFResult<usize>;

    /// Fold up to `limit` unfolded retirement declarations into the account,
    /// answering how many were folded.
    ///
    /// A retirement declaration is not a reading: it asserts that a row's
    /// identity is one the account should not hold -- a fixture, an identity a
    /// later declaration renamed, a path no criterion can read -- with the reason.
    /// Folding it applies a [`ChangeMeasurement::retired`] through the same
    /// [`Self::apply_measurement`] door every other state goes through, so the
    /// ledger still has exactly one way a state is written and the party that
    /// classified the row never wrote the account itself.
    ///
    /// Like the declaration fold, the retirement is written *before* the intake
    /// row is marked folded, and for the same reason: an interrupted fold repeats
    /// idempotently rather than dropping the retirement. Applying a retirement
    /// twice is applying it once.
    ///
    /// A retirement is re-examinable. A later measurement that finds the row in
    /// any other state clears it, the way a later measurement clears `landed`; this
    /// fold asserts the retirement as of now, it does not make it permanent.
    async fn fold_retirements(&self, limit: usize) -> SFResult<usize>;

    /// How many declarations are queued and not yet folded: the intake's backlog.
    ///
    /// This is the one reading that says whether "the ledger was told" actually
    /// happened. A change handed over and not yet folded appears here and
    /// nowhere else, so zero is the only state in which the account is known to
    /// have caught up with what has been declared.
    async fn pending_declarations(&self) -> SFResult<u64>;

    /// How many retirements are queued and not yet folded.
    ///
    /// The same reading the declaration backlog gives, for the retirement intake:
    /// a retirement handed over and not yet folded appears here and nowhere else,
    /// and a backlog that stays non-zero across passes is the one reading that
    /// says the account has not caught up with what the classifier retired.
    async fn pending_retirements(&self) -> SFResult<u64>;

    /// Apply one measurement: move the row's state, stamp `measured_at`, and
    /// stamp or clear `landed_at` to match.
    ///
    /// A measurement of an identity the ledger does not have yet creates the
    /// row, because the account is defined by what has been measured rather than
    /// by what has been read about.
    async fn apply_measurement(&self, measurement: &ChangeMeasurement) -> SFResult<()>;

    /// The rows that are still outstanding, most urgent band first and, within
    /// a band, stalest measurement first.
    ///
    /// Both terminal states are left out, [`ChangeState::Landed`] and
    /// [`ChangeState::Retired`]: a row whose question is answered is not work,
    /// and a reader that took "not landed" for "outstanding" would hand a retired
    /// row back to the tree and overwrite the retirement with whatever the tree
    /// says -- which for an identity that is not an artifact is the unsettled
    /// answer this state exists to stop repeating.
    ///
    /// The band leads because the bound is what makes the order matter. A caller
    /// takes a slice of a set thousands deep, and a slice ordered only by
    /// staleness spends itself on whichever rows have waited longest, which says
    /// nothing about which changes matter. With this order the highest band a
    /// document asked for is measured and handed over first, and every unstated
    /// row is behind all of them rather than interleaved by insertion luck.
    /// Within one band the rotation is unchanged: `None` in `measured_at` sorts
    /// first, because nothing has looked at those and that is less known than a
    /// stale measurement, not more current, and a row measured this pass goes to
    /// the back of its own band rather than being re-asked next pass.
    ///
    /// A band nobody asked for is not a band, but it is also not a guarantee
    /// against starvation, and the reading is not one either. The band order is
    /// a prefix, so a banded set at least as large as the batch holds every slot
    /// and what sits behind it is never reached -- a set of urgent rows that big
    /// is visible in the band counts, and being visible does not make the rows
    /// behind it move. So the bound is split: part of it is taken from the band
    /// order and the rest from the stalest rows the set holds, band not
    /// consulted. Every outstanding row is then asked within a number of passes
    /// that follows from the size of the set, whatever the bands in it are.
    async fn outstanding(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>>;

    /// The moves recorded for one identity, newest first.
    ///
    /// Takes the identity in its three parts rather than the flattened key: the
    /// key is a rendering, not a field, and a literal full of colons makes two
    /// different splits render the same. A caller that only holds the key is a
    /// caller that will send the wrong question and get an empty history back,
    /// which reads as "this row never moved" -- the answer that is never true.
    ///
    /// `limit` bounds the answer; an identity's whole history is not needed to
    /// answer "how did this get here", and the oldest steps are the ones a
    /// reader can live without.
    async fn events(
        &self,
        identity: &ChangeIdentity,
        limit: usize,
    ) -> SFResult<Vec<ChangeLedgerEvent>>;

    /// How much work is owed at each band, every band present, zero included.
    ///
    /// The whole owed set, not this pass's slice: [`Self::outstanding`] answers
    /// what to hand out next, and this answers whether the hand-outs are getting
    /// anywhere. Without it the band order is unobservable -- a pass that keeps
    /// taking P0 rows looks exactly like a pass that keeps taking P3 ones, and
    /// "the urgent work is not draining" has no reading to be read from.
    ///
    /// Every band is a cell in [`ChangePriority::ALL`] order whether or not it
    /// has rows. A caller that got only the bands with work would have to know
    /// which ones it was not told about to publish a zero, and the one it forgot
    /// would read as a band that stopped being published rather than a band that
    /// emptied.
    ///
    /// Counts rows that are outstanding, which is the same population
    /// [`Self::outstanding`] slices: a landed or retired row is not work owed and
    /// is not counted here.
    async fn owed_by_band(&self) -> SFResult<Vec<(ChangePriority, usize)>>;

    /// The rows the account calls done, stalest measurement first, whose check a
    /// later pass is due to ask again.
    ///
    /// [`Self::outstanding`] excluded [`ChangeState::Landed`] to define the work,
    /// and that exclusion is what left the account's strongest claim as the one
    /// claim nothing ever re-read: `landed` says a check came out satisfied, and
    /// a row that no read reaches keeps whatever it was first given. Measured on
    /// the live ledger before this call existed: 1369 landed rows, the oldest
    /// measured a day and fifteen hours earlier, none of them due to be looked at
    /// again ever -- while the tree they were judged against moves.
    ///
    /// A call of its own rather than a widened [`Self::outstanding`], because of
    /// where those rows sit in the latter's order. `measured_at ASC NULLS FIRST`
    /// makes an unmeasured row the least known and the first read, which is right
    /// for work; landed rows are measured, and the stale ones among them are
    /// measured *oldest*, so folding them into that set would put rows older than
    /// any pending row at its head and starve the work of the batch it shares.
    /// The caller takes a slice here and the whole set there, and the two bounds
    /// belong to two different questions.
    ///
    /// Stalest first for the same reason the other read is: a caller that takes
    /// `limit` rows has to sweep the state, not re-ask the same head of it.
    ///
    /// The band does not order this read, unlike [`Self::outstanding`]. A landed
    /// row is not work owed, and the question this read asks is freshness: has
    /// what we call done stopped being true. Sorting by band here would let a
    /// large urgent set push the oldest landed rows past every sweep, and an old
    /// landed row is exactly the one most likely to have quietly stopped
    /// holding. Urgency belongs to the work, staleness to the claim.
    async fn landed_due(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>>;

    /// The landed rows, each with the revision it landed at, stalest
    /// measurement first.
    ///
    /// [`Self::landed_due`] carries `measured_rev`, and a re-check moves that
    /// column to the revision the check was last asked at. The two are different
    /// facts wherever the tree has moved since the landing, and the difference is
    /// not academic: the caller that asks "is this landing in the image the
    /// workloads run" compares the answer against a running revision, and a
    /// re-check always runs at a tree that is at least one rollout ahead of that
    /// image. Measured on the live account before this read existed: 3319 landed
    /// rows carried five distinct `measured_rev` values, the in-effect pass
    /// counted 2005 of them out of effect, and every row whose landing the event
    /// log records -- 639 of those 2005 -- had landed at a commit the running
    /// image carries. The count read as the line's lag and was a count of rows
    /// re-checked since the image was built.
    ///
    /// The landing's own revision survives a re-check only in the event log,
    /// which records the revision of every transition into `landed`. A row with
    /// no such event landed before the log existed and comes back as `None`
    /// rather than falling back to `measured_rev`: a revision its check was not
    /// done at would answer a question nobody asked, and the caller already
    /// counts unplaceable rows apart from both sides.
    async fn landed_at_revision(
        &self,
        limit: usize,
    ) -> SFResult<Vec<(ChangeIdentity, Option<String>)>>;

    /// The landed rows whose check reads something one of `paths` could change,
    /// stalest measurement first.
    ///
    /// [`Self::landed_due`] keeps the state fresh on a window; this read is the
    /// event-shaped half of the same question. A check's answer depends only on
    /// what its scope holds, so a revision that touched no path under that scope
    /// cannot have changed the answer, and one that touched it should be asked
    /// again now rather than when the sweep happens to reach it. Together the two
    /// reads make a landed row's freshness a property of the tree moving, with
    /// the window as the floor under changes no diff can name -- a check whose
    /// own implementation changed, history that was rewritten.
    ///
    /// A scope is a path relative to the tree root; the whole tree (`.`, or the
    /// empty scope) is touched by every path, and a directory is touched by
    /// anything below it. Scopes are matched after normalizing the spellings the
    /// account already holds -- a leading `./` and a trailing `/` name the same
    /// location.
    async fn landed_touching(
        &self,
        paths: &[String],
        limit: usize,
    ) -> SFResult<Vec<ChangeLedgerRow>>;

    /// Landed rows that carry no parent reading, with the revision each one
    /// landed at, oldest landing first.
    ///
    /// The column was added after the rows were, and a re-check cannot fill it:
    /// [`Self::landed_due`] asks about today's parent, and answering "already
    /// there" for a row that landed weeks ago is exactly the reading the column
    /// exists to tell apart from a real arrival. The landing's own revision is
    /// a fact the account holds twice over -- as the row's `measured_rev` until
    /// a re-check moves it, and as the first transition into `landed` in the
    /// event log, which never moves. The event log is what this read answers
    /// with, so a row that has been re-checked still gets its own landing.
    ///
    /// The pair is the identity and that revision; the row is not returned
    /// because the only thing the caller can do with it is ask the tree at that
    /// revision, and a whole row would invite it to write one back.
    ///
    /// A one-off repair rather than a reading, and it says so by emptying: a
    /// caller that keeps finding rows here has a landing that writes no parent
    /// reading, which is the state to fix.
    async fn landed_awaiting_parent_observation(
        &self,
        limit: usize,
    ) -> SFResult<Vec<(ChangeIdentity, String)>>;

    /// Write the parent reading on a row that has none, returning whether this
    /// call wrote it.
    ///
    /// Never overwrites: the first landing's reading is the fact, and a backfill
    /// that could replace it would make the column say whatever ran last. A row
    /// that already has one returns false -- not an error, because the caller's
    /// work is done either way.
    async fn record_parent_observation(
        &self,
        identity: &ChangeIdentity,
        observation: &str,
    ) -> SFResult<bool>;

    /// Record that a row the account called done no longer holds.
    ///
    /// What this keeps is what the next write destroys. Moving a row out of
    /// `landed` clears `landed_at` -- the account's columns agree with its state
    /// by construction -- so after the measurement lands, "the account had this
    /// as done and the tree disagreed" and "nothing ever measured this" are one
    /// row apart and read the same. This is where the difference survives: the
    /// identity, the time it had landed, and the revision the pass read.
    ///
    /// Recording a reopen moves no state. The state moves through
    /// [`Self::apply_measurement`] like every other, and this call is made beside
    /// that one rather than instead of it, so the account still has exactly one
    /// door a state is written through.
    ///
    /// Appended, never updated: a row reopened three times is three observations,
    /// and an upserted row would only ever answer the last of them.
    async fn record_reopen(&self, reopen: &ChangeReopen) -> SFResult<()>;

    /// The rows settled as retired because their scope had left the tree,
    /// oldest retirement first.
    ///
    /// A retirement is a statement about the history -- "the deletion commit
    /// took this surface out" -- and it is true of the past, not of the tree
    /// that follows. A revert, a branch switch, or a surface moved out and back
    /// brings the scope into the tree again, and every read that could notice
    /// excludes these rows by design: [`Self::landed_due`] reads `landed`, and
    /// [`Self::outstanding`] excludes `retired` because "its question is
    /// answered". A returned surface falsifies that ground, and without this
    /// read nothing in the account would ever ask again -- the row would keep
    /// saying its surface is gone while the tree holds it.
    ///
    /// This read is the finding only; whether the scope is actually back is a
    /// question for the caller's tree, because that is where the answer lives.
    /// Ordered by `retired_at` so a bounded caller sweeps the set rather than
    /// re-asking the same head of it forever: a retirement that is still true
    /// has no measurement to advance, so nothing else in the order moves.
    async fn retired_surface_moved(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>>;

    /// The rows retired because another account had answered them, oldest
    /// retirement first.
    ///
    /// The counterpart of [`Self::retired_surface_moved`] for the one other
    /// retirement whose ground can be falsified from outside this tree, and its
    /// own read rather than a widened one because the fact that revives it lives
    /// in a different account: the scope coming back is a question for the tree,
    /// while "does the other account still carry the gate" is a question only
    /// that account answers. A caller handed both kinds in one list would have to
    /// know which door each row's answer comes from.
    ///
    /// Like its counterpart this is the finding only: the caller asks the account
    /// that answered, and a read that failed there is not a fact about the row.
    /// Ordered by `retired_at` for the same reason.
    async fn retired_answered_elsewhere(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>>;

    /// Ask the account to measure this row's identity again.
    ///
    /// A row's state is a measurement, and a measurement is only as good as the
    /// rule the pass that took it read. Most of those rules are the tree's and
    /// are re-read on their own: a check's answer comes back through the landed
    /// sweep, a returned surface through [`Self::retired_surface_moved`], an
    /// answer withdrawn from another account through
    /// [`Self::retired_answered_elsewhere`]. The rest are classifications made
    /// outside this account -- which identities name a fixture, which were
    /// renamed -- and when one of those rules is corrected, the rows it settled
    /// describe the rule as it was. Nothing revisits them: `outstanding` holds
    /// them out as answered, and no retirement read covers their reason.
    ///
    /// So a correction announces itself here rather than writing a state. This
    /// account has one door a state is written through, and it is the writer's
    /// own measurement: a producer can say the state was measured by a rule that
    /// has since changed, and only the writer can say what the row is now.
    ///
    /// Queuing the same identity twice is queuing it once, and a request already
    /// folded is not revived by a second one -- a row measured again needs no
    /// second measurement until another rule changes.
    async fn request_remeasurement(&self, identity: &ChangeIdentity, source: &str) -> SFResult<()>;

    /// The identities whose states were measured by a rule that has since been
    /// corrected, oldest request first.
    ///
    /// The finding only, like the two retirement reads beside it: whether the row
    /// moves is the caller's measurement to make.
    async fn pending_remeasurements(&self, limit: usize) -> SFResult<Vec<ChangeIdentity>>;

    /// Mark one request folded, once its measurement has been written.
    ///
    /// Guarded so that it is a statement about this fold rather than an
    /// overwrite: a request already marked keeps the time an earlier fold set.
    async fn mark_remeasured(&self, identity: &ChangeIdentity) -> SFResult<()>;

    /// Record that a commit delivered this row's change, and which commit.
    ///
    /// Written from a reader of the line, not from the landing process: the
    /// landing process commits and then returns, and a record made after the
    /// commit is a record that is missing for every change whose process died in
    /// between -- while the commit itself is on the line and plainly readable.
    ///
    /// Writes no state and no measurement time. A delivered change has been
    /// handed to the line; whether its claim holds is still the measurement's to
    /// say, and this door may not say it. The write is first-one-wins: a change
    /// landed twice keeps the delivery that was recorded first, which is the one
    /// a reader asking "when did this reach the line" wants.
    async fn record_delivery(&self, identity: &ChangeIdentity, rev: &str) -> SFResult<()>;

    /// Mark the outstanding queue as waiting on the LLM pool, with the fact that
    /// ends the wait.
    ///
    /// A block is not a state: the row is still owed and still measured, and
    /// nothing about the tree is said by this write. What it says is why nobody
    /// can act on the row *now* -- and that is the one thing a reader of the
    /// account could not get before. "Two thousand rows nobody has reached" and
    /// "two thousand rows waiting for an upstream the gateway can reach" read
    /// identically off the state columns, and the two want different actions:
    /// sending someone after the first is wasted work, and only the second can
    /// say what it is waiting for and when it ends by itself.
    ///
    /// Scoped to the rows that are still owed and either unblocked or blocked for
    /// this same reason: a block somebody else declared is not this pass's to
    /// overwrite, and clearing it by stamping over it would erase the one fact
    /// that made the row wait on a person rather than on the pool. Stamping again
    /// leaves the start where it was, so "waiting since" keeps meaning the first
    /// pass that found it stuck rather than the latest one.
    async fn stamp_pool_block(&self, unblock_condition: &str) -> SFResult<u64>;

    /// Clear the pool block from every row carrying it.
    ///
    /// Called by a pass that could hand work over again. Deliberately keyed on
    /// the reason rather than on "every blocked row": a block declared for
    /// another reason is still true when the pool comes back, and a clear written
    /// over all of them would report every waiting-on-a-person row as free -- the
    /// same silent rewrite the writer above avoids, in the other direction.
    async fn clear_pool_block(&self) -> SFResult<u64>;

    /// One row, by the identity that names it.
    async fn get(&self, identity: &ChangeIdentity) -> SFResult<Option<ChangeLedgerRow>>;

    /// How many rows the ledger holds, by state.
    ///
    /// Answers with every state present, including the zeroes: a reader
    /// comparing two censuses needs to see a state that emptied, which is
    /// exactly the reading an absent key would hide.
    async fn census(&self) -> SFResult<Vec<(ChangeState, u64)>>;

    /// Record what one dispatch pass decided, and why it decided that much.
    ///
    /// The pass is the account's executor-facing half, and it is the half whose
    /// stalling used to be invisible: a pass that handed nothing over and a pass
    /// with nothing to hand over leave the same rows in `change_ledger`, so the
    /// account alone cannot tell them apart. This is where the difference is
    /// written down -- not because the pass's health is a change, but because the
    /// reason dispatch stood still is a reading, and a reading that lives only in
    /// a process's log dies when the process rolls.
    ///
    /// Appended, never updated: one row per pass, so a reader can ask how long
    /// dispatch stood still and how often, which an upserted row could not answer.
    async fn record_dispatch_state(&self, state: &DispatchState) -> SFResult<()>;

    /// How many declarations have been answered, and how many are still owed.
    ///
    /// The demand side of the loop the account's own census is the supply side
    /// of. `census` answers "what is still untrue of the tree"; this answers
    /// "what did the documents ask for, and how much of it did the tree end up
    /// holding". Until it existed the intake could only say what had been *asked*
    /// -- a declaration was folded into the account and never spoken of again --
    /// so the one question a reader of a change process actually asks ("did what
    /// we said we would do get done") had no reading anywhere in the platform.
    /// The host-side projection was left to answer it, which is the wrong side
    /// of the boundary: the readers who need the number are the ones watching the
    /// platform.
    ///
    /// Counted by declaration rather than by change, because the intake is keyed
    /// per document: two documents asking for the same change are two demands,
    /// and a change that lands answers both. Counting changes here would report
    /// the same fact twice and lose the second document.
    ///
    /// `None` means this store cannot answer -- an implementation with no intake,
    /// or a deployment whose migration has not run. It is not zero: a count that
    /// was never taken and a count of nothing read the same on a dashboard, and
    /// the whole point of the cells published from this is that "nobody asked"
    /// and "nobody could look" stay apart.
    async fn demand_closure(&self) -> SFResult<Option<DemandClosure>> {
        Ok(None)
    }

    /// What the account is holding in front of the rest of itself.
    ///
    /// `None` means this store cannot answer -- an implementation with no
    /// ledger, or a deployment whose migration has not run -- and is not zero
    /// for the same reason [`Self::demand_closure`] is not.
    async fn blocking_surface(&self) -> SFResult<Option<BlockingSurface>> {
        Ok(None)
    }

    /// Who is holding the line, and how much each one holds.
    ///
    /// The surface counts; this names. A reader that can say "three changes
    /// hold eleven landings" still cannot act, because the first step is to
    /// find the change -- and a count with no name sends it to search the
    /// account in a different order than the one the count came from.
    ///
    /// `None` means this store cannot answer, not that nobody is holding.
    async fn blocking_holders(&self) -> SFResult<Option<Vec<BlockingHolder>>> {
        Ok(None)
    }

    /// The owed set counted by the face its documents named.
    ///
    /// `None` when the store cannot group by face; an empty vector means every
    /// row it holds is unfaced, which the `none` entry states rather than
    /// hides.
    async fn block_census(&self) -> SFResult<Option<Vec<BlockCensus>>> {
        Ok(None)
    }

    /// Every write window the account holds, whatever state it is in.
    ///
    /// All of them rather than the live ones: a query filtered by "approved and
    /// not revoked and unexpired" would make the revoked row unreachable, and
    /// that row is the one a review of what was written under the approval
    /// needs. The reduction to a reading is the caller's, done in one place, so
    /// a store cannot hold a second opinion about what an approved window is.
    ///
    /// `None` means this store has no write windows to offer -- an
    /// implementation that predates the table, or a deployment that has not run
    /// the migration. An empty vector is a different answer with a different
    /// meaning: the table is there and holds no window, so nothing is approved,
    /// which is a measurement. A store that cannot answer must not read as one.
    async fn write_windows(&self) -> SFResult<Option<Vec<WriteWindow>>> {
        Ok(None)
    }
}

/// The line a landed commit carries, followed by the id of the change it landed.
///
/// One definition because there are two sides to it and they are in different
/// crates: the landing machine writes it and the ledger reads it. A literal
/// copied into both would drift the day one of them gained a space, and the
/// failure would be silent -- the reader would keep finding nothing and report
/// every delivered change as undelivered, which is exactly the reading the
/// trailer exists to make possible.
pub const LANDING_TRAILER: &str = "Landing change ";

/// How much of what the documents asked for the tree has ended up holding.
///
/// Both halves, always: a reader given only the satisfied count cannot tell a
/// demand set that is fully answered from one that was never asked, and the
/// comparison across two ticks is what makes the loop's closure visible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandClosure {
    /// Declarations whose change the account holds as landed.
    pub satisfied: u64,
    /// Declarations whose change is still untrue of the tree, or which no
    /// measurement has reached at all.
    pub owed: u64,
}

/// How much of the account is standing in front of the rest of it.
///
/// The ordering half of the account: what a change blocks is the one quantity a
/// tree cannot state about urgency but the account can measure about itself. A
/// commit delivered a change and the account still owes it, so every row
/// measured on that same revision is standing on a claim that is not yet true.
/// Counting those rows is what turns "which of these should be fixed first"
/// from whoever is reading the dashboard's opinion into a number.
///
/// Both halves always. `holders` alone cannot say whether one change is holding
/// one row or the whole outstanding set; the pair is what a reader needs to
/// answer "how much of the queue is behind this".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockingSurface {
    /// Changes a commit delivered while the account still owes them.
    pub holders: u64,
    /// Rows, other than their holders, measured at a revision a holder
    /// delivered. Each row is counted once however many holders it stands on.
    pub held_landings: u64,
    /// Rows whose documents name more than one face. Folded to `none` by the
    /// row's own answer, and counted here so a disagreement never reads as
    /// silence.
    pub block_conflicts: u64,
}

/// One change that delivered a commit the tree has not confirmed, and how many
/// other rows that delivery is holding back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockingHolder {
    pub identity: ChangeIdentity,
    /// Rows, other than this one, measured at the revision it delivered.
    pub held: u64,
}

/// One face of the programme, counted by what the tree has made of it.
///
/// The grouping the account could not answer before: content was in the ledger
/// but the heading was only in prose, so "how much of this face is done" was a
/// substring search whose answer changed with who typed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockCensus {
    pub block: ChangeBlock,
    /// Rows the tree holds as landed.
    pub landed: u64,
    /// Rows still owed.
    pub owed: u64,
    /// Rows this face no longer asks for. Counted so the three buckets add up
    /// to the face's population: without it a face that shrank to nothing by
    /// retiring its work would be indistinguishable from one that finished.
    pub retired: u64,
    /// How long the oldest owed row in this face has gone without a
    /// measurement. `None` when nothing in it is owed or every owed row has
    /// been measured: the two are different facts and neither is zero.
    pub oldest_unmeasured_secs: Option<u64>,
    /// How long since anything in this face moved: the newest of its rows'
    /// first-seen, measured and landed stamps. `None` only for a face with no
    /// rows, which is `none` on an account holding nothing at all -- a face the
    /// census reports always has rows behind it.
    ///
    /// This is what separates a heading that is finished from one that is
    /// between jobs: both hold no live row, and only this says for how long.
    /// Measured rather than counted from passes, because a process that
    /// restarted would otherwise forget the count and one that replayed would
    /// invent it.
    pub newest_activity_secs: Option<u64>,
}

/// What one dispatch pass did, and the closed reason it did only that much.
///
/// The pass is not a verdict on any change, and this carries none: it says what
/// the handover did this tick, what was owed at the time, and -- when nothing was
/// handed over -- which of the agreed reasons stopped it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchState {
    pub decision: DispatchDecision,
    pub pool: PoolReading,
    /// Rows that were owed this pass. The reading that separates "nothing to do"
    /// from "work was owed and was not handed over".
    pub pending: u64,
    pub submitted: u64,
    /// Rows whose task the goal path already had: handed over on an earlier pass.
    pub already: u64,
    pub failed: u64,
    /// Rows the pass gave up on: a task held them, every generation its handover
    /// was allowed arrived at the environment and was refused, and the budget for
    /// trying another one is spent. They are owed and they are not in flight,
    /// which is a state no other column states -- without this one they were in
    /// `pending` and in nothing else, so the rows the environment killed were the
    /// rows the account could not name.
    pub dead_environment: u64,
    /// Rows this pass did not hand over and no task holds: the work that was owed
    /// at the moment the pass decided to hand nothing over. Only a pass that
    /// dispatched nothing has any, which is what lets the counts add up to
    /// `pending` on that pass as well as on one that dispatched -- the same
    /// subtraction, with the reason the remainder is a remainder.
    pub held_back: u64,
    /// Rows this pass handed over again at a later generation, their earlier
    /// handovers having been refused by the environment. Counted beside the
    /// outcome they reached rather than instead of it: a row re-armed and then
    /// submitted is in both numbers, and it is the pair that says how much of a
    /// pass's work went into getting back to where it started.
    pub rearmed: u64,
    /// Rows still owed whose change a commit on the line names as the one it
    /// landed. An annotation beside the outcome they reached, not an outcome of
    /// its own, and the two are deliberately different facts: a committed change
    /// has been delivered, while `landed` says the claim its row makes is true of
    /// the tree. A change can be delivered and never measure landed -- the diff
    /// went in and the property it was supposed to establish did not -- and
    /// without this number that row is indistinguishable from one no task ever
    /// finished. `None` when the commit log could not be read, because zero is a
    /// measurement and "nothing said" is not.
    pub delivered: Option<u64>,
    /// Rows this pass's own measurement could not settle, so they were not handed
    /// over. Counted apart from `pending` because the state alone cannot show
    /// them.
    pub unmeasurable: u64,
    /// When dispatch may resume, unix seconds; `0` when no bound is known.
    pub resume_at_unix: i64,
    /// The recovery instant the pool's own evidence stated, unix seconds; `0`
    /// when nothing stated one. Kept beside `resume_at_unix` so a reader can tell
    /// a bound derived from an upstream's statement from one this process chose.
    pub evidenced_recovery_unix: i64,
    /// Longest quota window any suspect upstream stated, seconds; `0` when none
    /// did. The only statement of how long a quota outage lasts when no instant
    /// was given.
    pub quota_window_secs: u64,
    /// The intakes this pass ran and could not read, comma-separated; empty
    /// when every one of them answered.
    ///
    /// An intake that fails contributes no readings and the account keeps the
    /// rows it already holds, so a pass that read nothing leaves exactly the
    /// rows of a pass that found nothing to read. While the tree's registries
    /// are still an intake, losing one of them on a tick costs nothing visible;
    /// once the account's only source of new debt is another account, that
    /// silence is the whole failure mode. Naming the source here is what lets a
    /// reader count consecutive passes instead of learning it from a log line
    /// that rolled away.
    ///
    /// `None` is what every row written before this column existed carries, so
    /// a reader must treat it as "nothing said" rather than as "every source
    /// answered". Every pass written since writes a string: empty when all
    /// sources were read, the names joined by commas when some were not -- which
    /// keeps "this pass read everything" and "this row predates the column"
    /// distinguishable instead of both reading as empty.
    pub sources_unread: Option<String>,
    /// The window pace the pool snapshot carried this pass, when it carried a
    /// measurement; `None` when no snapshot answered and when the one that did
    /// had measured no window.
    ///
    /// It is recorded, rather than only used to size the batch, because the two
    /// numbers answer each other: a pass that handed over 200 rows while the
    /// window allowed four is a pass whose batch outran its quota, and without
    /// this row that comparison cannot be made after the fact. `None` is not a
    /// pace of zero -- a snapshot from a build that did not carry the field, and
    /// a pass that read no snapshot at all, both read as "nothing said", which
    /// is why rows written before this column existed also read as `None`.
    pub window_pace: Option<crate::contract::supervisor::LlmWindowPace>,
}

/// One requested window in which a named account may write to a path, and who
/// approved it.
///
/// The row is a request from the moment it exists and an approved window once
/// `approved_at` is written; the two are told apart by the fields rather than by
/// a status column, because "approved and then revoked" is two facts and one
/// column would have to forget the first to record the second.
///
/// The reader's rule, and the only one there is: a window is approved when
/// `approved_at` is set, `revoked_at` is not, `expires_at` is in the future, and
/// the approver is not the writer. Each of those refusals is a way the record
/// can look like an approval without being one -- an unbounded permission, an
/// unnamed approver, an account approving itself -- and the reading counts the
/// live windows so that the last of them cannot hide inside the first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteWindow {
    /// The requester's own name for the window; the row's identity.
    pub window_id: String,
    /// The path the window covers, as the approver wrote it.
    pub path_scope: String,
    /// The account that asked to write.
    pub writer: String,
    /// The account that approved; empty when nobody has.
    pub approver: String,
    pub requested_at: DateTime<Utc>,
    /// `None` when the window was requested and never approved.
    pub approved_at: Option<DateTime<Utc>>,
    /// `None` when the approval states no end; such a window is not approved.
    pub expires_at: Option<DateTime<Utc>>,
    /// `None` while the window stands, whatever its expiry says.
    pub revoked_at: Option<DateTime<Utc>>,
}

impl WriteWindow {
    /// Whether the window is still open at `now`: stamped as approved, not
    /// revoked, and expiring after `now`.
    ///
    /// The three conditions that make an approval applicable at all. Who
    /// approved is a separate question -- see [`Self::approved`] -- and the two
    /// are kept apart because the self-approved row is a window that is open by
    /// the first question and refused by the second, which is exactly the row a
    /// reader has to be able to name.
    pub fn live(&self, now: DateTime<Utc>) -> bool {
        self.approved_at.is_some()
            && self.revoked_at.is_none()
            && self.expires_at.is_some_and(|expires| expires > now)
    }

    /// Whether this row is an approval that stands at `now`: open, and approved
    /// by somebody other than the writer.
    ///
    /// The rule in one place, because a second copy of it would drift from this
    /// one the day either gained a condition, and the drift would show up as a
    /// window counted as approved by one reader and not by the other.
    pub fn approved(&self, now: DateTime<Utc>) -> bool {
        self.live(now) && !self.approver.is_empty() && self.approver != self.writer
    }

    /// Whether the row is an open window its writer approved for itself.
    ///
    /// Refused by [`Self::approved`] already; counted separately so that a row
    /// cannot be refused and invisible at the same time. What the reader does
    /// with this number is a decision, and it is one the account should be able
    /// to make because the row is there.
    pub fn self_approved(&self, now: DateTime<Utc>) -> bool {
        self.live(now) && !self.approver.is_empty() && self.approver == self.writer
    }

    /// Whether this row records both that the window was approved and that it
    /// was withdrawn.
    ///
    /// Such a row is not a window that stands, and it is not a request either:
    /// something was approved and then taken back. Counting it is what keeps the
    /// withdrawal visible -- a reader that classified it as revoked and moved on
    /// would lose the half that says an approval once existed, and the windows
    /// that were never approved and the ones that were approved and then
    /// withdrawn would be one number -- zero -- in every other cell.
    pub fn inconsistent(&self) -> bool {
        self.approved_at.is_some() && self.revoked_at.is_some()
    }
}

/// The decision a dispatch pass reached, as a closed set.
///
/// One value per reason dispatch can stand still for, so a reader enumerating the
/// set does not have to learn about a new one from an unexpected row. The pool
/// member is the one that exists today; a second reason is a value here, a CHECK
/// in the table, and a migration -- which is what keeps the set closed rather
/// than merely documented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchDecision {
    /// Nothing withheld the hand-over: whatever this pass did not hand over, it
    /// did not leave because the pool could not serve.
    ///
    /// A pass that hands over its own slice of a large owed set and leaves the
    /// rest is this one, so this is not the same claim as "everything owed went
    /// out". The rows it left are counted with the rows a paused pass withholds,
    /// and the two are told apart here: a paused pass hands nothing over at all,
    /// so a pass that handed something over and still has rows left spent its own
    /// slice and no more.
    Proceed,
    /// The pass handed nothing over: no configured upstream could serve a
    /// request, so every task it submitted would have failed for that reason.
    PausedLlmPoolUnavailable,
}

impl DispatchDecision {
    /// Every decision, for a reader that has to enumerate the set.
    ///
    /// The reading face publishes one cell per combination of this set and
    /// [`PoolReading::ALL`] on every pass, so a combination is written as zero
    /// before its pass ever reaches it and no combination is left holding a
    /// value from a pass that has moved on. A variant missing from this list
    /// would be a cell no pass ever writes, and the count that the cells exist
    /// for would go on missing it.
    pub const ALL: [DispatchDecision; 2] = [Self::Proceed, Self::PausedLlmPoolUnavailable];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Proceed => "proceed",
            Self::PausedLlmPoolUnavailable => "paused_llm_pool_unavailable",
        }
    }
}

/// What the pool snapshot said, including that there was none.
///
/// Three values rather than two because "no evidence of a healthy pool" and "no
/// evidence of a sick one" arrive as the same absence, and a reader that saw
/// them as one value could not tell a pass that dispatched behind a healthy pool
/// from one that dispatched blind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolReading {
    /// A snapshot said the pool can serve.
    Available,
    /// A snapshot said no configured upstream can serve.
    Unavailable,
    /// No usable snapshot: nothing said either way.
    Absent,
}

impl PoolReading {
    /// Every reading of the pool snapshot, for a reader that has to enumerate
    /// the set. See [`DispatchDecision::ALL`] for what enumerating it is for.
    pub const ALL: [PoolReading; 3] = [Self::Available, Self::Unavailable, Self::Absent];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
            Self::Absent => "absent",
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every state and reason round-trips through its name, and the lists hold no
    /// duplicates.
    ///
    /// This is the gate on the closed sets: the storage writes a state by its
    /// name and reads it back by parsing, and a variant left out of `ALL` is one
    /// no reader enumerates -- the census would count it as absent and the row
    /// would vanish from every view without an error. Adding a variant without
    /// adding it to `ALL`, or giving two variants the same name, fails here.
    #[test]
    fn every_state_and_reason_round_trips_through_its_name() {
        for state in ChangeState::ALL {
            let name = state.as_str();
            assert_eq!(
                name.parse::<ChangeState>(),
                Ok(state),
                "state {name} does not parse back to itself"
            );
        }
        let mut states: Vec<&str> = ChangeState::ALL.iter().map(|s| s.as_str()).collect();
        states.sort_unstable();
        let unique = states.len();
        states.dedup();
        assert_eq!(unique, states.len(), "two states share a name");

        for reason in RetireReason::ALL {
            let name = reason.as_str();
            assert_eq!(
                name.parse::<RetireReason>(),
                Ok(reason),
                "reason {name} does not parse back to itself"
            );
        }
        let mut reasons: Vec<&str> = RetireReason::ALL.iter().map(|r| r.as_str()).collect();
        reasons.sort_unstable();
        let unique = reasons.len();
        reasons.dedup();
        assert_eq!(unique, reasons.len(), "two reasons share a name");
    }

    /// The two sets the dispatch reading face enumerates name each value once.
    ///
    /// The cells of `cogneva_change_dispatch_verdict` are the product of these
    /// two lists, so two variants sharing a name would make two cells one
    /// series, and whichever the pass wrote last would decide what the pair
    /// reads as -- a cell a reader can no longer take apart, and one whose
    /// count no longer answers its own question. Neither list has a name to
    /// parse back, so this is the whole of what can be asserted here; what a
    /// value *means* is asserted where it is read.
    #[test]
    fn the_dispatch_sets_name_each_of_their_values_once() {
        let mut decisions: Vec<&str> = DispatchDecision::ALL
            .iter()
            .map(|decision| decision.as_str())
            .collect();
        let listed = decisions.len();
        decisions.sort_unstable();
        decisions.dedup();
        assert_eq!(
            listed,
            decisions.len(),
            "two decisions share a name, so two cells of the verdict's key set are one series"
        );

        let mut readings: Vec<&str> = PoolReading::ALL
            .iter()
            .map(|reading| reading.as_str())
            .collect();
        let listed = readings.len();
        readings.sort_unstable();
        readings.dedup();
        assert_eq!(
            listed,
            readings.len(),
            "two pool readings share a name, so two cells of the verdict's key set are one series"
        );
    }

    /// The two terminal states are the two that are not outstanding.
    ///
    /// A reader that asked "is it landed" to answer "is there work here" would
    /// count every retired row as outstanding, and hand a fixture identity back
    /// to the tree. This pins the predicate to the set so the two cannot drift.
    #[test]
    fn only_the_terminal_states_are_not_outstanding() {
        for state in ChangeState::ALL {
            let terminal = matches!(state, ChangeState::Landed | ChangeState::Retired);
            assert_eq!(state.is_outstanding(), !terminal, "{}", state.as_str());
        }
    }

    /// A retirement cannot be built without a reason, and a reason cannot be hung
    /// on a non-retired measurement.
    ///
    /// The first half is a panic because the value is unbuildable; the second is
    /// checked in the store, and this only pins the constructor's side of it.
    #[test]
    #[should_panic(expected = "must be built with ChangeMeasurement::retired")]
    fn a_retirement_without_a_reason_cannot_be_built() {
        let _ = ChangeMeasurement::new(
            ChangeIdentity::new("grep", "crates/", "x"),
            ChangeState::Retired,
        );
    }

    #[test]
    fn a_retirement_carries_its_reason_and_a_plain_measurement_carries_none() {
        let retired = ChangeMeasurement::retired(
            ChangeIdentity::new("grep", "crates/", "x"),
            RetireReason::Fixture,
        );
        assert_eq!(retired.state, ChangeState::Retired);
        assert_eq!(retired.retire_reason, Some(RetireReason::Fixture));

        let pending = ChangeMeasurement::new(
            ChangeIdentity::new("grep", "crates/", "x"),
            ChangeState::Pending,
        );
        assert_eq!(pending.retire_reason, None);
    }

    /// An undecided row cannot be written without the reason it is undecided,
    /// the same way a retirement cannot be written without its reason: a row
    /// retracted from the outstanding set with nothing to say why would read as
    /// answered.
    #[test]
    #[should_panic(expected = "must be built with ChangeMeasurement::undecided")]
    fn an_undecided_measurement_without_a_reason_cannot_be_built() {
        let _ = ChangeMeasurement::new(
            ChangeIdentity::new("grep", "crates/", "x"),
            ChangeState::Undecided,
        );
    }

    #[test]
    fn an_undecided_measurement_carries_its_reason_and_a_plain_one_carries_none() {
        let undecided = ChangeMeasurement::undecided(
            ChangeIdentity::new("grep", "crates/", "x"),
            "kind_without_predicate",
        );
        assert_eq!(undecided.state, ChangeState::Undecided);
        assert_eq!(
            undecided.unsettled_reason.as_deref(),
            Some("kind_without_predicate")
        );
        assert_eq!(undecided.retire_reason, None);

        let pending = ChangeMeasurement::new(
            ChangeIdentity::new("grep", "crates/", "x"),
            ChangeState::Pending,
        );
        assert_eq!(pending.unsettled_reason, None);
    }

    /// The band order is the one a reader acts on: `P0` outranks `P1`, and a
    /// row nobody prioritised sits below every band rather than above them.
    ///
    /// Written out rather than derived, so this is the gate that says the
    /// written-out order says what the names say -- a derived `Ord` would rank
    /// `P0` by its position in the declaration and answer `P3`.
    #[test]
    fn p0_outranks_every_band_and_unstated_outranks_none() {
        assert!(ChangePriority::P0 > ChangePriority::P1);
        assert!(ChangePriority::P1 > ChangePriority::P2);
        assert!(ChangePriority::P2 > ChangePriority::P3);
        assert!(ChangePriority::P3 > ChangePriority::Unstated);
        assert_eq!(
            ChangePriority::ALL.iter().copied().max(),
            Some(ChangePriority::P0)
        );
        assert_eq!(
            ChangePriority::ALL.iter().copied().min(),
            Some(ChangePriority::Unstated)
        );
    }

    /// Every band round-trips through its name, and the set is closed: a name
    /// outside it is a caller error rather than a sixth band, which is what
    /// lets the column refuse one instead of storing it.
    #[test]
    fn every_band_round_trips_through_its_name() {
        for band in ChangePriority::ALL {
            assert_eq!(
                band.as_str().parse::<ChangePriority>(),
                Ok(band),
                "{} did not parse back",
                band.as_str()
            );
        }
        assert!("urgent".parse::<ChangePriority>().is_err());
        assert_eq!(ChangePriority::ALL.len(), 5);
    }

    /// A source written before the column existed reads back as unstated
    /// instead of failing: those rows were never asked, and refusing to read
    /// them would drop an answer the account already holds.
    #[test]
    fn a_source_that_states_no_band_reads_back_as_unstated() {
        let source: ChangeSource =
            serde_json::from_str(r#"{"doc":"2026-01-01_x.md","verdict":"pending"}"#).unwrap();
        assert_eq!(source.priority, ChangePriority::Unstated);
    }

    /// A row's band is the highest its sources stated, not the first and not
    /// the last: two documents asking for the same change at different bands
    /// are one row whose band is the one to act on.
    #[test]
    fn a_rows_band_is_the_highest_its_sources_stated() {
        let row = |bands: &[ChangePriority]| ChangeLedgerRow {
            identity: ChangeIdentity::new("grep", "crates/", "x"),
            state: ChangeState::Pending,
            title: None,
            assertion: None,
            sources: bands
                .iter()
                .enumerate()
                .map(|(i, band)| ChangeSource {
                    doc: format!("doc{i}.md"),
                    verdict: None,
                    priority: *band,
                    block: ChangeBlock::NONE,
                })
                .collect(),
            first_seen_at: Utc::now(),
            measured_at: None,
            measured_rev: None,
            delivered_rev: None,
            landed_at: None,
            unsettled_reason: None,
            retire_reason: None,
            retired_at: None,
        };
        assert_eq!(
            row(&[ChangePriority::P2, ChangePriority::P0]).priority(),
            ChangePriority::P0
        );
        assert_eq!(row(&[ChangePriority::P3]).priority(), ChangePriority::P3);
        assert_eq!(row(&[]).priority(), ChangePriority::Unstated);
        assert_eq!(
            row(&[ChangePriority::Unstated, ChangePriority::P1]).priority(),
            ChangePriority::P1
        );
    }

    /// A face name round-trips, and a name nobody has written here yet is a
    /// face the moment a document names it: the set of faces is measured from
    /// the account, so a new one needs no edit to this file.
    #[test]
    fn a_block_is_any_face_name_and_a_new_face_needs_no_code_change() {
        for name in [
            "tasks",
            "observability",
            "delivery",
            "a_face_declared_today",
        ] {
            let block = name.parse::<ChangeBlock>().expect("a face name");
            assert_eq!(block.as_str(), name);
            assert!(block.is_named());
            assert_eq!(block.as_str().parse::<ChangeBlock>(), Ok(block));
        }
        // The empty string, `-` and `none` are the answer "no document named a
        // face", which is a member rather than an error.
        assert_eq!("".parse::<ChangeBlock>(), Ok(ChangeBlock::NONE));
        assert_eq!("-".parse::<ChangeBlock>(), Ok(ChangeBlock::NONE));
        assert_eq!("none".parse::<ChangeBlock>(), Ok(ChangeBlock::NONE));
        assert!(!ChangeBlock::NONE.is_named());
        // A shape, not free text: the value travels through SQL, JSONB and
        // metric labels. A Chinese face name is transcribed to its literal by
        // whoever declares it, and a name that needs quoting is refused here
        // rather than stored in a form the three cannot agree on.
        assert!("端到端演练".parse::<ChangeBlock>().is_err());
        assert!("Tasks".parse::<ChangeBlock>().is_err());
        assert!("has space".parse::<ChangeBlock>().is_err());
    }

    /// A row takes the face its documents named, and a disagreement between
    /// them is `none` *and* counted: folding both to `none` without the second
    /// answer would let two documents asking for different things read exactly
    /// like nobody having said.
    #[test]
    fn a_rows_block_is_the_one_its_sources_named_or_a_counted_conflict() {
        let row = |blocks: &[ChangeBlock]| ChangeLedgerRow {
            identity: ChangeIdentity::new("grep", "crates/", "x"),
            state: ChangeState::Pending,
            title: None,
            assertion: None,
            sources: blocks
                .iter()
                .enumerate()
                .map(|(i, block)| ChangeSource {
                    doc: format!("doc{i}.md"),
                    verdict: None,
                    priority: ChangePriority::Unstated,
                    block: block.clone(),
                })
                .collect(),
            first_seen_at: Utc::now(),
            measured_at: None,
            measured_rev: None,
            delivered_rev: None,
            landed_at: None,
            unsettled_reason: None,
            retire_reason: None,
            retired_at: None,
        };
        let token = ChangeBlock::named("token").unwrap();
        let resources = ChangeBlock::named("resources").unwrap();

        let one = row(&[ChangeBlock::NONE, token.clone()]);
        assert_eq!(one.block(), token);
        assert!(!one.block_conflict());

        let silent = row(&[ChangeBlock::NONE, ChangeBlock::NONE]);
        assert_eq!(silent.block(), ChangeBlock::NONE);
        assert!(!silent.block_conflict());

        let split = row(&[token, resources]);
        assert_eq!(split.block(), ChangeBlock::NONE);
        assert!(split.block_conflict());
    }

    /// Standing in front of the rest is a function of two facts the row
    /// already holds, so it stops being true the moment the measurement says
    /// so -- there is no stored band that could keep saying it.
    #[test]
    fn a_delivered_change_that_is_still_owed_holds_the_line() {
        let mut row = ChangeLedgerRow {
            identity: ChangeIdentity::new("grep", "crates/", "x"),
            state: ChangeState::Pending,
            title: None,
            assertion: None,
            sources: Vec::new(),
            first_seen_at: Utc::now(),
            measured_at: None,
            measured_rev: None,
            delivered_rev: Some("abc123".into()),
            landed_at: None,
            unsettled_reason: None,
            retire_reason: None,
            retired_at: None,
        };
        assert!(row.holds_the_line());

        row.delivered_rev = None;
        assert!(
            !row.holds_the_line(),
            "owed but never delivered holds nothing"
        );

        row.delivered_rev = Some("abc123".into());
        row.state = ChangeState::Landed;
        assert!(!row.holds_the_line(), "a landed change holds nothing");
    }
}
