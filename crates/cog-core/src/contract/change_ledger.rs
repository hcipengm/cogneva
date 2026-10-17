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
}

impl RetireReason {
    /// Every reason, in the order the readings group them.
    pub const ALL: [RetireReason; 4] = [
        RetireReason::Superseded,
        RetireReason::Fixture,
        RetireReason::OutOfScope,
        RetireReason::SurfaceMoved,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RetireReason::Superseded => "superseded",
            RetireReason::Fixture => "fixture",
            RetireReason::OutOfScope => "out_of_scope",
            RetireReason::SurfaceMoved => "surface_moved",
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
    fn rank(self) -> u8 {
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
    /// Derived from the sources on every read rather than stored in a column of
    /// its own. A stored band would be a second answer to a question the
    /// sources already answer, and the two would drift the first time a
    /// declaration was corrected -- the reading path updates the source, and
    /// nothing would update the copy.
    pub fn priority(&self) -> ChangePriority {
        self.sources
            .iter()
            .map(|source| source.priority)
            .max()
            .unwrap_or_default()
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
        }
    }

    /// The same measurement, naming the revision it was taken against.
    pub fn at_revision(mut self, revision: impl Into<String>) -> Self {
        self.measured_rev = Some(revision.into());
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

    /// The rows that are still outstanding, stalest measurement first.
    ///
    /// Both terminal states are left out, [`ChangeState::Landed`] and
    /// [`ChangeState::Retired`]: a row whose question is answered is not work,
    /// and a reader that took "not landed" for "outstanding" would hand a retired
    /// row back to the tree and overwrite the retirement with whatever the tree
    /// says -- which for an identity that is not an artifact is the unsettled
    /// answer this state exists to stop repeating.
    ///
    /// `None` in `measured_at` sorts first: nothing has looked at those, which
    /// is less known than a stale measurement, not more current.
    async fn outstanding(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>>;

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
    async fn landed_due(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>>;

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
                })
                .collect(),
            first_seen_at: Utc::now(),
            measured_at: None,
            measured_rev: None,
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
}
