//!Contract for the principle ledger: which rules the project holds, which of
//!them anything actually checks, and how often a check has refused work.
//!
//!A principle is identified by its slug -- the memory's filename without its
//!extension -- and that is the whole identity. Three questions are asked of
//!every row, and the contract keeps them apart because their answers are
//!different facts:
//!
//!  * does a gate exist for it, on either side (`host_gate`, `platform_gate`);
//!  * has a look settled that (`state`, which is allowed to be absent);
//!  * how many times has a gate refused work because of it (`refusals`).
//!
//!The second one is the one that has to be able to stay unsaid. A registry that
//!could not be read, a carrier file that is not in the tree, a registry that
//!could not be parsed: none of those is "no gate", and a contract with only two
//!states would report all three as the debt they are not. So [`PrincipleState`]
//!is two-valued and a row's state is optional, and the look that could not
//!settle says so by reason rather than by a value.
//!
//!The contract is lopsided in the same direction the change ledger's is. There
//!is exactly one way to move a row's state ([`PrincipleLedgerStore::apply_principle_measurement`]),
//!and there is no call anywhere that sets a state on a principle's own behalf.
//!A reading contributes the principle's identity, its sentence, the memory that
//!carries it and the shape of the host's own gate -- never a verdict. That is
//!what keeps "we have a gate for this" from meaning "we say we have a gate for
//!this".

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// What a look at the registries concluded about one principle.
///
/// Two values, and no third: the third answer a reader needs -- "nobody could
/// tell" -- is the absence of this value on the row rather than a variant of it,
/// which is what stops a failed look from being written down as a missing gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipleState {
    /// A gate exists: the host's registry names a check for it, or a contract
    /// test in the platform's own tree declares that it enforces it.
    Enforced,
    /// Both registries were read and neither names it. This is a debt rather
    /// than a silence, which is the distinction the whole ledger is for.
    Unenforced,
}

impl PrincipleState {
    /// Every value the column may take, so a census can report the zeroes.
    pub const ALL: [PrincipleState; 2] = [PrincipleState::Enforced, PrincipleState::Unenforced];

    pub fn as_str(self) -> &'static str {
        match self {
            PrincipleState::Enforced => "enforced",
            PrincipleState::Unenforced => "unenforced",
        }
    }

    /// The value a stored string names, or `None` for anything else.
    ///
    /// No fallback: a string this does not know is a row written by something
    /// that disagrees with this contract, and reading it as `unenforced` would
    /// turn that disagreement into a debt against the principle.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.as_str() == value)
    }
}

impl std::fmt::Display for PrincipleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a principle is meant to be checked, when that has been said.
///
/// The ledger's other questions are all about whether a gate *exists*. This one
/// is about whether one *should*, and the two have to be kept apart: a principle
/// nobody machine-checks and a principle no machine can check both read as
/// `unenforced`, and only the first is work. Folding them together makes "how much
/// is ungated" a number that can never fall -- every rule that is a person's to
/// hold counts as a debt forever -- so the tier is stored and the debt reading can
/// subtract it.
///
/// Four values, and the three "tiers" a reader groups them into (`shape` |
/// `tree` + `live` | `judgement`) are the grouping, not the closed set: the tier a
/// value belongs to is a presentation, and a presentation stored as the identity
/// would have to be migrated whenever it was redrawn.
///
/// The enum is optional wherever it appears. A principle whose tier nobody has
/// declared has none, and every value here is a claim about how the principle is
/// checked that a reading has to make on purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipleCheckKind {
    /// The sentence or command is recognisable as written, so a check can run
    /// before the act -- a session-side hook, shipped in the image.
    Shape,
    /// The answer needs one walk of the tree, so it is read after the fact, at a
    /// revision.
    Tree,
    /// The answer needs a reading off the running system, read after the fact
    /// against the same revision.
    Live,
    /// No machine decides it. A person does, and the row is the record that it is
    /// a person's to decide -- never a debt, and never a gate.
    Judgement,
}

impl PrincipleCheckKind {
    /// Every value the column may take, so a census can report the zeroes.
    pub const ALL: [PrincipleCheckKind; 4] = [
        PrincipleCheckKind::Shape,
        PrincipleCheckKind::Tree,
        PrincipleCheckKind::Live,
        PrincipleCheckKind::Judgement,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            PrincipleCheckKind::Shape => "shape",
            PrincipleCheckKind::Tree => "tree",
            PrincipleCheckKind::Live => "live",
            PrincipleCheckKind::Judgement => "judgement",
        }
    }

    /// The value a stored string names, or `None` for anything else.
    ///
    /// No fallback, for the same reason the state column has none: a string this
    /// does not know is a row written by something that disagrees with this
    /// contract, and reading it as any of the four would invent a tier.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_str() == value)
    }

    /// Whether a resident loop is the thing that measures this tier.
    ///
    /// The tree and live tiers are read after the fact against a revision, which
    /// is what the loop does; `shape` is decided before the act by the session,
    /// and `judgement` is nobody's but a person's. Kept as a method rather than at
    /// each call site so the routing cannot disagree with itself.
    pub fn measured_by_resident_loop(self) -> bool {
        matches!(self, PrincipleCheckKind::Tree | PrincipleCheckKind::Live)
    }
}

impl std::fmt::Display for PrincipleCheckKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One principle as a host-side reading names it.
///
/// The fields are exactly the columns of `list-principles.sh --tsv`, which is
/// the producer's only parse of the memories' own frontmatter: a second parser
/// of the same frontmatter would drift from this one, and the drifting one is
/// always the reader's.
///
/// No state, and no way to carry one. A reading says the principle exists, what
/// it says, which memory carries it and what the host's registry holds for it;
/// whether anything enforces it is decided by a look at both registries, which
/// is a separate act with a separate entry point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleReading {
    pub slug: String,
    /// Which reading named it, so a re-push is idempotent and a principle named
    /// by two readings keeps both citations.
    pub source: String,
    /// The rule's own sentence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lead: Option<String>,
    /// The memory that carries the principle.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carrier_path: Option<String>,
    /// The bytes of that memory, hashed. Stored rather than compared and
    /// dropped: a hash that differs from the stored one is the drift reading --
    /// the rule's text changed and the gate did not follow -- and that is only
    /// sayable while the previous hash is still there.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carrier_sha256: Option<String>,
    /// The shape of the check the host's own registry runs for it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_gate: Option<String>,
    /// How this principle is meant to be checked, when the reading knows.
    ///
    /// Declared rather than measured: it says where the check *belongs*, which is
    /// a decision about the rule, not a finding about the tree. `None` is a
    /// reading that says nothing about the tier, and it does not move a tier
    /// already on the row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_kind: Option<PrincipleCheckKind>,
}

/// The result of looking at both registries for one principle.
///
/// Carries no timestamp: the store stamps `measured_at` from its own clock as it
/// writes, so the moment a row is recorded as settled is the moment the look was
/// applied rather than a time the caller chose. The revision is the caller's and
/// has to be -- it is not a time but the tree that was looked at, and only the
/// look knows which tree it opened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleMeasurement {
    pub slug: String,
    pub state: PrincipleState,
    /// The contract test in the platform's own tree that declares it enforces
    /// this principle, as the look found it. Written with the verdict rather
    /// than separately, so the columns a verdict was drawn from agree with it
    /// by construction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_gate: Option<String>,
    /// What the look saw, in prose.
    ///
    /// Carried by the look rather than stored: the columns a state was drawn
    /// from are written beside it, so what has to be arguable with is already on
    /// the row, and a rendered note nothing reads is not a reading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation: Option<String>,
    /// The revision the tree was read at, or `None` when the look had none to
    /// name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measured_rev: Option<String>,
}

impl PrincipleMeasurement {
    pub fn new(slug: impl Into<String>, state: PrincipleState) -> Self {
        Self {
            slug: slug.into(),
            state,
            platform_gate: None,
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

/// One row of the account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleRow {
    pub slug: String,
    /// The rule's own sentence, as the reading named it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lead: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carrier_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carrier_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_gate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_gate: Option<String>,
    /// How the principle is meant to be checked, when a reading has declared it.
    ///
    /// `None` until one does, and it stays `None` for a principle nobody has
    /// classified: an undeclared tier and a declared `judgement` are different
    /// findings, and only the latter is the record that the rule is a person's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_kind: Option<PrincipleCheckKind>,
    /// `None` until a look settles it, and it stays `None` for a look that could
    /// not settle -- a row nothing has looked at and one no look could reach are
    /// the same reading here, and both are different from `unenforced`.
    pub state: Option<PrincipleState>,
    /// How many times a gate has refused work because of this principle.
    pub refusals: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_refused_at: Option<DateTime<Utc>>,
    pub first_seen_at: DateTime<Utc>,
    /// When it was last measured. `None` while nothing has settled it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measured_at: Option<DateTime<Utc>>,
    /// The revision that measurement read, when it named one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measured_rev: Option<String>,
}

/// One gate refusal, as the side that refused it reports it.
///
/// `count` is how many refusals in that second came from that source: an event
/// keyed by its instant alone would collapse two refusals inside one second into
/// one, and a counter that under-counts silently is worse than no counter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleRefusal {
    pub slug: String,
    pub refused_at: DateTime<Utc>,
    /// What refused: a hook, a gate, a session. Kept so the count can be read
    /// by source rather than only in total.
    pub source: String,
    pub count: i64,
}

/// A principle whose carrier is no longer the text the account measured against.
///
/// Named by slug with both hashes, because a bare count of drifted principles
/// gives a reader nothing to look at: the pair is what says which memory moved
/// and whether the gate followed it. A drift is a fact about the rule's text, not
/// a verdict about the gate, so it carries no state of its own and none is
/// written for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleDrift {
    pub slug: String,
    /// The hash the account held, which is what the last measurement was
    /// against.
    pub was: String,
    /// The hash the reading just pushed.
    pub now: String,
}

/// What one fold did.
///
/// Both halves are reported because they answer different questions: how far the
/// account caught up (`folded`), and which of the principles it caught up on
/// arrived with a different carrier than the one they were measured against
/// (`drifted`). Only the first is progress; the second is work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleFold {
    pub folded: usize,
    pub drifted: Vec<PrincipleDrift>,
}

/// How many rows the account holds, by state.
///
/// The unmeasured rows are a field rather than a third state, because they are
/// not one: a row whose state is unset is a row no look has settled, and folding
/// it into `unenforced` would report every principle nobody has got to yet as a
/// principle with no gate. Both are work, and they are not the same work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleCensus {
    pub enforced: u64,
    pub unenforced: u64,
    pub unmeasured: u64,
}

impl PrincipleCensus {
    pub fn total(&self) -> u64 {
        self.enforced + self.unenforced + self.unmeasured
    }

    /// The share of the account a gate exists for, or `None` when it holds
    /// nothing.
    ///
    /// `None` rather than zero: an account with no rows has no coverage, and a
    /// zero would read as an account whose principles are all ungated.
    pub fn coverage(&self) -> Option<f64> {
        match self.total() {
            0 => None,
            total => Some(self.enforced as f64 / total as f64),
        }
    }
}

/// How many rows the account holds by the tier that checks them, the undeclared
/// ones counted apart.
///
/// The tier a row is on is the only thing that says why a row is ungated: a
/// `judgement` row has no gate because none is meant to exist, the three machine
/// tiers are ungated because nobody has gated them yet, and a row with no tier is
/// one nobody has said anything about. Folding the first into the debt makes the
/// ungated count a number that can never fall; folding the last into it charges
/// the account for a tier it was never told. So the count is taken here, keyed the
/// way the column is -- four values, not the three tiers a reader groups them
/// into, because the grouping is a presentation and this is a reading.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleCheckKindCensus {
    pub shape: u64,
    pub tree: u64,
    pub live: u64,
    pub judgement: u64,
    pub undeclared: u64,
}

impl PrincipleCheckKindCensus {
    /// How many rows carry a tier at all.
    pub fn declared(&self) -> u64 {
        self.total() - self.undeclared
    }

    pub fn total(&self) -> u64 {
        self.shape + self.tree + self.live + self.judgement + self.undeclared
    }
}

/// A slug a registry declares that the account has no row for.
///
/// Reported by name rather than by count, because the two things that produce it
/// are opposite acts: a mistyped slug is a defect in the declaration and has to
/// be fixed, while a principle the account has never been told about is a gap in
/// the reading. A total of one cannot tell them apart, and the work they call
/// for is not the same work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleOrphan {
    pub slug: String,
    /// The file that declares it.
    pub source: String,
}

/// Durable storage for the principle ledger.
///
/// The only way to move a row's state is
/// [`Self::apply_principle_measurement`]. Rows enter through the reading
/// pipeline -- a host-side list, folded in by the ledger's own writer -- which
/// is a separate surface and never touches a state.
///
/// The reading pipeline has two steps and the storage keeps them apart, for the
/// same reason the change ledger's does: a reading can be queued with
/// [`Self::declare`] by anyone who has news, and it enters the account only when
/// the writer folds it with [`Self::fold_declarations`]. The queue is durable
/// rather than in-process because the two steps happen at different times and in
/// different processes, and a handover that only ever existed in memory is one
/// the ledger cannot be shown to have received.
#[async_trait]
pub trait PrincipleLedgerStore: Send + Sync {
    /// Add or refresh what a reading contributes: the identity, the sentence,
    /// the carrier and its hash, and the shape of the host's own gate.
    ///
    /// Never writes `state`, `measured_at` or `measured_rev`: a reading is not a
    /// measurement, and a principle cannot become enforced by being described as
    /// enforced. Rows the reading does not mention are left alone -- the ledger
    /// outlives any one reading of the corpus, and a reading that omitted a row
    /// because a memory was renamed is not a statement that the principle is
    /// gone.
    ///
    /// The carrier hash moves to the pushed value, and the difference it made is
    /// read by [`Self::fold_declarations`] *before* this write rather than by
    /// holding the old hash here. Holding it would keep the difference visible
    /// forever: the drift would never clear, and the principle would never be
    /// measured against its own current text again.
    async fn record_readings(&self, readings: &[PrincipleReading]) -> SFResult<usize>;

    /// Queue readings for the ledger's own writer to fold in.
    ///
    /// Idempotent per (slug, source): re-running the same list queues nothing
    /// twice, so a push that failed part way can simply be repeated. A reading
    /// that says something the queued one did not **re-queues it** -- a reading
    /// taken before a column existed files a value the later corrected one has
    /// to be able to replace -- and a field a reading carries nothing for keeps
    /// what is stored rather than clearing it. Answers how many readings were
    /// queued or re-queued; the ones already saying exactly this are not counted.
    async fn declare(&self, readings: &[PrincipleReading]) -> SFResult<usize>;

    /// Fold up to `limit` unfolded readings into the account, and answer how
    /// many were folded and which of them drifted.
    ///
    /// Declarations are marked folded *after* their reading is written, so a
    /// fold interrupted in between is repeated rather than lost.
    ///
    /// This is where drift is read, because this is the only place both hashes
    /// exist at once: the pushed one is in hand and the held one is still on the
    /// row, one statement before the write replaces it.
    async fn fold_declarations(&self, limit: usize) -> SFResult<PrincipleFold>;

    /// How many readings are queued and not yet folded: whether "the host told
    /// the platform" actually happened.
    async fn pending_declarations(&self) -> SFResult<u64>;

    /// Apply one measurement: move the row's state, write the platform-side gate
    /// column the verdict was drawn from, and stamp `measured_at` and
    /// `measured_rev`.
    ///
    /// A measurement of a slug the account does not have creates the row: the
    /// account is defined by what has been measured rather than by what has been
    /// read about.
    async fn apply_principle_measurement(&self, measurement: &PrincipleMeasurement)
        -> SFResult<()>;

    /// The rows that are not `enforced`, stalest look first.
    ///
    /// `None` in `measured_at` sorts first: nothing has looked at those, which
    /// is less known than a stale look, not more current.
    async fn outstanding(&self, limit: usize) -> SFResult<Vec<PrincipleRow>>;

    /// The rows a gate has refused work because of, most recently refused first.
    ///
    /// A reading of the same table as [`Self::outstanding`] and a different
    /// question: that one asks which rules no gate enforces, this one asks which
    /// rules a gate has had to stop something for. The two are independent -- a
    /// row can be enforced and refused at once, and a row with no gate at all is
    /// refused by nothing -- which is why this is a method and not a field of
    /// [`PrincipleCensus`]. A count folded into that census would sit in two of
    /// its buckets at once and make `total` a number no reader could add up, which
    /// is the same reason the tiers have a census of their own.
    ///
    /// `refusals > 0` is the whole selector, and deliberately not a window of
    /// recent ones. The column counts what was reported; a second bound written
    /// here would be a second answer to "how much of the corpus has been
    /// refused", and one that moves when the clock does rather than when a gate
    /// refuses something.
    async fn refused(&self, limit: usize) -> SFResult<Vec<PrincipleRow>>;

    /// One row, by the slug that names it.
    async fn get(&self, slug: &str) -> SFResult<Option<PrincipleRow>>;

    /// Every slug the account holds.
    ///
    /// Read as a set rather than as rows because that is how the reverse
    /// reconciliation uses it: a registry declaration whose slug is not in here
    /// is an orphan, and comparing identities is all that question needs.
    async fn slugs(&self) -> SFResult<Vec<String>>;

    /// How many rows the ledger holds, by state, with the rows no look has
    /// settled counted apart from both.
    ///
    /// Every count is present, the zeroes included: a reader comparing two
    /// censuses needs to see a state that emptied, and an absent field is not a
    /// reading.
    async fn census(&self) -> SFResult<PrincipleCensus>;

    /// How many rows the ledger holds by the tier that checks them, with the
    /// rows whose tier nobody has declared counted apart from all four.
    ///
    /// The reading that says why a row is ungated: a `judgement` row is ungated
    /// because none is meant to exist, and subtracting it is what makes "how much
    /// is ungated" a number work can move. Every count is present, the zeroes
    /// included, for the same reason the state census names them.
    async fn check_kind_census(&self) -> SFResult<PrincipleCheckKindCensus>;

    /// Queue gate refusals for the writer to count.
    ///
    /// The side that refused work cannot add to `refusals` itself: that would
    /// make it a second writer of the account, and a count written by the party
    /// it counts is the number this row exists to replace. Idempotent per
    /// (slug, refused_at, source), so a reporter that cannot tell whether its
    /// last report landed may repeat it.
    ///
    /// Answers how many reports changed what the queue holds. A repeat of a fact
    /// already queued changes nothing and is not counted, which is how a caller
    /// tells "the platform now knows" from "the platform already knew" without
    /// reading the queue back.
    async fn report_refusals(&self, events: &[PrincipleRefusal]) -> SFResult<usize>;

    /// Add up to `limit` unfolded refusals into their principles' counts, and
    /// answer how many events were folded.
    async fn fold_refusals(&self, limit: usize) -> SFResult<usize>;

    /// How many refusal events are queued and not yet folded.
    async fn pending_refusals(&self) -> SFResult<u64>;
}
