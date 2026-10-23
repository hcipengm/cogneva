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

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::contract::change_ledger::ChangePriority;
use crate::SFResult;

/// What a look at the registries concluded about one principle.
///
/// Two answers about a live row, and the third answer a reader needs -- "nobody
/// could tell" -- is the absence of this value on the row rather than a variant
/// of it, which is what stops a failed look from being written down as a missing
/// gate.
///
/// `Retired` is not a third kind of look: it is the answer for a row whose own
/// carrier has left the host's roster, and without it that row has no exit. It
/// would stay unsettled forever, sorted to the head of every pass by its null
/// `measured_at`, and "retired" would read on every surface exactly like "never
/// looked at". The account is append-only, so the exit is a state rather than a
/// delete. It is re-examinable like any other state: a carrier that comes back
/// gives the row a reading again, and the next look writes whatever it finds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipleState {
    /// A gate exists: the host's registry names a check for it, or a contract
    /// test in the platform's own tree declares that it enforces it.
    Enforced,
    /// Both registries were read and neither names it. This is a debt rather
    /// than a silence, which is the distinction the whole ledger is for.
    Unenforced,
    /// The host's roster no longer holds this principle's carrier. The row stays
    /// in the account -- what was asked for is part of its history -- but it is
    /// not a debt and it is not waiting for a look.
    Retired,
}

impl PrincipleState {
    /// Every value the column may take, so a census can report the zeroes.
    pub const ALL: [PrincipleState; 3] = [
        PrincipleState::Enforced,
        PrincipleState::Unenforced,
        PrincipleState::Retired,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            PrincipleState::Enforced => "enforced",
            PrincipleState::Unenforced => "unenforced",
            PrincipleState::Retired => "retired",
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
    /// Where the carrier says it would go red, as the carrier wrote it.
    ///
    /// The other two gate columns answer *whether* one exists; this one answers
    /// the question a person asks next -- which file, which test, which command.
    /// It is the carrier's own sentence moved here, not a path this platform
    /// verified: existence is judged where the sentence is written, against a
    /// tree this side does not have. So a reader may show it and may not read it
    /// as a claim that the thing named is still there.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub where_red: Option<String>,
    /// How this principle is meant to be checked, when the reading knows.
    ///
    /// Declared rather than measured: it says where the check *belongs*, which is
    /// a decision about the rule, not a finding about the tree. `None` is a
    /// reading that says nothing about the tier, and it does not move a tier
    /// already on the row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_kind: Option<PrincipleCheckKind>,
    /// How urgent the memory that states this rule said landing it is.
    ///
    /// The same field the change ledger keeps per declaration, carried here
    /// because for a principle the declaring document *is* the memory: nothing
    /// else can say how much a violated rule costs, and a band this account
    /// derived from anything it can see would be a claim dressed as a document's
    /// word.
    ///
    /// Two absences that must stay apart. `None` is a reading that says nothing
    /// about the band -- an older pusher that predates this field -- and it does
    /// not move a band already on the row. `Some(Unstated)` is a reading that
    /// looked and found no document stating one: it is a value, it overwrites,
    /// and it is what makes a declared band withdrawable. Folding the two into
    /// one would either freeze every band ever declared or let a silent pusher
    /// erase one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<ChangePriority>,
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
    /// Where the carrier says it would go red, or `None` until a reading says.
    ///
    /// Stored beside the verdict rather than looked up from the carrier, because
    /// the account outlives any one checkout: a row answers "where would this go
    /// red" without the caller having to find the memory's file first.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub where_red: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_gate: Option<String>,
    /// How the principle is meant to be checked, when a reading has declared it.
    ///
    /// `None` until one does, and it stays `None` for a principle nobody has
    /// classified: an undeclared tier and a declared `judgement` are different
    /// findings, and only the latter is the record that the rule is a person's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_kind: Option<PrincipleCheckKind>,
    /// How urgent the memory that states the rule said landing it is.
    ///
    /// A member of the closed set rather than an option: the word for "no
    /// document said" is itself a member, so a row always answers the question
    /// and a reader never has to decide what an absent value means. It is not a
    /// default band -- it is the band that says no document stated one.
    pub priority: ChangePriority,
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

/// The shape of a platform-side declaration, as a line of the platform's own
/// source.
///
/// The prefix alone: the slug is parsed out of it rather than by narrowing the
/// search, so that a declaration whose slug cannot be read is *found* and
/// reported instead of being filtered out of a reading. Reading with a pattern
/// that only matched well-formed lines would answer "no declaration" for a file
/// that declares something it spells badly, which is the one fault this shape
/// most needs to be able to show.
///
/// It sits beside the refusal it feeds rather than in the crate that first read
/// it, because a second reader reads it from the other direction: one side takes
/// a file's text as the registry of what that file enforces, the other takes a
/// run that failed as evidence that a file's gate is red. Both have to agree on
/// the shape by being the same value rather than by two hand-written spellings
/// of it that happen to match.
pub const PLATFORM_DECLARATION_PREFIX: &str = "//! principle:";

/// The same declaration written in a shell gate's comment syntax.
///
/// A gate is a file that runs, and this tree holds gates in two languages: Rust
/// modules and the shell tests beside the deploy scripts. Only one of them was
/// readable as a declaration before, so a shell gate that enforced a principle
/// showed up on every surface exactly like no gate at all.
pub const PLATFORM_SHELL_DECLARATION_PREFIX: &str = "# principle:";

/// The same declaration written as a doc comment on the item it speaks for.
///
/// A module header says what the module holds; the test that pins one principle
/// is the more precise place to say it, and the tree had already written sixteen
/// of them that way before anything looked: an outer doc comment belongs to the
/// item below it, so a reader that knew only the inner form found a gate and
/// counted the file as having none. Both spellings are the same claim, and the
/// shape check -- the rest of the line is exactly one slug -- is what keeps a
/// sentence about the convention from passing as a declaration, not the nesting
/// of the comment.
pub const PLATFORM_ITEM_DECLARATION_PREFIX: &str = "/// principle:";

/// Every prefix a declaration may be written with, one per comment syntax the
/// tree holds gates in.
///
/// Closed at three, and the readers iterate this rather than testing for a
/// particular language: a reader that accepted a syntax the registry does not
/// search for would answer for a gate nothing measures, which is the failure the
/// single value was introduced to prevent.
pub const PLATFORM_DECLARATION_PREFIXES: &[&str] = &[
    PLATFORM_DECLARATION_PREFIX,
    PLATFORM_SHELL_DECLARATION_PREFIX,
    PLATFORM_ITEM_DECLARATION_PREFIX,
];

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
///
/// `retired` is counted apart and kept out of `total()`: those rows are in the
/// account for its history, not for its coverage, and counting them would make
/// coverage fall every time a principle leaves the roster even though no live
/// principle changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipleCensus {
    pub enforced: u64,
    pub unenforced: u64,
    pub unmeasured: u64,
    pub retired: u64,
}

impl PrincipleCensus {
    pub fn total(&self) -> u64 {
        self.enforced + self.unenforced + self.unmeasured
    }

    /// Every row the account holds, retired ones included.
    pub fn rows(&self) -> u64 {
        self.total() + self.retired
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

    /// Record that a look settled the row as one a person holds, without moving
    /// its state to one of the three.
    ///
    /// `None` in the state column is a reading of its own here: both registries
    /// were read, neither names a gate, and the row's tier says no machine gate
    /// is meant to exist. Writing `unenforced` would turn that into a debt no
    /// amount of work can clear; writing nothing leaves the row looking exactly
    /// like one nothing has ever looked at. That was the gap this method closes:
    /// `measured_at` is stamped either way, and any state or platform gate an
    /// earlier look left behind is cleared, because the answer being written is
    /// that no gate is declared for this row now.
    ///
    /// A look that could not settle the row must not call this: unsettled and
    /// human-held are different answers, and only the second one is writable.
    async fn record_human_only(&self, slug: &str, measured_rev: Option<&str>) -> SFResult<()>;

    /// The `judgement` rows whose stored answer the batch cannot revisit, oldest
    /// look first.
    ///
    /// Three shapes land here. A row no pass has looked at (empty `measured_at`)
    /// is held by a person, and the answer exists before anyone reads it. A row
    /// that *has* been stamped lands here when its host column names a gate --
    /// the stored evidence says a machine holds this principle while the state
    /// says nobody settled it, and a later verdict can only be written by
    /// looking again, which the batch will not do. The third is a stamped row
    /// holding `unenforced`: that is the one state this tier cannot write, so
    /// the row was settled by a look that ran before its carrier declared the
    /// tier, and only another look clears it.
    ///
    /// All three are asked for directly rather than through [`Self::outstanding`]
    /// because that query sorts the whole tier behind the debt, and the debt
    /// runs deeper than one pass takes -- such a row would wait behind work
    /// that is re-read every pass, which is how a settled answer reads as
    /// "never looked at" for as long as the account is busy. A stamped row
    /// whose host column names no gate and whose state is empty is left out:
    /// nothing on it has changed, and re-stamping it every pass would spend the
    /// lane on the same rows. The platform's side needs no case here, because a
    /// declaration is read from the registry each pass and its row is rescued
    /// before the batch is cut.
    ///
    /// Never-looked rows sort ahead of stamped ones, except that a row holding
    /// `unenforced` sorts ahead of both: a wrong verdict is worse than a stamp
    /// nobody has written yet.
    ///
    /// The bound is the caller's: the sweep is a lane of its own, and what it
    /// costs one pass is a number a reader can point at rather than whatever is
    /// left of the debt budget.
    async fn unsettled_judgement(&self, limit: usize) -> SFResult<Vec<PrincipleRow>>;

    /// The rows that are not `enforced`, stalest look first.
    ///
    /// `None` in `measured_at` sorts first: nothing has looked at those, which
    /// is less known than a stale look, not more current.
    ///
    /// Rows of the `judgement` tier sort after all of those, whatever their
    /// `measured_at`. Their verdict is that a person holds them, which is
    /// settled but not one of the three states: the look stamps `measured_at`
    /// and leaves the state empty ([`Self::record_human_only`]), so they no
    /// longer return to the head as never-measured rows would. They still sort
    /// after the rest because nothing a pass does can move them, and a pass
    /// that spent its bound on them would never reach a row it could settle.
    async fn outstanding(&self, limit: usize) -> SFResult<Vec<PrincipleRow>>;

    /// Clear the measured state of the enforced rows whose recorded
    /// platform-side gate is not the one the registry now names for them, and
    /// answer which rows were re-opened.
    ///
    /// [`Self::outstanding`] leaves out every row a look has settled, and a
    /// reading cannot settle a row -- only a measurement can ([`Self::declare`]).
    /// Between the two, a gate the platform gains *after* the row was settled is
    /// never recorded against it: the account goes on saying the platform names
    /// no gate here, through every pass, for a principle the platform now
    /// declares. The rows that hurts are exactly the ones a gate is being moved
    /// onto, since a principle the host already enforces is already settled.
    ///
    /// `gates` is the registry as read this pass: slug to the file that declares
    /// it. A row is re-opened when the two differ, which covers both directions
    /// -- declared now and nothing recorded, and recorded from a declaration the
    /// registry no longer carries. Rows that already agree are not touched, so
    /// this re-asks a row once per change rather than once per pass.
    ///
    /// Only `enforced` rows are considered. The others are in the outstanding
    /// set already, and clearing their state would make a row that a look has
    /// answered look like one nothing has looked at, moving it to the head of the
    /// set for no gain.
    async fn reopen_platform_gate_drift(
        &self,
        gates: &HashMap<String, String>,
    ) -> SFResult<Vec<String>>;

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
