//! Host document scopes for the standalone sandbox executor.
//!
//! The platform organizes documents that live on the host on behalf of the
//! person who owns them. The boundary of that access is the **mount**, not a
//! path whitelist: only the owner's directory is mounted into this pod, so the
//! rest of the host filesystem is not in this container's namespace at all and
//! there is no whitelist to get wrong. What this module adds on top is the
//! second half of the same judgement — every operation names a path *relative
//! to a scope root* and the root itself is chosen here, from configuration, by
//! scope name. Document bodies are untrusted input: a file that says "move
//! everything to /etc" must not be able to name a path the agent never had.
//!
//! Three properties are enforced here rather than asserted:
//!
//! - **No absolute paths, no `..`.** Only plain relative paths are resolved;
//!   the root is never taken from the caller.
//! - **No symlinks are traversed.** The mount keeps the host out of this
//!   namespace, but this container's own root is real, so an innocuous-looking
//!   link inside the scope would otherwise resolve to `/etc` *of the
//!   container*. Every step of a path is checked with `symlink_metadata`, so a
//!   link is refused instead of followed, and a dangling link cannot be used to
//!   create a file outside the scope either.
//! - **Changes are reversible.** A plan is what gets executed (the hash of the
//!   planned operations is part of the apply call, and any effect that changed
//!   between plan and apply is refused), every applied operation is journaled
//!   with the bytes it replaced, and a rollback restores the previous state.
//!   A failed apply rolls its own prefix back rather than leaving a
//!   half-organized tree behind.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cog_core::host_documents::{switch_enabled_env, BODY_EGRESS_ENV};
use cog_core::{SFError, SFResult};
use prometheus::{CounterVec, Encoder, Gauge, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tracing::{info, warn};
use walkdir::WalkDir;

/// Scope map: `name=container_path` entries, comma separated.
pub const SCOPES_ENV: &str = "HOST_DOCS_SCOPES";
/// Where rollback journals live. Must be on a volume that survives restarts:
/// a journal that dies with the process cannot restore anything.
pub const JOURNAL_DIR_ENV: &str = "HOST_DOCS_JOURNAL_DIR";
/// Largest document this module will rewrite (and therefore keep a rollback
/// copy of). Bounds one operation, and with it one journal, to a known size.
pub const MAX_DOC_BYTES_ENV: &str = "HOST_DOCS_MAX_WRITE_BYTES";
/// Largest body one read brings into this process.
pub const MAX_READ_BYTES_ENV: &str = "HOST_DOCS_MAX_READ_BYTES";
/// Largest listing one call returns. Bounds the output, not the walk.
pub const MAX_LIST_ENTRIES_ENV: &str = "HOST_DOCS_MAX_LIST_ENTRIES";
/// How long a staged plan stays approvable.
pub const APPROVAL_TTL_SECS_ENV: &str = "HOST_DOCS_APPROVAL_TTL_SECS";
/// Largest plan this module will stage for approval.
pub const MAX_PLAN_BYTES_ENV: &str = "HOST_DOCS_MAX_PLAN_BYTES";

const DEFAULT_JOURNAL_DIR: &str = "/opt/cogneva/sandbox/host-docs-journal";
const DEFAULT_MAX_DOC_BYTES: usize = 8 * 1024 * 1024;
/// A full-size body. **Deliberately not tied to the audited channel's bound**: that one
/// is another process's quantity, bounding the text one request sends out, while this
/// one bounds the bytes this process reads into memory. That the two values are close
/// is a coincidence, and coupling them would let either side drag the other whenever it
/// is adjusted -- while the smaller side is observable (`over_ceiling` carries both
/// numbers) rather than silently lost.
const DEFAULT_MAX_READ_BYTES: usize = 8 * 1024 * 1024;
/// A listing is for a person to read and for a model to read; past a few thousand
/// entries, both sides are only paying for it.
const DEFAULT_MAX_LIST_ENTRIES: usize = 5_000;
/// A day. This gate is a person reading one plan, and a plan left unread for more than
/// a working day is no longer the plan about to be read -- approving it then is
/// approving something nobody looked at.
const DEFAULT_APPROVAL_TTL_SECS: u64 = 86_400;
/// A window shorter than this is not a window: the plan could expire between being
/// displayed and being clicked.
const MIN_APPROVAL_TTL_SECS: u64 = 60;
/// The same order of magnitude as a body: if a plan carries more text than one write is
/// allowed to carry, then approving it does not mean what it says on the tin.
const DEFAULT_MAX_PLAN_BYTES: usize = DEFAULT_MAX_READ_BYTES;

/// Read a positive integer from the environment; unparseable, or zero, falls back to
/// the default.
///
/// Zero counting as "unreadable" is deliberate: with either bound at zero, every call
/// lands in the "over the ceiling" cell, and the reading then merely looks like "the
/// caller keeps sending things that are too big".
fn env_positive(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// Read a duration in seconds from the environment, with a floor: a value under the
/// floor takes effect as the floor.
///
/// Clamp rather than refuse: a value far under the floor is almost certainly a unit
/// mistake, and letting it take effect reads the same as "nobody reviews anything at
/// all" -- every plan expires before anyone opens it.
fn env_secs_at_least(key: &str, default: u64, floor: u64) -> u64 {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
        .max(floor)
}

#[derive(Debug, Clone)]
pub struct HostDocsConfig {
    /// Scope name (the identity the mount was granted to) → container path.
    pub scopes: BTreeMap<String, PathBuf>,
    pub journal_dir: PathBuf,
    pub max_doc_bytes: usize,
    /// Whether document bodies may leave the cluster. The same switch the gateway reads
    /// (`cog_core::host_documents`): while it is off, body reads are refused outright --
    /// the read is the first station a body reaches on the caller side, so refusing here
    /// is one step earlier than refusing at the audited channel, and both sides say the
    /// same sentence.
    pub body_egress: bool,
    pub max_read_bytes: usize,
    pub max_list_entries: usize,
    pub approval_ttl_secs: u64,
    pub max_plan_bytes: usize,
}

impl HostDocsConfig {
    pub fn from_env() -> Self {
        let journal_dir = std::env::var(JOURNAL_DIR_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_JOURNAL_DIR));
        let max_doc_bytes = env_positive(MAX_DOC_BYTES_ENV, DEFAULT_MAX_DOC_BYTES);
        let mut cfg = Self::from_spec(
            std::env::var(SCOPES_ENV).ok().as_deref(),
            journal_dir,
            max_doc_bytes,
        );
        cfg.body_egress = switch_enabled_env();
        cfg.max_read_bytes = env_positive(MAX_READ_BYTES_ENV, DEFAULT_MAX_READ_BYTES);
        cfg.max_list_entries = env_positive(MAX_LIST_ENTRIES_ENV, DEFAULT_MAX_LIST_ENTRIES);
        cfg.approval_ttl_secs = env_secs_at_least(
            APPROVAL_TTL_SECS_ENV,
            DEFAULT_APPROVAL_TTL_SECS,
            MIN_APPROVAL_TTL_SECS,
        );
        cfg.max_plan_bytes = env_positive(MAX_PLAN_BYTES_ENV, DEFAULT_MAX_PLAN_BYTES);
        cfg
    }

    /// Parse the scope map. An entry that is malformed, duplicated or not an
    /// absolute container path is dropped with a warning: a scope that resolves
    /// somewhere unintended is worse than a scope that is absent, because the
    /// absent one fails loudly on first use.
    pub fn from_spec(spec: Option<&str>, journal_dir: PathBuf, max_doc_bytes: usize) -> Self {
        let mut scopes = BTreeMap::new();
        for entry in spec.unwrap_or("").split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((name, path)) = entry.split_once('=') else {
                warn!(entry = %entry, "ignoring host document scope without `name=path`; expected name=container_path");
                continue;
            };
            let (name, path) = (name.trim(), path.trim());
            if name.is_empty() || path.is_empty() {
                warn!(entry = %entry, "ignoring host document scope with an empty name or path");
                continue;
            }
            if !Path::new(path).is_absolute() {
                warn!(entry = %entry, "ignoring host document scope whose path is not absolute; a relative root would resolve against the process cwd");
                continue;
            }
            if scopes
                .insert(name.to_string(), PathBuf::from(path))
                .is_some()
            {
                warn!(scope = %name, "duplicate host document scope name; the later entry wins");
            }
        }
        Self {
            scopes,
            journal_dir,
            max_doc_bytes,
            // Pure construction: the values are read in `from_env`, and a test wanting
            // another bound changes the field directly. The defaults are all "capability
            // off, bounds at their defaults", in the same direction as the deployment
            // defaults.
            body_egress: false,
            max_read_bytes: DEFAULT_MAX_READ_BYTES,
            max_list_entries: DEFAULT_MAX_LIST_ENTRIES,
            approval_ttl_secs: DEFAULT_APPROVAL_TTL_SECS,
            max_plan_bytes: DEFAULT_MAX_PLAN_BYTES,
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.scopes.is_empty()
    }
}

/// One organizing step. The set is closed and every variant names locations
/// relative to the scope root; there is no variant that carries a destination
/// outside it, so "where may this write" has no answer that a document can
/// influence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum HostDocOp {
    /// Create one directory. Parents are not created implicitly: an auditable
    /// plan lists every directory it makes.
    Mkdir { path: String },
    /// Move or rename inside the scope. The destination must not exist.
    Rename { from: String, to: String },
    /// Create or replace a document's contents.
    Write { path: String, content: String },
}

impl HostDocOp {
    fn kind(&self) -> &'static str {
        match self {
            Self::Mkdir { .. } => "mkdir",
            Self::Rename { .. } => "rename",
            Self::Write { .. } => "write",
        }
    }

    /// The relative locations this operation names, in the order the paths
    /// matter (source before destination).
    fn rel_paths(&self) -> Vec<&String> {
        match self {
            Self::Mkdir { path } => vec![path],
            Self::Rename { from, to } => vec![from, to],
            Self::Write { path, .. } => vec![path],
        }
    }
}

/// What an operation would do to the scope, decided by looking at the scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// The target does not exist yet.
    Create,
    /// The target exists and would be rewritten.
    Replace,
    /// An existing path would move to a vacant one.
    Move,
    /// An existing directory is already there.
    Present,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedOp {
    pub op: HostDocOp,
    /// Absolute container paths the operation resolves to, one per named path.
    /// Echoed so the reviewer sees where inside the scope each step lands.
    pub resolved: Vec<String>,
    pub effect: Effect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocPlan {
    pub scope: String,
    /// Hash of the planned operation set. Apply must present this value back,
    /// so what gets executed is the set that was reviewed.
    pub plan_hash: String,
    pub ops: Vec<PlannedOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedOp {
    pub resolved: String,
    pub effect: Effect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocApply {
    pub journal_id: String,
    pub applied: Vec<AppliedOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocRollback {
    pub journal_id: String,
    pub restored: usize,
}

/// What an entry in a listing is.
///
/// `Other` exists so that a socket, fifo or device node is *named* rather than
/// dropped: a listing that silently omits something is read as "this is all
/// there is", which is the one thing a listing must never be wrong about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Dir,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostDocEntry {
    /// Relative to the scope root, `/`-joined.
    pub path: String,
    pub kind: EntryKind,
    /// Zero for anything that is not a file.
    pub bytes: u64,
    /// `None` when the filesystem does not answer. Not zero: zero is 1970, a
    /// real timestamp, and a reader that sees it would take it for one.
    pub modified_unix: Option<u64>,
}

/// What is in a scope, as metadata only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostDocListing {
    pub scope: String,
    /// The prefix the walk started from, as the caller wrote it.
    pub prefix: Option<String>,
    pub entries: Vec<HostDocEntry>,
    /// Symbolic links seen and not followed. They are absent from `entries` by
    /// design, so this is the count of what the listing left out: without it,
    /// "these are all the entries" and "these are the entries I can name" read
    /// the same.
    pub skipped_symlinks: usize,
    /// Entries whose name is not valid UTF-8, likewise left out and counted.
    pub skipped_unnamed: usize,
    /// The walk stopped at the entry ceiling. A truncated listing looks exactly
    /// like a small directory — and the caller's next move is usually to
    /// organize what it was shown — so the ceiling has to be visible in the
    /// answer, not only in the configuration.
    pub truncated: bool,
}

/// Why a body read was refused, as the caller sees it and as the counter
/// recorded it.
///
/// One value carrying both, because the two must not be able to disagree: a
/// refusal answered with a status derived separately from the cell it was
/// counted under would only ever disagree on one branch, which is exactly the
/// shape a reader cannot find by looking at the metric.
#[derive(Debug)]
pub struct ReadRefusal {
    /// The `outcome` label this refusal was counted under, from
    /// [`READ_OUTCOMES`].
    pub outcome: &'static str,
    pub error: SFError,
}

/// One document body, read into this process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostDocRead {
    pub scope: String,
    pub path: String,
    pub bytes: u64,
    pub content: String,
    pub modified_unix: Option<u64>,
}

/// A journal entry: what was done, and what it takes to undo it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum JournalRecord {
    Mkdir {
        path: String,
        /// False when the directory already existed, in which case rollback
        /// leaves it alone.
        created: bool,
    },
    Rename {
        from: String,
        to: String,
    },
    Write {
        path: String,
        /// False when the file did not exist, in which case rollback removes
        /// it instead of restoring contents.
        existed: bool,
        /// Backup file name under the journal's `orig/` directory.
        backup: Option<String>,
    },
}

/// The closed set of listing outcomes; every one is published, zeros included.
///
/// Cells are cut by **cause**, not by error type: each cell implies a different
/// action.
/// - `unknown_scope`: the scope name the caller used is not in the configuration. The
///   action is to fix the caller, or to add the configuration for that identity.
/// - `refused_path`: the prefix is absolute, carries a `..`, or has a symlink as one of
///   its segments. **This cell is an out-of-bounds attempt**, and it has to stay
///   distinguishable from the previous one (a typo).
/// - `not_found`: the scope has no such prefix. The action is to fix the caller -- it is
///   a different thing from "the directory is empty", which is why the latter goes to
///   `listed` instead of here.
/// - `unreadable`: this process cannot read it (permissions, the volume, IO). The action
///   is to check this pod's mount and volume, not the caller.
/// - `listed`: it was listed (possibly truncated by a ceiling, and a truncation is
///   stated by the response's own `truncated`).
pub const LIST_OUTCOMES: [&str; 5] = [
    "unknown_scope",
    "refused_path",
    "not_found",
    "unreadable",
    "listed",
];

/// The closed set of read outcomes; every one is published, zeros included. The order is
/// the order of judgement: the switch outermost, then the scope name, the path, the entry
/// kind, the size, the encoding, and only then the content.
///
/// - `disabled`: the egress switch is off. The action is to turn it on (or to accept that
///   this is not being done today), not to inspect the caller. **Without this cell, "the
///   switch is off" and "nobody came to read" would look the same.**
/// - `unknown_scope` / `refused_path`: as in the listing.
/// - `over_ceiling`: the entry is larger than the readable ceiling. The action is to
///   adjust the bound, or to stop the caller reading something that big; it once looked
///   the same as the next cell (both read as "cannot read it"), and it is separate because
///   the side that has to change is a different one.
/// - `not_text`: there is no text body to return at that path -- it is not a regular file
///   (a directory, a fifo), or the bytes are not UTF-8. **Deliberately one cell**: for
///   whoever reads this number, "read something else" is the same action, and which of the
///   two it was is stated in the error message (with the path and the reason), not in the
///   cell name.
/// - `not_found`: the scope has no such path. The action is to fix the caller.
/// - `unreadable`: this process cannot read it. The action is to check the mount and the
///   volume.
/// - `read`: the body was obtained.
pub const READ_OUTCOMES: [&str; 8] = [
    "disabled",
    "unknown_scope",
    "refused_path",
    "over_ceiling",
    "not_text",
    "not_found",
    "unreadable",
    "read",
];

/// The closed set of outcomes of this apply gate; every one is published, zeros included.
/// The order is the order of judgement: first whether this record exists, then which state
/// it is in, and finally whether what it approved is the plan in hand.
///
/// - `released`: the record exists, is inside its window, and approves exactly this plan,
///   so the gate lets it through. **It counts before execution**: the gate judges "this is
///   allowed", which is a different thing from "it worked", and the latter is said
///   operation by operation by the ops counters.
/// - `refused_no_record`: no such record on disk. The action is to fix the caller (a
///   mistyped id, or nothing was ever staged).
/// - `refused_unreadable_record`: the record is on disk but this process cannot read it
///   (permissions, the volume, corrupt content). The action is to inspect this pod's
///   volume -- it must stay apart from the previous cell: that one is the caller's to fix,
///   this one is the operator's.
/// - `refused_not_approved`: the record exists and nobody approved it. **This cell is the
///   reason the gate exists**: unapproved means not done, not "wait and see".
/// - `refused_expired`: the window has passed (approved-but-expired included). The action
///   is to produce a new plan and approve it again.
/// - `refused_rejected`: it was rejected. The action is to ask why it was rejected.
/// - `refused_already_applied`: this approval has already been used once. What it guards
///   against is one approval being executed twice.
/// - `refused_hash_mismatch`: the record approves a different plan. **This cell is a
///   security event**: the plan in hand does not match the approval record, which means
///   someone touched the plan after it was approved.
pub const APPLY_OUTCOMES: [&str; 8] = [
    "refused_no_record",
    "refused_unreadable_record",
    "refused_not_approved",
    "refused_expired",
    "refused_rejected",
    "refused_already_applied",
    "refused_hash_mismatch",
    "released",
];

/// One staged plan awaiting approval, together with what has happened to it.
///
/// The state **is not stored in a separate field**: it is derived from the times and the
/// three optional records. Storing a state guarantees a moment where the stored one and
/// the computed one differ, and the place they would differ is exactly "was it approved"
/// -- the one thing that must not be ambiguous.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedPlan {
    pub staged_id: String,
    pub plan: HostDocPlan,
    pub staged_at_unix: u64,
    /// The end of the window. Past it this plan can be neither approved nor applied --
    /// **approved or not**: the window says "this plan is still fresh and still
    /// remembered", and a plan left around long enough to approve and then execute is one
    /// nobody is watching any more.
    pub expires_at_unix: u64,
    pub approval: Option<HostDocApproval>,
    pub rejection: Option<HostDocRejection>,
    pub applied: Option<HostDocApplied>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocApproval {
    /// Who approved it. **This is a recorded claim, not an identity this process
    /// verified**: the executor holds no credentials (the deployment deliberately mounts
    /// no Secret), so there is no credential surface against which an approver could be
    /// verified. What this field is for is being looked up afterwards -- who, when, and
    /// which hash they approved.
    pub approver: String,
    pub approved_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocRejection {
    pub reason: Option<String>,
    pub rejected_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocApplied {
    pub journal_id: String,
    pub applied_at_unix: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagedState {
    Pending,
    Approved,
    Rejected,
    Applied,
    Expired,
}

impl StagedState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Applied => "applied",
            Self::Expired => "expired",
        }
    }
}

impl StagedPlan {
    /// Which state this record is in right now.
    ///
    /// What has already happened outranks the clock: applied means applied and rejected
    /// means rejected -- the window governs "can this still be done", not "what has been
    /// done". Expiry in turn outranks approval: an approval is not a permanent pass.
    pub fn state(&self, now_unix: u64) -> StagedState {
        if self.applied.is_some() {
            StagedState::Applied
        } else if self.rejection.is_some() {
            StagedState::Rejected
        } else if now_unix >= self.expires_at_unix {
            StagedState::Expired
        } else if self.approval.is_some() {
            StagedState::Approved
        } else {
            StagedState::Pending
        }
    }

    pub fn view(&self, now_unix: u64) -> StagedPlanView {
        StagedPlanView {
            staged_id: self.staged_id.clone(),
            scope: self.plan.scope.clone(),
            plan_hash: self.plan.plan_hash.clone(),
            op_count: self.plan.ops.len(),
            staged_at_unix: self.staged_at_unix,
            expires_at_unix: self.expires_at_unix,
            state: self.state(now_unix),
            approver: self.approval.as_ref().map(|a| a.approver.clone()),
            rejected_reason: self.rejection.as_ref().and_then(|r| r.reason.clone()),
            journal_id: self.applied.as_ref().map(|a| a.journal_id.clone()),
        }
    }
}

/// One entry in the pending list. **It carries no plan body**: a list may hold dozens of
/// entries, and stuffing the bodies in would turn the review face itself into a heavy
/// path, while whoever wants to see a body is looking at one entry anyway. The body comes
/// from `staged_plan`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedPlanView {
    pub staged_id: String,
    pub scope: String,
    pub plan_hash: String,
    pub op_count: usize,
    pub staged_at_unix: u64,
    pub expires_at_unix: u64,
    pub state: StagedState,
    pub approver: Option<String>,
    pub rejected_reason: Option<String>,
    pub journal_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedPlanList {
    pub plans: Vec<StagedPlanView>,
    /// How many records in the directory could not be read. **Unreadable and absent are not
    /// the same thing**: folding this into "zero pending" would make a broken record look
    /// like nobody ever submitted a plan.
    pub unreadable_records: usize,
}

/// The two reasons a staged record could not be read. They are separate because **the
/// action differs**: the former is the caller's to fix (a mistyped id), the latter means
/// inspecting this pod's volume.
enum StagedLoad {
    NoRecord(SFError),
    Unreadable(SFError),
}

impl StagedLoad {
    fn outcome(&self) -> &'static str {
        match self {
            Self::NoRecord(_) => "refused_no_record",
            Self::Unreadable(_) => "refused_unreadable_record",
        }
    }

    fn into_error(self) -> SFError {
        match self {
            Self::NoRecord(e) | Self::Unreadable(e) => e,
        }
    }
}

struct HostDocsMetrics {
    registry: Registry,
    ops: CounterVec,
    rollbacks: CounterVec,
    lists: CounterVec,
    reads: CounterVec,
    gates: CounterVec,
}

impl HostDocsMetrics {
    fn new(scope_count: usize) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let scopes = Gauge::new(
            "sandbox_host_docs_scopes",
            "Host document scopes configured on this executor",
        )?;
        let ops = CounterVec::new(
            Opts::new(
                "sandbox_host_docs_ops_total",
                "Host document operations by kind and outcome",
            ),
            &["kind", "outcome"],
        )?;
        let rollbacks = CounterVec::new(
            Opts::new(
                "sandbox_host_docs_rollbacks_total",
                "Host document rollbacks by outcome",
            ),
            &["outcome"],
        )?;
        let lists = CounterVec::new(
            Opts::new(
                "sandbox_host_docs_lists_total",
                "Host document listings by outcome",
            ),
            &["outcome"],
        )?;
        let reads = CounterVec::new(
            Opts::new(
                "sandbox_host_docs_reads_total",
                "Host document body reads by outcome",
            ),
            &["outcome"],
        )?;
        let gates = CounterVec::new(
            Opts::new(
                "sandbox_host_docs_apply_total",
                "Host document applies by what the approval gate decided",
            ),
            &["outcome"],
        )?;
        registry.register(Box::new(scopes.clone()))?;
        registry.register(Box::new(ops.clone()))?;
        registry.register(Box::new(rollbacks.clone()))?;
        registry.register(Box::new(lists.clone()))?;
        registry.register(Box::new(reads.clone()))?;
        registry.register(Box::new(gates.clone()))?;
        // The scope count is a configuration fact, not a moving reading: it is
        // published once so an operator can see whether the capability is on
        // at all without reading the pod's env.
        scopes.set(scope_count as f64);
        // Both vocabularies are published at zero: **absent and zero are two different
        // things**. That goes especially for the read face -- what a closed switch most
        // looks like is "no reading at all", which is exactly what a channel that was
        // never wired up looks like.
        for outcome in LIST_OUTCOMES {
            lists.with_label_values(&[outcome]).inc_by(0.0);
        }
        for outcome in READ_OUTCOMES {
            reads.with_label_values(&[outcome]).inc_by(0.0);
        }
        for outcome in APPLY_OUTCOMES {
            gates.with_label_values(&[outcome]).inc_by(0.0);
        }
        Ok(Self {
            registry,
            ops,
            rollbacks,
            lists,
            reads,
            gates,
        })
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        if TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .is_ok()
        {
            String::from_utf8_lossy(&buf).into_owned()
        } else {
            String::new()
        }
    }

    fn count(&self, kind: &str, outcome: &str) {
        self.ops.with_label_values(&[kind, outcome]).inc();
    }

    fn count_rollback(&self, outcome: &str) {
        self.rollbacks.with_label_values(&[outcome]).inc();
    }

    fn count_list(&self, outcome: &str) {
        self.lists.with_label_values(&[outcome]).inc();
    }

    fn count_read(&self, outcome: &str) {
        self.reads.with_label_values(&[outcome]).inc();
    }

    fn count_apply(&self, outcome: &str) {
        self.gates.with_label_values(&[outcome]).inc();
    }
}

pub struct HostDocs {
    cfg: HostDocsConfig,
    metrics: HostDocsMetrics,
    /// Serializes applies so two plans cannot interleave their journals and
    /// perform decisions.
    apply_lock: Mutex<()>,
    /// A serial shared by the rollback journal and staged records; it exists only to make
    /// two ids in the same millisecond differ.
    id_seq: AtomicU64,
}

impl HostDocs {
    pub fn new(cfg: HostDocsConfig) -> SFResult<Arc<Self>> {
        let metrics = HostDocsMetrics::new(cfg.scopes.len()).map_err(|e| {
            SFError::Config(format!("host document metrics registration failed: {e}"))
        })?;
        info!(
            scopes = cfg.scopes.len(),
            journal = %cfg.journal_dir.display(),
            max_doc_bytes = cfg.max_doc_bytes,
            "host document scopes configured"
        );
        Ok(Arc::new(Self {
            cfg,
            metrics,
            apply_lock: Mutex::new(()),
            id_seq: AtomicU64::new(0),
        }))
    }

    pub fn metrics(&self) -> String {
        self.metrics.render()
    }

    /// Resolve a scope name to its configured root. An unknown name is a hard
    /// error: silently falling back to "some" root would hand a task the
    /// wrong person's documents.
    fn scope_root(&self, scope: &str) -> SFResult<&Path> {
        self.cfg
            .scopes
            .get(scope)
            .map(|p| p.as_path())
            .ok_or_else(|| {
                let known: Vec<&str> = self.cfg.scopes.keys().map(|k| k.as_str()).collect();
                SFError::Validation(format!(
                    "unknown host document scope {scope:?}; configured scopes: {known:?}"
                ))
            })
    }

    /// The plan a caller reviews. Effects are decided now, and apply refuses
    /// any operation whose effect no longer matches, so a review cannot be
    /// invalidated by a change that lands in between.
    pub fn plan(&self, scope: &str, ops: &[HostDocOp]) -> SFResult<HostDocPlan> {
        let planned = self.plan_ops(scope, ops)?;
        Ok(HostDocPlan {
            scope: scope.to_string(),
            plan_hash: plan_hash(scope, ops),
            ops: planned,
        })
    }

    fn plan_ops(&self, scope: &str, ops: &[HostDocOp]) -> SFResult<Vec<PlannedOp>> {
        if ops.is_empty() {
            return Err(SFError::Validation(
                "an organizing plan with no operations is refused".into(),
            ));
        }
        let root = self.scope_root(scope)?;
        let mut planned = Vec::with_capacity(ops.len());
        for op in ops {
            let mut resolved = Vec::new();
            for rel in op.rel_paths() {
                let p = resolve_within_scope(root, rel)?;
                if let HostDocOp::Write { content, .. } = op {
                    if content.len() > self.cfg.max_doc_bytes {
                        return Err(SFError::Validation(format!(
                            "document {} is {} bytes; the write ceiling is {} bytes",
                            p.display(),
                            content.len(),
                            self.cfg.max_doc_bytes
                        )));
                    }
                }
                resolved.push(p.display().to_string());
            }
            planned.push(PlannedOp {
                effect: effect_of(op, &resolved)?,
                resolved,
                op: op.clone(),
            });
        }
        Ok(planned)
    }

    /// List the entries in a scope, metadata only: relative path, kind, byte count,
    /// mtime.
    ///
    /// **Not subject to the egress switch**, deliberately: the switch governs bodies
    /// leaving the cluster, while a listing returns file names and timestamps. Gating the
    /// listing too would leave a deployment at its defaults unable to see even which files
    /// exist -- that is not "bodies stay in", it is the capability off. What needs the
    /// switch is the next step: reading a body.
    ///
    /// A prefix that does not exist is an **error**, not an empty listing: an empty
    /// directory and a mistyped path have to be distinguishable at the caller, otherwise
    /// the organiser would take "this scope is empty" as a conclusion.
    pub fn list(&self, scope: &str, prefix: Option<&str>) -> SFResult<HostDocListing> {
        let root = match self.scope_root(scope) {
            Ok(root) => root,
            Err(e) => {
                self.metrics.count_list("unknown_scope");
                return Err(e);
            }
        };
        let prefix = prefix.map(str::trim).filter(|p| !p.is_empty() && *p != ".");
        let start = match prefix {
            None => root.to_path_buf(),
            Some(rel) => match resolve_within_scope(root, rel) {
                Ok(path) => path,
                Err(e) => {
                    self.metrics.count_list("refused_path");
                    return Err(e);
                }
            },
        };
        let start_is_dir = match std::fs::symlink_metadata(&start) {
            Ok(meta) => meta.is_dir(),
            Err(_) => {
                self.metrics.count_list("not_found");
                return Err(SFError::Validation(format!(
                    "scope {scope:?} has no such prefix {start:?}; an empty directory and a path that does not exist have to be distinguishable at the caller"
                )));
            }
        };

        let mut entries = Vec::new();
        let mut skipped_symlinks = 0usize;
        let mut skipped_unnamed = 0usize;
        let mut truncated = false;
        // Symlinks are not followed: what is in the listing must not depend on files
        // outside the scope. Under `follow_links(false)`, walkdir hands the link itself to
        // the caller as an entry (so it can be counted), and it does not descend into the
        // directory the link points at.
        for entry in WalkDir::new(&start).follow_links(false).sort_by_file_name() {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    self.metrics.count_list("unreadable");
                    return Err(SFError::Config(format!(
                        "cannot list {}: {e} (this process cannot read it -- not the caller's problem; check the mount and the volume)",
                        start.display()
                    )));
                }
            };
            // A prefix naming a directory lists what is under it (the directory itself is
            // not the thing being organised); a prefix naming a file makes that one file
            // the answer. The two paths cannot be merged into one: skipping the start
            // unconditionally would make asking by file name list nothing, and an empty
            // listing reads downstream as "there is nothing here".
            if entry.path() == start && start_is_dir {
                continue;
            }
            if entry.file_type().is_symlink() {
                skipped_symlinks += 1;
                continue;
            }
            if entries.len() >= self.cfg.max_list_entries {
                truncated = true;
                break;
            }
            let Some(rel) = entry.path().strip_prefix(root).ok().and_then(posix_rel) else {
                skipped_unnamed += 1;
                continue;
            };
            let (kind, bytes, modified_unix) = match entry_facts(entry.path()) {
                Ok(facts) => facts,
                Err(e) => {
                    self.metrics.count_list("unreadable");
                    return Err(e);
                }
            };
            entries.push(HostDocEntry {
                path: rel,
                kind,
                bytes,
                modified_unix,
            });
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        self.metrics.count_list("listed");
        Ok(HostDocListing {
            scope: scope.to_string(),
            prefix: prefix.map(str::to_string),
            entries,
            skipped_symlinks,
            skipped_unnamed,
            truncated,
        })
    }

    /// Read one body. **Subject to the egress switch**: while it is off, this refuses
    /// outright rather than returning empty content.
    ///
    /// Empty content and "nothing was read" have to be distinguishable at the caller --
    /// the organiser will faithfully take an empty string as "this file is empty" and
    /// write back from that conclusion. So off means refuse, and it lands in the
    /// `disabled` cell so that it stays apart from "nobody came to read".
    ///
    /// Returns [`ReadRefusal`] rather than a bare error: the cell that gets recorded and
    /// the error shown to the caller come from **one judgement**. Written in two places,
    /// they would produce combinations like "the reading says this cell while the status
    /// code answers from another", and such a combination is visible only on one branch.
    pub fn read(&self, scope: &str, rel: &str) -> Result<HostDocRead, ReadRefusal> {
        // The switch is outermost: while it is off, not even the path is looked at. It is
        // "this is not done today", not "your path is bad" -- the two imply opposite next
        // moves (turn the switch on vs. change the request), so the cells stay separate as
        // well.
        if !self.cfg.body_egress {
            return Err(self.refuse_read(
                "disabled",
                SFError::Validation(format!(
                    "host document body reads are not enabled ({BODY_EGRESS_ENV} is off): what comes \
                     back here is a refusal, not empty content; an empty file and a failed read \
                     have to stay distinguishable at the caller"
                )),
            ));
        }
        let root = match self.scope_root(scope) {
            Ok(root) => root,
            Err(e) => return Err(self.refuse_read("unknown_scope", e)),
        };
        let path = match resolve_within_scope(root, rel) {
            Ok(path) => path,
            Err(e) => return Err(self.refuse_read("refused_path", e)),
        };
        let md = match std::fs::symlink_metadata(&path) {
            Ok(md) => md,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(self.refuse_read(
                    "not_found",
                    SFError::Validation(format!(
                        "scope {scope:?} has no such file {rel:?}; an empty file and a file that does not exist have to be distinguishable at the caller"
                    )),
                ))
            }
            Err(e) => {
                return Err(self.refuse_read(
                    "unreadable",
                    SFError::Config(format!("cannot read metadata for {}: {e}", path.display())),
                ))
            }
        };
        if !md.file_type().is_file() {
            return Err(self.refuse_read(
                "not_text",
                SFError::Validation(format!(
                    "{} is not a regular file ({:?}); this face reads text bodies only",
                    path.display(),
                    md.file_type()
                )),
            ));
        }
        if md.len() > self.cfg.max_read_bytes as u64 {
            return Err(self.refuse_read(
                "over_ceiling",
                SFError::Validation(format!(
                    "{} is {} bytes, over the readable ceiling of {} bytes",
                    path.display(),
                    md.len(),
                    self.cfg.max_read_bytes
                )),
            ));
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(self.refuse_read(
                    "unreadable",
                    SFError::Config(format!("cannot read {}: {e}", path.display())),
                ))
            }
        };
        let content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(e) => {
                return Err(self.refuse_read(
                    "not_text",
                    SFError::Validation(format!(
                        "{} is not UTF-8 text at byte {}; the text judgement yields no verdict for it, and the side that yields no verdict is refuse",
                        path.display(),
                        e.utf8_error().valid_up_to()
                    )),
                ))
            }
        };
        match path.strip_prefix(root).ok().and_then(posix_rel) {
            Some(resolved) => {
                self.metrics.count_read("read");
                Ok(HostDocRead {
                    scope: scope.to_string(),
                    path: resolved,
                    bytes: content.len() as u64,
                    content,
                    modified_unix: md.modified().ok().and_then(system_time_secs),
                })
            }
            None => Err(self.refuse_read(
                "unreadable",
                SFError::Config(format!("{} is not inside the scope root", path.display())),
            )),
        }
    }

    /// One way of reading a refusal: which cell the reading lands in, and what the caller
    /// gets to see.
    ///
    /// `outcome` is the very cell recorded into `sandbox_host_docs_reads_total` -- so the
    /// response and the reading cannot tell two different stories.
    fn refuse_read(&self, outcome: &'static str, error: SFError) -> ReadRefusal {
        self.metrics.count_read(outcome);
        ReadRefusal { outcome, error }
    }

    /// Staged records and the rollback journal share one volume: **an approval has to
    /// survive a process restart**, otherwise "approved, but not recognised after a
    /// restart" turns this gate into a matter of mood -- and its whole value is that
    /// someone approves every time.
    fn staged_dir(&self) -> PathBuf {
        self.cfg.journal_dir.join("staged")
    }

    /// Stage a plan for approval, returning its id and state.
    ///
    /// **Not governed by the egress switch**: what is staged is a description of the
    /// intended change and no body has moved yet (same as `list`). The switch governs
    /// bodies leaving the cluster, and no body has even been read here.
    pub async fn stage(&self, scope: &str, ops: &[HostDocOp]) -> SFResult<StagedPlanView> {
        let plan = self.plan(scope, ops)?;
        let body = serde_json::to_vec(&plan)
            .map_err(|e| SFError::Config(format!("cannot serialize a plan: {e}")))?;
        if body.len() > self.cfg.max_plan_bytes {
            self.metrics.count("stage", "refused_over_plan_ceiling");
            return Err(SFError::Validation(format!(
                "plan is {} bytes, over the {} byte ceiling for a reviewable plan; split it",
                body.len(),
                self.cfg.max_plan_bytes
            )));
        }
        let now = now_unix();
        let record = StagedPlan {
            staged_id: self.new_record_id(&plan.plan_hash),
            plan,
            staged_at_unix: now,
            expires_at_unix: now.saturating_add(self.cfg.approval_ttl_secs),
            approval: None,
            rejection: None,
            applied: None,
        };
        self.save_staged(&record).await?;
        self.metrics.count("stage", "staged");
        self.prune_staged(now).await;
        Ok(record.view(now))
    }

    /// Approve a staged plan. **It executes nothing**: approving only writes a record, and
    /// acting is a separate call (apply, carrying this id). Two steps because that keeps
    /// "I agree" and "it has happened" apart in the record -- a single record meaning both
    /// would leave no way to say afterwards which of the two came first.
    pub async fn approve(&self, staged_id: &str, approver: &str) -> SFResult<StagedPlanView> {
        let approver = approver.trim();
        if approver.is_empty() {
            self.metrics.count("approve", "refused_no_approver");
            return Err(SFError::Validation(
                "an approval must name who approved it; an anonymous approval is not one".into(),
            ));
        }
        let mut record = match self.load_staged(staged_id).await {
            Ok(record) => record,
            Err(load) => {
                self.metrics.count("approve", load.outcome());
                return Err(load.into_error());
            }
        };
        let now = now_unix();
        let state = record.state(now);
        if state != StagedState::Pending {
            self.metrics
                .count("approve", &format!("refused_{}", state.as_str()));
            return Err(SFError::Validation(format!(
                "the staged plan {staged_id} is {}; only a pending plan can be approved",
                state.as_str()
            )));
        }
        record.approval = Some(HostDocApproval {
            approver: approver.to_string(),
            approved_at_unix: now,
        });
        self.save_staged(&record).await?;
        self.metrics.count("approve", "approved");
        info!(
            staged_id = %staged_id,
            scope = %record.plan.scope,
            ops = record.plan.ops.len(),
            approver = %approver,
            "host document plan approved"
        );
        Ok(record.view(now))
    }

    /// Reject a plan that has not been executed.
    ///
    /// A plan already applied does not come through here: that is `rollback`'s business --
    /// something that has happened cannot be undone by rejecting it, only by another
    /// happening, and the records of the two are entirely different.
    pub async fn reject(&self, staged_id: &str, reason: Option<&str>) -> SFResult<StagedPlanView> {
        let mut record = match self.load_staged(staged_id).await {
            Ok(record) => record,
            Err(load) => {
                self.metrics.count("reject", load.outcome());
                return Err(load.into_error());
            }
        };
        let now = now_unix();
        let state = record.state(now);
        if state != StagedState::Pending {
            self.metrics
                .count("reject", &format!("refused_{}", state.as_str()));
            return Err(SFError::Validation(format!(
                "the staged plan {staged_id} is {}; only a pending plan can be rejected",
                state.as_str()
            )));
        }
        let reason = reason
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .map(str::to_string);
        record.rejection = Some(HostDocRejection {
            reason: reason.clone(),
            rejected_at_unix: now,
        });
        self.save_staged(&record).await?;
        self.metrics.count("reject", "rejected");
        info!(staged_id = %staged_id, reason = ?reason, "host document plan rejected");
        Ok(record.view(now))
    }

    /// The staged records in the directory, each with the state it is in now.
    ///
    /// **Records that reached a terminal state are listed too**: a record already applied,
    /// already rejected, already expired is exactly the one that gets asked about -- making
    /// it disappear while still fresh leaves "where did it go" with no answer.
    pub async fn staged_plans(&self) -> SFResult<StagedPlanList> {
        let dir = self.staged_dir();
        let now = now_unix();
        self.prune_staged(now).await;
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            // The directory does not exist until something is staged; that is a normal state,
            // not a fault.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(StagedPlanList {
                    plans: Vec::new(),
                    unreadable_records: 0,
                })
            }
            Err(e) => {
                return Err(SFError::Config(format!(
                    "cannot read the staged plan directory at {}: {e}",
                    dir.display()
                )))
            }
        };
        let mut plans = Vec::new();
        let mut unreadable_records = 0usize;
        loop {
            let entry = entries.next_entry().await.map_err(|e| {
                SFError::Config(format!(
                    "cannot read the staged plan directory at {}: {e}",
                    dir.display()
                ))
            })?;
            let Some(entry) = entry else { break };
            let name = entry.file_name();
            let Some(id) = name.to_str().and_then(|n| n.strip_suffix(".json")) else {
                continue;
            };
            match self.load_staged(id).await {
                Ok(record) => plans.push(record.view(now)),
                Err(load) => {
                    unreadable_records += 1;
                    warn!(record = %id, error = %load.into_error(), "a staged plan record could not be read");
                }
            }
        }
        plans.sort_by(|a, b| a.staged_id.cmp(&b.staged_id));
        Ok(StagedPlanList {
            plans,
            unreadable_records,
        })
    }

    /// One staged record in full, for a reviewer: it carries the plan body (the operations
    /// and their effects).
    pub async fn staged_plan(&self, staged_id: &str) -> SFResult<StagedPlan> {
        self.load_staged(staged_id)
            .await
            .map_err(StagedLoad::into_error)
    }

    /// Read one staged record, keeping the two reasons for "could not read it" apart.
    async fn load_staged(&self, staged_id: &str) -> Result<StagedPlan, StagedLoad> {
        if let Err(e) = validate_staged_id(staged_id) {
            return Err(StagedLoad::NoRecord(e));
        }
        let path = self.staged_dir().join(format!("{staged_id}.json"));
        let body = match tokio::fs::read(&path).await {
            Ok(body) => body,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StagedLoad::NoRecord(SFError::Validation(format!(
                    "no staged plan {staged_id:?}; stage a plan and have it approved before applying it"
                ))))
            }
            Err(e) => {
                return Err(StagedLoad::Unreadable(SFError::Config(format!(
                    "cannot read the staged plan at {}: {e}",
                    path.display()
                ))))
            }
        };
        serde_json::from_slice(&body).map_err(|e| {
            StagedLoad::Unreadable(SFError::Config(format!(
                "the staged plan at {} is unreadable: {e}",
                path.display()
            )))
        })
    }

    /// Records are written by "write a temp file, then rename": the review face lists the
    /// directory by name, and a half-written record would be read as a broken one -- which
    /// looks exactly like a broken volume.
    async fn save_staged(&self, record: &StagedPlan) -> SFResult<()> {
        let dir = self.staged_dir();
        tokio::fs::create_dir_all(&dir).await.map_err(|e| {
            SFError::Config(format!(
                "cannot open the staged plan directory at {}: {e}",
                dir.display()
            ))
        })?;
        let body = serde_json::to_vec_pretty(record)
            .map_err(|e| SFError::Config(format!("cannot serialize a staged plan: {e}")))?;
        let published = dir.join(format!("{}.json", record.staged_id));
        let tmp = dir.join(format!("{}.json.tmp", record.staged_id));
        tokio::fs::write(&tmp, body).await.map_err(|e| {
            SFError::Config(format!(
                "cannot write the staged plan at {}: {e}",
                tmp.display()
            ))
        })?;
        tokio::fs::rename(&tmp, &published).await.map_err(|e| {
            SFError::Config(format!(
                "cannot publish the staged plan at {}: {e}",
                published.display()
            ))
        })
    }

    /// Clean up expired records. **Only ones that reached a terminal state, or that have
    /// been expired for a full window**: a plan still waiting for approval and one that
    /// just expired are both left alone -- the latter is the one most likely to be asked
    /// about.
    ///
    /// An action that deletes things has to leave its own reading (`prune`/`pruned`):
    /// without it an operator cannot see where a plan they submitted went, and can only
    /// guess.
    async fn prune_staged(&self, now: u64) {
        let dir = self.staged_dir();
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(dir = %dir.display(), error = %e, "cannot read the staged plan directory to prune it");
                return;
            }
        };
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    warn!(dir = %dir.display(), error = %e, "cannot walk the staged plan directory to prune it");
                    break;
                }
            };
            let name = entry.file_name();
            let Some(id) = name.to_str().and_then(|n| n.strip_suffix(".json")) else {
                continue;
            };
            let Ok(record) = self.load_staged(id).await else {
                continue;
            };
            if !prune_due(&record, now, self.cfg.approval_ttl_secs) {
                continue;
            }
            if let Err(e) = tokio::fs::remove_file(entry.path()).await {
                warn!(record = %id, error = %e, "cannot prune an expired staged plan");
                continue;
            }
            self.metrics.count("prune", "pruned");
            info!(staged_id = %id, "pruned a staged plan record");
        }
    }

    /// Execute a reviewed plan. The operation set must hash to the plan the
    /// caller was given, and every effect must still hold; the first operation
    /// that fails rolls back the ones already applied.
    ///
    /// `approval_id` is the id given at staging time: this gate **does not look at what
    /// the caller says**, only at the record on disk -- the record exists, is inside its
    /// window, and approves exactly this hash, and only then does it act. Each kind of
    /// refusal is published separately into `sandbox_host_docs_apply_total`, because they
    /// ask different things of a person (see `APPLY_OUTCOMES`).
    pub async fn apply(&self, plan: &HostDocPlan, approval_id: &str) -> SFResult<HostDocApply> {
        let ops: Vec<HostDocOp> = plan.ops.iter().map(|p| p.op.clone()).collect();
        let expected = plan_hash(&plan.scope, &ops);
        if expected != plan.plan_hash {
            return Err(SFError::Validation(
                "plan hash does not match the operations it carries; apply the plan as it was returned"
                    .into(),
            ));
        }
        let record = match self.load_staged(approval_id).await {
            Ok(record) => record,
            Err(load) => {
                self.metrics.count_apply(load.outcome());
                return Err(load.into_error());
            }
        };
        // The name avoids the `for (planned, now) in ...` below: the gate's own instant is
        // read once outside the loop and fixed there, and must not be shadowed by that
        // `now`.
        let gate_now = now_unix();
        let state = record.state(gate_now);
        if state != StagedState::Approved {
            // Each state implies a different action, so each state gets its own sentence:
            // a pending one needs someone to read it, an expired one needs a new plan, and
            // an applied one says this approval has already been used once.
            let outcome = match state {
                StagedState::Pending => "refused_not_approved",
                StagedState::Expired => "refused_expired",
                StagedState::Rejected => "refused_rejected",
                StagedState::Applied => "refused_already_applied",
                StagedState::Approved => unreachable!("handled above"),
            };
            self.metrics.count_apply(outcome);
            let detail = match state {
                StagedState::Pending => {
                    "nothing has approved it; someone has to read the plan and approve it"
                }
                StagedState::Expired => {
                    "its approval window has passed; stage the plan again and have it approved again"
                }
                StagedState::Rejected => {
                    "it was rejected; find out why instead of applying it anyway"
                }
                StagedState::Applied => {
                    "it was already applied once; an approval is not a licence to run twice"
                }
                StagedState::Approved => unreachable!("handled above"),
            };
            return Err(SFError::Validation(format!(
                "the staged plan {approval_id} is {}: {detail}",
                state.as_str()
            )));
        }
        if record.plan.plan_hash != plan.plan_hash {
            self.metrics.count_apply("refused_hash_mismatch");
            return Err(SFError::Validation(format!(
                "approval {approval_id} is for plan {} but this plan hashes to {}; the plan changed after it was reviewed",
                record.plan.plan_hash, plan.plan_hash
            )));
        }
        self.metrics.count_apply("released");
        let _guard = self.apply_lock.lock().await;
        let recomputed = self.plan_ops(&plan.scope, &ops)?;
        for (planned, now) in plan.ops.iter().zip(recomputed.iter()) {
            if planned.resolved != now.resolved || planned.effect != now.effect {
                return Err(SFError::Validation(format!(
                    "the scope changed since the plan was made: {} is now {:?} (planned {:?}); re-plan before applying",
                    now.resolved.join(" -> "),
                    now.effect,
                    planned.effect
                )));
            }
        }

        let journal_id = self.new_record_id(&plan.plan_hash);
        let dir = self.cfg.journal_dir.join(&journal_id);
        tokio::fs::create_dir_all(dir.join("orig"))
            .await
            .map_err(|e| {
                SFError::Config(format!(
                    "cannot open a rollback journal at {}: {e}",
                    dir.display()
                ))
            })?;
        let mut records: Vec<JournalRecord> = Vec::new();
        let mut applied: Vec<AppliedOp> = Vec::new();
        for planned in &plan.ops {
            match self.apply_one(planned, &dir, records.len()).await {
                Ok(record) => {
                    records.push(record);
                    let resolved = planned.resolved.last().cloned().unwrap_or_default();
                    applied.push(AppliedOp {
                        resolved,
                        effect: planned.effect,
                    });
                    self.metrics.count(planned.op.kind(), "applied");
                }
                Err(e) => {
                    self.metrics.count(planned.op.kind(), "refused");
                    return Err(match self.undo(&dir, &records).await {
                        Ok(()) => {
                            // Nothing survives a failed apply, so the journal
                            // has no restore value: drop it with its backups.
                            let _ = tokio::fs::remove_dir_all(&dir).await;
                            SFError::Validation(format!(
                                "{e}; the {} operation(s) applied before it were rolled back, so the scope is unchanged",
                                records.len()
                            ))
                        }
                        Err(re) => {
                            // A partial undo is not a state this module can
                            // describe, let alone finish: leave the records on
                            // disk so the leftovers are discoverable instead of
                            // becoming silent.
                            let _ = write_journal(&dir, &records).await;
                            SFError::Agent(format!(
                                "{e}; rolling back the applied prefix failed ({re}); journal {journal_id} holds what was done ({}) and needs a manual look",
                                dir.display()
                            ))
                        }
                    });
                }
            }
        }
        write_journal(&dir, &records).await?;
        // The approval is consumed: one approval cannot be executed twice
        // (`refused_already_applied`). This is written after the log -- first let "what was
        // done" land, then let "what is allowed" lapse; the reverse order means that if
        // this step fails, the approval is still unused while the change has already been
        // made.
        //
        // The path that fails midway and rolls back does not come through here: the scope
        // is restored as it was and the plan is unchanged, so retrying the same operation
        // set under the same approval is still inside what was approved.
        let mut consumed = record;
        consumed.applied = Some(HostDocApplied {
            journal_id: journal_id.clone(),
            applied_at_unix: now_unix(),
        });
        if let Err(e) = self.save_staged(&consumed).await {
            return Err(SFError::Agent(format!(
                "{e}; the plan was applied (journal {journal_id}) but the approval record could not be updated, so that approval could still be replayed and needs a manual look"
            )));
        }
        info!(journal = %journal_id, scope = %plan.scope, applied = applied.len(), "host document plan applied");
        Ok(HostDocApply {
            journal_id,
            applied,
        })
    }

    /// Perform one operation and record what it takes to undo it.
    ///
    /// Every decision about "what is there right now" is taken from the
    /// filesystem at the moment the operation runs, never from the plan: an
    /// earlier step of the same plan may have created or moved a file into this
    /// path, so a plan-time `Create` can be a real replacement by the time it
    /// is executed — and rollback has to know whose contents it overwrote.
    async fn apply_one(
        &self,
        planned: &PlannedOp,
        journal: &Path,
        index: usize,
    ) -> SFResult<JournalRecord> {
        match &planned.op {
            HostDocOp::Mkdir { .. } => {
                let path = PathBuf::from(&planned.resolved[0]);
                match tokio::fs::create_dir(&path).await {
                    Ok(()) => Ok(JournalRecord::Mkdir {
                        path: planned.resolved[0].clone(),
                        created: true,
                    }),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        let md = tokio::fs::metadata(&path).await.map_err(|e| {
                            SFError::Validation(format!("cannot inspect {}: {e}", path.display()))
                        })?;
                        if !md.is_dir() {
                            return Err(SFError::Validation(format!(
                                "{} exists and is not a directory",
                                path.display()
                            )));
                        }
                        // Somebody else made it; rollback must not delete a
                        // directory this plan did not create.
                        Ok(JournalRecord::Mkdir {
                            path: planned.resolved[0].clone(),
                            created: false,
                        })
                    }
                    Err(e) => Err(SFError::Validation(format!(
                        "cannot create {}: {e}",
                        path.display()
                    ))),
                }
            }
            HostDocOp::Rename { .. } => {
                let (from, to) = (
                    PathBuf::from(&planned.resolved[0]),
                    PathBuf::from(&planned.resolved[1]),
                );
                tokio::fs::rename(&from, &to).await.map_err(|e| {
                    SFError::Validation(format!(
                        "cannot move {} to {}: {e}",
                        from.display(),
                        to.display()
                    ))
                })?;
                Ok(JournalRecord::Rename {
                    from: planned.resolved[0].clone(),
                    to: planned.resolved[1].clone(),
                })
            }
            HostDocOp::Write { content, .. } => {
                let path = PathBuf::from(&planned.resolved[0]);
                let existing = match tokio::fs::symlink_metadata(&path).await {
                    Ok(md) => {
                        if md.is_dir() {
                            return Err(SFError::Validation(format!(
                                "{} is a directory",
                                path.display()
                            )));
                        }
                        if md.len() as usize > self.cfg.max_doc_bytes {
                            return Err(SFError::Validation(format!(
                                "{} is {} bytes; keeping a rollback copy is capped at {} bytes",
                                path.display(),
                                md.len(),
                                self.cfg.max_doc_bytes
                            )));
                        }
                        true
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                    Err(e) => {
                        return Err(SFError::Validation(format!(
                            "cannot inspect {}: {e}",
                            path.display()
                        )))
                    }
                };
                let backup = if existing {
                    let name = format!("orig/{index}");
                    tokio::fs::copy(&path, journal.join(&name))
                        .await
                        .map_err(|e| {
                            SFError::Validation(format!(
                                "cannot keep a rollback copy of {}: {e}",
                                path.display()
                            ))
                        })?;
                    Some(name)
                } else {
                    None
                };
                tokio::fs::write(&path, content.as_bytes())
                    .await
                    .map_err(|e| {
                        SFError::Validation(format!("cannot write {}: {e}", path.display()))
                    })?;
                Ok(JournalRecord::Write {
                    path: planned.resolved[0].clone(),
                    existed: existing,
                    backup,
                })
            }
        }
    }

    /// Undo the records of an apply that failed part-way. The records are not
    /// on disk yet (a journal file is written only once every operation
    /// succeeded), so this walks the in-memory list in reverse order.
    async fn undo(&self, dir: &Path, records: &[JournalRecord]) -> SFResult<()> {
        for record in records.iter().rev() {
            restore_record(dir, record).await?;
        }
        Ok(())
    }

    /// Restore the state before an apply, in reverse order.
    pub async fn rollback(&self, journal_id: &str) -> SFResult<HostDocRollback> {
        validate_journal_id(journal_id)?;
        let _guard = self.apply_lock.lock().await;
        let dir = self.cfg.journal_dir.join(journal_id);
        let records = read_journal(&dir).await?;
        let mut restored = 0usize;
        for record in records.iter().rev() {
            restore_record(&dir, record).await.map_err(|e| {
                self.metrics.count_rollback("partial");
                SFError::Validation(format!(
                    "rollback of journal {journal_id} stopped at {record:?}: {e}"
                ))
            })?;
            restored += 1;
        }
        let done = dir.join("journal.rolled-back.json");
        tokio::fs::rename(dir.join("journal.json"), &done)
            .await
            .map_err(|e| {
                SFError::Config(format!("cannot mark journal {journal_id} rolled back: {e}"))
            })?;
        self.metrics.count_rollback("restored");
        info!(journal = %journal_id, restored, "host document journal rolled back");
        Ok(HostDocRollback {
            journal_id: journal_id.to_string(),
            restored,
        })
    }

    /// A new record id. Milliseconds first, a serial in the middle, the head of the plan
    /// hash last: sorting by name sorts by time, and those hash characters make "which
    /// plan is this record for" visible from the directory listing itself.
    fn new_record_id(&self, plan_hash: &str) -> String {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let seq = self.id_seq.fetch_add(1, Ordering::Relaxed);
        let digest = &plan_hash[..plan_hash.len().min(12)];
        format!("{millis}-{seq}-{digest}")
    }
}

/// Stable fingerprint of a planned operation set. Apply recomputes it from the
/// operations it was handed, so a plan that was edited after review cannot be
/// executed under the reviewed hash.
fn plan_hash(scope: &str, ops: &[HostDocOp]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(scope.as_bytes());
    for op in ops {
        hasher.update(b"\n");
        // `to_string` on a tag-carrying enum with struct variants emits fields
        // in declaration order, so the same operation always hashes the same.
        hasher.update(serde_json::to_string(op).unwrap_or_default().as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn effect_of(op: &HostDocOp, resolved: &[String]) -> SFResult<Effect> {
    let meta = |p: &str| std::fs::symlink_metadata(p);
    match op {
        HostDocOp::Mkdir { .. } => match meta(&resolved[0]) {
            Ok(md) if md.is_dir() => Ok(Effect::Present),
            // A file where a directory is expected is not "already there":
            // saying so would let a plan look like a no-op and then fail.
            Ok(_) => Err(SFError::Validation(format!(
                "{} exists and is not a directory",
                resolved[0]
            ))),
            Err(_) => Ok(Effect::Create),
        },
        HostDocOp::Rename { .. } => {
            if meta(&resolved[0]).is_err() {
                return Err(SFError::Validation(format!(
                    "nothing to move at {}",
                    resolved[0]
                )));
            }
            if meta(&resolved[1]).is_ok() {
                return Err(SFError::Validation(format!(
                    "{} already exists; a move never overwrites",
                    resolved[1]
                )));
            }
            Ok(Effect::Move)
        }
        HostDocOp::Write { .. } => {
            if let Ok(md) = meta(&resolved[0]) {
                if md.is_dir() {
                    return Err(SFError::Validation(format!(
                        "{} is a directory",
                        resolved[0]
                    )));
                }
                Ok(Effect::Replace)
            } else {
                Ok(Effect::Create)
            }
        }
    }
}

/// Resolve one caller-named location inside a scope root.
///
/// Only plain relative paths are accepted and no symlink is ever followed, so
/// neither `..` nor a link can name a location outside the scope — including
/// the container's own filesystem, which is real even when the host is not
/// reachable.
fn resolve_within_scope(root: &Path, rel: &str) -> SFResult<PathBuf> {
    let raw = rel.trim();
    if raw.is_empty() {
        return Err(SFError::Validation(
            "empty path in a document operation".into(),
        ));
    }
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(SFError::Validation(format!(
            "absolute path refused: {rel}; document operations name locations relative to the scope root"
        )));
    }
    let mut components: Vec<&std::ffi::OsStr> = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(name),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(SFError::Validation(format!(
                    "path {rel} contains `..`; document operations stay inside the scope root"
                )))
            }
            other => {
                return Err(SFError::Validation(format!(
                    "path {rel} contains an unsupported component ({other:?})"
                )))
            }
        }
    }
    if components.is_empty() {
        return Err(SFError::Validation(format!(
            "path {rel} names the scope root itself"
        )));
    }
    refuse_symlink(root, root)?;
    let mut current = root.to_path_buf();
    for name in components {
        current.push(name);
        refuse_symlink(root, &current)?;
    }
    Ok(current)
}

/// One entry's own facts: kind, byte count, mtime.
///
/// `symlink_metadata` rather than `metadata`: what is wanted here is **the entry itself**,
/// not where it points. Following links would make "what is in the listing" depend on
/// files outside the scope, and would let a link report the size of a body it is not.
fn entry_facts(path: &Path) -> SFResult<(EntryKind, u64, Option<u64>)> {
    let md = std::fs::symlink_metadata(path).map_err(|e| {
        SFError::Config(format!(
            "cannot read metadata for {}: {e} (this process cannot read it -- not the caller's problem)",
            path.display()
        ))
    })?;
    let file_type = md.file_type();
    let kind = if file_type.is_dir() {
        EntryKind::Dir
    } else if file_type.is_file() {
        EntryKind::File
    } else {
        EntryKind::Other
    };
    let bytes = if file_type.is_file() { md.len() } else { 0 };
    Ok((kind, bytes, md.modified().ok().and_then(system_time_secs)))
}

/// The `/`-joined form of a relative path; `None` when a name is not UTF-8.
///
/// Not `to_string_lossy`: two different names would be written as one, and the listing
/// would report A as B -- from which the caller may well rename and move things next.
/// Failing to list it (and counting that) is better than listing it wrong.
fn posix_rel(rel: &Path) -> Option<String> {
    let mut parts: Vec<&str> = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(name) => parts.push(name.to_str()?),
            _ => return None,
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join("/"))
}

fn system_time_secs(time: std::time::SystemTime) -> Option<u64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

/// A symlink anywhere along the path — including a dangling one, whose target
/// would otherwise be created by a write — is refused rather than followed.
fn refuse_symlink(root: &Path, path: &Path) -> SFResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(md) if md.file_type().is_symlink() => Err(SFError::Validation(format!(
            "{} is a symbolic link; links are not followed inside a document scope (scope root {})",
            path.display(),
            root.display()
        ))),
        _ => Ok(()),
    }
}

/// An id usable as a file name: non-empty, not absolute, a single segment. The rollback
/// journal and staged records both use it -- both are **strings the caller supplies**, and
/// pasting one straight into a path is a way out of the volume.
fn validate_plain_name(id: &str) -> SFResult<()> {
    let path = Path::new(id);
    let mut components = path.components();
    let plain = !id.trim().is_empty()
        && !path.is_absolute()
        && matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none();
    if plain {
        Ok(())
    } else {
        Err(SFError::Validation(format!("{id:?} is not a plain name")))
    }
}

fn validate_journal_id(id: &str) -> SFResult<()> {
    validate_plain_name(id).map_err(|_| {
        SFError::Validation(format!(
            "invalid journal id {id:?}; expected the id returned by an apply"
        ))
    })
}

fn validate_staged_id(id: &str) -> SFResult<()> {
    validate_plain_name(id).map_err(|_| {
        SFError::Validation(format!(
            "invalid staged plan id {id:?}; expected the id returned by a stage"
        ))
    })
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether this record should be cleaned up now.
///
/// A terminal state (applied, rejected) is cleaned once its window passes -- it has said
/// everything it has to say, and keeping it only takes up room. One that merely expired
/// without reaching a terminal state is kept for one more window: that is the one "where
/// did the plan I submitted go" will ask about.
fn prune_due(record: &StagedPlan, now_unix: u64, ttl_secs: u64) -> bool {
    match record.state(now_unix) {
        StagedState::Applied | StagedState::Rejected => now_unix >= record.expires_at_unix,
        StagedState::Expired => now_unix >= record.expires_at_unix.saturating_add(ttl_secs),
        StagedState::Pending | StagedState::Approved => false,
    }
}

async fn write_journal(dir: &Path, records: &[JournalRecord]) -> SFResult<()> {
    let body = serde_json::to_vec_pretty(records)
        .map_err(|e| SFError::Config(format!("cannot serialize a journal: {e}")))?;
    tokio::fs::write(dir.join("journal.json"), body)
        .await
        .map_err(|e| SFError::Config(format!("cannot write journal in {}: {e}", dir.display())))
}

async fn read_journal(dir: &Path) -> SFResult<Vec<JournalRecord>> {
    let rolled_back = dir.join("journal.rolled-back.json");
    if rolled_back.exists() {
        return Err(SFError::Validation(format!(
            "journal {} was already rolled back",
            dir.display()
        )));
    }
    let body = tokio::fs::read(dir.join("journal.json"))
        .await
        .map_err(|e| {
            SFError::Validation(format!("cannot read journal in {}: {e}", dir.display()))
        })?;
    serde_json::from_slice(&body)
        .map_err(|e| SFError::Config(format!("journal in {} is unreadable: {e}", dir.display())))
}

async fn restore_record(dir: &Path, record: &JournalRecord) -> SFResult<()> {
    match record {
        JournalRecord::Mkdir { path, created } => {
            if *created {
                tokio::fs::remove_dir(path).await.map_err(|e| {
                    SFError::Validation(format!("cannot remove the created directory {path}: {e}"))
                })?;
            }
            Ok(())
        }
        JournalRecord::Rename { from, to } => tokio::fs::rename(to, from)
            .await
            .map_err(|e| SFError::Validation(format!("cannot move {to} back to {from}: {e}"))),
        JournalRecord::Write {
            path,
            existed,
            backup,
        } => match (existed, backup) {
            (true, Some(name)) => tokio::fs::copy(dir.join(name), path)
                .await
                .map(|_| ())
                .map_err(|e| {
                    SFError::Validation(format!(
                        "cannot restore the previous contents of {path}: {e}"
                    ))
                }),
            (false, _) => tokio::fs::remove_file(path).await.map_err(|e| {
                SFError::Validation(format!("cannot remove the created file {path}: {e}"))
            }),
            (true, None) => Err(SFError::Config(format!(
                "journal for {path} says it existed but kept no copy"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_scope() -> (tempfile::TempDir, HostDocsConfig) {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("documents");
        std::fs::create_dir_all(&root).unwrap();
        let cfg = HostDocsConfig::from_spec(
            Some(&format!("alice={}", root.display())),
            dir.path().join("journal"),
            DEFAULT_MAX_DOC_BYTES,
        );
        (dir, cfg)
    }

    fn open_docs(cfg: &HostDocsConfig) -> Arc<HostDocs> {
        HostDocs::new(cfg.clone()).expect("host docs")
    }

    fn scope_root(cfg: &HostDocsConfig) -> PathBuf {
        cfg.scopes.get("alice").expect("scope").clone()
    }

    #[test]
    fn scope_spec_drops_entries_it_cannot_trust() {
        let cfg = HostDocsConfig::from_spec(
            Some("alice=/docs/alice, broken, bob=relative/path, carol=, =/docs/x, dave=/docs/dave"),
            PathBuf::from("/tmp/j"),
            DEFAULT_MAX_DOC_BYTES,
        );
        let names: Vec<&String> = cfg.scopes.keys().collect();
        assert_eq!(
            names,
            vec!["alice", "dave"],
            "a malformed or relative entry must not become a scope"
        );
    }

    #[test]
    fn absent_spec_means_the_capability_is_off() {
        let cfg = HostDocsConfig::from_spec(None, PathBuf::from("/tmp/j"), DEFAULT_MAX_DOC_BYTES);
        assert!(!cfg.is_enabled());
        let docs = open_docs(&cfg);
        let err = docs
            .plan("alice", &[HostDocOp::Mkdir { path: "a".into() }])
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown host document scope"),
            "{err}"
        );
    }

    #[test]
    fn absolute_paths_and_parent_components_are_refused_with_a_reason() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        for (path, needle) in [
            ("/etc/passwd", "absolute path refused"),
            ("../../etc/passwd", "contains `..`"),
            ("a/../../b", "contains `..`"),
            ("", "empty path"),
        ] {
            let err = docs
                .plan("alice", &[HostDocOp::Mkdir { path: path.into() }])
                .unwrap_err();
            assert!(err.to_string().contains(needle), "{path}: {err}");
        }
    }

    #[test]
    fn a_symlink_inside_the_scope_is_refused_not_followed() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        let docs = open_docs(&cfg);
        // A link out of the scope: even though the mount hides the host, this
        // container's own /etc is real, so following it would escape.
        std::os::unix::fs::symlink("/etc", root.join("elsewhere")).unwrap();
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "elsewhere/passwd".into(),
                    content: "x".into(),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("symbolic link"), "{err}");
        // A dangling link: writing through it would create the target file,
        // which is exactly the escape the check has to close.
        std::os::unix::fs::symlink(root.join("missing"), root.join("dangling")).unwrap();
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "dangling".into(),
                    content: "x".into(),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("symbolic link"), "{err}");
    }

    #[tokio::test]
    async fn plan_effects_match_what_apply_does() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::write(root.join("old.txt"), b"before").unwrap();
        std::fs::create_dir(root.join("keep")).unwrap();
        let docs = open_docs(&cfg);
        let ops = vec![
            HostDocOp::Mkdir {
                path: "archive".into(),
            },
            HostDocOp::Rename {
                from: "old.txt".into(),
                to: "archive/old.txt".into(),
            },
            HostDocOp::Write {
                path: "summary.md".into(),
                content: "sum".into(),
            },
            HostDocOp::Mkdir {
                path: "keep".into(),
            },
        ];
        let (plan, approval) = approved_plan(&docs, "alice", &ops).await;
        assert_eq!(
            plan.ops.iter().map(|p| p.effect).collect::<Vec<_>>(),
            vec![
                Effect::Create,
                Effect::Move,
                Effect::Create,
                Effect::Present
            ]
        );
        let applied = docs.apply(&plan, &approval).await.unwrap();
        assert_eq!(applied.applied.len(), 4);
        assert!(root.join("archive/old.txt").exists());
        assert!(!root.join("old.txt").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("summary.md")).unwrap(),
            "sum"
        );
        // A resolved path is always inside the scope, which is what makes the
        // plan reviewable at all.
        for planned in &plan.ops {
            for resolved in &planned.resolved {
                assert!(
                    Path::new(resolved).starts_with(&root),
                    "{resolved} escaped {}",
                    root.display()
                );
            }
        }
    }

    #[tokio::test]
    async fn apply_refuses_a_plan_whose_operations_were_edited() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        // The approval happens before the edit: what is changed is **the plan that was
        // already approved**, which is exactly what this check is there to stop.
        let (mut plan, approval) = approved_plan(
            &docs,
            "alice",
            &[HostDocOp::Write {
                path: "a.txt".into(),
                content: "one".into(),
            }],
        )
        .await;
        plan.ops[0].op = HostDocOp::Write {
            path: "a.txt".into(),
            content: "two".into(),
        };
        let err = docs.apply(&plan, &approval).await.unwrap_err();
        assert!(err.to_string().contains("plan hash"), "{err}");
    }

    #[tokio::test]
    async fn apply_refuses_when_the_scope_changed_since_the_plan() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        let docs = open_docs(&cfg);
        let (plan, approval) = approved_plan(
            &docs,
            "alice",
            &[HostDocOp::Write {
                path: "a.txt".into(),
                content: "one".into(),
            }],
        )
        .await;
        assert_eq!(plan.ops[0].effect, Effect::Create);
        // Something appears at the planned location between review and apply.
        std::fs::write(root.join("a.txt"), b"somebody else's work").unwrap();
        let err = docs.apply(&plan, &approval).await.unwrap_err();
        assert!(
            err.to_string().contains("the scope changed since the plan"),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("a.txt")).unwrap(),
            "somebody else's work",
            "a refused apply must not touch the file"
        );
    }

    #[tokio::test]
    async fn rollback_restores_replaced_created_moved_and_made_paths() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::write(root.join("notes.txt"), b"original contents").unwrap();
        std::fs::create_dir(root.join("keep")).unwrap();
        let docs = open_docs(&cfg);
        let (plan, approval) = approved_plan(
            &docs,
            "alice",
            &[
                HostDocOp::Mkdir {
                    path: "archive".into(),
                },
                HostDocOp::Rename {
                    from: "notes.txt".into(),
                    to: "archive/notes.txt".into(),
                },
                HostDocOp::Write {
                    path: "archive/notes.txt".into(),
                    content: "rewritten".into(),
                },
                HostDocOp::Write {
                    path: "new.md".into(),
                    content: "created".into(),
                },
            ],
        )
        .await;
        let applied = docs.apply(&plan, &approval).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("archive/notes.txt")).unwrap(),
            "rewritten"
        );
        let report = docs.rollback(&applied.journal_id).await.unwrap();
        assert_eq!(report.restored, 4);
        assert!(!root.join("new.md").exists(), "created file removed");
        assert!(!root.join("archive").exists(), "created directory removed");
        assert_eq!(
            std::fs::read_to_string(root.join("notes.txt")).unwrap(),
            "original contents",
            "the moved file came back with its previous contents"
        );
        // Rolling back twice must not undo somebody's later work.
        let err = docs.rollback(&applied.journal_id).await.unwrap_err();
        assert!(err.to_string().contains("already rolled back"), "{err}");
    }

    #[tokio::test]
    async fn a_failed_operation_rolls_back_the_applied_prefix() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::create_dir(root.join("dir")).unwrap();
        let docs = open_docs(&cfg);
        let ops = vec![
            HostDocOp::Write {
                path: "kept.txt".into(),
                content: "one".into(),
            },
            // The second write lands in a directory that exists while the plan
            // is made and disappears before apply, so the operation itself
            // fails even though its effect still holds. The write before it
            // must not survive.
            HostDocOp::Write {
                path: "dir/second.txt".into(),
                content: "two".into(),
            },
        ];
        let (plan, approval) = approved_plan(&docs, "alice", &ops).await;
        std::fs::remove_dir(root.join("dir")).unwrap();
        let err = docs.apply(&plan, &approval).await.unwrap_err();
        assert!(err.to_string().contains("rolled back"), "{err}");
        assert!(
            !root.join("kept.txt").exists(),
            "the applied prefix must not survive a failed plan"
        );
    }

    #[tokio::test]
    async fn a_write_larger_than_the_ceiling_is_refused_at_plan_time() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("documents");
        std::fs::create_dir_all(&root).unwrap();
        let cfg = HostDocsConfig::from_spec(
            Some(&format!("alice={}", root.display())),
            dir.path().join("journal"),
            16,
        );
        let docs = open_docs(&cfg);
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "big.txt".into(),
                    content: "x".repeat(17),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("ceiling"), "{err}");
    }

    #[tokio::test]
    async fn a_move_never_overwrites_an_existing_destination() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::write(root.join("a.txt"), b"a").unwrap();
        std::fs::write(root.join("b.txt"), b"b").unwrap();
        let docs = open_docs(&cfg);
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Rename {
                    from: "a.txt".into(),
                    to: "b.txt".into(),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(std::fs::read_to_string(root.join("b.txt")).unwrap(), "b");
    }

    #[tokio::test]
    async fn a_journal_id_that_is_not_a_name_is_refused() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        for id in ["../escape", "/absolute", "a/b", ""] {
            let err = docs.rollback(id).await.unwrap_err();
            assert!(
                err.to_string().contains("invalid journal id"),
                "{id}: {err}"
            );
        }
    }

    #[test]
    fn the_apply_vocabulary_is_published_at_zero() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let rendered = docs.metrics();
        for outcome in APPLY_OUTCOMES {
            assert_eq!(
                gates(&rendered, outcome),
                0.0,
                "every cell of the apply gate has to be published at zero: without the zero, 'nobody approved' and 'this gate has no reading at all' look the same"
            );
        }
    }

    #[tokio::test]
    async fn an_unapproved_plan_is_not_applied_and_the_refusal_names_what_is_missing() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        let docs = open_docs(&cfg);
        // Note that the capability here is built with the egress switch off (`from_spec`
        // default): producing a plan and staging it move no body, so they have to work with
        // the switch off as well -- which is the measured form of "the switch governs
        // bodies, not organising".
        assert!(!cfg.body_egress);
        let ops = vec![write("a.txt", "one")];
        let plan = docs.plan("alice", &ops).unwrap();
        let staged = docs.stage("alice", &ops).await.unwrap();
        assert_eq!(staged.state, StagedState::Pending);

        let err = docs.apply(&plan, &staged.staged_id).await.unwrap_err();
        assert!(err.to_string().contains("nothing has approved it"), "{err}");
        assert!(
            !root.join("a.txt").exists(),
            "not one character of an unapproved plan may land on disk"
        );

        // Only after the approval does the same plan become actionable -- which in turn
        // proves that the refusal above said "nobody approved it".
        docs.approve(&staged.staged_id, "alice").await.unwrap();
        docs.apply(&plan, &staged.staged_id).await.unwrap();
        assert_eq!(std::fs::read_to_string(root.join("a.txt")).unwrap(), "one");
    }

    #[tokio::test]
    async fn an_approval_covers_one_plan_and_is_used_once() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let (plan, approval) = approved_plan(&docs, "alice", &[write("a.txt", "one")]).await;
        docs.apply(&plan, &approval).await.unwrap();

        // One approval cannot be executed twice: 'already applied' and 'nobody approved it'
        // ask a person for different things.
        let err = docs.apply(&plan, &approval).await.unwrap_err();
        assert!(err.to_string().contains("already applied"), "{err}");
        assert!(err.to_string().contains("run twice"), "{err}");
    }

    #[tokio::test]
    async fn an_approval_is_for_one_plan_not_another() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        let docs = open_docs(&cfg);
        let staged = docs.stage("alice", &[write("a.txt", "one")]).await.unwrap();
        docs.approve(&staged.staged_id, "alice").await.unwrap();
        // A different plan, self-consistent in its own hash, arrives holding this
        // approval.
        let other = docs.plan("alice", &[write("b.txt", "two")]).unwrap();
        let err = docs.apply(&other, &staged.staged_id).await.unwrap_err();
        let text = err.to_string();
        assert!(text.contains("changed after it was reviewed"), "{text}");
        // The refusal has to carry both values the judgement used: given only "they do not
        // match", there is nothing to look up which of the two hashes is the odd one.
        assert!(
            text.contains(&staged.plan_hash),
            "the missing approval record hash: {text}"
        );
        assert!(
            text.contains(&other.plan_hash),
            "the missing plan-in-hand hash: {text}"
        );
        assert!(
            !root.join("b.txt").exists(),
            "a plan that does not match must not be acted on"
        );
    }

    #[tokio::test]
    async fn a_rejection_sticks_and_carries_its_reason() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let staged = docs.stage("alice", &[write("a.txt", "one")]).await.unwrap();
        let rejected = docs
            .reject(&staged.staged_id, Some("  not the tidy-up I asked for  "))
            .await
            .unwrap();
        assert_eq!(rejected.state, StagedState::Rejected);
        assert_eq!(
            rejected.rejected_reason.as_deref(),
            Some("not the tidy-up I asked for")
        );
        // After the rejection it cannot be approved: otherwise "rejected" is only a comment.
        let err = docs.approve(&staged.staged_id, "alice").await.unwrap_err();
        assert!(err.to_string().contains("rejected"), "{err}");
    }

    #[tokio::test]
    async fn an_approval_must_name_who_and_the_record_keeps_the_name() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let staged = docs.stage("alice", &[write("a.txt", "one")]).await.unwrap();
        let err = docs.approve(&staged.staged_id, "   ").await.unwrap_err();
        assert!(err.to_string().contains("name who approved"), "{err}");
        // Self-check: after the rejection the record is still pending, with no half-written
        // approval left behind.
        let list = docs.staged_plans().await.unwrap();
        assert_eq!(list.plans[0].state, StagedState::Pending);
        let approved = docs.approve(&staged.staged_id, "  ops  ").await.unwrap();
        assert_eq!(
            approved.approver.as_deref(),
            Some("ops"),
            "whitespace at both ends of the name has to be trimmed"
        );
    }

    #[tokio::test]
    async fn a_staged_plan_shows_its_state_and_the_reviewer_can_read_it_in_full() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let staged = docs.stage("alice", &[write("a.txt", "one")]).await.unwrap();
        assert_eq!(staged.state, StagedState::Pending);
        assert_eq!(staged.scope, "alice");
        assert_eq!(staged.op_count, 1);
        assert!(
            staged.expires_at_unix > staged.staged_at_unix,
            "the window has to have length, otherwise every plan expires before anyone opens it"
        );
        // The reviewer has to see the plan itself, not just how large it is.
        let record = docs.staged_plan(&staged.staged_id).await.unwrap();
        assert_eq!(record.staged_id, staged.staged_id);
        assert_eq!(record.plan.ops.len(), 1);
        assert_eq!(record.plan.ops[0].effect, Effect::Create);
        assert!(record.approval.is_none());

        let approved = docs.approve(&staged.staged_id, "ops").await.unwrap();
        assert_eq!(approved.state, StagedState::Approved);
        assert_eq!(approved.approver.as_deref(), Some("ops"));
        // The listing still holds one entry of metadata only: the body comes out only when
        // it is fetched by id.
        let list = docs.staged_plans().await.unwrap();
        assert_eq!(list.plans.len(), 1);
        assert_eq!(list.unreadable_records, 0);
    }

    #[tokio::test]
    async fn an_expired_plan_stays_visible_instead_of_disappearing() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let staged = docs.stage("alice", &[write("a.txt", "one")]).await.unwrap();
        docs.approve(&staged.staged_id, "ops").await.unwrap();
        // Time can only be waited for, not configured, so the window is moved to just past:
        // this cell measures the window judgement, not whether the machine is fast enough.
        let now = now_unix();
        rewrite_record(&docs, &staged.staged_id, |r| {
            r.expires_at_unix = now.saturating_sub(1)
        });
        let list = docs.staged_plans().await.unwrap();
        let mine = list
            .plans
            .iter()
            .find(|p| p.staged_id == staged.staged_id)
            .expect("an expired record still has to be listed");
        assert_eq!(mine.state, StagedState::Expired);
        assert_eq!(
            mine.approver.as_deref(),
            Some("ops"),
            "an approved-but-expired record has to show whether it was executed"
        );
    }

    #[tokio::test]
    async fn pruning_takes_terminal_records_and_leaves_the_just_expired_ones() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let rejected = docs
            .stage("alice", &[write("gone.txt", "x")])
            .await
            .unwrap();
        docs.reject(&rejected.staged_id, Some("calling the whole thing off"))
            .await
            .unwrap();
        let expired = docs
            .stage("alice", &[write("kept.txt", "x")])
            .await
            .unwrap();
        let fresh = docs
            .stage("alice", &[write("fresh.txt", "x")])
            .await
            .unwrap();
        let now = now_unix();
        for id in [&rejected.staged_id, &expired.staged_id] {
            rewrite_record(&docs, id, |r| r.expires_at_unix = now.saturating_sub(1));
        }

        // The next stage does maintenance along the way; every record it removes has to
        // leave a reading behind.
        docs.stage("alice", &[write("trigger.txt", "x")])
            .await
            .unwrap();
        let list = docs.staged_plans().await.unwrap();
        let ids: Vec<&str> = list.plans.iter().map(|p| p.staged_id.as_str()).collect();
        assert!(
            !ids.contains(&rejected.staged_id.as_str()),
            "a record that reached a terminal state and then passed its window should be removed"
        );
        assert!(
            ids.contains(&expired.staged_id.as_str()),
            "the one that just expired has to stay -- that is the one that gets asked about"
        );
        assert!(
            ids.contains(&fresh.staged_id.as_str()),
            "a plan still waiting for approval must not be touched"
        );
        assert_eq!(
            ops_cell(&docs.metrics(), "prune", "pruned"),
            1.0,
            "the cleanup deleted something, so it has to leave its own reading"
        );
    }

    #[tokio::test]
    async fn a_plan_over_the_review_ceiling_is_refused_before_it_is_stored() {
        let (_d, mut cfg) = temp_scope();
        cfg.max_plan_bytes = 200;
        let docs = open_docs(&cfg);
        let err = docs
            .stage("alice", &[write("a.txt", &"x".repeat(400))])
            .await
            .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("ceiling"), "{text}");
        assert!(
            text.contains("200"),
            "the refusal has to carry the value the judgement used, otherwise whoever adjusts the bound does not know in which direction: {text}"
        );
        assert!(
            docs.staged_plans().await.unwrap().plans.is_empty(),
            "the refusal happens before anything lands: no half-written record may appear in the directory"
        );
    }

    #[tokio::test]
    async fn every_published_apply_outcome_is_reachable() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);

        // released: an approved plan applied once.
        let (plan, approval) = approved_plan(&docs, "alice", &[write("a.txt", "one")]).await;
        docs.apply(&plan, &approval).await.unwrap();
        // refused_already_applied: the same approval, once more.
        assert!(docs.apply(&plan, &approval).await.is_err());

        // refused_no_record: no such record on disk.
        assert!(docs
            .apply(&plan, "1700000000000-0-aaaaaaaaaaaa")
            .await
            .is_err());

        // refused_unreadable_record: the record is on disk but cannot be read. The
        // difference from the previous cell is exactly the difference between "fix the
        // caller" and "inspect this volume", so a broken record really has to be built.
        let corrupt = "1700000000001-0-bbbbbbbbbbbb";
        std::fs::create_dir_all(docs.staged_dir()).unwrap();
        std::fs::write(
            docs.staged_dir().join(format!("{corrupt}.json")),
            b"{ not json",
        )
        .unwrap();
        assert!(docs.apply(&plan, corrupt).await.is_err());

        // refused_not_approved: staged, but nobody approved it.
        let staged = docs.stage("alice", &[write("b.txt", "two")]).await.unwrap();
        let unapproved = docs.plan("alice", &[write("b.txt", "two")]).unwrap();
        assert!(docs.apply(&unapproved, &staged.staged_id).await.is_err());

        // refused_hash_mismatch: what was approved is a different plan.
        let staged_c = docs
            .stage("alice", &[write("c.txt", "three")])
            .await
            .unwrap();
        docs.approve(&staged_c.staged_id, "ops").await.unwrap();
        let plan_d = docs.plan("alice", &[write("d.txt", "four")]).unwrap();
        assert!(docs.apply(&plan_d, &staged_c.staged_id).await.is_err());

        // refused_rejected: it was rejected.
        let staged_e = docs
            .stage("alice", &[write("e.txt", "five")])
            .await
            .unwrap();
        docs.reject(&staged_e.staged_id, None).await.unwrap();
        let plan_e = docs.plan("alice", &[write("e.txt", "five")]).unwrap();
        assert!(docs.apply(&plan_e, &staged_e.staged_id).await.is_err());

        // refused_expired: the window has passed (which also proves here that
        // approved-but-expired is no help).
        let staged_f = docs.stage("alice", &[write("f.txt", "six")]).await.unwrap();
        docs.approve(&staged_f.staged_id, "ops").await.unwrap();
        rewrite_record(&docs, &staged_f.staged_id, |r| r.expires_at_unix = 1);
        let plan_f = docs.plan("alice", &[write("f.txt", "six")]).unwrap();
        assert!(docs.apply(&plan_f, &staged_f.staged_id).await.is_err());

        let rendered = docs.metrics();
        let observed = observed_outcomes(
            &[rendered],
            "sandbox_host_docs_apply_total",
            &APPLY_OUTCOMES,
        );
        let expected: Vec<String> = APPLY_OUTCOMES.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            observed, expected,
            "measured landing cells do not match the vocabulary: a missing cell can never be read, and an extra one is read by nobody"
        );
    }

    /// One cell of the reading, taken from the **rendered** text.
    ///
    /// Rendered output rather than the in-memory counter: this is the same text an operator
    /// sees on `/metrics`, so "this cell is published" is said about the real reading
    /// surface.
    fn cell(rendered: &str, series: &str, outcome: &str) -> f64 {
        let needle = format!("{series}{{outcome=\"{outcome}\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(&needle))
            .and_then(|value| value.trim().parse::<f64>().ok())
            .unwrap_or_else(|| {
                panic!("no {series}{{outcome=\"{outcome}\"}} in the reading:\n{rendered}")
            })
    }

    fn reads(rendered: &str, outcome: &str) -> f64 {
        cell(rendered, "sandbox_host_docs_reads_total", outcome)
    }

    fn lists(rendered: &str, outcome: &str) -> f64 {
        cell(rendered, "sandbox_host_docs_lists_total", outcome)
    }

    fn gates(rendered: &str, outcome: &str) -> f64 {
        cell(rendered, "sandbox_host_docs_apply_total", outcome)
    }

    /// The `ops` counter is two-dimensional, so a value needs a kind as well: labels sort by
    /// name, so in the needle the kind comes first and the outcome second, matching the line
    /// as rendered.
    fn ops_cell(rendered: &str, kind: &str, outcome: &str) -> f64 {
        let needle =
            format!("sandbox_host_docs_ops_total{{kind=\"{kind}\",outcome=\"{outcome}\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(&needle))
            .and_then(|value| value.trim().parse::<f64>().ok())
            .unwrap_or_else(|| {
                panic!("no sandbox_host_docs_ops_total{{kind=\"{kind}\",outcome=\"{outcome}\"}} in the reading")
            })
    }

    fn write(path: &str, content: &str) -> HostDocOp {
        HostDocOp::Write {
            path: path.into(),
            content: content.into(),
        }
    }

    /// Collect the cells that **actually** appeared across several readings (a value above
    /// zero counts).
    ///
    /// Measured cells rather than hard-coded expectations: a hard-coded expectation passes
    /// just as well when not one line ran. Several readings because one cell is reachable
    /// only **on another instance** -- a switch has one value per process.
    fn observed_outcomes(rendered: &[String], series: &str, vocabulary: &[&str]) -> Vec<String> {
        vocabulary
            .iter()
            .filter(|outcome| {
                rendered
                    .iter()
                    .any(|text| cell(text, series, outcome) >= 1.0)
            })
            .map(|outcome| outcome.to_string())
            .collect()
    }

    fn chmod(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    /// A scope this process cannot read: directory 0o000, and a file under it 0o000.
    ///
    /// Building `unreadable` with it is deliberate: that cell's cause is "this process
    /// cannot read it", which is neither "it does not exist" nor "it is not text", and only
    /// the permission bits can produce it reliably in a test. Running the tests as root,
    /// root ignores the permission bits and that step cannot reach this cell -- so the test
    /// carries its own self-check saying plainly "this environment cannot produce it" rather
    /// than skipping silently.
    fn locked_scope(dir: &Path) -> PathBuf {
        let root = dir.join("locked");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("secret.txt"), b"secret").unwrap();
        chmod(&root.join("secret.txt"), 0o000);
        chmod(&root, 0o000);
        root
    }

    /// Fill a scope: `notes.txt`, the subdirectory `archive/`, and the symlink `link`
    /// (pointing outside the scope).
    fn populate(cfg: &HostDocsConfig) -> PathBuf {
        let root = scope_root(cfg);
        std::fs::write(root.join("notes.txt"), b"hello").unwrap();
        std::fs::create_dir_all(root.join("archive")).unwrap();
        std::fs::write(root.join("archive/old.txt"), b"old").unwrap();
        std::os::unix::fs::symlink("/etc/passwd", root.join("link")).unwrap();
        root
    }

    /// A configuration with the egress switch on: with it off, the read face only ever
    /// answers one refusal and nothing else is reachable.
    fn reading(cfg: &HostDocsConfig) -> HostDocsConfig {
        let mut on = cfg.clone();
        on.body_egress = true;
        on
    }

    /// Walk the approval face once: plan, stage, approve, returning the plan and its
    /// approval id.
    ///
    /// These tests are about what applying gets right, and applying now requires an
    /// approval record on disk -- so every step really is walked, rather than leaving apply a
    /// test-only back door: with a back door in place, a passing test says nothing about
    /// whether this gate is connected.
    async fn approved_plan(
        docs: &HostDocs,
        scope: &str,
        ops: &[HostDocOp],
    ) -> (HostDocPlan, String) {
        let plan = docs.plan(scope, ops).unwrap();
        let staged = docs.stage(scope, ops).await.unwrap();
        assert_eq!(
            staged.plan_hash, plan.plan_hash,
            "self-check: the staged plan is not the plan in hand, so the approval that follows would approve the wrong thing"
        );
        docs.approve(&staged.staged_id, "test").await.unwrap();
        (plan, staged.staged_id)
    }

    /// Rewrite a staged record directly to the given contents, to produce "time has passed",
    /// a state that can only be waited for and not configured.
    fn rewrite_record(docs: &HostDocs, staged_id: &str, patch: impl FnOnce(&mut StagedPlan)) {
        let path = docs.staged_dir().join(format!("{staged_id}.json"));
        let mut record: StagedPlan =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        patch(&mut record);
        std::fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    }

    #[test]
    fn a_listing_carries_metadata_and_counts_what_it_left_out() {
        let (_d, cfg) = temp_scope();
        let root = populate(&cfg);
        let docs = open_docs(&cfg);
        let listing = docs.list("alice", None).expect("listing");
        let paths: Vec<&str> = listing.entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["archive", "archive/old.txt", "notes.txt"],
            "the listing should hold only entries that really exist, ordered by path"
        );
        let notes = listing
            .entries
            .iter()
            .find(|e| e.path == "notes.txt")
            .expect("notes.txt");
        assert_eq!(notes.kind, EntryKind::File);
        assert_eq!(notes.bytes, 5);
        assert!(
            notes.modified_unix.is_some(),
            "the mtime should be readable"
        );
        let archive = listing
            .entries
            .iter()
            .find(|e| e.path == "archive")
            .expect("archive");
        assert_eq!(archive.kind, EntryKind::Dir);
        assert_eq!(archive.bytes, 0, "a directory has no byte count");
        // A symlink outside the scope is neither in the listing (not followed) nor "never
        // there": the count has to say so, otherwise "that is all of them" and "there are
        // more I did not list" read exactly the same.
        assert_eq!(listing.skipped_symlinks, 1);
        assert_eq!(listing.skipped_unnamed, 0);
        assert!(!listing.truncated);
        assert!(
            root.join("link").exists(),
            "self-check: the link really is on disk"
        );
    }

    #[test]
    fn a_listing_that_hit_its_ceiling_says_so() {
        let (_d, mut cfg) = temp_scope();
        let root = scope_root(&cfg);
        for i in 0..5 {
            std::fs::write(root.join(format!("f{i}.txt")), b"x").unwrap();
        }
        cfg.max_list_entries = 2;
        let docs = open_docs(&cfg);
        let listing = docs.list("alice", None).expect("listing");
        assert_eq!(
            listing.entries.len(),
            2,
            "the ceiling bounds the produced side"
        );
        assert!(
            listing.truncated,
            "a truncated listing looks just like a small directory, and the caller will typically organise from it"
        );
    }

    #[test]
    fn a_prefix_that_is_not_there_is_refused_rather_than_listed_empty() {
        let (_d, cfg) = temp_scope();
        populate(&cfg);
        let docs = open_docs(&cfg);
        let err = docs.list("alice", Some("nope")).unwrap_err();
        assert!(err.to_string().contains("has no such prefix"), "{err}");
        // The other direction: a directory that really is empty goes down the success path,
        // not the error path -- so the two are distinguishable.
        std::fs::create_dir_all(scope_root(&cfg).join("empty")).unwrap();
        let listing = docs
            .list("alice", Some("empty"))
            .expect("an empty directory");
        assert!(listing.entries.is_empty());
        assert_eq!(listing.prefix.as_deref(), Some("empty"));
        // When the prefix names a file, that one entry is the answer.
        let one = docs
            .list("alice", Some("notes.txt"))
            .expect("a file prefix");
        assert_eq!(one.entries.len(), 1);
        assert_eq!(one.entries[0].path, "notes.txt");
    }

    #[test]
    fn a_prefix_that_leaves_the_scope_is_refused() {
        let (_d, cfg) = temp_scope();
        populate(&cfg);
        let docs = open_docs(&cfg);
        // One case per kind of out-of-bounds path, checking **the reason for the refusal**:
        // saying only "it failed" lets the judgement be swapped for another cause and still
        // pass.
        for (prefix, expected) in [
            ("../etc", "`..`"),
            ("/etc", "absolute path refused"),
            ("archive/../../etc", "`..`"),
            ("link", "symbolic link"),
        ] {
            let err = docs.list("alice", Some(prefix)).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "{prefix} was refused for a reason that is not {expected}: {err}"
            );
        }
    }

    #[test]
    fn the_listing_vocabulary_is_published_at_zero() {
        let (_d, cfg) = temp_scope();
        let docs = open_docs(&cfg);
        let rendered = docs.metrics();
        for outcome in LIST_OUTCOMES {
            assert_eq!(
                lists(&rendered, outcome),
                0.0,
                "\"nobody came to list\" and \"it was listed\" have to be distinguishable, \
                 so the {outcome} cell has to be there first, and at zero"
            );
        }
    }

    #[test]
    fn every_published_listing_outcome_is_reachable() {
        let (dir, cfg) = temp_scope();
        populate(&cfg);
        let docs = open_docs(&cfg);

        docs.list("alice", None).expect("listed");
        docs.list("nobody", None).expect_err("unknown_scope");
        docs.list("alice", Some("../etc"))
            .expect_err("refused_path");
        docs.list("alice", Some("nope")).expect_err("not_found");

        let locked = locked_scope(dir.path());
        let mut hard = cfg.clone();
        hard.scopes.insert("locked".into(), locked.clone());
        let hard_docs = open_docs(&hard);
        let locked_err = hard_docs
            .list("locked", None)
            .expect_err("a 0o000 directory should not be listable");
        assert!(
            locked_err.to_string().contains("cannot list"),
            "self-check: this cell wants \"cannot read it\" (this process's problem), not another cause: {locked_err}"
        );

        let rendered = [docs.metrics(), hard_docs.metrics()];
        let observed =
            observed_outcomes(&rendered, "sandbox_host_docs_lists_total", &LIST_OUTCOMES);
        let expected: Vec<String> = LIST_OUTCOMES.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            observed, expected,
            "measured landing cells do not match the vocabulary: a missing cell can never be read, and an extra one is read by nobody"
        );
        chmod(&locked, 0o755);
    }

    #[test]
    fn reading_with_the_switch_off_is_a_refusal_not_an_empty_body() {
        let (_d, cfg) = temp_scope();
        populate(&cfg);
        let docs = open_docs(&cfg);
        assert!(!cfg.body_egress, "the default has to be off");
        let refusal = docs.read("alice", "notes.txt").unwrap_err();
        assert_eq!(refusal.outcome, "disabled");
        assert!(
            refusal
                .error
                .to_string()
                .contains("refusal, not empty content"),
            "with the switch off it has to be an outright refusal: {}",
            refusal.error
        );
        assert_eq!(reads(&docs.metrics(), "disabled"), 1.0);
        // The other direction: with the switch on, the same read returns content. Without
        // this, "always refuse" would pass the assertion above as well.
        let on_docs = open_docs(&reading(&cfg));
        let read = on_docs
            .read("alice", "notes.txt")
            .expect("with the switch on it should read");
        assert_eq!(read.content, "hello");
        assert_eq!(read.bytes, 5);
        assert_eq!(read.path, "notes.txt");
        assert!(read.modified_unix.is_some());
    }

    #[test]
    fn reading_refuses_a_body_over_the_ceiling_naming_both_numbers() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::write(root.join("big.txt"), vec![b'x'; 64]).unwrap();
        let mut on = reading(&cfg);
        on.max_read_bytes = 16;
        let docs = open_docs(&on);
        let refusal = docs.read("alice", "big.txt").unwrap_err();
        assert_eq!(refusal.outcome, "over_ceiling");
        let text = refusal.error.to_string();
        assert!(
            text.contains("64") && text.contains("16"),
            "both numbers have to be there (how big a thing hit how small a bound): {text}"
        );
    }

    #[test]
    fn reading_refuses_a_directory_and_non_utf8_bytes_as_not_text() {
        let (_d, cfg) = temp_scope();
        let root = populate(&cfg);
        std::fs::write(root.join("blob.bin"), [0xff, 0xfe, 0x00]).unwrap();
        let docs = open_docs(&reading(&cfg));
        for path in ["archive", "blob.bin"] {
            let refusal = docs.read("alice", path).unwrap_err();
            assert_eq!(refusal.outcome, "not_text", "{path}");
        }
        // The binary cell has to say at which byte it cannot go on, otherwise there is
        // nothing to act on in "it is not text".
        let refusal = docs.read("alice", "blob.bin").unwrap_err();
        assert!(
            refusal.error.to_string().contains("UTF-8"),
            "{:?}",
            refusal.error
        );
        // A symlink and an out-of-bounds path share one cell: content read through a link
        // belongs outside the scope, so it is not "read".
        assert_eq!(
            docs.read("alice", "link").unwrap_err().outcome,
            "refused_path"
        );
    }

    #[test]
    fn every_published_read_outcome_is_reachable() {
        let (dir, cfg) = temp_scope();
        let root = populate(&cfg);
        std::fs::write(root.join("big.txt"), vec![b'x'; 64]).unwrap();
        std::fs::write(root.join("blob.bin"), [0xff, 0xfe]).unwrap();
        let mut on = reading(&cfg);
        on.max_read_bytes = 16;
        let docs = open_docs(&on);

        assert!(docs.read("alice", "notes.txt").is_ok(), "read");
        for (path, expected) in [
            ("../etc/passwd", "refused_path"),
            ("link", "refused_path"),
            ("nope.txt", "not_found"),
            ("big.txt", "over_ceiling"),
            ("blob.bin", "not_text"),
            ("archive", "not_text"),
        ] {
            let refusal = docs.read("alice", path).unwrap_err();
            assert_eq!(refusal.outcome, expected, "{path}");
        }
        assert_eq!(
            docs.read("nobody", "x").unwrap_err().outcome,
            "unknown_scope"
        );

        // The switch-off cell is reachable only on another instance: a switch has one value
        // per process.
        let off_docs = open_docs(&cfg);
        assert_eq!(
            off_docs.read("alice", "notes.txt").unwrap_err().outcome,
            "disabled"
        );

        let locked = locked_scope(dir.path());
        let mut hard = cfg.clone();
        hard.body_egress = true;
        hard.scopes.insert("locked".into(), locked.clone());
        let hard_docs = open_docs(&hard);
        let locked_err = hard_docs
            .read("locked", "secret.txt")
            .expect_err("a 0o000 file should not be readable");
        assert_eq!(
            locked_err.outcome, "unreadable",
            "self-check: this cell wants \"cannot read it\": {:?}",
            locked_err.error
        );

        let rendered = [docs.metrics(), off_docs.metrics(), hard_docs.metrics()];
        let observed =
            observed_outcomes(&rendered, "sandbox_host_docs_reads_total", &READ_OUTCOMES);
        let expected: Vec<String> = READ_OUTCOMES.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            observed, expected,
            "measured landing cells do not match the vocabulary: a missing cell can never be read, and an extra one is read by nobody"
        );
        chmod(&locked, 0o755);
    }
}
