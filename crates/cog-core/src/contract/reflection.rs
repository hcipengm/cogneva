use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A detected pattern that groups related learnings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    pub key: String,
    pub description: String,
    pub learning_ids: Vec<String>,
    pub recurrence_count: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// A single usage observation of a skill in production.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillOutcome {
    pub skill_id: String,
    /// Task type fingerprint (e.g. "backend:api:migration").
    pub task_signature: String,
    pub success: bool,
    /// The score the producing squad reported for the run, when it reported
    /// one; callers that encode the outcome instead set 1.0 on success and 0.0
    /// on failure. It is neither produced by the self-review loop nor read by
    /// any gate — it is what the producer said about its own work, which is the
    /// only thing a run's outcome can carry across the process boundary.
    pub score: Option<f32>,
    /// Wall-clock latency in milliseconds.
    pub latency_ms: u64,
    /// Total token cost (input + output).
    pub token_cost: u64,
    pub observed_at: DateTime<Utc>,
}

/// Cross-session reflection engine for learning detection and self-improvement.
#[async_trait::async_trait]
pub trait ReflectionEngine: Send + Sync + std::fmt::Debug {
    /// Process tool execution results for pattern detection.
    async fn process_tool_result(
        &self,
        tool_name: &str,
        result: &serde_json::Value,
        is_error: bool,
    ) -> crate::SFResult<()>;

    /// Process a full context window after a run completes.
    async fn process_context(&self, messages: &[crate::Message]) -> crate::SFResult<()>;

    /// Process an agent event through the learning pipeline.
    async fn process_event(&self, event: &crate::AgentEvent) -> crate::SFResult<()>;

    /// Trigger skill extraction from a mature pattern.
    async fn extract_and_insert(&self, pattern: &Pattern) -> crate::SFResult<Option<String>>;

    /// Feed a skill usage outcome into the effectiveness tracker.
    async fn process_skill_outcome(&self, outcome: SkillOutcome) -> crate::SFResult<()>;

    /// Start the background periodic reviewer if configured.
    fn start_reviewer(&self) -> Option<tokio::task::JoinHandle<()>>;

    /// Record the outcome of a Squad run so reflection can learn from
    /// collaboration quality.
    async fn record_squad_result(
        &self,
        task_id: &str,
        goal: &str,
        success: bool,
        pge_mode: &str,
        score: Option<f32>,
        latency_ms: u64,
    ) -> crate::SFResult<()>;

    /// Record the outcome of a generated change after it has been applied,
    /// tested, built, or deployed.
    async fn record_change_outcome(
        &self,
        change_id: &str,
        success: bool,
        test_output: &str,
    ) -> crate::SFResult<()>;
}

/// A code change produced by the collaboration pipeline for self-evolution.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GeneratedChange {
    pub change_id: String,
    pub goal: String,
    pub content: String,
    pub affected_files: Vec<String>,
    pub rationale: Option<String>,
    pub pge_mode: String,
    /// The score the producing squad reported for the run that produced this
    /// change. Nothing compares it to a threshold: whether a change lands is
    /// decided elsewhere, so this is a reading of what the producer said and
    /// never evidence that the change passed a gate. The self-review loop's own
    /// score is a different number and lives in
    /// [`crate::SelfReviewResult`].
    pub self_review_score: Option<f32>,
    /// Public issue this change resolves, when the intent came from a tracked
    /// issue. Sinks use it to link the PR back (`Fixes #N`) so competing
    /// solutions for the same issue can be grouped.
    pub issue_number: Option<u64>,
    /// Which entry point produced this change, carried forward from the task.
    ///
    /// On the change rather than looked up from the task later: the stages that
    /// need it — landing and its census — run long after the task is gone, and
    /// a reading that has to resolve an identity against a deleted row reports
    /// nothing exactly when the backlog is oldest. Records written before this
    /// field read as `None`, which the census reports as unattributed.
    pub intent: Option<crate::types::task::EvolutionIntent>,
}

/// Which deterministic criterion a generated change failed.
///
/// A change can be refused by the gate for reasons that call for opposite
/// responses — a diff that does not parse is a generation defect, a patch whose
/// context no longer fits the tree is staleness, and a verification run that
/// could not start is neither — and every one of them used to leave the same
/// trace: one sentence of English in a free-text field, and one increment of an
/// aggregate counter. A count that cannot be split cannot be acted on: a run of
/// `tests_failed` says fix the generator, a run of `context_does_not_apply` says
/// fix the freshness of what it reads from, and the two are indistinguishable
/// once they share a number.
///
/// Closed, with no catch-all variant. A catch-all would be the same defect one
/// level down: an unclassified refusal would land in a bucket named "other" and
/// every reading taken from this axis would silently include it. The variants
/// below are the complete set of verdicts the gate can reach, and
/// [`Self::ALL`] carries them so a reader can publish the whole axis — including
/// the causes that have never happened, since an absent series and a zero read
/// alike to a scraper.
///
/// This is the criterion, not the outcome. Whether a refused change is retired
/// or retried, and whether the refusal is terminal, are separate questions the
/// caller answers; the cause is what the caller dispatches on when it decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionCause {
    /// The artifact is not a parseable unified diff.
    MalformedDiff,
    /// The promotion policy refuses the target set — a protected file, or a
    /// diff too large to review.
    PromotionGateRefused,
    /// The change touches a path it may not, or rewrites one that is not there.
    ForbiddenPath,
    /// The change does not answer the goal it carries.
    IntentMismatch,
    /// The patch does not fit the tree it is applied to: the dry-run rejected
    /// its context.
    ContextDoesNotApply,
    /// The dry run passed and the real apply failed anyway.
    ApplyFailed,
    /// The applied change is not what this workspace's formatter produces, so
    /// the commit it would land as is one CI rejects on its format check.
    FormattingDiffers,
    /// The verification suite could not be run to a verdict — no cargo, or the
    /// run outlived its budget.
    TestRunUnavailable,
    /// The suite ran and this change broke it.
    TestsFailed,
    /// The change writes a line the linter reports on.
    ///
    /// A whole-tree lint criterion cannot say this much: the same report holds
    /// the lints the tree already carried, and a change judged on that set is
    /// refused for a defect it did not write — which, on a tree that is already
    /// carrying one, refuses every change including the one that clears it.
    /// The attribution is by position: the diagnostic's span overlaps a line
    /// this change writes.
    LintIntroduced,
    /// The change writes a value nothing can read: every line it writes is a
    /// literal inside an `impl Default for *Config`, and every configuration
    /// document a deployment ships writes that key in that config's section.
    ///
    /// A document is deserialized into the type and the built-in default is
    /// only reached for a key the document omits. When every document writes
    /// the key, that default is not the value any process reads — the change
    /// has no effect anywhere, and no later reading can tell it apart from one
    /// that was never applied.
    UnreachableDefault,
}

impl RejectionCause {
    /// Every cause, so a reader publishes the axis instead of the values it
    /// happens to have seen.
    pub const ALL: &'static [RejectionCause] = &[
        Self::MalformedDiff,
        Self::PromotionGateRefused,
        Self::ForbiddenPath,
        Self::IntentMismatch,
        Self::ContextDoesNotApply,
        Self::ApplyFailed,
        Self::FormattingDiffers,
        Self::TestRunUnavailable,
        Self::TestsFailed,
        Self::LintIntroduced,
        Self::UnreachableDefault,
    ];

    /// The wire form, for label values and records.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MalformedDiff => "malformed_diff",
            Self::PromotionGateRefused => "promotion_gate_refused",
            Self::ForbiddenPath => "forbidden_path",
            Self::IntentMismatch => "intent_mismatch",
            Self::ContextDoesNotApply => "context_does_not_apply",
            Self::ApplyFailed => "apply_failed",
            Self::FormattingDiffers => "formatting_differs",
            Self::TestRunUnavailable => "test_run_unavailable",
            Self::TestsFailed => "tests_failed",
            Self::LintIntroduced => "lint_introduced",
            Self::UnreachableDefault => "unreachable_default",
        }
    }

    /// Where this cause sits in [`Self::ALL`].
    ///
    /// A reader keeps one counter per cause and addresses them by this. The
    /// invariant it leans on — that the list is every variant, once each — is
    /// held by the test beside this type rather than by a second hand-written
    /// ordering, which would be one more list to drift.
    pub fn slot(self) -> usize {
        Self::ALL
            .iter()
            .position(|cause| *cause == self)
            .expect("ALL lists every rejection cause")
    }
}

/// Sink for collaboration-generated changes. Implemented by the reflection
/// layer so that collaboration does not depend on reflection concrete types.
#[async_trait::async_trait]
pub trait ChangeSink: Send + Sync + std::fmt::Debug {
    /// Submit a generated change for persistence and downstream deployment.
    /// Returns the artifact id assigned by the sink.
    async fn submit_change(&self, change: GeneratedChange) -> crate::SFResult<String>;
}

/// A revision the sandbox has built and deployed, sitting in a local
/// repository the caller can read.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LandedSource {
    /// Local repository holding `rev`.
    pub repo: std::path::PathBuf,
    /// Commit the sandbox built and deployed.
    pub rev: String,
}

/// Flows a change the sandbox has verified onto the upstream base branch.
///
/// Implemented by the platform integration, which owns the git egress and the
/// CI signal; consumed by the evolution mainline, which calls it only after
/// the sandbox deploy succeeded. No branch and no pull request are involved:
/// the commit reaches the base branch directly, and a failing CI run on that
/// commit is answered by reverting the branch and re-driving generation once.
#[async_trait::async_trait]
pub trait ChangeLanding: Send + Sync + std::fmt::Debug {
    /// Land `change` on the base branch.
    ///
    /// Durable: the intent survives a process restart (switching to the newly
    /// built binary replaces this process), and an unfinished landing is
    /// retried by the channel's own loop. Idempotent per change: one already
    /// on the base branch is not landed twice.
    async fn land(
        &self,
        change: &GeneratedChange,
        source: Option<&LandedSource>,
    ) -> crate::SFResult<String>;

    /// Record a generated change that no sandbox has verified yet, so the
    /// channel reports it rather than dropping it silently. Such a change is
    /// landed only when a mainline later verifies it (or the owner approves
    /// it explicitly).
    async fn record_unverified(&self, change: &GeneratedChange) -> crate::SFResult<()>;

    /// The recorded changes still awaiting verification.
    ///
    /// "A mainline later verifies it" is a promise about work someone does,
    /// so the verify loop has to be able to read the set it applies to. The
    /// record is the durable side of that handoff: a change generated in one
    /// deployment is verified by whichever deployment owns the sandbox, and
    /// the record is all the two share.
    async fn unverified_changes(&self) -> crate::SFResult<Vec<GeneratedChange>>;

    /// Settle a change that verification proved must not land.
    ///
    /// Terminal, and the reason is kept: an unverified record that can never
    /// be applied would otherwise be re-verified on every pass and re-offered
    /// to the owner forever, and "rejected for this reason" would be
    /// indistinguishable from "never submitted".
    async fn retire_unverified(&self, change_id: &str, reason: &str) -> crate::SFResult<()>;

    /// Whether a diff is inside the surface this instance may contribute at
    /// all, decided without touching a remote, a branch or a working tree.
    ///
    /// `land` reaches the same verdict, but it reaches it at the far end of a
    /// pipeline that has already applied the change, run the workspace test
    /// suite and release-built it -- and that build is the single slot every
    /// other mainline step queues behind, so a change the rules will refuse
    /// every time spends twelve minutes of it before anyone asks. Asking first
    /// changes nothing about the answer and everything about what the answer
    /// costs.
    ///
    /// Takes the diff rather than the change because the diff is all it reads,
    /// and is not `async` for the same reason: an answer that needs no I/O is
    /// what makes asking it up front reasonable.
    ///
    /// A refusal crosses as [`crate::SFError::Validation`] -- the type a path
    /// refusal from `land` already crosses as -- so a caller that sees one
    /// settles the change instead of running it again. What is deliberately
    /// *not* part of this answer: the owner's `forbidden_paths` and the
    /// changed-line cap. Those are policy, re-read on every landing, and the
    /// cap is the one gate owner approval waives; settling a change early on
    /// them would take a decision the owner still holds.
    fn check_contribution_allowed(&self, diff: &str) -> crate::SFResult<()>;

    /// Which of the paths that diff names the surface refuses, in the order the
    /// diff names them. Empty for a diff that may flow upstream.
    ///
    /// The same rule [`Self::check_contribution_allowed`] decides with, asked
    /// for its reason in structure rather than only in the sentence the verdict
    /// crosses as. A refusal has to be recorded under its criterion and the
    /// files it was refused on — those two are what the recurrence key is built
    /// from and what a next attempt is aimed at — and reading them back out of
    /// the message would make a second parser of the one rule, judging the
    /// patch a second time. An implementor answers both from one predicate.
    fn contribution_refusal_paths(&self, diff: &str) -> crate::SFResult<Vec<std::path::PathBuf>>;
}

/// Owner policy for flowing evolved changes back upstream as PRs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContributionPolicy {
    /// Changes that pass the quality gates are PR'd without asking (default —
    /// the instance is genuinely autonomous).
    #[default]
    Auto,
    /// Passing changes are staged and listed for the owner; each one is PR'd
    /// only after explicit approval.
    Ask,
    /// Changes are staged locally and never submitted upstream.
    Local,
}

impl ContributionPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Ask => "ask",
            Self::Local => "local",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "auto" => Some(Self::Auto),
            "ask" => Some(Self::Ask),
            "local" => Some(Self::Local),
            _ => None,
        }
    }
}

/// Owner-facing control over the contribution channel: read/switch the
/// policy, inspect the staged backlog, and flush it on demand.
/// Implemented by the platform integration (which owns the staging dir and
/// the live PR sink); consumed by the gateway admin API. Policy persistence
/// is the gateway's job (it owns the cluster Secret); this handle keeps the
/// in-process view.
#[async_trait::async_trait]
pub trait ContributionControl: Send + Sync {
    fn policy(&self) -> ContributionPolicy;
    fn set_policy(&self, policy: ContributionPolicy);
    /// Staged changes awaiting a publish decision, oldest first.
    async fn pending(&self) -> crate::SFResult<Vec<GeneratedChange>>;
    /// Submit staged changes through the live sink, bypassing the policy gate
    /// (the owner's click IS the approval). `change_id = None` flushes all.
    /// Errors when the channel is not connected.
    async fn flush_pending(&self, change_id: Option<&str>) -> crate::SFResult<usize>;
}

/// How a diff treats the file it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffTargetKind {
    /// The diff rewrites a file that already exists.
    Modify,
    /// The diff creates a file that does not exist yet. A patch that creates a
    /// file is the only kind whose target may be absent, and git marks it as
    /// such: the old side is `/dev/null`.
    Create,
    /// The diff deletes the file it names.
    Delete,
}

/// One file a diff touches, with the way it touches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffTarget {
    pub path: String,
    pub kind: DiffTargetKind,
}

/// Which files a diff touches, and how, in the order it introduces them.
///
/// Read through the same section grammar as [`diff_file_entries`], so a body
/// line that merely looks like a header cannot invent a target. Whether the
/// target is expected to exist is a property of the diff, not of the tree it
/// will be applied to, and deriving it here is what lets a caller tell a file
/// the patch creates from a path the generator made up.
pub fn parse_diff_targets(content: &str) -> Vec<DiffTarget> {
    diff_file_entries(content)
        .into_iter()
        .filter(|entry| !entry.path.is_empty() && entry.path != "/dev/null")
        .map(|entry| DiffTarget {
            kind: target_kind(&entry.header),
            path: entry.path,
        })
        .collect()
}

/// A section's side markers say whether the file exists before the patch.
///
/// A section claiming both sides are absent is not a shape git produces, so it
/// keeps the strictest reading — an existing file — and lets the apply gate
/// reject the patch on its own terms.
fn target_kind(header: &str) -> DiffTargetKind {
    let mut creates = false;
    let mut deletes = false;
    for line in header.lines() {
        if let Some(rest) = line.strip_prefix("--- ") {
            creates = diff_path_field(rest) == "/dev/null";
        } else if let Some(rest) = line.strip_prefix("+++ ") {
            deletes = diff_path_field(rest) == "/dev/null";
        }
    }
    match (creates, deletes) {
        (true, false) => DiffTargetKind::Create,
        (false, true) => DiffTargetKind::Delete,
        _ => DiffTargetKind::Modify,
    }
}

/// Parse a unified diff change and return the list of files it touches.
///
/// Extracts the paths of every target in [`parse_diff_targets`], deletions
/// included: a deletion names its file only on the side that goes away, so a
/// caller reading paths off the `+++` line alone cannot see it, and a file the
/// change removes is one the change affects as much as one it rewrites. This is
/// a pure function shared by collaboration (static validation) and reflection
/// (change pipeline, which applies the same rules to deletions).
pub fn parse_diff_affected_files(content: &str) -> crate::SFResult<Vec<String>> {
    let files: Vec<String> = parse_diff_targets(content)
        .into_iter()
        .map(|target| target.path)
        .collect();

    if files.is_empty() {
        return Err(crate::SFError::Validation(
            "No file paths found in change (expected '--- a/<path>' or '+++ b/<path>' lines)"
                .into(),
        ));
    }

    Ok(files)
}

/// Changed lines in a unified diff: additions plus deletions, with the `---` /
/// `+++` file headers excluded.
///
/// Pure, and shared by every layer that measures a change against a line budget
/// — the landing policy's cap and the routing rule that tiers work by declared
/// size. Two counters would let the same diff be over the cap for one reader and
/// under it for another, and nothing downstream could tell which reading was the
/// one that let the change through.
pub fn count_diff_lines(content: &str) -> usize {
    content
        .lines()
        .filter(|l| {
            (l.starts_with('+') || l.starts_with('-'))
                && !l.starts_with("+++")
                && !l.starts_with("---")
        })
        .count()
}

/// Extensions whose files are prose rather than something a compiler, a schema
/// or a test run reads.
///
/// An allow-list on purpose: a path this list does not recognize counts as code,
/// so an unfamiliar extension can only cost a request the lightest route, never
/// earn it. The other direction would let a file nobody classified collect the
/// lightest route by being unreadable here.
pub const PROSE_EXTENSIONS: &[&str] = &["md", "markdown", "txt", "rst", "adoc"];

/// Extensions whose files carry configuration a running process reads by key.
///
/// Separate from [`PROSE_EXTENSIONS`] because a configuration change and a
/// documentation change are routed differently: prose is read by people, and a
/// key is read by code, so a key that a gate reads decides whether the gates
/// have to be re-run while a paragraph never can.
pub const CONFIG_EXTENSIONS: &[&str] = &["json", "yaml", "yml", "toml"];

/// The extension of a path, lowercased, or `None` when it has none.
fn path_extension(path: &str) -> Option<String> {
    path.rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())
}

/// Whether a path names prose.
///
/// A bare name (`LICENSE`, `README`) has no extension to read, and a dotted
/// directory (`docs/v1.2/notes`) is not a file name at all; both stay code for
/// the same reason an unknown extension does.
pub fn is_prose_path(path: &str) -> bool {
    match path_extension(path) {
        Some(ext) => PROSE_EXTENSIONS.contains(&ext.as_str()),
        None => false,
    }
}

/// Whether a path names a configuration file.
pub fn is_config_path(path: &str) -> bool {
    match path_extension(path) {
        Some(ext) => CONFIG_EXTENSIONS.contains(&ext.as_str()),
        None => false,
    }
}

/// What a diff does, in the three shapes a reader that only wants to route it
/// needs — and nothing else.
///
/// Distinct from [`parse_diff_targets`] on purpose. A target list answers "which
/// files, created/modified/deleted"; this answers "which configuration keys moved,
/// and which lines stopped existing". The routing decision needs the second: two
/// changes over the same path set are two different changes when one of them
/// rewrites `memory.ingest.extraction_input_budget_tokens` and the other rewrites
/// a comment beside it, and only a key set can tell them apart.
///
/// Only removals are recorded. An added line cannot take a check away, so a
/// caller asking "did this change take a gate point out" reads removals and
/// nothing else; recording additions too would make the answer larger than the
/// question.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiffShape {
    /// Configuration files the diff touches, whether or not a key was read out
    /// of them. Kept apart from the key set because a change to a config file
    /// that removes no key and adds no key is still a configuration change.
    pub config_files: BTreeSet<String>,
    /// The leaf names of the configuration keys the diff writes or removes.
    ///
    /// Leaf names, not dotted paths: a diff does not carry the indentation
    /// context that would let a dotted path be rebuilt without the file it
    /// applies to, and rebuilding one out of the diff's own text would be a
    /// reading that changes with how git chose to cut the hunks. A leaf name is
    /// a token the file and the reader both spell the same way.
    pub changed_config_keys: BTreeSet<String>,
    /// Removed lines, per file, with the leading `-` stripped.
    pub removed_lines: BTreeMap<String, Vec<String>>,
    /// Added lines, per file, with the leading `+` stripped.
    ///
    /// Carried so that a removal can be told from an edit in place. A value
    /// change removes the old line and adds the new one, and reading only the
    /// removal would count every configuration update as a deletion.
    pub added_lines: BTreeMap<String, Vec<String>>,
}

impl DiffShape {
    /// Whether the diff removes any line at all.
    pub fn removes_lines(&self) -> bool {
        !self.removed_lines.is_empty()
    }

    /// Whether the diff changes a configuration file.
    pub fn touches_config(&self) -> bool {
        !self.config_files.is_empty()
    }

    /// How many lines the diff removed, over every file.
    pub fn removed_line_count(&self) -> usize {
        self.removed_lines.values().map(Vec::len).sum()
    }

    /// The identifiers a file lost and did not get back, per file.
    ///
    /// This is what tells a deletion from an edit. Comparing whole lines would
    /// fail on the commonest change of all — a value rewritten in place removes
    /// one line and adds a different one — so the comparison is over the
    /// identifiers the two sides name: `x = 1` removed and `x = 2` added loses
    /// nothing, while `let cap = max_diff_lines;` removed with nothing put back
    /// loses `max_diff_lines`. A name that moved to another line in the same
    /// file is not a loss, which is the direction that keeps an ordinary
    /// refactor from reading as a gate point taken out.
    pub fn net_removed_tokens(&self) -> BTreeMap<String, BTreeSet<String>> {
        let mut net = BTreeMap::new();
        for (path, removed) in &self.removed_lines {
            let added: BTreeSet<String> = self
                .added_lines
                .get(path)
                .map(|lines| identifier_tokens(&lines.join("\n")))
                .unwrap_or_default();
            let gone: BTreeSet<String> = identifier_tokens(&removed.join("\n"))
                .into_iter()
                .filter(|token| !added.contains(token))
                .collect();
            if !gone.is_empty() {
                net.insert(path.clone(), gone);
            }
        }
        net
    }
}

/// The identifiers a text names: runs of letters, digits and underscores that
/// start with a letter or an underscore.
///
/// Deliberately wider than "looks like a configuration key" — this is the
/// comparison side of [`DiffShape::net_removed_tokens`], where the question is
/// whether a name survived, not whether it is a name of any particular shape.
pub fn identifier_tokens(text: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let mut current = String::new();
    for ch in text.chars() {
        let word = ch.is_ascii_alphanumeric() || ch == '_';
        if word {
            if current.is_empty() && ch.is_ascii_digit() {
                // A run starting with a digit is a number, not a name.
                continue;
            }
            current.push(ch);
            continue;
        }
        if !current.is_empty() {
            tokens.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.insert(current);
    }
    tokens
}

/// Read a diff into the shapes a routing decision reads it by.
///
/// Built on [`diff_file_entries`] rather than on a line scan so that a body line
/// that merely looks like a header cannot invent a key or a removal: the section
/// grammar is the one place that decides which lines are body, and a second
/// reading here would disagree with it exactly where a diff is malformed.
pub fn diff_shape(content: &str) -> DiffShape {
    let mut shape = DiffShape::default();
    for entry in diff_file_entries(content) {
        let config = is_config_path(&entry.path);
        if config {
            shape.config_files.insert(entry.path.clone());
        }
        for hunk in &entry.hunks {
            for line in hunk.lines().skip(1) {
                // `---` is a section header, never a body line, and the section
                // grammar has already kept it out of the body — so anything left
                // starting with `-` here is a removal.
                let body = if let Some(rest) = line.strip_prefix('-') {
                    shape
                        .removed_lines
                        .entry(entry.path.clone())
                        .or_default()
                        .push(rest.to_string());
                    rest
                } else if let Some(rest) = line.strip_prefix('+') {
                    shape
                        .added_lines
                        .entry(entry.path.clone())
                        .or_default()
                        .push(rest.to_string());
                    rest
                } else {
                    // A context line is one the change did not write.
                    continue;
                };
                if config {
                    if let Some(key) = config_key_token(body) {
                        shape.changed_config_keys.insert(key);
                    }
                }
            }
        }
    }
    shape
}

/// The leaf name of a configuration key written on one changed line, if any.
///
/// Reads the two spellings a configuration document uses for a key — a quoted
/// JSON name and a bare YAML name — and requires the `:` that makes it a key
/// rather than a value. Anything else (a comment, a scalar, a list entry's
/// value) yields nothing, which is the safe direction: a key this misses costs a
/// configuration change the strongest tier, while a key it invents would send a
/// change to the real gate for a line that moved no key at all.
fn config_key_token(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("---") {
        return None;
    }
    let name = if let Some(rest) = trimmed.strip_prefix('"') {
        let end = rest.find('"')?;
        let name = &rest[..end];
        if rest[end + 1..].trim_start().starts_with(':') {
            name
        } else {
            return None;
        }
    } else {
        let end = trimmed.find(':')?;
        let name = trimmed[..end].trim();
        name
    };
    if name.is_empty() {
        return None;
    }
    let leaf = name.rsplit('.').next().unwrap_or(name).trim();
    // A dotted path names the key; `memory.ingest.x` and `x` are the same key to
    // a reader that only compares names.
    let leaf: String = leaf
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .collect();
    if leaf.is_empty() || leaf.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(leaf)
}

/// The lines a diff writes, per file, numbered on the side the file ends up on.
///
/// The line budget answers "how much did this change write"; this answers
/// "where". A reader holding a position in the resulting file — a diagnostic's
/// span, say — can then ask whether this change is what put it there, and that
/// is the difference between a criterion about the change and a criterion about
/// the tree: judged the second way, a tree that already carries a finding
/// convicts every change, including the one that clears it.
///
/// Only additions are recorded. A removed line is not in the file the change
/// produces, so nothing can sit on it afterwards, and a context line is by
/// definition one the change did not write.
pub fn diff_added_lines(content: &str) -> BTreeMap<String, BTreeSet<u64>> {
    let mut added: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();

    for entry in diff_file_entries(content) {
        if entry.path.is_empty() || entry.path == "/dev/null" {
            continue;
        }
        let mut lines: BTreeSet<u64> = BTreeSet::new();
        for hunk in &entry.hunks {
            // The header carries the one thing a body does not: the line in the
            // new file its first line lands on.
            let mut body = hunk.lines();
            let Some(header) = body.next().and_then(parse_hunk_header_parts) else {
                continue;
            };
            let mut new_line = header.new_start;
            for line in body {
                match classify_hunk_line(line) {
                    HunkLine::Added => {
                        lines.insert(new_line);
                        new_line += 1;
                    }
                    HunkLine::Context => new_line += 1,
                    HunkLine::Removed | HunkLine::Marker => {}
                }
            }
        }
        if !lines.is_empty() {
            added.insert(entry.path, lines);
        }
    }
    added
}

/// A line this change writes whose only effect is a default no deployment
/// reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableDefault {
    pub file: String,
    pub line: u64,
    /// The field the line assigns, as the document would spell it.
    pub key: String,
}

/// The written lines whose value nothing can reach, empty when the change has
/// an effect anywhere.
///
/// The criterion is deliberately all-or-nothing, and every input it cannot
/// read answers "no". A change that also writes a line outside a
/// `Default`-impl block is not a no-op, and neither is one whose written line
/// is not a literal — a call, a constructor, a path. The documents are what
/// make the default unreachable: the loader deserializes a document into the
/// type and only falls back to the type's `Default` for a key the document
/// leaves out, so the criterion needs the key written by **every** document
/// and not merely by one. A section a document does not carry at all is a
/// document that does not write the key, so the refusal is not reached.
///
/// `written` is the line numbers the change adds, in the applied file;
/// `sources` is those files as the applied tree holds them, keyed the same way.
/// A file `sources` has nothing for, and an empty document list, both answer
/// "no" rather than convicting on a partial reading.
pub fn unreachable_defaults(
    written: &BTreeMap<String, BTreeSet<u64>>,
    sources: &BTreeMap<String, String>,
    documents: &[serde_json::Value],
) -> Vec<UnreachableDefault> {
    if written.is_empty() || documents.is_empty() {
        return Vec::new();
    }
    let mut found = Vec::new();
    for (file, lines) in written {
        let Some(source) = sources.get(file) else {
            return Vec::new();
        };
        let blocks = default_impl_blocks(source);
        let text: Vec<&str> = source.lines().collect();
        for &line in lines {
            let Some(block) = blocks.iter().find(|b| b.start <= line && line <= b.end) else {
                return Vec::new();
            };
            let Some(key) = text
                .get(line as usize - 1)
                .and_then(|text| literal_field_assignment(text))
            else {
                return Vec::new();
            };
            if !documents
                .iter()
                .all(|doc| section_writes(doc, &block.section, &key))
            {
                return Vec::new();
            }
            found.push(UnreachableDefault {
                file: file.clone(),
                line,
                key,
            });
        }
    }
    found
}

/// One `impl Default for <Name>Config` block: the lines it spans, and the
/// top-level section the document spells that struct as.
struct DefaultImpl {
    start: u64,
    end: u64,
    section: String,
}

/// The `Default`-impl blocks a file carries, by line range.
///
/// Braces are counted without regard for string literals or comments: a brace
/// inside either would move an end. That can only cut a block short, and a line
/// falling outside every block answers "no" above — the reading that costs a
/// round is the one that fires on a change it cannot place, not the one that
/// stays quiet.
fn default_impl_blocks(source: &str) -> Vec<DefaultImpl> {
    let mut blocks = Vec::new();
    let lines: Vec<&str> = source.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        let Some(rest) = line.trim_start().strip_prefix("impl Default for ") else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let Some(section) = name.strip_suffix("Config").map(snake_case) else {
            continue;
        };
        if section.is_empty() {
            continue;
        }
        let mut depth: i64 = 0;
        let mut opened = false;
        let mut end = None;
        for (offset, body) in lines[index..].iter().enumerate() {
            for c in body.chars() {
                match c {
                    '{' => {
                        depth += 1;
                        opened = true;
                    }
                    '}' => depth -= 1,
                    _ => {}
                }
            }
            if opened && depth <= 0 {
                end = Some(index as u64 + offset as u64 + 1);
                break;
            }
        }
        if let Some(end) = end {
            blocks.push(DefaultImpl {
                start: index as u64 + 1,
                end,
                section,
            });
        }
    }
    blocks
}

/// The field a line assigns, when the line assigns a literal and nothing else.
fn literal_field_assignment(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with("//") {
        return None;
    }
    let (key, value) = line.split_once(':')?;
    let key = key.trim();
    if key.is_empty() || !key.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    let value = value.trim().trim_end_matches(',').trim();
    // `500_000` is a Rust literal and not a number `f64::from_str` takes, so the
    // separators come out before the value is asked whether it is one.
    let literal = value.replace('_', "").parse::<f64>().is_ok()
        || matches!(value, "true" | "false" | "None")
        || (value.len() >= 2 && value.starts_with('"') && value.ends_with('"'));
    literal.then(|| key.to_string())
}

/// Whether this document writes the key anywhere inside the section.
///
/// Inside rather than at the top of it: a section's reader may take a value out
/// by pointer, and the key's own path is what the reader reads either way.
fn section_writes(document: &serde_json::Value, section: &str, key: &str) -> bool {
    document
        .get(section)
        .is_some_and(|section| subtree_has_key(section, key))
}

fn subtree_has_key(value: &serde_json::Value, key: &str) -> bool {
    match value {
        serde_json::Value::Object(map) => map
            .iter()
            .any(|(name, child)| name == key || subtree_has_key(child, key)),
        _ => false,
    }
}

/// `SelfEvolution` → `self_evolution`: the rule the document's section names
/// follow the struct names by.
fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (index, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// A diagnostic the change itself is answerable for: its span sits on a line
/// this diff writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntroducedLint {
    /// The code as the compiler reports it — `clippy::ptr_arg`, `dead_code`.
    pub code: String,
    /// Workspace-relative, the way both the compiler and a diff spell it.
    pub file: String,
    /// First line of the diagnostic's primary span.
    pub line: u64,
    pub message: String,
}

/// What a linter's machine-readable transcript says about one change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LintAttribution {
    /// Diagnostics whose span sits on a line this change writes. Only these are
    /// the change's to answer for; the rest are the tree's, and convicting a
    /// change of them retires a sound change for a defect it did not write.
    pub introduced: Vec<IntroducedLint>,
    /// Every lint diagnostic the transcript carried, attributed or not.
    ///
    /// Beside the list rather than instead of it, because an empty list reports
    /// "the tree is clean" and "the tree carries lints, none of them here"
    /// identically, and those are different facts about the change.
    pub diagnostics: usize,
}

/// The lint diagnostics a change is answerable for, read from a
/// `--message-format=json` transcript.
///
/// `written` is where the change wrote, as [`diff_added_lines`] reads it. The
/// distinction this draws is the one a whole-tree lint criterion cannot:
/// `cargo clippy --workspace` reports the tree's lints, and a change landing on
/// a tree that already carries one is not what put it there. Only diagnostics
/// whose primary span overlaps a line the change writes are returned; the paths
/// join because both sides are relative to the same workspace root.
///
/// Three kinds of record are deliberately not read:
///
/// - Anything without a lint code. That is a compilation error, and the run
///   that compiles the tree answers for those; naming a lint here would tell
///   the producer to fix something that is not what stopped its change.
/// - Anything without a primary span: a summary line ("aborting due to N
///   previous errors") is a tally of records already read.
/// - Anything outside the files the change wrote, and anything whose span
///   misses every line it wrote — by construction not this change's doing.
pub fn introduced_lints(
    written: &BTreeMap<String, BTreeSet<u64>>,
    transcript: &str,
) -> LintAttribution {
    let mut attribution = LintAttribution::default();
    let mut seen: BTreeSet<(String, String, u64)> = BTreeSet::new();

    for line in transcript.lines() {
        let line = line.trim();
        if !line.starts_with('{') {
            continue;
        }
        let Ok(record) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if record.get("reason").and_then(|r| r.as_str()) != Some("compiler-message") {
            continue;
        }
        let Some(message) = record.get("message") else {
            continue;
        };
        let level = message
            .get("level")
            .and_then(|l| l.as_str())
            .unwrap_or_default();
        // Both levels: a run that promotes warnings reports a lint as an error,
        // and one that does not still reports the same lint.
        if level != "error" && level != "warning" {
            continue;
        }
        let Some(code) = message
            .get("code")
            .and_then(|c| c.get("code"))
            .and_then(|c| c.as_str())
        else {
            continue;
        };
        if is_compiler_error_code(code) {
            continue;
        }
        attribution.diagnostics += 1;

        // The primary span is where the finding is. A secondary span is context
        // printed beside it and may sit in a file the change never touched.
        let Some(span) = message
            .get("spans")
            .and_then(|s| s.as_array())
            .and_then(|spans| {
                spans
                    .iter()
                    .find(|span| span.get("is_primary").and_then(|p| p.as_bool()) == Some(true))
            })
        else {
            continue;
        };
        let Some(file) = span.get("file_name").and_then(|f| f.as_str()) else {
            continue;
        };
        let file = file.trim_start_matches("./");
        let Some(lines) = written.get(file) else {
            continue;
        };
        let start = span.get("line_start").and_then(|l| l.as_u64()).unwrap_or(0);
        let end = span
            .get("line_end")
            .and_then(|l| l.as_u64())
            .unwrap_or(start);
        if !(start..=end).any(|line| lines.contains(&line)) {
            continue;
        }
        if !seen.insert((code.to_string(), file.to_string(), start)) {
            continue;
        }
        attribution.introduced.push(IntroducedLint {
            code: code.to_string(),
            file: file.to_string(),
            line: start,
            message: message
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or_default()
                .to_string(),
        });
    }

    attribution
        .introduced
        .sort_by(|a, b| (&a.file, a.line, &a.code).cmp(&(&b.file, b.line, &b.code)));
    attribution
}

/// Whether a diagnostic code is one of rustc's numbered errors rather than a
/// lint name. `E0308` is a type mismatch; `dead_code` and `clippy::ptr_arg` are
/// lints, and they are what a lint criterion is about.
fn is_compiler_error_code(code: &str) -> bool {
    let Some(digits) = code.strip_prefix('E') else {
        return false;
    };
    digits.len() == 4 && digits.chars().all(|c| c.is_ascii_digit())
}

/// Names a change may not rewrite: the build, deployment and configuration
/// manifests.
///
/// Rewriting one of these changes what the project is built into or how it is
/// deployed rather than what it does, and the pipeline that would apply the
/// change has nothing to test that against.
pub const PROTECTED_FILE_NAMES: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "cogneva.json",
    ".env",
    ".envrc",
    "Dockerfile",
    "Containerfile",
    "docker-compose.yml",
    "setup.sh",
];

/// Extensions a change may not rewrite: a diff must not be able to replace a
/// credential file that no reviewer ever saw.
pub const PROTECTED_FILE_EXTENSIONS: &[&str] = &["pem", "key", "crt", "p12"];

/// Why a change may not name `path`, or `None` when it may.
///
/// Answers only what is a property of the path itself — its shape and whether
/// it is protected — and never whether it exists. That split is what lets
/// every gate reach the same verdict here: the evaluator vets a change before
/// there is a checkout to resolve against, while the apply gate has one, so
/// existence is the one question the two cannot share and is left to the gate.
pub fn forbidden_target_reason(path: &str) -> Option<String> {
    let normalized = path.replace('\\', "/");
    let path = std::path::Path::new(&normalized);

    if path.is_absolute() {
        return Some(format!("absolute path not allowed: {normalized}"));
    }
    if path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Some(format!("path escapes project root: {normalized}"));
    }
    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
        if PROTECTED_FILE_NAMES.contains(&name) {
            return Some(format!("protected file: {name}"));
        }
    }
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        if PROTECTED_FILE_EXTENSIONS.contains(&ext) {
            return Some(format!("protected file extension: .{ext}"));
        }
    }

    None
}

/// Longest an extension may be before a trailing `.foo` reads as a sentence
/// fragment rather than a file type.
const GOAL_PATH_EXT_MAX: usize = 6;

/// Shortest extension worth reading as a file type. One letter is the shape of
/// an abbreviation (`e.g`, `i.e`), not of an extension anybody ships.
const GOAL_PATH_EXT_MIN: usize = 2;

/// Characters that separate one candidate token from the next. A goal is prose,
/// so a path arrives wrapped in whatever punctuation the sentence needed; these
/// are cut apart before anything is judged, and none of them can appear inside
/// a path.
const GOAL_TOKEN_BREAKS: &str = ",;()[]{}\"'`<>";

/// Punctuation that may trail a token because the sentence ended, not because
/// the path did. `README.md.` and `` `crates/x/y.rs`, `` both name a file.
const GOAL_TOKEN_TRAILING: &str = ".:!?*#";

/// Characters that cannot appear in a path this system would ever act on. A
/// token carrying one is prose that happens to look path-like — a glob, a URL
/// with a scheme, an assignment — and is dropped rather than guessed at.
const GOAL_PATH_ILLEGAL: &str = "*?|=:!<>\"'`";

/// The file names a goal holds itself to, in the order it names them.
///
/// A goal that says which file it is about is making a claim the change it
/// produces can be held to, but only if the claim can be read off the text
/// without asking a model. This reads it: a token counts as a path when it has
/// a directory separator or a trailing extension, which is what separates
/// `README.md` and `crates/x/y.rs` from the ordinary words around them.
///
/// Nothing here touches the filesystem. Whether a named path is real is the
/// caller's question, and only the side holding a checkout can answer it —
/// which is also the only side whose answer means anything, since a name that
/// resolves to nothing cannot be contradicted by an artifact.
///
/// Backslashes fold to `/` and a leading `./` drops, so one file written two
/// ways yields one token. Order is preserved and duplicates collapse, leaving
/// the first name a caller reported as the first one the goal gave.
pub fn paths_named_in_goal(goal: &str) -> Vec<String> {
    let mut named: Vec<String> = Vec::new();
    for raw in goal.split(|c: char| c.is_whitespace() || GOAL_TOKEN_BREAKS.contains(c)) {
        let trimmed = raw.trim_end_matches(|c: char| GOAL_TOKEN_TRAILING.contains(c));
        let normalized = trimmed.replace('\\', "/");
        let candidate = normalized.strip_prefix("./").unwrap_or(&normalized);
        if candidate.is_empty() || candidate.contains("..") {
            continue;
        }
        if candidate.chars().any(|c| GOAL_PATH_ILLEGAL.contains(c)) {
            continue;
        }
        if !looks_like_a_path(candidate) {
            continue;
        }
        if !named.iter().any(|seen| seen == candidate) {
            named.push(candidate.to_string());
        }
    }
    named
}

/// Whether `goal` uses `name` as a name rather than as a piece of a longer
/// word.
///
/// The caller brings candidates it got from somewhere real — the file names of
/// a checkout — and asks the goal which of them it is talking about. Running
/// the question in that direction is what keeps prose out: `improve` is never a
/// candidate and so can never be a name, while `README` beside a real
/// `README.md` is the goal saying which file it means.
///
/// A letter, digit, `_`, `-` or `.` touching the name counts as part of it, so
/// `README` does not match inside `README.md` while the goal is naming
/// `README.md` itself.
pub fn mentions_name(goal: &str, name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut from = 0usize;
    while let Some(offset) = goal[from..].find(name) {
        let start = from + offset;
        let end = start + name.len();
        let opens = goal[..start]
            .chars()
            .next_back()
            .is_none_or(|c| !name_word_char(c));
        let closes = goal[end..]
            .chars()
            .next()
            .is_none_or(|c| !name_word_char(c));
        if opens && closes {
            return true;
        }
        from = end;
        if from >= goal.len() {
            break;
        }
    }
    false
}

fn name_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-' || c == '.'
}

/// Whether a token has the shape of a path rather than of a word.
///
/// A separator is the strongest signal: prose does not put `crates/x.rs` in a
/// sentence by accident. A trailing extension is the weaker one, so it has to
/// look like a type anybody would name — two to six alphanumerics after the
/// last dot — or `e.g` and `i.e` become files.
fn looks_like_a_path(token: &str) -> bool {
    if token.contains('/') {
        return true;
    }
    std::path::Path::new(token)
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| {
            (GOAL_PATH_EXT_MIN..=GOAL_PATH_EXT_MAX).contains(&e.len())
                && e.chars().all(|c| c.is_ascii_alphanumeric())
        })
}

/// Structural check of a unified diff: every hunk must carry exactly the
/// number of lines its header declares, and the last line must be terminated.
///
/// `git apply` rejects a mismatch with "corrupt patch at line N", but only
/// after the artifact has travelled to the apply gate, where the reason is a
/// line number nobody can act on. Running the same arithmetic as a pure
/// function lets the defect be named where the diff was just written, while
/// the generator can still be asked to repair the header.
///
/// Returns a description of the first defect, or `None` when the diff is
/// structurally sound. The walk mirrors `git apply`: a hunk ends as soon as
/// both declared counts are consumed, so a following line that is neither a
/// hunk nor a file header is the same corruption git would report.
pub fn diff_structural_defect(content: &str) -> Option<String> {
    let mut file = String::from("<unknown>");
    let mut hunk: Option<(usize, u64, u64, u64, u64)> = None;

    for (idx, line) in content.lines().enumerate() {
        let lineno = idx + 1;

        if let Some((hunk_line, old_declared, new_declared, old_seen, new_seen)) = hunk {
            let (old, new) = classify_hunk_line(line).tally();
            hunk = Some((
                hunk_line,
                old_declared,
                new_declared,
                old_seen + old,
                new_seen + new,
            ));
            if let Some((_, od, nd, os, ns)) = hunk {
                if os >= od && ns >= nd {
                    hunk = None;
                }
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("--- ") {
            let path = rest.split_whitespace().next().unwrap_or(rest.trim());
            file = path.strip_prefix("a/").unwrap_or(path).to_string();
            continue;
        }
        if line.starts_with("+++ ") || line.starts_with("diff --git ") {
            continue;
        }
        if line.starts_with("@@") {
            match parse_hunk_header_parts(line) {
                Some(header) => {
                    hunk = Some((lineno, header.old_count, header.new_count, 0, 0))
                }
                None => {
                    return Some(format!(
                        "{file}:{lineno}: unparsable hunk header, git apply will reject the diff: {line}"
                    ))
                }
            }
            continue;
        }
        if line.is_empty() || is_git_extended_header(line) {
            continue;
        }
        return Some(format!(
            "{file}:{lineno}: unexpected line outside any hunk, git apply will reject the diff as corrupt: {line}"
        ));
    }

    if let Some((hunk_line, old_declared, new_declared, old_seen, new_seen)) = hunk {
        return Some(format!(
            "{file}:{hunk_line}: hunk header declares {old_declared} old / {new_declared} new lines \
             but the diff ends after {old_seen} old / {new_seen} new, git apply will reject it as corrupt"
        ));
    }

    if has_unterminated_last_line(content) {
        return Some(format!(
            "{file}: the diff's last line is not terminated by a newline, git apply will reject the \
             patch as corrupt"
        ));
    }

    None
}

/// Whether the diff's final line reaches the parser unterminated.
///
/// `git apply` reads a patch as a stream of newline-terminated lines, and its
/// reader rejects an unterminated final line as "corrupt patch at line N" —
/// naming the line it stopped on rather than the missing terminator. A
/// generator that emits the diff body and closes the string without a final
/// newline produces exactly that, and the hunk arithmetic cannot see it: the
/// last line is present, so every count still agrees. The rule therefore
/// belongs in the same walk as the counts, so the detector's grammar stays as
/// wide as the parser it stands in for.
fn has_unterminated_last_line(content: &str) -> bool {
    !content.is_empty() && !content.ends_with('\n')
}

/// One file section of a unified diff, decomposed so that a single hunk can be
/// put to the apply gate on its own.
///
/// A change is accepted or rejected whole, so the verdict on a rejected artifact
/// names one hunk and says nothing about the others: an artifact carrying one
/// bad hunk out of three and one carrying three bad hunks out of three look
/// identical, and neither can be trended against the other. Separating the hunks
/// is what makes the finer reading possible, and separating them is diff
/// grammar — the grammar `diff_structural_defect` walks, read here through the
/// same header parser and the same line classifier so the two cannot disagree
/// about where a hunk ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffFileEntry {
    /// Target path with the `a/`/`b/` prefix removed.
    pub path: String,
    /// Every line from the section's start up to the line before its first
    /// hunk, verbatim: the `diff --git`/`---`/`+++` lines and git's extended
    /// headers. A patch cut down to one hunk has to carry the same metadata as
    /// the artifact it was cut from, or the gate rejects it for a reason that
    /// artifact never had.
    pub header: String,
    /// One `@@ ... @@` line plus the body lines that follow it, verbatim. Empty
    /// for a section that only renames a file or changes its mode.
    pub hunks: Vec<String>,
}

impl DiffFileEntry {
    /// Rebuild this section carrying only the chosen hunks, so that one of them
    /// can be put to the same oracle as the whole diff.
    pub fn narrow(&self, hunks: &[usize]) -> String {
        let mut out = self.header.clone();
        for &i in hunks {
            if let Some(hunk) = self.hunks.get(i) {
                out.push('\n');
                out.push_str(hunk);
            }
        }
        out.push('\n');
        out
    }
}

/// Split a unified diff into its file sections and hunks.
///
/// Hunk bodies are delimited by the counts their headers declare, which is the
/// arithmetic `git apply` uses and the only rule that works here: a removed line
/// whose content starts with `-- ` renders exactly like a `--- ` section header,
/// so scanning for the next `--- ` would cut a body in half.
pub fn diff_file_entries(content: &str) -> Vec<DiffFileEntry> {
    let mut entries: Vec<DiffFileEntry> = Vec::new();
    let mut current: Option<Section> = None;
    // Lines the open hunk still owes, per its header. `None` means no hunk is
    // open.
    let mut owed: Option<(u64, u64)> = None;

    for line in content.lines() {
        if let Some((old, new)) = owed {
            // A line owed to the body is consumed whatever it looks like. A bare
            // `@@` cannot be a body line — added, removed, context and marker
            // lines all carry a leading `+`, `-`, space or `\` — so seeing one
            // means the open header declared fewer lines than its body holds,
            // and the body closes there instead of swallowing the rest of the
            // diff.
            let body_line = (old > 0 || new > 0) && !line.starts_with("@@")
                // A `\ No newline at end of file` marker arrives after the
                // counts are already met and still belongs to the hunk it
                // annotates.
                || (old == 0 && new == 0 && line.starts_with('\\'));
            if body_line {
                let (o, n) = classify_hunk_line(line).tally();
                owed = Some((old.saturating_sub(o), new.saturating_sub(n)));
                push_hunk_line(&mut current, line);
                continue;
            }
            owed = None;
        }

        if line.starts_with("diff --git ") {
            if let Some(section) = current.take() {
                entries.push(section.finish());
            }
            let mut section = Section::new(path_from_git_line(line));
            section.header.push(line.to_string());
            current = Some(section);
            continue;
        }

        if line.starts_with("@@") {
            owed = Some(match parse_hunk_header_parts(line) {
                Some(header) => (header.old_count, header.new_count),
                // An unparsable header still names a hunk the artifact claims to
                // carry, so it is counted; it owes no lines, which keeps the
                // text after it out of its body.
                None => (0, 0),
            });
            if let Some(section) = current.as_mut() {
                section.hunks.push(line.to_string());
            }
            continue;
        }

        if line.starts_with("--- ") {
            // Outside a body a `--- ` line is a section header. It starts the
            // next file of a diff that omits `diff --git` lines, or opens the
            // first section of one; after a `diff --git` line it is that
            // section's own header, so the section is only closed when it
            // already carries hunks.
            let starts_section = current.as_ref().is_none_or(|s| !s.hunks.is_empty());
            if starts_section {
                if let Some(section) = current.take() {
                    entries.push(section.finish());
                }
                current = Some(Section::new(String::new()));
            }
        }

        push_header_line(&mut current, line);
    }

    if let Some(section) = current.take() {
        entries.push(section.finish());
    }
    entries
}

/// A file section while it is being read. Header lines are accumulated rather
/// than joined in place so that a blank line inside a section's metadata
/// survives the round trip.
struct Section {
    path: String,
    header: Vec<String>,
    hunks: Vec<String>,
}

impl Section {
    fn new(path: String) -> Self {
        Self {
            path,
            header: Vec::new(),
            hunks: Vec::new(),
        }
    }

    /// The path comes from the section's first line when that line carries one,
    /// and from its `---`/`+++` pair otherwise.
    fn finish(self) -> DiffFileEntry {
        let header = self.header.join("\n");
        let path = if self.path.is_empty() || self.path == "/dev/null" {
            section_path(&header)
        } else {
            self.path
        };
        DiffFileEntry {
            path,
            header,
            hunks: self.hunks,
        }
    }
}

fn push_header_line(current: &mut Option<Section>, line: &str) {
    if let Some(section) = current.as_mut() {
        section.header.push(line.to_string());
    }
}

fn push_hunk_line(current: &mut Option<Section>, line: &str) {
    if let Some(section) = current.as_mut() {
        if let Some(last) = section.hunks.last_mut() {
            last.push('\n');
            last.push_str(line);
        }
    }
}

fn path_from_git_line(line: &str) -> String {
    let Some(rest) = line.strip_prefix("diff --git ") else {
        return String::new();
    };
    // `a/<path> b/<path>`; the second half is the target and wins because a
    // rename's two halves differ.
    match split_two_paths(rest) {
        Some((_, target)) => strip_diff_side_prefix(target),
        None => String::new(),
    }
}

/// The file a section targets when its first line does not name one: the `+++`
/// side, or the `---` side when the target side is `/dev/null` (a deletion).
fn section_path(header: &str) -> String {
    let mut old_side = String::new();
    for line in header.lines() {
        if let Some(rest) = line.strip_prefix("--- ") {
            old_side = diff_path_field(rest);
        } else if let Some(rest) = line.strip_prefix("+++ ") {
            let target = diff_path_field(rest);
            if target != "/dev/null" {
                return target;
            }
        }
    }
    if old_side == "/dev/null" {
        String::new()
    } else {
        old_side
    }
}

/// One path field of a `---`/`+++` line. `git diff` appends a tab and a
/// timestamp to lines it emits for files on disk, and the path is everything
/// before the first whitespace.
fn diff_path_field(rest: &str) -> String {
    strip_diff_side_prefix(rest.split_whitespace().next().unwrap_or(""))
}

/// An `a/` or `b/` prefix names the side rather than the file. `/dev/null` is
/// the absence of a side and is kept as written, so a caller can tell "this side
/// does not exist" from an empty parse.
fn strip_diff_side_prefix(path: &str) -> String {
    let path = path.trim();
    if path == "/dev/null" {
        return path.to_string();
    }
    path.strip_prefix("a/")
        .or_else(|| path.strip_prefix("b/"))
        .unwrap_or(path)
        .to_string()
}

/// Split `a/x b/y` on the space that separates the two paths. Paths containing
/// spaces make this ambiguous; the first space is taken, which is correct for
/// every path without one and never worse than refusing to parse.
fn split_two_paths(rest: &str) -> Option<(&str, &str)> {
    let idx = rest.find(' ')?;
    Some((&rest[..idx], rest[idx + 1..].trim()))
}

/// A parsed `@@ -old_start,old_count +new_start,new_count @@ tail` header. The
/// start lines and the section heading are carried through verbatim: a hunk
/// body says how many lines it holds but never where it belongs, so a repair
/// that recomputes the counts has to preserve everything else.
struct HunkHeader {
    old_start: u64,
    old_count: u64,
    new_start: u64,
    new_count: u64,
    /// Everything after the closing `@@`, including the `fn foo()` heading.
    tail: String,
}

/// Parse the `-old,count +new,count` fields of a hunk header. A missing count
/// means one line, matching the unified-diff grammar.
fn parse_hunk_header_parts(line: &str) -> Option<HunkHeader> {
    let inner = line.strip_prefix("@@")?;
    let end = inner.find("@@")?;
    let mut fields = inner[..end].split_whitespace();
    let old = fields.next()?.strip_prefix('-')?;
    let new = fields.next()?.strip_prefix('+')?;
    let bounds = |spec: &str| -> Option<(u64, u64)> {
        match spec.split_once(',') {
            Some((s, c)) => Some((s.parse().ok()?, c.parse().ok()?)),
            None => Some((spec.parse().ok()?, 1)),
        }
    };
    let (old_start, old_count) = bounds(old)?;
    let (new_start, new_count) = bounds(new)?;
    Some(HunkHeader {
        old_start,
        old_count,
        new_start,
        new_count,
        tail: inner[end + 2..].to_string(),
    })
}

/// How one line of a hunk body contributes to the two line counts.
///
/// The counts and the body are two views of the same thing, so both the
/// validator and the repair read them through this one classifier; a second
/// copy of the `+`/`-`/other split would let the two disagree about what a diff
/// means while each stayed self-consistent.
#[derive(Clone, Copy)]
enum HunkLine {
    Added,
    Removed,
    /// `\ No newline at end of file` belongs to neither side.
    Marker,
    /// Context. A blank line loses its leading space in some emitters; git
    /// reads it as context, so this does too.
    Context,
}

impl HunkLine {
    /// `(old, new)` lines this one line accounts for.
    fn tally(self) -> (u64, u64) {
        match self {
            HunkLine::Added => (0, 1),
            HunkLine::Removed => (1, 0),
            HunkLine::Marker => (0, 0),
            HunkLine::Context => (1, 1),
        }
    }
}

fn classify_hunk_line(line: &str) -> HunkLine {
    match line.chars().next() {
        Some('+') => HunkLine::Added,
        Some('-') => HunkLine::Removed,
        Some('\\') => HunkLine::Marker,
        _ => HunkLine::Context,
    }
}

/// Repair a diff the validator rejects: recompute every hunk header's declared
/// line counts from the hunk body that follows it and terminate the final line.
///
/// A generator that writes a correct patch body but a wrong `@@` header — an
/// off-by-N count, or a body longer than its header admits — produces a diff
/// that every apply gate rejects as "corrupt patch". So does a body the model
/// closed without a final newline. Both verdicts name a line number rather than
/// the mistake, so re-prompting the generator tends to reproduce them. The body
/// is the expensive part, the counts are pure arithmetic over it, and the
/// missing terminator is a single byte the parser requires, so both are derived
/// here instead. Whether the body actually applies at the declared start line is
/// a content question and stays with the apply/compile gate.
///
/// `None` means "keep the original bytes": the diff was already structurally
/// sound, nothing the repair can reach disagreed, or the rewrite would still be
/// unsound. A repair that cannot be shown to be an improvement is never
/// returned.
pub fn normalize_diff_hunk_headers(content: &str) -> Option<String> {
    // Only a diff the validator already rejects is a repair candidate. A diff
    // git would have accepted comes back untouched byte for byte, so this can
    // never turn a working patch into a broken one.
    if diff_structural_defect(content).is_some() {
        return repair_structural_defect(content);
    }
    None
}

/// Repair a diff already known to have a structural defect.
fn repair_structural_defect(content: &str) -> Option<String> {
    let lines: Vec<&str> = content.lines().collect();
    let mut out: Vec<String> = Vec::with_capacity(lines.len());
    let mut changed = false;
    let mut i = 0usize;

    while i < lines.len() {
        let line = lines[i];
        if !line.starts_with("@@") {
            out.push(line.to_string());
            i += 1;
            continue;
        }
        let Some(header) = parse_hunk_header_parts(line) else {
            // An unparsable header is not a count error; leave it for the gate.
            return None;
        };

        // The body runs to the next hunk or file section. `@@` and
        // `diff --git ` can never occur inside a body. A `--- ` line can — a
        // removed line whose content starts with `-- ` renders identically — so
        // it counts as a section boundary only when a `+++ ` line immediately
        // follows, which is how git starts the next file and how a body's
        // adjacent context/removed lines do not line up.
        let body_start = i + 1;
        let mut body_end = body_start;
        while body_end < lines.len() {
            let candidate = lines[body_end];
            if candidate.starts_with("@@") || candidate.starts_with("diff --git ") {
                break;
            }
            if candidate.starts_with("--- ")
                && lines
                    .get(body_end + 1)
                    .is_some_and(|next| next.starts_with("+++ "))
            {
                break;
            }
            body_end += 1;
        }
        // Blank lines trailing a body separate it from what follows rather than
        // being context lines of it; the validator skips a standalone blank line
        // for the same reason.
        while body_end > body_start && lines[body_end - 1].is_empty() {
            body_end -= 1;
        }

        let mut old_seen = 0u64;
        let mut new_seen = 0u64;
        for body_line in &lines[body_start..body_end] {
            let (old, new) = classify_hunk_line(body_line).tally();
            old_seen += old;
            new_seen += new;
        }

        if old_seen == header.old_count && new_seen == header.new_count {
            out.push(line.to_string());
        } else {
            changed = true;
            out.push(format!(
                "@@ -{},{} +{},{} @@{}",
                header.old_start, old_seen, header.new_start, new_seen, header.tail
            ));
        }
        for body_line in &lines[body_start..body_end] {
            out.push((*body_line).to_string());
        }
        i = body_end;
    }

    // `lines()` drops the terminator, so an unterminated input and a
    // count-disagreeing input are the same rewrite from here on: join the lines
    // and close the last one. The terminator is itself the repair when no count
    // disagreed, and re-adding it when one did keeps the bytes git would have
    // accepted identical.
    if !changed && content.ends_with('\n') {
        return None;
    }
    let mut repaired = out.join("\n");
    repaired.push('\n');
    if diff_structural_defect(&repaired).is_some() {
        return None;
    }
    Some(repaired)
}

/// Git's per-file extended headers, which sit between `diff --git` and the
/// first hunk and carry no hunk body.
fn is_git_extended_header(line: &str) -> bool {
    const PREFIXES: [&str; 11] = [
        "index ",
        "new file mode ",
        "deleted file mode ",
        "old mode ",
        "new mode ",
        "similarity index ",
        "copy from ",
        "copy to ",
        "rename from ",
        "rename to ",
        "Binary files ",
    ];
    PREFIXES.iter().any(|p| line.starts_with(p))
}

// ============================================================================
// Crew / Squad reflection types (migrated from cog-reflection to break
// cog-collaboration → cog-reflection dependency).
// ============================================================================

/// Summary of a single agent's contribution within a Squad run.
#[derive(Debug, Clone)]
pub struct AgentSquadContribution {
    pub agent_id: String,
    pub role: String,
    pub learnings: Vec<Learning>,
    pub errors: Vec<ErrorEntry>,
    pub result: Option<serde_json::Value>,
}

/// Result of a Squad-level reflection pass.
#[derive(Debug, Clone)]
pub struct SquadReflectionResult {
    pub squad_id: String,
    pub task_id: String,
    pub patterns: Vec<Pattern>,
    pub learnings: Vec<Learning>,
    pub upgrade_recommended: bool,
    pub upgrade_reason: Option<String>,
}

/// Aggregates individual-agent learnings into squad-level insights.
#[async_trait::async_trait]
pub trait SquadReflection: Send + Sync {
    /// Run the full squad reflection pipeline.
    async fn reflect(
        &self,
        squad_id: &str,
        task_id: &str,
        contributions: &[AgentSquadContribution],
        retry_count: u32,
    ) -> crate::SFResult<SquadReflectionResult>;

    /// Detect disagreement patterns in a Roundtable (plan vs generation mismatch).
    async fn detect_disagreements(&self, contributions: &[AgentSquadContribution])
        -> Vec<Learning>;

    /// Detect signals that suggest upgrading Pipeline → Roundtable.
    async fn detect_upgrade_signals(
        &self,
        contributions: &[AgentSquadContribution],
        retry_count: u32,
    ) -> Vec<Learning>;
}

// ============================================================================
// Meta-learning types
// ============================================================================

/// Features extracted from a task that serve as input to the mode selector.
///
/// These are recorded context, not a grouping key: which observations the
/// engine learns from together is decided by [`DecisionGroupKey`], so a field
/// that happens to vary per task (a goal, a tag) cannot silently become the
/// grouping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFeatures {
    pub task_type: String,
    pub domain_tags: Vec<String>,
    pub estimated_complexity: f32,
    pub has_external_dependencies: bool,
    pub historical_success_rate: f32,
    pub required_skills: Vec<String>,
}

/// The observation group a decision's outcome belongs to.
///
/// Two properties have to hold, and neither is expressible while the grouping
/// is "whatever string the caller formatted": the read side and the write side
/// of one decision must hand over the *same* key, and the key's value domain
/// must be bounded. A discriminator taken from a field that varies per object —
/// a goal string, a squad id — gives every group a single observation, so the
/// engine's sample floor becomes unreachable by construction no matter how many
/// tasks run. Both sides therefore name the grouping through this type, and the
/// constructors are where "what counts as one group" is written down.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DecisionGroupKey(String);

impl DecisionGroupKey {
    /// Group by task kind: every task of the same kind shares one group. The
    /// argument has to be a kind, not an instance.
    pub fn by_task_type(task_type: impl Into<String>) -> Self {
        Self(task_type.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DecisionGroupKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Recommendation returned by the meta-learning engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeRecommendation {
    Pipeline,
    Roundtable,
    /// Not enough data — fall back to the fixed-threshold heuristic.
    UseDefault,
}

/// Generic decision category for the meta-learning engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionCategory {
    PgeMode,
    ResetStrategy,
    RetryPolicy,
    SelfReviewThreshold,
}

/// Outcome of a single decision recorded by the meta-learning engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionOutcome {
    Success,
    Failed,
    Escalated,
}

// ============================================================================
// Core learning data model (migrated from cog-reflection to break
// cog-collaboration → cog-reflection dependency).
// ============================================================================

/// Category of a learning entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningCategory {
    Correction,
    Insight,
    KnowledgeGap,
    BestPractice,
}

/// Lifecycle status of a learning entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningStatus {
    Pending,
    InProgress,
    Resolved,
    Promoted,
    WontFix,
}

/// Priority level for a learning or error entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

impl Priority {
    /// This priority as a rating on the shared importance scale.
    ///
    /// Priority is a four-level ordinal and importance is a one-to-ten one, so
    /// the mapping has to pick where each level lands. It spreads them over the
    /// scale rather than clustering them: two levels a reader would call
    /// different should not sort as equal just because a five-point gap was
    /// rounded away. `Critical` sits at the top because it is the level an
    /// operator acts on first, and an importance ranking that put it below
    /// anything else would order the queue backwards.
    pub fn importance_rating(self) -> u8 {
        match self {
            Priority::Critical => 10,
            Priority::High => 8,
            Priority::Medium => 5,
            Priority::Low => 3,
        }
    }

    /// This priority on the shared importance scale, for an entry's
    /// `importance` field.
    pub fn importance(self) -> f32 {
        crate::contract::memory::importance_from_rating(self.importance_rating())
    }
}

/// Functional area affected by the learning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Area {
    Frontend,
    Backend,
    Infra,
    Tests,
    Docs,
    Config,
}

/// Source of the learning signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningSource {
    Conversation,
    Error,
    UserFeedback,
    SelfReview,
    /// A deterministic gate criterion refused an artifact this system produced.
    ///
    /// Its own variant because the downstream decision it feeds is a different
    /// kind of decision. Every other source is a *pattern* — an observation that
    /// only means something once it has been seen often enough to be worth
    /// acting on, which is what a recurrence threshold is for. A refusal is a
    /// *verdict*: one artifact, submitted on purpose, read by a check that needs
    /// no judgement, and it says which files it was refused on. Its evidence is
    /// already paid for, so the first one is worth acting on, and counting it
    /// before telling anyone is what let the same defect be generated, refused
    /// and generated again. The count stays as a reading of how often a defect
    /// comes back; it is not the price of being told about the first one.
    ChangeRefusal,
}

/// How a learning entry was resolved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Resolution {
    Resolved { resolution: String },
    WontFix { reason: String },
}

/// A single learning entry — the central data model of the reflection layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Learning {
    pub id: String,
    pub category: LearningCategory,
    pub priority: Priority,
    pub status: LearningStatus,
    pub area: Area,
    pub summary: String,
    pub details: String,
    pub suggested_action: String,
    pub source: LearningSource,
    pub related_files: Vec<String>,
    pub tags: Vec<String>,
    pub see_also: Vec<String>,
    pub pattern_key: Option<String>,
    pub recurrence_count: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub related_tasks: Vec<String>,
    /// The gate criterion that refused the change this learning is about.
    ///
    /// A refusal is recorded in order to be counted, and counting is merging:
    /// how many times a defect has recurred is what decides whether it is worth
    /// generating a fix for. Two refusals of *different* criteria are two
    /// different defects however alike their evidence reads, so the criterion
    /// travels as a value the similarity rule can veto on rather than only as
    /// words inside `details`, where it would be weighed against prose.
    ///
    /// `None` for every learning that is not about a refused change, and for
    /// rows written before this field existed — which is not the same as "was
    /// refused by nothing", so the veto only fires when both sides name a
    /// criterion.
    #[serde(default)]
    pub rejection_cause: Option<RejectionCause>,
}

impl Learning {
    pub fn generate_id() -> String {
        let now = Utc::now();
        format!(
            "LRN-{}-{}",
            now.format("%Y%m%d"),
            uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
        )
    }

    pub fn new(
        category: LearningCategory,
        priority: Priority,
        area: Area,
        summary: impl Into<String>,
        details: impl Into<String>,
        suggested_action: impl Into<String>,
        source: LearningSource,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Self::generate_id(),
            category,
            priority,
            status: LearningStatus::Pending,
            area,
            summary: summary.into(),
            details: details.into(),
            suggested_action: suggested_action.into(),
            source,
            related_files: Vec::new(),
            tags: Vec::new(),
            see_also: Vec::new(),
            pattern_key: None,
            recurrence_count: 1,
            first_seen: now,
            last_seen: now,
            related_tasks: Vec::new(),
            rejection_cause: None,
        }
    }

    pub fn bump_recurrence(&mut self) {
        self.recurrence_count += 1;
        self.last_seen = Utc::now();
    }

    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.first_seen
    }
}

/// A structured error entry for persistent failure tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEntry {
    pub id: String,
    pub priority: Priority,
    pub status: LearningStatus,
    pub summary: String,
    pub error_message: String,
    pub context: String,
    pub suggested_fix: String,
    pub reproducible: Option<bool>,
    pub related_files: Vec<String>,
    pub see_also: Vec<String>,
    pub pattern_key: Option<String>,
    pub recurrence_count: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

impl ErrorEntry {
    pub fn generate_id() -> String {
        let now = Utc::now();
        format!(
            "ERR-{}-{}",
            now.format("%Y%m%d"),
            uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
        )
    }

    pub fn new(
        priority: Priority,
        summary: impl Into<String>,
        error_message: impl Into<String>,
        context: impl Into<String>,
        suggested_fix: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Self::generate_id(),
            priority,
            status: LearningStatus::Pending,
            summary: summary.into(),
            error_message: error_message.into(),
            context: context.into(),
            suggested_fix: suggested_fix.into(),
            reproducible: None,
            related_files: Vec::new(),
            see_also: Vec::new(),
            pattern_key: None,
            recurrence_count: 1,
            first_seen: now,
            last_seen: now,
        }
    }
}

/// Lightweight trait abstracting the meta-learning mode selector.
#[async_trait::async_trait]
pub trait MetaLearning: Send + Sync + std::fmt::Debug {
    /// Recommend a mode for the given decision group.
    async fn recommend_mode(&self, group: &DecisionGroupKey) -> ModeRecommendation;

    /// Record the actual outcome of a mode decision so the model can learn.
    /// `features` is the recorded context; the group is passed separately so
    /// the two sides of the decision cannot group the observation differently.
    async fn record_outcome(
        &self,
        group: &DecisionGroupKey,
        features: &TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> crate::SFResult<()>;

    /// Recommend a decision for the given category based on historical data.
    async fn recommend(
        &self,
        category: DecisionCategory,
        group: &DecisionGroupKey,
    ) -> Option<String>;

    /// Record the outcome of a decision so the model can learn.
    async fn record(
        &self,
        category: DecisionCategory,
        group: &DecisionGroupKey,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> crate::SFResult<()>;
}

/// The tests a `cargo test` transcript names as failing, each qualified by the
/// test binary that ran it.
///
/// Cargo prints `test <name> ... FAILED` while running one of the binaries it
/// announced, and a name alone does not identify a test: two crates in one
/// workspace can both declare `hostdocs::tests::reachable`. Keying by
/// `<binary>/<name>` is what lets two transcripts be compared as sets and have
/// the comparison mean what it says.
///
/// The binary name comes from the path cargo prints with its trailing
/// `-<metadata hash>` removed, because that hash is part of a file name and
/// moves whenever the compiler's metadata moves; keeping it would make the same
/// test look like a different test from one run to the next, and every
/// comparison would come out empty.
///
/// Adjacency is not how the two are found. A caller that runs stdout and stderr
/// through separate pipes and joins them gets one stream followed by the other,
/// not the two interleaved: cargo writes `Running ...` to stderr and the harness
/// writes its own output to stdout, so every `FAILED` line can sit before every
/// `Running` line. What still holds is the order within each: cargo announces
/// the binaries in the order it runs them, and the harness announces each run in
/// that same order. So the k-th run is paired with the k-th announced binary,
/// and only when both sides counted the same number of runs -- an excerpt that
/// kept one side and dropped the other would otherwise attribute a failure to
/// whichever binary happened to land at that index.
pub fn failing_tests(output: &str) -> BTreeSet<String> {
    let binaries: Vec<String> = output
        .lines()
        .filter_map(|line| test_binary_of(line.trim_end()))
        .collect();
    let runs = output
        .lines()
        .filter(|line| harness_run_starts(line.trim_end()))
        .count();
    let paired = runs > 0 && runs == binaries.len();

    let mut run_index = 0usize;
    let mut last_announced = String::from("<unknown>");
    let mut failing = BTreeSet::new();

    for line in output.lines() {
        let line = line.trim_end();
        if paired && harness_run_starts(line) {
            last_announced = binaries[run_index].clone();
            run_index += 1;
            continue;
        }
        if let Some(name) = test_binary_of(line) {
            last_announced = name;
            continue;
        }
        // Only the run's own line carries the marker. The `failures:` summary
        // lists the same names indented and without it, so reading both would
        // count every failure twice and attribute none of them to a binary.
        if let Some(rest) = line.strip_prefix("test ") {
            if !line.ends_with("FAILED") {
                continue;
            }
            if let Some((name, _)) = rest.split_once(" ... ") {
                let name = name.trim();
                if !name.is_empty() {
                    failing.insert(format!("{last_announced}/{name}"));
                }
            }
        }
    }

    failing
}

/// The line the harness opens a run with: `running <n> tests`, or `running 1
/// test` in the singular. Cargo writes it to stdout once per test binary, in the
/// order the binaries are run, which is the half of the pairing that survives
/// the two streams being joined instead of interleaved.
fn harness_run_starts(line: &str) -> bool {
    let Some(rest) = line.strip_prefix("running ") else {
        return false;
    };
    let Some(count) = rest
        .strip_suffix(" tests")
        .or_else(|| rest.strip_suffix(" test"))
    else {
        return false;
    };
    !count.is_empty() && count.chars().all(|c| c.is_ascii_digit())
}

/// The test binary cargo is about to run, from the line it announces it with.
///
/// Four shapes reach here: `Running unittests src/lib.rs (<path>)`,
/// `Running tests/foo.rs (<path>)`, `Doc-tests <crate> (<path>)` and
/// `Doc-tests <crate>`. The first three end with the path in parentheses, which
/// is the part that carries the name. The fourth has no path at all, and its
/// trailing word is the crate's name -- the same name the parenthesised form
/// would have yielded, so a run of it is not a run this function failed to name.
fn test_binary_of(line: &str) -> Option<String> {
    let line = line.trim_start();
    let doc_tests = line.starts_with("Doc-tests ");
    if !line.starts_with("Running ") && !doc_tests {
        return None;
    }
    match line.rsplit_once('(') {
        Some((_, path)) => {
            let stem = path
                .trim_end()
                .trim_end_matches(')')
                .trim()
                .rsplit('/')
                .next()?;
            Some(without_metadata_hash(stem))
        }
        None if doc_tests => {
            let name = line.split_whitespace().last()?;
            Some(without_metadata_hash(name))
        }
        // `Running ...` without a path names no file to read a binary from.
        None => None,
    }
}

/// `cog_extension-3f2a1b4c5d6e7f80` -> `cog_extension`.
///
/// A binary name that happens to end in a hyphenated run of hex digits of the
/// wrong length is left alone: a name cargo did not decorate is better kept
/// intact than truncated on a guess.
fn without_metadata_hash(stem: &str) -> String {
    const HASH_LEN: usize = 16;
    match stem.rsplit_once('-') {
        Some((name, hash))
            if !name.is_empty()
                && hash.len() == HASH_LEN
                && hash.chars().all(|c| c.is_ascii_hexdigit()) =>
        {
            name.to_string()
        }
        _ => stem.to_string(),
    }
}

/// What a failing run actually reported, in the budget a record can afford.
///
/// A `cargo test --workspace` transcript is a few hundred kilobytes and the
/// diagnosis is at the far end of it: measured on the cluster's own refusals,
/// the first `FAILED` line sat at line 923 of 4568 while the head of the file
/// was 464 lines of `... ok`. Truncating such a dump to its first 2000 bytes
/// therefore keeps the noise and discards the symptom, and the learning that
/// reaches generation says "read what the test check reported" beside a report
/// with nothing in it. The same defect is then generated again, because nothing
/// in the second attempt knows anything the first did not.
///
/// So the failing test names lead, the panic sites follow, and what is left of
/// the budget goes to the harness's own account of the failure -- the block it
/// prints under `---- <test> stdout ----`, which holds the assertion message
/// and the values it printed. That block is *not* at the end of a transcript:
/// `--no-fail-fast` keeps running the binaries that come after the one that
/// failed, so the end of the run is their output and cargo's driver lines. The
/// tail is only where the block lands in a single-binary run, so it is spent
/// after the blocks rather than instead of them.
///
/// A dump with no failing test named gets its head instead. That case is a
/// formatter diff or a compile error, and both are identified by their opening
/// lines (`Diff in ...`, the first error block) rather than by their end.
///
/// The budget counts characters, not bytes: a byte slice of a transcript panics
/// when it lands inside a multi-byte character, and test names are not required
/// to be ASCII.
pub fn failure_digest(output: &str, budget: usize) -> String {
    let failing = failing_tests(output);
    if failing.is_empty() {
        return take_head(output, budget);
    }

    let mut digest = String::from("Failing tests:\n");
    for test in &failing {
        digest.push_str("  ");
        digest.push_str(test);
        digest.push('\n');
    }

    let panics: Vec<&str> = output
        .lines()
        .map(str::trim_end)
        .filter(|line| line.contains("panicked at "))
        .take(PANIC_LINES)
        .collect();
    if !panics.is_empty() {
        digest.push_str("Panics:\n");
        for line in panics {
            digest.push_str("  ");
            digest.push_str(line);
            digest.push('\n');
        }
    }

    let spent = digest.chars().count();
    if spent >= budget {
        return take_head(&digest, budget);
    }
    // The separator is only worth its characters when the text it introduces
    // survives the budget; an empty tail would leave a dangling heading.
    let remaining = budget - spent - 1;
    // The harness's own block first: it holds what the test said, which is the
    // half a re-attempt cannot get anywhere else.
    let mut rest = failure_blocks(output);
    if !rest.is_empty() {
        rest = take_head(&rest, remaining);
    }
    // The tail is what a run that names its failing test but prints no block
    // leaves there -- a compile error, or a harness that died. Whatever it
    // holds is what this function would otherwise have spent the budget on, so
    // it still gets what the blocks did not take.
    let left = remaining.saturating_sub(rest.chars().count() + 1);
    if left > 0 {
        let tail = take_tail(output, left);
        if !tail.is_empty() {
            if !rest.is_empty() {
                rest.push('\n');
            }
            rest.push_str(&tail);
        }
    }
    if !rest.is_empty() {
        digest.push('\n');
        digest.push_str(&rest);
    }
    digest
}

/// The harness's own account of what each failing test said: the lines from
/// every `---- <test> stdout ----` marker through the `test result: FAILED.`
/// line that closes the binary that ran it.
///
/// This is where a test's own words are -- the assertion message, the values it
/// printed -- and it is the one part of a transcript whose position says
/// nothing about where to look for it: `--no-fail-fast` runs the binaries after
/// the failing one too, so the block sits wherever that binary happened to run.
fn failure_blocks(output: &str) -> String {
    let mut taken: Vec<&str> = Vec::new();
    let mut inside = false;
    for line in output.lines() {
        let line = line.trim_end();
        if line.starts_with("---- ") {
            inside = true;
        }
        if inside {
            taken.push(line);
        }
        if line.starts_with("test result: FAILED") {
            inside = false;
        }
    }
    taken.join("\n")
}

/// How many panic sites a digest names. A run can panic in every test it has;
/// the first few name the places to look, and the rest is repetition that would
/// spend the budget the failing-test list needs.
const PANIC_LINES: usize = 8;

/// At most `budget` characters from the front.
fn take_head(text: &str, budget: usize) -> String {
    text.chars().take(budget).collect()
}

/// At most `budget` characters from the back.
fn take_tail(text: &str, budget: usize) -> String {
    let total = text.chars().count();
    text.chars().skip(total.saturating_sub(budget)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ALL` is every variant, once each — the invariant `slot` addresses
    /// counters by. The match below has no catch-all arm, so a new variant
    /// stops this test compiling; the pinned count is what then forces it into
    /// the list, because a variant listed nowhere would leave a counter nothing
    /// can reach and no reading of that axis would show the difference.
    #[test]
    fn the_cause_list_is_every_cause_once() {
        let mut seen: Vec<&'static str> = Vec::new();
        for cause in RejectionCause::ALL {
            let spelling = match cause {
                RejectionCause::MalformedDiff => "malformed_diff",
                RejectionCause::PromotionGateRefused => "promotion_gate_refused",
                RejectionCause::ForbiddenPath => "forbidden_path",
                RejectionCause::IntentMismatch => "intent_mismatch",
                RejectionCause::ContextDoesNotApply => "context_does_not_apply",
                RejectionCause::ApplyFailed => "apply_failed",
                RejectionCause::FormattingDiffers => "formatting_differs",
                RejectionCause::TestRunUnavailable => "test_run_unavailable",
                RejectionCause::TestsFailed => "tests_failed",
                RejectionCause::LintIntroduced => "lint_introduced",
                RejectionCause::UnreachableDefault => "unreachable_default",
            };
            assert_eq!(cause.as_str(), spelling, "{cause:?} spells two ways");
            assert_eq!(
                serde_json::to_string(&cause).unwrap(),
                format!("\"{spelling}\""),
                "{cause:?} does not reach a record under the spelling it reports"
            );
            assert!(!seen.contains(&spelling), "{spelling} is in the list twice");
            seen.push(spelling);
        }
        assert_eq!(seen.len(), 11, "ALL is missing a cause: {seen:?}");
        for (index, cause) in RejectionCause::ALL.iter().enumerate() {
            assert_eq!(cause.slot(), index, "{cause:?} does not sit at {index}");
        }
    }

    /// The refusal only fires on a change that can have no effect anywhere, and
    /// each half of that sentence is a thing the reading has to establish.
    #[test]
    fn only_a_default_no_document_leaves_open_is_unreachable() {
        let source = "\
impl MetricsConfig {
    fn budget(&self) -> u64 { self.sample_max_rows }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            sample_max_rows: 500_000,
            log_at_floor: false,
        }
    }
}
";
        let sources =
            BTreeMap::from([("crates/cog-core/src/config.rs".to_string(), source.into())]);
        let written = BTreeMap::from([(
            "crates/cog-core/src/config.rs".to_string(),
            BTreeSet::from([8]),
        )]);
        let both = vec![
            serde_json::json!({ "metrics": { "sample_max_rows": 200000 } }),
            serde_json::json!({ "metrics": { "sample_max_rows": 200000, "log_at_floor": false } }),
        ];

        let found = unreachable_defaults(&written, &sources, &both);
        assert_eq!(found.len(), 1, "这一笔的全部效果就是这个读不到的默认值");
        assert_eq!(found[0].key, "sample_max_rows");
        assert_eq!(found[0].line, 8);

        // 少一份文档写这个键 ⇒ 那份部署的进程读的就是这个默认值，它有读者。
        let one_writes_it = vec![
            serde_json::json!({ "metrics": { "sample_max_rows": 200000 } }),
            serde_json::json!({ "metrics": { "log_at_floor": false } }),
        ];
        assert!(
            unreachable_defaults(&written, &sources, &one_writes_it).is_empty(),
            "只有全部文档都写着这个键，默认值才不可达"
        );

        // 键在别的段里不算：读它的是那一段的类型，不是这个默认值。
        let elsewhere = vec![
            serde_json::json!({ "metrics": { "log_at_floor": false }, "memory": { "sample_max_rows": 1 } }),
            serde_json::json!({ "metrics": { "log_at_floor": false }, "memory": { "sample_max_rows": 1 } }),
        ];
        assert!(unreachable_defaults(&written, &sources, &elsewhere).is_empty());

        // 文档一份都读不到时不是「查过，没有」。
        assert!(unreachable_defaults(&written, &sources, &[]).is_empty());

        // 改到 impl 之外的行（第 2 行是那行函数体）⇒ 这笔变更有别的效果。
        let outside = BTreeMap::from([(
            "crates/cog-core/src/config.rs".to_string(),
            BTreeSet::from([2]),
        )]);
        assert!(unreachable_defaults(&outside, &sources, &both).is_empty());

        // 写的不是字面量（调用、构造、路径）就不是这一类。
        let call = "\
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            budget: default_budget(),
        }
    }
}
";
        let call_sources =
            BTreeMap::from([("crates/cog-core/src/config.rs".to_string(), call.into())]);
        let call_written = BTreeMap::from([(
            "crates/cog-core/src/config.rs".to_string(),
            BTreeSet::from([4]),
        )]);
        let call_docs = vec![serde_json::json!({ "metrics": { "budget": 1 } })];
        assert!(unreachable_defaults(&call_written, &call_sources, &call_docs).is_empty());

        // 读不到被写的那份文件 ⇒ 判不了，不出口。
        let unreadable = BTreeMap::from([("nope.rs".to_string(), BTreeSet::from([1]))]);
        assert!(unreachable_defaults(&unreadable, &sources, &both).is_empty());
    }

    /// 那笔实测的 no-op 就是这一形：结构名到段名的对应要按文档的写法来。
    #[test]
    fn a_section_name_is_the_struct_name_without_its_config_suffix() {
        assert_eq!(snake_case("SelfEvolution"), "self_evolution");
        assert_eq!(snake_case("Metrics"), "metrics");
        assert_eq!(snake_case("DagExecutor"), "dag_executor");
    }

    #[test]
    fn contribution_policy_roundtrips_strings() {
        for (p, s) in [
            (ContributionPolicy::Auto, "auto"),
            (ContributionPolicy::Ask, "ask"),
            (ContributionPolicy::Local, "local"),
        ] {
            assert_eq!(p.as_str(), s);
            assert_eq!(ContributionPolicy::parse(s), Some(p));
            assert_eq!(serde_json::to_string(&p).unwrap(), format!("\"{s}\""));
        }
        assert_eq!(ContributionPolicy::parse("bogus"), None);
        assert_eq!(ContributionPolicy::default(), ContributionPolicy::Auto);
    }

    /// A diff's size is its content lines, and the file headers are what both
    /// readers of that number exclude: counting them would put a one-line edit
    /// at three lines, and a header would then be deciding a line budget.
    #[test]
    fn a_diffs_size_counts_content_lines_only() {
        let diff = "diff --git a/src/a.rs b/src/a.rs\n\
                    --- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,2 +1,2 @@\n\
                    -old line\n\
                    +new line\n\
                    context line\n";
        assert_eq!(count_diff_lines(diff), 2);
        assert_eq!(count_diff_lines(""), 0);
        // A lone `+` or `-` is still a changed line: it is a line the change
        // rewrites, whatever the model wrote after it.
        assert_eq!(count_diff_lines("+\n-\n"), 2);
        // The `\ No newline at end of file` marker belongs to no side and is
        // not a change.
        assert_eq!(count_diff_lines("-old\n\\ No newline at end of file\n"), 1);
    }

    #[test]
    fn a_well_formed_diff_has_no_defect() {
        let diff = "diff --git a/src/a.rs b/src/a.rs\n\
                    index 1111111..2222222 100644\n\
                    --- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,3 +1,4 @@\n\
                    \x20fn a() {}\n\
                    \x20fn b() {}\n\
                    -fn c() {}\n\
                    +fn c() { /* changed */ }\n\
                    +fn d() {}\n";
        assert_eq!(diff_structural_defect(diff), None);
    }

    #[test]
    fn a_hunk_header_that_overstates_its_body_is_flagged() {
        // The shape that reached the apply gate and died as "corrupt patch":
        // the header claims 15 old / 49 new while the body carries one line.
        let diff = "diff --git a/src/a.rs b/src/a.rs\n\
                    --- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,15 +1,49 @@\n\
                    \x20fn a() {}\n\
                    +fn b() {}\n";
        let defect = diff_structural_defect(diff).expect("count mismatch must be reported");
        assert!(defect.contains("src/a.rs"), "{defect}");
        assert!(defect.contains("15"), "{defect}");
    }

    #[test]
    fn a_truncated_hunk_is_flagged() {
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,5 +1,5 @@\n\
                    \x20fn a() {}\n";
        assert!(diff_structural_defect(diff).is_some());
    }

    #[test]
    fn an_omitted_hunk_count_means_one_line() {
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -7 +7 @@\n\
                    -old\n\
                    +new\n";
        assert_eq!(diff_structural_defect(diff), None);
    }

    #[test]
    fn a_blank_body_line_counts_as_context() {
        // Some emitters strip the leading space from blank context lines.
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,3 +1,3 @@\n\
                    \x20fn a() {}\n\
                    \n\
                    \x20fn c() {}\n";
        assert_eq!(diff_structural_defect(diff), None);
    }

    #[test]
    fn the_reported_defect_names_the_file_and_the_hunk_line() {
        let diff = "--- a/src/deep/nested.rs\n\
                    +++ b/src/deep/nested.rs\n\
                    @@ -1,2 +1,2 @@\n\
                    \x20one\n";
        let defect = diff_structural_defect(diff).unwrap();
        assert!(defect.contains("src/deep/nested.rs:3"), "{defect}");
    }

    #[test]
    fn a_diff_whose_last_line_is_unterminated_is_a_defect() {
        // The hunk arithmetic agrees with the body — every line is present and
        // counted — so only git's requirement that the patch end in a newline
        // stands between this diff and the apply gate.
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,1 +1,2 @@\n\
                    \x20fn a() {}\n\
                    +fn b() {}";
        let defect = diff_structural_defect(diff).expect("unterminated last line must be reported");
        assert!(defect.contains("newline"), "{defect}");
    }

    #[test]
    fn the_repair_terminates_the_last_line_and_changes_nothing_else() {
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,2 +1,2 @@\n\
                    \x20old\n\
                    -gone\n\
                    +added";
        assert!(diff_structural_defect(diff).is_some());
        let repaired =
            normalize_diff_hunk_headers(diff).expect("a missing terminator is repairable");
        assert_eq!(repaired, format!("{diff}\n"));
        assert_eq!(diff_structural_defect(&repaired), None);
    }

    #[test]
    fn a_missing_terminator_and_a_wrong_count_are_repaired_together() {
        let diff = "diff --git a/src/a.rs b/src/a.rs\n\
                    --- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,15 +1,49 @@\n\
                    \x20fn a() {}\n\
                    +fn b() {}";
        let repaired = normalize_diff_hunk_headers(diff).expect("both defects are repairable");
        assert!(repaired.contains("@@ -1,1 +1,2 @@"), "{repaired}");
        assert!(repaired.ends_with("+fn b() {}\n"), "{repaired}");
        assert_eq!(diff_structural_defect(&repaired), None);
    }

    #[test]
    fn a_sound_diff_is_never_rewritten() {
        // The repair is allowed to fire only where the validator already
        // objects. Anything git would have accepted must come back untouched,
        // byte for byte, so a repair can never turn a working patch into a
        // broken one.
        let diff = "diff --git a/src/a.rs b/src/a.rs\n\
                    index 1111111..2222222 100644\n\
                    --- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,3 +1,4 @@\n\
                    \x20fn a() {}\n\
                    \x20fn b() {}\n\
                    -fn c() {}\n\
                    +fn c() { /* changed */ }\n\
                    +fn d() {}\n";
        assert_eq!(normalize_diff_hunk_headers(diff), None);
    }

    #[test]
    fn a_header_that_understates_its_body_is_recomputed() {
        // The header closes the hunk after one line, so the lines that follow
        // are what the validator reports as "outside any hunk".
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    \x20fn a() {}\n\
                    +fn b() {}\n\
                    \x20fn c() {}\n";
        assert!(diff_structural_defect(diff).is_some());
        let repaired = normalize_diff_hunk_headers(diff).expect("count mismatch is repairable");
        assert!(repaired.contains("@@ -1,2 +1,3 @@"), "{repaired}");
        assert_eq!(diff_structural_defect(&repaired), None);
    }

    #[test]
    fn a_header_that_overstates_its_body_is_recomputed() {
        let diff = "diff --git a/src/a.rs b/src/a.rs\n\
                    --- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,15 +1,49 @@\n\
                    \x20fn a() {}\n\
                    +fn b() {}\n";
        let repaired = normalize_diff_hunk_headers(diff).expect("count mismatch is repairable");
        assert!(repaired.contains("@@ -1,1 +1,2 @@"), "{repaired}");
        assert_eq!(diff_structural_defect(&repaired), None);
    }

    #[test]
    fn a_truncated_body_shrinks_the_header() {
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,5 +1,5 @@\n\
                    \x20fn a() {}\n";
        let repaired = normalize_diff_hunk_headers(diff).expect("declared counts exceed the body");
        assert!(repaired.contains("@@ -1,1 +1,1 @@"), "{repaired}");
        assert_eq!(diff_structural_defect(&repaired), None);
    }

    #[test]
    fn the_repair_keeps_the_start_lines_and_the_section_heading() {
        // A body says how many lines it holds but never where it belongs, so
        // only the counts may move.
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -7,1 +9,1 @@ fn foo()\n\
                    \x20old\n\
                    \x20also\n";
        let repaired = normalize_diff_hunk_headers(diff).expect("count mismatch is repairable");
        assert!(repaired.contains("@@ -7,2 +9,2 @@ fn foo()"), "{repaired}");
    }

    #[test]
    fn a_blank_line_between_hunks_separates_rather_than_counts() {
        // An unindented blank line between hunks is a separator; reading it as
        // context would inflate the preceding hunk by one line on each side.
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    \x20one\n\
                    \x20two\n\
                    \n\
                    @@ -10,1 +10,1 @@\n\
                    \x20x\n";
        let repaired = normalize_diff_hunk_headers(diff).expect("count mismatch is repairable");
        assert!(
            repaired.contains("@@ -1,2 +1,2 @@\n\x20one\n\x20two\n\n@@ -10,1 +10,1 @@"),
            "{repaired}"
        );
        assert_eq!(diff_structural_defect(&repaired), None);
    }

    #[test]
    fn the_body_stops_at_the_next_file_section() {
        // Without `diff --git`, the next file starts at `--- `/`+++ `. Swallowing
        // those into the previous hunk would corrupt the following file.
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    \x20a1\n\
                    -a2\n\
                    --- a/src/b.rs\n\
                    +++ b/src/b.rs\n\
                    @@ -1,1 +1,1 @@\n\
                    \x20b1\n";
        let repaired = normalize_diff_hunk_headers(diff).expect("count mismatch is repairable");
        assert!(repaired.contains("@@ -1,2 +1,1 @@"), "{repaired}");
        let second = repaired.split("--- a/src/b.rs").nth(1).unwrap();
        assert!(second.contains("@@ -1,1 +1,1 @@"), "{repaired}");
        assert_eq!(diff_structural_defect(&repaired), None);
    }

    #[test]
    fn an_unparsable_header_is_left_alone() {
        // A header that cannot be read is not a count error, and the counts
        // cannot be rebuilt from a header that is missing a side. Failing
        // closed leaves the original for the apply gate to reject.
        let diff = "--- a/src/a.rs\n\
                    +++ b/src/a.rs\n\
                    @@ -1,1 @@\n\
                    \x20a\n";
        assert!(diff_structural_defect(diff).is_some());
        assert_eq!(normalize_diff_hunk_headers(diff), None);
    }

    #[test]
    fn every_repair_leaves_a_diff_the_validator_accepts() {
        // The repair's own precondition, asserted over the shapes a generator
        // produces: whatever comes back must be structurally sound, and a
        // repair that cannot achieve that must not come back at all.
        let diffs = [
            "--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,1 +1,1 @@\n\x20a\n\x20b\n",
            "--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,4 +1,4 @@\n\x20a\n",
            "--- a/src/a.rs\n+++ b/src/a.rs\n@@ -9 @@\n-x\n+y\n-z\n",
            "--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,2 +1,2 @@\n\x20a\n\n\x20b\n",
            "--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,1 +1,1 @@\n\x20a\n\x20b",
            "--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,9 +1,9 @@\n\x20a\n\x20b",
        ];
        for diff in diffs {
            if let Some(repaired) = normalize_diff_hunk_headers(diff) {
                assert_eq!(diff_structural_defect(&repaired), None, "{repaired}");
            }
        }
    }

    const TWO_FILES: &str = "diff --git a/one.txt b/one.txt\n\
                             index 1111111..2222222 100644\n\
                             --- a/one.txt\n\
                             +++ b/one.txt\n\
                             @@ -1,3 +1,3 @@\n\
                             \x20alpha\n\
                             -beta\n\
                             +BETA\n\
                             \x20gamma\n\
                             @@ -10,3 +10,3 @@\n\
                             \x20delta\n\
                             -epsilon\n\
                             +EPSILON\n\
                             \x20zeta\n\
                             diff --git a/two.txt b/two.txt\n\
                             new file mode 100644\n\
                             index 0000000..3333333\n\
                             --- /dev/null\n\
                             +++ b/two.txt\n\
                             @@ -0,0 +1,2 @@\n\
                             +first\n\
                             +second\n";

    #[test]
    fn entries_carry_headers_verbatim_and_hunks_whole() {
        let entries = diff_file_entries(TWO_FILES);
        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0].path, "one.txt");
        assert_eq!(entries[0].hunks.len(), 2);
        assert!(entries[0]
            .header
            .starts_with("diff --git a/one.txt b/one.txt"));
        // The header stops at the first hunk, so the narrowed patch rebuilt from
        // it carries the same mode and index lines as the artifact.
        assert!(entries[0].header.ends_with("+++ b/one.txt"));
        assert!(entries[0].hunks[0].starts_with("@@ -1,3 +1,3 @@"));
        // Both sides of the change survive, so a narrowed patch can be replayed
        // against the tree rather than merely parsed.
        assert!(entries[0].hunks[0].contains("\n-beta\n+BETA\n"));
        assert!(entries[0].hunks[1].starts_with("@@ -10,3 +10,3 @@"));

        // A new file's target side is `/dev/null` in `---`, so the path has to
        // come from the `+++` side, and its mode line survives in the header.
        assert_eq!(entries[1].path, "two.txt");
        assert_eq!(entries[1].hunks.len(), 1);
        assert!(entries[1].header.contains("new file mode 100644"));
        assert!(entries[1].header.contains("+++ b/two.txt"));
    }

    #[test]
    fn a_removed_line_that_looks_like_a_section_header_stays_in_its_body() {
        // `-` followed by a line whose content starts with `-- ` renders as
        // `--- `, indistinguishable from the line that opens a file section.
        // Only the declared counts tell the two apart, which is why the walk
        // reads them instead of scanning for the next section.
        let diff = "diff --git a/x.txt b/x.txt\n\
                    --- a/x.txt\n\
                    +++ b/x.txt\n\
                    @@ -1,3 +1,3 @@\n\
                    \x20keep\n\
                    --- removed heading\n\
                    +---- kept heading\n\
                    \x20tail\n";
        let entries = diff_file_entries(diff);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hunks.len(), 1);
        assert!(entries[0].hunks[0].contains("--- removed heading"));
        assert!(entries[0].hunks[0].contains("+---- kept heading"));
    }

    #[test]
    fn a_deletion_takes_its_path_from_the_old_side() {
        let with_git_header = "diff --git a/gone.txt b/gone.txt\n\
                               deleted file mode 100644\n\
                               --- a/gone.txt\n\
                               +++ /dev/null\n\
                               @@ -1,2 +0,0 @@\n\
                               -first\n\
                               -second\n";
        let entries = diff_file_entries(with_git_header);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "gone.txt");
        assert_eq!(entries[0].hunks.len(), 1);

        // Without a `diff --git` line the only name either side carries is the
        // old one, since the new side does not exist.
        let bare = "--- a/gone.txt\n\
                    +++ /dev/null\n\
                    @@ -1,2 +0,0 @@\n\
                    -first\n\
                    -second\n";
        let entries = diff_file_entries(bare);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "gone.txt");
    }

    #[test]
    fn a_section_without_git_headers_is_still_one_entry() {
        let bare = "--- a/x.txt\n+++ b/x.txt\n@@ -1 +1 @@\n-a\n+b\n";
        let entries = diff_file_entries(bare);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "x.txt");
        assert_eq!(entries[0].hunks.len(), 1);
    }

    #[test]
    fn a_diff_without_git_headers_splits_on_its_own_sections() {
        let bare = "--- a/x.txt\n\
                    +++ b/x.txt\n\
                    @@ -1 +1 @@\n\
                    -a\n\
                    +b\n\
                    --- a/y.txt\n\
                    +++ b/y.txt\n\
                    @@ -1 +1 @@\n\
                    -c\n\
                    +d\n";
        let entries = diff_file_entries(bare);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "x.txt");
        assert_eq!(entries[1].path, "y.txt");
    }

    #[test]
    fn narrowing_keeps_the_header_and_only_the_chosen_hunk() {
        let entries = diff_file_entries(TWO_FILES);
        let patch = entries[0].narrow(&[1]);
        assert!(patch.contains("diff --git a/one.txt b/one.txt"));
        assert!(patch.contains("@@ -10,3 +10,3 @@"));
        assert!(!patch.contains("@@ -1,3 +1,3 @@"));
    }

    #[test]
    fn a_section_that_only_renames_has_a_header_and_no_hunks() {
        let diff = "diff --git a/old.txt b/new.txt\n\
                    similarity index 100%\n\
                    rename from old.txt\n\
                    rename to new.txt\n";
        let entries = diff_file_entries(diff);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "new.txt");
        assert!(entries[0].hunks.is_empty());
    }

    #[test]
    fn an_empty_diff_has_no_entries() {
        assert!(diff_file_entries("").is_empty());
    }

    /// A two-file diff, so a test can ask about a line inside the change, a
    /// line beside it, and a file the change never opened.
    ///
    /// The first hunk's new side runs 10 `keep`, 11 `new`, 12 `added`, 13
    /// `tail`; the second declares no count, which the grammar reads as one
    /// line.
    const CHANGED: &str = "diff --git a/crates/cog-core/src/lib.rs b/crates/cog-core/src/lib.rs\n\
                           --- a/crates/cog-core/src/lib.rs\n\
                           +++ b/crates/cog-core/src/lib.rs\n\
                           @@ -10,3 +10,4 @@\n\
                           \x20keep\n\
                           -old\n\
                           +new\n\
                           +added\n\
                           \x20tail\n\
                           @@ -40 +41 @@\n\
                           -before\n\
                           +after\n";

    /// One `--message-format=json` record, built rather than pasted so that
    /// this reader is held to the shape a run emits instead of to a copy of it.
    fn compiler_message(
        level: &str,
        code: Option<&str>,
        file: &str,
        line_start: u64,
        line_end: u64,
    ) -> String {
        serde_json::json!({
            "reason": "compiler-message",
            "message": {
                "level": level,
                "code": code.map(|code| serde_json::json!({ "code": code })),
                "message": "a message",
                "spans": [{
                    "file_name": file,
                    "line_start": line_start,
                    "line_end": line_end,
                    "is_primary": true,
                }],
            }
        })
        .to_string()
    }

    #[test]
    fn the_lines_a_diff_writes_are_its_added_ones() {
        let added = diff_added_lines(CHANGED);
        let lines: BTreeSet<u64> = added.get("crates/cog-core/src/lib.rs").unwrap().clone();
        // 10 is context, 11 and 12 are the two added lines, 13 is context
        // again; the second hunk's addition lands on 41.
        assert_eq!(lines, BTreeSet::from([11, 12, 41]));
        assert_eq!(added.len(), 1);
    }

    #[test]
    fn a_deletion_writes_no_lines() {
        let diff = "diff --git a/gone.txt b/gone.txt\n\
                    --- a/gone.txt\n\
                    +++ /dev/null\n\
                    @@ -1,2 +0,0 @@\n\
                    -one\n\
                    -two\n";
        assert!(diff_added_lines(diff).is_empty());
    }

    #[test]
    fn a_lint_on_a_line_the_change_writes_is_the_changes() {
        // The diagnostic names its file the way a run in this workspace names
        // it: relative to the workspace root (`crates/<crate>/src/lib.rs`),
        // which is the same root the diff's paths are relative to.
        let transcript = [
            compiler_message(
                "error",
                Some("clippy::ptr_arg"),
                "crates/cog-core/src/lib.rs",
                12,
                12,
            ),
            compiler_message(
                "error",
                Some("dead_code"),
                "crates/cog-core/src/lib.rs",
                11,
                12,
            ),
        ]
        .join("\n");
        let attribution = introduced_lints(&diff_added_lines(CHANGED), &transcript);
        assert_eq!(attribution.diagnostics, 2);
        assert_eq!(
            attribution.introduced,
            vec![
                // A span reaching from a context line into a written one is
                // still a finding on a line the change wrote.
                IntroducedLint {
                    code: "dead_code".into(),
                    file: "crates/cog-core/src/lib.rs".into(),
                    line: 11,
                    message: "a message".into(),
                },
                IntroducedLint {
                    code: "clippy::ptr_arg".into(),
                    file: "crates/cog-core/src/lib.rs".into(),
                    line: 12,
                    message: "a message".into(),
                },
            ]
        );
    }

    #[test]
    fn a_lint_the_tree_already_carried_is_not_the_changes() {
        let transcript = [
            // Line 10 is context and line 13 is context: the tree carried both
            // before the change and carries them after it.
            compiler_message(
                "error",
                Some("dead_code"),
                "crates/cog-core/src/lib.rs",
                10,
                10,
            ),
            compiler_message(
                "warning",
                Some("unused_variables"),
                "crates/cog-core/src/lib.rs",
                13,
                13,
            ),
        ]
        .join("\n");
        let attribution = introduced_lints(&diff_added_lines(CHANGED), &transcript);
        // Counted, and attributed to no one: an empty list alone would report
        // this tree and a clean one identically.
        assert_eq!(attribution.diagnostics, 2);
        assert!(attribution.introduced.is_empty());
    }

    #[test]
    fn a_lint_in_a_file_the_change_never_opened_is_not_the_changes() {
        let transcript = compiler_message(
            "error",
            Some("clippy::needless_return"),
            "crates/elsewhere/src/lib.rs",
            11,
            11,
        );
        let attribution = introduced_lints(&diff_added_lines(CHANGED), &transcript);
        assert_eq!(attribution.diagnostics, 1);
        assert!(attribution.introduced.is_empty());
    }

    #[test]
    fn a_bare_file_name_is_not_joined_to_a_path_that_ends_the_same_way() {
        // Matching on the tail would attribute one crate's lint to another
        // crate's change whenever both carry a file of the same name — and it
        // would read as a pass on the tree where that is wrong. The reader
        // joins on the whole path, so a short one matches nothing.
        let transcript = compiler_message("error", Some("clippy::ptr_arg"), "src/lib.rs", 11, 11);
        assert!(introduced_lints(&diff_added_lines(CHANGED), &transcript)
            .introduced
            .is_empty());
    }

    #[test]
    fn a_compilation_error_is_not_read_as_a_lint() {
        let transcript = [
            // A numbered rustc error, and a diagnostic with no code at all.
            compiler_message("error", Some("E0308"), "crates/cog-core/src/lib.rs", 11, 11),
            compiler_message("error", None, "crates/cog-core/src/lib.rs", 12, 12),
            // The summary a failing run prints after the records it counts.
            "{\"reason\":\"build-finished\",\"success\":false}".to_string(),
        ]
        .join("\n");
        let attribution = introduced_lints(&diff_added_lines(CHANGED), &transcript);
        // Nothing considered and nothing attributed: the run that compiles the
        // tree is what answers for a tree that does not compile, and naming a
        // lint here would send the producer after the wrong defect.
        assert_eq!(attribution, LintAttribution::default());
    }

    #[test]
    fn a_transcript_that_parses_to_nothing_attributes_nothing() {
        // A run that printed no records cannot convict anyone, and cannot
        // silently pass a lint either: the caller still has the exit status it
        // asked for, and the counts here say the transcript was empty.
        assert_eq!(
            introduced_lints(&diff_added_lines(CHANGED), "not json\n\n"),
            LintAttribution::default()
        );
    }

    /// The four levels must stay distinguishable on the ten-point scale. A
    /// mapping that collapsed two of them would make importance ordering depend
    /// on which level happened to round where.
    #[test]
    fn every_priority_level_lands_on_its_own_rating() {
        let ratings: Vec<u8> = [
            Priority::Low,
            Priority::Medium,
            Priority::High,
            Priority::Critical,
        ]
        .iter()
        .map(|p| p.importance_rating())
        .collect();
        let mut sorted = ratings.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ratings.len(), "two levels share a rating");
    }

    /// Importance sorts a queue, so the level an operator acts on first has to
    /// sort above the rest — a mapping that inverted the order would send the
    /// queue out backwards.
    #[test]
    fn priority_order_survives_the_mapping() {
        let ordered = [
            Priority::Critical,
            Priority::High,
            Priority::Medium,
            Priority::Low,
        ];
        for pair in ordered.windows(2) {
            assert!(
                pair[0].importance() > pair[1].importance(),
                "{:?} must outrank {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn a_goal_naming_files_yields_those_files() {
        assert_eq!(
            paths_named_in_goal("在 `README.md` 中补充 Windows 快速开始"),
            vec!["README.md".to_string()]
        );
        assert_eq!(
            paths_named_in_goal("fix the parser in crates/cog-core/src/lib.rs, keep it small"),
            vec!["crates/cog-core/src/lib.rs".to_string()]
        );
    }

    /// Prose is where a path gets its punctuation. A goal that ends a sentence
    /// on the file name, or wraps it in backticks, names the same file as one
    /// that does neither.
    #[test]
    fn surrounding_punctuation_is_not_part_of_the_name() {
        assert_eq!(
            paths_named_in_goal("please update README.md."),
            vec!["README.md".to_string()]
        );
        assert_eq!(
            paths_named_in_goal("see (`deploy/values.yaml`), it is stale"),
            vec!["deploy/values.yaml".to_string()]
        );
    }

    #[test]
    fn ordinary_words_do_not_become_paths() {
        assert!(paths_named_in_goal("improve the frontend module").is_empty());
        assert!(paths_named_in_goal("make it faster and safer").is_empty());
        // Abbreviations carry a dot and a single trailing letter; nothing ships
        // an extension one character long, so they stay words.
        assert!(paths_named_in_goal("e.g. tidy this up").is_empty());
        assert!(paths_named_in_goal("i.e. do the same thing").is_empty());
    }

    /// A glob, a URL and an assignment all contain the characters of a path
    /// without being one. Reading them as names would invent anchors that no
    /// checkout can confirm or deny.
    #[test]
    fn path_lookalikes_are_dropped() {
        assert!(paths_named_in_goal("touch crates/**/*.rs instead").is_empty());
        assert!(paths_named_in_goal("see https://example.com/docs/guide.md").is_empty());
        assert!(paths_named_in_goal("set KEY=deploy/values.yaml first").is_empty());
    }

    #[test]
    fn one_file_written_two_ways_is_one_name() {
        assert_eq!(
            paths_named_in_goal(r"update .\src\main.rs and src/main.rs"),
            vec!["src/main.rs".to_string()]
        );
        assert_eq!(
            paths_named_in_goal("check ./README.md and README.md"),
            vec!["README.md".to_string()]
        );
    }

    /// A name that walks out of the checkout can never be confirmed by it, so
    /// it is not a claim worth carrying to a caller that resolves against one.
    #[test]
    fn a_name_that_escapes_the_root_is_not_kept() {
        assert!(paths_named_in_goal("read ../../etc/passwd now").is_empty());
    }

    #[test]
    fn a_name_the_goal_uses_is_seen() {
        assert!(mentions_name("改 README 里的快速开始", "README"));
        assert!(mentions_name("update README.md please", "README.md"));
        assert!(mentions_name("README", "README"));
    }

    /// The reason the caller gets its candidates from a filesystem instead of
    /// from the text: prose is full of words, and only a real name can be the
    /// subject of a claim.
    #[test]
    fn a_name_inside_a_longer_word_is_not_used() {
        assert!(!mentions_name("improve the module", "README"));
        assert!(!mentions_name("the README.md is stale", "README"));
        assert!(!mentions_name("readmore about it", "readme"));
        assert!(!mentions_name("anything at all", ""));
    }

    /// The transcript below is the shape `cargo test --workspace --no-fail-fast`
    /// prints: a `Running` line per binary, then that binary's failures, then
    /// the same names again in its summary. Reading only the run lines is what
    /// keeps the summary from counting every failure a second time, and the
    /// binary is what keeps two crates' same-named test apart.
    const FAILING_TRANSCRIPT: &str = "\
     Running unittests src/lib.rs (target/debug/deps/cog_extension-3f2a1b4c5d6e7f80)\n\
\n\
running 2 tests\n\
test hostdocs::tests::every_published_read_outcome_is_reachable ... FAILED\n\
test hostdocs::tests::another ... ok\n\
\n\
failures:\n\
\n\
---- hostdocs::tests::every_published_read_outcome_is_reachable stdout ----\n\
a 0o000 directory should not be listable\n\
\n\
failures:\n\
    hostdocs::tests::every_published_read_outcome_is_reachable\n\
\n\
test result: FAILED. 1 passed; 1 failed; 0 ignored\n\
\n\
     Running unittests src/lib.rs (target/debug/deps/cog_auth-0a1b2c3d4e5f6071)\n\
\n\
running 1 test\n\
test hostdocs::tests::every_published_read_outcome_is_reachable ... FAILED\n\
\n\
test result: FAILED. 0 passed; 1 failed; 0 ignored\n";

    #[test]
    fn a_failing_test_is_read_once_and_carries_the_binary_that_ran_it() {
        let failing = failing_tests(FAILING_TRANSCRIPT);
        assert_eq!(
            failing.into_iter().collect::<Vec<_>>(),
            vec![
                "cog_auth/hostdocs::tests::every_published_read_outcome_is_reachable",
                "cog_extension/hostdocs::tests::every_published_read_outcome_is_reachable",
            ],
            "same-named tests in two crates have to stay two entries, and the \
             summary must not add a third"
        );
    }

    /// The shape a caller gets when it runs stdout and stderr through separate
    /// pipes and joins them: the harness's whole output first, cargo's
    /// announcements after it. No `FAILED` line has a `Running` line above it
    /// here, so reading the nearest preceding one names every test `<unknown>`
    /// -- which is what the cluster's own refusals looked like.
    #[test]
    fn a_failure_keeps_its_binary_when_the_two_streams_are_joined() {
        let output = "\
running 2 tests\n\
test hostdocs::tests::every_published_read_outcome_is_reachable ... FAILED\n\
test hostdocs::tests::another ... ok\n\
\n\
failures:\n\
\n\
---- hostdocs::tests::every_published_read_outcome_is_reachable stdout ----\n\
a 0o000 directory should not be listable\n\
\n\
test result: FAILED. 1 passed; 1 failed; 0 ignored\n\
\n\
running 1 test\n\
test quota::tests::a_window_that_has_passed_is_not_waited_on ... FAILED\n\
\n\
test result: FAILED. 0 passed; 1 failed; 0 ignored\n\
\n\
     Running unittests src/lib.rs (target/debug/deps/cog_extension-3f2a1b4c5d6e7f80)\n\
     Running unittests src/lib.rs (target/debug/deps/cog_auth-0a1b2c3d4e5f6071)\n";
        assert_eq!(
            failing_tests(output).into_iter().collect::<Vec<_>>(),
            vec![
                "cog_auth/quota::tests::a_window_that_has_passed_is_not_waited_on",
                "cog_extension/hostdocs::tests::every_published_read_outcome_is_reachable",
            ]
        );
    }

    /// The pairing is by position, so it may only be used when the two sides
    /// counted the same runs. An excerpt that kept the binaries and dropped the
    /// harness output would otherwise hand the first failure whichever binary
    /// came first rather than admitting it does not know.
    #[test]
    fn a_failure_is_not_guessed_a_binary_when_the_two_counts_differ() {
        let output = "\
test a::b ... FAILED\n\
\n\
     Running unittests src/lib.rs (target/debug/deps/cog_core-1111111111111111)\n\
     Running unittests src/lib.rs (target/debug/deps/cog_core-2222222222222222)\n";
        assert_eq!(
            failing_tests(output).into_iter().collect::<Vec<_>>(),
            vec!["<unknown>/a::b"],
            "one run, two binaries: nothing says which one it was"
        );
    }

    /// The metadata hash is part of the binary's file name, so it moves when the
    /// compiler's metadata moves. Two runs of an unchanged test must read as the
    /// same test or a comparison between them is always empty and every failure
    /// looks like the change's own.
    #[test]
    fn the_metadata_hash_does_not_enter_the_identity() {
        let run = |hash: &str| {
            let output = format!(
                "     Running unittests src/lib.rs (target/debug/deps/cog_core-{hash})\n\
                 \n\
                 test a::b ... FAILED\n"
            );
            failing_tests(&output)
        };
        assert_eq!(run("1111111111111111"), run("2222222222222222"));
        assert!(run("1111111111111111").contains("cog_core/a::b"));
    }

    #[test]
    fn a_doc_test_failure_belongs_to_its_crate() {
        let output = "   Doc-tests cog_core (target/debug/deps/cog_core-abcdef0123456789)\n\
                      \n\
                      test src/lib.rs - read (line 12) ... FAILED\n";
        assert!(failing_tests(output).contains("cog_core/src/lib.rs - read (line 12)"));
    }

    /// Cargo announces a crate whose doc tests it is about to run without a
    /// path, and that shape counts as a run like any other. Reading it as
    /// "no binary here" makes the runs outnumber the announced binaries, which
    /// costs every failure in the transcript its name -- on the cluster's own
    /// refusal 27 of 157 runs were announced this way.
    #[test]
    fn a_doc_test_run_is_announced_without_a_path() {
        let output = "running 1 test\n\
                      test src/lib.rs - read (line 12) ... FAILED\n\
                      \n\
                      test result: FAILED. 0 passed; 1 failed; 0 ignored\n\
                      \n\
                      134 tests, 0 failures\n\
                      \n\
                      running 2 tests\n\
                      test a::b ... FAILED\n\
                      \n\
                      test result: FAILED. 1 passed; 1 failed; 0 ignored\n\
                      \n\
                      \x20  Doc-tests cog_core\n\
                      \x20  Doc-tests cog_auth\n";
        assert_eq!(
            failing_tests(output).into_iter().collect::<Vec<_>>(),
            vec!["cog_auth/a::b", "cog_core/src/lib.rs - read (line 12)",]
        );
    }

    /// A run that failed without naming a test did not fail a test: it failed
    /// to build one, or the harness died. The empty set is what the caller
    /// reads as "nothing here can be blamed on the tree's own state".
    #[test]
    fn a_build_failure_names_no_test() {
        let output = "   Compiling cog-auth v0.5.8\n\
                      error[E0308]: mismatched types\n\
                      error: could not compile `cog-auth` (lib) due to 1 previous error\n";
        assert!(failing_tests(output).is_empty());
    }

    /// The reading that set the budget: on the cluster's own refusals the first
    /// `FAILED` line sat at line 923 of 4568, so a 2000-character window taken
    /// from the front held 464 lines of `... ok` and stopped short of the one
    /// line that said what broke.
    #[test]
    fn the_diagnosis_survives_a_transcript_that_buries_it() {
        let mut output = String::from(
            "     Running unittests src/lib.rs (target/debug/deps/cog_extension-3f2a1b4c5d6e7f80)\n\
             \n\
             running 1000 tests\n",
        );
        for case in 0..900 {
            output.push_str(&format!("test hostdocs::tests::case_{case} ... ok\n"));
        }
        output.push_str(
            "test hostdocs::tests::every_published_read_outcome_is_reachable ... FAILED\n\
             \n\
             ---- hostdocs::tests::every_published_read_outcome_is_reachable stdout ----\n\
             thread 'main' panicked at crates/cog-extension/src/hostdocs.rs:3271:14:\n\
             a 0o000 directory should not be listable\n",
        );

        let digest = failure_digest(&output, 2000);
        assert!(
            digest.chars().count() <= 2000,
            "the budget is a budget, not a suggestion"
        );
        assert!(
            digest.contains(
                "cog_extension/hostdocs::tests::every_published_read_outcome_is_reachable"
            ),
            "the digest has to name the test the run failed; got:\n{digest}"
        );
        assert!(
            digest.contains("hostdocs.rs:3271"),
            "the panic site is the other half of the diagnosis; got:\n{digest}"
        );
        assert!(
            !digest.starts_with("test hostdocs::tests::case_"),
            "the passing tests are what the budget was being spent on"
        );
    }

    /// The measured shape of a `--workspace --no-fail-fast` refusal: the
    /// binary that failed ran in the middle of the run, and the binaries after
    /// it kept printing, so the end of the transcript is their output and
    /// cargo's driver lines. A window taken from the tail is not the failure --
    /// it names no test that failed and holds none of what the test said.
    #[test]
    fn the_diagnosis_survives_the_binaries_that_ran_after_the_failing_one() {
        let mut output = String::from(
            "     Running tests/a_test.rs (target/debug/deps/a_test-0123456789abcdef)\n\
             \n\
             running 2 tests\n\
             test a::one ... ok\n\
             test a::two ... ok\n\
             \n\
             test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n\
             \n\
             \x20    Running tests/contract_test.rs (target/debug/deps/contract_test-fedcba9876543210)\n\
             \n\
             running 31 tests\n",
        );
        for case in 0..30 {
            output.push_str(&format!("test contract::case_{case} ... ok\n"));
        }
        output.push_str(
            "test contract::every_series_has_a_reader ... FAILED\n\
             \n\
             failures:\n\
             \n\
             ---- contract::every_series_has_a_reader stdout ----\n\
             \n\
             thread 'contract::every_series_has_a_reader' panicked at tests/contract_test.rs:1139:5:\n\
             闭集里的这些序列没有任何告警规则或面板在读:\n\
             llm_upstream_registered\n\
             llm_upstream_request_shape_last_rejection_unix\n\
             note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\n\
             \n\
             failures:\n\
             \x20   contract::every_series_has_a_reader\n\
             \n\
             test result: FAILED. 30 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n\
             \n",
        );
        // Six more binaries run after the one that failed, none of them failing.
        for n in 0..6 {
            output.push_str(&format!(
                "     Running tests/tail_{n}_test.rs (target/debug/deps/tail_{n}_test-abcdefabcdefabcd)\n\
                 \n\
                 running 0 tests\n\
                 \n\
                 test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n\
                 \n"
            ));
        }
        output.push_str("error: 1 target failed:\n    `-p cogneva --test contract_test`\n");

        let digest = failure_digest(&output, 2000);
        assert!(
            digest.chars().count() <= 2000,
            "the budget is a budget, not a suggestion; got {}",
            digest.chars().count()
        );
        assert!(
            digest.contains("llm_upstream_request_shape_last_rejection_unix"),
            "the assertion message is the half a re-attempt needs; got:\n{digest}"
        );
        assert!(
            digest.contains("tests/contract_test.rs:1139"),
            "the site the assertion was made at locates the tree to read; got:\n{digest}"
        );
    }

    /// A refusal that names no failing test is a formatter diff or a compile
    /// error, and both say what they are in their opening lines. Taking the tail
    /// of one would drop the `Diff in <file>:<line>:` header that locates it.
    #[test]
    fn a_refusal_with_no_failing_test_keeps_its_head() {
        let output = "Change is not what this workspace's formatter produces:\n\
                      Diff in crates/cog-gateway/src/chat.rs:156:\n\
                      -    if !claims.permissions.contains(&cog_core::Permission::AgentWrite) {\n";
        assert_eq!(failure_digest(output, 2000), output);
    }

    /// The budget used to be spent as a byte slice, which panics when the cut
    /// lands inside a multi-byte character. A panic here would take down the
    /// recorder whose whole job is to keep the refusal.
    #[test]
    fn a_budget_that_lands_inside_a_character_does_not_panic() {
        let output = "test a::b ... FAILED\né".repeat(200);
        let digest = failure_digest(&output, 17);
        assert!(digest.chars().count() <= 17, "got {digest:?}");
    }

    /// Only a trailing run of sixteen hex digits is the metadata hash. Anything
    /// else that happens to follow a hyphen is part of the name and is kept:
    /// truncating on a guess would merge two binaries into one identity, which
    /// is the failure this whole key exists to avoid.
    #[test]
    fn a_name_that_is_not_hash_decorated_is_kept() {
        let decorated = |binary: &str| {
            let output = format!(
                "     Running tests/foo.rs (target/debug/deps/{binary})\n\n\
                 test x ... FAILED\n"
            );
            failing_tests(&output).into_iter().next().unwrap()
        };
        assert_eq!(decorated("foo_bar-1234567890abcdef"), "foo_bar/x");
        assert_eq!(
            decorated("foo_bar-1234567890abcde"),
            "foo_bar-1234567890abcde/x",
            "fifteen hex digits is not the hash cargo writes"
        );
        assert_eq!(
            decorated("foo_bar-zzzzzzzzzzzzzzzz"),
            "foo_bar-zzzzzzzzzzzzzzzz/x",
            "a suffix that is not hex at all is part of the name"
        );
    }

    #[test]
    fn prose_and_config_are_an_allow_list_each_way() {
        assert!(is_prose_path("docs/guide.md"));
        assert!(is_prose_path("README.MARKDOWN"));
        assert!(!is_prose_path("crates/cog-core/src/lib.rs"));
        // A bare name has no extension to read, so it stays code.
        assert!(!is_prose_path("LICENSE"));
        // A dotted directory is not a file name at all.
        assert!(!is_prose_path("docs/v1.2/notes"));

        assert!(is_config_path("config/cogneva.json"));
        assert!(is_config_path("deploy/x.yaml"));
        assert!(is_config_path("Cargo.toml"));
        assert!(!is_config_path("docs/guide.md"));
        // The reference document is configuration the deployment ships, so it
        // has to read as configuration. A rule that went by how a file looks —
        // "an example, so documentation" — would put it in the prose row.
        assert!(is_config_path("cogneva.example.json"));
    }

    #[test]
    fn a_value_rewritten_in_place_losses_no_name() {
        let diff = "\
--- a/cogneva.json
+++ b/cogneva.json
@@ -1,2 +1,2 @@
-    \"extraction_input_budget_tokens\": 4096,
+    \"extraction_input_budget_tokens\": 8192,
";
        let shape = diff_shape(diff);
        assert!(shape
            .changed_config_keys
            .contains("extraction_input_budget_tokens"));
        assert_eq!(shape.net_removed_tokens(), BTreeMap::new());
    }

    #[test]
    fn a_name_taken_out_and_not_put_back_is_net_removed() {
        let diff = "\
--- a/src/gate.rs
+++ b/src/gate.rs
@@ -1,3 +1,2 @@
-    let cap = policy.max_diff_lines;
-    let other = 1;
+    let mode = policy.mode;
";
        let net = diff_shape(diff).net_removed_tokens();
        let gone = net.get("src/gate.rs").expect("the file lost names");
        // `policy` came back on the added line, so it did not move away; the
        // names that did are the two the added side never repeats.
        assert!(gone.contains("max_diff_lines"));
        assert!(gone.contains("other"));
        assert!(!gone.contains("policy"));
    }

    #[test]
    fn identifier_tokens_skip_numbers_and_keep_names() {
        let tokens = identifier_tokens("for (i, x_1) in v.iter() { let a9 = 12; }");
        assert!(tokens.contains("x_1"));
        assert!(tokens.contains("a9"));
        assert!(!tokens.contains("12"));
        assert!(!tokens.contains("9"));
    }

    #[test]
    fn a_malformed_diff_cannot_invent_a_target_or_a_key() {
        // The section grammar decides which lines are body; a body line that
        // merely looks like a header must not start a section, and a key read
        // off such a line would be a reading of the diff's formatting rather
        // than of the change.
        let diff = "\
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -1,2 +1,2 @@
-    let fake_key_here = 1;
+    let other = 2;
";
        let shape = diff_shape(diff);
        assert!(shape.changed_config_keys.is_empty());
        assert!(shape.config_files.is_empty());
    }
}
