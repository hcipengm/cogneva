use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    /// Self-review score (0.0–1.0) if available.
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
        assert_eq!(seen.len(), 9, "ALL is missing a cause: {seen:?}");
        for (index, cause) in RejectionCause::ALL.iter().enumerate() {
            assert_eq!(cause.slot(), index, "{cause:?} does not sit at {index}");
        }
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
}
