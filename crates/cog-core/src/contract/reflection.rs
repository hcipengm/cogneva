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

/// Parse a unified diff change and return the list of files it touches.
///
/// Extracts paths from `+++ b/<path>` lines. New files appear as
/// `+++ b/<path>` with `--- /dev/null`, so this also handles additions.
/// This is a pure function shared by collaboration (static validation) and
/// reflection (change pipeline).
pub fn parse_diff_affected_files(content: &str) -> crate::SFResult<Vec<String>> {
    let mut files = Vec::new();

    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("+++") {
            let rest = rest.trim();
            // Unified diff produced by git uses "+++ b/<path>".
            // Strip the "b/" prefix when present.
            let path_str = rest.strip_prefix("b/").unwrap_or(rest);

            // Skip the timestamp header that `git diff` sometimes emits.
            let path_str = path_str.split_whitespace().next().unwrap_or(path_str);

            if path_str == "/dev/null" {
                continue;
            }

            files.push(path_str.to_string());
        }
    }

    if files.is_empty() {
        return Err(crate::SFError::Validation(
            "No file paths found in change (expected '+++ b/<path>' lines)".into(),
        ));
    }

    Ok(files)
}

/// Structural check of a unified diff: every hunk must carry exactly the
/// number of lines its header declares.
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
            match line.chars().next() {
                Some('+') => {
                    hunk = Some((
                        hunk_line,
                        old_declared,
                        new_declared,
                        old_seen,
                        new_seen + 1,
                    ))
                }
                Some('-') => {
                    hunk = Some((
                        hunk_line,
                        old_declared,
                        new_declared,
                        old_seen + 1,
                        new_seen,
                    ))
                }
                Some('\\') => {}
                // Context line. A blank line loses its leading space in some
                // emitters; git reads it as context, so this does too.
                _ => {
                    hunk = Some((
                        hunk_line,
                        old_declared,
                        new_declared,
                        old_seen + 1,
                        new_seen + 1,
                    ))
                }
            }
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
            match parse_hunk_header(line) {
                Some((old_declared, new_declared)) => {
                    hunk = Some((lineno, old_declared, new_declared, 0, 0))
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

    None
}

/// Parse the `-old,count +new,count` fields of a hunk header. A missing count
/// means one line, matching the unified-diff grammar.
fn parse_hunk_header(line: &str) -> Option<(u64, u64)> {
    let inner = line.strip_prefix("@@")?;
    let end = inner.find("@@")?;
    let mut fields = inner[..end].split_whitespace();
    let old = fields.next()?.strip_prefix('-')?;
    let new = fields.next()?.strip_prefix('+')?;
    let count = |spec: &str| -> Option<u64> {
        match spec.split_once(',') {
            Some((_, c)) => c.parse().ok(),
            None => Some(1),
        }
    };
    Some((count(old)?, count(new)?))
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
}
