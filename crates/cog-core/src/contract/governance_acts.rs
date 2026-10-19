//! Handovers and decisions as rows of the task graph.
//!
//! A handover says that one unit's standing responsibility moves to another,
//! and it must be signed before the move counts. A decision says that a
//! question was answered -- either by a principle that settles it, or by a
//! ballot with rules fixed before the first vote. Both are rows in the same
//! task table the rest of the platform uses, in the same reserved workspace as
//! the faces. There is no handover table and no decision table: a second store
//! answering "who owns this" or "what was decided" disagrees with the first
//! the day one of them is updated.
//!
//! The mapping onto a task row is deliberate and small:
//!
//! - A handover is one non-executable row naming a face and the two units. Its
//!   signatures live in the payload. It is `pending` while drafts are being
//!   signed, `scheduled` once every required signature is present, `completed`
//!   when the face row was moved, `cancelled` when withdrawn, and `failed`
//!   when it expired unused.
//! - A decision is one non-executable row. Its rules -- options, eligible
//!   units, quorum, tie-break, deadline -- are written when it opens and are
//!   never rewritten. Its state is in the payload and mirrored by the row's
//!   status, so a reader selecting by status still sees terminal decisions as
//!   terminal.
//! - A vote is a child row whose `owner_unit` is the voter. Casting writes it;
//!   withdrawing sets it to `cancelled`. Counting reads the children, so the
//!   tally is never a stored number that could disagree with the votes.
//!
//! Reading by status alone cannot tell a resolved decision from a withdrawn
//! one, so the readers in this module always take the payload state and treat
//! the status as a coarse mirror. The two are written together in one row.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Task, TaskStatus, TaskType, GOVERNANCE_WORKSPACE};

/// The task type a handover row carries.
pub const HANDOVER_TASK_TYPE: &str = "handover";
/// The task type a decision row carries.
pub const DECISION_TASK_TYPE: &str = "decision";
/// The task type a vote row carries.
pub const VOTE_TASK_TYPE: &str = "vote";

/// Where each row keeps its payload.
pub const HANDOVER_INPUT_KEY: &str = "handover";
pub const DECISION_INPUT_KEY: &str = "decision";
pub const VOTE_INPUT_KEY: &str = "vote";

/// How long a fully signed handover may sit unapplied before it reads as a
/// drift of its own.
///
/// Fifteen minutes: enough for the two parties to finish the move they just
/// signed, short enough that a signature nobody applied is visible inside the
/// same working session. A signed handover that is never applied is the shape
/// a change of responsibility takes when the change was only on paper.
pub const SIGNED_UNAPPLIED_LIMIT_SECS: i64 = 900;

/// One signature on a handover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    pub unit: String,
    pub at: DateTime<Utc>,
}

/// What a handover row says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoverSpec {
    pub face_id: String,
    /// Who held the face before. `None` when the face was vacant.
    pub from_unit: Option<String>,
    pub to_unit: String,
    pub witness_unit: String,
    /// The reading the move was justified by, and when it was taken.
    pub criterion_reading: Option<String>,
    pub reading_at: Option<DateTime<Utc>>,
    /// What the new owner inherits as unfinished business.
    #[serde(default)]
    pub open_actions: Vec<String>,
    #[serde(default)]
    pub open_task_ids: Vec<String>,
    /// Anything the new owner needs to be told about access or permissions.
    pub permissions_context: Option<String>,
    /// The conditions under which the handover is no longer valid.
    pub expires_when: Option<DateTime<Utc>>,
    #[serde(default)]
    pub signed_by: Vec<Signature>,
}

impl HandoverSpec {
    /// The three units whose signatures make a handover valid.
    pub fn required_signers(&self) -> Vec<&str> {
        let mut signers = vec![self.to_unit.as_str(), self.witness_unit.as_str()];
        if let Some(from) = self.from_unit.as_deref() {
            signers.push(from);
        }
        signers
    }

    pub fn is_fully_signed(&self) -> bool {
        self.required_signers()
            .iter()
            .all(|unit| self.signed_by.iter().any(|s| s.unit == *unit))
    }
}

/// The state a handover row is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoverState {
    Draft,
    Signed,
    Applied,
    Withdrawn,
    Expired,
}

impl HandoverState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Signed => "signed",
            Self::Applied => "applied",
            Self::Withdrawn => "withdrawn",
            Self::Expired => "expired",
        }
    }

    /// The row status this state is written as.
    pub fn status(self) -> TaskStatus {
        match self {
            Self::Draft => TaskStatus::Pending,
            Self::Signed => TaskStatus::Scheduled,
            Self::Applied => TaskStatus::Completed,
            Self::Withdrawn => TaskStatus::Cancelled,
            Self::Expired => TaskStatus::Failed,
        }
    }
}

/// One handover as the graph holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handover {
    pub id: String,
    pub status: TaskStatus,
    pub spec: HandoverSpec,
    pub updated_at: DateTime<Utc>,
}

impl Handover {
    pub fn state(&self) -> HandoverState {
        match self.status {
            TaskStatus::Pending | TaskStatus::Running => HandoverState::Draft,
            TaskStatus::Scheduled => HandoverState::Signed,
            TaskStatus::Completed => HandoverState::Applied,
            TaskStatus::Cancelled => HandoverState::Withdrawn,
            TaskStatus::Failed => HandoverState::Expired,
        }
    }

    pub fn from_task(task: &Task) -> Option<Self> {
        if !is_typed(task, HANDOVER_TASK_TYPE) {
            return None;
        }
        Some(Self {
            id: task.id.clone(),
            status: task.status,
            spec: serde_json::from_value(task.input.get(HANDOVER_INPUT_KEY)?.clone()).ok()?,
            updated_at: task.updated_at,
        })
    }
}

/// Build a handover row in its draft state.
pub fn handover_task(id: &str, spec: HandoverSpec, now: DateTime<Utc>) -> Task {
    governance_task(
        id,
        HANDOVER_TASK_TYPE,
        HANDOVER_INPUT_KEY,
        serde_json::to_value(&spec).expect("a handover spec serialises"),
        TaskStatus::Pending,
        Some(spec.to_unit.clone()),
        now,
    )
}

/// How the ballot settles a question.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TieBreak {
    /// The option already in force wins a tie.
    StatusQuo,
    /// A tie is not a result; a new round is opened.
    SecondRound,
    /// A tie is handed to a named body; until it answers the question is open.
    EscalateNamedBody,
}

impl TieBreak {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StatusQuo => "status_quo",
            Self::SecondRound => "second_round",
            Self::EscalateNamedBody => "escalate_named_body",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "status_quo" => Some(Self::StatusQuo),
            "second_round" => Some(Self::SecondRound),
            "escalate_named_body" => Some(Self::EscalateNamedBody),
            _ => None,
        }
    }
}

/// How a decision was or will be reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionMode {
    /// A principle settles it outright; nobody votes.
    Principle,
    /// The eligible units vote under rules fixed at open time.
    Ballot,
}

impl DecisionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Principle => "principle",
            Self::Ballot => "ballot",
        }
    }
}

/// Where a decision stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionState {
    Open,
    Resolved,
    /// The rules ran and produced no winner (quorum, tie, no votes).
    Unresolved,
    /// A later decision replaced this one without rewriting it.
    Superseded,
    Withdrawn,
}

impl DecisionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
            Self::Unresolved => "unresolved",
            Self::Superseded => "superseded",
            Self::Withdrawn => "withdrawn",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "resolved" => Some(Self::Resolved),
            "unresolved" => Some(Self::Unresolved),
            "superseded" => Some(Self::Superseded),
            "withdrawn" => Some(Self::Withdrawn),
            _ => None,
        }
    }

    /// The row status this state is mirrored as.
    pub fn status(self) -> TaskStatus {
        match self {
            Self::Open => TaskStatus::Pending,
            Self::Resolved => TaskStatus::Completed,
            Self::Unresolved => TaskStatus::Failed,
            Self::Superseded | Self::Withdrawn => TaskStatus::Cancelled,
        }
    }
}

/// What a decision row says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionSpec {
    pub kind: String,
    pub subject: String,
    pub options: Vec<String>,
    /// The option in force before the question was asked. Required for a
    /// ballot, because a tie has to be resolvable without asking again.
    pub status_quo: Option<String>,
    pub mode: DecisionMode,
    pub state: DecisionState,
    /// The voters, fixed when the decision opens.
    #[serde(default)]
    pub eligible_units: Vec<String>,
    #[serde(default)]
    pub quorum: u32,
    pub tie_break: Option<TieBreak>,
    pub deadline: Option<DateTime<Utc>>,
    /// Principle mode: which principle settled it, and why that one applies.
    pub principle_slug: Option<String>,
    pub principle_rationale: Option<String>,
    /// The option the decision came out as, once it is resolved.
    pub result_option: Option<String>,
    /// Why that was the result, in the rules' own words.
    pub result_reason: Option<String>,
    /// Set on the replacement row: which decision this one supersedes.
    pub supersedes: Option<String>,
}

/// One decision as the graph holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub id: String,
    pub status: TaskStatus,
    pub spec: DecisionSpec,
    pub updated_at: DateTime<Utc>,
}

impl Decision {
    pub fn from_task(task: &Task) -> Option<Self> {
        if !is_typed(task, DECISION_TASK_TYPE) {
            return None;
        }
        Some(Self {
            id: task.id.clone(),
            status: task.status,
            spec: serde_json::from_value(task.input.get(DECISION_INPUT_KEY)?.clone()).ok()?,
            updated_at: task.updated_at,
        })
    }
}

/// Build a decision row. Rules are written once, here, and never rewritten.
pub fn decision_task(id: &str, spec: DecisionSpec, status: TaskStatus, now: DateTime<Utc>) -> Task {
    governance_task(
        id,
        DECISION_TASK_TYPE,
        DECISION_INPUT_KEY,
        serde_json::to_value(&spec).expect("a decision spec serialises"),
        status,
        None,
        now,
    )
}

/// What a vote row says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteSpec {
    pub decision_id: String,
    pub option: String,
    pub cast_at: DateTime<Utc>,
}

/// One vote as the graph holds it. A cancelled vote is a withdrawn one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Vote {
    pub decision_id: String,
    pub unit: String,
    pub option: String,
    pub withdrawn: bool,
}

impl Vote {
    pub fn from_task(task: &Task) -> Option<Self> {
        if !is_typed(task, VOTE_TASK_TYPE) {
            return None;
        }
        let spec: VoteSpec =
            serde_json::from_value(task.input.get(VOTE_INPUT_KEY)?.clone()).ok()?;
        Some(Self {
            decision_id: spec.decision_id,
            unit: task.owner_unit.clone()?,
            option: spec.option,
            withdrawn: task.status == TaskStatus::Cancelled,
        })
    }
}

/// The row id of one unit's vote on one decision.
///
/// Derived rather than random so that casting twice writes the same row: a
/// second vote is a change of mind, not a second ballot. Uniqueness then comes
/// from the table's primary key, which no writer can forget to check.
pub fn vote_id(decision_id: &str, unit: &str) -> String {
    format!("{decision_id}:{unit}")
}

/// Build (or rewrite) one unit's vote row.
pub fn vote_task(decision_id: &str, unit: &str, option: &str, now: DateTime<Utc>) -> Task {
    let spec = VoteSpec {
        decision_id: decision_id.to_string(),
        option: option.to_string(),
        cast_at: now,
    };
    let mut task = governance_task(
        &vote_id(decision_id, unit),
        VOTE_TASK_TYPE,
        VOTE_INPUT_KEY,
        serde_json::to_value(&spec).expect("a vote spec serialises"),
        TaskStatus::Pending,
        Some(unit.to_string()),
        now,
    );
    task.parent_task_id = Some(decision_id.to_string());
    task
}

/// One option's count and who is behind it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    /// Option -> units that voted for it, sorted by unit.
    pub by_option: BTreeMap<String, Vec<String>>,
    /// Eligible units that cast a countable vote.
    pub voters: Vec<String>,
}

impl Tally {
    pub fn count(&self, option: &str) -> usize {
        self.by_option.get(option).map(Vec::len).unwrap_or(0)
    }
}

/// Count the votes that count.
///
/// A vote counts when its unit is on the roster fixed at open time and it was
/// not withdrawn. A vote from outside the roster is not a vote: taking it would
/// let anyone join a ballot after the roll was closed.
pub fn tally(spec: &DecisionSpec, votes: &[Vote]) -> Tally {
    let mut tally = Tally::default();
    let mut seen: Vec<&str> = Vec::new();
    for vote in votes {
        if vote.withdrawn {
            continue;
        }
        if !spec.eligible_units.iter().any(|u| u == &vote.unit) {
            continue;
        }
        if seen.contains(&vote.unit.as_str()) {
            continue;
        }
        seen.push(&vote.unit);
        tally.voters.push(vote.unit.clone());
        tally
            .by_option
            .entry(vote.option.clone())
            .or_default()
            .push(vote.unit.clone());
    }
    tally.voters.sort();
    for units in tally.by_option.values_mut() {
        units.sort();
    }
    tally
}

/// What closing a ballot produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BallotResult {
    pub state: DecisionState,
    pub option: Option<String>,
    pub reason: String,
    pub tally: Tally,
}

/// Apply the rules fixed at open time.
///
/// `now` is a parameter so the same rules can be replayed over a past ballot
/// without the answer depending on when the replay runs.
pub fn close_ballot(spec: &DecisionSpec, votes: &[Vote], now: DateTime<Utc>) -> BallotResult {
    let tally = tally(spec, votes);
    let base = |state: DecisionState, option: Option<String>, reason: &str| BallotResult {
        state,
        option,
        reason: reason.to_string(),
        tally: tally.clone(),
    };

    if let Some(deadline) = spec.deadline {
        if now < deadline {
            return base(DecisionState::Open, None, "the deadline has not passed");
        }
    } else {
        return base(
            DecisionState::Open,
            None,
            "a ballot without a deadline can never close on its own",
        );
    }

    if (tally.voters.len() as u32) < spec.quorum {
        return base(
            DecisionState::Unresolved,
            None,
            &format!(
                "quorum not met: {} of {} eligible voted, {} required",
                tally.voters.len(),
                spec.eligible_units.len(),
                spec.quorum
            ),
        );
    }

    let mut ranked: Vec<(String, usize)> = spec
        .options
        .iter()
        .map(|option| (option.clone(), tally.count(option)))
        .filter(|(_, count)| *count > 0)
        .collect();
    if ranked.is_empty() {
        return base(DecisionState::Unresolved, None, "no vote was cast");
    }
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let top = ranked[0].1;
    let leaders: Vec<String> = ranked
        .into_iter()
        .filter(|(_, count)| *count == top)
        .map(|(option, _)| option)
        .collect();

    if leaders.len() == 1 {
        return base(
            DecisionState::Resolved,
            Some(leaders[0].clone()),
            &format!(
                "majority: {} of {} countable votes",
                top,
                tally.voters.len()
            ),
        );
    }

    match spec.tie_break {
        Some(TieBreak::StatusQuo) => match spec.status_quo.as_ref() {
            Some(status_quo) if leaders.contains(status_quo) => base(
                DecisionState::Resolved,
                Some(status_quo.clone()),
                "tie, settled by the predeclared status quo",
            ),
            Some(_) => base(
                DecisionState::Unresolved,
                None,
                "tie not involving the status quo; the predeclared rule does not settle it",
            ),
            None => base(
                DecisionState::Unresolved,
                None,
                "tie with no status quo declared",
            ),
        },
        Some(TieBreak::SecondRound) => base(
            DecisionState::Unresolved,
            None,
            "tie; the predeclared rule opens a second round",
        ),
        Some(TieBreak::EscalateNamedBody) => base(
            DecisionState::Unresolved,
            None,
            "tie; the predeclared rule escalates it",
        ),
        None => base(
            DecisionState::Unresolved,
            None,
            "tie with no tie-break declared",
        ),
    }
}

/// Refuse a ballot whose rules cannot produce an answer later.
///
/// Everything checked here is written at open time and never rewritten, so a
/// rule that is missing now is missing when the votes are in.
pub fn validate_ballot(spec: &DecisionSpec) -> Result<(), String> {
    if spec.mode != DecisionMode::Ballot {
        return Err("not a ballot".into());
    }
    if spec.options.len() < 2 {
        return Err("a ballot needs at least two options".into());
    }
    if spec.eligible_units.is_empty() {
        return Err("a ballot needs a roster fixed at open time".into());
    }
    if spec.quorum == 0 || spec.quorum as usize > spec.eligible_units.len() {
        return Err(format!(
            "quorum {} is outside 1..={}",
            spec.quorum,
            spec.eligible_units.len()
        ));
    }
    if spec.deadline.is_none() {
        return Err("a ballot needs a deadline".into());
    }
    match spec.status_quo.as_deref() {
        Some(status_quo) if spec.options.iter().any(|o| o == status_quo) => {}
        Some(_) => return Err("the status quo must be one of the options".into()),
        None => return Err("a ballot needs a status quo option".into()),
    }
    if spec.tie_break.is_none() {
        return Err("a ballot needs a tie-break declared before the first vote".into());
    }
    Ok(())
}

/// Refuse a principle decision that does not name what settled it.
pub fn validate_principle(spec: &DecisionSpec) -> Result<(), String> {
    if spec.mode != DecisionMode::Principle {
        return Err("not a principle decision".into());
    }
    match spec.principle_slug.as_deref() {
        Some(slug) if !slug.trim().is_empty() => {}
        _ => return Err("a principle decision must name the principle".into()),
    }
    match spec.principle_rationale.as_deref() {
        Some(text) if !text.trim().is_empty() => {}
        _ => return Err("a principle decision must say why that principle applies".into()),
    }
    if spec.options.len() < 2 {
        return Err("a decision needs at least two options to have chosen one".into());
    }
    Ok(())
}

/// Whether `task_type` names `expected`.
///
/// Custom task types serialise as `{"custom": "<name>"}`, so a row is matched
/// through that object rather than through a bare string.
pub fn is_typed(task: &Task, expected: &str) -> bool {
    matches!(&task.task_type, TaskType::Custom(name) if name == expected)
}

fn governance_task(
    id: &str,
    task_type: &str,
    input_key: &str,
    payload: serde_json::Value,
    status: TaskStatus,
    owner: Option<String>,
    now: DateTime<Utc>,
) -> Task {
    Task {
        id: id.to_string(),
        task_type: TaskType::Custom(task_type.to_string()),
        status,
        input: serde_json::json!({ input_key: payload }),
        result: None,
        error: None,
        error_cause: None,
        blocked_by: Vec::new(),
        blocks: Vec::new(),
        priority: 0,
        created_at: now,
        updated_at: now,
        agent_id: None,
        workspace_id: Some(GOVERNANCE_WORKSPACE.to_string()),
        retry_count: 0,
        max_retries: 0,
        retry_not_before: None,
        started_at: None,
        timeout_seconds: 0,
        lease_owner: None,
        lease_expires_at: None,
        action_planner_meta: None,
        goal_id: None,
        parent_task_id: None,
        sink_task_id: None,
        is_executable: false,
        face_id: None,
        owner_unit: owner,
        bound_by_handover_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_760_000_000 + secs, 0).unwrap()
    }

    fn ballot() -> DecisionSpec {
        DecisionSpec {
            kind: "who-approves-writes".into(),
            subject: "who approves writes".into(),
            options: vec!["operator".into(), "8e".into(), "6e".into()],
            status_quo: Some("operator".into()),
            mode: DecisionMode::Ballot,
            state: DecisionState::Open,
            eligible_units: vec!["a".into(), "b".into(), "c".into()],
            quorum: 2,
            tie_break: Some(TieBreak::StatusQuo),
            deadline: Some(at(60)),
            principle_slug: None,
            principle_rationale: None,
            result_option: None,
            result_reason: None,
            supersedes: None,
        }
    }

    fn vote(unit: &str, option: &str) -> Vote {
        Vote {
            decision_id: "d".into(),
            unit: unit.into(),
            option: option.into(),
            withdrawn: false,
        }
    }

    #[test]
    fn a_ballot_cannot_open_without_the_rules_that_settle_it() {
        let mut spec = ballot();
        spec.quorum = 0;
        assert!(validate_ballot(&spec).is_err());
        let mut spec = ballot();
        spec.deadline = None;
        assert!(validate_ballot(&spec).is_err());
        let mut spec = ballot();
        spec.tie_break = None;
        assert!(validate_ballot(&spec).is_err());
        let mut spec = ballot();
        spec.status_quo = Some("nobody".into());
        assert!(validate_ballot(&spec).is_err());
        assert!(validate_ballot(&ballot()).is_ok());
    }

    #[test]
    fn a_ballot_does_not_close_before_its_deadline() {
        let result = close_ballot(&ballot(), &[vote("a", "8e"), vote("b", "8e")], at(0));
        assert_eq!(result.state, DecisionState::Open);
        assert_eq!(result.option, None);
    }

    #[test]
    fn only_eligible_units_votes_are_counted() {
        let votes = vec![vote("a", "8e"), vote("outsider", "8e")];
        let result = close_ballot(&ballot(), &votes, at(120));
        assert_eq!(result.tally.voters, vec!["a".to_string()]);
        // One of two required voters: quorum is not met, so the outsider's
        // vote could not have changed the outcome even if it were counted.
        assert_eq!(result.state, DecisionState::Unresolved);
        assert!(
            result.reason.contains("quorum not met"),
            "{}",
            result.reason
        );
    }

    #[test]
    fn a_withdrawn_vote_does_not_count() {
        let mut withdrawn = vote("a", "8e");
        withdrawn.withdrawn = true;
        let result = close_ballot(&ballot(), &[withdrawn], at(120));
        assert_eq!(result.state, DecisionState::Unresolved);
    }

    #[test]
    fn a_clear_majority_resolves_the_ballot() {
        let votes = vec![vote("a", "8e"), vote("b", "8e"), vote("c", "operator")];
        let result = close_ballot(&ballot(), &votes, at(120));
        assert_eq!(result.state, DecisionState::Resolved);
        assert_eq!(result.option.as_deref(), Some("8e"));
        assert!(result.reason.starts_with("majority"), "{}", result.reason);
    }

    #[test]
    fn a_tie_falls_to_the_status_quo_that_was_declared_first() {
        let votes = vec![vote("a", "8e"), vote("b", "operator")];
        let result = close_ballot(&ballot(), &votes, at(120));
        assert_eq!(result.state, DecisionState::Resolved);
        assert_eq!(result.option.as_deref(), Some("operator"));
        assert!(result.reason.contains("status quo"), "{}", result.reason);
    }

    #[test]
    fn a_tie_away_from_the_status_quo_is_unresolved_not_a_guess() {
        let votes = vec![vote("a", "8e"), vote("b", "6e")];
        let result = close_ballot(&ballot(), &votes, at(120));
        assert_eq!(result.state, DecisionState::Unresolved);
        assert_eq!(result.option, None);
    }

    #[test]
    fn casting_twice_is_the_same_row_not_a_second_ballot() {
        let first = vote_task("d", "a", "8e", at(0));
        let second = vote_task("d", "a", "operator", at(10));
        assert_eq!(first.id, second.id);
        assert_eq!(first.owner_unit, second.owner_unit);
        assert_ne!(first.input, second.input);
    }

    #[test]
    fn rows_are_read_back_by_their_own_type() {
        let spec = ballot();
        let task = decision_task("d", spec.clone(), TaskStatus::Pending, at(0));
        let read = Decision::from_task(&task).unwrap();
        assert_eq!(read.spec, spec);
        assert!(Handover::from_task(&task).is_none());
        assert!(Vote::from_task(&task).is_none());
    }

    #[test]
    fn a_handover_is_signed_only_by_its_three_units() {
        let spec = HandoverSpec {
            face_id: "任务管理".into(),
            from_unit: Some("worker".into()),
            to_unit: "6e".into(),
            witness_unit: "8b2".into(),
            criterion_reading: None,
            reading_at: None,
            open_actions: Vec::new(),
            open_task_ids: Vec::new(),
            permissions_context: None,
            expires_when: None,
            signed_by: vec![Signature {
                unit: "8b2".into(),
                at: at(0),
            }],
        };
        assert!(!spec.is_fully_signed());
        let mut signed = spec.clone();
        signed.signed_by.push(Signature {
            unit: "worker".into(),
            at: at(1),
        });
        signed.signed_by.push(Signature {
            unit: "6e".into(),
            at: at(2),
        });
        assert!(signed.is_fully_signed());
        assert_eq!(signed.required_signers(), vec!["6e", "8b2", "worker"]);
    }

    #[test]
    fn each_state_has_its_own_row_status() {
        // Three terminal handover states must not collapse into one status, or
        // a reader selecting by status could not tell an applied move from a
        // withdrawn one.
        let statuses = [
            HandoverState::Applied.status(),
            HandoverState::Withdrawn.status(),
            HandoverState::Expired.status(),
        ];
        assert_ne!(statuses[0], statuses[1]);
        assert_ne!(statuses[1], statuses[2]);
        assert_ne!(statuses[0], statuses[2]);
        for state in [
            DecisionState::Resolved,
            DecisionState::Unresolved,
            DecisionState::Withdrawn,
        ] {
            assert_eq!(
                state.status(),
                DecisionState::parse(state.as_str()).unwrap().status()
            );
        }
    }
}
