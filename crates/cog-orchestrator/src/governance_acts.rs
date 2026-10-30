//! principle: feedback_an_error_branch_must_not_be_counted_as_a_named_outcome
//! principle: feedback_a_deadline_should_run_on_its_own_clock
//! Handovers, decisions and votes as rows of the task graph -- the part that
//! acts.
//!
//! The row shapes live in the core contract, because a face's reading names
//! the handovers that are waiting to be applied and every crate that reads a
//! face names the same types. What lives here is everything that does
//! something with those rows: building them, counting the votes, deciding
//! whether a ballot closed, and the pass that writes the verdicts the rules
//! already reached. The core keeps the vocabulary; the graph's runtime keeps
//! the grammar, so adding a rule does not rebuild every crate that only
//! speaks the vocabulary.
//!
//! Nothing in this module is a second ledger. Every function here reads and
//! writes the same `cog_dag_tasks` rows the rest of the platform uses.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::Serialize;

use cog_core::{
    Decision, DecisionMode, DecisionSpec, DecisionState, Face, Handover, HandoverSpec,
    HandoverState, Signature, Task, TaskStatus, TaskType, TieBreak, Vote, VoteSpec,
    DECISION_INPUT_KEY, DECISION_TASK_TYPE, FACE_INPUT_KEY, GOVERNANCE_WORKSPACE,
    HANDOVER_INPUT_KEY, HANDOVER_TASK_TYPE, VOTE_INPUT_KEY, VOTE_TASK_TYPE,
};

/// Fold a name into a segment that can travel as a task id.
///
/// Folding every unsafe character to a dash would not do: face ids are written
/// in whatever language names them, and two names that differ only in
/// characters outside `[a-z0-9_-]` would fold to one id -- one face's work
/// would then be refused as already submitted because another face had
/// submitted one. Each unsafe character is escaped to its code point instead,
/// under a marker no literal character can produce, so the mapping is
/// injective.
pub fn task_id_segment(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c.to_string()
            } else {
                format!("~{code:x}", code = c as u32)
            }
        })
        .collect()
}

/// The id of the standing work row a face's owner works from.
///
/// One row per face, not one per day: a face's duty is continuous, and a row
/// that expired at midnight would make every face read idle every morning
/// until somebody re-created its work.
pub fn face_work_id(face_id: &str) -> String {
    format!("face-work-{}", task_id_segment(face_id))
}

/// The task type that marks a row as a face's standing duty.
///
/// Its own type rather than a plain `dag_node`, because two readers ask two
/// different questions of the same row. The drift reading asks whether the
/// owner has open work on the face and counts only executable rows, so the duty
/// has to stay executable. The dispatcher asks what it may start as a run, and
/// must never start this: the duty has no body, so a run of it spends its whole
/// budget on nothing and is failed on the degenerate-loop rule, which takes the
/// face's only evidence of staffing with it and leaves the face reading idle.
pub const FACE_DUTY_TASK_TYPE: &str = "face-duty";

/// Put a standing row into the one shape both of its readers agree on.
///
/// Two readers ask two different questions of this row and a row has to answer
/// both: the drift reading counts only executable rows as the face's open work,
/// and the dispatcher must never start a row that has no body to run. Either
/// half alone loses one of the two -- non-executable is invisible to the
/// reading, and an untyped executable row is eaten by the dispatcher and failed,
/// which takes the face's only evidence of staffing with it. A row written
/// before this shape settled can be in either broken form, so every writer that
/// already has the row in hand puts it right here.
pub fn ensure_duty_shape(task: &mut Task) {
    task.is_executable = true;
    if !matches!(&task.task_type, TaskType::Custom(kind) if kind == FACE_DUTY_TASK_TYPE) {
        task.task_type = TaskType::Custom(FACE_DUTY_TASK_TYPE.to_string());
    }
    attach_to_its_face(task);
}

/// The one condition that makes a face's standing duty done.
///
/// A duty row that says nothing about what "done" means can only ever be an
/// unfinished leaf: the project views draw it, its earned-value row never
/// closes, and the decomposition gate has nothing to apply to it. The wording
/// is the completion condition the row's owner is already handed in the goal
/// text, so the two cannot drift apart.
pub const FACE_DUTY_ACCEPTANCE: &str = "本面的判据读数有新的读取时刻与值，且开着的活都归属本面";

/// Hang the duty on the face's own node, so a face is a project *with a tree*
/// rather than two flat lists that only agree by coincidence.
///
/// The face node's id is also its project id: a project is a group of rows
/// under one `goal_id`, and the row in that group with no parent is its root.
/// The face node is that root. Writing both directions from one place keeps
/// the group's root unique — a duty whose `goal_id` names a root it does not
/// declare as its parent is a group with no root, which replan refuses to
/// plan into and the earned-value view cannot date.
fn attach_to_its_face(task: &mut Task) {
    let Some(face_id) = task.face_id.clone() else {
        return;
    };
    task.goal_id.get_or_insert_with(|| face_id.clone());
    task.parent_task_id.get_or_insert(face_id);
    if task.acceptance.is_empty() {
        task.acceptance.push(FACE_DUTY_ACCEPTANCE.to_string());
    }
}

/// The standing duty row a face's owner works from, as one shape written once.
///
/// The row carries the face's id and the name of the unit answerable for it,
/// and that pair is what the drift reading measures when it asks whether
/// anybody is on the face. It is executable so that reading can see it, and
/// typed as a face duty so the dispatcher leaves it alone; see
/// [`FACE_DUTY_TASK_TYPE`].
///
/// The owner is written three times for the same reason `rebind` writes it
/// three times: the reading takes the row's own binding, the dispatcher reads
/// the struct field, and the executor hands `input` to whoever claims it. A
/// copy that disagrees is a row that reads as somebody else's work.
pub fn standing_row(face: &Face, workspace: &str) -> Task {
    let reading = face
        .spec
        .first_reading
        .as_deref()
        .unwrap_or("本面的判据读数");
    let gap = face.spec.gap_task_id.as_deref().unwrap_or("-");
    let goal = format!(
        "以生产商用为唯一目标，保持「{}」这一面在目标态。\n\
         本轮判据读数：{}（必须有新的读取时刻与值，只刷新 updated_at 不算）。\n\
         缺口任务：{}。\n\
         完成条件：读数可读、任务归属与面一致；不属于本面的工作转给该面的 owner。",
        face.face_id, reading, gap
    );
    let mut task = Task::new(
        face_work_id(&face.face_id),
        TaskType::Custom(FACE_DUTY_TASK_TYPE.to_string()),
        serde_json::json!({
            "goal": goal,
            "face": face.face_id,
            "owner_unit": face.owner_unit,
        }),
    );
    task.face_id = Some(face.face_id.clone());
    task.owner_unit = face.owner_unit.clone();
    task.workspace_id = Some(workspace.to_string());
    ensure_duty_shape(&mut task);
    task
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

/// What one signature did to a handover's terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignedBy {
    /// The signature was added, and the row is now in this state.
    Added(HandoverState),
    /// The unit had already signed; nothing was written.
    Already,
}

/// Add one unit's signature to a handover's terms.
///
/// The rule lives here for the same reason the row shape does: the command a
/// person types and the pass that staffs a face both have to sign, and a
/// handover whose digest is fixed by one caller and not the other is a
/// signature over terms nobody read. `cast_by` is `None` when the unit signs
/// for itself and names the actor otherwise.
pub fn sign_spec(
    spec: &mut HandoverSpec,
    unit: &str,
    cast_by: Option<String>,
    at: DateTime<Utc>,
) -> Result<SignedBy, String> {
    if spec.signed_by.iter().any(|signed| signed.unit == unit) {
        return Ok(SignedBy::Already);
    }
    // The first signature fixes the terms. A later signature over a payload
    // that has since changed would attest to something nobody read, so refuse
    // instead of quietly re-hashing.
    if !spec.signature_covers_payload() {
        return Err(
            "changed after it was signed; the terms must be re-read before another signature"
                .into(),
        );
    }
    if spec.payload_sha256.is_none() {
        spec.payload_sha256 = Some(spec.payload_digest());
    }
    spec.signed_by.push(Signature {
        unit: unit.to_string(),
        at,
        cast_by,
    });
    Ok(SignedBy::Added(if spec.is_fully_signed() {
        HandoverState::Signed
    } else {
        HandoverState::Draft
    }))
}

/// The face row as it stands after a handover moves it, and the revision the
/// move was read against.
///
/// Shared by the command and the staffing pass: a field that has to be carried
/// across a move -- the vacancy a filled seat ends, the revision an import uses
/// to tell a moved row from an untouched one -- must be carried the same way by
/// both, or one of them writes a face that says two different things about
/// itself.
pub fn moved_face_row(
    face_task: &Task,
    spec: &HandoverSpec,
    handover_id: &str,
    now: DateTime<Utc>,
) -> Result<(Task, i64), String> {
    let face =
        Face::from_task(face_task).ok_or_else(|| format!("{} is not a face", spec.face_id))?;
    let expected_revision = face.revision();
    let mut next = face.spec.clone();
    next.revision += 1;
    // Filling a declared vacancy ends it. The three cells name who was going
    // to fill the seat, by when, and with which task; once somebody is in the
    // seat they describe a search that is over. Left behind, `gap_task` keeps
    // pointing at a closing task that may never have existed while the row
    // looks staffed and nobody re-checks it.
    if spec.from_unit.is_none() {
        next.vacancy_until = None;
        next.vacancy_assigner = None;
        next.gap_task_id = None;
    }
    let mut row = face_task.clone();
    row.input
        .as_object_mut()
        .ok_or("the face payload is not an object")?
        .insert(
            FACE_INPUT_KEY.to_string(),
            serde_json::to_value(&next).map_err(|e| e.to_string())?,
        );
    row.owner_unit = Some(spec.to_unit.clone());
    row.bound_by_handover_id = Some(handover_id.to_string());
    row.updated_at = now;
    Ok((row, expected_revision))
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
        Some(TieBreak::NotPredeclared) => base(
            DecisionState::Unresolved,
            None,
            "tie; no tie rule was declared before this vote was taken",
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
    // A live ballot whose tie rule is "not predeclared" is exactly the state
    // open() exists to refuse. Only a record reconstructed after the fact may
    // carry it, because only that record is allowed to say the procedure was
    // missing rather than invent one.
    if spec.tie_break == Some(TieBreak::NotPredeclared)
        && spec.provenance.as_deref() != Some("backfilled")
    {
        return Err(
            "not_predeclared is only allowed on a backfilled record; a live ballot must \
             declare its tie rule before the first vote"
                .into(),
        );
    }
    Ok(())
}

/// A ballot reconstructed from the replies it was decided by.
pub struct BackfilledBallot {
    pub spec: DecisionSpec,
    /// One `(unit, option)` per reply, sorted by unit so the write order is
    /// the same on every run.
    pub votes: Vec<(String, String)>,
}

/// Turn reply files into the decision row and votes they describe.
///
/// This function exists so a historical vote can be put into the ledger
/// honestly. It never invents a rule: the row carries
/// `tie_break = not_predeclared` and `predeclared = false`, because the
/// quorum below is the number of people who happened to reply, not a quorum
/// anybody set before the vote. A tally computed from the bytes is real; the
/// procedure is not implied by it.
pub fn backfill_ballot(
    kind: &str,
    subject: &str,
    options: &[String],
    status_quo: Option<String>,
    deadline: DateTime<Utc>,
    replies: &[(String, String)],
) -> Result<BackfilledBallot, String> {
    if options.len() < 2 {
        return Err("a ballot needs at least two options".into());
    }
    if replies.is_empty() {
        return Err("there are no replies to backfill from".into());
    }
    let status_quo =
        status_quo.ok_or_else(|| "a backfilled ballot must name its status quo".to_string())?;
    if !options.iter().any(|o| o == &status_quo) {
        return Err("the status quo must be one of the options".into());
    }
    let mut units = BTreeSet::new();
    let mut counts: BTreeMap<String, usize> = options.iter().map(|o| (o.clone(), 0)).collect();
    for (unit, option) in replies {
        if unit.trim().is_empty() {
            return Err("a reply file names no unit".into());
        }
        if !units.insert(unit.clone()) {
            return Err(format!("{unit} replied more than once"));
        }
        if !options.iter().any(|o| o == option) {
            return Err(format!(
                "{unit} chose {option}, which is not one of the options"
            ));
        }
        *counts.get_mut(option).expect("option checked above") += 1;
    }

    let top = counts.values().copied().max().unwrap_or(0);
    let leaders: Vec<&String> = counts
        .iter()
        .filter(|(_, count)| **count == top)
        .map(|(option, _)| option)
        .collect();
    let (state, result_option, reason) = if top == 0 {
        (
            DecisionState::Unresolved,
            None,
            "no reply named one of the options".to_string(),
        )
    } else if leaders.len() > 1 {
        (
            DecisionState::Unresolved,
            None,
            format!(
                "tie between {} with no tie rule declared before the vote",
                leaders
                    .iter()
                    .map(|o| o.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
    } else {
        (
            DecisionState::Resolved,
            Some(leaders[0].clone()),
            format!(
                "plurality: {top} of {} replies; no quorum or tie rule was declared first",
                replies.len()
            ),
        )
    };

    let mut votes = replies.to_vec();
    votes.sort();
    let spec = DecisionSpec {
        kind: kind.to_string(),
        subject: subject.to_string(),
        options: options.to_vec(),
        status_quo: Some(status_quo),
        mode: DecisionMode::Ballot,
        state,
        // The roster and quorum are reconstructed from who replied. They are
        // recorded so the tally can be checked later; `predeclared = false`
        // is what stops them being read as having governed the vote.
        eligible_units: units.into_iter().collect(),
        quorum: replies.len() as u32,
        tie_break: Some(TieBreak::NotPredeclared),
        deadline: Some(deadline),
        principle_slug: None,
        principle_rationale: None,
        result_option,
        result_reason: Some(reason),
        supersedes: None,
        provenance: Some("backfilled".into()),
        predeclared: false,
    };
    validate_ballot(&spec)?;
    Ok(BackfilledBallot { spec, votes })
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

/// What one settle pass changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SettleReport {
    /// Ballots whose deadline had passed and whose rules produced a verdict.
    pub ballots_closed: usize,
    /// Signed handovers whose own expiry had passed unused.
    pub handovers_expired: usize,
    /// Rows the pass tried to move but that another writer had already moved.
    /// Left untouched: the verdict was computed from votes and a roster read
    /// before that move, so writing it would state an outcome for a state that
    /// no longer exists.
    pub lost_races: usize,
    /// Ballots the pass did not close because their history does not verify.
    ///
    /// A decision whose chain is broken is a decision somebody edited; closing
    /// it would append an event vouching for the edited history, so the pass
    /// leaves it alone and says so. The ballot stays open rather than being
    /// resolved on the strength of a record that cannot be read.
    pub chains_broken: usize,
}

/// Apply the rules that close things by themselves.
///
/// Nothing here is a special case for the loop: closing a ballot by hand and
/// closing it on schedule take the same function, so a decision settled either
/// way is settled the same way. Only rows still in the state this pass read
/// are written; a concurrent close or withdrawal wins and is counted, not
/// overwritten.
pub async fn settle(
    store: &dyn cog_core::GovernanceStore,
    now: DateTime<Utc>,
) -> cog_core::SFResult<SettleReport> {
    let mut report = SettleReport::default();
    let votes: Vec<Vote> = store
        .governance_nodes(VOTE_TASK_TYPE)
        .await?
        .iter()
        .filter_map(Vote::from_task)
        .collect();
    for task in store.governance_nodes(DECISION_TASK_TYPE).await? {
        let Some(mut decision) = Decision::from_task(&task) else {
            continue;
        };
        if decision.spec.mode != DecisionMode::Ballot || decision.spec.state != DecisionState::Open
        {
            continue;
        }
        let cast: Vec<Vote> = votes
            .iter()
            .filter(|vote| vote.decision_id == decision.id)
            .cloned()
            .collect();
        let result = close_ballot(&decision.spec, &cast, now);
        let status = match result.state {
            DecisionState::Open => continue,
            DecisionState::Resolved => TaskStatus::Completed,
            DecisionState::Unresolved => TaskStatus::Failed,
            // A ballot never closes into these; a row carrying one was moved
            // by somebody else and is not this pass's to rewrite.
            DecisionState::Superseded | DecisionState::Withdrawn => continue,
        };
        decision.spec.state = result.state;
        decision.spec.result_option = result.option;
        decision.spec.result_reason = Some(result.reason);
        let mut row = task.clone();
        row.status = status;
        row.updated_at = now;
        rewrite_payload(&mut row, DECISION_INPUT_KEY, &decision.spec);

        // The row and its last event are one fact, so the store writes them in
        // one transaction. A history that does not verify stops the close, and
        // a close that lost its race writes neither the row nor a step
        // claiming it.
        let detail = format!(
            "{}: {}",
            decision.spec.state.as_str(),
            decision
                .spec
                .result_reason
                .clone()
                .unwrap_or_else(|| "no reason recorded".into())
        );
        match cog_core::close_decision_with_event(
            store,
            &decision.id,
            &row,
            &[TaskStatus::Pending, TaskStatus::Running],
            Some(&detail),
            now,
        )
        .await
        {
            Ok(cog_core::CloseOutcome::Closed) => report.ballots_closed += 1,
            Ok(cog_core::CloseOutcome::LostRace) => report.lost_races += 1,
            Ok(cog_core::CloseOutcome::ChainBroken { .. }) => report.chains_broken += 1,
            // A storage failure is none of the three outcomes; counting it as
            // a broken history would report an edited record every time the
            // database hiccuped, and hide the hiccup.
            Err(err) => return Err(err),
        }
    }
    for task in store.governance_nodes(HANDOVER_TASK_TYPE).await? {
        let Some(handover) = Handover::from_task(&task) else {
            continue;
        };
        if handover.status != TaskStatus::Scheduled {
            continue;
        }
        let Some(expires_when) = handover.spec.expires_when else {
            continue;
        };
        if now < expires_when {
            continue;
        }
        let mut row = task.clone();
        row.status = TaskStatus::Failed;
        row.updated_at = now;
        if store
            .save_governance_node_if_status(&row, &[TaskStatus::Scheduled])
            .await?
        {
            report.handovers_expired += 1;
        } else {
            report.lost_races += 1;
        }
    }
    Ok(report)
}

/// The loop's name in the health registry.
pub const DECISION_CLOSE_LOOP: &str = "governance_decision_close";

/// The role one process at a time holds while writing verdicts.
///
/// Reading needs no arbitration, but a close is a write, and the writes are
/// guarded by the state they read; one holder keeps the two replicas from
/// computing the same verdict twice and counting each other as a lost race.
pub const DECISION_CLOSE_ROLE: &str = "governance_decision_close";

/// How often the closed-by-rule pass runs.
///
/// Thirty seconds: a ballot's deadline is a statement about when the question
/// is answered, and a reader arriving a minute later should not have to know
/// that a pass was due. A pass with nothing due writes nothing.
// cap: bound — 选票的截止时刻是对「问题什么时候有答案」的陈述；晚一分钟来的读者不该知道这一拍本来什么时候到
pub const DECISION_CLOSE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Start the resident pass that writes the verdicts the rules already reached.
///
/// Not started is not a fault: a deployment without a shared database holds no
/// ballots and no handovers, and the CLI reaches the same function directly.
pub fn spawn_decision_close_loop(
    store: std::sync::Arc<dyn cog_core::GovernanceStore>,
    role: Option<std::sync::Arc<dyn cog_core::OwnerLeaseBroker>>,
    shutdown: cog_core::ShutdownSignal,
) {
    tracing::info!(
        interval_secs = DECISION_CLOSE_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        "Governance close loop enabled"
    );
    drop(cog_core::loop_health::spawn(
        DECISION_CLOSE_LOOP,
        cog_core::loop_health::Cadence::Periodic(DECISION_CLOSE_INTERVAL),
        shutdown.clone(),
        move |beat| {
            let store = store.clone();
            let role = role.clone();
            let shutdown = shutdown.clone();
            async move {
                let hold =
                    cog_core::RoleHold::start(role, DECISION_CLOSE_ROLE, beat.clone(), &shutdown);
                let mut ticker = tokio::time::interval(DECISION_CLOSE_INTERVAL);
                // The first tick of an interval fires immediately; skipping it
                // keeps startup from doing a pass before the role is settled.
                ticker.tick().await;
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    if !hold.may_act().await {
                        continue;
                    }
                    match settle(&*store, Utc::now()).await {
                        Ok(report)
                            if report.ballots_closed > 0
                                || report.handovers_expired > 0
                                || report.lost_races > 0 =>
                        {
                            tracing::info!(
                                ballots_closed = report.ballots_closed,
                                handovers_expired = report.handovers_expired,
                                lost_races = report.lost_races,
                                "governance close pass wrote the verdicts the rules had already \
                                 reached"
                            );
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!(
                            error = %e,
                            "governance close pass could not read or write the graph; this tick \
                             closed nothing, and the ballots stay open rather than being \
                             reported closed"
                        ),
                    }
                }
            }
        },
    ));
}

/// Replace one payload key in a row, keeping whatever else the row carries.
fn rewrite_payload<T: Serialize>(row: &mut Task, key: &str, payload: &T) {
    let object = row
        .input
        .as_object_mut()
        .expect("a governance row carries an object payload");
    object.insert(
        key.to_string(),
        serde_json::to_value(payload).expect("a governance payload serialises"),
    );
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
        // 治理动作不来自分解，没有人给过验收标准。空着是实话。
        acceptance: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use cog_core::{is_typed, GovernanceStore, HandoverState, Signature};

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
            provenance: None,
            predeclared: false,
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
            from_absent: None,
            to_unit: "6e".into(),
            witness_unit: "8b2".into(),
            source: None,
            criterion_reading: None,
            reading_at: None,
            open_actions: Vec::new(),
            open_task_ids: Vec::new(),
            permissions_context: None,
            artifact_path: None,
            artifact_expires_at: None,
            expires_when: None,
            withdraw_condition: None,
            payload_sha256: None,
            signed_by: vec![Signature {
                unit: "8b2".into(),
                at: at(0),
                cast_by: None,
            }],
        };
        assert!(!spec.is_fully_signed());
        let mut signed = spec.clone();
        signed.signed_by.push(Signature {
            unit: "worker".into(),
            at: at(1),
            cast_by: None,
        });
        signed.signed_by.push(Signature {
            unit: "6e".into(),
            at: at(2),
            cast_by: None,
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

    fn replies(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(unit, option)| (unit.to_string(), option.to_string()))
            .collect()
    }

    #[test]
    fn a_backfilled_ballot_counts_the_replies_it_was_given() {
        let options = vec!["operator".into(), "8e".into(), "6e".into()];
        let filled = backfill_ballot(
            "who-approves-writes",
            "who approves writes",
            &options,
            Some("operator".into()),
            at(60),
            &replies(&[
                ("operator", "operator"),
                ("8b2", "operator"),
                ("8e", "operator"),
                ("6e", "6e"),
                ("43", "8e"),
            ]),
        )
        .unwrap();
        assert_eq!(filled.spec.state, DecisionState::Resolved);
        assert_eq!(filled.spec.result_option.as_deref(), Some("operator"));
        // The count is real; the rules that would have settled a tie were not
        // written before the votes were cast, and the row says so.
        assert_eq!(filled.spec.provenance.as_deref(), Some("backfilled"));
        assert!(!filled.spec.predeclared);
        assert_eq!(filled.spec.tie_break, Some(TieBreak::NotPredeclared));
        assert_eq!(filled.spec.quorum, 5);
        assert_eq!(
            filled.spec.eligible_units,
            vec!["43", "6e", "8b2", "8e", "operator"]
        );
        assert_eq!(filled.votes.len(), 5);
    }

    #[test]
    fn a_backfilled_tie_is_unresolved_rather_than_settled_by_a_rule_added_later() {
        let options = vec!["operator".into(), "8e".into(), "6e".into()];
        let filled = backfill_ballot(
            "k",
            "s",
            &options,
            Some("operator".into()),
            at(60),
            &replies(&[("a", "operator"), ("b", "8e"), ("c", "6e")]),
        )
        .unwrap();
        assert_eq!(filled.spec.state, DecisionState::Unresolved);
        assert_eq!(filled.spec.result_option, None);
        assert!(
            filled
                .spec
                .result_reason
                .as_deref()
                .unwrap_or("")
                .contains("tie"),
            "{:?}",
            filled.spec.result_reason
        );
    }

    #[test]
    fn a_backfill_refuses_replies_that_do_not_describe_a_ballot() {
        let options = vec!["operator".into(), "8e".into()];
        let call = |status_quo: Option<String>, pairs: &[(&str, &str)]| {
            backfill_ballot("k", "s", &options, status_quo, at(60), &replies(pairs))
        };
        // A choice outside the options is not a vote on this question.
        assert!(call(Some("operator".into()), &[("a", "nobody")]).is_err());
        // One unit, two votes would double its weight.
        assert!(call(Some("operator".into()), &[("a", "8e"), ("a", "operator")]).is_err());
        // No replies is not a zero-vote ballot, it is a missing input.
        assert!(call(Some("operator".into()), &[]).is_err());
        // The status quo must be one of the options for a tie to resolve to it.
        assert!(call(Some("nobody".into()), &[("a", "8e")]).is_err());
        assert!(call(None, &[("a", "8e")]).is_err());
    }

    #[test]
    fn only_a_backfilled_row_may_declare_that_no_tie_rule_was_predeclared() {
        let mut spec = ballot();
        spec.tie_break = Some(TieBreak::NotPredeclared);
        assert!(validate_ballot(&spec).is_err());
        spec.provenance = Some("backfilled".into());
        assert!(validate_ballot(&spec).is_ok());
    }

    /// The rows a settle pass can see, with the same conditional write the
    /// database store performs. The pass is about the rules, not about SQL, so
    /// the rules are tested here where they run on every build.
    struct MemoryStore {
        rows: std::sync::Mutex<Vec<Task>>,
    }

    impl MemoryStore {
        fn with(rows: Vec<Task>) -> Self {
            Self {
                rows: std::sync::Mutex::new(rows),
            }
        }

        fn row(&self, id: &str) -> Task {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .find(|row| row.id == id)
                .cloned()
                .expect("the row was written")
        }
    }

    #[async_trait::async_trait]
    impl cog_core::GovernanceStore for MemoryStore {
        async fn face_nodes(&self) -> cog_core::SFResult<Vec<Task>> {
            Ok(Vec::new())
        }

        async fn open_face_bound_tasks(&self) -> cog_core::SFResult<Vec<Task>> {
            Ok(Vec::new())
        }

        async fn save_face_node(&self, _task: &Task) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn save_face_node_if_revision(
            &self,
            _task: &Task,
            _expected: i64,
        ) -> cog_core::SFResult<bool> {
            Ok(false)
        }

        async fn governance_nodes(&self, task_type: &str) -> cog_core::SFResult<Vec<Task>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|row| is_typed(row, task_type))
                .cloned()
                .collect())
        }

        async fn save_governance_node(&self, task: &Task) -> cog_core::SFResult<()> {
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|row| row.id == task.id) {
                Some(slot) => *slot = task.clone(),
                None => rows.push(task.clone()),
            }
            Ok(())
        }

        async fn save_governance_node_if_status(
            &self,
            task: &Task,
            expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|row| row.id == task.id) {
                Some(slot) if expected.contains(&slot.status) => {
                    *slot = task.clone();
                    Ok(true)
                }
                _ => Ok(false),
            }
        }

        async fn save_governance_node_with_event(
            &self,
            event: &Task,
            node: &Task,
            expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            let mut rows = self.rows.lock().unwrap();
            let Some(slot) = rows.iter_mut().find(|row| row.id == node.id) else {
                return Ok(false);
            };
            if !expected.contains(&slot.status) {
                return Ok(false);
            }
            *slot = node.clone();
            match rows.iter_mut().find(|row| row.id == event.id) {
                Some(entry) => *entry = event.clone(),
                None => rows.push(event.clone()),
            }
            Ok(true)
        }

        async fn apply_handover(
            &self,
            _handover: &Task,
            _face: &Task,
            _expected_revision: i64,
        ) -> cog_core::SFResult<Option<usize>> {
            Ok(None)
        }

        async fn reassign_face_work(
            &self,
            _face_id: &str,
            _to_unit: &str,
        ) -> cog_core::SFResult<usize> {
            Ok(0)
        }

        async fn release_face_work(&self, _face_id: &str) -> cog_core::SFResult<usize> {
            Ok(0)
        }
        async fn ensure_face_duty(&self, row: &Task) -> cog_core::SFResult<bool> {
            self.rows.lock().unwrap().push(row.clone());
            Ok(true)
        }
    }

    /// Reads what it was given but fails every transactional write, standing
    /// in for a database that is briefly unreachable. The pass must not turn
    /// that failure into a verdict about the record.
    struct FailingWriteStore {
        inner: MemoryStore,
    }

    #[async_trait::async_trait]
    impl cog_core::GovernanceStore for FailingWriteStore {
        async fn face_nodes(&self) -> cog_core::SFResult<Vec<Task>> {
            cog_core::GovernanceStore::face_nodes(&self.inner).await
        }

        async fn open_face_bound_tasks(&self) -> cog_core::SFResult<Vec<Task>> {
            cog_core::GovernanceStore::open_face_bound_tasks(&self.inner).await
        }

        async fn save_face_node(&self, task: &Task) -> cog_core::SFResult<()> {
            cog_core::GovernanceStore::save_face_node(&self.inner, task).await
        }

        async fn save_face_node_if_revision(
            &self,
            task: &Task,
            expected: i64,
        ) -> cog_core::SFResult<bool> {
            cog_core::GovernanceStore::save_face_node_if_revision(&self.inner, task, expected).await
        }

        async fn governance_nodes(&self, task_type: &str) -> cog_core::SFResult<Vec<Task>> {
            cog_core::GovernanceStore::governance_nodes(&self.inner, task_type).await
        }

        async fn save_governance_node(&self, task: &Task) -> cog_core::SFResult<()> {
            cog_core::GovernanceStore::save_governance_node(&self.inner, task).await
        }

        async fn save_governance_node_if_status(
            &self,
            task: &Task,
            expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            cog_core::GovernanceStore::save_governance_node_if_status(&self.inner, task, expected)
                .await
        }

        async fn save_governance_node_with_event(
            &self,
            _event: &Task,
            _node: &Task,
            _expected: &[TaskStatus],
        ) -> cog_core::SFResult<bool> {
            Err(cog_core::SFError::Database("unreachable".into()))
        }

        async fn apply_handover(
            &self,
            handover: &Task,
            face: &Task,
            expected_revision: i64,
        ) -> cog_core::SFResult<Option<usize>> {
            cog_core::GovernanceStore::apply_handover(
                &self.inner,
                handover,
                face,
                expected_revision,
            )
            .await
        }

        async fn reassign_face_work(
            &self,
            _face_id: &str,
            _to_unit: &str,
        ) -> cog_core::SFResult<usize> {
            Err(cog_core::SFError::Database("unreachable".into()))
        }

        async fn release_face_work(&self, _face_id: &str) -> cog_core::SFResult<usize> {
            Err(cog_core::SFError::Database("unreachable".into()))
        }
        async fn ensure_face_duty(&self, _row: &Task) -> cog_core::SFResult<bool> {
            Err(cog_core::SFError::Database("unreachable".into()))
        }
    }

    fn open_ballot_row() -> Task {
        decision_task("d", ballot(), TaskStatus::Pending, at(0))
    }

    #[tokio::test]
    async fn a_storage_failure_ends_the_pass_instead_of_counting_a_broken_history() {
        let store = FailingWriteStore {
            inner: MemoryStore::with(vec![open_ballot_row()]),
        };
        let err = settle(&store, at(120))
            .await
            .expect_err("a write that failed must not read as a closed ballot");
        assert!(
            matches!(err, cog_core::SFError::Database(_)),
            "the failure has to stay the storage's, not become a verdict: {err}"
        );
        assert_eq!(
            store.inner.row("d").status,
            TaskStatus::Pending,
            "the row must not move when the write failed"
        );
        let events = cog_core::GovernanceStore::governance_nodes(
            &store.inner,
            cog_core::DECISION_EVENT_TASK_TYPE,
        )
        .await
        .unwrap();
        assert!(
            events.is_empty(),
            "a close that was never written must not leave a step explaining it"
        );
    }

    #[tokio::test]
    async fn a_ballot_closes_itself_when_the_deadline_passes() {
        let store = MemoryStore::with(vec![
            open_ballot_row(),
            vote_task("d", "a", "8e", at(0)),
            vote_task("d", "b", "8e", at(1)),
        ]);
        let report = settle(&store, at(120)).await.unwrap();
        assert_eq!(report.ballots_closed, 1);
        assert_eq!(report.lost_races, 0);
        let row = store.row("d");
        assert_eq!(row.status, TaskStatus::Completed);
        let spec: DecisionSpec =
            serde_json::from_value(row.input[DECISION_INPUT_KEY].clone()).unwrap();
        assert_eq!(spec.state, DecisionState::Resolved);
        assert_eq!(spec.result_option.as_deref(), Some("8e"));
        assert!(spec.result_reason.unwrap().starts_with("majority"));
    }

    #[tokio::test]
    async fn a_ballot_before_its_deadline_is_left_alone() {
        let store = MemoryStore::with(vec![
            open_ballot_row(),
            vote_task("d", "a", "8e", at(0)),
            vote_task("d", "b", "8e", at(1)),
        ]);
        let report = settle(&store, at(0)).await.unwrap();
        assert_eq!(report.ballots_closed, 0);
        assert_eq!(store.row("d").status, TaskStatus::Pending);
    }

    #[tokio::test]
    async fn a_ballot_still_short_of_quorum_closes_as_unresolved_not_as_resolved() {
        let store = MemoryStore::with(vec![open_ballot_row(), vote_task("d", "a", "8e", at(0))]);
        let report = settle(&store, at(120)).await.unwrap();
        assert_eq!(report.ballots_closed, 1);
        let row = store.row("d");
        assert_eq!(row.status, TaskStatus::Failed);
        let spec: DecisionSpec =
            serde_json::from_value(row.input[DECISION_INPUT_KEY].clone()).unwrap();
        assert_eq!(spec.state, DecisionState::Unresolved);
        assert_eq!(spec.result_option, None);
    }

    #[tokio::test]
    async fn a_ballot_somebody_else_already_moved_is_counted_not_overwritten() {
        // A withdrawn decision keeps its state in the payload, so a pass that
        // only looked at the payload would think it still open. The row status
        // is the second witness, and the conditional write is what refuses.
        let mut withdrawn = open_ballot_row();
        withdrawn.status = TaskStatus::Cancelled;
        let store = MemoryStore::with(vec![withdrawn]);
        let report = settle(&store, at(120)).await.unwrap();
        assert_eq!(report.ballots_closed, 0);
        assert_eq!(report.lost_races, 1);
        assert_eq!(store.row("d").status, TaskStatus::Cancelled);
    }

    /// A record somebody edited is reported as that, and the ballot stays open.
    /// The pass must not close it -- a close would append a step vouching for
    /// the edited part -- and it must not read as a lost race either: nobody
    /// else moved this row, its history is simply no longer trustworthy.
    #[tokio::test]
    async fn a_decision_whose_history_does_not_verify_is_counted_not_closed() {
        let store = MemoryStore::with(vec![open_ballot_row()]);
        let first = cog_core::append_decision_event(&store, "d", "opened", None, None, at(0))
            .await
            .unwrap();
        let mut tampered = first.clone();
        tampered.input[cog_core::DECISION_EVENT_INPUT_KEY]["detail"] =
            serde_json::json!("a detail the row never carried");
        store.save_governance_node(&tampered).await.unwrap();

        let report = settle(&store, at(120)).await.unwrap();
        assert_eq!(report.chains_broken, 1);
        assert_eq!(report.ballots_closed, 0);
        assert_eq!(report.lost_races, 0);
        assert_eq!(
            store.row("d").status,
            TaskStatus::Pending,
            "a ballot on an edited record must stay open"
        );
    }

    fn handover_row(status: TaskStatus) -> Task {
        let spec = HandoverSpec {
            face_id: "task-management".into(),
            from_unit: Some("worker".into()),
            from_absent: None,
            to_unit: "6e".into(),
            witness_unit: "8b2".into(),
            source: None,
            criterion_reading: Some("drifted_binding".into()),
            reading_at: Some(at(0)),
            open_actions: Vec::new(),
            open_task_ids: Vec::new(),
            permissions_context: None,
            artifact_path: None,
            artifact_expires_at: None,
            expires_when: Some(at(60)),
            withdraw_condition: None,
            payload_sha256: None,
            signed_by: Vec::new(),
        };
        let mut row = handover_task("h", spec, at(0));
        row.status = status;
        row
    }

    #[tokio::test]
    async fn a_signed_handover_past_its_own_expiry_is_marked_expired() {
        let store = MemoryStore::with(vec![handover_row(TaskStatus::Scheduled)]);
        let report = settle(&store, at(120)).await.unwrap();
        assert_eq!(report.handovers_expired, 1);
        let row = store.row("h");
        assert_eq!(row.status, TaskStatus::Failed);
        let handover = Handover::from_task(&row).unwrap();
        assert_eq!(handover.state(), HandoverState::Expired);
    }

    #[tokio::test]
    async fn a_draft_handover_does_not_expire() {
        // Expiry is about a signature that was never applied. A draft has no
        // signature, so its expiry is not yet a finding.
        let store = MemoryStore::with(vec![handover_row(TaskStatus::Pending)]);
        let report = settle(&store, at(120)).await.unwrap();
        assert_eq!(report.handovers_expired, 0);
        assert_eq!(store.row("h").status, TaskStatus::Pending);
    }

    #[tokio::test]
    async fn an_applied_handover_is_not_reopened_as_expired() {
        let store = MemoryStore::with(vec![handover_row(TaskStatus::Completed)]);
        let report = settle(&store, at(120)).await.unwrap();
        assert_eq!(report.handovers_expired, 0);
        assert_eq!(store.row("h").status, TaskStatus::Completed);
    }

    /// A close is a state change, so it leaves a step in the history. Without
    /// one a resolved row looks the same whether a tally resolved it or
    /// somebody rewrote the payload.
    #[tokio::test]
    async fn a_close_appends_a_step_to_the_decisions_history() {
        let store = MemoryStore::with(vec![
            open_ballot_row(),
            vote_task("d", "a", "8e", at(0)),
            vote_task("d", "b", "8e", at(1)),
        ]);
        settle(&store, at(120)).await.unwrap();

        let events = cog_core::decision_events(&store, "d").await.unwrap();
        assert_eq!(events.len(), 1, "one close, one step");
        assert_eq!(events[0].spec.kind, "closed");
        assert_eq!(events[0].spec.seq, 0);
        assert_eq!(
            cog_core::verify_decision_chain(&store, "d").await.unwrap(),
            cog_core::ChainVerdict::Intact { events: 1 }
        );
    }

    /// Each append takes the next position and covers the hash before it; the
    /// chain is the sequence, not a pile of rows with timestamps.
    #[tokio::test]
    async fn appending_numbers_and_links_every_step() {
        let store = MemoryStore::with(vec![open_ballot_row()]);
        for (index, kind) in ["opened", "vote_cast", "vote_withdrawn"].iter().enumerate() {
            let event = cog_core::append_decision_event(
                &store,
                "d",
                kind,
                Some("8e"),
                None,
                at(index as i64),
            )
            .await
            .unwrap();
            assert_eq!(event.id, cog_core::decision_event_id("d", index as u64));
        }
        let events = cog_core::decision_events(&store, "d").await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(
            cog_core::verify_decision_chain(&store, "d").await.unwrap(),
            cog_core::ChainVerdict::Intact { events: 3 }
        );
        assert_eq!(events[2].spec.prev_hash, events[1].spec.hash);
    }

    /// A history that does not verify cannot be extended: the new step would
    /// vouch for the edited part. The append is refused and nothing is written,
    /// so the break stays where a reader can find it.
    #[tokio::test]
    async fn an_append_is_refused_when_the_history_does_not_verify() {
        let store = MemoryStore::with(vec![open_ballot_row()]);
        let first = cog_core::append_decision_event(&store, "d", "opened", None, None, at(0))
            .await
            .unwrap();

        let mut tampered = first.clone();
        tampered.input[cog_core::DECISION_EVENT_INPUT_KEY]["detail"] =
            serde_json::json!("a detail the row never carried");
        store.save_governance_node(&tampered).await.unwrap();

        let refused =
            cog_core::append_decision_event(&store, "d", "vote_cast", Some("8e"), None, at(1))
                .await;
        assert!(refused.is_err(), "a broken history must not be extended");
        assert_eq!(
            cog_core::decision_events(&store, "d").await.unwrap().len(),
            1,
            "the refused append writes nothing"
        );
    }

    fn face(owner: Option<&str>) -> Face {
        Face {
            face_id: "变更管理".into(),
            owner_unit: owner.map(str::to_string),
            bound_by_handover_id: None,
            spec: cog_core::FaceSpec::first("变更管理", cog_core::FaceKind::Face),
            updated_at: at(0),
        }
    }

    /// The standing duty row carries who is answerable for the face, and the
    /// dispatcher must leave it alone while the drift reading still counts it.
    ///
    /// Both halves are load-bearing and they pull in opposite directions: the
    /// reading counts only executable rows as open work, so a duty that is not
    /// executable makes the face read `no_bound_task` even though the row is
    /// right there; a duty that is executable and untyped is what the executor
    /// took before this, spending the whole budget on a row with no body and
    /// failing it -- which removes the face's only evidence of staffing.
    #[test]
    fn the_standing_row_is_a_duty_the_dispatcher_leaves_alone() {
        let row = standing_row(&face(Some("worker")), "default");

        assert_eq!(
            row.task_type,
            TaskType::Custom(FACE_DUTY_TASK_TYPE.to_string()),
            "a duty that is a plain dag node is work the dispatcher will start"
        );
        assert!(
            row.is_executable,
            "the drift reading counts only executable rows as open work on the face"
        );
        assert!(
            !crate::ready_queue::is_dispatchable_work(&row),
            "the dispatcher started this row once and failed the face's only binding"
        );
        assert_eq!(row.id, face_work_id("变更管理"));
        assert_eq!(row.face_id.as_deref(), Some("变更管理"));
        assert_eq!(row.owner_unit.as_deref(), Some("worker"));
        assert_eq!(row.workspace_id.as_deref(), Some("default"));
        assert_eq!(
            row.input["owner_unit"],
            serde_json::json!("worker"),
            "the copy the claimer reads names the same unit as the row"
        );
        assert_eq!(
            row.input["face"],
            serde_json::json!("变更管理"),
            "the duty names the face it is for"
        );
        assert!(
            row.input["goal"]
                .as_str()
                .is_some_and(|goal| goal.contains("变更管理")),
            "the goal text is the duty the owner is handed"
        );
    }

    /// A duty is the face's child row, not a second flat list entry.
    ///
    /// The drift reading and the dispatcher only need the row to exist; the
    /// project face needs it to be *in* the face's project, which means the
    /// group the earned-value and tree views group by is the face node's own
    /// id and the duty names that node as its parent. Without both, every face
    /// is a project of one and the tree has no second level to draw.
    #[test]
    fn the_duty_hangs_under_the_face_node_as_that_projects_child() {
        let row = standing_row(&face(Some("worker")), "default");

        assert_eq!(
            row.goal_id.as_deref(),
            Some("变更管理"),
            "the face node's own id is the project the duty belongs to"
        );
        assert_eq!(
            row.parent_task_id.as_deref(),
            Some("变更管理"),
            "a group whose root is the face node needs the duty to name it"
        );
        assert_eq!(
            row.acceptance,
            vec![FACE_DUTY_ACCEPTANCE.to_string()],
            "a leaf with no acceptance criterion is work nobody can call done"
        );
    }

    /// A duty written before this shape settled is repaired by whoever holds it.
    ///
    /// Rows already in the database carry no `goal_id`, no parent and no
    /// acceptance criterion. Every writer that touches one puts the whole shape
    /// right, so the pass that already reads those rows is what migrates them —
    /// there is no separate backfill to run and no window where a repaired row
    /// and a stale one are both live.
    #[test]
    fn an_old_duty_is_repaired_into_the_tree_when_its_writer_holds_it() {
        let mut row = standing_row(&face(Some("worker")), "default");
        row.goal_id = None;
        row.parent_task_id = None;
        row.acceptance.clear();

        ensure_duty_shape(&mut row);

        assert_eq!(row.goal_id.as_deref(), Some("变更管理"));
        assert_eq!(row.parent_task_id.as_deref(), Some("变更管理"));
        assert_eq!(row.acceptance, vec![FACE_DUTY_ACCEPTANCE.to_string()]);
    }

    #[test]
    fn closing_runs_on_its_own_clock_rather_than_the_drift_pass() {
        // A deadline is a statement about when the answer lands, not about
        // when somebody next looked at a face. Sharing the drift pass's tick
        // would make a ballot's close time depend on that pass's read path:
        // one failed graph read there and the deadline silently slips.
        assert!(
            DECISION_CLOSE_INTERVAL <= std::time::Duration::from_secs(30),
            "the close pass runs slower than the deadline it serves"
        );
        assert!(
            DECISION_CLOSE_INTERVAL < crate::face_drift::FACE_DRIFT_PASS_INTERVAL,
            "closing was folded back into the drift pass's cadence"
        );
    }
}
