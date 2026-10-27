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
//! one, so every reader takes the payload state and treats
//! the status as a coarse mirror. The two are written together in one row.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Task, TaskStatus, TaskType};

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
    /// Who cast this signature, when it was not the unit it names.
    ///
    /// A signature is a claim of agreement. The staffing pass is the platform
    /// agreeing to hand a face to a unit that is present, and its name goes
    /// into the row as the caster so a reader can tell an attestation the unit
    /// made from one made on its behalf. Absent for every signature a unit
    /// casts itself, which keeps the digest of every handover written before
    /// this field existed exactly as it was -- and the digest never covered
    /// signatures in the first place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cast_by: Option<String>,
}

/// What a handover row says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoverSpec {
    pub face_id: String,
    /// Who held the face before. `None` when the face was vacant.
    pub from_unit: Option<String>,
    /// When the outgoing owner was observed absent, for a handover that moves a
    /// face whose owner has already left.
    ///
    /// A signature says the signer agreed; an owner that no longer exists can
    /// never say that, and asking another unit to sign on its behalf would put
    /// a name on an attestation it did not make. What replaces that signature
    /// is not a person but a reproducible fact: the heartbeat key for
    /// `from_unit` was gone at this time. The fact is part of the signed
    /// payload, so every other signer attests to the same observation, and
    /// applying it re-reads the key -- an owner that came back before the move
    /// was applied still holds the face, and the absence the others signed no
    /// longer holds.
    ///
    /// Serialised only when present: a handover that does not use this path
    /// must keep the digest it had before the field existed, or every signature
    /// taken under the old shape would stop verifying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_absent: Option<DateTime<Utc>>,
    pub to_unit: String,
    pub witness_unit: String,
    /// Where the reading behind this move came from: a unit's report, the
    /// owner's instruction, a drift row. Without it a reader can check the
    /// number but not who said it.
    #[serde(default)]
    pub source: Option<String>,
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
    /// The artifact the handover hands over, and when it stops being the one
    /// to read. A path without a lifetime is an invitation to read a stale
    /// copy years later.
    #[serde(default)]
    pub artifact_path: Option<String>,
    #[serde(default)]
    pub artifact_expires_at: Option<DateTime<Utc>>,
    /// The conditions under which the handover is no longer valid.
    pub expires_when: Option<DateTime<Utc>>,
    /// The conditions under which it should be withdrawn before then.
    #[serde(default)]
    pub withdraw_condition: Option<String>,
    /// Digest of the payload as it stood when the first unit signed.
    ///
    /// Signing a row is only worth something if the row cannot change under the
    /// signature. The digest covers everything except the signatures
    /// themselves, so adding a second signer does not invalidate the first, but
    /// editing the terms after one signature no longer verifies.
    #[serde(default)]
    pub payload_sha256: Option<String>,
    #[serde(default)]
    pub signed_by: Vec<Signature>,
}

impl HandoverSpec {
    /// The three units whose signatures make a handover valid, or two when the
    /// outgoing owner was recorded absent.
    ///
    /// The outgoing owner's signature is the attestation that it agreed to
    /// leave. A unit that is gone cannot give it, and waiting for it left every
    /// face whose owner had exited unsignable -- the one state from which no
    /// tool in the tree could move the face. `from_absent` is what stands in
    /// for that signature: a timestamp of an observation every other signer
    /// covers, re-checked when the move is applied.
    pub fn required_signers(&self) -> Vec<&str> {
        let mut signers = vec![self.to_unit.as_str(), self.witness_unit.as_str()];
        if self.from_absent.is_none() {
            if let Some(from) = self.from_unit.as_deref() {
                signers.push(from);
            }
        }
        signers
    }

    pub fn is_fully_signed(&self) -> bool {
        self.required_signers()
            .iter()
            .all(|unit| self.signed_by.iter().any(|s| s.unit == *unit))
    }

    /// The terms without the attestations: what every signature is over.
    ///
    /// Signatures are excluded because each one is added to a payload other
    /// units already signed; including them would make the second signature
    /// invalidate the first. The stored digest is excluded too, so computing it
    /// and storing it is not a change to what was signed.
    pub fn payload_digest(&self) -> String {
        let mut unsigned = self.clone();
        unsigned.signed_by.clear();
        unsigned.payload_sha256 = None;
        let bytes = serde_json::to_vec(&unsigned).expect("a handover spec serialises");
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    /// Whether the payload still matches what the first signer attested to.
    ///
    /// A draft that has never been signed carries no digest and cannot be
    /// checked; that is not a failure, it is the state before the first
    /// signature.
    pub fn signature_covers_payload(&self) -> bool {
        match self.payload_sha256.as_deref() {
            None => self.signed_by.is_empty(),
            Some(expected) => expected == self.payload_digest(),
        }
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
    /// The question was decided before this record existed and no tie rule was
    /// written down first. The row says so instead of lending the vote a rule
    /// it never had; only a backfilled record may carry this value.
    NotPredeclared,
}

impl TieBreak {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StatusQuo => "status_quo",
            Self::SecondRound => "second_round",
            Self::EscalateNamedBody => "escalate_named_body",
            Self::NotPredeclared => "not_predeclared",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "status_quo" => Some(Self::StatusQuo),
            "second_round" => Some(Self::SecondRound),
            "escalate_named_body" => Some(Self::EscalateNamedBody),
            "not_predeclared" => Some(Self::NotPredeclared),
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
    /// How this row came to exist: a live question, or a reconstruction of one
    /// answered before the ledger did. A record that cannot say which is a
    /// record a reader will mistake for having been governed at the time.
    #[serde(default)]
    pub provenance: Option<String>,
    /// Whether the rules that settled it were written before the votes were
    /// cast. False on a backfilled row: the tally is real, the procedure was
    /// not. Nothing may read false as an endorsement.
    #[serde(default)]
    pub predeclared: bool,
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

/// The task type a decision event row carries.
pub const DECISION_EVENT_TASK_TYPE: &str = "decision_event";

/// Where a decision event keeps its payload.
pub const DECISION_EVENT_INPUT_KEY: &str = "decision_event";

/// One step in a decision's history.
///
/// A decision row is rewritten as the question moves -- opened, cast, closed --
/// and the row a reader sees is the latest state, not the way it was reached.
/// Votes are separate rows, but the state changes themselves leave nothing
/// behind: a resolved row looks the same whether it was closed by a tally or
/// quietly rewritten by somebody with a database client.
///
/// So each state change appends an event whose hash covers the event before it.
/// The chain proves the sequence was not edited in place; it cannot prove it was
/// not rewritten wholesale, because nothing outside this database anchors the
/// head. That limit is the point of stating it here rather than implying the
/// chain is stronger than it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionEventSpec {
    pub decision_id: String,
    /// 0-based position in this decision's chain.
    pub seq: u64,
    /// `opened` | `vote_cast` | `vote_withdrawn` | `closed` | `backfilled`.
    pub kind: String,
    /// Who did it, when the caller can say. An open run from a script has no
    /// unit and is not given a made-up one.
    pub actor_unit: Option<String>,
    pub detail: Option<String>,
    /// The previous event's hash; empty for the first event.
    pub prev_hash: String,
    /// What this event hashes to. It does not cover itself -- a hash cannot
    /// contain its own value -- so it covers everything else.
    pub hash: String,
}

/// One decision event as the graph holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionEvent {
    pub id: String,
    pub spec: DecisionEventSpec,
}

impl DecisionEvent {
    pub fn from_task(task: &Task) -> Option<Self> {
        if !is_typed(task, DECISION_EVENT_TASK_TYPE) {
            return None;
        }
        Some(Self {
            id: task.id.clone(),
            spec: serde_json::from_value(task.input.get(DECISION_EVENT_INPUT_KEY)?.clone()).ok()?,
        })
    }
}

/// The bytes an event's hash is computed over.
///
/// Field by field with a version tag, rather than a serialised struct: a
/// serialiser that reordered fields would change every hash in the chain
/// without any event having changed.
fn decision_event_payload(
    decision_id: &str,
    seq: u64,
    kind: &str,
    actor_unit: Option<&str>,
    detail: Option<&str>,
    prev_hash: &str,
) -> String {
    format!(
        "v1\n{decision_id}\n{seq}\n{kind}\n{}\n{}\n{prev_hash}",
        actor_unit.unwrap_or(""),
        detail.unwrap_or("")
    )
}

/// The hash of one event.
pub fn decision_event_hash(
    decision_id: &str,
    seq: u64,
    kind: &str,
    actor_unit: Option<&str>,
    detail: Option<&str>,
    prev_hash: &str,
) -> String {
    blake3::hash(
        decision_event_payload(decision_id, seq, kind, actor_unit, detail, prev_hash).as_bytes(),
    )
    .to_hex()
    .to_string()
}

/// The id an event at this position carries. One id per position means a second
/// write to the same position overwrites rather than forks the chain, which is
/// what verification can see and report.
pub fn decision_event_id(decision_id: &str, seq: u64) -> String {
    format!("decision-event:{decision_id}:{seq:06}")
}

/// Build one event row.
pub fn decision_event_task(
    decision_id: &str,
    seq: u64,
    kind: &str,
    actor_unit: Option<&str>,
    detail: Option<&str>,
    prev_hash: &str,
    now: DateTime<Utc>,
) -> Task {
    let hash = decision_event_hash(decision_id, seq, kind, actor_unit, detail, prev_hash);
    let spec = DecisionEventSpec {
        decision_id: decision_id.to_string(),
        seq,
        kind: kind.to_string(),
        actor_unit: actor_unit.map(str::to_string),
        detail: detail.map(str::to_string),
        prev_hash: prev_hash.to_string(),
        hash,
    };
    let id = decision_event_id(decision_id, seq);
    let mut task = Task::new(
        &id,
        TaskType::Custom(DECISION_EVENT_TASK_TYPE.into()),
        serde_json::json!({ DECISION_EVENT_INPUT_KEY: spec }),
    );
    task.status = TaskStatus::Completed;
    task.updated_at = now;
    task
}

/// Every event of one decision, in chain order.
pub async fn decision_events(
    store: &dyn crate::GovernanceStore,
    decision_id: &str,
) -> crate::SFResult<Vec<DecisionEvent>> {
    let mut events: Vec<DecisionEvent> = store
        .governance_nodes(DECISION_EVENT_TASK_TYPE)
        .await?
        .iter()
        .filter_map(DecisionEvent::from_task)
        .filter(|event| event.spec.decision_id == decision_id)
        .collect();
    events.sort_by_key(|event| event.spec.seq);
    Ok(events)
}

/// What verification of one chain found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainVerdict {
    /// Every event is at the expected position, links to the one before it, and
    /// hashes to what it says.
    Intact {
        events: usize,
    },
    Broken {
        at_seq: Option<u64>,
        reason: String,
    },
}

/// Verify a chain that was already read.
pub fn verify_decision_events(events: &[DecisionEvent]) -> ChainVerdict {
    let mut prev_hash = String::new();
    for (expected_seq, event) in events.iter().enumerate() {
        let expected_seq = expected_seq as u64;
        if event.spec.seq != expected_seq {
            return ChainVerdict::Broken {
                at_seq: Some(event.spec.seq),
                reason: format!(
                    "seq {} where {expected_seq} was expected; an event was removed or added",
                    event.spec.seq
                ),
            };
        }
        if event.spec.prev_hash != prev_hash {
            return ChainVerdict::Broken {
                at_seq: Some(event.spec.seq),
                reason: "the event does not link to the hash of the one before it".into(),
            };
        }
        let recomputed = decision_event_hash(
            &event.spec.decision_id,
            event.spec.seq,
            &event.spec.kind,
            event.spec.actor_unit.as_deref(),
            event.spec.detail.as_deref(),
            &event.spec.prev_hash,
        );
        if recomputed != event.spec.hash {
            return ChainVerdict::Broken {
                at_seq: Some(event.spec.seq),
                reason: "the event's contents do not hash to the value it carries".into(),
            };
        }
        prev_hash = event.spec.hash.clone();
    }
    ChainVerdict::Intact {
        events: events.len(),
    }
}

/// Append one event to a decision's chain.
///
/// Refuses when the chain that exists does not verify: appending to a broken
/// history would make the new event vouch for it. A refused append writes
/// nothing, so the break stays exactly where it was.
pub async fn append_decision_event(
    store: &dyn crate::GovernanceStore,
    decision_id: &str,
    kind: &str,
    actor_unit: Option<&str>,
    detail: Option<&str>,
    now: DateTime<Utc>,
) -> crate::SFResult<Task> {
    let events = decision_events(store, decision_id).await?;
    if let ChainVerdict::Broken { at_seq, reason } = verify_decision_events(&events) {
        return Err(crate::SFError::Conflict(format!(
            "decision {decision_id} has a broken history at seq {}: {reason}; the new event \
             would vouch for it",
            at_seq
                .map(|seq| seq.to_string())
                .unwrap_or_else(|| "?".into())
        )));
    }
    let seq = events.last().map(|event| event.spec.seq + 1).unwrap_or(0);
    let prev_hash = events
        .last()
        .map(|event| event.spec.hash.clone())
        .unwrap_or_default();
    let task = decision_event_task(decision_id, seq, kind, actor_unit, detail, &prev_hash, now);
    store.save_governance_node(&task).await?;
    Ok(task)
}

/// What came of trying to close a decision.
///
/// The three outcomes are kept apart because a caller that counts them reports
/// three different facts: a close that landed, a race it lost, and a record
/// somebody edited. Collapsing any two into one counter makes the reading claim
/// something it never measured, and a storage failure is none of the three --
/// it comes back as an error and stops the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseOutcome {
    /// The row moved and the step that explains it was written with it.
    Closed,
    /// The row was no longer in an expected status, so neither was written.
    LostRace,
    /// The recorded history does not verify, so the close was refused.
    ChainBroken { at_seq: Option<u64>, reason: String },
}

/// Close a decision and append the step that closes it, as one write.
///
/// The row and its last event are one fact. Written one after the other, a
/// reader can find the row closed with no step explaining it, or a step
/// claiming a close that lost its race to another writer. The store writes
/// both or neither.
///
/// A history that does not verify is refused rather than retried: until
/// somebody reads the record, the close would vouch for an edit.
pub async fn close_decision_with_event(
    store: &dyn crate::GovernanceStore,
    decision_id: &str,
    row: &Task,
    expected: &[TaskStatus],
    detail: Option<&str>,
    now: DateTime<Utc>,
) -> crate::SFResult<CloseOutcome> {
    let events = decision_events(store, decision_id).await?;
    if let ChainVerdict::Broken { at_seq, reason } = verify_decision_events(&events) {
        return Ok(CloseOutcome::ChainBroken { at_seq, reason });
    }
    let seq = events.last().map(|event| event.spec.seq + 1).unwrap_or(0);
    let prev_hash = events
        .last()
        .map(|event| event.spec.hash.clone())
        .unwrap_or_default();
    let event = decision_event_task(decision_id, seq, "closed", None, detail, &prev_hash, now);
    if store
        .save_governance_node_with_event(&event, row, expected)
        .await?
    {
        Ok(CloseOutcome::Closed)
    } else {
        Ok(CloseOutcome::LostRace)
    }
}

/// Read and verify one decision's chain.
pub async fn verify_decision_chain(
    store: &dyn crate::GovernanceStore,
    decision_id: &str,
) -> crate::SFResult<ChainVerdict> {
    let events = decision_events(store, decision_id).await?;
    Ok(verify_decision_events(&events))
}

/// Whether `task_type` names `expected`.
///
/// Custom task types serialise as `{"custom": "<name>"}`, so a row is matched
/// through that object rather than through a bare string.
pub fn is_typed(task: &Task, expected: &str) -> bool {
    matches!(&task.task_type, TaskType::Custom(name) if name == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_760_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn a_handover_is_signed_only_by_its_three_units() {
        let spec = HandoverSpec {
            face_id: "\u{4efb}\u{52a1}\u{7ba1}\u{7406}".into(),
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

    /// A face whose owner has left cannot be handed over on a signature that
    /// owner will never give. The observation that it was gone stands in for
    /// that signature, and it has to be inside the payload the other units
    /// sign -- otherwise the two signers attest to a move whose stated reason
    /// a later writer could rewrite.
    #[test]
    fn an_absent_owner_is_replaced_by_an_observation_the_signature_covers() {
        let mut spec = HandoverSpec {
            face_id: "\u{5b89}\u{5168}\u{4e0e}\u{51ed}\u{8bc1}".into(),
            from_unit: Some("b4".into()),
            from_absent: Some(at(0)),
            to_unit: "worker".into(),
            witness_unit: "operator".into(),
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
            signed_by: Vec::new(),
        };
        assert_eq!(spec.required_signers(), vec!["worker", "operator"]);
        spec.payload_sha256 = Some(spec.payload_digest());
        spec.signed_by.push(Signature {
            unit: "worker".into(),
            at: at(1),
            cast_by: None,
        });
        assert!(!spec.is_fully_signed());
        assert!(spec.signature_covers_payload());
        spec.signed_by.push(Signature {
            unit: "operator".into(),
            at: at(2),
            cast_by: None,
        });
        assert!(spec.is_fully_signed());

        // The time of the observation is a term like any other: moving it is a
        // change to what the signers attested to.
        let before = spec.payload_digest();
        spec.from_absent = Some(at(3));
        assert_ne!(spec.payload_digest(), before);
        assert!(!spec.signature_covers_payload());
    }

    /// A handover that does not use the exit path must digest exactly as it did
    /// before the field existed, or every signature taken under the old shape
    /// would stop verifying the moment this code shipped.
    #[test]
    fn a_handover_without_an_absence_keeps_the_digest_it_had_before_the_field() {
        let spec = HandoverSpec {
            face_id: "f".into(),
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
            signed_by: Vec::new(),
        };
        let bytes = serde_json::to_value(&spec).unwrap();
        assert!(
            bytes.get("from_absent").is_none(),
            "an unused field must not reach the serialised payload: {bytes}"
        );
        let old_shape = serde_json::json!({
            "face_id": "f",
            "from_unit": "worker",
            "to_unit": "6e",
            "witness_unit": "8b2",
            "source": null,
            "criterion_reading": null,
            "reading_at": null,
            "open_actions": [],
            "open_task_ids": [],
            "permissions_context": null,
            "artifact_path": null,
            "artifact_expires_at": null,
            "expires_when": null,
            "withdraw_condition": null,
            "payload_sha256": null,
            "signed_by": [],
        });
        assert_eq!(bytes, old_shape);
    }

    #[test]
    fn a_signature_stops_covering_the_payload_once_a_term_changes() {
        let mut spec = HandoverSpec {
            face_id: "f".into(),
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
            signed_by: Vec::new(),
        };
        // Before the first signature there is nothing to cover.
        assert!(spec.signature_covers_payload());
        spec.payload_sha256 = Some(spec.payload_digest());
        spec.signed_by.push(Signature {
            unit: "worker".into(),
            at: at(0),
            cast_by: None,
        });
        assert!(spec.signature_covers_payload());
        // A second signature is over the same terms, not a change to them.
        let digest = spec.payload_sha256.clone();
        spec.signed_by.push(Signature {
            unit: "6e".into(),
            at: at(1),
            cast_by: None,
        });
        assert_eq!(spec.payload_sha256, digest);
        assert!(spec.signature_covers_payload());
        // Editing the terms after the first signature does not re-hash; the
        // reader must see that the row no longer matches.
        spec.to_unit = "8e".into();
        assert!(!spec.signature_covers_payload());
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

    fn chain_of(events: usize) -> Vec<DecisionEvent> {
        let mut rows = Vec::new();
        let mut prev = String::new();
        for seq in 0..events as u64 {
            let task = decision_event_task(
                "d",
                seq,
                if seq == 0 { "opened" } else { "vote_cast" },
                Some("8e"),
                Some(&format!("step {seq}")),
                &prev,
                at(seq as i64),
            );
            let event = DecisionEvent::from_task(&task).expect("the row is an event");
            prev = event.spec.hash.clone();
            rows.push(event);
        }
        rows
    }

    /// The chain is only worth writing if each link covers the one before it;
    /// otherwise a reader has a list of rows, not a history.
    #[test]
    fn a_chain_links_each_event_to_the_one_before_it() {
        let events = chain_of(3);
        for pair in events.windows(2) {
            assert_eq!(
                pair[1].spec.prev_hash, pair[0].spec.hash,
                "event {} does not link to event {}",
                pair[1].spec.seq, pair[0].spec.seq
            );
        }
        assert_eq!(
            verify_decision_events(&events),
            ChainVerdict::Intact { events: 3 }
        );
    }

    /// Editing an event's words after the fact leaves its hash describing a row
    /// that no longer exists; this is the case the chain exists to catch.
    #[test]
    fn editing_an_event_in_place_breaks_the_chain() {
        let mut events = chain_of(3);
        events[1].spec.detail = Some("a different story".into());
        match verify_decision_events(&events) {
            ChainVerdict::Broken { at_seq, .. } => assert_eq!(at_seq, Some(1)),
            other => panic!("edited history verified as {other:?}"),
        }
    }

    /// Removing a step leaves a hole the next event's `prev_hash` cannot close:
    /// the link points at a hash that is no longer in the chain.
    #[test]
    fn removing_an_event_breaks_the_chain() {
        let mut events = chain_of(3);
        events.remove(1);
        match verify_decision_events(&events) {
            ChainVerdict::Broken { at_seq, .. } => assert_eq!(at_seq, Some(2)),
            other => panic!("a chain with a hole verified as {other:?}"),
        }
    }

    /// Reordering is the same defect as removal seen from the other end: the
    /// positions no longer run from zero without a gap.
    #[test]
    fn reordering_events_breaks_the_chain() {
        let mut events = chain_of(3);
        events.swap(0, 1);
        assert!(matches!(
            verify_decision_events(&events),
            ChainVerdict::Broken { .. }
        ));
    }

    /// An empty history verifies: a decision that was opened but never moved
    /// has nothing to check, and saying it is broken would make every new
    /// decision unreadable.
    #[test]
    fn an_empty_chain_is_intact() {
        assert_eq!(
            verify_decision_events(&[]),
            ChainVerdict::Intact { events: 0 }
        );
    }
}
