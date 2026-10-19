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
}

/// What a handover row says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoverSpec {
    pub face_id: String,
    /// Who held the face before. `None` when the face was vacant.
    pub from_unit: Option<String>,
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
    fn a_signature_stops_covering_the_payload_once_a_term_changes() {
        let mut spec = HandoverSpec {
            face_id: "f".into(),
            from_unit: Some("worker".into()),
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
        });
        assert!(spec.signature_covers_payload());
        // A second signature is over the same terms, not a change to them.
        let digest = spec.payload_sha256.clone();
        spec.signed_by.push(Signature {
            unit: "6e".into(),
            at: at(1),
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
}
