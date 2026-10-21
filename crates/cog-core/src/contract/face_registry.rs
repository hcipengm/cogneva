//! Faces as nodes of the task graph.
//!
//! A face is a standing area of responsibility. The platform already has a
//! place for standing things that are not executed: a task row with
//! `is_executable = false`, exactly the shape a submitted goal keeps while its
//! decomposition runs. A face is that row, living in one reserved workspace,
//! carrying its owner and the rest of its description in the task. There is no
//! second table of faces, because a second table would be a second answer to
//! "who owns this face" and the two would eventually disagree.
//!
//! Three names matter here and nowhere else:
//!
//! - The workspace faces live in. It is reserved so that a face node can never
//!   be mistaken for work submitted to a product workspace, and so that a
//!   product task can never be counted as a face.
//! - The task type a face node carries. Non-executability is what keeps it out
//!   of the queue; the type is what makes a reader able to say what the row is
//!   without guessing from its payload.
//! - The key inside `Task::input` that holds the descriptive half. Ownership
//!   lives on the task's own `owner_unit`, where every other task's owner also
//!   lives, so there is one way to answer "whose is this".
//!
//! Drift is not stored. A reading is what a pass computed from the task graph
//! and the workers' heartbeats; keeping the last one in a table would let a
//! reader print a verdict from a loop that had stopped, which is the failure
//! the reading exists to expose. The pass publishes metrics and a read endpoint
//! recomputes on demand, and both name the moment they observed.
//!
//! Every count over the task graph has to say whether it means work or nodes.
//! A face node is not outstanding work and is never completed, so a total that
//! folds the two together reports a queue that can never empty. That rule is
//! stated once here and applied by the readers.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{SFError, SFResult, Task, TaskStatus};

/// The workspace that holds governance nodes.
///
/// Reserved: a product workspace is named by its work, and this one is named by
/// the fact that its rows are not work.
pub const GOVERNANCE_WORKSPACE: &str = "__governance__";

/// The task type a face node carries.
pub const FACE_TASK_TYPE: &str = "face";

/// The key inside [`Task::input`] holding a face's description.
pub const FACE_INPUT_KEY: &str = "face";

/// How long an owner may go unseen before the face reads as idle.
///
/// Ten minutes: long enough to cover a build or a long model call, short enough
/// that an owner who stopped is visible inside the same working session.
pub const OWNER_IDLE_AFTER_SECS: i64 = 600;

/// Whether a row is one of the standing faces or one of the lines that cut
/// across all of them.
///
/// The two are counted separately -- twelve faces and five cross-cutting
/// lines -- so a table that lost a face and gained a line could still satisfy a
/// single total.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaceKind {
    Face,
    Cross,
}

impl FaceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Face => "face",
            Self::Cross => "cross",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "face" => Some(Self::Face),
            "cross" => Some(Self::Cross),
            _ => None,
        }
    }
}

/// The descriptive half of a face, as it sits inside a face node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaceSpec {
    pub display_name: String,
    pub kind: FaceKind,
    /// A unit that carries one slice of the face and reports the rest as gaps.
    /// Never the same unit as the owner: a deputy that also owns the face is
    /// one name written twice, and the "who do I escalate to" answer disappears
    /// while the row still looks staffed.
    pub deputy_unit: Option<String>,
    /// The unit that can see this face's reading independently of its owner.
    pub witness_unit: Option<String>,
    /// A share of a person's time, when the face is not full-time work.
    pub time_share: Option<String>,
    /// When a declared vacancy stops being acceptable.
    pub vacancy_until: Option<DateTime<Utc>>,
    /// Who is responsible for filling a declared vacancy.
    pub vacancy_assigner: Option<String>,
    /// The task that is expected to close the vacancy.
    pub gap_task_id: Option<String>,
    /// The reading the face is currently judged by.
    pub first_reading: Option<String>,
    /// Where work on this face enters the change path.
    pub entry_path: Option<String>,
    /// The name this face had in an earlier registry, kept so a reader can
    /// follow a rename rather than seeing a face vanish and another appear.
    pub legacy_front_id: Option<String>,
    /// The earlier owners this face was assembled from.
    pub source_fronts: Option<String>,
    /// Bumped by an ownership move. An import leaves it alone; a value above
    /// one is how an import knows the row has moved past the file it reads.
    ///
    /// Defaulted because the file an import reads has no revision: it
    /// describes ownership, not how many times ownership has moved.
    #[serde(default = "default_revision")]
    pub revision: i64,
}

fn default_revision() -> i64 {
    1
}

impl FaceSpec {
    /// A description with no history: the state an import writes first.
    pub fn first(display_name: impl Into<String>, kind: FaceKind) -> Self {
        Self {
            display_name: display_name.into(),
            kind,
            deputy_unit: None,
            witness_unit: None,
            time_share: None,
            vacancy_until: None,
            vacancy_assigner: None,
            gap_task_id: None,
            first_reading: None,
            entry_path: None,
            legacy_front_id: None,
            source_fronts: None,
            revision: 1,
        }
    }

    /// Read the description out of a task's payload.
    ///
    /// An unreadable payload is `None`, not a default: a face whose description
    /// the platform cannot parse is a face that is not registered, and filling
    /// in a blank one would put a silently empty owner in the table.
    pub fn from_task(task: &Task) -> Option<Self> {
        serde_json::from_value(task.input.get(FACE_INPUT_KEY)?.clone()).ok()
    }

    /// Whether the row carries a complete declared vacancy.
    pub fn has_named_vacancy(&self) -> bool {
        self.vacancy_until.is_some()
            && self.vacancy_assigner.is_some()
            && self.gap_task_id.is_some()
    }
}

/// One face as the task graph holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Face {
    pub face_id: String,
    /// `None` is legal only with a complete declaration of who will fill it and
    /// by when. An empty owner with none of that is `no_owner`, not a vacancy.
    pub owner_unit: Option<String>,
    /// The signed handover that moved ownership here, when there was one.
    ///
    /// A revision above one with no such reference is an ownership move nobody
    /// signed, which is the difference between a handover and a drift.
    pub bound_by_handover_id: Option<String>,
    pub spec: FaceSpec,
    pub updated_at: DateTime<Utc>,
}

impl Face {
    /// The face a task row describes, if the row is a face at all.
    pub fn from_task(task: &Task) -> Option<Self> {
        if !is_face_node(task) {
            return None;
        }
        Some(Self {
            face_id: task.id.clone(),
            owner_unit: task.owner_unit.clone(),
            bound_by_handover_id: task.bound_by_handover_id.clone(),
            spec: FaceSpec::from_task(task)?,
            updated_at: task.updated_at,
        })
    }

    pub fn revision(&self) -> i64 {
        self.spec.revision
    }

    /// Whether a reader can name somebody answerable for this face.
    pub fn is_staffed(&self) -> bool {
        self.owner_unit.is_some() || self.spec.has_named_vacancy()
    }

    /// Whether the vacancy's own deadline has passed with nobody in the seat.
    pub fn vacancy_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.owner_unit.is_none()
            && self
                .spec
                .vacancy_until
                .map(|until| until <= now)
                .unwrap_or(false)
    }

    /// Build the face node for this face. `input` carries the description, and
    /// ownership is written on the task's own owner field so every task answers
    /// "whose is this" the same way.
    ///
    /// The node is never executable. That, and not a special case in the queue,
    /// is what keeps a standing row from being picked up as work.
    pub fn to_task(&self, mut existing: Option<Task>) -> Task {
        let mut task = existing.take().unwrap_or_else(|| {
            Task::new(
                self.face_id.clone(),
                crate::TaskType::Custom(FACE_TASK_TYPE.into()),
                serde_json::json!({}),
            )
        });
        task.id = self.face_id.clone();
        task.task_type = crate::TaskType::Custom(FACE_TASK_TYPE.into());
        task.status = crate::TaskStatus::Pending;
        task.is_executable = false;
        task.owner_unit = self.owner_unit.clone();
        task.workspace_id = Some(GOVERNANCE_WORKSPACE.to_string());
        task.updated_at = self.updated_at;
        // The row is saved whole, so every field this function does not write
        // back is dropped. The handover that moved the face is one of them:
        // losing it turns a moved face into "a revision above one with no
        // signed handover", which a reader can only report as drift, forever.
        task.bound_by_handover_id = self.bound_by_handover_id.clone();
        if let Some(input) = task.input.as_object_mut() {
            if let Ok(spec) = serde_json::to_value(&self.spec) {
                input.insert(FACE_INPUT_KEY.to_string(), spec);
            }
        }
        task
    }
}

/// Whether a task row is a face node rather than work.
pub fn is_face_node(task: &Task) -> bool {
    task.workspace_id.as_deref() == Some(GOVERNANCE_WORKSPACE)
        && matches!(&task.task_type, crate::TaskType::Custom(kind) if kind == FACE_TASK_TYPE)
}

/// What the pass said about one face.
///
/// The set is closed on purpose: a reader that has to interpret free text
/// cannot tell "owner is fine" from "the loop has not looked", and the
/// difference between those two is the entire value of the reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftState {
    /// The owner exists and holds open work on this face.
    OnTarget,
    /// The owner is not visibly working the face: no open task on it, or no
    /// heartbeat inside the idle window. Both mean the same thing to a reader
    /// -- the name is there and the work is not.
    OwnerIdle,
    /// An open task's binding disagrees with the face, or a running task is
    /// held by somebody who is not the owner.
    DriftedBinding,
    /// A face with no owner and no complete vacancy.
    NoOwner,
    /// A declared vacancy whose deadline has passed.
    VacancyExpired,
    /// Ownership moved without a signed handover, or a signature that was
    /// never applied.
    UnsignedHandover,
    /// The witness is the owner, so the reading has no independent observer.
    WitnessIsOwner,
    /// The owner is also the deputy.
    DeputyOverlap,
    /// Nothing read this face: no face row, storage unreachable, or no loop
    /// holding the seat. Never green.
    Unread,
}

impl DriftState {
    pub const ALL: [DriftState; 9] = [
        DriftState::OnTarget,
        DriftState::OwnerIdle,
        DriftState::DriftedBinding,
        DriftState::NoOwner,
        DriftState::VacancyExpired,
        DriftState::UnsignedHandover,
        DriftState::WitnessIsOwner,
        DriftState::DeputyOverlap,
        DriftState::Unread,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::OnTarget => "on_target",
            Self::OwnerIdle => "owner_idle",
            Self::DriftedBinding => "drifted_binding",
            Self::NoOwner => "no_owner",
            Self::VacancyExpired => "vacancy_expired",
            Self::UnsignedHandover => "unsigned_handover",
            Self::WitnessIsOwner => "witness_is_owner",
            Self::DeputyOverlap => "deputy_overlap",
            Self::Unread => "unread",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.as_str() == value)
    }

    /// The rules that block a matching change.
    pub fn is_red(self) -> bool {
        matches!(
            self,
            Self::DriftedBinding | Self::NoOwner | Self::VacancyExpired | Self::UnsignedHandover
        )
    }

    /// The rules that are reported and never block.
    pub fn is_yellow(self) -> bool {
        matches!(
            self,
            Self::OwnerIdle | Self::WitnessIsOwner | Self::DeputyOverlap
        )
    }

    /// Neither a pass nor a failure: the face was not observed.
    pub fn is_gray(self) -> bool {
        matches!(self, Self::Unread)
    }
}

/// One face's observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaceDriftReading {
    pub face_id: String,
    pub state: DriftState,
    /// The red rules that held, in the order the pass checked them.
    pub red_rules: Vec<String>,
    pub yellow_rules: Vec<String>,
    /// `owner_idle` stands for several different situations, and a reader that
    /// sees only the state cannot tell them apart. `detail` says which one in
    /// prose; `cause` says it in one token from a closed set, because a label on
    /// a metric may not carry the prose. A reader that gets the state alone has
    /// to guess between "nobody has picked this up", "nobody has renewed the
    /// heartbeat" and "this name is not a unit at all" -- three different
    /// repairs behind one word.
    #[serde(default)]
    pub cause: Option<String>,
    pub detail: Option<String>,
    pub observed_at: DateTime<Utc>,
    /// The face's revision at the moment of the reading.
    pub source_revision: i64,
}

/// The closed set `cause` is drawn from.
///
/// Only the readings where the state alone is ambiguous carry one. A red rule
/// names itself in `red_rules`, so a red reading has no cause to add; neither
/// does `on_target`.
pub const DRIFT_CAUSES: &[&str] = &[
    // No face row carries this id, so there is nothing to observe.
    "no_face_node",
    // The face has an owner but no open task on it is bound to that owner.
    "no_bound_task",
    // The owner is a unit the registry knows, and its last heartbeat is older
    // than the idle window.
    "heartbeat_old",
    // The registry answered and has no such unit.
    "unknown_unit",
    // The registry could not be asked, so no verdict may be printed.
    "registry_unreadable",
];

/// What is known about the owner's liveness.
///
/// "Not seen recently" and "nobody can see" are different facts and only the
/// first one is about the owner. Folding them together would report a registry
/// outage as an idle fleet, and an idle fleet is the shape a real stall takes,
/// so the two would become indistinguishable exactly when it matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerLiveness {
    /// The registry has a heartbeat for this unit at this moment.
    Seen(DateTime<Utc>),
    /// The registry answered and has no such unit. That is a reading: the name
    /// in the face row is not a unit the platform knows.
    Unknown,
    /// The registry itself could not be asked. Nothing is known about the
    /// owner, so no verdict about the face may be printed from this pass.
    Unreadable,
}

/// What a face's owner has been seen doing, gathered by the caller.
///
/// The caller owns the reads; this type exists so the rules are one function
/// rather than one per surface. `face` is `None` when no node carries this id:
/// that is an observation about the graph, and it reads `unread` rather than a
/// verdict, because a face nobody registered is not a face whose owner is fine.
#[derive(Debug, Clone, Copy)]
pub struct FaceObservation<'a> {
    /// The face this observation is about. Named separately from `face` so an
    /// orphan -- a task bound to an id no node carries -- can still be reported
    /// under that id instead of vanishing from the reading.
    pub face_id: &'a str,
    pub face: Option<&'a Face>,
    /// Open, executable tasks carrying this face id.
    pub open_tasks: &'a [Task],
    /// Handovers naming this face that are fully signed and not yet applied.
    pub signed_handovers: &'a [crate::Handover],
    pub owner_liveness: OwnerLiveness,
    pub now: DateTime<Utc>,
}

/// Apply the closed rule set to one face.
///
/// Order is the rule: a red finding wins over a yellow one, and a yellow one
/// wins over "fine". Reporting only the first would hide a drifted binding
/// behind an idle owner, and reporting all of them as one state would make the
/// blocking decision depend on which the reader looked at.
pub fn observe(observation: FaceObservation<'_>) -> FaceDriftReading {
    let now = observation.now;
    let Some(face) = observation.face else {
        return FaceDriftReading {
            face_id: observation.face_id.to_string(),
            state: DriftState::Unread,
            red_rules: Vec::new(),
            yellow_rules: Vec::new(),
            detail: Some("no face node carries this id".into()),
            cause: Some("no_face_node".into()),
            observed_at: now,
            source_revision: 0,
        };
    };

    let mut red = Vec::new();
    let mut yellow = Vec::new();
    let mut detail: Option<String> = None;
    let mut cause: Option<String> = None;

    if !face.is_staffed() {
        red.push(DriftState::NoOwner.as_str().to_string());
    }
    if face.vacancy_expired_at(now) {
        red.push(DriftState::VacancyExpired.as_str().to_string());
    }
    // An ownership move that nobody signed, and a signature nobody applied,
    // are the same defect seen from the two ends of the handover: the row says
    // the face changed hands and the paper that would justify it is missing on
    // one side or stale on the other.
    if face.revision() > 1 && face.bound_by_handover_id.is_none() {
        red.push(DriftState::UnsignedHandover.as_str().to_string());
    }
    if observation.signed_handovers.iter().any(|handover| {
        (now - handover.updated_at).num_seconds() >= crate::SIGNED_UNAPPLIED_LIMIT_SECS
    }) {
        red.push(DriftState::UnsignedHandover.as_str().to_string());
    }
    if let Some(owner) = face.owner_unit.as_deref() {
        // Work is misbound when it names a different unit, or when it is
        // running under somebody else. An unassigned task on a staffed face is
        // not drift: nobody has claimed it yet, and an owner who has not
        // started it is a separate fact from the binding being wrong.
        let misbound = observation.open_tasks.iter().any(|task| {
            task.owner_unit
                .as_deref()
                .map(|bound| bound != owner)
                .unwrap_or(task.status == crate::TaskStatus::Running)
        });
        if misbound {
            red.push(DriftState::DriftedBinding.as_str().to_string());
        }
        if face.spec.witness_unit.as_deref() == Some(owner) {
            yellow.push(DriftState::WitnessIsOwner.as_str().to_string());
        }
        if face.spec.deputy_unit.as_deref() == Some(owner) {
            yellow.push(DriftState::DeputyOverlap.as_str().to_string());
        }
        let working = observation
            .open_tasks
            .iter()
            .any(|task| task.owner_unit.as_deref() == Some(owner));
        // `owner_idle` covers three different situations, and a reader that
        // only sees the state cannot tell them apart: the owner has no work on
        // the face, the owner's heartbeat is old, or the registry answered and
        // does not know this name at all. The third one used to fall through
        // to `on_target` whenever the owner held work -- a green verdict for an
        // owner nobody can see. All three stay in the closed set, and `detail`
        // says which one fired.
        if !working {
            yellow.push(DriftState::OwnerIdle.as_str().to_string());
            detail = Some("no open task on this face is bound to its owner".into());
            cause = Some("no_bound_task".into());
        } else if let OwnerLiveness::Seen(seen) = observation.owner_liveness {
            if (now - seen).num_seconds() >= OWNER_IDLE_AFTER_SECS {
                yellow.push(DriftState::OwnerIdle.as_str().to_string());
                detail = Some(format!(
                    "the owner's last heartbeat is {}s old",
                    (now - seen).num_seconds()
                ));
                cause = Some("heartbeat_old".into());
            }
        } else if observation.owner_liveness == OwnerLiveness::Unknown {
            yellow.push(DriftState::OwnerIdle.as_str().to_string());
            detail = Some("the owner is not a unit the heartbeat registry knows".into());
            cause = Some("unknown_unit".into());
        }
    }

    // A red rule is a fact about the graph and stands regardless of whether the
    // owner could be asked. Anything below red turns on liveness, so a registry
    // that could not be read means no verdict rather than a yellow one.
    let state = if !red.is_empty() {
        DriftState::parse(red[0].as_str()).unwrap_or(DriftState::Unread)
    } else if observation.owner_liveness == OwnerLiveness::Unreadable && face.owner_unit.is_some() {
        DriftState::Unread
    } else if !yellow.is_empty() {
        DriftState::parse(yellow[0].as_str()).unwrap_or(DriftState::Unread)
    } else {
        DriftState::OnTarget
    };

    FaceDriftReading {
        face_id: face.face_id.clone(),
        state,
        red_rules: red,
        yellow_rules: yellow,
        // An unreadable registry outranks any detail the rules could write: the
        // reason there is no verdict is the absence of a reader, not the state
        // of the face.
        detail: if observation.owner_liveness == OwnerLiveness::Unreadable {
            Some("the owner registry could not be read".into())
        } else {
            detail
        },
        // Same precedence as `detail`: what the reader could not see outranks
        // any cause the rules would have written about what it did see.
        cause: if observation.owner_liveness == OwnerLiveness::Unreadable
            && face.owner_unit.is_some()
        {
            Some("registry_unreadable".into())
        } else {
            cause
        },
        observed_at: now,
        source_revision: face.revision(),
    }
}

/// The counts an acceptance check reads.
///
/// Kept as a type so the question "how many faces and how many cross-cutting
/// lines" has one answer computed in one place, instead of each reader doing
/// its own filter and one of them forgetting that a face node is not work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaceCensus {
    pub faces: usize,
    pub cross: usize,
    /// Rows with neither a complete owner nor a complete vacancy.
    pub unstaffed: usize,
    /// Rows whose declared vacancy has expired.
    pub expired: usize,
}

/// The port that reads and writes face nodes in the task graph.
///
/// The methods are phrased in the graph's own terms -- task rows, open work --
/// because that is what the store holds. Nothing here creates a table: a face
/// is a row the task backend already knows how to keep, and this port is the
/// narrow slice of it that governance needs.
#[async_trait]
pub trait GovernanceStore: Send + Sync {
    /// Every node in the reserved workspace that is typed as a face.
    async fn face_nodes(&self) -> SFResult<Vec<Task>>;

    /// Every executable, non-terminal task that carries a face id, across all
    /// workspaces.
    ///
    /// Global on purpose: a face is a standing responsibility, and work for it
    /// may be submitted to any workspace. A per-workspace read would report a
    /// face idle while work for it ran one workspace over.
    async fn open_face_bound_tasks(&self) -> SFResult<Vec<Task>>;

    /// Write a face node unconditionally.
    async fn save_face_node(&self, task: &Task) -> SFResult<()>;

    /// Write a face node only while the stored row is still at `expected`
    /// revision.
    ///
    /// Returns whether it wrote. A handover and an import can meet here: the
    /// import read revision 1, the handover moved the row to 2, and a
    /// last-writer-wins update would quietly undo the signed move. The caller
    /// re-reads and decides; the store only refuses to be the one that erases
    /// the other's work.
    async fn save_face_node_if_revision(&self, task: &Task, expected: i64) -> SFResult<bool>;

    /// Rows in the governance workspace of one task type.
    ///
    /// Handovers, decisions and votes are task rows like the faces; this is the
    /// one read that finds them without a table of their own.
    async fn governance_nodes(&self, task_type: &str) -> SFResult<Vec<Task>>;

    /// Write one governance row unconditionally.
    async fn save_governance_node(&self, task: &Task) -> SFResult<()>;

    /// Write one governance row only while it is still in one of `expected`.
    ///
    /// Signatures and votes arrive from different sessions at the same time; a
    /// last-writer-wins update would drop one of them and the row would look
    /// like the other never happened.
    async fn save_governance_node_if_status(
        &self,
        task: &Task,
        expected: &[TaskStatus],
    ) -> SFResult<bool>;

    /// Write one governance row and one event row in a single transaction.
    ///
    /// The event is what makes the move reconstructable, so the two are one
    /// fact: written separately, the row can say it closed with no step in its
    /// history explaining it, or a step can claim a close that lost its race to
    /// another writer. Returns whether the row was written; when it was not in
    /// one of `expected`, the event is not written either.
    async fn save_governance_node_with_event(
        &self,
        event: &Task,
        node: &Task,
        expected: &[TaskStatus],
    ) -> SFResult<bool>;

    /// Move a face's ownership, record the handover that did it, and move the
    /// work that has not started, atomically.
    ///
    /// Returns how many tasks were rebound, or `None` when nothing was written.
    /// The face write is guarded by the revision the caller read, so an import
    /// and a handover cannot both win.
    ///
    /// Unstarted work follows the face: a `pending` or `scheduled` task bound
    /// to this face and owned by the outgoing unit is reassigned to the
    /// incoming one and stamped with the handover id. A `running` task is left
    /// where it is -- its holder's lease is what makes it executable, and
    /// moving the owner under a lease would give two units a claim on the same
    /// run. Those rows read as `drifted_binding` until the run ends, which is
    /// the truthful reading while a unit that no longer owns the face is still
    /// executing its work.
    async fn apply_handover(
        &self,
        handover: &Task,
        face: &Task,
        expected_revision: i64,
    ) -> SFResult<Option<usize>>;
}

/// What an import did with one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum FaceImportOutcome {
    Inserted,
    Updated,
    /// The row already held exactly what the file said.
    Unchanged,
    /// The file disagrees with an ownership the graph has already moved past
    /// its first revision. The description was updated; the ownership was not.
    OwnershipKept {
        revision: i64,
    },
}

/// Whether an import is allowed to move a face's ownership.
///
/// A roster is an input: it says what ownership is proposed. A handover is the
/// only act that carries the work bound to a face with it. When the roster is
/// imported continuously by a host clock, "the file changed" is not a decision
/// anyone made, so that path asks for [`ImportOwnership::HandoverOnly`] and the
/// file can refresh a description without quietly transferring a face.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportOwnership {
    /// Move ownership while the face is still at its first revision, which is
    /// the manual import's contract: no handover has ever touched the row.
    FromFile,
    /// Never move an existing face's ownership; only a handover may.
    HandoverOnly,
}

/// Import one face node under the manual contract: the file owns the row until
/// a handover moves it.
pub async fn import_face(
    store: &dyn GovernanceStore,
    incoming: &Face,
) -> SFResult<FaceImportOutcome> {
    import_face_with(store, incoming, ImportOwnership::FromFile).await
}

/// Import one face node.
///
/// Idempotent: the same file imported twice leaves the graph exactly as one
/// import did. Under [`ImportOwnership::FromFile`] ownership moves only while
/// the row is at its first revision; past that, and always under
/// [`ImportOwnership::HandoverOnly`], the import refreshes the description and
/// reports which revision kept its ownership.
pub async fn import_face_with(
    store: &dyn GovernanceStore,
    incoming: &Face,
    ownership: ImportOwnership,
) -> SFResult<FaceImportOutcome> {
    let nodes = store.face_nodes().await?;
    let existing_task = nodes
        .iter()
        .find(|task| is_face_node(task) && task.id == incoming.face_id)
        .cloned();
    let existing = existing_task.as_ref().and_then(Face::from_task);

    let Some(current) = existing else {
        let mut fresh = incoming.clone();
        fresh.spec.revision = 1;
        store.save_face_node(&fresh.to_task(None)).await?;
        return Ok(FaceImportOutcome::Inserted);
    };

    let ownership_same = current.owner_unit == incoming.owner_unit
        && current.spec.deputy_unit == incoming.spec.deputy_unit
        && current.spec.witness_unit == incoming.spec.witness_unit
        && current.spec.vacancy_until == incoming.spec.vacancy_until
        && current.spec.vacancy_assigner == incoming.spec.vacancy_assigner
        && current.spec.gap_task_id == incoming.spec.gap_task_id;

    let ownership_kept =
        !ownership_same && (current.revision() > 1 || ownership == ImportOwnership::HandoverOnly);
    let mut merged = if ownership_kept {
        let mut kept = current.clone();
        kept.spec.display_name = incoming.spec.display_name.clone();
        kept.spec.kind = incoming.spec.kind;
        kept.spec.time_share = incoming.spec.time_share.clone();
        kept.spec.first_reading = incoming.spec.first_reading.clone();
        kept.spec.entry_path = incoming.spec.entry_path.clone();
        kept.spec.legacy_front_id = incoming.spec.legacy_front_id.clone();
        kept.spec.source_fronts = incoming.spec.source_fronts.clone();
        kept
    } else {
        let mut updated = incoming.clone();
        updated.spec.revision = current.revision();
        updated
    };

    // Compare before stamping: `Face` equality includes `updated_at`, so a
    // timestamp written first would make this branch unreachable and turn every
    // idempotent import into a write.
    if merged == current {
        return Ok(FaceImportOutcome::Unchanged);
    }
    merged.updated_at = Utc::now();

    // Reuse the row that is already there: the face keeps one identity across
    // description refreshes, and a fresh task would reset `created_at` (and
    // anything else the face's own struct does not carry) on every import.
    let wrote = store
        .save_face_node_if_revision(&merged.to_task(existing_task), current.revision())
        .await?;
    if !wrote {
        return Err(SFError::Validation(format!(
            "face {} changed while it was being imported; re-read and import again",
            merged.face_id
        )));
    }

    if ownership_kept {
        return Ok(FaceImportOutcome::OwnershipKept {
            revision: current.revision(),
        });
    }
    Ok(FaceImportOutcome::Updated)
}

/// Read every face and its verdict from the task graph.
///
/// One function for every surface: the resident pass, the HTTP endpoint and the
/// projection all call this, so there is no second rule set to drift.
///
/// `agents` is the registry the owners' heartbeats are read from. `None` means
/// no registry is published in this deployment, and that is `unread` rather
/// than idle -- a deployment that cannot see heartbeats has not seen an idle
/// owner, it has seen nothing.
pub async fn read_drift(
    store: &dyn GovernanceStore,
    agents: Option<&dyn crate::AgentRegistry>,
    now: DateTime<Utc>,
) -> SFResult<Vec<FaceDriftReading>> {
    let faces: Vec<Face> = store
        .face_nodes()
        .await?
        .iter()
        .filter_map(Face::from_task)
        .collect();
    let open = store.open_face_bound_tasks().await?;
    let handovers: Vec<crate::Handover> = store
        .governance_nodes(crate::HANDOVER_TASK_TYPE)
        .await?
        .iter()
        .filter_map(crate::Handover::from_task)
        .filter(|handover| handover.state() == crate::HandoverState::Signed)
        .collect();

    let mut by_face: std::collections::BTreeMap<&str, Vec<Task>> = Default::default();
    for task in &open {
        if let Some(face_id) = task.face_id.as_deref() {
            by_face.entry(face_id).or_default().push(task.clone());
        }
    }

    // A task bound to an id no node carries is a drift of its own kind; it is
    // reported under that id rather than dropped, so the work cannot point at
    // a face that does not exist without anybody seeing it.
    let mut ids: Vec<&str> = faces.iter().map(|f| f.face_id.as_str()).collect();
    for face_id in by_face.keys() {
        if !ids.contains(face_id) {
            ids.push(face_id);
        }
    }
    ids.sort_unstable();

    let mut readings = Vec::with_capacity(ids.len());
    for face_id in ids {
        let face = faces.iter().find(|f| f.face_id == face_id);
        let owner_liveness = match face.and_then(|f| f.owner_unit.as_deref()) {
            Some(owner) => match agents {
                Some(registry) => match registry.get(owner).await {
                    Ok(Some(registration)) => OwnerLiveness::Seen(registration.last_heartbeat),
                    Ok(None) => OwnerLiveness::Unknown,
                    Err(_) => OwnerLiveness::Unreadable,
                },
                None => OwnerLiveness::Unreadable,
            },
            // No owner is not an unreadable owner: the vacancy rules answer
            // this row on their own, and asking about a name that is not there
            // would turn a declared vacancy into a gray row.
            None => OwnerLiveness::Unknown,
        };
        let open_tasks = by_face.get(face_id).map(Vec::as_slice).unwrap_or_default();
        let signed_handovers: Vec<crate::Handover> = handovers
            .iter()
            .filter(|handover| handover.spec.face_id == face_id)
            .cloned()
            .collect();
        readings.push(observe(FaceObservation {
            face_id,
            face,
            open_tasks,
            signed_handovers: &signed_handovers,
            owner_liveness,
            now,
        }));
    }
    Ok(readings)
}

pub fn census(faces: &[Face], now: DateTime<Utc>) -> FaceCensus {
    let mut counts = FaceCensus::default();
    for face in faces {
        match face.spec.kind {
            FaceKind::Face => counts.faces += 1,
            FaceKind::Cross => counts.cross += 1,
        }
        if !face.is_staffed() {
            counts.unstaffed += 1;
        }
        if face.vacancy_expired_at(now) {
            counts.expired += 1;
        }
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskStatus;

    fn face(owner: Option<&str>) -> Face {
        Face {
            face_id: "task-management".into(),
            owner_unit: owner.map(str::to_string),
            bound_by_handover_id: None,
            spec: FaceSpec::first("任务管理", FaceKind::Face),
            updated_at: Utc::now(),
        }
    }

    fn task(owner: Option<&str>, status: TaskStatus) -> Task {
        let mut task = Task::new("work-1", crate::TaskType::DagNode, serde_json::json!({}));
        task.face_id = Some("task-management".into());
        task.owner_unit = owner.map(str::to_string);
        task.status = status;
        task
    }

    #[test]
    fn a_staffed_owner_with_open_work_is_on_target() {
        let face = face(Some("unit-operator"));
        let tasks = vec![task(Some("unit-operator"), TaskStatus::Running)];
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &tasks,
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(Utc::now()),
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::OnTarget);
        assert!(reading.red_rules.is_empty());
    }

    #[test]
    fn an_owner_that_is_alive_but_holds_no_work_on_the_face_is_idle() {
        let face = face(Some("unit-operator"));
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(Utc::now()),
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::OwnerIdle);
        assert!(reading.state.is_yellow());
    }

    #[test]
    fn a_witness_who_is_the_owner_is_yellow_rather_than_a_second_signature() {
        let mut face = face(Some("unit-operator"));
        face.spec.witness_unit = Some("unit-operator".into());
        let tasks = vec![task(Some("unit-operator"), TaskStatus::Running)];
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &tasks,
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(Utc::now()),
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::WitnessIsOwner);
        assert!(reading.state.is_yellow());
        assert!(reading.red_rules.is_empty());
    }

    #[test]
    fn a_deputy_who_is_the_owner_is_yellow_rather_than_a_backup() {
        let mut face = face(Some("unit-operator"));
        face.spec.deputy_unit = Some("unit-operator".into());
        let tasks = vec![task(Some("unit-operator"), TaskStatus::Running)];
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &tasks,
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(Utc::now()),
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::DeputyOverlap);
        assert!(reading.state.is_yellow());
    }

    #[test]
    fn an_idle_verdict_says_which_of_the_three_situations_fired() {
        // No work on the face.
        let face = face(Some("unit-operator"));
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(Utc::now()),
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::OwnerIdle);
        assert!(reading
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("no open task"));
        // The same three situations, as tokens a metric label can carry.
        assert_eq!(reading.cause.as_deref(), Some("no_bound_task"));

        // Work, but the registry does not know the owner. This used to be
        // `on_target`: a green verdict for a name nobody could see.
        let tasks = vec![task(Some("unit-operator"), TaskStatus::Running)];
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &tasks,
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Unknown,
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::OwnerIdle);
        assert!(reading
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("registry"));
        assert_eq!(reading.cause.as_deref(), Some("unknown_unit"));

        // Work, and a heartbeat that is older than the idle window.
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &tasks,
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(
                Utc::now() - chrono::Duration::seconds(OWNER_IDLE_AFTER_SECS + 1),
            ),
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::OwnerIdle);
        assert!(reading
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("heartbeat"));
        assert_eq!(reading.cause.as_deref(), Some("heartbeat_old"));
    }

    #[test]
    fn every_cause_comes_from_the_closed_set() {
        // A label value that no reader has seen before is a new situation only
        // by accident; the set is what makes "which of these fired" a question
        // with a bounded answer.
        let face = face(Some("unit-operator"));
        let tasks = [task(Some("unit-operator"), TaskStatus::Running)];
        let observed = [
            (None, &[][..], OwnerLiveness::Seen(Utc::now())),
            (None, &[][..], OwnerLiveness::Unknown),
            (None, &[][..], OwnerLiveness::Unreadable),
            (Some(&face), &[][..], OwnerLiveness::Seen(Utc::now())),
            (Some(&face), &tasks[..], OwnerLiveness::Unknown),
            (Some(&face), &tasks[..], OwnerLiveness::Unreadable),
            (
                Some(&face),
                &tasks[..],
                OwnerLiveness::Seen(
                    Utc::now() - chrono::Duration::seconds(OWNER_IDLE_AFTER_SECS + 1),
                ),
            ),
            (Some(&face), &tasks[..], OwnerLiveness::Seen(Utc::now())),
        ];
        for (f, open, liveness) in observed {
            let reading = observe(FaceObservation {
                face_id: "task-management",
                face: f,
                open_tasks: open,
                signed_handovers: &[],
                owner_liveness: liveness,
                now: Utc::now(),
            });
            if let Some(cause) = reading.cause.as_deref() {
                assert!(
                    DRIFT_CAUSES.contains(&cause),
                    "cause {cause} for state {} is outside the closed set",
                    reading.state.as_str()
                );
            }
            // `on_target` is the one state where nothing fired, so it is the one
            // state that must not carry a cause.
            if reading.state == DriftState::OnTarget {
                assert_eq!(reading.cause, None);
            }
        }
    }

    #[test]
    fn work_bound_to_a_different_owner_blocks_the_face() {
        let face = face(Some("unit-a"));
        let tasks = vec![task(Some("unit-b"), TaskStatus::Running)];
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &tasks,
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(Utc::now()),
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::DriftedBinding);
        assert!(reading.state.is_red());
    }

    #[test]
    fn an_empty_owner_without_a_declared_vacancy_is_red() {
        let face = face(None);
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Unknown,
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::NoOwner);
        assert!(reading.state.is_red());
    }

    #[test]
    fn a_declared_vacancy_past_its_deadline_is_red_even_with_its_three_fields() {
        let mut face = face(None);
        face.spec.vacancy_until = Some(Utc::now() - chrono::Duration::minutes(1));
        face.spec.vacancy_assigner = Some("unit-operator".into());
        face.spec.gap_task_id = Some("gap-1".into());
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Unknown,
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::VacancyExpired);
        assert!(reading.state.is_red());
    }

    #[test]
    fn no_face_row_is_unread_and_never_green() {
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: None,
            open_tasks: &[],
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Unknown,
            now: Utc::now(),
        });
        assert_eq!(reading.state, DriftState::Unread);
        assert!(reading.state.is_gray());
        assert!(!reading.state.is_red() && !reading.state.is_yellow());
    }

    #[test]
    fn a_face_node_is_never_executable_and_carries_its_description() {
        let face = face(Some("unit-operator"));
        let task = face.to_task(None);
        assert!(!task.is_executable);
        assert!(matches!(
            task.status,
            TaskStatus::Pending | TaskStatus::Scheduled | TaskStatus::Running
        ));
        assert_eq!(task.workspace_id.as_deref(), Some(GOVERNANCE_WORKSPACE));
        assert!(is_face_node(&task));
        let back = Face::from_task(&task).expect("round trip");
        assert_eq!(back, face);
    }

    #[test]
    fn a_product_task_is_not_a_face_node() {
        let mut task = Task::new("work-1", crate::TaskType::DagNode, serde_json::json!({}));
        task.workspace_id = Some("default".into());
        assert!(!is_face_node(&task));
        assert!(Face::from_task(&task).is_none());
    }

    fn handover(face_id: &str, updated_at: DateTime<Utc>) -> crate::Handover {
        crate::Handover {
            id: "h1".into(),
            status: TaskStatus::Scheduled,
            spec: crate::HandoverSpec {
                face_id: face_id.into(),
                from_unit: Some("unit-a".into()),
                from_absent: None,
                to_unit: "unit-b".into(),
                witness_unit: "unit-c".into(),
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
            },
            updated_at,
        }
    }

    #[test]
    fn an_ownership_move_with_no_signed_handover_is_red() {
        let mut face = face(Some("unit-operator"));
        face.bound_by_handover_id = None;
        face.spec.revision = 2;
        let now = Utc::now();
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[],
            owner_liveness: OwnerLiveness::Seen(now),
            now,
        });
        assert_eq!(reading.state, DriftState::UnsignedHandover);
        assert!(reading.red_rules.contains(&"unsigned_handover".to_string()));
    }

    #[test]
    fn a_signed_handover_left_unapplied_past_the_limit_is_red() {
        let face = face(Some("unit-a"));
        let now = Utc::now();
        let stale = handover(
            "task-management",
            now - chrono::Duration::seconds(crate::SIGNED_UNAPPLIED_LIMIT_SECS + 1),
        );
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[stale],
            owner_liveness: OwnerLiveness::Seen(now),
            now,
        });
        assert!(reading.red_rules.contains(&"unsigned_handover".to_string()));
    }

    #[test]
    fn a_handover_signed_moments_ago_is_not_red_yet() {
        let face = face(Some("unit-a"));
        let now = Utc::now();
        let fresh = handover("task-management", now);
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[fresh],
            owner_liveness: OwnerLiveness::Seen(now),
            now,
        });
        assert!(!reading.red_rules.contains(&"unsigned_handover".to_string()));
    }

    #[test]
    fn the_handover_rule_trusts_its_slice_so_the_caller_must_filter_by_face() {
        let face = face(Some("unit-a"));
        let now = Utc::now();
        let stale = handover(
            "another-face",
            now - chrono::Duration::seconds(crate::SIGNED_UNAPPLIED_LIMIT_SECS + 1),
        );
        // The rule does not re-check the face id: it is handed the rows that
        // belong to this face. Pin that, so a caller that stops filtering is
        // caught by a test rather than by a face turning red for its
        // neighbour's paperwork.
        let reading = observe(FaceObservation {
            face_id: "task-management",
            face: Some(&face),
            open_tasks: &[],
            signed_handovers: &[stale],
            owner_liveness: OwnerLiveness::Seen(now),
            now,
        });
        assert!(reading.red_rules.contains(&"unsigned_handover".to_string()));
    }
}
