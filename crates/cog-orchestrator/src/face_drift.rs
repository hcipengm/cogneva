//! The resident face pass: read the task graph, and hand red faces to the
//! action planner.
//!
//! There is no face table to maintain and no drift table to fill. A face is a
//! non-executable node in the task graph, the reading is computed from the
//! graph and the workers' heartbeats, and this loop exists for the one thing a
//! reading cannot do by itself: act on what it found.
//!
//! Acting is not a second dispatch path. A red face is submitted as a goal
//! through the same entry every other goal uses, so the action planner
//! decomposes it, the DAG holds the pieces, and the ordinary executors carry
//! them. The goal row is stamped with the face, and the decomposition copies
//! that stamp onto the children, so the work it produces counts as work on the
//! face from the moment it is planned rather than only after somebody
//! remembers to label it.
//!
//! One goal per face, state and day. The id is derived from those three
//! instead of being random because the DAG already treats a held id as an
//! idempotent no-op: a pass that runs every two minutes must not submit the
//! same remediation forty times an hour, and a restart must not submit it
//! again. A new day gets a new id, because a face that is still red tomorrow is
//! a finding somebody did not act on, not the same submission.
//!
//! The metrics are computed on scrape rather than cached from the pass. A
//! cached reading would be served by a process whose loop had stopped, which is
//! the exact failure the reading exists to make visible; computing it from the
//! graph costs one query per scrape and cannot be stale. What the loop's own
//! liveness means is already answered by the loop-health beat, so there is no
//! second "is the reader alive" metric to keep in step.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{info, warn};

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{
    AgentRegistry, DriftState, Face, FaceDriftReading, GovernanceStore, Handover, HandoverState,
    OrchestratorControl, OwnerSource, SFResult, Task,
};

/// The loop's name in the health registry.
pub const FACE_DRIFT_LOOP: &str = "governance_face_drift";

/// The role one process at a time holds while dispatching remediation.
///
/// Reading is not arbitrated -- any replica can answer a scrape from the shared
/// graph -- but submitting a goal is a write, and one writer is what keeps the
/// per-day id meaningful.
pub const FACE_DRIFT_ROLE: &str = "governance_face_drift";

/// How often a pass runs.
pub const FACE_DRIFT_PASS_INTERVAL: Duration = Duration::from_secs(120);

/// The scrape-only metric carrying each face's current verdict.
pub const FACE_DRIFT_METRIC: &str = "cogneva_governance_face_state";

/// The goal id for one face's remediation on one day.
///
/// Public so a test can assert the shape without running a pass, and so the
/// reader of a task id can tell what produced it.
pub fn gap_goal_id(face_id: &str, state: &str, day: &str) -> String {
    format!("face-gap-{}-{state}-{day}", face_slug(face_id))
}

/// A face id as it can travel inside a task id.
///
/// The id travels through the graph as a task id, so it has to be an ASCII task
/// id. Folding every unsafe character to a dash would not do: face ids are
/// written in whatever language names them, and two names that differ only in
/// characters outside `[a-z0-9_-]` would fold to one id -- one face's daily
/// remediation would then be refused as already submitted because another face
/// had submitted one. Each unsafe character is escaped to its code point
/// instead, under a marker no literal character can produce, so the mapping is
/// injective. Shared with the lifecycle goals so that two ids naming one face
/// are escaped the same way rather than by two rules that agree today.
pub fn face_slug(face_id: &str) -> String {
    face_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c.to_string()
            } else {
                format!("~{code:x}", code = c as u32)
            }
        })
        .collect()
}

/// The text a red face is handed to the planner with.
///
/// The first line is the landing header, so it leads with the face: a run of
/// remediation changes is told apart by which face it was about, and every
/// other word would be shared by all of them. The rest names the verdict and
/// the rules that fired, and says what a closing look would have to show. It
/// deliberately does not prescribe steps: the planner decomposes, and a loop
/// that already knew the steps would be doing the planning it is supposed to
/// delegate.
pub fn gap_goal_text(reading: &FaceDriftReading) -> String {
    let rules = reading
        .red_rules
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Close the gap on face \"{}\"\n\n面「{}」当前读数为 {}（触发的红档规则：{}）。\
         请按生产商用总目标找出差距并补齐，让这一面重新可读：面主明确、任务归属与面一致、\
         缺位有承接人和截止时间。完成后必须能从上一步读数看到它从 {} 变为 on_target。",
        reading.face_id,
        reading.face_id,
        reading.state.as_str(),
        if rules.is_empty() { "无" } else { &rules },
        reading.state.as_str(),
    )
}

/// The row a red face's remediation is submitted under.
///
/// The reading already names who answers for the face; the row carries that
/// name so the decomposition hands it to that unit rather than to whoever
/// happens to poll first. That is the difference between a finding and an
/// announcement: an unaddressed goal is work nobody was asked to do, and a
/// face with no owner stays unaddressed on purpose -- there is nobody to
/// address it to, and inventing an addressee would queue the work behind a
/// unit that cannot act on it.
///
/// The same reasoning applies to a name the registry does not answer to. A
/// reading that fired `unknown_unit` is saying that the owner is not a unit at
/// all, so an owned row would never be offered -- the shared queue refuses
/// owned work by design and only the owner can claim it -- and the remediation
/// would sit Pending for as long as the name stays wrong. The finding still
/// goes out; it goes out unaddressed, where an executor can take it.
pub fn gap_goal_row(reading: &FaceDriftReading, id: String) -> Task {
    let mut row = Task::goal_row(id, &gap_goal_text(reading));
    row.face_id = Some(reading.face_id.clone());
    row.owner_unit = match reading.cause.as_deref() {
        Some("unknown_unit") => None,
        _ => reading.owner.clone(),
    };
    // The row is the goal's identity marker and is never executed; the planner
    // decomposes it into work and stamps the same face and owner on the
    // children.
    row.is_executable = false;
    row
}

/// What one pass does with a face's unstarted work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkPlacement {
    /// The named owner answers to the registry; the rows stay where they are.
    Keep,
    /// The named owner does not answer, and no unit is present to take it.
    Stranded,
    /// The named owner does not answer; the rows go back to the shared queue.
    Release,
}

/// Where a face's unstarted work belongs, given who answers right now.
///
/// A face's owner is a declaration of intent; whether that name answers is a
/// fact measured from the heartbeat registry on every pass. When it does, the
/// face stays where it was put.
///
/// When it does not, the work is released to the shared queue rather than
/// handed to whichever present unit is least loaded. Handing it on does not
/// make it runnable: a row that carries an owner is offered to that owner
/// alone, and a registered unit takes owned work only through the claim
/// command, so re-addressing a row to a pool worker or a squad moves the name
/// and leaves the row exactly as stuck as before -- while the face stops
/// reading as drifted, because its rows now name somebody the registry
/// answers. Measured on a live cluster: eighteen face rows sat unstarted for
/// five days through repeated re-addressing, first to operator names and then
/// to three pool workers whose pods a rollout deleted. Clearing the owner is
/// the one placement every executor can take, and the face's declared owner
/// keeps the face node, so a returning owner is given the duty back by the
/// assignment pass or by a signed handover.
///
/// `Stranded` is the one case a face is left alone: with nobody present there
/// is nothing to hand the work to, and rewriting the rows would only erase the
/// record of who was asked to do it.
pub fn placement_for(preferred: Option<&str>, live: &[String]) -> WorkPlacement {
    if let Some(name) = preferred {
        if live.iter().any(|unit| unit == name) {
            return WorkPlacement::Keep;
        }
    }
    if live.is_empty() {
        return WorkPlacement::Stranded;
    }
    WorkPlacement::Release
}

/// One handover reduced to the three facts this rule reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedMove {
    pub face_id: String,
    pub to_unit: String,
    pub state: HandoverState,
}

impl RecordedMove {
    /// Every handover row, as moves. Nothing is filtered here: whether a state
    /// can explain an owner is the rule's question, not the caller's, and a
    /// caller that filtered would be a second copy of the rule.
    pub fn from_handovers(handovers: &[Handover]) -> Vec<Self> {
        handovers
            .iter()
            .map(|handover| Self {
                face_id: handover.spec.face_id.clone(),
                to_unit: handover.spec.to_unit.clone(),
                state: handover.state(),
            })
            .collect()
    }
}

/// A face whose owner neither the roster nor any signed move accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnexplainedOwner {
    pub face_id: String,
    pub owner_unit: String,
    /// What the roster recorded the last time the row was imported. `None`
    /// when it recorded nothing, which is itself a gap and not an excuse.
    pub roster_said: Option<String>,
}

/// Faces whose owner came from neither the roster nor a signed handover.
///
/// The graph stores an owner as a bare unit name, and every reader of it --
/// the projection, the face reading, a person comparing the two trees -- has
/// to answer "why this unit" from the name alone. Two things can put a name
/// there legitimately: an import that applied the roster, which leaves its own
/// copy of what the file said in `source_fronts`, and a handover that moved
/// the face, which leaves a row naming the unit it moved to. A row that
/// matches neither is a move nobody recorded: the shape a name written
/// straight into the graph has, and the one shape no other reading can tell
/// apart from a legitimate one.
///
/// Which handover counts is part of the rule, not a detail for the caller:
/// same face, `to_unit` equal to the owner, and a state that is signed or
/// applied. A withdrawn or expired row explains nothing even though it still
/// exists, and a draft explains nothing because nobody has agreed to it yet.
///
/// Revision is deliberately not consulted. A row at revision one whose owner
/// matches nothing looks exactly like one whose owner was set by an import,
/// and using revision as an exemption would read every direct write as green
/// the moment it stayed at revision one -- which is what a direct write does.
pub fn owners_no_act_explains(faces: &[Face], moves: &[RecordedMove]) -> Vec<UnexplainedOwner> {
    let agreed: std::collections::HashSet<(&str, &str)> = moves
        .iter()
        .filter(|mov| matches!(mov.state, HandoverState::Signed | HandoverState::Applied))
        .map(|mov| (mov.face_id.as_str(), mov.to_unit.as_str()))
        .collect();
    let mut out: Vec<UnexplainedOwner> = faces
        .iter()
        .filter_map(|face| {
            let owner = face.owner_unit.as_deref()?;
            let roster_said = face
                .spec
                .source_fronts
                .as_deref()
                .and_then(|fronts| fronts.split('/').next())
                .map(str::trim)
                .filter(|unit| !unit.is_empty() && *unit != "-");
            if roster_said == Some(owner) {
                return None;
            }
            if agreed.contains(&(face.face_id.as_str(), owner)) {
                return None;
            }
            Some(UnexplainedOwner {
                face_id: face.face_id.clone(),
                owner_unit: owner.to_string(),
                roster_said: roster_said.map(str::to_string),
            })
        })
        .collect();
    out.sort_by(|a, b| a.face_id.cmp(&b.face_id));
    out
}

/// Put each face's unstarted work back where it can be picked up, and give it
/// back to the owner once that owner is present again.
///
/// Only unstarted rows move; a run in flight belongs to the session holding
/// its lease, and taking it away would leave two units claiming one run. The
/// move is a row rewrite, not a handover: no signature is involved because the
/// face did not change hands on paper, the unit holding its rows simply left,
/// and a departed unit cannot sign. The declared owner stays on the face node,
/// so the reading still says who the face was meant for while the work itself
/// is available to whoever is running.
///
/// The two directions are one rule, not two: a row with no owner is right only
/// while the unit named on the face is gone. Carrying out the release and not
/// its return makes a single lapse in the owner's presence permanent -- the
/// row goes back to the shared queue, nothing binds it again, and the face
/// reads `no_bound_task` for the rest of its life even though the owner came
/// back minutes later. The return names the face's *declared* owner, not a
/// name picked from the registry, so it is not the re-addressing that was
/// measured to strand rows: that guess lands on a unit with no reason to hold
/// this face, while the declared owner is the unit the face row already
/// answers to.
///
/// A measured owner is skipped: it was read off the very rows this would
/// rewrite, so writing it back says nothing the rows do not already say.
async fn reassign_faces(
    store: &dyn GovernanceStore,
    readings: &[FaceDriftReading],
    live: &[String],
) {
    if live.is_empty() {
        info!(
            faces = readings.len(),
            "no unit is present; face work stays where it is and the idle readings are the \
             truth rather than a dispatch fault"
        );
        return;
    }
    for reading in readings {
        match placement_for(reading.owner.as_deref(), live) {
            WorkPlacement::Release => match store.release_face_work(&reading.face_id).await {
                Ok(0) => {}
                Ok(released) => info!(
                    face = %reading.face_id,
                    owner = reading.owner.as_deref().unwrap_or("nobody"),
                    rows = released,
                    "face work released to the shared queue; the unit it named is gone"
                ),
                Err(e) => warn!(
                    face = %reading.face_id,
                    error = %e,
                    "face work could not be released; the reading still reports the face \
                     as it stands"
                ),
            },
            // Give the rows back only to an owner that is here *now*: a
            // registration whose heartbeat has gone stale is still in the
            // registry but is not working the face, and binding rows to it
            // takes them off the queue for nobody -- the same strand the
            // release exists to prevent. `unknown_unit` cannot reach here with
            // a declared owner, but the two are read together everywhere else.
            WorkPlacement::Keep
                if reading.owner_source == OwnerSource::Declared
                    && matches!(
                        reading.state,
                        DriftState::OwnerIdle | DriftState::DriftedBinding
                    )
                    && !matches!(
                        reading.cause.as_deref(),
                        Some("heartbeat_old") | Some("unknown_unit")
                    ) =>
            {
                let Some(owner) = reading.owner.as_deref() else {
                    continue;
                };
                match store.reassign_face_work(&reading.face_id, owner).await {
                    Ok(0) => {}
                    Ok(rows) => info!(
                        face = %reading.face_id,
                        owner = %owner,
                        rows = rows,
                        "a present owner took its face's released rows back"
                    ),
                    Err(e) => warn!(
                        face = %reading.face_id,
                        owner = %owner,
                        error = %e,
                        "the face's rows could not be given back to its owner; the reading \
                         still reports the face as it stands"
                    ),
                }
            }
            _ => {}
        }
    }
}

/// Write back the standing row a face's owner works from.
///
/// That row is the only thing the reading has to tell "the owner has nothing
/// to do" from "somebody has been assigned this face": the owner's presence
/// and an open row bound to it are the whole evidence. It used to be written
/// by a command, once, and nothing wrote it again -- so a row that died stayed
/// dead, and the face read idle for good. A duty typed as an ordinary node is
/// started by the dispatcher, spends its budget on a body it does not have,
/// and is failed on the degenerate-loop rule; a terminal row is not open work,
/// so no later reader ever looks at it again.
///
/// Only a face whose owner is present gets a row. An absent owner's rows are
/// released on the same pass, and writing one would race that release; a
/// returning owner gets its duty back here rather than through a command.
async fn ensure_duty_rows(
    store: &dyn GovernanceStore,
    faces: &[Face],
    live: &[String],
    workspace: &str,
) -> Vec<String> {
    let mut written = Vec::new();
    for face in faces {
        let Some(owner) = face.owner_unit.as_deref() else {
            continue;
        };
        if !live.iter().any(|unit| unit == owner) {
            continue;
        }
        let row = crate::governance_acts::standing_row(face, workspace);
        match store.ensure_face_duty(&row).await {
            Ok(true) => written.push(face.face_id.clone()),
            Ok(false) => {}
            Err(e) => warn!(
                face = %face.face_id,
                owner = %owner,
                error = %e,
                "a face's standing duty row could not be written; the face keeps reading \
                 idle until a pass can write it"
            ),
        }
    }
    written
}

/// Submit one remediation goal per red face, at most once a day each.
async fn dispatch_red_faces(
    readings: &[FaceDriftReading],
    orchestrator: &dyn OrchestratorControl,
    now: DateTime<Utc>,
) {
    let day = now.format(cog_core::DAY_KEY_FORMAT).to_string();
    for reading in readings.iter().filter(|r| r.state.is_red()) {
        let id = gap_goal_id(&reading.face_id, reading.state.as_str(), &day);
        let goal = gap_goal_text(reading);
        let row = gap_goal_row(reading, id.clone());
        match orchestrator.submit_goal_auto(&goal, vec![row]).await {
            Ok(added) if added.is_empty() => {
                info!(
                    face = %reading.face_id,
                    state = reading.state.as_str(),
                    goal_id = %id,
                    "face already has today's remediation goal; nothing new queued"
                );
            }
            Ok(added) => {
                info!(
                    face = %reading.face_id,
                    state = reading.state.as_str(),
                    goal_id = %id,
                    tasks = added.len(),
                    "face remediation handed to the action planner"
                );
            }
            Err(e) => warn!(
                face = %reading.face_id,
                state = reading.state.as_str(),
                error = %e,
                "face remediation could not be submitted; the reading still reports the face red"
            ),
        }
    }
}

/// Start the resident pass.
///
/// Not started is not a fault: a deployment without a shared database holds no
/// face nodes, and one without an orchestrator has nowhere to send a
/// remediation. The reading is still served by the HTTP face wherever the store
/// is published.
pub fn spawn_face_drift_loop(
    store: Arc<dyn GovernanceStore>,
    agents: Option<Arc<dyn AgentRegistry>>,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
    role: Option<Arc<dyn cog_core::OwnerLeaseBroker>>,
    workspace: String,
    shutdown: cog_core::ShutdownSignal,
) {
    info!(
        interval_secs = FACE_DRIFT_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        reads_heartbeats = agents.is_some(),
        dispatches = orchestrator.is_some(),
        %workspace,
        "Face drift loop enabled"
    );
    drop(cog_core::loop_health::spawn(
        FACE_DRIFT_LOOP,
        cog_core::loop_health::Cadence::Periodic(FACE_DRIFT_PASS_INTERVAL),
        shutdown.clone(),
        move |beat| {
            let store = store.clone();
            let agents = agents.clone();
            let orchestrator = orchestrator.clone();
            let role = role.clone();
            let workspace = workspace.clone();
            let shutdown = shutdown.clone();
            async move {
                let hold =
                    cog_core::RoleHold::start(role, FACE_DRIFT_ROLE, beat.clone(), &shutdown);
                let mut ticker = tokio::time::interval(FACE_DRIFT_PASS_INTERVAL);
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
                    let now = Utc::now();
                    // Closing expired ballots and handovers is a separate,
                    // faster loop: a deadline is a promise about when the
                    // answer lands, and it should not wait for the drift pass.
                    let readings = match cog_core::read_drift(&*store, agents.as_deref(), now).await
                    {
                        Ok(readings) => readings,
                        Err(e) => {
                            warn!(
                                error = %e,
                                "face drift pass could not read the task graph; no verdict is \
                                 reported from this pass, and the face's previous state is not \
                                 reused"
                            );
                            continue;
                        }
                    };
                    let red = readings.iter().filter(|r| r.state.is_red()).count();
                    info!(
                        faces = readings.len(),
                        red, "face drift pass read the graph"
                    );
                    // Who is present decides where work goes. A registry that
                    // cannot be read is not an empty roster: without it there
                    // is no way to tell a unit that left from one whose
                    // heartbeat was briefly unreadable, so no row is moved and
                    // the reading the pass just took stands as it is.
                    if let Some(ref registry) = agents {
                        match registry.list().await {
                            Ok(registrations) => {
                                let mut live: Vec<String> = registrations
                                    .iter()
                                    .map(|registration| registration.agent_id.clone())
                                    .collect();
                                live.sort();
                                live.dedup();
                                // Which of the present units can hold a face is
                                // a second question, answered by the role they
                                // registered under: an in-process planner is
                                // alive for the length of its run, and a face
                                // handed to it moves again when the run ends.
                                let mut holders: Vec<String> = registrations
                                    .iter()
                                    .filter(|registration| {
                                        registration.role == crate::face_staffing::FACE_HOLDER_ROLE
                                    })
                                    .map(|registration| registration.agent_id.clone())
                                    .collect();
                                holders.sort();
                                holders.dedup();
                                // Staffing runs before the work is released: a
                                // handover carries the unstarted work bound to
                                // the outgoing unit with it, and releasing the
                                // rows first would leave them unowned for a
                                // pass instead.
                                match crate::face_staffing::staff_faces(
                                    &*store,
                                    &**registry,
                                    &live,
                                    &holders,
                                    now,
                                )
                                .await
                                {
                                    Ok(report) if report != Default::default() => info!(
                                        opened = report.opened,
                                        retargeted = report.retargeted,
                                        moved = report.moved,
                                        withdrawn = report.withdrawn,
                                        "face staffing pass: the graph and the presence registry \
                                         were compared"
                                    ),
                                    Ok(_) => {}
                                    Err(e) => warn!(
                                        error = %e,
                                        "the face staffing pass could not read or write the \
                                         graph; no face moved on its account this pass"
                                    ),
                                }
                                reassign_faces(&*store, &readings, &live).await;
                                // The duty row is the evidence the reading
                                // counts, and nothing wrote it once it died.
                                // Written after the release: a row put back
                                // for an owner and taken away by the same pass
                                // would leave the face reading idle with a
                                // write to its name.
                                if !workspace.trim().is_empty()
                                    && workspace != cog_core::GOVERNANCE_WORKSPACE
                                {
                                    match store.face_nodes().await {
                                        Ok(nodes) => {
                                            let faces: Vec<Face> =
                                                nodes.iter().filter_map(Face::from_task).collect();
                                            let written = ensure_duty_rows(
                                                &*store, &faces, &live, &workspace,
                                            )
                                            .await;
                                            if !written.is_empty() {
                                                info!(
                                                    rows = written.len(),
                                                    faces = ?written,
                                                    "standing duty rows written back; those \
                                                     faces have work to be idle on again"
                                                );
                                            }
                                        }
                                        Err(e) => warn!(
                                            error = %e,
                                            "the face nodes could not be read; no standing \
                                             duty row was written this pass"
                                        ),
                                    }
                                }
                            }
                            Err(e) => warn!(
                                error = %e,
                                "the unit registry could not be read; no face work was moved \
                                 this pass"
                            ),
                        }
                    }
                    if let Some(ref orchestrator) = orchestrator {
                        dispatch_red_faces(&readings, &**orchestrator, now).await;
                    }
                }
            }
        },
    ));
}

/// The scrape-only view of the same readings.
///
/// It asks the graph on every scrape instead of reading a cache: the process
/// serving a scrape is not necessarily the one holding the dispatch seat, and a
/// follower serving the leader's last value would report a stopped loop as
/// healthy.
pub struct FaceDriftObservable {
    store: Arc<dyn GovernanceStore>,
    agents: Option<Arc<dyn AgentRegistry>>,
}

impl FaceDriftObservable {
    pub fn new(store: Arc<dyn GovernanceStore>, agents: Option<Arc<dyn AgentRegistry>>) -> Self {
        Self { store, agents }
    }
}

#[async_trait]
impl Observable for FaceDriftObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let readings =
            cog_core::read_drift(&*self.store, self.agents.as_deref(), Utc::now()).await?;
        Ok(readings
            .into_iter()
            .map(|reading| {
                RawMetric::new(FACE_DRIFT_METRIC, 1.0)
                    .with_label(crate::LABEL_FACE, reading.face_id)
                    .with_label(crate::LABEL_STATE, reading.state.as_str())
                    // A label value has to come from a closed set, so the cause
                    // travels as a token and not as the prose in `detail`. Where
                    // the state alone says what happened, there is no cause and
                    // the label says so, rather than being left off: a series
                    // that sometimes carries a label and sometimes does not is
                    // two shapes for one question.
                    .with_label(
                        crate::LABEL_CAUSE,
                        reading.cause.unwrap_or_else(|| "-".into()),
                    )
            })
            .collect())
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{DriftState, FaceKind, FaceSpec, TaskStatus};

    #[test]
    fn the_goal_id_is_stable_within_a_day_and_changes_with_it() {
        assert_eq!(
            gap_goal_id("task-management", "no_owner", "2026-10-19"),
            "face-gap-task-management-no_owner-2026-10-19"
        );
        assert_eq!(
            gap_goal_id("task-management", "no_owner", "2026-10-20"),
            "face-gap-task-management-no_owner-2026-10-20"
        );
    }

    #[test]
    fn characters_that_cannot_travel_in_a_task_id_are_escaped_and_stay_distinct() {
        let id = gap_goal_id("安全与凭证", "vacancy_expired", "2026-10-19");
        assert!(id.starts_with("face-gap-"));
        assert!(id.ends_with("-vacancy_expired-2026-10-19"));
        assert!(id.is_ascii());

        // Names that share no ASCII characters must not share an id, or one
        // face's daily goal would suppress the other's as already submitted.
        assert_ne!(
            gap_goal_id("安全与凭证", "no_owner", "2026-10-19"),
            gap_goal_id("通信", "no_owner", "2026-10-19")
        );
        // A literal marker character in the name is escaped too, so it cannot
        // imitate an escape sequence.
        assert_ne!(
            gap_goal_id("a~5b", "no_owner", "2026-10-19"),
            gap_goal_id("a[", "no_owner", "2026-10-19")
        );
    }

    fn units(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// A name the registry answers to keeps the face it was given: a face that
    /// moves for reasons other than absence reports a different owner every
    /// pass.
    #[test]
    fn a_present_owner_keeps_its_face() {
        let live = units(&["f5", "worker"]);
        assert_eq!(placement_for(Some("worker"), &live), WorkPlacement::Keep);
    }

    /// A departed owner's work is released to the shared queue, not handed to
    /// another name. Handing it on was measured to strand rows on a live
    /// cluster: the units the registry answers to are pool workers and squads,
    /// which take owned work from nobody, so the row kept its owner, never
    /// reached the queue, and the face stopped reading as drifted.
    #[test]
    fn a_gone_owners_work_is_released_rather_than_readdressed() {
        let live = units(&["alice-0fca56bd", "squad:change-ledger-1-planner"]);
        assert_eq!(
            placement_for(Some("gone-8b"), &live),
            WorkPlacement::Release,
            "no present unit can claim an owned row, so re-addressing it only moves the name"
        );
    }

    /// Nobody present is the one case a face stays idle: there is no unit to
    /// give the work to, and rewriting the owner would erase the record of who
    /// was asked.
    #[test]
    fn with_nobody_present_the_work_is_left_where_it_is() {
        assert_eq!(placement_for(Some("f5"), &[]), WorkPlacement::Stranded);
    }

    fn reading(owner: Option<&str>) -> FaceDriftReading {
        FaceDriftReading {
            face_id: "task-management".into(),
            state: DriftState::NoOwner,
            owner: owner.map(str::to_string),
            owner_source: Default::default(),
            red_rules: vec!["no_owner".into()],
            yellow_rules: Vec::new(),
            cause: None,
            detail: None,
            observed_at: Utc::now(),
            source_revision: 1,
        }
    }

    #[test]
    fn a_red_face_is_handed_to_the_unit_the_reading_names() {
        // A finding with no addressee is an announcement: the goal would sit
        // in the queue until whoever polls first picked it up, and the face it
        // came from would not be anybody's in particular. The reading already
        // knows who answers for the face, so the row carries that name and the
        // decomposition copies it onto the children.
        let row = gap_goal_row(&reading(Some("unit-7")), "goal-1".into());
        assert_eq!(row.owner_unit.as_deref(), Some("unit-7"));
        assert_eq!(row.face_id.as_deref(), Some("task-management"));
        assert!(!row.is_executable);
    }

    #[test]
    fn a_goal_is_never_addressed_to_a_name_the_registry_does_not_answer_to() {
        // Owned work is never offered to the shared queue and only the owner
        // can claim it, so binding a finding to a name that is not a unit is
        // the same as deleting it: the row stays Pending while its face stays
        // broken.
        let mut unknown = reading(Some("worker"));
        unknown.cause = Some("unknown_unit".into());
        let row = gap_goal_row(&unknown, "goal-3".into());
        assert_eq!(row.owner_unit, None);
        assert_eq!(row.face_id.as_deref(), Some("task-management"));
    }

    #[test]
    fn a_face_with_no_owner_is_left_unaddressed_rather_than_addressed_to_nobody() {
        let row = gap_goal_row(&reading(None), "goal-2".into());
        assert_eq!(row.owner_unit, None);
        assert_eq!(row.face_id.as_deref(), Some("task-management"));
        assert!(!row.is_executable);
    }

    /// Records which faces the pass asked to release.
    #[derive(Default)]
    struct RecordingStore {
        adopted: std::sync::Mutex<Vec<(String, String)>>,
        released: std::sync::Mutex<Vec<String>>,
        /// Every duty row the pass asked the store to put back, in the shape
        /// it asked for. Recording the row rather than its id is what lets a
        /// test tell a duty that can survive the dispatcher from one that
        /// cannot: the dead rows in production were the right face and the
        /// wrong type.
        duties: std::sync::Mutex<Vec<Task>>,
    }

    #[async_trait::async_trait]
    impl GovernanceStore for RecordingStore {
        async fn face_nodes(&self) -> SFResult<Vec<Task>> {
            Ok(Vec::new())
        }
        async fn open_face_bound_tasks(&self) -> SFResult<Vec<Task>> {
            Ok(Vec::new())
        }
        async fn save_face_node(&self, _task: &Task) -> SFResult<()> {
            Ok(())
        }
        async fn save_face_node_if_revision(&self, _task: &Task, _expected: i64) -> SFResult<bool> {
            Ok(false)
        }
        async fn governance_nodes(&self, _task_type: &str) -> SFResult<Vec<Task>> {
            Ok(Vec::new())
        }
        async fn save_governance_node(&self, _task: &Task) -> SFResult<()> {
            Ok(())
        }
        async fn save_governance_node_if_status(
            &self,
            _task: &Task,
            _expected: &[TaskStatus],
        ) -> SFResult<bool> {
            Ok(false)
        }
        async fn save_governance_node_with_event(
            &self,
            _event: &Task,
            _node: &Task,
            _expected: &[TaskStatus],
        ) -> SFResult<bool> {
            Ok(false)
        }
        async fn apply_handover(
            &self,
            _handover: &Task,
            _face: &Task,
            _expected_revision: i64,
        ) -> SFResult<Option<usize>> {
            Ok(None)
        }
        async fn reassign_face_work(&self, face_id: &str, to_unit: &str) -> SFResult<usize> {
            self.adopted
                .lock()
                .expect("the recorder lock is not poisoned")
                .push((face_id.to_string(), to_unit.to_string()));
            Ok(0)
        }
        async fn release_face_work(&self, face_id: &str) -> SFResult<usize> {
            self.released
                .lock()
                .expect("the recorder lock is not poisoned")
                .push(face_id.to_string());
            Ok(0)
        }
        async fn ensure_face_duty(&self, row: &Task) -> SFResult<bool> {
            self.duties
                .lock()
                .expect("the recorder lock is not poisoned")
                .push(row.clone());
            Ok(true)
        }
    }

    /// The pass releases a departed owner's face to the queue and touches
    /// nothing when nobody is present: re-addressing is the shape that was
    /// measured to strand rows, so the pass must not do it at all, and the
    /// second half is what keeps "idle" meaning "nobody is here" rather than
    /// "the pass did not run".
    #[tokio::test]
    async fn the_pass_releases_a_gone_owners_face_and_touches_nothing_without_a_unit() {
        let store = RecordingStore::default();
        let readings = vec![reading(Some("gone-unit"))];

        reassign_faces(&store, &readings, &["present-unit".to_string()]).await;
        assert_eq!(
            store.released.lock().expect("lock").clone(),
            vec!["task-management".to_string()],
            "work addressed to a session that is gone goes back to the shared queue"
        );
        assert!(
            store.adopted.lock().expect("lock").is_empty(),
            "a name picked from the registry cannot claim the row, so none is chosen"
        );

        store.released.lock().expect("lock").clear();
        reassign_faces(&store, &readings, &[]).await;
        assert!(
            store.released.lock().expect("lock").is_empty(),
            "nobody present means nobody to hand the face to; the idle reading is the truth"
        );
    }

    /// A face whose seat is filled by a unit that answers is not released --
    /// its rows are given back to it instead.
    ///
    /// The release is only half the rule. A row with no owner is right while
    /// the unit named on the face is gone; once that unit answers again the
    /// row belongs to it, and nothing but this pass can put the name back
    /// (the assignment pass is a command somebody has to type). Without the
    /// return, one lapse in presence leaves the face reading `no_bound_task`
    /// for the rest of its life -- which is exactly how it was found: a
    /// standing row released during a restart, the owner back within minutes,
    /// and the face still idle hours later.
    #[tokio::test]
    async fn a_present_owner_is_given_its_released_rows_back() {
        let store = RecordingStore::default();
        let mut idle = reading(Some("present-unit"));
        idle.state = DriftState::OwnerIdle;
        idle.cause = Some("no_bound_task".into());
        let readings = vec![idle];

        reassign_faces(&store, &readings, &["present-unit".to_string()]).await;
        assert!(
            store.released.lock().expect("lock").is_empty(),
            "a unit that answers is not a departed one; its rows stay addressed to it"
        );
        assert_eq!(
            store.adopted.lock().expect("lock").clone(),
            vec![("task-management".to_string(), "present-unit".to_string())],
            "the rows go back to the owner the face row names"
        );
    }

    /// A registry record whose heartbeat went stale is not presence: binding
    /// the rows to it takes them off the shared queue and gives them to a unit
    /// that has not shown up, which is the strand the release prevents.
    #[tokio::test]
    async fn a_stale_heartbeat_does_not_get_the_rows_back() {
        let store = RecordingStore::default();
        let mut stale = reading(Some("present-unit"));
        stale.state = DriftState::OwnerIdle;
        stale.cause = Some("heartbeat_old".into());

        reassign_faces(&store, &[stale], &["present-unit".to_string()]).await;
        assert!(
            store.adopted.lock().expect("lock").is_empty(),
            "a name whose heartbeat is old is not somebody to hand work to"
        );
        assert!(
            store.released.lock().expect("lock").is_empty(),
            "the row is neither released nor rebound: the registry answered, it just is not there"
        );
    }

    #[test]
    fn the_goal_text_names_the_face_the_state_and_the_rules() {
        let reading = FaceDriftReading {
            face_id: "security".into(),
            state: DriftState::VacancyExpired,
            owner: None,
            owner_source: Default::default(),
            red_rules: vec!["vacancy_expired".into(), "no_owner".into()],
            yellow_rules: Vec::new(),
            cause: None,
            detail: None,
            observed_at: Utc::now(),
            source_revision: 1,
        };
        let text = gap_goal_text(&reading);
        assert!(text.contains("security"));
        assert!(text.contains("vacancy_expired"));
        assert!(text.contains("on_target"));
    }

    /// A face node as the graph holds it: an owner, and the roster's own copy
    /// of what it said the last time the row was imported.
    fn owned(face_id: &str, owner: Option<&str>, roster_said: Option<&str>) -> Face {
        let mut spec = FaceSpec::first(face_id, FaceKind::Face);
        spec.source_fronts = roster_said.map(str::to_string);
        Face {
            face_id: face_id.to_string(),
            owner_unit: owner.map(str::to_string),
            bound_by_handover_id: None,
            spec,
            updated_at: Utc::now(),
        }
    }

    fn mov(face_id: &str, to_unit: &str, state: HandoverState) -> RecordedMove {
        RecordedMove {
            face_id: face_id.to_string(),
            to_unit: to_unit.to_string(),
            state,
        }
    }

    /// The shape measured on 2026-10-26: three faces whose owner matches
    /// neither the roster's copy nor any handover, sitting beside a face that
    /// moved by signed handover. All four have a name in the owner column, so
    /// nothing else in the reading tells them apart.
    #[test]
    fn an_owner_no_act_explains_is_named_and_a_handed_over_one_is_not() {
        let faces = vec![
            owned("任务管理", Some("f5"), Some("operator/8b2")),
            owned("写操作窗口", Some("f5"), None),
            owned("可评估", Some("f5"), None),
            owned("通信", Some("f5"), Some("worker/-")),
        ];
        let moves = vec![mov("通信", "f5", HandoverState::Applied)];

        let unexplained = owners_no_act_explains(&faces, &moves);

        assert_eq!(
            unexplained
                .iter()
                .map(|row| row.face_id.as_str())
                .collect::<Vec<_>>(),
            vec!["任务管理", "写操作窗口", "可评估"],
            "the three faces no record explains are exactly the ones reported"
        );
        assert_eq!(unexplained[0].roster_said.as_deref(), Some("operator"));
        assert_eq!(
            unexplained[1].roster_said, None,
            "a roster that recorded nothing is a gap to report, not an excuse"
        );
    }

    /// The reverse control: without it, a rule that reported every face whose
    /// owner differs from the roster would pass the test above while crying
    /// wolf on every ordinary import.
    #[test]
    fn an_owner_the_roster_itself_names_is_explained() {
        let faces = vec![owned("变更管理", Some("worker"), Some("worker/-"))];
        assert!(owners_no_act_explains(&faces, &[]).is_empty());
    }

    /// The second reverse control, and the one that keeps the rule from being
    /// satisfied by counting rows: a handover that was withdrawn still exists
    /// in the graph, and it explains nothing. A draft is the same -- nobody
    /// agreed to it -- so a face carrying one is still reported.
    #[test]
    fn a_withdrawn_or_draft_handover_explains_no_owner() {
        let faces = vec![
            owned("token 管理", Some("f5"), Some("worker/-")),
            owned("资源管理", Some("f5"), Some("operator/-")),
        ];
        let moves = vec![
            mov("token 管理", "f5", HandoverState::Withdrawn),
            mov("资源管理", "f5", HandoverState::Draft),
        ];
        assert_eq!(owners_no_act_explains(&faces, &moves).len(), 2);
    }

    /// The duty row is a face's only evidence of staffing, and the pass writes
    /// it because nothing else does. In production the rows were written once
    /// by a command, started by the dispatcher, and failed on the
    /// degenerate-loop rule; a terminal row is not open work, so sixteen of
    /// eighteen faces read idle for good and the repair was somebody
    /// remembering to run the command again.
    #[tokio::test]
    async fn a_present_owners_duty_row_is_written_in_the_shape_that_survives() {
        let store = RecordingStore::default();
        let faces = vec![owned("任务管理", Some("f5"), None)];

        let written = ensure_duty_rows(&store, &faces, &["f5".to_string()], "default").await;
        assert_eq!(written, vec!["任务管理".to_string()]);

        let duties = store.duties.lock().expect("lock").clone();
        assert_eq!(duties.len(), 1);
        assert_eq!(
            duties[0].id,
            crate::governance_acts::face_work_id("任务管理")
        );
        assert_eq!(duties[0].owner_unit.as_deref(), Some("f5"));
        assert_eq!(duties[0].status, TaskStatus::Pending);
        assert_eq!(duties[0].workspace_id.as_deref(), Some("default"));
        // The type and the executable flag are the whole reason the old rows
        // died: the dispatcher starts anything executable it can see, and a
        // duty has no body to run, so it burns the budget and is failed.
        assert!(
            matches!(
                &duties[0].task_type,
                cog_core::TaskType::Custom(kind)
                    if kind == crate::governance_acts::FACE_DUTY_TASK_TYPE
            ),
            "a duty row typed as an ordinary node is one the dispatcher eats"
        );
        assert!(duties[0].is_executable);
    }

    /// A row for an owner who is not there would be released by the same
    /// pass, and a vacancy has nobody to hand work to at all.
    #[tokio::test]
    async fn no_duty_row_is_written_for_an_owner_who_is_not_there_or_for_a_vacancy() {
        let store = RecordingStore::default();
        let faces = vec![
            owned("任务管理", Some("f5"), None),
            owned("通信", None, None),
        ];

        let written =
            ensure_duty_rows(&store, &faces, &["someone-else".to_string()], "default").await;
        assert!(written.is_empty());
        assert!(
            store.duties.lock().expect("lock").is_empty(),
            "an absent owner's row is released on the same pass; writing one races it"
        );
    }

    /// A handover that moved the face to some *other* unit does not explain
    /// the owner it has now: the rule matches the pair, not the face.
    #[test]
    fn a_handover_to_another_unit_leaves_the_current_owner_unexplained() {
        let faces = vec![owned("通信", Some("f5"), Some("worker/-"))];
        let moves = vec![mov("通信", "operator", HandoverState::Applied)];
        assert_eq!(owners_no_act_explains(&faces, &moves).len(), 1);
    }
}
