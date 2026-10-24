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
    AgentRegistry, FaceDriftReading, GovernanceStore, OrchestratorControl, SFResult, Task,
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
    // The id travels through the graph as a task id, so it has to be an ASCII
    // task id. Folding every unsafe character to a dash would not do: face ids
    // are written in whatever language names them, and two names that differ
    // only in characters outside `[a-z0-9_-]` would fold to one id -- one
    // face's daily remediation would then be refused as already submitted
    // because another face had submitted one. Each unsafe character is escaped
    // to its code point instead, under a marker no literal character can
    // produce, so the mapping is injective.
    let face: String = face_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c.to_string()
            } else {
                format!("~{code:x}", code = c as u32)
            }
        })
        .collect();
    format!("face-gap-{face}-{state}-{day}")
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

/// Put each face's unstarted work back where it can be picked up.
///
/// Only unstarted rows move; a run in flight belongs to the session holding
/// its lease, and taking it away would leave two units claiming one run. The
/// move is a row rewrite, not a handover: no signature is involved because the
/// face did not change hands on paper, the unit holding its rows simply left,
/// and a departed unit cannot sign. The declared owner stays on the face node,
/// so the reading still says who the face was meant for while the work itself
/// is available to whoever is running.
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
        if placement_for(reading.owner.as_deref(), live) != WorkPlacement::Release {
            continue;
        }
        match store.release_face_work(&reading.face_id).await {
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
                "face work could not be released; the reading still reports the face as it stands"
            ),
        }
    }
}

/// Submit one remediation goal per red face, at most once a day each.
async fn dispatch_red_faces(
    readings: &[FaceDriftReading],
    orchestrator: &dyn OrchestratorControl,
    now: DateTime<Utc>,
) {
    let day = now.format("%Y-%m-%d").to_string();
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
    shutdown: cog_core::ShutdownSignal,
) {
    info!(
        interval_secs = FACE_DRIFT_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        reads_heartbeats = agents.is_some(),
        dispatches = orchestrator.is_some(),
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
                                    .into_iter()
                                    .map(|registration| registration.agent_id)
                                    .collect();
                                live.sort();
                                live.dedup();
                                reassign_faces(&*store, &readings, &live).await;
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
                    .with_label("face", reading.face_id)
                    .with_label("state", reading.state.as_str())
                    // A label value has to come from a closed set, so the cause
                    // travels as a token and not as the prose in `detail`. Where
                    // the state alone says what happened, there is no cause and
                    // the label says so, rather than being left off: a series
                    // that sometimes carries a label and sometimes does not is
                    // two shapes for one question.
                    .with_label("cause", reading.cause.unwrap_or_else(|| "-".into()))
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
    use cog_core::{DriftState, TaskStatus};

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

    /// A face whose seat is filled by a unit that answers is left alone, even
    /// though every other face in the same pass is released.
    #[tokio::test]
    async fn a_present_owners_rows_are_never_released() {
        let store = RecordingStore::default();
        let readings = vec![reading(Some("present-unit"))];

        reassign_faces(&store, &readings, &["present-unit".to_string()]).await;
        assert!(store.released.lock().expect("lock").is_empty());
        assert!(store.adopted.lock().expect("lock").is_empty());
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
}
