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
pub fn gap_goal_row(reading: &FaceDriftReading, id: String) -> Task {
    let mut row = Task::goal_row(id, &gap_goal_text(reading));
    row.face_id = Some(reading.face_id.clone());
    row.owner_unit = reading.owner.clone();
    // The row is the goal's identity marker and is never executed; the planner
    // decomposes it into work and stamps the same face and owner on the
    // children.
    row.is_executable = false;
    row
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
    use cog_core::DriftState;

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
    fn a_face_with_no_owner_is_left_unaddressed_rather_than_addressed_to_nobody() {
        let row = gap_goal_row(&reading(None), "goal-2".into());
        assert_eq!(row.owner_unit, None);
        assert_eq!(row.face_id.as_deref(), Some("task-management"));
        assert!(!row.is_executable);
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
