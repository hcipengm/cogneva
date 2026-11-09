//! An id the graph already holds is not work in hand.
//!
//! `cogneva work file --task <id>` asks `work::preflight` before it builds the
//! request it sends to the submission door: a row filed under an id the graph
//! already holds is dropped by the injector without a word, so the caller would
//! be told nothing and would believe work it never filed had been filed. What
//! is pinned here is that refusal, on the graph, in the three shapes a caller
//! meets it in:
//!
//! - the collision is refused, and the refusal names the row it collided with
//!   -- id, status and owner -- because "that id is taken" without saying whose
//!   it is leaves a caller unable to tell a retry of its own work from a
//!   collision with someone else's;
//! - a blocker that is not in the graph is refused too: readiness is computed
//!   over rows that exist, so the row would wait forever while reading as
//!   queued;
//! - a free id whose blockers exist passes, and an id held in another workspace
//!   is not held in this one -- the two refusals above are about what this
//!   workspace's graph holds, not a check that always says no.
//!
//! Every case states the reading that goes red: the occupied-id cases fail if
//! `preflight` stops reading the graph or answers from a constant, and the
//! admissible cases fail if a refusal appears where the row exists or where the
//! key names another workspace.

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::MemoryStateBackend;

use cogneva::work::preflight;

const WORKSPACE: &str = "ws-work";

/// A row as the graph would hold it once something was filed under that id.
fn held(id: &str, status: TaskStatus, owner: Option<&str>) -> Task {
    let mut row = Task::new(
        id.to_string(),
        TaskType::Custom("unit-filed".to_string()),
        serde_json::json!({ "goal": "carry the step forward" }),
    );
    row.status = status;
    row.owner_unit = owner.map(str::to_string);
    row
}

fn blockers(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| (*id).to_string()).collect()
}

/// A filing under an id the graph holds is refused, and the refusal says which
/// row it collided with. Goes red if the id is accepted (the injector would
/// then drop the row silently) or if the refusal names only the id, leaving the
/// caller unable to tell whose work it collided with.
#[tokio::test]
async fn an_id_the_graph_already_holds_is_refused_and_the_row_it_collided_with_is_named() {
    let graph = MemoryStateBackend::new();
    graph
        .dag_set_task(
            WORKSPACE,
            "t-1",
            &held("t-1", TaskStatus::Scheduled, Some("unit-a")),
        )
        .await
        .expect("the graph holds the row this case puts in it");

    let refusal = preflight(&graph, WORKSPACE, "t-1", &[])
        .await
        .expect_err("an id the graph already holds is not an id a worker can file under")
        .to_string();

    assert!(
        refusal.contains("already in the graph"),
        "the refusal has to say that the id is taken, and it said: {refusal}"
    );
    assert!(
        refusal.contains("t-1") && refusal.contains("Scheduled") && refusal.contains("unit-a"),
        "the refusal has to name the row it collided with -- its id, the status it is in and \
         the unit that owns it -- and it said: {refusal}"
    );
}

/// A row no unit owns is still a taken id. Goes red if an unowned row is read
/// as free: the injector drops a duplicate all the same, and the caller would
/// be the one unit that could have acted on it.
#[tokio::test]
async fn a_row_no_unit_owns_is_still_a_taken_id() {
    let graph = MemoryStateBackend::new();
    graph
        .dag_set_task(
            WORKSPACE,
            "t-goal",
            &held("t-goal", TaskStatus::Running, None),
        )
        .await
        .expect("the graph holds the row this case puts in it");

    let refusal = preflight(&graph, WORKSPACE, "t-goal", &[])
        .await
        .expect_err("an unowned row still occupies its id")
        .to_string();

    assert!(
        refusal.contains("nobody"),
        "the refusal has to say that no unit owns the row rather than leaving the owner blank, \
         and it said: {refusal}"
    );
}

/// A blocker the graph does not hold is refused, because the row filed against
/// it would wait forever. Goes red if the blocker check is dropped, or if it
/// writes: the refusal is a read, so the id it refused must stay free.
#[tokio::test]
async fn a_blocker_the_graph_does_not_hold_is_refused_rather_than_filed() {
    let graph = MemoryStateBackend::new();

    let refusal = preflight(&graph, WORKSPACE, "t-new", &blockers(&["t-ghost"]))
        .await
        .expect_err("a blocker that is not in the graph never completes")
        .to_string();

    assert!(
        refusal.contains("t-ghost") && refusal.contains("not in the graph"),
        "the refusal has to name the blocker that is missing, and it said: {refusal}"
    );
    assert!(
        graph
            .dag_get_task(WORKSPACE, "t-new")
            .await
            .expect("the graph answers a read")
            .is_none(),
        "a preflight refusal must leave the graph as it was, and it created t-new"
    );
}

/// A free id whose blockers exist passes, and an id held in another workspace
/// is not held in this one. Goes red if the checks above turn into a function
/// that refuses everything, or if it reads the graph under another workspace's
/// key -- which would make one workspace's ids collide with another's.
#[tokio::test]
async fn a_free_id_with_blockers_that_exist_passes_and_another_workspace_does_not_occupy_it() {
    let graph = MemoryStateBackend::new();
    graph
        .dag_set_task(
            WORKSPACE,
            "t-dep",
            &held("t-dep", TaskStatus::Completed, Some("unit-a")),
        )
        .await
        .expect("the graph holds the blocker this case puts in it");
    graph
        .dag_set_task(
            "ws-other",
            "t-new",
            &held("t-new", TaskStatus::Pending, Some("unit-b")),
        )
        .await
        .expect("the other workspace's graph holds its own row");

    preflight(&graph, WORKSPACE, "t-new", &blockers(&["t-dep"]))
        .await
        .expect("an id free in this workspace with a blocker the graph holds can be filed");
}

/// The refusal is a read: the row it collided with is left exactly as it was.
/// Goes red if the check starts writing to the row it just read -- a status
/// moved or an owner taken while the caller was being told the id is taken.
#[tokio::test]
async fn the_refusal_leaves_the_row_it_collided_with_as_it_was() {
    let graph = MemoryStateBackend::new();
    graph
        .dag_set_task(
            WORKSPACE,
            "t-1",
            &held("t-1", TaskStatus::Pending, Some("unit-a")),
        )
        .await
        .expect("the graph holds the row this case puts in it");

    preflight(&graph, WORKSPACE, "t-1", &[String::from("t-missing")])
        .await
        .expect_err("the id is already taken, so the filing is refused");

    let after = graph
        .dag_get_task(WORKSPACE, "t-1")
        .await
        .expect("the graph answers a read")
        .expect("the refused row is still there");
    assert_eq!(
        after.status,
        TaskStatus::Pending,
        "a refusal must not move the row it refused"
    );
    assert_eq!(
        after.owner_unit.as_deref(),
        Some("unit-a"),
        "a refusal must not take the row it refused away from its owner"
    );
}
