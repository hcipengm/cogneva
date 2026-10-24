//! principle: feedback_a_state_row_and_the_step_that_explains_it_are_one_write
//! The governance store, against a live PostgreSQL.
//!
//! A face is a task row, and every rule the governance layer trusts is a claim
//! about which rows a query selects: which rows are faces, which are open work
//! for a face, and which rows a handover moves. Those claims live in SQL, so the
//! only place they can be shown true is the server -- a unit test over an
//! in-memory list would be testing the fixture, not the statement.
//!
//! Three of them fail silently when they are wrong, which is why they get
//! fixtures here:
//!
//! - a face read that matches the wrong type returns an empty registry, which
//!   reads as "no faces" rather than as a broken query;
//! - a work read that forgets the terminal filter keeps reading a finished task
//!   as drift, and one that forgets the workspace filter counts a face node as
//!   work;
//! - a handover that moved a running task would leave its lease and its owner
//!   describing two units, so the row must stay where it is and read as drifted
//!   until the run ends.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test governance_store -- --ignored
//! ```
//!
//! Every id carries a per-run marker and every row is deleted at the end; a
//! round that died before its own cleanup is reclaimed by the next one (see
//! [`sweep_abandoned`]), because a leftover row is a face the resident drift
//! pass would keep working on.

use chrono::Utc;
use cog_core::{
    decision_event_task, Face, FaceKind, FaceSpec, GovernanceStore, HandoverSpec, StateBackend,
    Task, TaskStatus, TaskType, DECISION_EVENT_TASK_TYPE, GOVERNANCE_WORKSPACE, HANDOVER_TASK_TYPE,
};
use cog_storage::{PostgresGovernanceStore, PostgresStateBackend};
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

fn marker() -> String {
    format!(
        "governance-store-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    )
}

/// Rows a round that never reached its own cleanup left behind.
///
/// Cleanup runs at the end of a test, and "the end" is exactly what an aborted
/// round does not reach: an assertion failure, a deadlock, a Ctrl-C. A leaked
/// row here is not inert -- it is a face node in the graph, and the resident
/// drift pass reads every face node in that workspace, so it rewrites the
/// abandoned row's owner every couple of minutes and logs a face that no
/// person put there.
///
/// The marker carries the round's start time, so a row whose marker is older
/// than [`ABANDONED_AFTER`] cannot belong to a round still running: a round
/// that is still going started less than that ago.
const ABANDONED_AFTER: chrono::Duration = chrono::Duration::minutes(10);

async fn sweep_abandoned(pool: &PgPool) {
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT task_id, workspace_id FROM cog_dag_tasks \
          WHERE workspace_id LIKE 'governance-store-test%' \
             OR task_id LIKE 'governance-store-test%'",
    )
    .fetch_all(pool)
    .await
    .expect("read the test family");

    let cutoff = Utc::now().timestamp_nanos_opt().unwrap_or_default()
        - ABANDONED_AFTER.num_nanoseconds().unwrap_or_default();
    for (task_id, workspace) in rows {
        let started = marker_stamp(&task_id).or_else(|| {
            workspace
                .as_deref()
                .and_then(marker_stamp)
                .filter(|_| task_id.starts_with("governance-store-test:"))
        });
        if started.is_some_and(|nanos| nanos < cutoff) {
            sqlx::query("DELETE FROM cog_dag_tasks WHERE task_id = $1")
                .bind(&task_id)
                .execute(pool)
                .await
                .expect("delete an abandoned row");
        }
    }
}

/// The start time embedded in a marker: `governance-store-test:<pid>:<nanos>`.
fn marker_stamp(id: &str) -> Option<i64> {
    id.split(':')
        .nth(2)
        .and_then(|nanos| nanos.parse::<i64>().ok())
}

/// The per-run marker, after reclaiming whatever an earlier round abandoned.
async fn begin(pool: &PgPool) -> String {
    sweep_abandoned(pool).await;
    marker()
}

fn face_node(id: &str, owner: &str, revision: i64) -> Task {
    let mut spec = FaceSpec::first(id, FaceKind::Face);
    spec.revision = revision;
    Face {
        face_id: id.to_string(),
        owner_unit: Some(owner.to_string()),
        bound_by_handover_id: None,
        spec,
        updated_at: Utc::now(),
    }
    .to_task(None)
}

fn handover_node(id: &str, face_id: &str, from: &str, to: &str, status: TaskStatus) -> Task {
    let spec = HandoverSpec {
        face_id: face_id.to_string(),
        from_unit: Some(from.to_string()),
        from_absent: None,
        to_unit: to.to_string(),
        witness_unit: "witness".into(),
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
    let mut task = Task::new(
        id,
        TaskType::Custom(HANDOVER_TASK_TYPE.into()),
        serde_json::json!({ "handover": spec }),
    );
    task.status = status;
    task
}

fn work(id: &str, owner: Option<&str>, face_id: Option<&str>, status: TaskStatus) -> Task {
    let mut task = Task::new(id, TaskType::DagNode, serde_json::json!({}));
    task.owner_unit = owner.map(str::to_string);
    task.face_id = face_id.map(str::to_string);
    task.status = status;
    task
}

async fn clean(pool: &PgPool, prefix: &str) {
    // The marker is in the task id, not in the workspace. Fixtures live in
    // throwaway product workspaces (`test-ws-a`) whose names know nothing
    // about the round, and an event id carries its marker in the middle
    // (`decision-event:<id>:<seq>`). Deleting by workspace alone leaves the
    // product-workspace fixtures behind, and every leftover row is a face the
    // resident drift pass keeps rewriting.
    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id LIKE $1 OR task_id LIKE $1")
        .bind(format!("%{prefix}%"))
        .execute(pool)
        .await
        .expect("clean up");
}

/// A face is a row in the reserved workspace typed as a face. A row carrying
/// the same type in a product workspace is work somebody submitted, and a row
/// of another governance type is not a face either.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn only_the_reserved_workspace_rows_typed_as_faces_are_faces() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    let product_id = format!("{prefix}:product");
    let handover_id = format!("{prefix}:handover");

    store
        .save_face_node(&face_node(&face_id, "8b", 1))
        .await
        .expect("seed the face");
    backend
        .dag_set_task(
            "test-product-workspace",
            &product_id,
            &face_node(&product_id, "8b", 1),
        )
        .await
        .expect("seed a look-alike row");
    store
        .save_governance_node(&handover_node(
            &handover_id,
            &face_id,
            "8b",
            "6e",
            TaskStatus::Scheduled,
        ))
        .await
        .expect("seed a handover");

    let faces: Vec<Face> = store
        .face_nodes()
        .await
        .expect("read the roster")
        .iter()
        .filter_map(Face::from_task)
        .collect();
    let ids: Vec<&str> = faces.iter().map(|face| face.face_id.as_str()).collect();
    assert!(ids.contains(&face_id.as_str()), "the face is in the roster");
    assert!(
        !ids.contains(&product_id.as_str()),
        "a face-typed row outside the reserved workspace is work, not a face: {ids:?}"
    );
    assert!(!ids.is_empty());

    let handovers = store
        .governance_nodes(HANDOVER_TASK_TYPE)
        .await
        .expect("read the handovers");
    assert!(handovers.iter().any(|row| row.id == handover_id));

    clean(&pool, &prefix).await;
    // The look-alike was seeded into a workspace whose name has nothing to do
    // with the round, so this is the row a workspace-only cleanup misses.
    let left: Vec<(String,)> =
        sqlx::query_as("SELECT task_id FROM cog_dag_tasks WHERE task_id LIKE $1")
            .bind(format!("%{prefix}%"))
            .fetch_all(&pool)
            .await
            .expect("read back what survived");
    assert!(
        left.is_empty(),
        "cleanup left fixture rows behind, and a leftover row is a face the \
         drift pass will keep working on: {left:?}"
    );
}

/// Open work for a face is every non-terminal row that names it, in any product
/// workspace, and nothing else. A finished task cannot drift, a row without a
/// face belongs to no face's reading, and a face node in the reserved
/// workspace is not work.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn open_work_is_global_and_excludes_terminal_unbound_and_face_rows() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    let open = format!("{prefix}:open");
    let running = format!("{prefix}:running");
    let done = format!("{prefix}:done");
    let unbound = format!("{prefix}:unbound");

    store
        .save_face_node(&face_node(&face_id, "8b", 1))
        .await
        .expect("seed the face");
    backend
        .dag_set_task(
            "test-ws-a",
            &open,
            &work(&open, Some("8b"), Some(&face_id), TaskStatus::Pending),
        )
        .await
        .expect("seed pending work");
    backend
        .dag_set_task(
            "test-ws-b",
            &running,
            &work(&running, Some("8b"), Some(&face_id), TaskStatus::Running),
        )
        .await
        .expect("seed running work");
    backend
        .dag_set_task(
            "test-ws-a",
            &done,
            &work(&done, Some("8b"), Some(&face_id), TaskStatus::Completed),
        )
        .await
        .expect("seed a finished task");
    backend
        .dag_set_task(
            "test-ws-a",
            &unbound,
            &work(&unbound, Some("8b"), None, TaskStatus::Pending),
        )
        .await
        .expect("seed unbound work");

    let rows = store.open_face_bound_tasks().await.expect("read open work");
    let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
    assert!(ids.contains(&open.as_str()), "pending work counts: {ids:?}");
    assert!(
        ids.contains(&running.as_str()),
        "running work counts -- it is the row most likely to be misbound: {ids:?}"
    );
    assert!(
        !ids.contains(&done.as_str()),
        "a finished task cannot drift, so it is not open work: {ids:?}"
    );
    assert!(
        !ids.contains(&unbound.as_str()),
        "a row that names no face belongs to no face's reading: {ids:?}"
    );
    assert!(
        !ids.contains(&face_id.as_str()),
        "the face node is not work: {ids:?}"
    );

    clean(&pool, &prefix).await;
}

/// The revision guard is what keeps an import and a handover from both winning.
/// A caller that read revision 1 and an ownership move that already went to 2
/// cannot both write: the second matches nothing and is told it wrote nothing.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_face_write_needs_the_revision_the_caller_actually_read() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    store
        .save_face_node(&face_node(&face_id, "8b", 1))
        .await
        .expect("seed the face");

    let stale = face_node(&face_id, "6e", 2);
    assert!(
        !store
            .save_face_node_if_revision(&stale, 2)
            .await
            .expect("guarded write"),
        "a write that read the wrong revision must not land"
    );
    let stored = store
        .face_nodes()
        .await
        .expect("read back")
        .into_iter()
        .filter_map(|task| Face::from_task(&task))
        .find(|face| face.face_id == face_id)
        .expect("the face is still there");
    assert_eq!(
        stored.owner_unit.as_deref(),
        Some("8b"),
        "a refused write must leave the row as it was"
    );

    assert!(
        store
            .save_face_node_if_revision(&stale, 1)
            .await
            .expect("guarded write"),
        "a write at the revision the caller read lands"
    );

    clean(&pool, &prefix).await;
}

/// A handover moves the face, the unstarted work that belongs to the outgoing
/// unit, and the handover row itself -- all of it or none of it. Work already
/// running stays where its lease is, and work owned by somebody else is not
/// touched at all.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn applying_a_handover_moves_the_face_and_the_unstarted_work_only() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    let handover_id = format!("{prefix}:handover");
    let pending = format!("{prefix}:pending");
    let scheduled = format!("{prefix}:scheduled");
    let running = format!("{prefix}:running");
    let other = format!("{prefix}:other");

    store
        .save_face_node(&face_node(&face_id, "8b", 1))
        .await
        .expect("seed the face");
    store
        .save_governance_node(&handover_node(
            &handover_id,
            &face_id,
            "8b",
            "6e",
            TaskStatus::Scheduled,
        ))
        .await
        .expect("seed the signed handover");
    for (workspace, id, owner, status) in [
        ("test-ws-a", &pending, "8b", TaskStatus::Pending),
        ("test-ws-a", &scheduled, "8b", TaskStatus::Scheduled),
        ("test-ws-b", &running, "8b", TaskStatus::Running),
        ("test-ws-a", &other, "6e", TaskStatus::Pending),
    ] {
        backend
            .dag_set_task(
                workspace,
                id,
                &work(id, Some(owner), Some(&face_id), status),
            )
            .await
            .expect("seed work");
    }

    let mut applied_handover =
        handover_node(&handover_id, &face_id, "8b", "6e", TaskStatus::Completed);
    applied_handover.bound_by_handover_id = Some(handover_id.clone());
    let mut moved_face = face_node(&face_id, "6e", 2);
    moved_face.bound_by_handover_id = Some(handover_id.clone());

    let rebound = store
        .apply_handover(&applied_handover, &moved_face, 1)
        .await
        .expect("apply the handover")
        .expect("the handover applied");
    assert_eq!(rebound, 2, "pending and scheduled moved");

    let rows = store
        .open_face_bound_tasks()
        .await
        .expect("read the work back");
    let by_id = |id: &str| {
        rows.iter()
            .find(|row| row.id == id)
            .unwrap_or_else(|| panic!("{id} is still open"))
            .clone()
    };
    assert_eq!(by_id(&pending).owner_unit.as_deref(), Some("6e"));
    assert_eq!(
        by_id(&pending).bound_by_handover_id.as_deref(),
        Some(handover_id.as_str())
    );
    assert_eq!(by_id(&scheduled).owner_unit.as_deref(), Some("6e"));
    assert_eq!(
        by_id(&running).owner_unit.as_deref(),
        Some("8b"),
        "a running task keeps the owner whose lease makes it executable"
    );
    assert_eq!(
        by_id(&other).owner_unit.as_deref(),
        Some("6e"),
        "work owned by a third unit is not the outgoing unit's to hand over"
    );

    let faces: Vec<Face> = store
        .face_nodes()
        .await
        .expect("read the roster")
        .iter()
        .filter_map(Face::from_task)
        .collect();
    let stored = faces
        .iter()
        .find(|face| face.face_id == face_id)
        .expect("the face is still there");
    assert_eq!(stored.owner_unit.as_deref(), Some("6e"));
    assert_eq!(stored.revision(), 2);

    // The same handover cannot apply twice: the handover row is no longer in
    // `scheduled`, so the transaction writes nothing.
    assert!(
        store
            .apply_handover(&applied_handover, &moved_face, 1)
            .await
            .expect("second apply")
            .is_none(),
        "a handover already applied must not apply again"
    );

    clean(&pool, &prefix).await;
}

/// A face whose unit left keeps its unstarted work: the rows move to a unit
/// that is present, in both the row's binding and the goal the claimer reads,
/// while a run in flight and an ownerless row stay where they are.
///
/// The two copies of the owner are the failure this test exists for. The
/// reading takes the row's binding and the executor hands the claimer the goal
/// payload; a pass that moved one and not the other would report the face as
/// reassigned while telling whoever picked it up that it belongs to a unit
/// that is not there.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn unstarted_face_work_follows_a_unit_that_is_present() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    let pending = format!("{prefix}:pending");
    let running = format!("{prefix}:running");
    let ownerless = format!("{prefix}:ownerless");
    let workspace = format!("{prefix}-ws");

    store
        .save_face_node(&face_node(&face_id, "gone-unit", 1))
        .await
        .expect("seed the face");
    for (id, owner, status) in [
        (&pending, Some("gone-unit"), TaskStatus::Pending),
        (&running, Some("gone-unit"), TaskStatus::Running),
        (&ownerless, None, TaskStatus::Pending),
    ] {
        let mut row = work(id, owner, Some(&face_id), status);
        row.input = serde_json::json!({"goal": "hold the face", "owner_unit": owner.unwrap_or("")});
        backend
            .dag_set_task(&workspace, id, &row)
            .await
            .expect("seed the work");
    }

    let moved = store
        .reassign_face_work(&face_id, "present-unit")
        .await
        .expect("move the unstarted work");
    assert_eq!(
        moved, 1,
        "only the unstarted row owned by the departed unit moves"
    );

    let row = backend
        .dag_get_task(&workspace, &pending)
        .await
        .expect("read the pending row")
        .expect("the pending row is still there");
    assert_eq!(row.owner_unit.as_deref(), Some("present-unit"));
    assert_eq!(
        row.input.get("owner_unit").and_then(|v| v.as_str()),
        Some("present-unit"),
        "the claimer is told from the goal who it is working for: {row:?}"
    );
    assert_eq!(
        row.input.get("goal").and_then(|v| v.as_str()),
        Some("hold the face"),
        "moving the owner must not rewrite the goal: {row:?}"
    );

    let running_row = backend
        .dag_get_task(&workspace, &running)
        .await
        .expect("read the running row")
        .expect("the running row is still there");
    assert_eq!(
        running_row.owner_unit.as_deref(),
        Some("gone-unit"),
        "a run in flight belongs to the session holding its lease: {running_row:?}"
    );

    let ownerless_row = backend
        .dag_get_task(&workspace, &ownerless)
        .await
        .expect("read the ownerless row")
        .expect("the ownerless row is still there");
    assert_eq!(
        ownerless_row.owner_unit, None,
        "a row nobody owns is already claimable and must not be taken: {ownerless_row:?}"
    );

    assert_eq!(
        store
            .reassign_face_work(&face_id, "present-unit")
            .await
            .expect("a second pass"),
        0,
        "a pass that finds the work already where it belongs writes nothing"
    );

    clean(&pool, &prefix).await;
}

/// Releasing a face's work clears the owner from both the row and the goal the
/// claimer reads, which is what puts the rows back in front of every worker.
///
/// A row that keeps either copy is still refused by the shared queue, so a
/// release that wrote only one of them would report the work as freed while
/// nothing could pick it up -- the same shape as re-addressing it to a name
/// nobody answers to.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn released_face_work_is_offered_to_every_worker() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    let pending = format!("{prefix}:pending");
    let running = format!("{prefix}:running");
    let workspace = format!("{prefix}-ws");

    store
        .save_face_node(&face_node(&face_id, "gone-unit", 1))
        .await
        .expect("seed the face");
    for (id, status) in [
        (&pending, TaskStatus::Pending),
        (&running, TaskStatus::Running),
    ] {
        let mut row = work(id, Some("gone-unit"), Some(&face_id), status);
        row.input = serde_json::json!({"goal": "hold the face", "owner_unit": "gone-unit"});
        backend
            .dag_set_task(&workspace, id, &row)
            .await
            .expect("seed the work");
    }

    let released = store
        .release_face_work(&face_id)
        .await
        .expect("release the unstarted work");
    assert_eq!(released, 1, "only the unstarted row is released");

    let row = backend
        .dag_get_task(&workspace, &pending)
        .await
        .expect("read the pending row")
        .expect("the pending row is still there");
    assert_eq!(row.owner_unit, None, "the row is offered again: {row:?}");
    assert_eq!(
        row.input.get("owner_unit"),
        None,
        "the goal must not still name a unit the queue never routed it from: {row:?}"
    );
    assert_eq!(
        row.input.get("goal").and_then(|v| v.as_str()),
        Some("hold the face"),
        "releasing must not rewrite the goal: {row:?}"
    );

    let running_row = backend
        .dag_get_task(&workspace, &running)
        .await
        .expect("read the running row")
        .expect("the running row is still there");
    assert_eq!(
        running_row.owner_unit.as_deref(),
        Some("gone-unit"),
        "a run in flight belongs to the session holding its lease: {running_row:?}"
    );

    assert_eq!(
        store
            .release_face_work(&face_id)
            .await
            .expect("a second pass"),
        0,
        "a row that already carries no owner is not rewritten every pass"
    );

    clean(&pool, &prefix).await;
}

/// A stale face revision rolls the whole transaction back: the handover row must
/// not read as applied while the face still says the old owner.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_handover_whose_face_moved_under_it_writes_nothing() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    let handover_id = format!("{prefix}:handover");

    store
        .save_face_node(&face_node(&face_id, "8b", 2))
        .await
        .expect("seed the face at revision 2");
    store
        .save_governance_node(&handover_node(
            &handover_id,
            &face_id,
            "8b",
            "6e",
            TaskStatus::Scheduled,
        ))
        .await
        .expect("seed the signed handover");

    let mut applied_handover =
        handover_node(&handover_id, &face_id, "8b", "6e", TaskStatus::Completed);
    applied_handover.bound_by_handover_id = Some(handover_id.clone());
    let moved_face = face_node(&face_id, "6e", 3);

    assert!(
        store
            .apply_handover(&applied_handover, &moved_face, 1)
            .await
            .expect("apply with a stale revision")
            .is_none(),
        "a handover that read revision 1 against a row at 2 must not apply"
    );

    let handover = store
        .governance_nodes(HANDOVER_TASK_TYPE)
        .await
        .expect("read the handovers")
        .into_iter()
        .find(|row| row.id == handover_id)
        .expect("the handover is still there");
    assert_eq!(
        handover.status,
        TaskStatus::Scheduled,
        "the handover row must not read as applied while the face did not move"
    );

    clean(&pool, &prefix).await;
}

/// A status move and the event that explains it are one write. A caller whose
/// expected status no longer holds gets `false` and leaves nothing behind; a
/// caller that wins leaves both rows. Written as two statements in either order
/// the window between them reads as a legal history of a move that never
/// happened (or a move with no history), and neither the chain check nor the
/// row read can tell.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_status_move_and_its_event_are_written_together_or_not_at_all() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");
    let store = PostgresGovernanceStore::new(pool.clone());

    let prefix = begin(&pool).await;
    let face_id = format!("{prefix}:face");
    let decision_id = format!("{prefix}:decision");
    let row_id = format!("{prefix}:handover");

    store
        .save_governance_node(&handover_node(
            &row_id,
            &face_id,
            "8b",
            "6e",
            TaskStatus::Pending,
        ))
        .await
        .expect("seed the row");
    let event = decision_event_task(&decision_id, 0, "opened", None, None, "", Utc::now());

    let losing_move = handover_node(&row_id, &face_id, "8b", "6e", TaskStatus::Completed);
    assert!(
        !store
            .save_governance_node_with_event(&event, &losing_move, &[TaskStatus::Running])
            .await
            .expect("write against a status the row is not in"),
        "a row that is not in the expected status must report the race, not write"
    );

    let rows = store
        .governance_nodes(HANDOVER_TASK_TYPE)
        .await
        .expect("read the rows");
    let row = rows
        .iter()
        .find(|row| row.id == row_id)
        .expect("the row is still there");
    assert_eq!(
        row.status,
        TaskStatus::Pending,
        "a losing write must not move the row"
    );
    assert!(
        store
            .governance_nodes(DECISION_EVENT_TASK_TYPE)
            .await
            .expect("read the events")
            .iter()
            .all(|event| event.id != cog_core::decision_event_id(&decision_id, 0)),
        "a losing write must not leave the event behind either"
    );

    let winning_move = handover_node(&row_id, &face_id, "8b", "6e", TaskStatus::Completed);
    assert!(
        store
            .save_governance_node_with_event(&event, &winning_move, &[TaskStatus::Pending])
            .await
            .expect("write against the status the row is in"),
        "the caller that read the current status must win"
    );

    let rows = store
        .governance_nodes(HANDOVER_TASK_TYPE)
        .await
        .expect("read the rows");
    let row = rows
        .iter()
        .find(|row| row.id == row_id)
        .expect("the row is still there");
    assert_eq!(row.status, TaskStatus::Completed, "the move must land");
    let events = store
        .governance_nodes(DECISION_EVENT_TASK_TYPE)
        .await
        .expect("read the events");
    assert!(
        events
            .iter()
            .any(|event| event.id == cog_core::decision_event_id(&decision_id, 0)),
        "the step that explains the move must land with it"
    );

    clean(&pool, &prefix).await;
}

/// A round that died before its cleanup leaves face nodes behind, and the
/// resident drift pass reads every face node in the graph: an abandoned face
/// is a face it keeps rewriting. The next round reclaims them.
///
/// The age is the discriminator, not the name: two rounds can be running at
/// once, and sweeping the whole family would delete the other one's fixtures
/// mid-assertion. A row whose marker is older than the abandon window cannot
/// belong to a round still in progress.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_round_that_died_before_its_cleanup_is_reclaimed_by_the_next_one() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");

    let long_ago = (Utc::now() - chrono::Duration::hours(1))
        .timestamp_nanos_opt()
        .unwrap_or_default();
    let abandoned = format!("governance-store-test:1:{long_ago}:face");
    let current = format!(
        "governance-store-test:{}:{}:face",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    for id in [&abandoned, &current] {
        backend
            .dag_set_task(GOVERNANCE_WORKSPACE, id, &face_node(id, "gone-unit", 1))
            .await
            .expect("seed a face");
    }

    sweep_abandoned(&pool).await;

    let left: Vec<(String,)> =
        sqlx::query_as("SELECT task_id FROM cog_dag_tasks WHERE task_id = ANY($1)")
            .bind(vec![abandoned.clone(), current.clone()])
            .fetch_all(&pool)
            .await
            .expect("read back what survived");
    let names: Vec<&str> = left.iter().map(|(id,)| id.as_str()).collect();
    assert!(
        !names.contains(&abandoned.as_str()),
        "an hour-old fixture is an abandoned round's leftover: {names:?}"
    );
    assert!(
        names.contains(&current.as_str()),
        "a fixture from this minute may belong to a round still running: {names:?}"
    );

    let prefix = marker();
    clean(&pool, &prefix).await;
    sqlx::query("DELETE FROM cog_dag_tasks WHERE task_id = $1")
        .bind(&current)
        .execute(&pool)
        .await
        .expect("clean up the fresh fixture");
}
