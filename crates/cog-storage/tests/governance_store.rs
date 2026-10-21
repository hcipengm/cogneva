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
//! Every id carries a per-run marker and every row is deleted at the end.

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
    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(pool)
        .await
        .expect("clean up");
    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id LIKE $2")
        .bind(GOVERNANCE_WORKSPACE)
        .bind(format!("{prefix}%"))
        .execute(pool)
        .await
        .expect("clean up");
    // Event ids carry the decision id in the middle (`decision-event:<id>:<seq>`),
    // so the marker is not a prefix there.
    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id LIKE $2")
        .bind(GOVERNANCE_WORKSPACE)
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

    let prefix = marker();
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

    let prefix = marker();
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

    let prefix = marker();
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

    let prefix = marker();
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

    let prefix = marker();
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

    let prefix = marker();
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
