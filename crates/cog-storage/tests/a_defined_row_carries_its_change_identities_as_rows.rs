//! A dispatched change identity is a **row** in `task_change_identity`, not a key
//! buried in the task's input text.
//!
//! The two-hop reconciliation (change ledger ↔ dispatched work) asks by identity:
//! kind, scope, literal. Reading those out of `task_defined.input` makes every
//! reader parse text on every beat, and a misspelled key answers NULL instead of
//! failing — the same bug as a missing column, silently. So the writer settles the
//! identities once, in the same statement that writes the definition row, and the
//! reader joins on columns.
//!
//! Two shapes reach this writer:
//!   * `change_identities` — the list a handover carries (one or many);
//!   * `change_identity` — the single identity older writers left behind. When both
//!     are present the list is the whole truth; when only the single one is, it is
//!     one row. Dropping that case would make the side table thinner than the text
//!     reader ever was (15 live rows), and the ledger side would read them as never
//!     handed over.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test a_defined_row_carries_its_change_identities_as_rows -- --ignored
//! ```

use chrono::Utc;
use cog_core::{StateBackend, Task, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

fn task(id: &str, input: serde_json::Value) -> Task {
    Task::new(id, TaskType::DagNode, input)
}

async fn rows(pool: &PgPool, id: &str) -> Vec<(i32, String, String, String)> {
    sqlx::query_as(
        "SELECT ordinal, kind, scope, literal FROM task_change_identity \
          WHERE task_id = $1 ORDER BY ordinal",
    )
    .bind(id)
    .fetch_all(pool)
    .await
    .expect("read the side table")
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_defined_row_carries_every_identity_and_the_old_single_shape_too() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");

    let marker = format!(
        "change-identity-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let workspace = format!("{marker}:ws");

    let plural = format!("{marker}:plural");
    backend
        .dag_set_task(
            &workspace,
            &plural,
            &task(
                &plural,
                serde_json::json!({
                    "change_identities": [
                        {"kind": "criterion", "scope": "a.rs", "literal": "L1"},
                        {"kind": "capability", "scope": "b.rs", "literal": "L2"},
                    ],
                    "change_identity": {"kind": "criterion", "scope": "a.rs", "literal": "L1"},
                }),
            ),
        )
        .await
        .expect("write the definition row");
    assert_eq!(
        rows(&pool, &plural).await,
        vec![
            (0, "criterion".into(), "a.rs".into(), "L1".into()),
            (1, "capability".into(), "b.rs".into(), "L2".into()),
        ],
        "the list is the whole truth: one row per identity, in order"
    );

    let single = format!("{marker}:single");
    backend
        .dag_set_task(
            &workspace,
            &single,
            &task(
                &single,
                serde_json::json!({
                    "change_identity": {"kind": "criterion", "scope": "a.rs", "literal": "L1"},
                }),
            ),
        )
        .await
        .expect("write the old single shape");
    assert_eq!(
        rows(&pool, &single).await,
        vec![(0, "criterion".into(), "a.rs".into(), "L1".into())],
        "a task that only carries the old single shape still gets its row"
    );

    let none = format!("{marker}:none");
    backend
        .dag_set_task(
            &workspace,
            &none,
            &task(&none, serde_json::json!({"goal": "unrelated"})),
        )
        .await
        .expect("write a definition row without identities");
    assert!(
        rows(&pool, &none).await.is_empty(),
        "a task that carries no identity gets no virtual row"
    );
}
