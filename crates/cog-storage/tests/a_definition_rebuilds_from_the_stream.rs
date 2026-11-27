//! 定义账是流上的影子：删掉 `task_defined`，只读 `task_defined` 事件能把它拼回来。
//!
//! 判三件事：一版定义一条事件；删表之后重建的行数与删之前相同；重建出来的关键格
//! （任务、版本、工作区、种类、输入、验收、目标）与写进去的那一版一致。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

mod common;

use cog_test_support::{database_url, own_schema, schema_pool};
use serde_json::json;

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_task_definition_rebuilds_from_the_stream() {
    let url = database_url();
    let schema = own_schema(&url, "definition_rebuild").await;
    let pool = schema_pool(&url, &schema).await;
    common::apply_migration_files(
        &pool,
        &[
            "117_event_stream.up.sql",
            "119_event_payload_is_json.up.sql",
            "130_event_session_id.up.sql",
        ],
    )
    .await;

    // 表由 state backend 的 DDL 建；这里只建这一族要读的那张。
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS task_defined (
            task_id TEXT NOT NULL,
            rev BIGINT NOT NULL,
            at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            workspace_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            input TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 0,
            declared_duration_seconds BIGINT NOT NULL DEFAULT 0,
            is_executable BOOLEAN NOT NULL DEFAULT TRUE,
            acceptance TEXT[] NOT NULL DEFAULT '{}',
            blocked_by TEXT[] NOT NULL DEFAULT '{}',
            agent_id TEXT, goal_id TEXT, parent_task_id TEXT, sink_task_id TEXT,
            face_id TEXT, venue TEXT, owner_unit TEXT, bound_by_handover_id TEXT,
            submitted_by TEXT, planner_verified BOOLEAN, planner_version TEXT,
            planner_note TEXT, planner_source TEXT, planner_confidence DOUBLE PRECISION,
            planner_at TIMESTAMPTZ,
            PRIMARY KEY (task_id, rev)
        )",
    )
    .execute(&pool)
    .await
    .expect("the definition table exists");

    let mut task = cog_core::Task::new(
        "probe-def-1",
        cog_core::TaskType::Custom("probe".into()),
        json!({ "task": "把那一处改掉" }),
    );
    task.acceptance = vec!["那一处不再出现".into()];
    task.goal_id = Some("goal-1".into());
    task.submitted_by = Some("employee:1".into());
    task.agent_id = Some("employee:7".into());
    let attributes = json!({ "rev": 1, "workspace_id": "ws-1", "task": task }).to_string();

    sqlx::query(
        "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
            step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
            attributes, at) \
         VALUES ($1, 'task_defined', 'task', $2, $2, 'defined', 'ok', 1, $3, 'employee:1', \
                 'probe', 'task', 1, $4, NOW())",
    )
    .bind("D0000000000000000000000001")
    .bind("probe-def-1")
    .bind("probe-def-1:defined:1")
    .bind(attributes)
    .execute(&pool)
    .await
    .expect("the definition event is written");

    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_defined")
        .fetch_one(&pool)
        .await
        .expect("count before");
    assert_eq!(before, 0, "这一行还没有进过表");

    let written = cog_storage::rebuild_task_definitions(&pool, None)
        .await
        .expect("the stream rebuilds the table");
    assert_eq!(written, 1, "一版定义应当重建出一行");

    let row: (
        String,
        i64,
        String,
        String,
        String,
        Vec<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT task_id, rev, workspace_id, kind, input, acceptance, goal_id \
               FROM task_defined",
    )
    .fetch_one(&pool)
    .await
    .expect("the rebuilt row is there");
    assert_eq!(row.0, "probe-def-1");
    assert_eq!(row.1, 1);
    assert_eq!(row.2, "ws-1");
    assert_eq!(row.3, "probe", "种类由任务的那一版原样带回");
    assert!(
        row.4.contains("把那一处改掉"),
        "输入没有逐字回来：{}",
        row.4
    );
    assert_eq!(row.5, vec!["那一处不再出现".to_string()]);
    assert_eq!(row.6.as_deref(), Some("goal-1"));
}
