//! 待办的三格随任务定义落成真列，读法折的是最后一条定义。
//!
//! 没有这次改动会红的两件事：
//!
//! - `task_defined` 上没有 `terminal`／`now`／`gap` 三列时，写进去的定义只留下
//!   `input` 里的副本，按列读不到；
//! - 读法只认一条定义时，第二版写了新差距仍读回第一版。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test task_definition_carries_the_five_cells -- --ignored
//! ```

use cog_core::contract::ledger_item::{LedgerItem, LedgerState};
use cog_core::{StateBackend, Task, TaskType};
use cog_storage::{item_definition, PostgresStateBackend};
use sqlx::PgPool;

use cog_test_support::{database_url, own_schema, schema_pool};

mod common;

const WORKSPACE: &str = "ws-cells";

fn cells(terminal: &str, now: &str, gap: &str) -> LedgerItem {
    LedgerItem {
        id: "cell-task".into(),
        terminal: terminal.into(),
        now: now.into(),
        gap: gap.into(),
        state: LedgerState::Open,
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_definition_writes_its_cells_as_columns_and_the_reader_folds_the_last_one() {
    let url = database_url();
    let schema = own_schema(&url, "task_cells").await;
    let pool: PgPool = schema_pool(&url, &schema).await;
    // 事件流由迁移建；任务表由 store 的启动批次建（与全新安装同一份形状）。
    common::apply_migration_files(
        &pool,
        &[
            "117_event_stream.up.sql",
            "119_event_payload_is_json.up.sql",
            "129_event_envelope_position.up.sql",
            "130_event_session_id.up.sql",
        ],
    )
    .await;
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");

    let first = cells("到终态", "此刻", "缺的是：接线");
    let task = Task::new("cell-task", TaskType::DagNode, serde_json::json!({})).with_cells(
        first.terminal.clone(),
        first.now.clone(),
        first.gap.clone(),
    );
    backend
        .dag_set_task(WORKSPACE, "cell-task", &task)
        .await
        .expect("write the first definition");

    let columns: (String, String, String) = sqlx::query_as(
        "SELECT terminal, now, gap FROM task_defined \
          WHERE task_id = 'cell-task' ORDER BY rev DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("the cells are columns on the definition");
    assert_eq!(
        columns,
        ("到终态".into(), "此刻".into(), "缺的是：接线".into())
    );
    // 字段是正本：写下去的这一份不再留输入里的副本。
    let input: String = sqlx::query_scalar(
        "SELECT input FROM task_defined WHERE task_id = 'cell-task' ORDER BY rev DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("the input column reads");
    assert!(
        !input.contains("ledger_item"),
        "写下去的定义不该再带输入副本：{input}"
    );

    // 读树的人改「现在／差距」：下一版定义带着新的两句，最后一条胜出。
    let revised = cells("到终态", "又发现一处没接", "缺的是：最后一条门");
    let task = Task::new("cell-task", TaskType::DagNode, serde_json::json!({})).with_cells(
        revised.terminal.clone(),
        revised.now.clone(),
        revised.gap.clone(),
    );
    backend
        .dag_set_task(WORKSPACE, "cell-task", &task)
        .await
        .expect("write the revised definition");

    let read = item_definition(&pool, "cell-task")
        .await
        .expect("the definition reads")
        .expect("the item exists");
    assert_eq!(read.now, "又发现一处没接", "读的是最后一版定义");
    assert_eq!(read.gap, "缺的是：最后一条门");
    assert_eq!(read.terminal, "到终态");
    assert_eq!(
        read.state,
        LedgerState::Open,
        "任务还没完成，差距还在：未落"
    );

    // 旧入口只把三格放在输入里：落列之前会被提到字段上，按列仍读得到。
    let mut legacy = cells("旧的到终态", "旧入口", "缺的是：没提格");
    legacy.id = "legacy-task".into();
    let mut old = Task::new(
        "legacy-task",
        TaskType::DagNode,
        serde_json::json!({
            cog_core::contract::ledger_item::ITEM_INPUT_KEY: serde_json::to_value(&legacy)
                .expect("the cells serialize"),
        }),
    );
    old.terminal = String::new();
    old.now = String::new();
    old.gap = String::new();
    backend
        .dag_set_task(WORKSPACE, "legacy-task", &old)
        .await
        .expect("write the legacy definition");
    let lifted: (String, String, String) = sqlx::query_as(
        "SELECT terminal, now, gap FROM task_defined \
          WHERE task_id = 'legacy-task' ORDER BY rev DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("the legacy cells are lifted before they land");
    assert_eq!(
        lifted,
        (
            "旧的到终态".into(),
            "旧入口".into(),
            "缺的是：没提格".into()
        )
    );
}
