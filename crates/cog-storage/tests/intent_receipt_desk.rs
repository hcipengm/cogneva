//! 收件台与 planner 的读者问的是同一条身份：真 PostgreSQL 上问三件事。
//!
//! - **写进去的读得回来**：一句从对话进来的意图落成一行 `intent_received`，原文与
//!   身份逐字相同；
//! - **重投不写第二条**：同一句话再进来一次，流里仍是一行——「收到过没有」由库回答；
//! - **派生了就不在收件台上**：任务行带着 `input.intent_id`（协作那一侧写字时用的正是
//!   这个键）之后，同一条意图不再出现在「未派生」里。
//!
//! 第三条是这整条路存在的理由：没有它，planner 的每一次调用都会把同一条意图再分解
//! 一遍，而分解出来的待办会越积越多。两处读的是同一个键（`INTENT_ID_KEY`），所以
//! 这里写的任务行走的是产品自己的任务存储（`dag_set_task`），不是手写的 INSERT——手写
//! 的那一份只能证明这条 SQL 认得自己。
//!
//! 默认忽略，要 `COGNEVA_TEST_DATABASE_URL`。

use cog_core::contract::intent_events::{IntentReader, IntentRow};
use cog_core::{IntentEvents, StateBackend, Task, TaskType};
use cog_storage::postgres::intent_events::IntentEventStore;

const WORKSPACE: &str = "probe-intent-desk";

/// 事件流那张表由迁移建；任务那一侧的表由 `init_schema` 建。两处都是产品自己的
/// DDL——这里不手抄一份列清单，手抄的那份只能证明这条 SQL 认得自己。
async fn apply_event_stream_migration(pool: &sqlx::PgPool) {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/postgres");
    for file in [
        "117_event_stream.up.sql",
        "119_event_payload_is_json.up.sql",
        // 收件台读的那几格（`intent_source` 等）与 `task_defined.intent_id`
        // 是这一版加上来的生成列；不应用它，读的还是载荷。
        "127_intent_columns.up.sql",
    ] {
        let sql =
            std::fs::read_to_string(dir.join(file)).unwrap_or_else(|e| panic!("read {file}: {e}"));
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("apply {file}: {e}"));
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_intent_stops_being_undispatched_once_a_task_carries_its_identity() {
    let url = cog_test_support::database_url();
    let schema = cog_test_support::own_schema(&url, "intent_receipt_desk").await;
    let pool = cog_test_support::schema_pool(&url, &schema).await;
    let backend = cog_storage::PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("任务那一侧的表建出来");
    apply_event_stream_migration(&pool).await;

    let desk = IntentEventStore::new(pool.clone(), "probe-intent-desk");
    let row = IntentRow {
        source: "webui".into(),
        external_id: "session-7:3f2a".into(),
        text: "把编不过的那三个 crate 修好".into(),
        parent: String::new(),
    };
    let identity = row.identity();

    desk.record(&row, "webui").await.expect("意图落成一行");
    desk.record(&row, "webui").await.expect("重投不该报错");

    let open = desk.undispatched(64).await.expect("读收件台");
    assert_eq!(open.len(), 1, "同一句意图重投之后收件台上不止一条：{open:?}");
    assert_eq!(open[0].identity(), identity);
    assert_eq!(open[0].text, row.text, "读回来的原文与写进去的不是同一段");

    // 派生的那一条任务：输入的写法与协作那一侧一致（`INTENT_ID_KEY`）。
    let mut task = Task::new(identity.clone(), TaskType::DagNode, serde_json::json!({}));
    task.input
        .as_object_mut()
        .expect("input 是一个对象")
        .insert(
            cog_core::contract::intent_events::INTENT_ID_KEY.to_string(),
            serde_json::Value::String(identity.clone()),
        );
    backend
        .dag_set_task(WORKSPACE, &identity, &task)
        .await
        .expect("派生的任务写进任务存储");

    let after = desk.undispatched(64).await.expect("再读一次收件台");
    assert!(
        after.is_empty(),
        "任务行已经带着 {identity} 的意图身份，收件台上却还有：{after:?}"
    );

    // 另一条意图不受影响：收件台是按身份逐条回答的，不是「有任务就全清」。
    let other = IntentRow {
        source: "webui".into(),
        external_id: "session-7:9c11".into(),
        text: "再看一眼告警面".into(),
        parent: String::new(),
    };
    desk.record(&other, "webui").await.expect("第二条意图");
    let rest = desk.undispatched(64).await.expect("读剩下的");
    assert_eq!(rest.len(), 1, "派生一条任务把别的意图也清掉了：{rest:?}");
    assert_eq!(rest[0].text, other.text);
}
