//! 任务定义里的五格与任务租约各有一条流上的读法：删掉投影，这两问仍答得出来。
//! 默认忽略，需要测试库。

mod common;

use cog_test_support::{database_url, own_schema, schema_pool};

async fn stream_of(pool: &sqlx::PgPool) -> std::sync::Arc<dyn cog_core::EventStore> {
    std::sync::Arc::new(cog_storage::postgres::event_log::EventLogStore::new(
        pool.clone(),
        "probe",
    ))
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_definition_and_a_lease_read_back_from_the_stream() {
    let url = database_url();
    let schema = own_schema(&url, "two_facts").await;
    let pool = schema_pool(&url, &schema).await;
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
    let stream = stream_of(&pool).await;

    let item = cog_core::contract::ledger_item::LedgerItem {
        id: "S1".into(),
        terminal: "到了".into(),
        now: "此刻".into(),
        gap: String::new(),
        state: cog_core::contract::ledger_item::LedgerState::Reached,
    };
    // 五格随任务定义写进流：只写这一条，没有第二条 `item_defined`。
    let task = cog_core::Task::new("S1", cog_core::TaskType::DagNode, serde_json::json!({}))
        .with_cells(item.terminal.clone(), item.now.clone(), item.gap.clone());
    stream
        .append(cog_core::FactEvent::new(
            cog_core::EventPayload::TaskDefined {
                task: Box::new(task),
                rev: 1,
                workspace_id: "ws-1".into(),
            },
        ))
        .await
        .expect("the definition is written");

    let mut lease = cog_core::FactEvent::new(cog_core::EventPayload::TaskLeased {
        task_id: "t-1".into(),
        owner: "employee:7".into(),
        expires_at: chrono::Utc::now(),
        actor: "store".into(),
    });
    lease.event_id = Some("L0000000000000000000000001".into());
    stream.append(lease).await.expect("the lease is written");

    let read_item = cog_storage::item_definition(&pool, "S1")
        .await
        .expect("definition reads")
        .expect("definition exists");
    assert_eq!(read_item.terminal, "到了");
    assert!(read_item.gap_is_empty());

    // 读的是**最后一版**：读树的人更新现在与差距时写下一版定义（rev+1），
    // 最后一条折出此刻的五格。停在第一版等于把「后来改掉了」藏起来。
    let updated = cog_core::contract::ledger_item::LedgerItem {
        id: "S1".into(),
        terminal: "到了".into(),
        now: "又发现一处没接".into(),
        gap: "缺的是：最后一条门".into(),
        state: cog_core::contract::ledger_item::LedgerState::Open,
    };
    let revised = cog_core::Task::new("S1", cog_core::TaskType::DagNode, serde_json::json!({}))
        .with_cells(
            updated.terminal.clone(),
            updated.now.clone(),
            updated.gap.clone(),
        );
    stream
        .append(cog_core::FactEvent::new(
            cog_core::EventPayload::TaskDefined {
                task: Box::new(revised),
                rev: 2,
                workspace_id: "ws-1".into(),
            },
        ))
        .await
        .expect("the revision is written");
    let reread = cog_storage::item_definition(&pool, "S1")
        .await
        .expect("definition reads")
        .expect("definition exists");
    assert_eq!(reread.now, "又发现一处没接");
    assert_eq!(reread.gap, "缺的是：最后一条门");
    assert!(!reread.gap_is_empty());

    // 改终态是另一条事实（带理由）：只读定义会把修订前的判据答给读者。
    stream
        .append(cog_core::FactEvent::new(
            cog_core::EventPayload::TerminalRevised {
                item_id: "S1".into(),
                previous: "到了".into(),
                terminal: "到了，且门也在".into(),
                reason: "判据写宽了".into(),
                actor: "owner".into(),
            },
        ))
        .await
        .expect("the revision is written");
    let revised = cog_storage::item_definition(&pool, "S1")
        .await
        .expect("definition reads")
        .expect("definition exists");
    assert_eq!(revised.terminal, "到了，且门也在");
    assert_eq!(
        revised.state,
        cog_core::contract::ledger_item::LedgerState::Open,
        "第二版定义还写着差距，状态仍是未落"
    );

    // 状态取最后一条转移：完成但差距还在，仍读未落。
    stream
        .append(cog_core::FactEvent::new(
            cog_core::EventPayload::TaskTransition {
                task_id: "S1".into(),
                from: Some("running".into()),
                to: "completed".into(),
                actor: "worker".into(),
                cause: "done".into(),
                seq: 1,
            },
        ))
        .await
        .expect("the transition is written");
    let finished = cog_storage::item_definition(&pool, "S1")
        .await
        .expect("definition reads")
        .expect("definition exists");
    assert_eq!(
        finished.state,
        cog_core::contract::ledger_item::LedgerState::Open,
        "完成却还写着差距，不许冒充终态"
    );

    let read_lease = cog_storage::lease_from_stream(&pool, "t-1")
        .await
        .expect("lease reads")
        .expect("lease exists");
    assert_eq!(read_lease.owner, "employee:7");
    assert!(cog_storage::lease_from_stream(&pool, "none")
        .await
        .unwrap()
        .is_none());
}
