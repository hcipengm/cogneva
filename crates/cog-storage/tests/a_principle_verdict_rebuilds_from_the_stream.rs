//! 原则账的状态几格是流上的影子：删掉表，只读 `principle_judged` 事件能把它拼回来。
//!
//! 判两件事：同一条原则以最后一次判词为准（旧判词不会盖住新判词）；「只有人能判」
//! 这一档以 null 恢复，而不是恢复成上一条枚举值。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

mod common;

use cog_test_support::{database_url, own_schema, schema_pool};
use serde_json::json;

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_principle_verdict_rebuilds_from_the_stream() {
    let url = database_url();
    let schema = own_schema(&url, "principle_verdict_rebuild").await;
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

    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS principle_ledger (
            slug TEXT PRIMARY KEY,
            state TEXT,
            platform_gate TEXT,
            measured_at TIMESTAMPTZ,
            measured_rev TEXT
        )",
    )
    .execute(&pool)
    .await
    .expect("the principle ledger exists");

    // 同一条原则先被判成 enforced，后被判成 unenforced；另一条是「只有人能判」。
    for (i, (slug, state, gate, at)) in [
        (
            "p-one",
            Some("enforced"),
            Some("gate-a"),
            "2026-11-24T01:00:00Z",
        ),
        (
            "p-one",
            Some("unenforced"),
            Some("gate-b"),
            "2026-11-24T02:00:00Z",
        ),
        ("p-two", None, None, "2026-11-24T03:00:00Z"),
    ]
    .into_iter()
    .enumerate()
    {
        let attributes = json!({
            "state": state,
            "platform_gate": gate,
            "measured_rev": serde_json::Value::Null,
        })
        .to_string();
        sqlx::query(
            "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
                step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
                attributes, at) \
             VALUES ($1, 'principle_judged', 'principle', $2, $2, 'measured', 'ok', 1, $3, \
                     'principle-ledger', 'probe', 'principle', 1, $4, $5::timestamptz)",
        )
        .bind(format!("P{i:0>25}"))
        .bind(slug)
        .bind(format!("{slug}:judged:{i}"))
        .bind(attributes)
        .bind(at)
        .execute(&pool)
        .await
        .expect("the verdict event is written");
    }

    let written = cog_storage::rebuild_principle_states(&pool)
        .await
        .expect("the stream rebuilds the states");
    assert_eq!(written, 2, "两条原则各恢复一行");

    let one: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT state, platform_gate FROM principle_ledger WHERE slug = 'p-one'")
            .fetch_one(&pool)
            .await
            .expect("p-one is there");
    assert_eq!(one.0.as_deref(), Some("unenforced"), "最后一次判词说了算");
    assert_eq!(one.1.as_deref(), Some("gate-b"));

    let two: Option<String> =
        sqlx::query_scalar("SELECT state FROM principle_ledger WHERE slug = 'p-two'")
            .fetch_one(&pool)
            .await
            .expect("p-two is there");
    assert_eq!(two, None, "「只有人能判」恢复成 null，不是上一条枚举值");
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_principle_declaration_rebuilds_from_the_stream() {
    let url = database_url();
    let schema = own_schema(&url, "principle_declaration_rebuild").await;
    let pool = schema_pool(&url, &schema).await;
    common::apply_migration_files(
        &pool,
        &[
            "117_event_stream.up.sql",
            "119_event_payload_is_json.up.sql",
            "123_projection_snapshots.up.sql",
            "129_event_envelope_position.up.sql",
            "130_event_session_id.up.sql",
        ],
    )
    .await;

    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS principle_ledger_declared (
            slug TEXT NOT NULL,
            source TEXT NOT NULL,
            lead TEXT,
            carrier_path TEXT,
            carrier_sha256 TEXT,
            host_gate TEXT,
            where_red TEXT,
            check_kind TEXT,
            priority TEXT,
            declared_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            folded_at TIMESTAMPTZ,
            PRIMARY KEY (slug, source)
        )",
    )
    .execute(&pool)
    .await
    .expect("the declaration table exists");

    // 同一个 (slug, source) 声明两次：后一条的 lead 为准；另一条 slug 各一行。
    for (i, (slug, lead, at)) in [
        ("p-one", "旧的说法", "2026-11-24T01:00:00Z"),
        ("p-one", "新的说法", "2026-11-24T02:00:00Z"),
        ("p-two", "另一条", "2026-11-24T03:00:00Z"),
    ]
    .into_iter()
    .enumerate()
    {
        let attributes = json!({
            "slug": slug,
            "source": "list-principles.sh",
            "lead": lead,
            "carrier_path": "memory/x.md",
            "carrier_sha256": "abc",
            "host_gate": "gate-x",
            "where_red": "crates/x/tests/y.rs",
            "check_kind": "tree",
            "priority": "p1",
        })
        .to_string();
        sqlx::query(
            "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
                step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
                attributes, at) \
             VALUES ($1, 'principle_declared', 'principle', $2, $2, 'declared', 'ok', 1, $3, \
                     'principle-ledger', 'probe', 'principle', 1, $4, $5::timestamptz)",
        )
        .bind(format!("D{i:0>25}"))
        .bind(slug)
        .bind(format!("{slug}:declared:{i}"))
        .bind(attributes)
        .bind(at)
        .execute(&pool)
        .await
        .expect("the declaration event is written");
    }

    let written = cog_storage::rebuild_principle_declarations(&pool)
        .await
        .expect("the stream rebuilds the declarations");
    assert_eq!(written, 2, "两个 (slug, source) 各恢复一行");

    let lead: (String,) = sqlx::query_as(
        "SELECT lead FROM principle_ledger_declared WHERE slug = 'p-one' AND source = 'list-principles.sh'",
    )
    .fetch_one(&pool)
    .await
    .expect("the row reads back");
    assert_eq!(lead.0, "新的说法", "重建取的是最后一条声明");
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_second_rebuild_resumes_from_the_snapshot_watermark() {
    let url = database_url();
    let schema = own_schema(&url, "principle_verdict_incremental").await;
    let pool = schema_pool(&url, &schema).await;
    common::apply_migration_files(
        &pool,
        &[
            "117_event_stream.up.sql",
            "119_event_payload_is_json.up.sql",
            "123_projection_snapshots.up.sql",
            "129_event_envelope_position.up.sql",
            "130_event_session_id.up.sql",
        ],
    )
    .await;
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS principle_ledger (
            slug TEXT PRIMARY KEY,
            state TEXT,
            platform_gate TEXT,
            measured_at TIMESTAMPTZ,
            measured_rev TEXT
        )",
    )
    .execute(&pool)
    .await
    .expect("the principle ledger exists");

    let write = |i: i32, slug: &'static str, state: &'static str, at: &'static str| {
        let pool = pool.clone();
        async move {
            let attributes =
                json!({ "state": state, "platform_gate": null, "measured_rev": null }).to_string();
            sqlx::query(
                "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
                    step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
                    attributes, at) \
                 VALUES ($1, 'principle_judged', 'principle', $2, $2, 'measured', 'ok', 1, $3, \
                         'principle-ledger', 'probe', 'principle', 1, $4, $5::timestamptz)",
            )
            .bind(format!("P{i:0>25}"))
            .bind(slug)
            .bind(format!("{slug}:judged:{i}"))
            .bind(attributes)
            .bind(at)
            .execute(&pool)
            .await
            .expect("the verdict event is written");
        }
    };

    write(0, "p-one", "enforced", "2026-11-24T01:00:00Z").await;
    cog_storage::rebuild_principle_states(&pool)
        .await
        .expect("the first rebuild folds the stream");
    let watermark: (String,) = sqlx::query_as(
        "SELECT event_id FROM projection_snapshot WHERE projection = 'principle_ledger'",
    )
    .fetch_one(&pool)
    .await
    .expect("a watermark snapshot is recorded");
    assert_eq!(watermark.0, format!("P{:0>25}", 0));

    write(1, "p-one", "unenforced", "2026-11-24T02:00:00Z").await;
    cog_storage::rebuild_principle_states(&pool)
        .await
        .expect("the second rebuild resumes");
    let state: (Option<String>,) =
        sqlx::query_as("SELECT state FROM principle_ledger WHERE slug = 'p-one'")
            .fetch_one(&pool)
            .await
            .expect("the row reads back");
    assert_eq!(state.0.as_deref(), Some("unenforced"));
    let advanced: (String,) =
        sqlx::query_as("SELECT event_id FROM projection_snapshot WHERE projection = 'principle_ledger' ORDER BY taken_at DESC, event_id DESC LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("the watermark advanced");
    assert_eq!(advanced.0, format!("P{:0>25}", 1));
}
