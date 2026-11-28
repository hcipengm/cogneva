//! 名册是员工事件的投影：只读 `employee_number_allocated` / `employee_retired`
//! 两条事实，能把名册的每一行拼回来（工号、人、墓碑）。
//!
//! 默认忽略：要一个真 PG（事件流）与一个一次性 Redis（名册）。
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://… COGNEVA_TEST_REDIS_URL=redis://… \
//!   cargo test -p cog-storage --test a_roster_rebuilds_from_the_stream -- --ignored
//! ```

mod common;

use cog_core::AgentRegistry;
use cog_storage::RedisAgentRegistry;
use cog_test_support::{database_url, own_schema, redis_url, schema_pool};

const PROBE: &str = "probe_roster_rebuild";

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL and COGNEVA_TEST_REDIS_URL"]
async fn the_roster_rebuilds_from_the_employee_facts() {
    let url = database_url();
    let schema = own_schema(&url, "roster_rebuild").await;
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

    let employee = format!("{PROBE}:employee:7");
    for (i, (kind, attributes, at)) in [
        (
            "employee_number_allocated",
            serde_json::json!({"number": 7, "person": employee}),
            "2026-11-24T01:00:00Z",
        ),
        (
            "employee_retired",
            serde_json::json!({"at": "2026-11-24T02:00:00Z"}),
            "2026-11-24T02:00:00Z",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query(
            "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
                step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
                attributes, at) \
             VALUES ($1, $2, 'employee', $3, $3, 'ok', 'ok', 1, $4, $3, 'probe', 'employee', 1, \
                     $5, $6::timestamptz)",
        )
        .bind(format!("R{i:0>25}"))
        .bind(kind)
        .bind(&employee)
        .bind(format!("{employee}:{kind}:{i}"))
        .bind(attributes.to_string())
        .bind(at)
        .execute(&pool)
        .await
        .expect("the employee fact is written");
    }

    let registry = RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("a throwaway Redis is required: set COGNEVA_TEST_REDIS_URL");
    let written = registry
        .rebuild_roster(&pool)
        .await
        .expect("the stream rebuilds the roster");
    assert!(written >= 1, "至少这一行该被重建出来");

    let row = registry
        .employee(&employee)
        .await
        .expect("the roster is readable")
        .expect("the rebuilt row is there");
    assert_eq!(row.number, Some(7), "工号由发号那一条带回");
    assert_eq!(
        cog_core::person_id_for(&row.agent_id),
        Some(employee.as_str()),
        "重建之后读到的人仍是它自己；这一格不再重复存"
    );
    assert!(row.retired_at.is_some(), "离场那一条要盖墓碑");
}
