//! Whether the agent log store can tell a log nobody owns from one still in
//! use, against a live Redis.
//!
//! A `wal:{session}` list has exactly one appender -- the agent whose id is the
//! session -- and its registration is renewed by that agent's heartbeat. The
//! cap sweep bounds how large a log may grow; it does not remove any. So the
//! store grew with the number of sessions ever run, not the number alive: a
//! live deployment held 3,833 logs and 1,241,597 records in 560 MB, nearly all
//! of them belonging to agents that had long since retired.
//!
//! ```text
//! COGNEVA_TEST_REDIS_URL=redis://:pw@127.0.0.1:16379 \
//!   cargo test -p cog-storage --test wal_retirement -- --ignored
//! ```
//!
//! Every session name here starts with [`PROBE`], which no deployment uses, so
//! a run pointed at a live Redis never touches a log an agent is still writing.

use chrono::Utc;

use cog_core::wal::{WalEventType, WalRecord};
use cog_core::{AgentRegistration, AgentRegistry, ResourceInfo};
use cog_storage::wal::RedisWalBackend;
use cog_storage::RedisAgentRegistry;

/// Every session this file creates starts here.
const PROBE: &str = "probe_wal_retirement";

fn redis_url() -> String {
    std::env::var("COGNEVA_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

async fn backend() -> RedisWalBackend {
    let client = redis::Client::open(redis_url()).expect("redis url");
    let conn = cog_redis::connect(&client)
        .await
        .expect("connect to the Redis named by COGNEVA_TEST_REDIS_URL");
    RedisWalBackend::from_connection(conn)
}

async fn raw_conn() -> redis::aio::ConnectionManager {
    let client = redis::Client::open(redis_url()).expect("redis url");
    cog_redis::connect(&client).await.expect("connection")
}

fn record(session: &str, seq: u64) -> WalRecord {
    WalRecord {
        seq,
        session_id: session.to_string(),
        event_type: WalEventType::TurnStart,
        payload: serde_json::json!({"probe": true}),
        timestamp: Utc::now(),
        checksum: None,
    }
}

/// Write `count` records the way the appender does, so the length the scan
/// reports is a length the real store would have.
async fn plant(session: &str, count: usize) {
    let mut conn = raw_conn().await;
    let key = format!("wal:{session}");
    for seq in 0..count as u64 {
        let line = record(session, seq)
            .encode_to_json_line()
            .expect("encode record");
        let _: () = redis::cmd("RPUSH")
            .arg(&key)
            .arg(line)
            .query_async(&mut conn)
            .await
            .expect("planted record");
    }
}

async fn register(session: &str) {
    let registry = RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("registry");
    let now = Utc::now();
    let reg = AgentRegistration {
        agent_id: session.to_string(),
        role: "probe".into(),
        workspace_id: "probe".into(),
        capabilities: vec![],
        resources: ResourceInfo::default(),
        hostname: "probe".into(),
        pod_ip: "127.0.0.1".into(),
        registered_at: now,
        last_heartbeat: now,
    };
    registry.register(&reg).await.expect("register");
}

/// The scan walks every `wal:*` key in the store, so it reports the whole
/// population; the assertions below are about this file's two probes, and a
/// live cluster's other logs are none of their business.
fn find(candidates: &[(String, i64)], session: &str) -> Option<i64> {
    candidates
        .iter()
        .find(|(key, _)| key == &format!("wal:{session}"))
        .map(|(_, len)| *len)
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_log_whose_session_lost_its_registration_is_an_orphan() {
    let backend = backend().await;
    let session = format!("{PROBE}:orphan");
    plant(&session, 3).await;

    let candidates = backend.orphaned_logs().await.expect("scan");
    assert_eq!(
        find(&candidates, &session),
        Some(3),
        "a log with no live session is an orphan, and the scan reports how long it is"
    );
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_log_whose_session_is_registered_is_not_an_orphan() {
    let backend = backend().await;
    let session = format!("{PROBE}:owned");
    plant(&session, 2).await;
    register(&session).await;

    let candidates = backend.orphaned_logs().await.expect("scan");
    assert_eq!(
        find(&candidates, &session),
        None,
        "a session that is still registered still owns its log"
    );

    RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("registry")
        .deregister(&session)
        .await
        .expect("deregister");
}

/// The scan and the delete are two round trips. A task re-dispatched to the
/// same role recreates the same agent id — the name comes from the task and the
/// role, not from the process — and the first thing it does is register, then
/// replay. If that registration lands between the scan and the delete, the key
/// nominated as an orphan now has an owner, and deleting it takes the tail the
/// resume came back for.
#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_log_whose_session_registered_after_the_scan_survives_the_delete() {
    let backend = backend().await;
    let session = format!("{PROBE}:reowned");
    plant(&session, 2).await;
    let key = format!("wal:{session}");

    // The scan's answer, taken while the name was still free.
    assert_eq!(
        find(&backend.orphaned_logs().await.expect("scan"), &session),
        Some(2),
        "the probe starts out as an orphan, which is what nominates it"
    );
    // Then the owner comes back before the pass acts on that answer.
    register(&session).await;

    let outcome = backend
        .retire_logs(std::slice::from_ref(&key))
        .await
        .expect("retire");
    assert_eq!(
        (outcome.retired, outcome.records),
        (0, 0),
        "a key that gained an owner is not retired, and the outcome says so"
    );
    assert_eq!(
        find(&backend.orphaned_logs().await.expect("scan"), &session),
        None,
        "the log is still there and still owned"
    );

    RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("registry")
        .deregister(&session)
        .await
        .expect("deregister");
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn retiring_a_log_reports_what_it_held_and_removes_it() {
    let backend = backend().await;
    let session = format!("{PROBE}:retired");
    plant(&session, 4).await;
    let key = format!("wal:{session}");

    let outcome = backend
        .retire_logs(std::slice::from_ref(&key))
        .await
        .expect("retire");
    assert_eq!(
        (outcome.retired, outcome.records),
        (1, 4),
        "the pass reports one log and the four records it held, not just that it ran"
    );
    assert_eq!(
        find(&backend.orphaned_logs().await.expect("scan"), &session),
        None,
        "the retired log is gone rather than reported again on the next pass"
    );
}
