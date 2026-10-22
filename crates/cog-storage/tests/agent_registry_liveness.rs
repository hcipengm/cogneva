//! Whether the agent registry tells "registered" apart from "still there",
//! against a live Redis.
//!
//! The registry answers two different questions through one key: who holds a
//! name (`get` / `list`), and whether the process behind it is still beating.
//! The key's expiry was the only thing carrying the second one, so a record
//! that lost its expiry -- written by `SET` and then `EXPIRE`, with the writer
//! dying in between -- stayed addressable forever. A live cluster held twenty
//! of those, three days cold, listed as agents that could be sent work.
//!
//! ```text
//! COGNEVA_TEST_REDIS_URL=redis://:pw@127.0.0.1:16379 \
//!   cargo test -p cog-storage --features redis --test agent_registry_liveness -- --ignored
//! ```
//!
//! The names carry a prefix no deployment uses: a run pointed at a live Redis
//! must not touch a registration a running process holds.

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use redis::AsyncCommands;

use cog_core::{generate_agent_id, AgentRegistration, AgentRegistry, ResourceInfo};
use cog_storage::RedisAgentRegistry;

/// Every name in this file starts here, so a leftover from an interrupted run
/// is recognisable as one of these tests'.
const PROBE: &str = "probe_agent_registry_liveness";

fn redis_url() -> String {
    std::env::var("COGNEVA_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

fn registration(name: &str, beaten_at: chrono::DateTime<Utc>) -> AgentRegistration {
    AgentRegistration {
        agent_id: format!("{PROBE}:{name}"),
        role: "probe".into(),
        workspace_id: "probe".into(),
        capabilities: vec![],
        resources: ResourceInfo::default(),
        hostname: "probe".into(),
        pod_ip: "127.0.0.1".into(),
        registered_at: beaten_at,
        last_heartbeat: beaten_at,
    }
}

async fn registry() -> RedisAgentRegistry {
    RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("connect to the Redis named by COGNEVA_TEST_REDIS_URL")
}

/// Write a record the way a writer that died between `SET` and `EXPIRE` leaves
/// it: no expiry at all, and whatever beat time the payload happens to hold.
async fn plant_without_expiry(reg: &AgentRegistration) {
    let client = redis::Client::open(redis_url()).expect("redis url");
    // The same self-healing, budget-bounded connect every other caller uses:
    // building the manager straight from the driver leaves its retry delays
    // uncapped, so a probe aimed at an address that is not there waits minutes
    // before it can say so.
    let mut conn = cog_redis::connect(&client).await.expect("connection");
    let payload = serde_json::to_string(reg).expect("serialize");
    let key = format!("orchestrator:agents:{}", reg.agent_id);
    let _: () = conn.set(&key, payload).await.expect("planted record");
    let _: () = conn
        .sadd("orchestrator:agents:index", &reg.agent_id)
        .await
        .expect("indexed");
}

async fn ttl_secs(agent_id: &str) -> i64 {
    let client = redis::Client::open(redis_url()).expect("redis url");
    let mut conn = cog_redis::connect(&client).await.expect("connection");
    conn.ttl(format!("orchestrator:agents:{agent_id}"))
        .await
        .expect("ttl")
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_registered_agent_is_live_and_carries_its_own_expiry() {
    let registry = registry().await;
    let reg = registration("live", Utc::now());
    registry.register(&reg).await.expect("register");

    let read = registry.get(&reg.agent_id).await.expect("get");
    assert_eq!(
        read.map(|r| r.agent_id),
        Some(reg.agent_id.clone()),
        "a freshly registered agent is live"
    );
    // The expiry has to arrive with the write, not after it: a reader that
    // finds the key but no TTL cannot tell a live agent from a record whose
    // writer died mid-registration. `-1` is exactly that state.
    let ttl = ttl_secs(&reg.agent_id).await;
    assert!(
        ttl > 0,
        "registration must carry a TTL, got {ttl} (-1 = no expiry, -2 = no key)"
    );
    assert!(
        registry
            .list()
            .await
            .expect("list")
            .iter()
            .any(|r| r.agent_id == reg.agent_id),
        "a live registration is listed"
    );

    registry
        .deregister(&reg.agent_id)
        .await
        .expect("deregister");
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_record_whose_writer_stopped_beating_is_not_live() {
    let registry = registry().await;
    let cold = Utc::now() - ChronoDuration::seconds(3 * 24 * 60 * 60);
    let reg = registration("cold", cold);
    plant_without_expiry(&reg).await;
    assert_eq!(
        ttl_secs(&reg.agent_id).await,
        -1,
        "the planted record has no expiry, which is the whole defect"
    );

    assert_eq!(
        registry.get(&reg.agent_id).await.expect("get"),
        None,
        "a record whose last heartbeat is days old is absent, whatever its key says"
    );
    assert_eq!(
        ttl_secs(&reg.agent_id).await,
        -2,
        "the absent record is dropped rather than left to be re-read"
    );
    // The index is the roster `list` walks, so an id left there is the same
    // dead name arriving through the other door.
    assert!(
        !registry
            .list()
            .await
            .expect("list")
            .iter()
            .any(|r| r.agent_id == reg.agent_id),
        "a record that was not evidence of life is not on the roster"
    );
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_fresh_record_is_live_even_before_its_expiry_is_rearmed() {
    let registry = registry().await;
    // The crash window leaves the beat time current: the writer died *after*
    // writing the payload, so the record still describes an agent that was
    // alive a moment ago. Calling it absent would retire a working agent on
    // the strength of a missing expiry it cannot observe.
    let reg = registration("fresh", Utc::now());
    plant_without_expiry(&reg).await;

    assert!(
        registry.get(&reg.agent_id).await.expect("get").is_some(),
        "a current heartbeat is evidence of life on its own"
    );

    // One beat is what re-arms it: the record goes back to carrying its own
    // expiry, so the bound above is only ever the fallback.
    registry.heartbeat(&reg.agent_id).await.expect("heartbeat");
    assert!(
        ttl_secs(&reg.agent_id).await > 0,
        "a heartbeat re-arms the expiry of a record that lost it"
    );

    registry
        .deregister(&reg.agent_id)
        .await
        .expect("deregister");
}

/// Plant a record with whatever expiry the writer chose, the way `register`
/// and the operator presence heartbeat do (`SET ... EX`).
async fn plant_with_expiry(reg: &AgentRegistration, seconds: u64) {
    let client = redis::Client::open(redis_url()).expect("redis url");
    // Bounded like the other plant helper: a probe must not spend minutes on an
    // address that is not there.
    let mut conn = cog_redis::connect(&client).await.expect("connection");
    let payload = serde_json::to_string(reg).expect("serialize");
    let key = format!("orchestrator:agents:{}", reg.agent_id);
    let _: () = conn
        .set_ex(&key, payload, seconds)
        .await
        .expect("planted record");
    let _: () = conn
        .sadd("orchestrator:agents:index", &reg.agent_id)
        .await
        .expect("indexed");
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_record_written_for_a_long_window_is_judged_by_its_own_window() {
    // The operator presence heartbeat writes with a window measured in
    // minutes; agents register with one measured in seconds. The reader used
    // to apply its own (agent) window to both and delete the slow-beating
    // record well before the writer said it was over -- which is why a face
    // whose owner is present flipped to `unknown_unit` between two beats.
    let registry = registry().await; // default registration window: 30s
    let slow = Utc::now() - ChronoDuration::seconds(300);
    let reg = registration("long_window", slow);
    plant_with_expiry(&reg, 900).await;

    assert_eq!(
        registry.get(&reg.agent_id).await.expect("get").map(|r| r.agent_id),
        Some(reg.agent_id.clone()),
        "a record that still has minutes left on its own expiry is live, however little the reader's window is"
    );
    assert!(
        ttl_secs(&reg.agent_id).await > 0,
        "the reader must not delete a record whose writer declared it alive"
    );

    registry
        .deregister(&reg.agent_id)
        .await
        .expect("deregister");
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_heartbeat_keeps_an_agent_live_past_its_first_expiry() {
    let registry = registry().await;
    let mut reg = registration("beating", Utc::now());
    reg.agent_id = format!(
        "{PROBE}:{}",
        generate_agent_id("probe", "127.0.0.1", "probe", "beating")
    );
    registry.register(&reg).await.expect("register");

    tokio::time::sleep(Duration::from_millis(1200)).await;
    registry.heartbeat(&reg.agent_id).await.expect("heartbeat");

    let read = registry.get(&reg.agent_id).await.expect("get");
    assert_eq!(
        read.map(|r| r.agent_id),
        Some(reg.agent_id.clone()),
        "a beaten record stays live"
    );
    assert!(
        ttl_secs(&reg.agent_id).await > 0,
        "the beat re-armed the TTL"
    );

    registry
        .deregister(&reg.agent_id)
        .await
        .expect("deregister");
}
