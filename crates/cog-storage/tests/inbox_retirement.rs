//! principle: feedback_a_length_counts_history_not_work_on_a_store_that_never_trims
//!
//! Whether the inbox retirement can tell an inbox nobody owns from one that is
//! waiting to be read, against a live Redis.
//!
//! Every agent gets `orchestrator:agent:{id}:inbox` and nothing removed it when
//! the agent did. A live deployment held 12,571 of them, 12,568 empty; each is a
//! stream with its own metadata and group table, and the store is `noeviction`,
//! so that memory is memory the working set cannot have. The pass that removes
//! them is judged here on the two things it must never get wrong: an inbox that
//! still owes a reader a command is not a candidate, and an inbox that gains an
//! entry between the scan and the delete has just as much right to survive. The
//! other side of the same rule is judged too — an inbox its group read and
//! acknowledged is drained even though its entries are still on disk.
//!
//! ```text
//! COGNEVA_TEST_REDIS_URL=redis://:pw@127.0.0.1:16379 \
//!   cargo test -p cog-storage --test inbox_retirement -- --ignored
//! ```
//!
//! Every agent id here starts with [`PROBE`], which no deployment uses, so a run
//! pointed at a live Redis never touches an inbox an agent is still reading.

use chrono::Utc;

use cog_core::{AgentRegistration, AgentRegistry, ResourceInfo};
use cog_storage::inbox_retirement::InboxRetirement;
use cog_storage::RedisAgentRegistry;

/// Every agent this file creates starts here.
const PROBE: &str = "probe_inbox_retirement";

fn redis_url() -> String {
    std::env::var("COGNEVA_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

async fn raw_conn() -> redis::aio::ConnectionManager {
    let client = redis::Client::open(redis_url()).expect("redis url");
    cog_redis::connect(&client).await.expect("connection")
}

async fn pass() -> InboxRetirement {
    InboxRetirement::new(raw_conn().await)
}

/// Remove what a test made.
///
/// The pass itself will not: two of these streams end with a command in them, and
/// a stream with an entry is exactly what it refuses to touch. Cleaning up here
/// is what keeps a run against a live Redis from leaving a key nobody will ever
/// read and no sweep will ever take.
async fn clear(agent_id: &str) {
    let mut conn = raw_conn().await;
    let _: () = redis::cmd("DEL")
        .arg(cog_core::agent_inbox_stream(agent_id))
        .query_async(&mut conn)
        .await
        .expect("clear");
}

async fn key(agent_id: &str) -> String {
    let mut conn = raw_conn().await;
    clear(agent_id).await;
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(cog_core::agent_inbox_stream(agent_id))
        .arg(format!("agent-{agent_id}"))
        .arg("$")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await
        .expect("create the empty stream the way a consumer does");
    cog_core::agent_inbox_stream(agent_id)
}

async fn publish(agent_id: &str, body: &str) {
    let mut conn = raw_conn().await;
    let _: String = redis::cmd("XADD")
        .arg(cog_core::agent_inbox_stream(agent_id))
        .arg("*")
        .arg("type")
        .arg(body)
        .query_async(&mut conn)
        .await
        .expect("publish");
}

/// Take every entry the way the agent's own consumer does: read them with the
/// group, then acknowledge them. That is what moves an unowned inbox from owing
/// work to holding history.
async fn read_and_ack(agent_id: &str) -> usize {
    let k = cog_core::agent_inbox_stream(agent_id);
    let group = format!("agent-{agent_id}");
    let mut conn = raw_conn().await;
    let entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
        .arg(&k)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .expect("range");
    let _: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&group)
        .arg("probe")
        .arg("COUNT")
        .arg(100)
        .arg("STREAMS")
        .arg(&k)
        .arg(">")
        .query_async(&mut conn)
        .await
        .expect("read");
    for (id, _) in &entries {
        let _: i64 = redis::cmd("XACK")
            .arg(&k)
            .arg(&group)
            .arg(id)
            .query_async(&mut conn)
            .await
            .expect("ack");
    }
    entries.len()
}

async fn register(agent_id: &str) {
    let registry = RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("registry");
    let now = Utc::now();
    registry
        .register(&AgentRegistration {
            agent_id: agent_id.to_string(),
            role: "probe".into(),
            workspace_id: "probe".into(),
            capabilities: vec![],
            resources: ResourceInfo::default(),
            hostname: "probe".into(),
            pod_ip: "127.0.0.1".into(),
            registered_at: now,
            last_heartbeat: now,
        })
        .await
        .expect("register");
}

async fn deregister(agent_id: &str) {
    RedisAgentRegistry::from_url(&redis_url())
        .await
        .expect("registry")
        .deregister(agent_id)
        .await
        .expect("deregister");
}

/// The scan walks every inbox in the store, so it reports the whole population;
/// the assertions below are about this file's own probes.
fn find(candidates: &[(String, i64)], agent_id: &str) -> Option<i64> {
    candidates
        .iter()
        .find(|(k, _)| k == &cog_core::agent_inbox_stream(agent_id))
        .map(|(_, len)| *len)
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn an_empty_inbox_whose_agent_is_not_registered_is_a_candidate() {
    let agent = format!("{PROBE}-orphan");
    key(&agent).await;

    let found = find(
        &pass().await.orphaned_streams().await.expect("scan"),
        &agent,
    );
    assert_eq!(
        found,
        Some(0),
        "an empty inbox with no owner is a candidate, and its length comes with it"
    );
    clear(&agent).await;
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn an_inbox_whose_agent_is_registered_is_not_a_candidate() {
    let agent = format!("{PROBE}-owned");
    key(&agent).await;
    register(&agent).await;

    let found = find(
        &pass().await.orphaned_streams().await.expect("scan"),
        &agent,
    );
    assert_eq!(found, None, "a registered agent still owns its inbox");

    deregister(&agent).await;
    clear(&agent).await;
}

/// An entry is a command somebody was told had been queued. Empty is the whole
/// entry condition, so a single pending command is enough to keep the key.
#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn an_inbox_with_a_pending_command_is_not_a_candidate() {
    let agent = format!("{PROBE}-pending");
    key(&agent).await;
    publish(&agent, "prompt").await;

    let found = find(
        &pass().await.orphaned_streams().await.expect("scan"),
        &agent,
    );
    assert_eq!(
        found, None,
        "an inbox holding a command is never a candidate, owned or not"
    );
    clear(&agent).await;
}

/// The other half of "owes nothing": entries still on disk but taken and
/// acknowledged. Counting them as stranded work — which the reading did when it
/// counted `XLEN` — made a queue that did its job look like a leak, and kept the
/// key alive for a sweep that refused to touch a non-empty stream.
#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn an_inbox_its_group_drained_is_a_candidate_even_though_entries_remain() {
    let agent = format!("{PROBE}-drained");
    key(&agent).await;
    publish(&agent, "abort").await;
    publish(&agent, "steering").await;
    assert_eq!(
        read_and_ack(&agent).await,
        2,
        "both commands are read and acked, the way the owner's consumer did"
    );

    let scan = pass().await.scan().await.expect("scan");
    assert_eq!(
        find(&scan.empty, &agent),
        None,
        "the stream is not empty; the history is still on disk"
    );
    assert_eq!(
        find(&scan.holding, &agent),
        None,
        "and it owes no reader a thing"
    );
    assert_eq!(
        find(&scan.consumed, &agent),
        Some(2),
        "it is reported as drained, with the length it carries"
    );
    assert!(
        !scan
            .holding
            .iter()
            .any(|(k, _)| k == &cog_core::agent_inbox_stream(&agent)),
        "history a group already read is not stranded work"
    );

    let k = cog_core::agent_inbox_stream(&agent);
    let outcome = pass()
        .await
        .retire_streams(std::slice::from_ref(&k))
        .await
        .expect("retire");
    assert_eq!(
        (outcome.retired, outcome.records),
        (1, 2),
        "the drained stream is removed and the entries that went with it are reported"
    );
    clear(&agent).await;
}

#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn retiring_removes_the_stream_and_reports_it() {
    let agent = format!("{PROBE}-retired");
    let k = key(&agent).await;

    let outcome = pass()
        .await
        .retire_streams(std::slice::from_ref(&k))
        .await
        .expect("retire");
    assert_eq!(
        (outcome.retired, outcome.records),
        (1, 0),
        "the pass reports one stream and the zero entries it held"
    );
    assert_eq!(
        find(
            &pass().await.orphaned_streams().await.expect("scan"),
            &agent
        ),
        None,
        "the retired stream is gone rather than reported again on the next pass"
    );
    clear(&agent).await;
}

/// The scan and the delete are two round trips. A command published to an
/// unowned inbox in between is exactly how a message waits for an agent that is
/// starting up, and dropping it would be the pass destroying accepted work.
#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn a_stream_that_gained_a_command_after_the_scan_survives_the_delete() {
    let agent = format!("{PROBE}-raced");
    let k = key(&agent).await;

    assert_eq!(
        find(
            &pass().await.orphaned_streams().await.expect("scan"),
            &agent
        ),
        Some(0),
        "the probe is a candidate at the moment it is nominated"
    );
    publish(&agent, "prompt").await;

    let outcome = pass()
        .await
        .retire_streams(std::slice::from_ref(&k))
        .await
        .expect("retire");
    assert_eq!(
        (outcome.retired, outcome.records),
        (0, 0),
        "a stream that became non-empty is not retired, and nothing is claimed as removed"
    );
    assert_eq!(
        find(
            &pass().await.orphaned_streams().await.expect("scan"),
            &agent
        ),
        None,
        "the command is still in the stream, which is why the stream is no longer a candidate"
    );
    clear(&agent).await;
}

/// The half the pass refuses to remove is the half nobody could see.
///
/// An inbox whose agent is gone but which still holds commands is never
/// delivered, never retired and — before this reading — never counted: the
/// pass's output says only what it removed. The same walk that nominates the
/// empty half must report this one with its length, and it must stay out of the
/// retirement list, or the reading would turn into a delete.
#[tokio::test]
#[ignore = "needs a live Redis named by COGNEVA_TEST_REDIS_URL"]
async fn the_inboxes_retirement_refuses_are_reported_with_what_they_hold() {
    let agent = format!("{PROBE}-stranded");
    key(&agent).await;
    publish(&agent, "prompt").await;
    publish(&agent, "continue").await;

    let scan = pass().await.scan().await.expect("scan");
    let stream = cog_core::agent_inbox_stream(&agent);

    assert_eq!(
        find(&scan.empty, &agent),
        None,
        "a stream holding commands is not a retirement candidate"
    );
    assert_eq!(
        find(&scan.holding, &agent),
        Some(2),
        "and it is reported on the other half, with the number of commands it holds"
    );
    assert!(
        scan.held_entries() >= 2,
        "the entry count adds up what the per-stream lengths say: {}",
        scan.held_entries()
    );
    assert!(
        !pass()
            .await
            .orphaned_streams()
            .await
            .expect("scan")
            .iter()
            .any(|(k, _)| k == &stream),
        "the retirement list is still exactly the empty half"
    );
    clear(&agent).await;
}
