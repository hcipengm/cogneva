//! The agent conversation's storage, against a live PostgreSQL.
//!
//! Two facts here are the database's rather than the caller's: that a
//! conversation is read from both sides (an exchange between two agents belongs
//! to both of their histories, and reading only the receiving side would show a
//! reader half a dialogue), and that the retention bound is per receiving peer
//! rather than over the table, so a chatty peer cannot push another
//! conversation out of reach.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test agent_message_store -- --ignored
//! ```
//!
//! Every row this test writes carries a per-run peer name and is deleted at the
//! end.

use sqlx::PgPool;

use cog_storage::{AgentMessageStore, NewAgentMessage, PostgresAgentMessageStore, SendOutcome};

use cog_test_support::database_url;

/// Names no other run shares, so the trim bound is measured on this test's rows
/// alone.
struct Probe {
    store: PostgresAgentMessageStore,
    pool: PgPool,
    left: String,
    right: String,
}

async fn probe() -> Probe {
    let url = database_url();
    migrate_once(&url).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the throwaway database");
    let run = uuid::Uuid::new_v4();
    Probe {
        store: PostgresAgentMessageStore::new(pool.clone()),
        pool,
        left: format!("probe-left-{run}"),
        right: format!("probe-right-{run}"),
    }
}

/// Apply the migrations under a session advisory lock, so concurrent test
/// binaries do not race on the same DDL.
async fn migrate_once(url: &str) {
    let pool = PgPool::connect(url)
        .await
        .expect("connect to take the lock");
    let mut conn = pool.acquire().await.expect("acquire the schema lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(97_004_i64)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    cog_storage::migrate::Migrator::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .run(url)
        .await
        .expect("apply the migrations to the throwaway database");
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(97_004_i64)
        .execute(&mut *conn)
        .await
        .expect("release the schema lock");
}

async fn cleanup(probe: &Probe) {
    sqlx::query("DELETE FROM agent_messages WHERE from_unit = $1 OR to_unit = $1 OR to_unit = $2")
        .bind(&probe.left)
        .bind(&probe.right)
        .execute(&probe.pool)
        .await
        .expect("delete the rows this test wrote");
}

/// A pair of peers, read from the side that did not send.
#[tokio::test]
#[ignore]
async fn a_conversation_is_read_from_both_sides() {
    let probe = probe().await;
    probe
        .store
        .record(&NewAgentMessage {
            from_unit: &probe.left,
            to_unit: &probe.right,
            kind: "prompt",
            body: "run the tests",
            outcome: SendOutcome::Replied,
            reply: Some("all green"),
            outcome_detail: None,
        })
        .await
        .expect("record a send");
    probe
        .store
        .record(&NewAgentMessage {
            from_unit: &probe.right,
            to_unit: &probe.left,
            kind: "continue",
            body: "now the release build",
            outcome: SendOutcome::Queued,
            reply: None,
            outcome_detail: None,
        })
        .await
        .expect("record the answer's follow-up");

    let from_right = probe
        .store
        .conversation(&probe.right, 50)
        .await
        .expect("read the addressee's side");
    assert_eq!(
        from_right.len(),
        2,
        "the addressee must see both the message and its own reply: {from_right:?}"
    );
    assert_eq!(from_right[0].body, "run the tests");
    assert_eq!(from_right[1].body, "now the release build");

    let from_left = probe
        .store
        .conversation(&probe.left, 50)
        .await
        .expect("read the sender's side");
    assert_eq!(
        from_left.len(),
        2,
        "the sender must see the exchange it took part in: {from_left:?}"
    );
    assert_eq!(from_left[0].reply.as_deref(), Some("all green"));

    cleanup(&probe).await;
}

/// The bound is per receiving peer: a peer with a hundred sends must not be
/// able to push another peer's lines out of the window.
#[tokio::test]
#[ignore]
async fn the_bound_applies_per_receiving_peer() {
    let probe = probe().await;
    probe
        .store
        .record(&NewAgentMessage {
            from_unit: &probe.left,
            to_unit: &probe.right,
            kind: "prompt",
            body: "the one that must survive",
            outcome: SendOutcome::Queued,
            reply: None,
            outcome_detail: None,
        })
        .await
        .expect("record the line under test");
    let noisy = format!("{}-noisy", probe.left);
    for _ in 0..cog_storage::ROWS_KEPT_PER_PEER + 10 {
        probe
            .store
            .record(&NewAgentMessage {
                from_unit: &noisy,
                to_unit: &noisy,
                kind: "continue",
                body: "noise",
                outcome: SendOutcome::Queued,
                reply: None,
                outcome_detail: None,
            })
            .await
            .expect("record noise");
    }
    let kept = probe
        .store
        .conversation(&probe.right, 10)
        .await
        .expect("read the quiet peer's conversation");
    assert_eq!(kept.len(), 1, "the quiet peer's window was pushed out");
    assert_eq!(kept[0].body, "the one that must survive");

    sqlx::query("DELETE FROM agent_messages WHERE from_unit = $1")
        .bind(&noisy)
        .execute(&probe.pool)
        .await
        .expect("delete the noise");
    cleanup(&probe).await;
}
