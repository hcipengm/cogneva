//! The shared-database lease, against a live PostgreSQL.
//!
//! What these tests check is the part only a server can decide: that of two
//! asks landing on one role at the same instant exactly one is answered yes,
//! that a term nobody renews is taken over, and that the process displaced by
//! that takeover stops being told it may act. The statement's text is guarded
//! without a server in `cog_storage::owner_lease`; this is the half that has to
//! run a real one, and it is the half that holds the behaviour — that module's
//! own note says as much.
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test owner_lease -- --ignored
//! ```
//!
//! The tests share one database and run in parallel, so they contend over a
//! role named for these tests rather than over one a deployment holds, and each
//! clears it before and after. A live test pointed at a database someone else
//! is using must not touch a term belonging to a running process.

use std::time::Duration;

use sqlx::{PgPool, Row};

use cog_core::OwnerLeaseBroker;
use cog_storage::{PgOwnerLeaseBroker, LEASE_TABLE};

/// The role these tests contend over.
const ROLE: &str = "probe_owner_lease";

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

async fn connect() -> PgPool {
    PgPool::connect(&database_url()).await.unwrap()
}

/// Open a broker under a holder named here. Which is also what creates the
/// table, so a caller can clear its role afterwards without guarding for a
/// table that is not there.
async fn broker(pool: &PgPool, holder: &str) -> PgOwnerLeaseBroker {
    PgOwnerLeaseBroker::with_holder(pool.clone(), holder)
        .await
        .unwrap()
}

/// Clear the role, so a term left by an interrupted earlier run cannot decide
/// this one.
async fn clear_role(pool: &PgPool) {
    sqlx::query(&format!("DELETE FROM {LEASE_TABLE} WHERE role = $1"))
        .bind(ROLE)
        .execute(pool)
        .await
        .unwrap();
}

/// The role's row as anyone asking "who has this" reads it. `None` when no term
/// is written at all.
#[derive(Debug)]
struct WrittenTerm {
    holder: String,
    acquired_at: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
    expires_at: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
}

async fn written(pool: &PgPool) -> Option<WrittenTerm> {
    sqlx::query(&format!(
        "SELECT holder, acquired_at, expires_at FROM {LEASE_TABLE} WHERE role = $1"
    ))
    .bind(ROLE)
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(|row| WrittenTerm {
        holder: row.get("holder"),
        acquired_at: row.get("acquired_at"),
        expires_at: row.get("expires_at"),
    })
}

/// The holder `holders()` reports for the role, which is the reading an
/// operator takes rather than the one this process believes.
async fn reported_holder(broker: &PgOwnerLeaseBroker) -> Option<String> {
    broker
        .holders()
        .await
        .unwrap()
        .into_iter()
        .find(|(role, _)| role == ROLE)
        .map(|(_, holder)| holder)
}

/// End the term written for the role, which is the state the row is in once its
/// holder has stopped renewing it. Used where a test wants that state without
/// waiting out a real term.
async fn expire_role(pool: &PgPool) {
    sqlx::query(&format!(
        "UPDATE {LEASE_TABLE} SET expires_at = now() - interval '1 second' WHERE role = $1"
    ))
    .bind(ROLE)
    .execute(pool)
    .await
    .unwrap();
}

/// The event the lease exists for: two replicas starting at once, both asking
/// for the role in the same instant. Exactly one may be told yes.
///
/// This is the case a read followed by a write loses, and it is not
/// reproducible by asking twice in a row — the second ask would be answered from
/// the row the first one had already written.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn two_simultaneous_asks_leave_exactly_one_holder() {
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    let b = broker(&pool, "probe-b").await;
    clear_role(&pool).await;

    let ttl = Duration::from_secs(60);
    let lease_a = a.lease(ROLE, ttl);
    let lease_b = b.lease(ROLE, ttl);
    let (held_a, held_b) = tokio::join!(lease_a.try_hold(), lease_b.try_hold());
    let (held_a, held_b) = (held_a.unwrap(), held_b.unwrap());

    assert!(
        held_a ^ held_b,
        "of two simultaneous asks exactly one may hold the role, got a={held_a} b={held_b}"
    );
    let row = written(&pool)
        .await
        .expect("the winner's term must be written");
    let winner = if held_a { "probe-a" } else { "probe-b" };
    assert_eq!(row.holder, winner, "the row must name the process that won");

    clear_role(&pool).await;
}

/// A term that is live refuses everyone but its own holder, and the holder
/// asking again renews it rather than re-acquiring it.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_live_term_refuses_the_second_holder_and_renews_for_its_own() {
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    let b = broker(&pool, "probe-b").await;
    clear_role(&pool).await;

    let ttl = Duration::from_secs(60);
    let holder = a.lease(ROLE, ttl);
    assert!(holder.try_hold().await.unwrap(), "a free role is taken");
    let taken = written(&pool).await.unwrap();
    assert_eq!(taken.holder, "probe-a");

    assert!(
        !b.lease(ROLE, ttl).try_hold().await.unwrap(),
        "a role with a live term must be refused to a second holder"
    );
    assert_eq!(
        written(&pool).await.unwrap().holder,
        "probe-a",
        "a refused ask must leave the row as it was"
    );

    // The renewal has to move the term forward without re-acquiring it. A row
    // that rewrote `acquired_at` on every renewal would report a role changing
    // hands on every cycle, and an operator reading it could not tell a healthy
    // holder from a takeover.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        holder.try_hold().await.unwrap(),
        "the holder must be able to renew its own term"
    );
    let renewed = written(&pool).await.unwrap();
    assert_eq!(renewed.holder, "probe-a");
    assert!(
        renewed.expires_at > taken.expires_at,
        "the renewal must move the term forward"
    );
    assert_eq!(
        renewed.acquired_at, taken.acquired_at,
        "a renewal is not a new acquisition"
    );

    clear_role(&pool).await;
}

/// A holder that stops renewing — the pod was killed, the process wedged, the
/// node went away — loses the role to whoever asks next, and stops being told it
/// may act.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_term_that_stops_being_renewed_is_taken_over() {
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    let b = broker(&pool, "probe-b").await;
    clear_role(&pool).await;

    // Short enough that the wait below outlasts it, long enough that the two
    // asks before the wait cannot.
    let ttl = Duration::from_secs(1);
    assert!(a.lease(ROLE, ttl).try_hold().await.unwrap());
    assert!(!b.lease(ROLE, ttl).try_hold().await.unwrap());

    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert!(
        b.lease(ROLE, ttl).try_hold().await.unwrap(),
        "once the term has run out the role is free again"
    );
    assert_eq!(written(&pool).await.unwrap().holder, "probe-b");
    assert!(
        !a.lease(ROLE, ttl).try_hold().await.unwrap(),
        "the displaced holder must stop being told it may act"
    );

    clear_role(&pool).await;
}

/// The reading an operator takes — the table itself — says what the arbitration
/// did, and says nothing about a role no one has asked for.
///
/// An expired term is the one shape worth naming: the row stays, because the
/// last holder is a fact about the role's history even when the term is over,
/// and who held it last is exactly what an operator looking at a stolen role
/// needs to see. Liveness is a question asked of the term, not a condition for
/// the row existing.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_table_is_the_reading_of_who_holds_what() {
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    clear_role(&pool).await;

    assert_eq!(
        reported_holder(&a).await,
        None,
        "a role no one has asked for must not appear as held"
    );

    assert!(a
        .lease(ROLE, Duration::from_secs(60))
        .try_hold()
        .await
        .unwrap());
    assert_eq!(
        reported_holder(&a).await.as_deref(),
        Some("probe-a"),
        "the table is the reading, and it names the holder"
    );

    expire_role(&pool).await;
    assert_eq!(
        reported_holder(&a).await.as_deref(),
        Some("probe-a"),
        "an expired term is still a row: who held it last is a fact about the \
         role, and whether the term is live is a separate question"
    );

    clear_role(&pool).await;
}
