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
//! The tests share one database and run in parallel, so each contends over a
//! role named for itself rather than over one a deployment holds, and each
//! clears its own before and after. The role has to be per test, not per file:
//! a single role shared by tests run at the same time makes every one of them a
//! writer of the others' rows, and the readings here are exactly the ones a
//! sibling's write can decide -- a role nobody asked for reads as held, a term
//! that should have run out is renewed, an acquisition time comes from a row
//! another test wrote. Naming the role per case is what makes each assertion
//! about this test's own ask. The prefix is not a role any deployment holds: a
//! live test pointed at a database someone else is using must not touch a term
//! belonging to a running process.

use std::time::Duration;

use sqlx::{PgPool, Row};

use cog_core::OwnerLeaseBroker;
use cog_storage::{PgOwnerLeaseBroker, LEASE_TABLE};

/// The prefix every role in this file carries, so a leftover row from an
/// interrupted run is recognisable as one of these tests'.
const PROBE: &str = "probe_owner_lease";

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
async fn clear_role(pool: &PgPool, role: &str) {
    sqlx::query(&format!("DELETE FROM {LEASE_TABLE} WHERE role = $1"))
        .bind(role)
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

async fn written(pool: &PgPool, role: &str) -> Option<WrittenTerm> {
    sqlx::query(&format!(
        "SELECT holder, acquired_at, expires_at FROM {LEASE_TABLE} WHERE role = $1"
    ))
    .bind(role)
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
async fn reported_holder(broker: &PgOwnerLeaseBroker, role: &str) -> Option<String> {
    broker
        .holders()
        .await
        .unwrap()
        .into_iter()
        .find(|(name, _)| name == role)
        .map(|(_, holder)| holder)
}

/// End the term written for the role, which is the state the row is in once its
/// holder has stopped renewing it. Used where a test wants that state without
/// waiting out a real term.
async fn expire_role(pool: &PgPool, role: &str) {
    sqlx::query(&format!(
        "UPDATE {LEASE_TABLE} SET expires_at = now() - interval '1 second' WHERE role = $1"
    ))
    .bind(role)
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
    let role = format!("{PROBE}_simultaneous");
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    let b = broker(&pool, "probe-b").await;
    clear_role(&pool, &role).await;

    let ttl = Duration::from_secs(60);
    let lease_a = a.lease(&role, ttl);
    let lease_b = b.lease(&role, ttl);
    let (held_a, held_b) = tokio::join!(lease_a.try_hold(), lease_b.try_hold());
    let (held_a, held_b) = (held_a.unwrap(), held_b.unwrap());

    assert!(
        held_a ^ held_b,
        "of two simultaneous asks exactly one may hold the role, got a={held_a} b={held_b}"
    );
    let row = written(&pool, &role)
        .await
        .expect("the winner's term must be written");
    let winner = if held_a { "probe-a" } else { "probe-b" };
    assert_eq!(row.holder, winner, "the row must name the process that won");

    clear_role(&pool, &role).await;
}

/// A term that is live refuses everyone but its own holder, and the holder
/// asking again renews it rather than re-acquiring it.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_live_term_refuses_the_second_holder_and_renews_for_its_own() {
    let role = format!("{PROBE}_live_term");
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    let b = broker(&pool, "probe-b").await;
    clear_role(&pool, &role).await;

    let ttl = Duration::from_secs(60);
    let holder = a.lease(&role, ttl);
    assert!(holder.try_hold().await.unwrap(), "a free role is taken");
    let taken = written(&pool, &role).await.unwrap();
    assert_eq!(taken.holder, "probe-a");

    assert!(
        !b.lease(&role, ttl).try_hold().await.unwrap(),
        "a role with a live term must be refused to a second holder"
    );
    assert_eq!(
        written(&pool, &role).await.unwrap().holder,
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
    let renewed = written(&pool, &role).await.unwrap();
    assert_eq!(renewed.holder, "probe-a");
    assert!(
        renewed.expires_at > taken.expires_at,
        "the renewal must move the term forward"
    );
    assert_eq!(
        renewed.acquired_at, taken.acquired_at,
        "a renewal is not a new acquisition"
    );

    clear_role(&pool, &role).await;
}

/// A holder that stops renewing — the pod was killed, the process wedged, the
/// node went away — loses the role to whoever asks next, and stops being told it
/// may act.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_term_that_stops_being_renewed_is_taken_over() {
    let role = format!("{PROBE}_takeover");
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    let b = broker(&pool, "probe-b").await;
    clear_role(&pool, &role).await;

    // Short enough that the wait below outlasts it, long enough that the two
    // asks before the wait cannot.
    let ttl = Duration::from_secs(1);
    assert!(a.lease(&role, ttl).try_hold().await.unwrap());
    assert!(!b.lease(&role, ttl).try_hold().await.unwrap());

    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert!(
        b.lease(&role, ttl).try_hold().await.unwrap(),
        "once the term has run out the role is free again"
    );
    assert_eq!(written(&pool, &role).await.unwrap().holder, "probe-b");
    assert!(
        !a.lease(&role, ttl).try_hold().await.unwrap(),
        "the displaced holder must stop being told it may act"
    );

    clear_role(&pool, &role).await;
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
    let role = format!("{PROBE}_table");
    let pool = connect().await;
    let a = broker(&pool, "probe-a").await;
    clear_role(&pool, &role).await;

    assert_eq!(
        reported_holder(&a, &role).await,
        None,
        "a role no one has asked for must not appear as held"
    );

    assert!(a
        .lease(&role, Duration::from_secs(60))
        .try_hold()
        .await
        .unwrap());
    assert_eq!(
        reported_holder(&a, &role).await.as_deref(),
        Some("probe-a"),
        "the table is the reading, and it names the holder"
    );

    expire_role(&pool, &role).await;
    assert_eq!(
        reported_holder(&a, &role).await.as_deref(),
        Some("probe-a"),
        "an expired term is still a row: who held it last is a fact about the \
         role, and whether the term is live is a separate question"
    );

    clear_role(&pool, &role).await;
}

/// The read a writer outside the loops takes: the term still on the row, and
/// the same row once the term has run out.
///
/// This is the half the statement's text cannot carry. A row whose term ran out
/// has to stay readable as a row — "the holder stopped asking" is the reading a
/// takeover needs, and reporting it as absent would spell it the same way as a
/// role nobody ever asked for. And the remaining term has to be the database's
/// arithmetic: a reader that subtracted its own clock would report a different
/// term than the arbiter acts on.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_read_reports_the_term_and_keeps_a_spent_one_as_a_row() {
    let role = format!("{PROBE}_read");
    let pool = connect().await;
    let a = broker(&pool, "probe-read-a").await;
    let outside = broker(&pool, "probe-read-outside").await;
    clear_role(&pool, &role).await;

    assert_eq!(
        outside.read(&role).await.unwrap(),
        None,
        "a role no one has asked for has no row to read"
    );

    let ttl = Duration::from_secs(60);
    assert!(a.lease(&role, ttl).try_hold().await.unwrap());
    let seen = outside
        .read(&role)
        .await
        .unwrap()
        .expect("the holder's term is on the row");
    assert_eq!(seen.holder, "probe-read-a");
    assert!(
        seen.seconds_left > 0 && seen.seconds_left <= 60,
        "a term just written has time left on it, got {}",
        seen.seconds_left
    );

    // The state the out-of-band meets when the in-band has stood down: the term
    // is over, the row is not. Read before the takeover, because the takeover
    // is what overwrites it.
    expire_role(&pool, &role).await;
    let spent = outside
        .read(&role)
        .await
        .unwrap()
        .expect("a spent term is still a row, not an absent one");
    assert_eq!(spent.holder, "probe-read-a");
    assert!(
        spent.seconds_left <= 0,
        "a term that has run out must read as spent, got {}",
        spent.seconds_left
    );

    // And the arbiter agrees with what the reader was told: the free role goes
    // to the next asker.
    assert!(outside.lease(&role, ttl).try_hold().await.unwrap());

    clear_role(&pool, &role).await;
}
