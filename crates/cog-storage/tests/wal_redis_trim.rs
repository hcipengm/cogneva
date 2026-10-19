//! What a capped agent log is, against a live Redis.
//!
//! The log is a list that is appended to for as long as an agent lives and is
//! never deleted, so a cap is the only thing bounding it. Turning one on
//! exposes a coupling the uncapped backend never had to face: it treated the
//! list position as the sequence number, and a trim moves every position. Left
//! that way, `read_since` would skip as many records as were dropped and
//! `next_seq` would hand out numbers already in the log — two defects that only
//! appear after the cap fires, which is to say only in production.
//!
//! Three things are checked here: a log past the cap keeps the newest records
//! and forgets the rest; what it keeps is read back by the sequence recorded in
//! each record rather than by position, so the oldest retained record is
//! returned for its own number; and the next sequence keeps climbing from the
//! newest record instead of falling back to the shortened length. A fourth test
//! covers the explicit cut, whose point has the same shape: it is found by
//! reading records, not by indexing the list.
//!
//! Ignored by default and needs `COGNEVA_TEST_REDIS_URL` pointing at a
//! throwaway Redis:
//!
//! ```text
//! COGNEVA_TEST_REDIS_URL=redis://127.0.0.1:6390 \
//!   cargo test -p cog-storage --test wal_redis_trim -- --ignored
//! ```
//!
//! Every test writes under a session id of its own and deletes it at the end,
//! so a run leaves the instance as it found it.

use cog_core::{WalBackend, WalEventType, WalRecord};
use cog_storage::wal::RedisWalBackend;

/// Small enough that twelve appends cross it.
const CAP: usize = 5;

fn redis_url() -> String {
    std::env::var("COGNEVA_TEST_REDIS_URL").expect(
        "set COGNEVA_TEST_REDIS_URL to a throwaway Redis before running this test; \
         it deletes only keys under its own per-run session ids",
    )
}

fn record(seq: u64, session: &str) -> WalRecord {
    WalRecord::new(
        seq,
        session,
        WalEventType::TurnStart,
        serde_json::json!({ "seq": seq }),
    )
}

fn session_id() -> String {
    format!("wal-trim-test-{}", uuid::Uuid::new_v4())
}

async fn length(backend_key: &str, session: &str) -> usize {
    let client = redis::Client::open(backend_key).expect("redis url");
    let mut conn = cog_redis::connect(&client).await.expect("connect");
    redis::cmd("LLEN")
        .arg(format!("wal:{session}"))
        .query_async::<usize>(&mut conn)
        .await
        .expect("LLEN")
}

async fn delete(backend_key: &str, session: &str) {
    let client = redis::Client::open(backend_key).expect("redis url");
    let mut conn = cog_redis::connect(&client).await.expect("connect");
    redis::cmd("DEL")
        .arg(format!("wal:{session}"))
        .query_async::<()>(&mut conn)
        .await
        .expect("DEL");
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_REDIS_URL pointing at a throwaway Redis"]
async fn a_log_past_the_cap_keeps_the_newest_records_and_their_own_numbers() {
    let url = redis_url();
    let session = session_id();
    let backend = RedisWalBackend::new(&url)
        .await
        .expect("open backend")
        .with_max_entries(CAP);

    for seq in 0..12u64 {
        backend.append(record(seq, &session)).await.expect("append");
    }

    assert_eq!(
        length(&url, &session).await,
        CAP,
        "the list is trimmed to the cap"
    );

    let kept = backend.read_since(&session, 0).await.expect("read_since");
    let seqs: Vec<u64> = kept.iter().map(|r| r.seq).collect();
    assert_eq!(
        seqs,
        (12 - CAP as u64..12).collect::<Vec<_>>(),
        "the newest records survive and the oldest are gone"
    );

    // The sequence survives the trim. Asked for the number of the oldest
    // retained record, the log answers with exactly that record — a backend
    // reading positions would have answered from the front of the list.
    let from_oldest = backend
        .read_since(&session, 12 - CAP as u64)
        .await
        .expect("read_since");
    assert_eq!(from_oldest.first().map(|r| r.seq), Some(12 - CAP as u64));

    // And the next number does not fall back to the shortened length.
    assert_eq!(
        backend.next_seq(&session).await.expect("next_seq"),
        12,
        "a trimmed log continues from the newest record's own number"
    );

    delete(&url, &session).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_REDIS_URL pointing at a throwaway Redis"]
async fn a_cut_lands_on_the_record_that_holds_the_requested_number() {
    let url = redis_url();
    let session = session_id();
    let backend = RedisWalBackend::new(&url).await.expect("open backend");

    for seq in 0..12u64 {
        backend.append(record(seq, &session)).await.expect("append");
    }
    backend
        .truncate_before(&session, 9)
        .await
        .expect("truncate_before");

    let kept = backend.read_since(&session, 0).await.expect("read_since");
    let seqs: Vec<u64> = kept.iter().map(|r| r.seq).collect();
    assert_eq!(
        seqs,
        (9..12).collect::<Vec<_>>(),
        "the cut is the record holding 9, not the position 9"
    );

    // A cut past the newest record leaves nothing behind at all.
    backend
        .truncate_before(&session, 50)
        .await
        .expect("truncate_before");
    assert_eq!(length(&url, &session).await, 0);
    assert_eq!(backend.next_seq(&session).await.expect("next_seq"), 0);

    delete(&url, &session).await;
}
