//! Maintenance against a live PostgreSQL.
//!
//! The interesting paths are the ones driven by the database: whether a month
//! really is open, and whether rows parked in a DEFAULT partition really come
//! back out when the partition covering them is created. Neither can be
//! checked without a server, so these tests are ignored by default and need
//! `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test partition_maintainer -- --ignored
//! ```
//!
//! The tables are created and dropped by the test itself, so the database is
//! left as it was found.

use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use sqlx::PgPool;

use cog_core::{RawLogIndexEntry, RawLogIndexStore, RawLogQuery, StorageTier};
use cog_storage::partition_maintainer::{PartitionMaintainer, PartitionedTable};
use cog_storage::PostgresRawLogIndexStore;

const TABLE: &str = "partition_maintainer_probe";

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// The probe is partitioned by `ts` and named so that its partition prefix
/// differs from its own name, exercising the prefix field.
fn probe_table() -> PartitionedTable {
    PartitionedTable::new(TABLE, "ts", "probe_part")
}

fn month_start(year: i32, month: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, 1).unwrap()
}

async fn fresh_probe(pool: &PgPool) {
    sqlx::query(&format!("DROP TABLE IF EXISTS {TABLE} CASCADE"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE {TABLE} (id BIGSERIAL, ts TIMESTAMPTZ NOT NULL, note TEXT) PARTITION BY RANGE (ts)"
    ))
    .execute(pool)
    .await
    .unwrap();
}

async fn drop_probe(pool: &PgPool) {
    sqlx::query(&format!("DROP TABLE IF EXISTS {TABLE} CASCADE"))
        .execute(pool)
        .await
        .unwrap();
}

async fn count_in(pool: &PgPool, relation: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {relation}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn insert_at(pool: &PgPool, ts: &str) {
    sqlx::query(&format!(
        "INSERT INTO {TABLE} (ts, note) VALUES ('{ts}'::timestamptz, 'probe')"
    ))
    .execute(pool)
    .await
    .unwrap();
}

fn maintainer(pool: PgPool) -> PartitionMaintainer {
    PartitionMaintainer::new(pool, vec![probe_table()])
}

#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn opens_the_current_month_and_a_default_partition() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool).await;

    let maintainer = maintainer(pool.clone());
    maintainer.maintain().await.unwrap();

    let today = Utc::now().date_naive();
    let current = format!("probe_part_y{}m{:02}", today.year(), today.month());
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)")
            .bind(&current)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(exists, "{current} should have been created");
    assert_eq!(count_in(&pool, "probe_part_default").await, 0);

    // A row for the current month routes into the month, not into DEFAULT.
    insert_at(&pool, &format!("{today} 12:00:00+00")).await;
    assert_eq!(count_in(&pool, &current).await, 1);
    assert_eq!(count_in(&pool, "probe_part_default").await, 0);

    // Running again changes nothing.
    maintainer.maintain().await.unwrap();
    assert_eq!(count_in(&pool, &current).await, 1);
    assert_eq!(count_in(&pool, "probe_part_default").await, 0);

    drop_probe(&pool).await;
}

#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn rows_parked_in_default_are_moved_when_their_partition_appears() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool).await;

    let maintainer = maintainer(pool.clone());
    maintainer.maintain().await.unwrap();

    // Pick a month inside the maintained window, then remove its partition so
    // the next insert has only DEFAULT to fall into.
    let target = Utc::now().date_naive() + chrono::Months::new(1);
    let name = format!("probe_part_y{}m{:02}", target.year(), target.month());
    sqlx::query(&format!("DROP TABLE {name}"))
        .execute(&pool)
        .await
        .unwrap();

    let start = month_start(target.year(), target.month());
    insert_at(&pool, &format!("{start} 12:00:00+00")).await;
    assert_eq!(count_in(&pool, "probe_part_default").await, 1);

    // Maintenance recreates the partition and relocates the row.
    maintainer.maintain().await.unwrap();
    assert_eq!(count_in(&pool, &name).await, 1);
    assert_eq!(count_in(&pool, "probe_part_default").await, 0);

    // And it stays put on the next round.
    maintainer.maintain().await.unwrap();
    assert_eq!(count_in(&pool, &name).await, 1);
    assert_eq!(count_in(&pool, "probe_part_default").await, 0);

    drop_probe(&pool).await;
}

/// The index store reads and writes the real `raw_log_index`, whose layout is
/// owned by the migrations. A column renamed in one place and not the other
/// fails only at runtime, so the round trip is checked here. Rows use
/// far-future dates so they land in the DEFAULT partition, and are deleted
/// again. `(stream_name, log_date)` is the key, so each row needs its own date.
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn raw_log_index_round_trips_every_tier() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = PostgresRawLogIndexStore::new(pool.clone());
    let created_at = Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap();
    let rows = [
        (1u32, 3u8, StorageTier::Hot),
        (2, 4, StorageTier::Warm),
        (3, 5, StorageTier::Cold),
    ];
    let first_date = NaiveDate::from_ymd_opt(2099, 1, 1).unwrap();
    let last_date = NaiveDate::from_ymd_opt(2099, 1, 3).unwrap();

    for (day, hour, tier) in rows {
        store
            .upsert(RawLogIndexEntry {
                hour,
                stream_name: "system_raw".into(),
                log_date: NaiveDate::from_ymd_opt(2099, 1, day).unwrap(),
                file_path: format!("/probe/{hour}"),
                tier,
                size_bytes: 1024,
                event_count: 7,
                checksum: format!("probe-{hour}"),
                start_time: created_at,
                end_time: created_at,
                created_at,
            })
            .await
            .unwrap();
    }

    // Scoped to this test's dates so a populated database cannot skew counts.
    let window = || RawLogQuery {
        stream: Some("system_raw".into()),
        start: Some(Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap()),
        end: Some(Utc.with_ymd_and_hms(2099, 1, 4, 0, 0, 0).unwrap()),
        ..Default::default()
    };

    let found = store.query(&window()).await.unwrap();
    assert_eq!(found.len(), 3, "the rows just written should be readable");
    for (_, hour, tier) in rows {
        let back = found
            .iter()
            .find(|e| e.hour == hour)
            .unwrap_or_else(|| panic!("hour {hour} missing"));
        assert_eq!(
            back.tier, tier,
            "tier {tier:?} did not survive the round trip"
        );
        assert_eq!(back.file_path, format!("/probe/{hour}"));
        assert_eq!(back.event_count, 7);
    }

    // Each optional filter narrows rather than shadowing the others.
    let by_hour = store
        .query(&RawLogQuery {
            hour: Some(5),
            ..window()
        })
        .await
        .unwrap();
    assert_eq!(by_hour.len(), 1);
    assert_eq!(by_hour[0].hour, 5);

    let by_tier = store
        .query(&RawLogQuery {
            tier: Some(StorageTier::Warm),
            ..window()
        })
        .await
        .unwrap();
    assert_eq!(by_tier.len(), 1, "only the warm row should match");
    assert_eq!(by_tier[0].hour, 4);

    let limited = store
        .query(&RawLogQuery {
            limit: Some(2),
            ..window()
        })
        .await
        .unwrap();
    assert_eq!(limited.len(), 2);

    sqlx::query("DELETE FROM raw_log_index WHERE log_date BETWEEN $1 AND $2")
        .bind(first_date)
        .bind(last_date)
        .execute(&pool)
        .await
        .unwrap();
}
