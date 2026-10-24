//! The dataset catalog, against a live PostgreSQL.
//!
//! What is checked here cannot be checked in memory: that the unique constraint
//! really makes a version mean one thing, that a nullable row count stays
//! absent rather than becoming zero, and that the ordering a restore depends on
//! (newest version first) is the database's, not the caller's.
//!
//! Ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test dataset_catalog -- --ignored
//! ```

use chrono::{TimeZone, Utc};
use sqlx::PgPool;

use cog_core::SFError;
use cog_storage::{DatasetCatalog, NewDataset, PostgresDatasetCatalog};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before running these \
         tests",
    )
}

struct Probe {
    catalog: PostgresDatasetCatalog,
    pool: PgPool,
    /// A dataset name no other run and no other test shares.
    name: String,
}

async fn probe() -> Probe {
    let url = database_url();
    migrate_once(&url).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the throwaway database");
    let name = format!("probe-dataset-{}", uuid::Uuid::new_v4());
    Probe {
        catalog: PostgresDatasetCatalog::new(pool.clone()),
        pool,
        name,
    }
}

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
    sqlx::query("DELETE FROM datasets WHERE name = $1")
        .bind(&probe.name)
        .execute(&probe.pool)
        .await
        .expect("delete the rows this test wrote");
}

fn row(probe: &Probe, version: &str, digest_byte: char) -> NewDataset {
    NewDataset {
        name: probe.name.clone(),
        version: version.to_string(),
        object_key: format!("datasets/{}/{version}/sha256-x", probe.name),
        content_sha256: format!("sha256:{}", digest_byte.to_string().repeat(64)),
        bytes: 1024,
        row_count: None,
        coverage_from: None,
        coverage_to: None,
        source: "crawler:arxiv".into(),
        media_type: Some("application/pdf".into()),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_version_reads_back_exactly_as_it_was_registered() {
    let probe = probe().await;
    let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let to = Utc.with_ymd_and_hms(2026, 6, 30, 0, 0, 0).unwrap();
    let mut entry = row(&probe, "v1", 'a');
    entry.row_count = Some(1_234);
    entry.coverage_from = Some(from);
    entry.coverage_to = Some(to);
    entry.media_type = Some("text/csv".into());

    let written = probe
        .catalog
        .register(entry.clone())
        .await
        .expect("register");
    assert_eq!(written.bytes, 1024);
    assert_eq!(written.row_count, Some(1_234));
    assert_eq!(written.coverage_from, Some(from));
    assert_eq!(written.coverage_to, Some(to));
    assert_eq!(written.media_type.as_deref(), Some("text/csv"));
    assert_eq!(written.source, "crawler:arxiv");

    let read = probe
        .catalog
        .find(&probe.name, "v1")
        .await
        .expect("find")
        .expect("the row that was just written");
    assert_eq!(read, written, "读回来的行和写进去的不是同一行");

    cleanup(&probe).await;
}

/// A blob has no row count and no coverage, and the catalog must keep those
/// absent instead of filling in zero and an epoch.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn what_a_blob_cannot_answer_stays_absent() {
    let probe = probe().await;
    probe
        .catalog
        .register(row(&probe, "blob-v1", 'b'))
        .await
        .expect("register");
    let read = probe
        .catalog
        .find(&probe.name, "blob-v1")
        .await
        .expect("find")
        .expect("row");
    assert_eq!(read.row_count, None, "不可数的那一格被填成了 0");
    assert_eq!(read.coverage_from, None);
    assert_eq!(read.coverage_to, None);
    cleanup(&probe).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn one_version_holds_one_content_and_the_same_content_registers_twice_as_one_row() {
    let probe = probe().await;
    let first = probe
        .catalog
        .register(row(&probe, "v1", 'c'))
        .await
        .expect("register");
    let again = probe
        .catalog
        .register(row(&probe, "v1", 'c'))
        .await
        .expect("re-registering the same bytes");
    assert_eq!(first, again, "同样的内容登记两次成了两行");

    let refused = probe.catalog.register(row(&probe, "v1", 'd')).await;
    assert!(
        matches!(refused, Err(SFError::Validation(_))),
        "一个版本被两组字节占用必须被拒：{refused:?}"
    );

    let versions = probe.catalog.list(&probe.name, 10).await.expect("list");
    assert_eq!(
        versions.len(),
        1,
        "同一个版本在账上有不止一行：{versions:?}"
    );
    assert_eq!(versions[0].content_sha256, first.content_sha256);
    cleanup(&probe).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_newest_version_is_the_latest_and_listing_walks_back_from_it() {
    let probe = probe().await;
    for (version, digest) in [("v1", 'e'), ("v2", 'f'), ("v3", '0')] {
        probe
            .catalog
            .register(row(&probe, version, digest))
            .await
            .expect("register");
    }

    let latest = probe
        .catalog
        .latest(&probe.name)
        .await
        .expect("latest")
        .expect("three versions were written");
    assert_eq!(latest.version, "v3", "最新的版本读错了：{}", latest.version);

    let versions = probe.catalog.list(&probe.name, 10).await.expect("list");
    let seen: Vec<&str> = versions.iter().map(|v| v.version.as_str()).collect();
    assert_eq!(seen, vec!["v3", "v2", "v1"], "版本的先后顺序不对");

    let zero = probe.catalog.list(&probe.name, 0).await.expect("limit 0");
    assert!(zero.is_empty(), "limit 0 读成了「不限」：{zero:?}");
    cleanup(&probe).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_half_interval_or_an_empty_name_is_refused_rather_than_stored() {
    let probe = probe().await;
    let mut half = row(&probe, "half", '1');
    half.coverage_from = Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap());
    let refused = probe.catalog.register(half).await;
    assert!(
        matches!(refused, Err(SFError::Validation(_))),
        "只给了一个端点的覆盖区间必须被拒：{refused:?}"
    );

    let mut unnamed = row(&probe, "unnamed", '2');
    unnamed.name = "   ".into();
    let refused = probe.catalog.register(unnamed).await;
    assert!(
        matches!(refused, Err(SFError::Validation(_))),
        "没有名字的数据集必须被拒：{refused:?}"
    );

    let versions = probe.catalog.list(&probe.name, 10).await.expect("list");
    assert!(versions.is_empty(), "被拒的两行有一行落库了：{versions:?}");
    cleanup(&probe).await;
}

/// A migration asks the whole catalog, not one dataset at a time: "did they all
/// come across" is a different question from "did this one".
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn every_dataset_walks_out_newest_first() {
    let probe = probe().await;
    probe
        .catalog
        .register(row(&probe, "v1", '3'))
        .await
        .expect("register");
    probe
        .catalog
        .register(row(&probe, "v2", '4'))
        .await
        .expect("register");

    let all = probe.catalog.all(10).await.expect("walk every dataset");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].version, "v2", "最新的不在最前：{:?}", all);
    let zero = probe.catalog.all(0).await.expect("limit 0");
    assert!(zero.is_empty(), "limit 0 读成了「不限」：{zero:?}");
    cleanup(&probe).await;
}
