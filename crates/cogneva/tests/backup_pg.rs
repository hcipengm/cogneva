//! Backup/restore round trip against a live PostgreSQL.
//!
//! COPY routing into partitioned parents, sequence restoration and row-level
//! content equality can only be checked against a real server, so this test is
//! ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cogneva --test backup_pg -- --ignored
//! ```
//!
//! The probe tables are created and dropped by the test itself, so the
//! database is left as it was found. The session role toggle in restore
//! requires a superuser; both the in-cluster database (POSTGRES_USER) and the
//! CI postgres image satisfy this.

use sqlx::PgPool;

use cogneva::backup::{backup_postgres, discover_tables, restore_postgres};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

const PARENT: &str = "backup_probe_events";
const PLAIN: &str = "backup_probe_plain";

async fn fresh_schema(pool: &PgPool) {
    for stmt in [
        format!("DROP TABLE IF EXISTS {PARENT} CASCADE"),
        format!("DROP TABLE IF EXISTS {PLAIN} CASCADE"),
        format!(
            "CREATE TABLE {PARENT} (id BIGSERIAL, ts TIMESTAMPTZ NOT NULL, note TEXT) \
             PARTITION BY RANGE (ts)"
        ),
        // 一个过去月分区 + DEFAULT：恢复路径必须证明 COPY 进父表会按 ts 路由，
        // 落在 DEFAULT 的行也完整回来。
        format!(
            "CREATE TABLE {PARENT}_y2026m08 PARTITION OF {PARENT} \
             FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')"
        ),
        format!("CREATE TABLE {PARENT}_default PARTITION OF {PARENT} DEFAULT"),
        format!("CREATE TABLE {PLAIN} (id BIGSERIAL PRIMARY KEY, label TEXT)"),
    ] {
        sqlx::query(&stmt).execute(pool).await.unwrap();
    }
}

async fn count_in(pool: &PgPool, relation: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {relation}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn pg_backup_restore_round_trips_rows_partitions_and_sequences() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_schema(&pool).await;

    sqlx::query(&format!(
        "INSERT INTO {PARENT} (ts, note) VALUES \
         ('2026-08-15 12:00:00+00', 'august row'), \
         ('2026-08-20 12:00:00+00', 'another august row'), \
         ('2027-01-01 00:00:00+00', 'default partition row')"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {PLAIN} (label) VALUES ('alpha'), ('beta')"
    ))
    .execute(&pool)
    .await
    .unwrap();

    // 表发现必须包含分区父表、排除子表。
    let tables = discover_tables(&pool).await.unwrap();
    assert!(tables.contains(&PARENT.to_string()));
    assert!(tables.contains(&PLAIN.to_string()));
    assert!(
        !tables.iter().any(|t| t.starts_with(&format!("{PARENT}_"))),
        "分区子表不许单独进清单：{tables:?}"
    );

    let dir = tempfile::tempdir().unwrap();
    let pg_dir = dir.path().join("pg");
    let report = backup_postgres(&pool, &pg_dir).await.unwrap();

    let parent_report = report
        .tables
        .iter()
        .find(|t| t.name == PARENT)
        .expect("分区父表必须进报告");
    assert_eq!(parent_report.rows, 3, "父表行数 = 全分区行数");
    assert!(pg_dir.join(format!("public.{PARENT}.csv")).exists());
    assert!(pg_dir.join("sequences.sql").exists());
    assert!(
        report.sequences >= 2,
        "两张 BIGSERIAL 表至少各有一个序列，实得 {}",
        report.sequences
    );

    // 记录备份时的序列位：恢复后必须回到同一位置。
    let seq_before: i64 = sqlx::query_scalar(&format!("SELECT last_value FROM {PLAIN}_id_seq"))
        .fetch_one(&pool)
        .await
        .unwrap();

    // 模拟换机：清空数据并推进序列（新库上迁移会重建表，但这里直接清表
    // 等价且更快；序列推进模拟"空库上又写过别的行"的漂移）。
    sqlx::query(&format!(
        "TRUNCATE {PARENT}, {PLAIN} RESTART IDENTITY CASCADE"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!("INSERT INTO {PLAIN} (label) VALUES ('drift')"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("DELETE FROM {PLAIN}"))
        .execute(&pool)
        .await
        .unwrap();

    let restored = restore_postgres(&pool, &pg_dir).await.unwrap();
    assert!(restored.contains(&PARENT.to_string()));
    assert!(restored.contains(&PLAIN.to_string()));

    assert_eq!(count_in(&pool, PARENT).await, 3);
    assert_eq!(count_in(&pool, &format!("{PARENT}_y2026m08")).await, 2);
    assert_eq!(
        count_in(&pool, &format!("{PARENT}_default")).await,
        1,
        "COPY 进父表必须按分区键路由，DEFAULT 里的行也要回来"
    );
    assert_eq!(count_in(&pool, PLAIN).await, 2);

    let note: String = sqlx::query_scalar(&format!(
        "SELECT note FROM {PARENT} WHERE ts = '2027-01-01 00:00:00+00'"
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(note, "default partition row");

    let seq_after: i64 = sqlx::query_scalar(&format!("SELECT last_value FROM {PLAIN}_id_seq"))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        seq_after, seq_before,
        "序列必须回到备份时的位置，不是恢复后数据的行数"
    );

    for stmt in [
        format!("DROP TABLE IF EXISTS {PARENT} CASCADE"),
        format!("DROP TABLE IF EXISTS {PLAIN} CASCADE"),
    ] {
        sqlx::query(&stmt).execute(&pool).await.unwrap();
    }
}
