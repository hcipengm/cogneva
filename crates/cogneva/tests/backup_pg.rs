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
//!
//! Each test works in a database of its own, derived from that URL by replacing
//! its database name. Sharing one database looks harmless and is not: the backup
//! path enumerates every table it can see, so a test running in parallel would
//! package its neighbour's probe tables and then find some of them gone. The
//! failure lands in the fixture, never in the code under test.

use sqlx::PgPool;

use cogneva::backup::{backup_postgres, claim_drill_lock, discover_tables, restore_postgres};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// The test's own database: same server and credentials, its own name.
///
/// Dropped and recreated first, so a run that was killed leaves nothing for the
/// next one to mistake for its own data.
async fn own_database(suffix: &str) -> (PgPool, String) {
    let base = database_url();
    let cut = base
        .rfind('/')
        .expect("a database URL has a path holding the database name");
    let name = format!("cog_restore_probe_{suffix}");
    let admin = PgPool::connect(&format!("{}/postgres", &base[..cut]))
        .await
        .unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name}"))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let url = format!("{}/{name}", &base[..cut]);
    (PgPool::connect(&url).await.unwrap(), url)
}

const PARENT: &str = "backup_probe_events";
const PLAIN: &str = "backup_probe_plain";
const COMPUTED: &str = "backup_probe_computed";

async fn fresh_schema(pool: &PgPool) {
    for stmt in [
        format!("DROP TABLE IF EXISTS {PARENT} CASCADE"),
        format!("DROP TABLE IF EXISTS {PLAIN} CASCADE"),
        format!("DROP TABLE IF EXISTS {COMPUTED} CASCADE"),
        // 生成列能进包的表头、却进不了 COPY FROM：包里带上它，整张表就恢复不了。
        // 它与下面的 PLAIN 成对，一条路径证明搬迁，一条证明「算出来的列不搬」。
        format!(
            "CREATE TABLE {COMPUTED} (id BIGSERIAL PRIMARY KEY, half INTEGER NOT NULL, \
             doubled INTEGER GENERATED ALWAYS AS (half * 2) STORED)"
        ),
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
    let (pool, _url) = own_database("roundtrip").await;
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
    sqlx::query(&format!("INSERT INTO {COMPUTED} (half) VALUES (2), (3)"))
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
    // 包的表头是搬迁的列清单：生成列在这里出现，恢复时必然整表失败。
    {
        use std::io::BufRead;
        let mut header = String::new();
        std::io::BufReader::new(
            std::fs::File::open(pg_dir.join(format!("public.{COMPUTED}.csv"))).unwrap(),
        )
        .read_line(&mut header)
        .unwrap();
        assert_eq!(
            header.trim_end(),
            "id,half",
            "包不该带上目标表自己算出来的列"
        );
    }

    sqlx::query(&format!(
        "TRUNCATE {PARENT}, {PLAIN}, {COMPUTED} RESTART IDENTITY CASCADE"
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
    assert!(restored.tables.contains(&PARENT.to_string()));
    assert!(restored.tables.contains(&PLAIN.to_string()));
    assert!(
        restored.recomputed.is_empty(),
        "新包不带生成列，装载时没有一列要靠目标重算：{:?}",
        restored.recomputed
    );

    assert_eq!(count_in(&pool, PARENT).await, 3);
    assert_eq!(count_in(&pool, &format!("{PARENT}_y2026m08")).await, 2);
    assert_eq!(
        count_in(&pool, &format!("{PARENT}_default")).await,
        1,
        "COPY 进父表必须按分区键路由，DEFAULT 里的行也要回来"
    );
    assert_eq!(count_in(&pool, PLAIN).await, 2);
    let doubled: Vec<i32> =
        sqlx::query_scalar(&format!("SELECT doubled FROM {COMPUTED} ORDER BY id"))
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(doubled, vec![4, 6], "生成列必须由目标表自己算回来");

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
        format!("DROP TABLE IF EXISTS {COMPUTED} CASCADE"),
    ] {
        sqlx::query(&stmt).execute(&pool).await.unwrap();
    }
}

/// 旧导出写的包：表头里有生成列。
///
/// 保留窗口里的每一份包都是这个形状（导出改成点名搬迁列之前写的）。装载语句
/// 不能把生成列点进去（`COPY` 直接拒），也不能把它的值留在流里改成按位置对齐
/// （整表错位）——只能进临时表中转，再按「非生成列」搬进目标表。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn pg_restore_loads_a_package_that_still_carries_the_generated_column() {
    // 这张表不叫 COMPUTED：`cargo test` 默认并行，而 `fresh_schema` 会先 DROP
    // 另外两个探针表。共用一个库的两个用例各自建自己的表，「同一个库」才不等于
    // 「同一张表」——撞表会表现成一次跟被测代码无关的建表失败。
    const LEGACY: &str = "backup_probe_legacy_computed";
    let (pool, _url) = own_database("legacy").await;
    for stmt in [
        format!("DROP TABLE IF EXISTS {LEGACY} CASCADE"),
        format!(
            "CREATE TABLE {LEGACY} (id BIGSERIAL PRIMARY KEY, half INTEGER NOT NULL, \
             doubled INTEGER GENERATED ALWAYS AS (half * 2) STORED)"
        ),
    ] {
        sqlx::query(&stmt).execute(&pool).await.unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let pg_dir = dir.path().join("pg");
    std::fs::create_dir_all(&pg_dir).unwrap();
    // 旧导出的形状：`SELECT *` 把目标自己算的 `doubled` 也写进了包。
    std::fs::write(
        pg_dir.join(format!("public.{LEGACY}.csv")),
        "id,half,doubled\n1,2,4\n2,3,6\n",
    )
    .unwrap();

    let restored = restore_postgres(&pool, &pg_dir).await.unwrap();
    assert_eq!(restored.tables, vec![LEGACY.to_string()]);
    // 装回来的这一列不是包里的字节，是目标按同一行算出来的。这一格存在的
    // 全部理由就是让这件事被记下来——行数与表数看不见它。
    assert_eq!(
        restored.recomputed,
        vec![format!("{LEGACY}.doubled")],
        "由目标重算的列必须被点名，否则一次表达式改动会静默改写历史值"
    );
    assert_eq!(count_in(&pool, LEGACY).await, 2, "整包的行数必须回来");

    // 包内的 id 与 half 原样，`doubled` 由目标按同一行重算——不搬它不丢数据。
    let rows: Vec<(i64, i32, i32)> = sqlx::query_as(&format!(
        "SELECT id, half, doubled FROM {LEGACY} ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows, vec![(1, 2, 4), (2, 3, 6)]);

    sqlx::query(&format!("DROP TABLE IF EXISTS {LEGACY} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
}

/// 用保留窗口里**真实存在**的一份包验收恢复路径。
///
/// 上面那个用例的包是我自己按旧形状写出来的，它只证明「这条路径认得这个形状」，
/// 证明不了「这一晚之前写的包装得回来」。真正的验收必须拿一个真包跑：包里有这个
/// 镜像不认识的分区子表、有当前 DDL 之后才加的列、有导出改成点名搬迁列之前留下的
/// 生成列。所以这个用例的输入由外部给，不自己造。
///
/// ```text
/// COGNEVA_TEST_DATABASE_URL=postgres://…/cog_restore_probe \
/// COGNEVA_TEST_PACKAGE_PG_DIR=/tmp/pkg/pg \
/// COGNEVA_TEST_PACKAGE_MANIFEST=/tmp/pkg/manifest.json \
///   cargo test -p cogneva --test backup_pg -- --ignored --nocapture
/// ```
///
/// 这是这个二进制里唯一需要外部输入的一条：CI 的步骤按名字跳过它（没有包时它会
/// 拒绝而不是通过），其余三条在那里跑。它是一场**手工读数**，不是 CI 的一格。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL, COGNEVA_TEST_PACKAGE_PG_DIR and COGNEVA_TEST_PACKAGE_MANIFEST"]
async fn pg_restore_loads_a_package_written_before_the_schema_moved_on() {
    use cogneva::backup::BackupManifest;

    // 这个用例把一个完整的生产包灌进目标库，它必须独占一个库：与探针表共用
    // 一个库时，`discover_tables` 会把别的用例建的表也算进包。
    let (_probe, url) = own_database("package").await;
    let pg_dir = std::env::var("COGNEVA_TEST_PACKAGE_PG_DIR")
        .map(std::path::PathBuf::from)
        .expect("set COGNEVA_TEST_PACKAGE_PG_DIR to the pg/ directory of an extracted package");
    let manifest_path = std::env::var("COGNEVA_TEST_PACKAGE_MANIFEST")
        .map(std::path::PathBuf::from)
        .expect("set COGNEVA_TEST_PACKAGE_MANIFEST to that package's manifest.json");
    let manifest: BackupManifest =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();

    // 目标库必须是**这个镜像**自己的结构，而不是备份那一刻的结构：恢复只做
    // TRUNCATE + COPY，空库的形状先由迁移与各 store 的 DDL 立出来。
    let migrations =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../cog-storage/migrations");
    cog_storage::migrate::Migrator::new(&migrations)
        .run(&url)
        .await
        .unwrap();
    let pool = PgPool::connect(&url).await.unwrap();
    cogneva::schema_bundle::apply(&pool).await.unwrap();

    let restored = restore_postgres(&pool, &pg_dir).await.unwrap();
    assert_eq!(
        restored.tables.len(),
        manifest.postgres.tables.len(),
        "包里声明的表与装回来的表不是同一批"
    );
    // 这一份包写于导出改成点名搬迁列之前，它的 `raw_log_index` 表头带着
    // `is_hot`。恢复能成功，但那一列的值是目标算的，账上必须点名。
    assert!(
        restored
            .recomputed
            .iter()
            .any(|c| c == "raw_log_index.is_hot"),
        "真包里的生成列没有被点名，恢复记录会漏掉「值不是包里的」这件事：{:?}",
        restored.recomputed
    );
    let mut disagreements = Vec::new();
    for table in &manifest.postgres.tables {
        let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{}\"", table.name))
            .fetch_one(&pool)
            .await
            .unwrap();
        if rows != table.rows {
            disagreements.push(format!("{}: {} != {}", table.name, rows, table.rows));
        }
    }
    assert!(
        disagreements.is_empty(),
        "装回来的行数与包清单对不上：{disagreements:?}"
    );
}

/// 账上那一格必须真的存得住、读得回来。
///
/// 记录里点名了「哪些列是目标算的」，判据却只看表数、行数与源库比对——所以
/// 这一格如果写不进库或读不回来，**没有任何一项判据会变红**。这里用真库把
/// 写入与读取各走一遍，钉住的是存储面，不是恢复路径。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn pg_a_drill_row_keeps_the_columns_the_target_recomputed() {
    use cog_core::{BackupDrillRecord, BackupRunStore};
    use cog_storage::PostgresBackupRunStore;

    let (_probe, url) = own_database("drill").await;
    let migrations =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../cog-storage/migrations");
    cog_storage::migrate::Migrator::new(&migrations)
        .run(&url)
        .await
        .unwrap();
    let pool = PgPool::connect(&url).await.unwrap();
    let store = PostgresBackupRunStore::new(pool.clone());

    let at = chrono::Utc::now();
    let record = BackupDrillRecord {
        drill_id: "drill-generated-columns".into(),
        started_at: at,
        finished_at: at,
        package: Some("cogneva-backup-20261018T031700Z.tar.zst".into()),
        package_written_at: Some(at),
        package_bytes: Some(1),
        segment_skew_seconds: Some(12),
        segment_skew_bound_seconds: Some(300),
        packages_on_volume: 1,
        oldest_package_at: Some(at),
        offsite_newest_at: None,
        offsite_matches_newest: None,
        offsite_missing_packages: Some(3),
        offsite_observed_at: None,
        offsite_remote_newest_at: None,
        offsite_key_created_at: None,
        credential_violations_backup_pvc: Some(2),
        credential_violations_offsite: None,
        credential_violations_state_dir: None,
        tables_restored: Some(1),
        tables_expected: Some(1),
        rows_restored: Some(1),
        rows_expected: Some(1),
        source_runs_matched: Some(true),
        generated_columns_recomputed: vec!["raw_log_index.is_hot".into()],
        artifacts_expected: 0,
        artifacts_restored: 0,
        artifact_readings: Vec::new(),
        artifact_errors: Vec::new(),
        error: None,
    };
    store.record_drill(&record).await.unwrap();

    let read = store.read_latest_drill().await.unwrap().unwrap();
    assert_eq!(
        read.generated_columns_recomputed,
        vec!["raw_log_index.is_hot".to_string()],
        "由目标重算的列必须原样回来：这一格丢了，改表达式就会被装成一次静默改写"
    );
    assert!(read.ok(), "算回来的列不进成败判决");

    // 三格相邻的新列各自带回自己那个数。写成三个不同的值是有意的：绑定顺序若
    // 与列清单错开一格，NULL 与 NULL 之间换不出声，而 12 / 300 / 3 会立刻对不上。
    assert_eq!(
        read.segment_skew_seconds,
        Some(12),
        "演练行的分段偏差没有原样回来"
    );
    assert_eq!(
        read.segment_skew_bound_seconds,
        Some(300),
        "演练行的分段界没有原样回来"
    );
    assert_eq!(
        read.offsite_missing_packages,
        Some(3),
        "窗口里没有卷外副本的包数没有原样回来"
    );
}

/// Two drills must not drop each other's database.
///
/// The drill's throwaway database name is fixed, so a second drill starting while
/// the first is running `DROP DATABASE`s the very database the first one is
/// restoring into -- on 2026-10-19 a drill that began at 17:27 was killed by one
/// that began at 17:35 and recorded as `failed`, a verdict about the schedule
/// rather than about the package. The exclusion has to be asked of the database,
/// because a deployment convention cannot be checked.
#[tokio::test]
#[ignore = "needs a live database: COGNEVA_TEST_DATABASE_URL"]
async fn two_drills_at_once_do_not_drop_each_others_database() {
    let url = std::env::var("COGNEVA_TEST_DATABASE_URL").expect("COGNEVA_TEST_DATABASE_URL");
    use sqlx::Connection as _;
    let first = claim_drill_lock(&url)
        .await
        .unwrap()
        .expect("第一场应当拿到锁");
    assert!(
        claim_drill_lock(&url).await.unwrap().is_none(),
        "第二场必须被拒绝：拿到锁它就会 DROP 掉第一场正在恢复的那个库"
    );
    first.close().await.unwrap();
    assert!(
        claim_drill_lock(&url).await.unwrap().is_some(),
        "第一场的会话关掉之后锁必须能再取到，否则一次崩溃会让演练永远开不了工"
    );
}
