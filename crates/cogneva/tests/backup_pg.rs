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

mod common;

use common::routes::repo_root;

use sqlx::PgPool;

use cogneva::backup::{backup_postgres, claim_drill_lock, discover_tables, restore_postgres};

use cog_test_support::database_url;

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

/// 现建一个**没有超级用户位**的角色，连同它自己的库；返回管理连接、受限连接与库 URL。
///
/// 同名的角色与库先清掉：上一次跑挂在半路会留下它们，那时 `CREATE` 直接失败，而那个
/// 失败与要证明的事无关。口令是写死的假值——这个角色只活在这一条测试里，跑完就删，
/// 库与实例都在本地端口转发后面。
async fn restricted_role_database(role: &str, db: &str) -> (PgPool, PgPool, String) {
    let base = database_url();
    let cut = base
        .rfind('/')
        .expect("a database URL has a path holding the database name");
    let server = &base[..cut];
    let (_, host) = server
        .rsplit_once('@')
        .expect("the test database URL carries credentials");
    let admin = PgPool::connect(&format!("{server}/postgres"))
        .await
        .unwrap();
    for stmt in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("DROP ROLE IF EXISTS {role}"),
        format!(
            "CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD 'probe-only'"
        ),
        format!("CREATE DATABASE {db} OWNER {role}"),
    ] {
        sqlx::query(&stmt).execute(&admin).await.unwrap();
    }
    let url = format!("postgres://{role}:probe-only@{host}/{db}");
    let pool = PgPool::connect(&url).await.unwrap();
    let is_superuser: bool = sqlx::query_scalar("SELECT current_setting('is_superuser')::bool")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!is_superuser, "这条腿要的正是没有超级用户位的连接");
    (admin, pool, url)
}

/// 收尾：先把受限连接关掉，再删库与角色（有活连接时 `DROP DATABASE` 会被拒）。
async fn drop_restricted_database(admin: &PgPool, role: &str, db: &str) {
    for stmt in [
        format!("DROP DATABASE IF EXISTS {db}"),
        format!("DROP ROLE IF EXISTS {role}"),
    ] {
        sqlx::query(&stmt).execute(admin).await.unwrap();
    }
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

/// 一次删表的迁移之后，旧包的 `sequences.sql` 还点名着那张表的自增计数器。
///
/// 与上面那条是同一件事的第二个落点：`085_drop_tables_no_code_names` 删掉
/// `agent_performance` 时，它的序列一起消失；保留窗口里的包写在那之前，
/// `sequences.sql` 里仍有一句 `setval('agent_performance_id_seq', …)`。恢复端整句
/// 执行会在「关系不存在」上失败，整个包被判成不可恢复——代价是同一个包里其余
/// 66 张表和活着的序列一起退回去。判据与跳表那条相同：跳过并在账上留名字，
/// 静默跳过会让装回去的序列数看起来与清单完全一致。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn pg_restore_skips_a_sequence_the_schema_no_longer_has_and_names_it() {
    const KEPT: &str = "backup_probe_seq_kept";
    let (pool, _url) = own_database("seq").await;
    for stmt in [
        format!("DROP TABLE IF EXISTS {KEPT} CASCADE"),
        format!("CREATE TABLE {KEPT} (id BIGSERIAL PRIMARY KEY, label TEXT NOT NULL)"),
    ] {
        sqlx::query(&stmt).execute(&pool).await.unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let pg_dir = dir.path().join("pg");
    std::fs::create_dir_all(&pg_dir).unwrap();
    std::fs::write(
        pg_dir.join(format!("public.{KEPT}.csv")),
        "id,label\n7,alpha\n8,beta\n",
    )
    .unwrap();
    // 已删表的 CSV 一起在包里：真包就是这个形状，跳表与跳序列发生在同一份包上。
    std::fs::write(
        pg_dir.join("public.agent_performance.csv"),
        "id,agent_id\n1,a\n",
    )
    .unwrap();
    // 导出端写下的两种序列名：一个活着，一个随表一起没了。
    std::fs::write(
        pg_dir.join("sequences.sql"),
        format!(
            "SELECT setval('{KEPT}_id_seq', 8, true);\n\
             SELECT setval('agent_performance_id_seq', 42, true);\n"
        ),
    )
    .unwrap();

    let restored = restore_postgres(&pool, &pg_dir).await.unwrap();
    assert_eq!(
        restored.sequences_without_a_home,
        vec!["agent_performance_id_seq".to_string()],
        "被跳过的序列必须点名：不点名，装回去的序列数会与清单一致，读起来像完整恢复"
    );
    // 活着的那个序列要真被 setval 到包内的值；跳过一条不能连累其余。
    let next: i64 = sqlx::query_scalar(&format!("SELECT nextval('{KEPT}_id_seq')"))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(next, 9, "包内 setval 没有生效：下一行会与包里的 id 撞主键");

    sqlx::query(&format!("DROP TABLE IF EXISTS {KEPT} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
}

/// 一次删表的迁移之后，旧包点出了一张今天的结构里已经不存在的表。
///
/// 现场：`085_drop_tables_no_code_names` 删掉了 `agent_performance`，而保留窗口里
/// 有它之前写出的包。恢复按包里的 CSV 逐表 TRUNCATE，第一句就撞上「关系不存在」，
/// 整个包被判不可恢复——那张表的数据早已没有任何读者，代价却是同一个包里其余
/// 66 张表也一并退回去。这条路径必须跳过它并把名字带回去：静默跳过会让装回来的
/// 表数与行数与清单完全一致，读起来像一份完整的恢复。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn pg_restore_skips_a_table_the_schema_no_longer_has_and_names_it() {
    const KEPT: &str = "backup_probe_kept";
    let (pool, _url) = own_database("gone").await;
    for stmt in [
        format!("DROP TABLE IF EXISTS {KEPT} CASCADE"),
        format!("CREATE TABLE {KEPT} (id BIGSERIAL PRIMARY KEY, label TEXT NOT NULL)"),
    ] {
        sqlx::query(&stmt).execute(&pool).await.unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let pg_dir = dir.path().join("pg");
    std::fs::create_dir_all(&pg_dir).unwrap();
    std::fs::write(
        pg_dir.join(format!("public.{KEPT}.csv")),
        "id,label\n1,alpha\n2,beta\n",
    )
    .unwrap();
    std::fs::write(
        pg_dir.join("public.agent_performance.csv"),
        "id,agent_id\n1,a\n",
    )
    .unwrap();

    let restored = restore_postgres(&pool, &pg_dir).await.unwrap();
    assert_eq!(
        restored.tables,
        vec![KEPT.to_string()],
        "装回去的只有目标结构还记得的那张表"
    );
    assert_eq!(
        restored.tables_without_a_home,
        vec!["agent_performance".to_string()],
        "被跳过的表必须点名：不点名，这份包读起来与完整恢复一模一样"
    );
    assert_eq!(
        count_in(&pool, KEPT).await,
        2,
        "跳掉一张表不能连累同一个包里其余的表的行"
    );

    sqlx::query(&format!("DROP TABLE IF EXISTS {KEPT} CASCADE"))
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
/// 输入由外部给，前提也就由外部定：保留窗口滑过导出改动之后，窗口里最老的包
/// 也不再带生成列，这时这个用例证明不了它要证明的事。它先读包的表头；前提不在
/// 就报 unjudged（判不了）——点明手上的包写于何时、要哪个 commit 之前的包，然后
/// 不做任何断言。既不是「恢复漏了」，也不是「恢复对了」：没有输入就没有结论。
///
/// ```text
/// COGNEVA_TEST_DATABASE_URL=postgres://…/cog_restore_probe \
/// COGNEVA_TEST_PACKAGE_PG_DIR=/tmp/pkg/pg \
/// COGNEVA_TEST_PACKAGE_MANIFEST=/tmp/pkg/manifest.json \
///   cargo test -p cogneva --test backup_pg -- --ignored --nocapture
/// ```
///
/// 这是这个二进制里唯一需要外部输入的一条：CI 的步骤按名字跳过它，其余三条在那里
/// 跑。它是一场**手工读数**，不是 CI 的一格。前提不在时它报 unjudged 并在 stderr
/// 留一行（`--nocapture` 之外也能看见），所以「这次跑绿了」与「这次判过了」不是
/// 同一句话——别把前者当前者。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL, COGNEVA_TEST_PACKAGE_PG_DIR and COGNEVA_TEST_PACKAGE_MANIFEST"]
async fn pg_restore_loads_a_package_written_before_the_schema_moved_on() {
    use cogneva::backup::BackupManifest;

    // 这个用例把一个完整的生产包灌进目标库，它必须独占一个库：与探针表共用
    // 一个库时，`discover_tables` 会把别的用例建的表也算进包。
    let pg_dir = std::env::var("COGNEVA_TEST_PACKAGE_PG_DIR")
        .map(std::path::PathBuf::from)
        .expect("set COGNEVA_TEST_PACKAGE_PG_DIR to the pg/ directory of an extracted package");
    let manifest_path = std::env::var("COGNEVA_TEST_PACKAGE_MANIFEST")
        .map(std::path::PathBuf::from)
        .expect("set COGNEVA_TEST_PACKAGE_MANIFEST to that package's manifest.json");
    let manifest: BackupManifest =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();

    // 这个用例要的是一份**旧包**：导出还没改成点名搬迁列，所以包里那份
    // `raw_log_index` 的表头带着生成列 `is_hot`。保留窗口滑过去以后手上会给到
    // 新包，这时失败必须说「包不对」，不能说「恢复漏了」——后者把人指向恢复代码，
    // 而恢复代码根本没被这份包考到。
    let header = std::fs::read_dir(&pg_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with("raw_log_index.csv"))
        })
        .find_map(|path| {
            std::fs::read_to_string(&path)
                .ok()
                .and_then(|text| text.lines().next().map(str::to_string))
        })
        .unwrap_or_default();
    if !header.split(',').any(|column| column.trim() == "is_hot") {
        // 判不了 ≠ 判不过。输入由外部给，保留窗口按**运行次数**滚，合格包滚完以后
        // 这条用例手上没有任何能考生成列重算的东西。判红会把人送去修恢复代码，
        // 而那里没有可修的；判绿会把「没判过」记成「判过了」。两种都不对，
        // 所以只报 unjudged：把理由和它从什么时候起判不了写出来，然后什么都不断言。
        //
        // 直接写 stderr 的句柄而不走 `eprintln!`：libtest 捕获的是 print 宏的输出，
        // 通过的用例默认不打印它——那样这行提示会跟着绿一起消失，等于把 unjudged
        // 藏进绿里。这条提示必须在测试**通过**时也能看见。
        let notice = format!(
            "unjudged: pg_restore_loads_a_package_written_before_the_schema_moved_on\n\
             \x20 为什么判不了：手上的包写于 {created_at}，`raw_log_index` 表头没有生成列 `is_hot`；\
             只有导出改动（commit 1d41e52b，2026-10-18 17:30Z，改成点名搬迁列）之前写下的包才带它。\n\
             \x20 从什么时候起判不了：合格包滚出保留窗口那一刻。保留按运行次数算\
             （keep_target 份，每次成功运行裁掉最老的一份），所以那个时点由运行史决定，日期算不出来。\n\
             \x20 这不是恢复路径的失败，也不是能从代码里修掉的缺陷——别拿它禁行。\
             要判这条，换一份 1d41e52b 之前写下的包。\n\
             \x20 表头：{header}",
            created_at = manifest.created_at,
        );
        use std::io::Write as _;
        let _ = std::io::stderr().write_all(format!("\n{notice}\n").as_bytes());
        return;
    }

    let (_probe, url) = own_database("package").await;

    // 目标库必须是**这个镜像**自己的结构，而不是备份那一刻的结构：恢复只做
    // TRUNCATE + COPY，空库的形状先由迁移与各 store 的 DDL 立出来。
    let migrations = repo_root().join("crates/cog-storage/migrations");
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
    let migrations = repo_root().join("crates/cog-storage/migrations");
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
        restore_source: Some("postgres:5432/cogneva".into()),
        restore_target: Some("postgres:5432/cogneva_drill".into()),
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
        tables_without_a_home: vec!["agent_performance".into()],
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
    assert!(
        read.ok(),
        "算回来的列与被迁移删掉的表都不进成败判决：判决问的是装回来的那批对不对"
    );
    assert_eq!(
        read.tables_without_a_home,
        vec!["agent_performance".to_string()],
        "被跳过的表名必须原样回来：这一格丢了，一份缺表的包读起来就是完整恢复"
    );

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

    // 「装回了哪儿」两格：值故意写得不一样、且都不是 NULL——两格若在绑定或
    // SELECT 里错开一位，读回来的是对方那一格，而两格都非空时这种错位不会报错，
    // 只会让判据把「另一台机器」读成「同一台」。
    assert_eq!(
        read.restore_source.as_deref(),
        Some("postgres:5432/cogneva"),
        "演练行的源地址没有原样回来"
    );
    assert_eq!(
        read.restore_target.as_deref(),
        Some("postgres:5432/cogneva_drill"),
        "演练行的目标地址没有原样回来"
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
    let url = database_url();
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

/// 一个**没有超级用户位**的角色，连同它自己的库：恢复必须能在这种角色上跑通。
///
/// 这条腿管的是整条降权路线的最后一段——演练与换机恢复过去都只能用实例超级用户
/// 跑，理由就是恢复里的 `session_replication_role` 只对超级用户开放。这个位要是
/// 拿不掉，`ALTER ROLE cog_user NOSUPERUSER` 就永远做不成：降完当晚演练当场红。
///
/// 子表名排在父表名之前（`..._members` < `..._users`），所以它能装进去这件事本身
/// 就说明装载顺序按依赖排过，不是碰巧。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn pg_restore_runs_on_a_role_without_the_superuser_bit() {
    const USERS: &str = "restricted_probe_users";
    const MEMBERS: &str = "restricted_probe_members";

    const ROLE: &str = "cog_restore_probe_limited";
    const DB: &str = "cog_restore_probe_restricted";
    let (admin, pool, _url) = restricted_role_database(ROLE, DB).await;

    for stmt in [
        format!("CREATE TABLE {USERS} (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)"),
        format!(
            "CREATE TABLE {MEMBERS} (id BIGSERIAL PRIMARY KEY, \
             user_id BIGINT NOT NULL REFERENCES {USERS}(id), note TEXT)"
        ),
        format!("INSERT INTO {USERS} (name) VALUES ('ada'), ('grace'), ('alan')"),
        format!(
            "INSERT INTO {MEMBERS} (user_id, note) VALUES (1, 'first'), (2, 'second'), (3, 'third')"
        ),
    ] {
        sqlx::query(&stmt).execute(&pool).await.unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let pg_dir = dir.path().join("pg");
    backup_postgres(&pool, &pg_dir).await.unwrap();

    // 装回去之前把两边清空：留着原行时「恢复成功」与「什么都没做」同形。
    sqlx::query(&format!(
        "TRUNCATE {MEMBERS}, {USERS} RESTART IDENTITY CASCADE"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let restored = restore_postgres(&pool, &pg_dir).await.unwrap();
    assert!(restored.tables.contains(&MEMBERS.to_string()));
    assert!(restored.tables.contains(&USERS.to_string()));

    assert_eq!(count_in(&pool, USERS).await, 3);
    assert_eq!(count_in(&pool, MEMBERS).await, 3);

    // 外键在装载之后必须仍然有效：关掉检查再打开会留下一个「装着坏数据却报成功」
    // 的恢复，那是比装不上更坏的结局。
    let dangling = sqlx::query(&format!(
        "INSERT INTO {MEMBERS} (user_id, note) VALUES (999, 'no such user')"
    ))
    .execute(&pool)
    .await;
    assert!(dangling.is_err(), "恢复完成后外键检查必须是开着的");

    pool.close().await;
    drop_restricted_database(&admin, ROLE, DB).await;
}

/// 演练在源库那台实例上建一个一次性库，**用同一个角色**跑迁移、建各 store 的结构、
/// 再把包装回去。前两段今天没有覆盖：迁移里有 `CREATE EXTENSION`，它要不要超权位
/// 取决于扩展的 trusted 位。`ALTER ROLE cog_user NOSUPERUSER` 之后，演练要是只能靠
/// 超权位建出目标结构，它会在 03:47 那一趟才红——那时没有第二个包可选。
#[tokio::test]
#[ignore = "requires COGNEVA_TEST_DATABASE_URL pointing at a live PostgreSQL"]
async fn the_drill_schema_builds_on_a_role_without_the_superuser_bit() {
    const ROLE: &str = "cog_drill_probe_limited";
    const DB: &str = "cog_drill_probe_restricted";

    let (admin, pool, url) = restricted_role_database(ROLE, DB).await;

    let migrations = repo_root().join("crates/cog-storage/migrations");
    cog_storage::migrate::Migrator::new(&migrations)
        .run(&url)
        .await
        .unwrap();
    cogneva::schema_bundle::apply(&pool).await.unwrap();

    // 迁移装的是 `uuid-ossp`。它在 PG16 上是 trusted（库属主装得动），所以上面那一条
    // 能过；把装了哪些扩展印出来，读的人才分得清「迁移没跑」与「扩展装不上」。
    let extensions: Vec<String> =
        sqlx::query_scalar("SELECT extname::text FROM pg_extension ORDER BY extname")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        extensions.iter().any(|name| name == "uuid-ossp"),
        "uuid-ossp 没装上：{extensions:?}"
    );

    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        tables > 50,
        "受限角色只建出 {tables} 张表——结构没立起来，恢复会在第一张缺表上停"
    );

    pool.close().await;
    drop_restricted_database(&admin, ROLE, DB).await;
}
