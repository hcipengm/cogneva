//! 跨 crate 的夹具约定：同一个外部服务、同一条读取规则，不要在每个 crate 里各写一遍。
//!
//! 这里的每一条都曾经是散在各 crate 的 `tests/` 里的抄件。它们不是「各自的测试数据」——
//! 它们说的是同一件事：这台机器上用哪个 PostgreSQL、哪个 Redis，没有的时候是停还是跳过，
//! 以及一个用例怎么从那个库里切出一块只属于自己、用完就删的空间。抄件之间没有东西比较，
//! 一边改了措辞或改了「没有就停 / 没有就跳过」这条规矩，另一边不会红：两边都还是绿的，
//! 只是绿的来源不同。
//!
//! 判据在 `crates/cogneva/tests/the_fixture_reading_steps_are_written_once_test.rs`：
//! 这份文件里定义过的函数体，不许在任何一份夹具里再出现一遍。

use std::path::{Path, PathBuf};

use sqlx::Connection as _;

mod backends;

pub use backends::{MemoryObjectBackend, MemoryVectorBackend};

/// 下面那几个入口读的名字。一条名字只在这里拼写一次。
///
/// 拼第二遍的地方不会因为拼错而红：`env::var` 读一个没人设置的名字，与「本来就没配」
/// 在调用点上完全同形，旋钮静默失效。所以名字是常量，函数只读常量。
const DATABASE_URL_ENV: &str = "COGNEVA_TEST_DATABASE_URL";
const REDIS_URL_ENV: &str = "COGNEVA_TEST_REDIS_URL";

/// 这些用例要用的那个 PostgreSQL：一次性的库，由 `COGNEVA_TEST_DATABASE_URL` 指定。
///
/// 没设就停，不回落默认地址——这些用例会建表、写行、删数据，落到一台恰好跑着
/// PostgreSQL 的机器上，比干脆不跑更糟。
pub fn database_url() -> String {
    std::env::var(DATABASE_URL_ENV).expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// 这些用例要用的那个 Redis，没设就用开发机上的默认端口。
///
/// 回落在这里是对的而落库地址那里是错的，因为两者的失败方式不同：这些用例只读写自己
/// 那一轮生成的 session id，连到一台开发机自带的 Redis 上最多是读不到别人的数据；
/// 而建表、删行的用例落到一台不是为此准备的库上，删掉的是真东西。
pub fn redis_url() -> String {
    std::env::var(REDIS_URL_ENV).unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

/// 同上，但没设就停：这些用例会删键，必须落在调用方点名的那个实例上，不能靠猜。
pub fn redis_url_required() -> String {
    std::env::var(REDIS_URL_ENV)
        .expect("set COGNEVA_TEST_REDIS_URL to a throwaway Redis before running this test")
}

/// 一块用完就自己消失的临时目录。
///
/// 名字由 `label` 与本次的进程号拼成，所以并行跑的用例不会踩到彼此的路径；也正因为
/// 名字里带着进程号，进程退出之后没有任何东西会回来收拾它——一次全量单测会给每个
/// 用到它的用例留下一份几十兆的副本，而单测、构建和集群在这台机器上共用同一块盘。
/// 所以清理挂在 `Drop` 上：作用域结束就删，用例 panic 也删，不靠谁记得收尾。
///
/// `label` 由调用方说，它只决定目录名的前半截。同一次运行里两处用同一个 `label` 会
/// 落到同一个路径上（先建的被后建的覆盖），所以它要带上用例自己的名字。
pub struct ScratchDir(PathBuf);

impl ScratchDir {
    /// 建目录。同名旧目录先删掉：上一轮若是被 SIGKILL 打断、`Drop` 没跑成，留着的那份
    /// 会让本轮读到上一轮写进去的字节。
    pub fn new(label: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("cog-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("the scratch dir is creatable");
        Self(dir)
    }

    /// 目录本身，离开作用域之前一直存在。
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl std::ops::Deref for ScratchDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        // 删不掉不算失败：用例的结论说的是被测代码，不是收尾时磁盘上还剩什么。
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod scratch_dir_tests {
    use super::ScratchDir;

    /// 目录活着的时候能用，出了作用域什么都不留。
    ///
    /// 判的是 `Drop` 上真的挂了清理。这个类型存在的全部理由就是「没人回来收拾的那一份
    /// 要被收拾掉」，而把清理删掉、或者退回成返回一个 `PathBuf`，用例照样全绿——绿的是
    /// 被测模块，漏的是每个用例几十兆的残留，而残留只有下一轮扫盘时才会显形。
    #[test]
    fn a_scratch_dir_leaves_nothing_behind_when_it_goes_out_of_scope() {
        let path;
        {
            let dir = ScratchDir::new("scratch-dir-contract");
            std::fs::write(dir.path().join("payload"), b"x").expect("the payload is written");
            assert!(
                dir.join("payload").is_file(),
                "the dir is usable while it is alive"
            );
            path = dir.path().to_path_buf();
        }
        assert!(!path.exists(), "{} survived its scope", path.display());
    }
}

/// 一次用例独占的那个 schema：名字由 `label` 加上本次的进程号与纳秒数拼成。
///
/// `label` 由调用方说，它只是这个名字的前半截——谁建的 schema 谁按名字删它，所以这个
/// 标签是调用方自己的事实，不在这里替它们编一个。它会被直接拼进 DDL，因此必须是合法的
/// 标识符片段（全小写字母与下划线）。
pub async fn own_schema(url: &str, label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let schema = format!("{label}_{}_{nanos}", std::process::id());
    let mut admin = sqlx::PgConnection::connect(url)
        .await
        .expect("the throwaway database is reachable");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await
        .expect("the probe schema is created");
    schema
}

/// 删掉 [`own_schema`] 建的那一个。
///
/// 删不掉不算失败：用例的结论说的是被测代码，不是收尾时数据库还在不在。
pub async fn drop_schema(url: &str, schema: &str) {
    if let Ok(mut admin) = sqlx::PgConnection::connect(url).await {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&mut admin)
            .await
            .ok();
    }
}

/// 连到 `schema` 上的池，`search_path` 指过去，所以用例里的表名不用带前缀。
pub async fn schema_pool(url: &str, schema: &str) -> sqlx::PgPool {
    sqlx::PgPool::connect_with(schema_options(url, schema))
        .await
        .expect("the throwaway database is reachable")
}

/// 同上，但连接数由调用方给。
///
/// 单独一个入口而不给 [`schema_pool`] 加参数：要同时占住多个连接的用例（判锁的那几个）
/// 关心的是「还差一个连接时会怎样」，不关心这件事的用例不该为了签名齐整去写一个默认值。
pub async fn schema_pool_with(url: &str, schema: &str, max_connections: u32) -> sqlx::PgPool {
    sqlx::pool::PoolOptions::new()
        .max_connections(max_connections)
        .connect_with(schema_options(url, schema))
        .await
        .expect("the throwaway database is reachable")
}

fn schema_options(url: &str, schema: &str) -> sqlx::postgres::PgConnectOptions {
    use std::str::FromStr as _;
    sqlx::postgres::PgConnectOptions::from_str(url)
        .expect("the test database URL parses")
        .options([("search_path", schema)])
}

/// 把一段 schema 文本直接交给数据库跑的 runner。
///
/// 生产上跑建表语句的那个 runner 有锁等待上限、退避重放，并在失败时报出等在哪张表上
/// ——那是它要对付「旧副本还在服务」这件事。用例这边没有旧副本：库是一次性的，
/// 表是这一轮刚切的 schema 里的，跑不完只说明语句写错了。所以夹具给的是不带上限的那个，
/// 语义差着一件事：**用这个 runner 的用例测不到锁等待行为**，那条行为在
/// `crates/cog-storage/tests/schema_lock_bound.rs` 里对着真的 runner 量。
///
/// 它存在是因为其余 crate 的用例不能为了建一张表去依赖存储层：那是生产 crate，
/// 用例伸手进去，就会长出一条「测试替契约把关」的边进来。
pub struct SchemaTextRunner {
    pool: sqlx::PgPool,
}

impl SchemaTextRunner {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl cog_core::SchemaRunner for SchemaTextRunner {
    async fn apply_schema(&self, ddl: &str) -> cog_core::SFResult<()> {
        sqlx::raw_sql(ddl)
            .execute(&self.pool)
            .await
            .map_err(|e| cog_core::SFError::Database(e.to_string()))?;
        Ok(())
    }
}
