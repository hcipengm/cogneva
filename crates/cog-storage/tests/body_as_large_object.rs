//! 正文的大对象落地：对着真 PostgreSQL 问三件事。
//!
//! - **无损**：封口时把整段读回来的哈希与上游字节的哈希相等，长度也相等——包括一段
//!   远超旧实现那道上限的正文（它是按字节截断的，数字已随那次删除一起消失）。
//!   旧实现在那里截断，那一段落库的字节比上游短，
//!   而短的那一份在任何一列上都读不出「它短」。
//! - **第一时间**：封口之前，已经写进去的块就在库里。进程死在这里时留下的就是这个形状
//!   ——有字节、没有行——所以「崩在中途」与「没有正文」在库面上分得开。
//! - **没人引用的那段可以单独丢掉**：写失败退回行内之后，库里不该留一段既没有行、
//!   又不该被读成「崩在中途」的字节。
//!
//! 默认忽略，要 `COGNEVA_TEST_DATABASE_URL`。大对象是全库共享的（不随 schema 分），
//! 所以这个用例自己丢掉它开的每一段，不靠 schema 的删除收尾：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test body_as_large_object -- --ignored
//! ```

use sqlx::PgPool;

use cog_core::{BodyStore, BodyWriter};
use cog_storage::PostgresBodyStore;

fn pool_url() -> String {
    cog_test_support::database_url()
}

/// 一段正文按 CHUNK 分块喂进去，返回封口读数。分块大小只是这一条用例的喂法。
async fn write_in_chunks(writer: &dyn BodyWriter, body: &[u8]) -> (String, u64) {
    // 用例自己的分块粒度：它模拟「一块一块到达」，不是软件的界。
    const CHUNK: usize = 64 * 1024;
    for chunk in body.chunks(CHUNK) {
        writer.write(chunk).await.expect("一块写不进去");
    }
    writer.seal().await.expect("封口失败")
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

/// 库里这一段现在多长。封口之前读它，就是「已经到了的字节」。
async fn object_len(pool: &PgPool, oid: u32) -> i64 {
    // `length(bytea)` 是 int4：读成 i64 时类型对不上，转一次再读。
    let len: i64 = sqlx::query_scalar("SELECT COALESCE(length(lo_get($1::oid)), 0)::bigint")
        .bind(oid as i64)
        .fetch_one(pool)
        .await
        .expect("读不回这一段");
    len
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_body_past_the_old_limit_is_sealed_whole() {
    let url = pool_url();
    let pool = sqlx::PgPool::connect(&url).await.expect("连不上测试库");
    let store = PostgresBodyStore::new(pool.clone());

    // 这一条用例自己选的长度：比旧实现那道截断大得多，所以它在旧实现上会红。
    let mut body = Vec::with_capacity(2 * 1024 * 1024 + 7);
    for i in 0..(2 * 1024 * 1024 + 7) {
        body.push((i % 251) as u8);
    }

    let writer = store.open().await.expect("开不出大对象");
    let (sealed_sha, sealed_len) = write_in_chunks(writer.as_ref(), &body).await;

    assert_eq!(
        sealed_len,
        body.len() as u64,
        "封口读回来的长度与上游字节不等"
    );
    assert_eq!(sealed_sha, sha256(&body), "封口读回来的哈希与上游字节不等");

    writer.discard().await.expect("丢不掉这一段");
    pool.close().await;
}

/// 封口之前，已经到达的字节就在库里。
///
/// 这条用例判的是「第一时间」：崩溃留下的不是零，是**已经写进去的那一段**。所以这里
/// 故意不封口，直接去问库里的长度。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_bytes_that_arrived_are_in_the_database_before_the_seal() {
    let url = pool_url();
    let pool = sqlx::PgPool::connect(&url).await.expect("连不上测试库");
    let store = PostgresBodyStore::new(pool.clone());

    let writer = store.open().await.expect("开不出大对象");
    let first = vec![7u8; 4096];
    let second = vec![9u8; 1000];
    writer.write(&first).await.expect("第一块写不进去");
    writer.write(&second).await.expect("第二块写不进去");

    let seen = object_len(&pool, writer.oid()).await;
    assert_eq!(
        seen,
        (first.len() + second.len()) as i64,
        "封口之前库里就该有已经到达的那 {}+{} 字节，实得 {seen}",
        first.len(),
        second.len()
    );

    writer.discard().await.expect("丢不掉这一段");
    pool.close().await;
}

/// 丢掉之后这段不再被任何东西引用：`lo_unlink` 之后再读它就是空。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_discarded_body_is_gone() {
    let url = pool_url();
    let pool = sqlx::PgPool::connect(&url).await.expect("连不上测试库");
    let store = PostgresBodyStore::new(pool.clone());

    let writer = store.open().await.expect("开不出大对象");
    let oid = writer.oid();
    writer.write(b"half a body").await.expect("写不进去");
    writer.discard().await.expect("丢不掉这一段");

    let exists: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_largeobject_metadata WHERE oid = $1::oid")
            .bind(oid as i64)
            .fetch_one(&pool)
            .await
            .expect("查不到目录");
    assert_eq!(exists, 0, "丢掉之后目录里还留着 oid {oid}");

    pool.close().await;
}
