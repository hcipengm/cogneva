//! 正文段进包、再从包按原 oid 装回去：对着真 PostgreSQL 问三件事。
//!
//! - **在包里**：清单声明了这一段（oid 与字节数），`pg/largeobjects/<oid>.bin` 存在
//!   且长度相等。只导表不导大对象的那一版，表行逐行对得上而字节一段都不在包里。
//! - **按原 oid 回来**：恢复先丢后建，建出来的 oid 必须与行里存的那个相同——换一个
//!   oid，表里的引用就指向空处。
//! - **逐字相同**：读回来的字节与封口时记下的哈希、长度都相等。
//!
//! 默认忽略，要 `COGNEVA_TEST_DATABASE_URL`：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cogneva --test backup_large_objects -- --ignored
//! ```

use sqlx::PgPool;

use cog_core::{BodyStore, BodyWriter};
use cog_storage::PostgresBodyStore;
use cogneva::backup::{backup_postgres, restore_large_objects};

fn pool_url() -> String {
    cog_test_support::database_url()
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex(&hasher.finalize())
}

/// 十六进制，小写；与 `BodyWriter::seal` 报的那种写法相同。
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_body_is_carried_in_the_package_and_comes_back_under_its_own_oid() {
    let url = pool_url();
    let pool = PgPool::connect(&url).await.expect("连不上测试库");
    let store = PostgresBodyStore::new(pool.clone());

    // 非文本字节：0x00 与整段 0..=250 都出现，文本编码混不进来。
    let mut body = Vec::with_capacity(8192 + 3);
    for i in 0..(8192 + 3) {
        body.push((i % 251) as u8);
    }

    let writer = store.open().await.expect("开不出大对象");
    let oid = writer.oid();
    for chunk in body.chunks(1024) {
        writer.write(chunk).await.expect("一块写不进去");
    }
    let (sealed_sha, sealed_len) = writer.seal().await.expect("封口失败");
    assert_eq!(sealed_sha, sha256(&body), "封口读数与上游字节不等");

    let dir = tempfile::tempdir().unwrap();
    let pkg_pg = dir.path().join("pg");
    let report = backup_postgres(&pool, &pkg_pg)
        .await
        .expect("备份失败");

    let declared = report
        .large_objects
        .iter()
        .find(|lo| lo.oid == oid)
        .unwrap_or_else(|| panic!("清单里没有这一段正文（oid {oid}）：{:?}", report.large_objects));
    assert_eq!(declared.bytes, sealed_len, "清单里的字节数与封口读数不等");

    let file = pkg_pg
        .join("largeobjects")
        .join(format!("{oid}.bin"));
    let on_disk = std::fs::metadata(&file)
        .unwrap_or_else(|e| panic!("包里没有这一段：{}（{e}）", file.display()))
        .len();
    assert_eq!(on_disk, sealed_len, "包里那一段的字节数与清单不等");

    // 丢掉原段，再从包里装回去：oid 必须原样回来，字节必须逐字相同。
    cog_storage::postgres::large_object::unlink(&pool, oid)
        .await
        .expect("丢不掉原段");
    let restored = restore_large_objects(&pool, &pkg_pg.join("largeobjects"))
        .await
        .expect("装不回去");
    let back_report = restored
        .iter()
        .find(|lo| lo.oid == oid)
        .expect("装回去的读数里没有这一段");
    assert_eq!(back_report.bytes, sealed_len);

    let back: Vec<u8> = sqlx::query_scalar("SELECT COALESCE(lo_get($1::oid), ''::bytea)")
        .bind(oid as i64)
        .fetch_one(&pool)
        .await
        .expect("读不回这一段");
    assert_eq!(back.len() as u64, sealed_len, "装回来的长度不等");
    assert_eq!(sha256(&back), sealed_sha, "装回来的字节与封口时不是同一段");

    writer.discard().await.expect("丢不掉这一段");
    pool.close().await;
}
