//! Which recorded principles stand a gate on the file I am about to change.
//!
//! The question the generator asks before it writes: this project has thousands
//! of rules, and the only ones it can be told about with a straight face are the
//! ones the account says go red somewhere — `platform_gate` and `where_red` are
//! where a reading records that place. A row with neither recorded answers no
//! question about any path, and this case holds that it is not returned: putting
//! it in front of a writer would claim it governs the file, and for most of the
//! account that claim would be invented.
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test principle_gating_paths -- --ignored
//! ```
//!
//! The throwaway database starts empty, so the migrations are applied first, and
//! every row carries a per-run marker in its slug and is deleted at the end.

mod common;

use sqlx::PgPool;

use cog_core::{PrincipleCheckKind, PrincipleLedgerStore, PrincipleReading};
use cog_storage::PostgresPrincipleLedgerStore;

/// The bound the pass uses. Spelled out here rather than imported, for the same
/// reason its sibling suite does: it is another crate's number.
const PASS_BOUND: usize = 200;

use cog_test_support::database_url;

async fn store() -> (PostgresPrincipleLedgerStore, PgPool, String) {
    let url = database_url();
    common::migrate_once(&url, 97_101).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the database");
    let store = PostgresPrincipleLedgerStore::new(pool.clone());
    let marker = format!("probe-gating-paths:{}", uuid::Uuid::new_v4());
    for table in ["principle_ledger", "principle_ledger_declared"] {
        sqlx::query(&format!(
            "DELETE FROM {table} WHERE slug LIKE 'probe-gating-paths:%'"
        ))
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("clear leftover rows in {table}: {e}"));
    }
    (store, pool, marker)
}

/// File one principle whose reading records where it goes red.
async fn file(store: &PostgresPrincipleLedgerStore, slug: &str, where_red: Option<&str>) {
    let reading = PrincipleReading {
        slug: slug.to_string(),
        source: "host".to_string(),
        lead: Some("一条这次要改的文件上站着门的原则".into()),
        carrier_path: Some("memory/probe-gating-paths.md".into()),
        carrier_sha256: None,
        host_gate: None,
        where_red: where_red.map(str::to_string),
        check_kind: Some(PrincipleCheckKind::Tree),
        priority: None,
    };
    store.declare(&[reading]).await.expect("queue the reading");
    for _ in 0..4 {
        let fold = store
            .fold_declarations(PASS_BOUND * 2)
            .await
            .expect("fold the queue");
        if fold.folded == 0 {
            break;
        }
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_principle_whose_gate_names_the_path_is_the_answer() {
    let (store, pool, marker) = store().await;

    file(
        &store,
        &format!("{marker}/on-the-path"),
        Some("crates/cog-gateway/tests/progress_tells_a_named_owner_from_a_present_one.rs"),
    )
    .await;
    file(&store, &format!("{marker}/nowhere-in-particular"), None).await;

    let rows = store
        .gating_paths(&["crates/cog-gateway/src/progress.rs".to_string()], 10)
        .await
        .expect("read the account");

    let slugs: Vec<&str> = rows.iter().map(|r| r.slug.as_str()).collect();
    assert!(
        slugs.contains(&format!("{marker}/on-the-path").as_str()),
        "这次要改的路径上有门，账里那条原则却没被取出来：{slugs:?}"
    );
    assert!(
        !slugs.contains(&format!("{marker}/nowhere-in-particular").as_str()),
        "没有记录门在哪里的原则被当成了这条路径的答案：它管不管这条路，账里没有任何\
         一句话说过"
    );

    for table in ["principle_ledger", "principle_ledger_declared"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE slug LIKE $1"))
            .bind(format!("{marker}%"))
            .execute(&pool)
            .await
            .expect("clean up");
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_answer_is_ordered_and_bounded() {
    let (store, pool, marker) = store().await;

    // 三条都站在同一个文件上，写进来的顺序与 slug 的顺序相反：取出来若是写入
    // 顺序，两轮同一件任务就会把同一批原则排成两个顺序，缓存前缀每一轮作废。
    for tail in ["c", "a", "b"] {
        file(
            &store,
            &format!("{marker}/{tail}"),
            Some("crates/cog-store/src/lib.rs"),
        )
        .await;
    }

    let rows = store
        .gating_paths(&["crates/cog-store/src/lib.rs".to_string()], 2)
        .await
        .expect("read the account");
    let mine: Vec<&str> = rows
        .iter()
        .map(|r| r.slug.as_str())
        .filter(|s| s.starts_with(&marker))
        .collect();
    assert_eq!(
        mine,
        vec![
            format!("{marker}/a").as_str(),
            format!("{marker}/b").as_str()
        ],
        "取出来的既不是按 slug 排的，也没有守住条数上限"
    );

    for table in ["principle_ledger", "principle_ledger_declared"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE slug LIKE $1"))
            .bind(format!("{marker}%"))
            .execute(&pool)
            .await
            .expect("clean up");
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn nothing_to_ask_about_is_not_a_reason_to_scan() {
    let (store, _pool, _marker) = store().await;
    let rows = store
        .gating_paths(&[], 10)
        .await
        .expect("an empty question is answered without touching the table");
    assert!(rows.is_empty());
}
