//! 「变更账不再自己留第二条流」这句话的判据：把整本账从事实折一遍，与表逐格比，
//! 一份分歧都不该有。
//!
//! 折与比各只有一处实现：读库的那一半把表读成同一个形状，比较的那一半是纯函数——
//! 于是「表与流相等」这件事在没有库的地方也反问道得出。分歧必须逐格点名（哪条身份、
//! 哪一格、表里什么、流上什么），并且分开「那一侧没有这一格」与「那一侧说的是空」。

use std::collections::BTreeMap;

use cog_storage::{diff_change_accounts, FoldedChangeAccount, FoldedCitation};

fn account(state: &str) -> FoldedChangeAccount {
    FoldedChangeAccount {
        state: state.into(),
        title: Some("一条规则".into()),
        assertion: Some("函数 f 不再返回 None".into()),
        first_seen_rev: Some("rev-1".into()),
        measured_rev: Some("rev-2".into()),
        delivered_rev: Some("rev-3".into()),
        sources: BTreeMap::from([(
            "specs/a.md".to_string(),
            FoldedCitation {
                verdict: Some("must".into()),
                priority: "P1".into(),
                block: "none".into(),
            },
        )]),
        ..Default::default()
    }
}

#[test]
fn an_account_folded_twice_shows_no_divergence() {
    let identity = "source|crates/x|a rule".to_string();
    let table = BTreeMap::from([(identity.clone(), account("landed"))]);
    let stream = BTreeMap::from([(identity, account("landed"))]);
    assert!(diff_change_accounts(&table, &stream).is_empty());
}

#[test]
fn a_divergence_names_the_cell_and_both_sides() {
    let identity = "source|crates/x|a rule".to_string();
    let table = BTreeMap::from([(identity.clone(), account("pending"))]);
    let stream = BTreeMap::from([(identity.clone(), account("landed"))]);
    let divergences = diff_change_accounts(&table, &stream);
    assert_eq!(divergences.len(), 1, "{divergences:?}");
    let divergence = &divergences[0];
    assert_eq!(divergence.identity, identity);
    assert_eq!(divergence.column, "state");
    assert_eq!(divergence.table.as_deref(), Some("pending"));
    assert_eq!(divergence.stream.as_deref(), Some("landed"));
}

#[test]
fn a_row_missing_on_one_side_says_which_side_it_is_missing_from() {
    let identity = "source|crates/x|a rule".to_string();
    let table = BTreeMap::from([(identity.clone(), account("pending"))]);
    let stream = BTreeMap::new();
    let divergences = diff_change_accounts(&table, &stream);
    assert_eq!(divergences.len(), 1, "{divergences:?}");
    assert_eq!(divergences[0].column, "<row>");
    assert_eq!(divergences[0].table.as_deref(), Some("present"));
    assert_eq!(
        divergences[0].stream, None,
        "「流上没有这一行」不是「流上说它是空的」"
    );
}

#[test]
fn a_citation_that_differs_names_the_document() {
    let identity = "source|crates/x|a rule".to_string();
    let mut stream_account = account("landed");
    stream_account
        .sources
        .get_mut("specs/a.md")
        .unwrap()
        .priority = "P0".into();
    let table = BTreeMap::from([(identity.clone(), account("landed"))]);
    let stream = BTreeMap::from([(identity, stream_account)]);
    let divergences = diff_change_accounts(&table, &stream);
    assert_eq!(divergences.len(), 1, "{divergences:?}");
    assert_eq!(divergences[0].column, "source:specs/a.md");
}

/// 读数面确实存在，两条路径都经过同一处比对。
#[test]
fn the_parity_reading_is_reachable_and_shares_one_comparison() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/cli.rs")).expect("cli.rs");
    assert!(
        cli.contains("change-ledger-parity"),
        "没有这个子命令：逐格比对的读数没有人读得到"
    );
    let store = std::fs::read_to_string(
        root.join("crates/cog-storage/src/postgres/change_ledger_store.rs"),
    )
    .expect("change_ledger_store.rs");
    assert!(
        store.contains("pub fn diff_change_accounts"),
        "比较不是一处纯函数：判据离不开库"
    );
    assert!(
        store.contains("fold_change_account(pool).await?"),
        "比对没有用那一份唯一的折叠：两份口径相等的时机只有它们刚好都没改过"
    );
}
