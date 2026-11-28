//! 每个用量读口都要写清「这个数从哪儿来」：流上、行上、还是写口/窗口读数。
//!
//! 停写用量行之前，读者从行切到流是逐口进行的；没有这张表，切换进度不可数，
//! 而「还差谁没切」只能靠人记。

use std::collections::BTreeSet;

use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;

use cog_core::contract::usage_ledger::USAGE_READER_SOURCES;

#[test]
fn every_async_reader_in_the_usage_contract_is_in_the_census() {
    let root = repo_root();
    let contract =
        std::fs::read_to_string(root.join("crates/cog-core/src/contract/usage_ledger.rs"))
            .expect("the usage contract is in the tree");
    let mut found = BTreeSet::new();
    for line in contract.lines() {
        let line = line.trim_start();
        let Some(rest) = line.strip_prefix("async fn ") else {
            continue;
        };
        let name = rest
            .split(['(', '<'])
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        if !name.is_empty() {
            found.insert(name);
        }
    }
    let listed: BTreeSet<String> = USAGE_READER_SOURCES
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect();
    let missing: Vec<&String> = found.difference(&listed).collect();
    assert!(
        missing.is_empty(),
        "这些读口没有写「从哪取数」：{missing:?}"
    );
    let stale: Vec<&String> = listed.difference(&found).collect();
    assert!(stale.is_empty(), "普查表点名了不存在的方法：{stale:?}");
}

#[test]
fn a_stream_reader_has_its_stream_implementation_and_every_source_is_known() {
    let root = repo_root();
    let observability: String = production_texts(&root)
        .into_iter()
        .filter(|(path, _)| {
            path.to_string_lossy()
                .contains("crates/cog-observability/src/")
        })
        .map(|(_, text)| text)
        .collect::<Vec<_>>()
        .join("\n");
    for (name, source) in USAGE_READER_SOURCES {
        assert!(
            matches!(*source, "stream" | "ledger" | "write" | "window"),
            "{name}: 未知的来源词 {source}"
        );
        if *source == "stream" {
            assert!(
                observability.contains(&format!("{name}_from_stream"))
                    || observability.contains(&format!("\"{name}_from_stream\"")),
                "{name}: 标成从流读，却没有 {name}_from_stream"
            );
        }
    }
}
