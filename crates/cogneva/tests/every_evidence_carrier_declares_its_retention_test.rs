//! 每一份被指针指到的字节，都要说得出它能不能被删。
//!
//! 指针能活过保留期，只有当它指向的载体永久；载体被清掉，指针就悬空——看起来有
//! 证据，取回来是空的。这条门判三件事：载体清单里每一份都写了保留方式；「先归档
//! 再删」的那类点得出归档去处（且那个前缀在树里出现）；指针真的用到的载体
//! （`artifact://`、`attachments/`）都在清单里。直接到点删的载体是明确的不合格，
//! 张数钉死，只许减。

use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;

use cog_storage::table_shapes::{CarrierRetention, EVIDENCE_CARRIERS};

/// 「还没归档就删」的载体张数上限（只许减）。
const UNARCHIVED_SWEEP_BUDGET: usize = 1;

#[test]
fn every_carrier_declares_how_it_may_be_removed() {
    let root = repo_root();
    let production: String = production_texts(&root)
        .into_iter()
        .map(|(_, text)| text)
        .collect::<Vec<_>>()
        .join("\n");
    let mut problems = Vec::new();
    let mut unarchived = 0usize;
    for carrier in EVIDENCE_CARRIERS {
        if carrier.name.trim().is_empty() || carrier.carrier.trim().is_empty() {
            problems.push("有一份载体没有名字或没有位置".to_string());
        }
        match carrier.retention {
            CarrierRetention::Permanent => {}
            CarrierRetention::ArchiveThenDelete { archive } => {
                if archive.trim().is_empty() {
                    problems.push(format!("{}: 说先归档再删，却没写归档去处", carrier.name));
                } else if !production.contains(archive) {
                    problems.push(format!(
                        "{}: 归档去处 `{archive}` 在生产代码里找不到",
                        carrier.name
                    ));
                }
            }
            CarrierRetention::UnarchivedSweep => unarchived += 1,
        }
    }
    assert!(
        problems.is_empty(),
        "载体清单没有说清：\n{}",
        problems.join("\n")
    );
    assert!(
        unarchived <= UNARCHIVED_SWEEP_BUDGET,
        "「还没归档就删」的载体从 {UNARCHIVED_SWEEP_BUDGET} 涨到了 {unarchived}：先给它一条归档去处，再删热副本"
    );
}

#[test]
fn the_pointers_in_production_are_covered_by_the_carriers() {
    let root = repo_root();
    let production: String = production_texts(&root)
        .into_iter()
        .map(|(_, text)| text)
        .collect::<Vec<_>>()
        .join("\n");
    let named: Vec<&str> = EVIDENCE_CARRIERS
        .iter()
        .map(|carrier| carrier.carrier)
        .collect();
    for pointer in ["artifact://", "attachments/"] {
        let used = production.contains(pointer);
        assert!(
            !used
                || named
                    .iter()
                    .any(|carrier| *carrier == pointer || carrier.contains(pointer)),
            "生产代码在写 `{pointer}` 指针，而载体清单里没有它的去处"
        );
    }
}
