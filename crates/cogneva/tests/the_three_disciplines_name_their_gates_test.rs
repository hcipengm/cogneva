//! 三条纪律各由哪一道门守着，写在一处：界的形状、常量不许人拍、决定不许交给人。
//!
//! 分开写的时候，每条纪律都有自己的「门在哪」版本，而其中一条改了名字、另一处还指着旧
//! 名字时，两边都读得通——直到有人按错的那一份去找门。登记只有这一张：纪律 → 门的路径
//! → 门拒什么。`other:` 前缀是另一棵树里的门（它守的正是「把人拉进决定」这件事），这一
//! 棵树只认它的前缀与路径形状，不假装它在自己树上。
//!
//! 没有这次改动这条会红：登记不存在，或者点到一个不存在的门。

use std::path::Path;

/// 三条纪律：名字、门的路径、门拒的是什么。
const DISCIPLINES: &[(&str, &str, &str)] = &[
    (
        "界的形状",
        "deploy/scripts/tests/bounds-census-shapes.test.sh",
        "普查器认不出来的那种界：一个数既不是实测导出、也不是具名约定",
    ),
    (
        "常量不许人拍",
        "crates/cogneva/tests/rust_capacity_constants_registry_test.rs",
        "没有 `cap: bound`／`cap: fixed` 与理由的常量，以及 `derived` 却指不出读数的那一类",
    ),
    (
        "决定不许交给人",
        "other:scripts/gates/check-ownership-deferral.sh",
        "把「该谁拍板」推回去给属主的那一句话：它拒绝一次没有归属的推迟",
    ),
];

#[test]
fn each_discipline_names_a_gate_that_exists() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let mut seen = Vec::new();
    for (discipline, gate, rejects) in DISCIPLINES {
        assert!(!seen.contains(discipline), "{discipline} 登记了两次");
        seen.push(*discipline);
        assert!(!rejects.trim().is_empty(), "{discipline}: 没说这门拒什么");
        if let Some(relative) = gate.strip_prefix("other:") {
            assert!(
                std::path::Path::new(relative)
                    .extension()
                    .is_some_and(|ext| ext == "sh"),
                "{discipline}: 另一棵树里的门要点到脚本本身，而不是一个目录：{relative}"
            );
            continue;
        }
        assert!(
            root.join(gate).is_file(),
            "{discipline}: 点名的门 {gate} 在树里不存在——点一个不存在的路径等于没有门"
        );
    }
    assert_eq!(seen.len(), 3, "三条纪律各一条：{seen:?}");
}
