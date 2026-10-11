//! 中立性判据：评测台里不许出现任何**外壳的专有名**。
//!
//! 方法无关性不是一句声明——声明会随着第一次「顺手在这里替某个外壳判一下」而失效。
//! 这里把它做成一个**能红的**检查：去运行器 / 判分面 / 外壳抽象的源码里找任何一个
//! 外壳的名字。找得到，就说明评测台已经在为某个外壳做事，那一行的数就是「我们的
//! 外壳 ＋ 别人的方法」——拿自己当公共基座比出来的数不是那个方法。
//!
//! 这个文件**必须**在 `tests/` 而不是那三份源码里：判据一旦写在被判的文件里，
//! 它自己的禁用表就会被自己命中的（判据变成了它自己的一部分，检查当场变红，然后
//! 就会被「顺手」把表挪走或加豁免）。检查者和被检查者不能是同一份文件。
//!
//! 禁用表有一半是**自动长出来的**：外壳目录里每多一个文件，它的名字就自动进表。
//! 加第五行外壳时不会因为忘了改这张表而让检查悄悄变弱——那种失效没有任何症状。

use std::path::{Path, PathBuf};

/// 评测台本体。外壳插件放哪里都行，就是不许出现在这三份里。
const RIG_FILES: &[&str] = &["src/scaffold.rs", "src/bench.rs", "src/rig.rs"];

/// 设计里点名的外壳与阶段名。自动那一半管不到还没写出来的东西，所以要有人把
/// 「表里那四行叫什么」也钉一遍。
const NAMED_IN_THE_DESIGN: &[&str] = &[
    "codeact",
    "gepa",
    "agentflow",
    "nql",
    "planner",
    "evaluator",
    "quality gate",
    "quality_gate",
];

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn forbidden_names(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = NAMED_IN_THE_DESIGN.iter().map(|s| s.to_string()).collect();
    if let Ok(entries) = std::fs::read_dir(root.join("src/scaffolds")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let stem = path.file_stem().unwrap().to_string_lossy().to_lowercase();
            if stem != "mod" {
                names.push(stem);
            }
        }
    }
    names
}

#[test]
fn the_rig_names_no_scaffold() {
    let root = manifest_dir();
    let forbidden = forbidden_names(&root);
    assert!(
        forbidden.len() >= NAMED_IN_THE_DESIGN.len(),
        "禁用表不该缩水"
    );

    for file in RIG_FILES {
        let text = std::fs::read_to_string(root.join(file))
            .unwrap_or_else(|e| panic!("cannot read {file}: {e}"))
            .to_lowercase();
        for name in &forbidden {
            assert!(
                !text.contains(name.as_str()),
                "{file} 里出现了 `{name}`：评测台认了某个外壳的专有名。\
                 评测台只该调 AgentScaffold，四行外壳都必须是插件。"
            );
        }
    }
}

#[test]
fn the_check_would_catch_a_rigged_rig() {
    // 检查本身要能被证明不是空转：拿一段**确实**提到外壳的文本喂给同一条判据。
    let forbidden = forbidden_names(&manifest_dir());
    let tainted = "let p = Planner::new(); if nql_gate { }".to_lowercase();
    let caught: Vec<&String> = forbidden
        .iter()
        .filter(|n| tainted.contains(n.as_str()))
        .collect();
    assert!(
        caught.len() >= 2,
        "这条判据抓不住明显被污染的实现，它就是个假门：{caught:?}"
    );
}
