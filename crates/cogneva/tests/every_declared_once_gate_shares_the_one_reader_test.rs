//! principle: feedback_no_hardcoding_config_surface
//!
//! 「从一行生产代码里读出双引号字面量」这条读法，曾经在夹具里抄了十一遍。
//! 每一遍都只参数化那一小段真正不同的东西，于是抄出来的每一份都能单独改歪：
//! 有的把 `\"` 当成字面量的结尾，有的按字节读、把非 ASCII 的半个字符也算进词里，
//! 有的只读到一行里的第一个字面量。两份对同一行给出不同答案时，没有任何东西会红——
//! 用其中一份判出来的「全都合规」，另一份根本不会去问。
//!
//! 所以读法只留一份：`common::declared_once::literals_in`，控制律也只留一份：
//! `common::declared_once::Wall` / `Report`。本门管的是**不再长出第二份**：
//! 走遍每个 crate 的 `tests/`，任何定义了「参数是 `&str`、名字里带 literal」的
//! 自由函数的文件，要么已经改读共享实现（文件里出现 `declared_once::`），要么在
//! [`OTHER_READERS`] 里写明它读的是另一种方言、另一种形状。登记了却不再有定义的
//! 条目按过期报出来——一张通行证留在没人走的路口，下一个真的抄一遍的人就没有判据拦。
//!
//! 判不了什么：
//!
//! * `src/` 里的读取器（`bootstrap` 读模板、`takeover` 读迁移脚本）。那是生产代码
//!   自己的解析，测试支撑 crate 编不进它；本门读的是夹具这一侧，而生产侧的硬编码
//!   由各道墙按全树生产源判。
//! * 「同一个词读出来之后怎么比」。本门只判读法有几份，判词在不在每道墙自己的
//!   拼法与登记表里。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::common;

use common::routes::{repo_root, walk};
use common::rust_production::relative_name;

/// 共享读法的家；它自己定义一个以 literal 命名的函数，是这份实现的入口。
const HOME: &str = "crates/cogneva/tests/common/declared_once.rs";

/// 明明带着一个字面量读取器、读的却是另一种东西的文件。
///
/// 每条都要点名理由：与共享读法同形的，改读共享读法；不同形的，写清楚为什么。
const OTHER_READERS: &[(&str, &str)] = &[
    (
        "crates/cogneva/tests/chart_template_literals_contract_test.rs",
        "读的是 Helm 模板里的 YAML 数字与 `{{ }}` action 里的词，不是 Rust 双引号字符串；\
         模板里没有注释与转义的同一套规则，套用共享读法会把整段模板读成代码",
    ),
    (
        "crates/cogneva/tests/store_tables_are_in_the_restore_bundle_test.rs",
        "从 Rust 源码里取 `const NAME: &str = \"…\"` 声明，带块注释与嵌套括号；\
         它读的是「哪条声明」，不是「这一行里有几个字面量」",
    ),
    (
        "crates/cogneva/tests/web_event_vocabulary_contract_test.rs",
        "读的是 WebUI 的 JavaScript 单引号字面量：方言不同（`'` 是引号），\
         那边没有 Rust 的生命周期与原始字符串规则",
    ),
    (
        "crates/cog-gateway/tests/cluster_read_probe_test.rs",
        "读的是本测试夹具里那段抄进来的客户端源码常量 `SOURCE`（按 `\"` 切分取 \
         `/api` 前缀的路径），题目是夹具自己而不是生产源；cogneva 测试侧的共享读法\
         编不进 cog-gateway 的测试目标，这里也不存在第二份生产读法可替代",
    ),
];

/// 至少读到这么多份夹具源码：整棵树的 `tests/` 有两百多份，读少了说明走目录那段停了。
const SOURCE_FLOOR: usize = 200;

/// 至少这么多份夹具已经改读共享实现：读法只剩一份的那一半，得有人真的在用。
const USER_FLOOR: usize = 8;

/// 至少还要读到这么多个「另一种方言」的读取器：登记表里的每一条都必须真是读取器。
const OTHER_FLOOR: usize = 4;

/// 一份夹具里定义的、读字面量的自由函数。
///
/// 按名字判而不是按签名判：抄件最常换的就是参数类型（整段文本、一行、一串
/// `&Path` 读进来的源码），而名字几乎总是从「literal」那一族里取的。按名字判的
/// 边界写在 [`OTHER_READERS`]：碰到同名不同物的，登记一条理由。
fn reader_definitions(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        let Some(rest) = trimmed.strip_prefix("fn ") else {
            continue;
        };
        let name = rest.split('(').next().unwrap_or("").trim();
        let reads_literals = name.starts_with("literals")
            || name.starts_with("literal_")
            || name.starts_with("string_literals")
            || name.starts_with("quoted_strings")
            || name.starts_with("strings_on");
        if reads_literals {
            out.push(name.to_string());
        }
    }
    out
}

/// 每个 crate 的 `tests/` 下的全部 `*.rs`。
fn fixture_sources(root: &Path) -> Vec<PathBuf> {
    let crates = root.join("crates");
    let entries =
        std::fs::read_dir(&crates).unwrap_or_else(|e| panic!("{}: {e}", crates.display()));
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let tests = entry.path().join("tests");
        if tests.is_dir() {
            walk(&tests, &["rs"], &mut out);
        }
    }
    out.sort();
    out
}

#[test]
fn every_literal_reader_is_the_one_in_the_shared_module() {
    let root = repo_root();
    let sources = fixture_sources(&root);
    assert!(
        sources.len() >= SOURCE_FLOOR,
        "只读到 {} 份夹具源码（下限 {SOURCE_FLOOR}）：走目录那段停了，\
         下面「没有再长出一份读取器」是在一个空集上说的",
        sources.len()
    );

    let registered: BTreeSet<&str> = OTHER_READERS.iter().map(|(file, _)| *file).collect();
    let mut users = 0;
    let mut readers = 0;
    let mut unexpected: Vec<String> = Vec::new();

    for path in &sources {
        let file = relative_name(&root, path);
        if file == HOME {
            continue;
        }
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        if text.contains("declared_once::") {
            users += 1;
        }
        let definitions = reader_definitions(&text);
        if definitions.is_empty() {
            continue;
        }
        readers += 1;
        // 定义本身就是第二份：改读共享实现的意思是没有本地定义，不是多一句 import。
        if !registered.contains(file.as_str()) {
            unexpected.push(format!("  {file}: {}", definitions.join(", ")));
        }
    }

    assert!(
        unexpected.is_empty(),
        "这些夹具定义了自己的字面量读取器。读法只许有一份：\
         `common::declared_once::literals_in`（整字面量、认转义、行内每一个都读）；\
         同一行在两份读取器下给出不同答案时，没有任何东西会红——用其中一份判的\
         「全都合规」，另一份根本没问。改读共享实现，或者在 OTHER_READERS 里写明\
         这里读的是另一种方言：\n{}",
        unexpected.join("\n")
    );
    assert!(
        readers >= OTHER_FLOOR,
        "只读到 {readers} 份仍带本地读取器的夹具（下限 {OTHER_FLOOR}）：\
         登记表里的条目没有一条指向真在的定义，「没有新的抄件」与「一段都没读到」同形"
    );
    assert!(
        users >= USER_FLOOR,
        "只有 {users} 份夹具在用 `declared_once::`（下限 {USER_FLOOR}）：\
         读法只剩一份的前提是有人真的在用它，否则「共享」只是另一个名字"
    );
}

/// 登记表里的每一条都必须还指向一份真带本地读取器的文件，且那份文件没有改读共享实现。
#[test]
fn a_registered_reader_that_is_gone_or_migrated_is_stale() {
    let root = repo_root();
    let mut stale = Vec::new();
    for (file, why) in OTHER_READERS {
        let path = root.join(file);
        let Ok(text) = std::fs::read_to_string(&path) else {
            stale.push(format!("  {file}: 文件不在了（{why}）"));
            continue;
        };
        if text.contains("declared_once::") {
            stale.push(format!("  {file}: 已经改读共享实现，登记该收回（{why}）"));
            continue;
        }
        if reader_definitions(&text).is_empty() {
            stale.push(format!("  {file}: 已经没有本地读取器了（{why}）"));
        }
    }
    assert!(
        stale.is_empty(),
        "这些登记已经不指向任何一份本地读取器：\n{}\n\
         登记是给一处现场发的许可，现场没了许可就该收回——留着它，\
         下一次有人在这里抄一份读取器时，这条登记会替他放行。",
        stale.join("\n")
    );
}

/// 读法自己也要有反证：共享读取器认得出转义、认得出一行里的第二处，\
/// 而这两点正是抄件们抄歪过的地方。
#[test]
fn the_shared_reader_sees_what_the_copies_missed() {
    use common::declared_once::literal_values;
    assert_eq!(
        literal_values(r#"let a = "a\"b"; let b = "c";"#),
        vec![r#"a\"b"#, "c"]
    );
    assert_eq!(literal_values(r#"let a = "x" + "y";"#), vec!["x", "y"]);
    assert_eq!(
        literal_values(r#"let a = "unterminated"#),
        Vec::<&str>::new()
    );
}
