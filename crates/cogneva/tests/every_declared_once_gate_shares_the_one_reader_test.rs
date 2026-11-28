//! principle: feedback_no_hardcoding_config_surface
//!
//! 「从一行生产代码里读出双引号字面量」这条读法，曾经在夹具里抄了十一遍。
//! 每一遍都只参数化那一小段真正不同的东西，于是抄出来的每一份都能单独改歪：
//! 有的把 `\"` 当成字面量的结尾，有的按字节读、把非 ASCII 的半个字符也算进词里，
//! 有的只读到一行里的第一个字面量。两份对同一行给出不同答案时，没有任何东西会红——
//! 用其中一份判出来的「全都合规」，另一份根本不会去问。
//!
//! 所以读法只留一份，在 dev-only 的 `cog-test-support` 里
//! （`cog_test_support::literals_in` / `literal_values`）；控制律也只留一份：
//! `common::declared_once::Wall` / `Report`。本门管的是**不再长出第二份**：
//! 走遍每个 crate 的 `tests/`，以及每个 crate `src/` 文件里内联 `#[cfg(test)]`
//! 的测试半边，任何定义了「参数是 `&str`、名字里带 literal」的自由函数的文件，
//! 要么已经改读共享实现（文件里出现 `declared_once::` 或
//! `cog_test_support::literal`），要么在 [`OTHER_READERS`] 里写明它读的是另一种
//! 方言、另一种形状。登记了却不再有定义的条目按过期报出来——一张通行证留在没人走
//! 的路口，下一个真的抄一遍的人就没有判据拦。
//!
//! `src/` 的内联测试半边曾经不在人口里：`bootstrap` 的 `Key=value` 读取器就住在
//! 那里的 `#[cfg(test)]` 模块里，普查看不见它。测试支撑 crate 能编进任何 crate 的
//! 测试构建（dev-dependency），所以「编不进它」不是理由；`bootstrap` 现在读共享
//! 实现，`takeover` 的单引号读取器登记在 [`OTHER_READERS`]。
//!
//! 判不了什么：
//!
//! * 生产侧（`#[cfg(test)]` 之外）的解析器：`cog-reflection::config_reachability` 的
//!   `literal_field_assignment` 判的是「整行右值是不是一个字面量」，不枚举、不认
//!   转义；`bootstrap` 的生产代码读 `/etc/os-release`、apt sources 与 kubespray
//!   输出；`takeover` 发的是静态 HTML。它们问的是别的形状，不是「这一行里有几个
//!   字面量」。生产侧的硬编码由各道墙按全树生产源判。
//! * 「同一个词读出来之后怎么比」。本门只判读法有几份，判词在不在每道墙自己的
//!   拼法与登记表里。

use std::collections::BTreeSet;
use std::path::Path;

use crate::common;

use common::routes::{repo_root, walk};
use common::rust_production::{production_line_mask, relative_name};

/// 共享读法的家；它自己定义 `literals_in` / `literal_values`，是这份实现的入口。
///
/// 它住在这里而不是 `cog-core`：core 是契约宪法，不实现业务逻辑，也不做词法扫描；
/// 住在 dev-only 的支撑 crate 里，生产侧就不会被这份读法拖成依赖。
const HOME: &str = "crates/cog-test-support/src/literals.rs";

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
        "crates/cog-gateway/src/takeover.rs",
        "读的是自包含页面里 JavaScript 的单引号字面量（`$('m-events')`）：方言不同\
         （`'` 是引号），且它要按括号配平取 `$()` 的整段实参，不是「这一行里有几个\
         Rust 双引号字面量」",
    ),
];

/// 至少读到这么多份夹具源码：整棵树的 `tests/` 有四百多份，读少了说明走目录那段停了。
const SOURCE_FLOOR: usize = 200;

/// 至少读到这么多份带内联 `#[cfg(test)]` 的 `src/` 源码（实测 500 余份）。
///
/// 下限单立一条：两个人口合在一个计数里时，`tests/` 读到多少都能把「`src/` 一份
/// 没读到」盖过去，而这一半正是这次补上的缺口。
const INLINE_FLOOR: usize = 400;

/// 至少这么多份夹具已经改读共享实现：读法只剩一份的那一半，得有人真的在用。
const USER_FLOOR: usize = 8;

/// 至少还要读到这么多个「另一种方言」的读取器：登记表里的每一条都必须真是读取器。
const OTHER_FLOOR: usize = 4;

/// 一份要读的源码：相对名与这一次要读的文本。
///
/// `src/` 的文件只放测试半边——生产侧的字面量解析器是另一件事（见文件头），把整份
/// 算进来会把生产解析器误判成「又一份夹具读法」。
struct Reading {
    file: String,
    text: String,
}

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

/// 每个 crate 的 `tests/` 下的全部 `*.rs`，以及每个 crate `src/` 里内联
/// `#[cfg(test)]` 的测试半边。
fn fixture_sources(root: &Path) -> (Vec<Reading>, Vec<Reading>) {
    let crates = root.join("crates");
    let entries =
        std::fs::read_dir(&crates).unwrap_or_else(|e| panic!("{}: {e}", crates.display()));
    let mut fixtures = Vec::new();
    let mut inline = Vec::new();
    for entry in entries.flatten() {
        for (dir, into) in [
            (entry.path().join("tests"), &mut fixtures),
            (entry.path().join("src"), &mut inline),
        ]
        .into_iter()
        {
            if !dir.is_dir() {
                continue;
            }
            let mut paths = Vec::new();
            walk(&dir, &["rs"], &mut paths);
            paths.sort();
            for path in paths {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                let file = relative_name(root, &path);
                if dir.ends_with("src") {
                    let half = test_half(&text);
                    if !half.trim().is_empty() {
                        into.push(Reading { file, text: half });
                    }
                } else {
                    into.push(Reading { file, text });
                }
            }
        }
    }
    (fixtures, inline)
}

/// 一份源码里内联 `#[cfg(test)]` 的那一半。
///
/// 边界由 `rust_production::production_line_mask` 算：它已经知道 `#[cfg(test)]`
/// 可以装点一个条目、也可以开一个模块，不在每个普查里各切一次。
fn test_half(text: &str) -> String {
    let production = production_line_mask(text);
    text.lines()
        .enumerate()
        .filter(|(index, _)| !production.get(*index).copied().unwrap_or(true))
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn every_literal_reader_is_the_one_in_the_shared_module() {
    let root = repo_root();
    let (fixtures, inline) = fixture_sources(&root);
    assert!(
        fixtures.len() >= SOURCE_FLOOR,
        "只读到 {} 份夹具源码（下限 {SOURCE_FLOOR}）：走目录那段停了，\
         下面「没有再长出一份读取器」是在一个空集上说的",
        fixtures.len()
    );
    assert!(
        inline.len() >= INLINE_FLOOR,
        "只读到 {} 份带内联测试的 `src/` 源码（下限 {INLINE_FLOOR}）：`src/` 那一半\
         没走到，「测试半边里也没有第二份读法」是在一个空集上说的",
        inline.len()
    );

    let registered: BTreeSet<&str> = OTHER_READERS.iter().map(|(file, _)| *file).collect();
    let mut users = 0;
    let mut readers = 0;
    let mut unexpected: Vec<String> = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();

    for reading in fixtures.iter().chain(inline.iter()) {
        if reading.file == HOME {
            continue;
        }
        if reading.text.contains("declared_once::")
            || reading.text.contains("cog_test_support::literal")
        {
            users += 1;
        }
        let definitions = reader_definitions(&reading.text);
        if definitions.is_empty() {
            continue;
        }
        readers += 1;
        // 定义本身就是第二份：改读共享实现的意思是没有本地定义，不是多一句 import。
        if !registered.contains(reading.file.as_str()) {
            unexpected.push(format!("  {}: {}", reading.file, definitions.join(", ")));
        } else if let Some(file) = OTHER_READERS
            .iter()
            .map(|(file, _)| *file)
            .find(|file| *file == reading.file.as_str())
        {
            seen.insert(file);
        }
    }

    // 登记表里的一条必须真的在人**口**里被读到，而不只是「整份文件里有这个定义」。
    // `src/` 的测试半边若没被剥出来，那一条登记就成了一块没走到路口的通行证：
    // 它不红，因为它从没被问过。
    let unseen: Vec<&str> = registered
        .iter()
        .copied()
        .filter(|file| !seen.contains(file))
        .collect();
    assert!(
        unseen.is_empty(),
        "这些登记的读取器在普查的人口里没有读到：{unseen:?}\n\
         `tests/` 与 `src/` 的测试半边是两个人口，登记表却只有一个；条目不指向\
         人口里的定义时，「这里读的是另一种方言」是在替一处从没被看过的现场作保。"
    );

    assert!(
        unexpected.is_empty(),
        "这些夹具（或 `src/` 的内联测试半边）定义了自己的字面量读取器。读法只许有\
         一份：`cog_test_support::literals_in`（整字面量、认转义、行内每一个都读），\
         `common::declared_once` 只是把它转给用 `common` 的夹具；同一行在两份读取器\
         下给出不同答案时，没有任何东西会红——用其中一份判的「全都合规」，另一份\
         根本没问。改读共享实现，或者在 OTHER_READERS 里写明这里读的是另一种方言：\n{}",
        unexpected.join("\n")
    );
    assert!(
        readers >= OTHER_FLOOR,
        "只读到 {readers} 份仍带本地读取器的夹具（下限 {OTHER_FLOOR}）：\
         登记表里的条目没有一条指向真在的定义，「没有新的抄件」与「一段都没读到」同形"
    );
    assert!(
        users >= USER_FLOOR,
        "只有 {users} 份夹具在用共享读法（`declared_once::` 或 \
         `cog_test_support::literal`，下限 {USER_FLOOR}）：读法只剩一份的前提是有人\
         真的在用它，否则「共享」只是另一个名字"
    );

    // 反过来问：共享读法真的还在那个家里吗。搬走之后忘了更新 HOME，上面那条
    // 「别处没有第二份」照样成立，只是它指着的文件已经不在干这件事。
    let home =
        std::fs::read_to_string(root.join(HOME)).unwrap_or_else(|e| panic!("{HOME} 读不到：{e}"));
    for entry in ["pub fn literals_in(", "pub fn literal_values("] {
        assert!(
            home.contains(entry),
            "{HOME} 里不再定义 {entry}：那么「读法只有一份」就是指着\
             一份不干这件事的文件说的"
        );
    }
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
    use cog_test_support::literal_values;
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
