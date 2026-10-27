//! principle: feedback_no_hardcoding_config_surface
//!
//! 指标标签的名字被重复声明成一个常量，而不是从一处导出：`OUTCOME_LABEL` 在
//! 八个文件里各写一遍，`DEPLOYMENT_LABEL` 在六个文件里各写一遍，
//! `SOURCE_LABEL` 在两个 crate 里各写一遍。写成常量本身是对的——总比在
//! `with_label("outcome", …)` 里散着拼好——但八份声明之间没有任何东西比较。
//!
//! 漂移的后果在读数面，而且是最难认出的那一种：改了一处，那一族指标就把自己
//! 归到一个别人不写的标签名下。按 `outcome="failed"` 汇总的面板读不到它，
//! PromQL 对匹配不到的标签一声不吭——空向量与「这一族什么都没发生」是同一个
//! 读数。
//!
//! 所以这里按名字判：**同名即同词**。两个 crate 里都叫 `OUTCOME_LABEL` 的常量，
//! 必须都写着 `"outcome"`。代码平台的词一样：`GITHUB_PLATFORM` 在网关与
//! `cog-github` 各声明一份，两份之间没有任何东西比较。
//!
//! 判不了什么：
//!
//! - 名字不同而意思相同的两个常量（`OUTCOME_LABEL` 与 `LABEL_OUTCOME`）。
//!   那要靠人看出它们是同一个标签，本门只比同名的；
//! - 标签名与它被用在哪个指标上对不对。把 `outcome` 记到 `state` 那一格，
//!   本门看不出来；
//! - 非 `const` 的写法（`let label = "outcome";`、直接写进 `with_label`）。
//!   这一族在树上确实存在：把「声明的词不许再被逐字拼一遍」当判据试过，实测
//!   命中五十多处，其中大多是同名的另一种东西（`"kind"` 是 k8s 清单字段、
//!   `"namespace"` 是 k8s 字段、`"deployment"` 是 kubectl 的资源名），所以本门
//!   不判这一面。
//!
//! 为什么只限名字命中 [`is_word_constant`] 的 `&str` 常量：同名的 `&str` 常量
//! 在别处**合法地**各不相同——`USAGE`、`CLAIM_ENV`、`ROOT_ENV`、`INSERT_SQL` 都是
//! 每个模块自己那一份，名字撞上而词不同是正当的（实测 30 组这样同名不同词）。
//! 标签名与平台名不是：它们在两处必须是同一个词，否则读数合不到一起、路由挑不中。
//!
//! 实测（2026-10-27）：生产代码里 63 个命中该判据的 `&str` 常量、41 个不同名字，
//! 其中 9 个名字被声明了不止一次（`OUTCOME_LABEL` 8 次、`DEPLOYMENT_LABEL` 6 次、
//! `GITHUB_PLATFORM` 3 次、`GITEE_PLATFORM` 3 次、`LABEL_READOUT` 3 次、
//! `SOURCE_LABEL` 2 次、`DIR_LABEL` 2 次、`NODE_LABEL` 2 次、`CHANGE_ID_TRAILER`
//! 2 次），落地之前每一个都是同一个词，一处矛盾也没有——除了 `CHANGE_ID_TRAILER`，
//! 它在收进本门时就已经分成 `"Change-Id"` 与 `"Change-Id: "` 两半，那一笔把它改回
//! 一个词。
//! 消融：把 `cog-memory` 的 `OUTCOME_LABEL` 改成 `"result"`，编译通过、本门红并
//! 点名 `OUTCOME_LABEL`；改回绿。把 `cog-gateway` 的 `GITEE_PLATFORM` 改成
//! `"gitee2"`，编译通过、本门红并点名 `GITEE_PLATFORM`；改回绿。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

mod common;

use common::routes::{repo_root, walk};
use common::rust_production::production_text;

/// 这个常量名是不是在宣称「我是某个词的正身」。
///
/// 名字里带 `LABEL`：指标标签名。两个 crate 里都叫 `OUTCOME_LABEL` 的常量必须都
/// 写着 `"outcome"`，否则按旧名字汇总的面板读到一个空向量。
///
/// 名字以 `_PLATFORM` 结尾：代码平台的词。两个 crate 各声明一份，路由、存储的配置
/// 与上报都按它挑分支；改一处会让那条路径落到兜底分支上。
///
/// 名字以 `_TRAILER` 结尾：写进提交信息的 git trailer 键。写出端与读回端在两个
/// crate 里，读回端读不到就当作「这条提交没有 trailer」——提交照样落地，只是台账
/// 从此对不上。这一族不是假想的：`CHANGE_ID_TRAILER` 在被本门收进来之前就已经
/// 分成两半，一处写 `"Change-Id"`、一处写 `"Change-Id: "`，而没有任何东西在比较。
///
/// 只判名字，是因为「这是同一个词的两份声明」在代码里没有别的形状可认。为什么不是
/// 所有同名 `&str` 常量：标签族之外同名不同词是正当的（见文件头实测）。为什么是
/// 后缀而不是 `PLATFORM` 子串：`PLATFORM_BASE_ENV`、`PLATFORM_REGISTRY_FILES` 这类
/// 前缀命名是各模块自己的环境变量名与路径，撞名而不同值是正当的。
fn is_word_constant(name: &str) -> bool {
    name.contains("LABEL") || name.ends_with("_PLATFORM") || name.ends_with("_TRAILER")
}

/// 一段生产代码里所有命中 [`is_word_constant`]、类型是 `&str` 的常量。
///
/// 类型不是 `&str` 的跳过（`ALERT_LABEL_VALUE_MAX_CHARS: usize` 就是一个名字
/// 撞上的数常量，它不是一个词）。类型对、值读不出来的按错误返回，不静默跳过：
/// 一处漏读就是一处不比较，而「少读了一个」与「那个常量没有矛盾」在结果上
/// 分不开。
fn word_constants(source: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("const ") {
        let keyword = cursor + offset;
        let at = keyword + "const ".len();
        cursor = at;
        // 注释掉的声明不是任何一个词的发源地：看这一行在它之前有没有 `//`。
        // 这里不调 `strip_comments`——那把 `'` 当字符串引号，遇到 `&'static str`
        // 会把等号后面的字面量一起吞掉。
        let line_start = source[..keyword].rfind('\n').map_or(0, |p| p + 1);
        if source[line_start..keyword].contains("//") {
            continue;
        }
        let name: String = source[at..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        // 不是「`const` 后面跟一个名字」的那些出现：`as *const ()` 这样的类型
        // 转换、`macro_rules!` 里的 `pub const $ident`。它们不是声明，跳过。
        if name.is_empty() || !is_word_constant(&name) {
            continue;
        }
        let Some(colon) = source[at..].find(':').map(|o| at + o) else {
            return Err(format!("`{name}` 后面没有 `:`：读不出来按红处理"));
        };
        let Some(eq) = source[colon..].find('=').map(|o| colon + o) else {
            return Err(format!("`{name}` 那一行没有 `=`：读不出来按红处理"));
        };
        let ty = source[colon + 1..eq].trim();
        if ty != "&str" && ty != "&'static str" {
            continue;
        }
        let rest = source[eq + 1..].trim_start();
        if !rest.starts_with('"') {
            // 转抄另一个常量（`pub const CLAIM_LABEL: &str =
            // cog_core::claim_footprint::CLAIM_LABEL;`）：两边由编译器拴着，
            // 不可能各自漂移，所以它不贡献自己的词。其余形状读不出来，按红。
            let value: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                .collect();
            if value.contains("::") && !value.is_empty() {
                continue;
            }
            return Err(format!(
                "`{name}` 是 `&str` 常量，但值既不是字面量也不是另一个常量的路径\
                 （等号后面是 `{}`）：读不出来按红处理",
                rest.chars().take(16).collect::<String>()
            ));
        }
        let Some(close) = rest[1..].find('"').map(|o| o + 1) else {
            return Err(format!("`{name}` 的字面量没有收尾：读不出来按红处理"));
        };
        out.push((name, rest[1..close].to_string()));
    }
    Ok(out)
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// `crates/*/src` 下的每一个 Rust 文件。
fn sources(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(root.join("crates")) else {
        return files;
    };
    for entry in entries.flatten() {
        let src = entry.path().join("src");
        if src.is_dir() {
            walk(&src, &["rs"], &mut files);
        }
    }
    files.sort();
    files
}

fn contradictions(root: &Path) -> Vec<String> {
    let mut complaints = Vec::new();
    let mut by_name: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
    let mut declared = 0usize;

    for path in sources(root) {
        let where_ = relative(root, &path);
        let Ok(raw) = std::fs::read_to_string(&path) else {
            complaints.push(format!("  `{where_}` 读不出来：读不出来按红处理"));
            continue;
        };
        let text = production_text(&raw);
        let declarations = match word_constants(&text) {
            Ok(declarations) => declarations,
            Err(complaint) => {
                complaints.push(format!("  `{where_}`：{complaint}"));
                continue;
            }
        };
        for (name, word) in declarations {
            by_name
                .entry(name)
                .or_default()
                .entry(word)
                .or_default()
                .push(where_.clone());
            declared += 1;
        }
    }

    for (name, words) in &by_name {
        if words.len() < 2 {
            continue;
        }
        let spellings: Vec<String> = words
            .iter()
            .map(|(word, sites)| format!("`{word}`（{}）", sites.join("、")))
            .collect();
        complaints.push(format!(
            "  `{name}` 被声明了 {} 次，写出了 {} 个不同的词：{}。\n    这个名字的两处声明 \
             必须是同一个词：标签名不同则那一族指标归到别人不写的标签名下，读数面上与\
             「这一族什么都没发生」是同一个空向量；平台名不同则那条路径落到兜底分支；\
             trailer 键不同则读回端当作没有这条 trailer，台账从此对不上",
            words.values().map(Vec::len).sum::<usize>(),
            words.len(),
            spellings.join("；")
        ));
    }

    if declared < 30 || by_name.len() < 20 {
        complaints.push(format!(
            "普查只读到 {declared} 个命中本门判据的 `&str` 常量、{} 个不同名字：读得太少，\
             本门判不了这一棵树，按红处理",
            by_name.len()
        ));
    }
    complaints
}

#[test]
fn a_label_constant_declared_twice_says_the_same_word_both_times() {
    let complaints = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "同名常量必须写着同一个词；它们是各文件各写一遍的，编译器不比较，改一处不会有\
         任何报错。\n{}",
        complaints.join("\n")
    );
}

// ── 夹具：临时树里放几个 crate，逐条验证红与绿 ──

fn fixture_complaints(files: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    contradictions(dir.path())
        .into_iter()
        .filter(|c| !c.contains("普查只读到"))
        .collect()
}

const ONE: &str = "crates/one/src/a.rs";
const TWO: &str = "crates/two/src/b.rs";

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
        (TWO, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

#[test]
fn a_label_constant_renamed_in_one_crate_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
        (TWO, "pub const OUTCOME_LABEL: &str = \"result\";\n"),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("OUTCOME_LABEL") && c.contains("result")),
        "{complaints:?}"
    );
}

/// 一个读不出来的声明不能被跳过：跳过就等于不比较，而不比较与「没有矛盾」
/// 在结果上是同一个绿。
#[test]
fn a_declaration_that_is_not_a_literal_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const OUTCOME_LABEL: &str = pick_label();\n"),
        (TWO, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
    ]);
    assert!(
        complaints.iter().any(|c| c.contains("OUTCOME_LABEL")),
        "{complaints:?}"
    );
}

/// 注释里的同名常量不算一次声明：它不是任何一个词的发源地。
#[test]
fn a_commented_out_declaration_does_not_count() {
    let complaints = fixture_complaints(&[(
        ONE,
        "// pub const OUTCOME_LABEL: &str = \"outcome\";\npub const X_LABEL: &str = \"x\";\n",
    )]);
    assert!(
        !complaints.iter().any(|c| c.contains("OUTCOME_LABEL")),
        "{complaints:?}"
    );
}

/// 转抄另一个常量的声明不是第二个词的来源：两边由编译器拴着，不可能各自漂移。
#[test]
fn a_re_export_of_another_constant_is_not_a_second_spelling() {
    let complaints = fixture_complaints(&[
        (
            ONE,
            "pub const CLAIM_LABEL: &str = crate::other::CLAIM_LABEL;\n",
        ),
        (TWO, "pub const CLAIM_LABEL: &str = \"claim\";\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

/// 代码平台的词一样按名字判：两处同名而词不同就是红。
#[test]
fn a_platform_word_renamed_in_one_crate_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const GITEE_PLATFORM: &str = \"gitee\";\n"),
        (TWO, "pub const GITEE_PLATFORM: &str = \"gitee2\";\n"),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("GITEE_PLATFORM") && c.contains("gitee2")),
        "{complaints:?}"
    );
}

/// 以 `PLATFORM_` 开头的常量是各模块自己的环境变量名与路径，不是某个词的正身：
/// 撞名而不同值是正当的，不许把它读成红。
#[test]
fn a_platform_prefixed_setting_is_not_a_word() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const PLATFORM_REGISTRY_FILES: &str = \"*.rs\";\n"),
        (TWO, "pub const PLATFORM_REGISTRY_FILES: &str = \"*.go\";\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

/// 提交信息上的 trailer 键一样按名字判：写出端与读回端各声明一份。
#[test]
fn a_trailer_key_renamed_in_one_crate_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "const CHANGE_ID_TRAILER: &str = \"Change-Id\";\n"),
        (TWO, "const CHANGE_ID_TRAILER: &str = \"Change-Id: \";\n"),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("CHANGE_ID_TRAILER") && c.contains("Change-Id: ")),
        "{complaints:?}"
    );
}

/// 名字撞上但不是词的常量（数上限、切片）不归本门管，也不许把它读成红。
#[test]
fn a_const_that_is_not_a_word_is_ignored() {
    let complaints = fixture_complaints(&[
        (
            ONE,
            "pub const ALERT_LABEL_VALUE_MAX_CHARS: usize = 1024;\n",
        ),
        (TWO, "pub const ALERT_LABEL_VALUE_MAX_CHARS: u32 = 32;\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

/// 普查本身也要有下限：一个常量都没读到时，「没有矛盾」是个空承诺。
#[test]
fn a_tree_that_declares_nothing_is_not_green() {
    let dir = tempfile::tempdir().expect("temp dir");
    let full = dir.path().join(ONE);
    std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
    std::fs::write(full, "pub fn nothing() {}\n").expect("write");
    let complaints = contradictions(dir.path());
    assert!(
        complaints.iter().any(|c| c.contains("普查只读到")),
        "{complaints:?}"
    );
}
