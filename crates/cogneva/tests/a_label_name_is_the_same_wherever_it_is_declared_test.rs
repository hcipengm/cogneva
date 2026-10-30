//! principle: feedback_no_hardcoding_config_surface
//!
//! 指标标签的名字是**写者与读者之间的接头**：写下 `LABEL_REASON = "reason"` 的那一刻，
//! 这个名字就成了一个 PromQL 选择器（`…{reason="…"}`）、一条告警的判据、一份报表的
//! 分组键。而这个常量被每个 crate 各定义了一遍：`LABEL_OUTCOME` 在七个 crate 里各有一行，
//! `LABEL_REASON` 在六个，`LABEL_CONSUMER` 在五个。它们今天都写着同一个词，因为当初是
//! 抄的。
//!
//! 抄出来的这些东西没有一处会比较它们。某个 crate 把 `LABEL_OUTCOME` 改成 `"result"`，
//! 编译不失败、测试不失败、发布照常——它发布的那条序列只是换了一个标签名字，于是所有
//! 按 `outcome=` 选行的规则对这条序列读到的是空。**空与「这一类从来没发生过」在读数上
//! 同形**，而「这条规则不再匹配任何东西」这件事本身没有读数。
//!
//! 所以本门把这几份拷贝摆在一起比：同一个 `const` 名字，在树上的每一处定义必须是同一个
//! 字符串。判据按「名字 → 取值」判，不要求它们合并成一处——合并是另一件事（那要动
//! `cog-core`），而本门要保证的是**分叉会当场红**，而不是分叉之后安静地活很久。
//!
//! 判不了什么：
//!
//! - 标签的**取值**（`OUTCOME_FAILED = "failed"` 这类）与指标名本身。它们同族但不同题：
//!   取值随枚举走，指标名在 `metric_names` 契约里，各有各的登记。
//! - 不同名字、同一个值（`LABEL_OUTCOME` 与 `OUTCOME_LABEL` 都写 `"outcome"`）。那不是
//!   分叉，是同一个词的两条别名，本门不管。
//! - 不叫 `*LABEL*` 的常量。本门只普查名字里带 `LABEL` 的那些——这是「标签名」在这棵树
//!   上的写法约定。

use crate::common;

use std::collections::BTreeMap;
use std::path::Path;

use common::routes::repo_root;
use common::rust_production::{production_lines, production_sources, relative_name};

/// 普查下限。测到的那一天，生产代码里名字带 `LABEL` 的字符串常量有 40 余处。
const MIN_DECLARATIONS: usize = 20;

/// 一个名字在树上被写成的全部取值，按文件记下来。
type Spellings = BTreeMap<String, Vec<(String, String)>>;

/// 从一行里认出 `const LABEL_FOO: &str = "foo";`，返回（名字，取值）。
///
/// 取值必须是一段字面量：`const X: &str = crate::LABEL_NODE;` 是引用，不是第二份拼法。
fn declaration(line: &str) -> Option<(String, String)> {
    let start = line.find("const ")? + "const ".len();
    let name: String = line[start..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if !name.contains("LABEL") {
        return None;
    }
    let value = line[start..].split_once('=')?.1.trim();
    let literal = value.strip_suffix(';')?.trim();
    let literal = literal.strip_prefix('"')?.strip_suffix('"')?;
    Some((name, literal.to_string()))
}

fn census(root: &Path) -> (usize, Spellings) {
    let files = production_sources(root);
    let mut spellings: Spellings = BTreeMap::new();
    let mut declarations = 0;
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, path);
        for line in production_lines(&text) {
            if let Some((name, value)) = declaration(line) {
                declarations += 1;
                spellings
                    .entry(name)
                    .or_default()
                    .push((file.clone(), value));
            }
        }
    }
    (declarations, spellings)
}

#[test]
fn a_label_name_is_spelt_the_same_in_every_crate_that_declares_it() {
    let (declarations, spellings) = census(&repo_root());
    assert!(
        declarations >= MIN_DECLARATIONS,
        "只读到 {declarations} 处标签名常量（下限 {MIN_DECLARATIONS}），这条判据没有题目"
    );

    let split: Vec<String> = spellings
        .iter()
        .filter(|(_, sites)| {
            sites
                .iter()
                .map(|(_, v)| v)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                > 1
        })
        .map(|(name, sites)| {
            let mut rendered = format!("  {name}");
            for (file, value) in sites {
                rendered.push_str(&format!("\n      {file}: {value:?}"));
            }
            rendered
        })
        .collect();

    assert!(
        split.is_empty(),
        "同一个标签名在树上有不止一种写法。这不是编译错误：改掉的那一处只是发布了一条\
         换了标签名的序列，所有按旧名字选行的规则对它读到空，而「空」与「这一类从来没\
         发生过」在读数上同形。\n{}",
        split.join("\n")
    );
}

#[test]
fn the_census_reads_a_second_spelling_and_ignores_a_reference_or_a_comment() {
    let declared = r#"const LABEL_OUTCOME: &str = "outcome";"#;
    assert_eq!(
        declaration(declared),
        Some(("LABEL_OUTCOME".to_string(), "outcome".to_string()))
    );

    // 引用同一份常量，不是第二份拼法。
    assert_eq!(
        declaration("const OUTCOME_LABEL: &str = crate::LABEL_OUTCOME;"),
        None
    );

    // 不叫 LABEL 的常量不在本门的题上。
    assert_eq!(declaration(r#"const TABLE: &str = "cog_events";"#), None);

    // 两处声明、两种取值就是分叉，本门报的那个形状。
    let a = declaration(r#"const LABEL_OUTCOME: &str = "outcome";"#).unwrap();
    let b = declaration(r#"const LABEL_OUTCOME: &str = "result";"#).unwrap();
    assert_eq!(a.0, b.0);
    assert_ne!(a.1, b.1);

    // 注释里提到同一个声明不是一处声明：普查读的是生产行，注释行在读之前就没了。
    let prose = "// const LABEL_OUTCOME: &str = \"outcome\";\nlet a = 1;\n";
    let read: Vec<_> = production_lines(prose)
        .into_iter()
        .filter_map(declaration)
        .collect();
    assert!(read.is_empty(), "注释行被当成了声明：{read:?}");
}
