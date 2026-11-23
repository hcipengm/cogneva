//! 员工页读的是人，不是角色，也不是能力。
//!
//! 属主 2026-11-20 看员工页连问三句：「员工还是和角色绑定？为啥有这么多角色，
//! 角色能不要的都不要」「每个智能体的能力都是一样的，为啥在员工里面要分能力」。
//! 能力那一列是角色名的抄件（`runtime/manager.rs` 的 `vec![role]`），已经在注册表
//! 一层整列删掉；角色这一列同样不是人的属性——角色是这一段活里的职务分工，写在
//! 任职（`task_attempt.role`）与账行上，同一个工号今天当 planner、明天当 generator
//! 还是同一个人。把角色画在人的行上，等于把「换一段活」读成「换一个人」。
//!
//! 判据是**闭集**：这一面渲染的列与读数各是一个点过名的集合，多一列、少一列都红。
//! 只断言「没有『角色』二字」不够——换一列名叫「专长」的同样不是人的属性，而闭集
//! 是唯一能说「这一页画的就是这些」的判据。判据不读注释（`strip_comments`）：改一句
//! 说明不该让它变红。

use std::path::{Path, PathBuf};

use crate::common;

fn root() -> PathBuf {
    common::root::repo_root()
}

/// 读一份前端源码，去掉注释。
fn source(root: &Path, rel: &str) -> String {
    common::routes::strip_comments(&common::fsio::read(&root.join(rel)))
}

/// 表头里每一个 `title: '...'`。条件列（对话）的标题是空串，照样收进来：
/// 空标题也是一列，闭集里给它留了位置。
fn column_titles(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find("title: '") {
        let start = from + at + "title: '".len();
        let len = text[start..]
            .find('\'')
            .unwrap_or_else(|| panic!("`title: '` 没有闭合的引号，位置 {start}"));
        out.push(text[start..start + len].to_string());
        from = start + len + 1;
    }
    out.sort();
    out
}

/// 每一处 `<Reading` 标签上的 `label="..."`。同一行里别的组件（`Strip` 也有
/// `label`）不算读数格：闭集管的是名册顶上那排数，不是页面上所有带 label 的东西。
fn reading_labels(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find("<Reading") {
        let start = from + at;
        let rest = &text[start..];
        let tag_end = rest.find('>').map(|i| start + i).unwrap_or(text.len());
        if let Some(label_at) = rest.find("label=\"") {
            let label_start = start + label_at + "label=\"".len();
            if label_start < tag_end {
                if let Some(len) = text[label_start..].find('"') {
                    out.push(text[label_start..label_start + len].to_string());
                }
            }
        }
        from = start + "<Reading".len();
    }
    out.sort();
    out
}

/// 名册表渲染的列是一个闭集：员工、状态、主机、Pod IP、最后一次心跳，加上条件
/// 出现的「对话」按钮列。角色与能力不在里面，任何一列以「人有什么属性」的口吻
/// 被加回来，这条就红。
///
/// 「帽子」（这个人此刻占着几个座位）也删了：属主 2026-11-22 判「帽子没有存在
/// 的必要，它能带来什么好处没看到」。地址数不画成列——主机与 Pod IP 两列只在
/// 这个人只占一个地址时才有意义，那两列自己判得出这件事。
#[test]
fn the_roster_table_renders_exactly_these_columns() {
    let text = source(&root(), "web/src/views/AgentsView.tsx");
    let titles = column_titles(&text);
    let mut expected = vec!["", "Pod IP", "主机", "最后一次心跳", "员工", "状态"];
    expected.sort();
    assert_eq!(
        titles,
        expected,
        "名册的列是闭集：多出来的是 {:?}。角色、能力（或任何换个名字的个人属性列）\
         都不是人这一页该画的东西——角色是这一段活里的职务，能力人人同一份",
        titles
            .iter()
            .filter(|title| !expected.contains(&title.as_str()))
            .collect::<Vec<_>>()
    );
}

/// 名册顶上那排读数也是一个闭集：人、地址、比快照新、没派人的座位。
/// 「N 种角色」与「每个角色在岗几人」两张角色读数已随角色列一起删掉：它们按角色
/// 把人分堆，而平台上的角色是活的职务，不是人的分类。
#[test]
fn the_roster_reads_exactly_these_figures() {
    let text = source(&root(), "web/src/views/AgentsView.tsx");
    let labels = reading_labels(&text);
    let mut expected = vec!["人", "地址", "比快照新", "没派人的座位"];
    expected.sort();
    assert_eq!(
        labels,
        expected,
        "名册的读数是闭集：多出来的读数是 {:?}；角色读数（「种角色」「每个角色几人」）\
         不该出现在人的页面上",
        labels
            .iter()
            .filter(|label| !expected.contains(&label.as_str()))
            .collect::<Vec<_>>()
    );
    for gone in ["种角色", "每个角色"] {
        assert!(
            !text.contains(gone),
            "名册里还有「{gone}」：按角色把人分堆的读数删干净，角色不是人的属性"
        );
    }
    // 「帽子」不是换个名字留着就行的词：属主判的是这个概念没有存在的必要。
    // 判的是去掉注释之后的源码——改一句说明不该让这一条变红，但把这一列换个
    // 名字加回来，或者把地址数换个说法再画一遍，都在这里红。
    assert!(
        !text.contains("帽子"),
        "员工页还有「帽子」：这个概念属主 2026-11-22 判过没有存在的必要，\
         占着几个地址由地址列自己数，不另画一格"
    );
}
