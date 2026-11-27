//! 每一类事件都要说得出谁在写、谁在读；缺席要写明原因。
//!
//! 数表回答不了这件事：写者可能藏在动态构造里，读面可能按清单传入。所以答案是
//! 一份普查表，门只判两件可机判的事：闭集里每个词都有一行；点名的路径在树里存在。
//! 缺席（`writer: None` / `reader: None`）必须带一句原因——「还没有」本身是读数。

use crate::common::routes::repo_root;
use cog_core::contract::event_kinds::EVENT_CLASS_CENSUS;

#[test]
fn every_closed_set_word_has_one_census_row() {
    let listed: std::collections::BTreeSet<&str> =
        EVENT_CLASS_CENSUS.iter().map(|row| row.kind).collect();
    let closed: std::collections::BTreeSet<&str> = cog_core::contract::event_envelope::KINDS
        .into_iter()
        .collect();
    assert_eq!(
        listed.len(),
        EVENT_CLASS_CENSUS.len(),
        "普查表里同一个 kind 出现了两行"
    );
    assert_eq!(listed, closed, "普查表与闭集不是同一组词");
}

#[test]
fn every_named_writer_and_reader_exists_and_every_absence_says_why() {
    let root = repo_root();
    let mut problems = Vec::new();
    for row in EVENT_CLASS_CENSUS {
        match row.writer {
            Some(path) if !root.join(path).exists() => {
                problems.push(format!("{}: 写者 {path} 在树里不存在", row.kind));
            }
            None if row.why.trim().is_empty() => {
                problems.push(format!("{}: 没有写者，也没说为什么", row.kind));
            }
            _ => {}
        }
        match row.reader {
            Some(path) if !root.join(path).exists() => {
                problems.push(format!("{}: 读者 {path} 在树里不存在", row.kind));
            }
            None if row.why.trim().is_empty() => {
                problems.push(format!("{}: 没有读者，也没说为什么", row.kind));
            }
            _ => {}
        }
    }
    assert!(
        problems.is_empty(),
        "普查表没有说清：\n{}",
        problems.join("\n")
    );
}
