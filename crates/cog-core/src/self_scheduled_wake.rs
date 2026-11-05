//! 生产者与消费者靠一个标记互相认领：卡正文的开头写着它，领卡的一侧按它认。
//!
//! 两侧靠一段字面量对齐时，任何一侧单独改字都会改写另一侧的读数——它读成
//! 「这一轮没有卡」，与「确实没有卡」在输出上同形，绿得一样。所以这个标记在这里
//! 拼**一次**，写卡的一侧与领卡的一侧都引用 [`SELF_SCHEDULED_WAKE_MARKER`]；本
//! 文件末尾那一条门从树的形状回答：树里还剩几种拼法，剩下的那一种是不是这个常量。
//!
//! 门问的不是某张卡有没有被领走，而是这条规则真正说的那件事——有几处拼写、住在
//! 哪一行。前者是运行时的读数，后者在卡被写出来、被领走之前就有答案。

/// 自排唤醒卡正文开头的那个标记：写卡的一侧写它，领卡的一侧按它认领。
///
/// 全仓库唯一的一处拼写。两侧都从这里取，于是「某一侧自己改了字」不再可能发生：
/// 改这份常量是一次改动，落在两侧共用的同一个值上，而不是落在某一侧那份副本上、
/// 让另一侧继续读旧字。
pub const SELF_SCHEDULED_WAKE_MARKER: &str = "【自排唤醒】";

/// 把标记贴到一段正文的开头，得到一张自排唤醒卡该有的样子。
///
/// 不去重：正文开头已经有标记时再贴一次由调用方负责。在这里悄悄去掉一次会让
/// 「贴过两次」与「贴过一次」读起来一样，而那正是这条门要防的那种同形。
pub fn announce(body: &str) -> String {
    format!("{SELF_SCHEDULED_WAKE_MARKER}{body}")
}

/// 这段正文是不是一张自排唤醒卡：只看开头是不是那个标记。
///
/// 按前缀判而不按「包含」判：标记出现在正文中间是引用或转述，不是认领。
pub fn is_self_scheduled(body: &str) -> bool {
    body.starts_with(SELF_SCHEDULED_WAKE_MARKER)
}

#[cfg(test)]
mod tests {
    use super::{announce, is_self_scheduled, SELF_SCHEDULED_WAKE_MARKER};
    use std::path::{Path, PathBuf};

    /// 标记在 `crates/` 下只有一处拼写，而那一处就是这个常量。
    ///
    /// principle: feedback_a_marker_shared_by_producer_and_reader_breaks_when_either_side_rewords_it
    ///
    /// 问的是这条规则所禁止的那个动作的形状：第二处拼写，由先动手的那一侧在自己的
    /// 调用点上敲出来。走一遍源码就答了，答在卡被写出来、被领走之前——于是把标记
    /// 改成自己那份写法的生产者，会在构建里被拒，而不是在运行里变成一个没人应答的
    /// 唤醒。
    ///
    /// 针就是这个常量本身，绝不在这里再敲一遍字面量：门若自己另拼一份，标记被改字
    /// 的那天它照样是绿的，那正是它要抓的失败被翻到了里面。
    #[test]
    fn the_wake_marker_is_spelled_once_and_that_spelling_is_the_constant() {
        let root = crate::checkout::repo_root().join("crates");
        let mut sources = Vec::new();
        collect_sources(&root, &mut sources);
        sources.sort();
        assert!(
            !sources.is_empty(),
            "{} 下一份 `.rs` 都没读到：读空的遍历与「只有一处拼写」在输出上同形",
            root.display()
        );

        let mut hits = Vec::new();
        for path in &sources {
            let text = std::fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("读 {} 失败：{err}", path.display()));
            for (index, line) in text.lines().enumerate() {
                if line.contains(SELF_SCHEDULED_WAKE_MARKER) {
                    hits.push((path.clone(), index + 1, line.trim().to_string()));
                }
            }
        }

        assert_eq!(
            hits.len(),
            1,
            "这个标记在 `crates/` 下被拼了几处，两侧就不再靠同一个值认领：{hits:#?}"
        );

        // 活下来的那一行必须住在本模块里：别处的那一份哪怕今天字一样，也只是
        // 另一侧手里的副本，改起来与这一侧没有关系。
        let (path, line, text) = &hits[0];
        assert!(
            path.ends_with(file!()),
            "标记的唯一一处拼写在 {}，不在本模块：两侧都在拼字面量，而不是引用常量",
            path.display()
        );
        assert!(
            text.starts_with("pub const ") && text.contains("SELF_SCHEDULED_WAKE_MARKER: &str"),
            "{} 第 {line} 行在常量声明之外拼了标记，于是调用点上又多了一份副本：{text}",
            path.display()
        );

        // 两端在同一个值上对齐，而不是在两段今天恰好相同的字面量上对齐：不含标记的
        // 正文不是卡，生产者写出来的东西正是读者认得的东西。
        let body = "这一轮没做完的三件事，挑一件接着做";
        assert!(
            !is_self_scheduled(body),
            "不以此标记开头的正文不是自排唤醒卡，读者却认了：{body}"
        );
        let card = announce(body);
        assert!(
            is_self_scheduled(&card),
            "读者不认识生产者写出来的卡片，说明两端认的不是同一个值：{card}"
        );
    }

    /// 这一层目录下的全部 `.rs`，顺序由文件系统给，调用方自己排。
    fn collect_sources(dir: &Path, out: &mut Vec<PathBuf>) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|err| panic!("列 {} 失败：{err}", dir.display()));
        for entry in entries {
            let entry = entry.unwrap_or_else(|err| panic!("列 {} 失败：{err}", dir.display()));
            let path = entry.path();
            if path.is_dir() {
                collect_sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
}
