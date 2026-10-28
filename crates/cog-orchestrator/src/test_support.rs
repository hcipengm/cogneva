//! 单测共用的样例：把几行「路径 ＋ 文本」解析成一份声明账。
//!
//! 它不是任何一处的题目，是两个测量模块都要先摆好的东西——`principle_ledger` 与
//! `principle_measure` 各写了一遍同一个 `declarations`，`Declarations::parse` 的入参
//! 形状一变，两份不会同时红。

use crate::principle_measure::Declarations;

/// 把 `(路径, 文本)` 的行解析成一份声明账。
pub(crate) fn declarations(lines: &[(&str, &str)]) -> Declarations {
    Declarations::parse(
        &lines
            .iter()
            .map(|(path, text)| (path.to_string(), text.to_string()))
            .collect::<Vec<_>>(),
    )
}
