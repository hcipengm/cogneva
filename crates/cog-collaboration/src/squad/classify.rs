//! 失败分类的单一权威表，以及「声明的分类是否真的可达」的自查判据。
//!
//! 一条链路上的失败分类有两个端点：产生端把分类写成带前缀的文本（Ralph、
//! pipeline、roundtable、squad executor 认的是同一批前缀），记录端把分类落成
//! 指标序列。两端各走各的路，就可能分叉——产生端已经声明了某个分类，中间的
//! 边界把它翻成一个不含前缀的常量 reason，于是那个分类的序列结构性恒为 0，
//! 而日志里对应事件一直在发生。这种自相矛盾过去只能靠外部评估者拿着日志和
//! 指标人工对账才发现。
//!
//! 表放在这里只有一份，分类判定与可达性自查不可能各说各话。声明在产生端计数，
//! 终止在记录端计数，"声明过却从未被记录"由观测面自己报出来——系统对自己
//! 提出问题，不等外部比对。

use std::collections::HashMap;

use cog_core::contract::outcome::{DEGENERATE_LOOP_PREFIX, TERMINAL_ENV_FAILURE_PREFIX};

/// 终止性环境/协议故障的分类标签，与 [`TERMINAL_ENV_FAILURE_PREFIX`] 配对。
pub const TERMINAL_ENV_FAILURE_CLASS: &str = "terminal_env_failure";

/// 退化环（花费买不到进展）的分类标签，与 [`DEGENERATE_LOOP_PREFIX`] 配对。
pub const DEGENERATE_LOOP_CLASS: &str = "degenerate_loop";

/// 兜底分类：没有声明前缀的不可恢复失败（重复同一失败、无可行动输出等）。
/// 它不对应任何前缀，因此不参与可达性比对——没有声明就谈不上"声明丢了"。
pub const UNCLASSIFIED_CLASS: &str = "unrecoverable";

/// 已声明的分类：wire 前缀 → 分类标签。判定与自查共用这一份，任何一边
/// 单独维护一份都会在改动时分叉。
pub const DECLARED_CLASSES: &[(&str, &str)] = &[
    (TERMINAL_ENV_FAILURE_PREFIX, TERMINAL_ENV_FAILURE_CLASS),
    (DEGENERATE_LOOP_PREFIX, DEGENERATE_LOOP_CLASS),
];

/// 文本声明了哪一个已声明的分类，没有声明则 `None`。
///
/// 判定交给前缀自身的归属层：reason 在到达记录端前会被各层错误类型加上
/// `"<上下文>: <内层>"` 的前缀，只认首字节会把带包装的声明读成"没有声明"，
/// 于是分类序列结构性恒为 0、事件却一直在发生——正是本模块要防的那种自相
/// 矛盾。各有各的匹配实现就等于各有各的盲区。
pub fn declared_in(text: &str) -> Option<&'static str> {
    DECLARED_CLASSES
        .iter()
        .find(|(prefix, _)| cog_core::contract::outcome::declares(text, prefix))
        .map(|(_, class)| *class)
}

/// 文本所属的分类标签；没有声明前缀的落 [`UNCLASSIFIED_CLASS`]。
/// 分类只从文本推，不另立判据：前缀本来就是全链路共用的 wire 标记
/// （squad executor 也按它决定不再升级策略），另立一套只会在两处之间分叉。
pub fn classify(reason: &str) -> &'static str {
    declared_in(reason).unwrap_or(UNCLASSIFIED_CLASS)
}

/// 声明「本次运行的失败属于 `class`」：记一次声明计数，文本原样返回。
/// 产生端在写出带前缀的 reason/feedback 时调用它；记录端在落指标时计数，
/// 两边一旦分叉，可达性自查就能说出"这个分类声明过却从没被记录"。
pub fn declare(class: &'static str, text: String) -> String {
    crate::observable::global_observable().announce_class(class);
    text
}

/// 声明过、却从未被记录成终止的分类：事件在发生而序列恒为 0。
/// 纯函数：只看两端的计数，不依赖现场状态。空集合不等于"没问题"——它只
/// 说明还没有声明可查，一旦有声明而记录缺失就会列出来。
pub fn unreachable_classes(
    announced: &HashMap<String, u64>,
    recorded: &HashMap<String, u64>,
) -> Vec<&'static str> {
    DECLARED_CLASSES
        .iter()
        .map(|(_, class)| *class)
        .filter(|class| {
            announced.get(*class).copied().unwrap_or(0) > 0
                && recorded.get(*class).copied().unwrap_or(0) == 0
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::squad::pge::types::no_artifacts_reason;

    #[test]
    fn classify_follows_the_shared_wire_prefixes() {
        // The exact reason the pipeline and Ralph both produce, and the exact
        // reason the squad executor keys off to stop upgrading strategies.
        assert_eq!(classify(&no_artifacts_reason()), TERMINAL_ENV_FAILURE_CLASS);
        assert_eq!(
            classify(&format!(
                "{TERMINAL_ENV_FAILURE_PREFIX}: generator prompt failed: HTTP 503"
            )),
            TERMINAL_ENV_FAILURE_CLASS
        );
        assert_eq!(
            classify(&format!("{DEGENERATE_LOOP_PREFIX}: flat across the window")),
            DEGENERATE_LOOP_CLASS
        );
        // Everything else still has to land somewhere countable.
        assert_eq!(
            classify("Ralph Loop stagnated: no progress signal"),
            UNCLASSIFIED_CLASS
        );
    }

    #[test]
    fn declared_in_reads_the_prefix_not_the_wording() {
        assert_eq!(
            declared_in(&format!("{DEGENERATE_LOOP_PREFIX}: anything at all")),
            Some(DEGENERATE_LOOP_CLASS)
        );
        assert_eq!(declared_in("degenerate debate loop detected"), None);
        assert_eq!(declared_in(""), None);
    }

    /// 声明在到达这里之前已经被上层错误类型包了一层上下文。只认首字节的
    /// 实现会把这种 reason 记成"未分类"，而分类序列恒为 0 恰恰会被自查读成
    /// "这个分类从没发生过"，掩盖事件一直在发生的事实。
    #[test]
    fn a_wrapped_reason_is_still_classified() {
        assert_eq!(
            classify(&format!(
                "Agent execution error: {TERMINAL_ENV_FAILURE_PREFIX}: generator produced no artifacts"
            )),
            TERMINAL_ENV_FAILURE_CLASS
        );
    }

    fn counts(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
    }

    #[test]
    fn a_class_announced_and_never_recorded_is_unreachable() {
        let announced = counts(&[(DEGENERATE_LOOP_CLASS, 3)]);
        let recorded = counts(&[]);
        assert_eq!(
            unreachable_classes(&announced, &recorded),
            vec![DEGENERATE_LOOP_CLASS]
        );
    }

    #[test]
    fn a_class_announced_and_recorded_is_fine() {
        let announced = counts(&[(DEGENERATE_LOOP_CLASS, 3), (TERMINAL_ENV_FAILURE_CLASS, 1)]);
        let recorded = counts(&[(DEGENERATE_LOOP_CLASS, 2)]);
        // terminal_env_failure was announced and never recorded: the check
        // names it even though a sibling class looks healthy.
        assert_eq!(
            unreachable_classes(&announced, &recorded),
            vec![TERMINAL_ENV_FAILURE_CLASS]
        );
    }

    #[test]
    fn no_declaration_means_nothing_to_report() {
        // Silence is not a contradiction: nothing was announced, so there is
        // no declared class whose series could be missing.
        assert!(unreachable_classes(&counts(&[]), &counts(&[])).is_empty());
        // Counters outside the declared axis (stagnated, budget_exhausted)
        // never produce a reachability verdict.
        let recorded = counts(&[("stagnated", 4), (UNCLASSIFIED_CLASS, 1)]);
        assert!(unreachable_classes(&counts(&[]), &recorded).is_empty());
    }
}
