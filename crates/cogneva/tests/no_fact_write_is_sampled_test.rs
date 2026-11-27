//! 事实面上不许采样：采样只在传输/观测面，且点名的文件就是那几处。
//!
//! 一条「记了 1 条代表 N 条」的样本若没有「本行代表 N 条」这一格，读的人会把它
//! 当成全量。今天的事实写入口没有采样；这条门把「没有」变成一句可判的话：生产代码
//! 里出现采样词的文件必须在名单里，新加一处就红，红的人要么说明它只在传输面，
//! 要么给样本补上「代表多少条」。

use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;

/// 允许出现采样词的文件与理由：要么是迹的传输面，要么是读数（不是事实写入口）。
const DECLARED_SAMPLING: &[(&str, &str)] = &[
    (
        "crates/cog-core/src/contract/observability.rs",
        "OTLP 迹标志的解析与编码，传输面",
    ),
    ("crates/cog-observability/src/traces.rs", "迹的传播，传输面"),
    (
        "crates/cog-reflection/src/change_execution.rs",
        "沿用父迹的采样标志，传输面",
    ),
    (
        "crates/cog-reflection/src/promotion_trend.rs",
        "周趋势的抽样读数，不是事实写入",
    ),
];

const SAMPLING_WORDS: &[&str] = &["sample_rate", "sampling_ratio", "sampled:"];

#[test]
fn a_sampling_site_outside_the_transport_list_is_refused() {
    let root = repo_root();
    let mut offenders = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        if DECLARED_SAMPLING.iter().any(|(path, _)| *path == relative) {
            continue;
        }
        for word in SAMPLING_WORDS {
            if text.contains(word) {
                offenders.push(format!("{relative}: {word}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "这几处出现了采样词，而不在传输面名单里。事实不许采样；若它只是传输/观测面，\
         把文件加进 DECLARED_SAMPLING 并写清理由：\n{}",
        offenders.join("\n")
    );
}
