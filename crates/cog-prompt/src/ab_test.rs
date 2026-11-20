//! Prompt A/B 测试 — 流量分割 + 效果对比 + 自动择优。
//! 同一 `domain:purpose` 下可存在多个 `PromptVariant`，按 `traffic_split`
//! 比例随机分配。`cog-eval` 对比各 variant 的指标后，`PromptManager`
//! 自动提升表现最好的 variant 为默认版本。

use rand::Rng;
use serde::{Deserialize, Serialize};

/// A/B 测试分组。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbTestGroup {
    pub name: String,
    pub description: String,
    pub variants: Vec<PromptVariant>,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    pub winner: Option<String>,
}

/// Prompt 变体（A/B 测试的一个分支）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptVariant {
    pub id: String,
    pub key: String,
    pub content: String,
    pub traffic_split: f64, // 0.0 ~ 1.0
    pub metrics: VariantMetrics,
}

/// 变体指标。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VariantMetrics {
    pub impressions: u64,
    pub successes: u64,
    pub avg_score: f64,
    pub avg_latency_ms: u64,
}

/// A/B 测试配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbTestConfig {
    pub min_samples: u64,
    /// 没有 `confidence_threshold`：`pick_winner` 比的是成功率，从来没有算过
    /// 置信区间，一个没人算的显著性阈值只会让配置面声称一件代码没做的事。
    pub auto_promote: bool,
}

// 五十次曝光：一个变体的成功率在五十次以内只能分辨几十个百分点的差别，再少
// 胜出者就可能只是运气；这个数是「够不够比」的门，不是统计显著性的门。
// cap: bound — 五十次：再少分辨不出几十个百分点的差别，胜出者可能只是运气
const AB_WINNER_MIN_SAMPLES: u64 = 50;

impl Default for AbTestConfig {
    fn default() -> Self {
        Self {
            min_samples: AB_WINNER_MIN_SAMPLES,
            auto_promote: false,
        }
    }
}

/// 根据 traffic_split 随机选择一个变体。
pub fn select_variant(variants: &[PromptVariant]) -> Option<&PromptVariant> {
    if variants.is_empty() {
        return None;
    }
    let mut rng = rand::thread_rng();
    let roll: f64 = rng.gen();
    let mut cumulative = 0.0;
    for v in variants {
        cumulative += v.traffic_split;
        if roll <= cumulative {
            return Some(v);
        }
    }
    // Fallback to last variant if rounding errors occur
    variants.last()
}

/// 判断哪个变体获胜（简单成功率比较，生产环境应使用统计检验）。
pub fn pick_winner(variants: &[PromptVariant], config: &AbTestConfig) -> Option<String> {
    let mut best: Option<(&PromptVariant, f64)> = None;
    for v in variants {
        if v.metrics.impressions < config.min_samples {
            continue; // 样本不足
        }
        let rate = v.metrics.successes as f64 / v.metrics.impressions.max(1) as f64;
        match best {
            None => best = Some((v, rate)),
            Some((_, best_rate)) if rate > best_rate => best = Some((v, rate)),
            _ => {}
        }
    }
    best.map(|(v, _)| v.id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 胜出门槛只有一处声明：默认值读常量，源码面 `impl Default` 的字段里不再有
    /// 数字。`confidence_threshold` 已删除——`pick_winner` 从来没有算过它。
    #[test]
    fn the_ab_winner_threshold_reads_its_name() {
        assert_eq!(AbTestConfig::default().min_samples, AB_WINNER_MIN_SAMPLES);
        // 针不在这一行里原样出现：否则这条判据读到的就是它自己。
        let needle = concat!("confidence_", "threshold: f64");
        assert!(
            !include_str!("ab_test.rs").contains(needle),
            "没人算的显著性阈值不许留在配置面上"
        );
        let body = include_str!("ab_test.rs")
            .split("impl Default for AbTestConfig")
            .nth(1)
            .expect("the impl is in this file")
            .split("\n}\n")
            .next()
            .unwrap();
        let bare: Vec<&str> = body
            .lines()
            .filter(|line| {
                line.split_once(':').is_some_and(|(field, value)| {
                    let value = value.trim().trim_end_matches(',');
                    !field.is_empty()
                        && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                        && value.starts_with(|c: char| c.is_ascii_digit())
                        // 哨兵 0 与空串不是旋钮，是「没写」。
                        && value != "0"
                })
            })
            .map(str::trim)
            .collect();
        assert!(bare.is_empty(), "默认值要读具名常量：{bare:?}");
    }

    #[test]
    fn test_select_variant_deterministic() {
        let variants = vec![
            PromptVariant {
                id: "a".into(),
                key: "test".into(),
                content: "A".into(),
                traffic_split: 0.5,
                metrics: Default::default(),
            },
            PromptVariant {
                id: "b".into(),
                key: "test".into(),
                content: "B".into(),
                traffic_split: 0.5,
                metrics: Default::default(),
            },
        ];
        let selected = select_variant(&variants);
        assert!(selected.is_some());
    }
}
