//! principle: feedback_the_unit_of_work_is_the_batch_not_the_turn
//!
//! 从「想做什么」到「落没落」的那条链，按阶段给读数：`goal → plan → change → landed`。
//!
//! 只有分段计数才答得出「哪一段在漏」：整体成功率把「计划得太多」与「生成不出来」与
//! 「生成了落不下去」加成一个数，而三者要做的事完全不同。分母也要说清楚——计划过的
//! 目标、生成的变更、以及**真正落地**的变更，是三个不同的集合。

/// 一条链的四段计数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LandingChain {
    pub goals: u64,
    pub plans: u64,
    pub changes: u64,
    pub landed: u64,
}

impl LandingChain {
    /// 生成阶段的转化率；计划为 0 时是 `None`，不是 0——「没有计划」与「计划全落空」
    /// 要做的处置相反。
    pub fn to_changes(&self) -> Option<f64> {
        if self.plans == 0 {
            return None;
        }
        Some(self.changes as f64 / self.plans as f64)
    }

    /// 落地阶段：生成了的变更里有多少真落了。
    pub fn to_landed(&self) -> Option<f64> {
        if self.changes == 0 {
            return None;
        }
        Some(self.landed as f64 / self.changes as f64)
    }

    /// 哪一段在漏：返回转化率最低、且分母非零的那一段；都一样就返回 `None`。
    pub fn weakest_stage(&self) -> Option<&'static str> {
        let mut stages: Vec<(&'static str, f64)> = Vec::new();
        if let Some(r) = self.to_changes() {
            stages.push(("plan→change", r));
        }
        if let Some(r) = self.to_landed() {
            stages.push(("change→landed", r));
        }
        stages.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        match stages.first() {
            Some((name, low)) if stages.len() > 1 && *low < stages[1].1 => Some(*name),
            Some((name, _)) if stages.len() == 1 => Some(*name),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 生成这一段落空：读数指得出是它。
    #[test]
    fn the_stage_that_leaks_is_named() {
        let chain = LandingChain {
            goals: 10,
            plans: 10,
            changes: 2,
            landed: 2,
        };
        assert_eq!(chain.to_changes(), Some(0.2));
        assert_eq!(chain.to_landed(), Some(1.0));
        assert_eq!(chain.weakest_stage(), Some("plan→change"));
    }

    /// 分母为零的那一段没有率：不能拿 0 冒充「全落空」。
    #[test]
    fn a_stage_with_no_input_has_no_rate() {
        let chain = LandingChain {
            goals: 5,
            plans: 0,
            changes: 0,
            landed: 0,
        };
        assert_eq!(chain.to_changes(), None);
        assert_eq!(chain.to_landed(), None);
        assert_eq!(chain.weakest_stage(), None);
    }

    /// 两段一样低：说不出谁在漏，就不点名。
    #[test]
    fn two_equally_low_stages_name_neither() {
        let chain = LandingChain {
            goals: 4,
            plans: 4,
            changes: 2,
            landed: 1,
        };
        assert_eq!(chain.weakest_stage(), None);
    }
}
