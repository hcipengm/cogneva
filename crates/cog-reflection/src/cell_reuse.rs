//! principle: feedback_the_unit_of_work_is_the_batch_not_the_turn
//!
//! 验证的**格**：一次改动要跑哪些、哪些可以复用。
//!
//! 一个格是「一个 crate 的一组测试目标」。改一个 crate，别的 crate 的格与这次改动无关，
//! 它们的绿与这一批之前那次跑出来的绿是同一件事——重跑付的是同一份验证的第二遍钱。
//!
//! 判据只有一句：**只跑输入变了的格**。所以这里把「哪些格要跑」算成读得出来的东西，
//! 而不是执行者临场决定要不要省。

/// 一次跑要动哪些格。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CellPlan {
    /// 输入变了、必须重跑。
    pub rerun: Vec<String>,
    /// 输入没变、可以复用上一次的结论。
    pub reused: Vec<String>,
}

impl CellPlan {
    /// 复用的那些占全体的比例；一个格都没有时是 `None`——「没格子」与「全都复用了」
    /// 不是同一个数。
    pub fn reused_ratio(&self) -> Option<f64> {
        let total = self.rerun.len() + self.reused.len();
        if total == 0 {
            return None;
        }
        Some(self.reused.len() as f64 / total as f64)
    }

    /// 这次是否白跑了：一条都没变却仍然重跑了格子。
    pub fn reran_without_input(&self) -> bool {
        self.reused.is_empty() && !self.rerun.is_empty()
    }
}

/// 每个格的名字形如 `<crate>:<target>`；`changed` 是变了的 crate 名。
///
/// 名字里没有 `:` 的格读不出它属于哪个 crate——那不是可以猜的事，直接按「要跑」处理：
/// 猜错的代价是一条本该重跑的格被复用，而它看起来与省下了一次跑一模一样。
pub fn plan(cells: &[String], changed: &[String]) -> CellPlan {
    let mut out = CellPlan::default();
    for cell in cells {
        let crate_name = cell.split_once(':').map(|(c, _)| c);
        match crate_name {
            Some(name) if !changed.iter().any(|c| c == name) => out.reused.push(cell.clone()),
            _ => out.rerun.push(cell.clone()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells() -> Vec<String> {
        vec![
            "cog-storage:suite".into(),
            "cog-gateway:suite".into(),
            "cogneva:suite".into(),
        ]
    }

    /// 只改一个 crate：那个格重跑，其余复用。
    #[test]
    fn only_the_changed_crate_is_rerun() {
        let p = plan(&cells(), &["cog-gateway".to_string()]);
        assert_eq!(p.rerun, vec!["cog-gateway:suite"]);
        assert_eq!(p.reused.len(), 2);
        assert!(!p.reran_without_input());
    }

    /// 一个都没改：全都复用。
    #[test]
    fn an_empty_change_reuses_everything() {
        let p = plan(&cells(), &[]);
        assert!(p.rerun.is_empty());
        assert_eq!(p.reused.len(), 3);
        assert_eq!(p.reused_ratio(), Some(1.0));
    }

    /// 读不出 crate 的格按「要跑」算：猜错的方向必须是多跑，不是少跑。
    #[test]
    fn a_cell_whose_crate_cannot_be_read_is_rerun() {
        let p = plan(&["nameless".to_string()], &[]);
        assert_eq!(p.rerun, vec!["nameless"]);
        assert!(p.reused.is_empty());
    }

    /// 没有格时没有比例。
    #[test]
    fn no_cells_has_no_ratio() {
        assert_eq!(plan(&[], &[]).reused_ratio(), None);
    }
}
