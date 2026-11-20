//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//! 上下文窗口由**型号规格**给出，配置里的那个数只是种子，而读数是哪一个必须看得见。
//!
//! 这条界以前是一个常量：`agent_loop.context_window_size` 现网 64000、代码默认 32000，
//! 而主流模型 200K–1M。差值不是「保守」，是**自设的界**：它按自己的想象裁掉模型本来
//! 看得见的东西，而没有任何读数说裁得对不对。
//!
//! 所以窗口有两个来源，且必须分得开：
//!
//! - `Model`：上游条目声明了 `context_window`，按这一轮**实际用的**上游取；
//! - `Seed`：还没读到型号规格，配置里那个数顶着。
//!
//! 「谁说的」不是装饰：把 `Seed` 读成 `Model` 会让读的人以为窗口已经按型号量过了；
//! 反过来把 `Model` 读成 `Seed` 会让告警在已经对齐的时候继续响。两者在数值上可以一样，
//! 在读数上必须分开。
//!
//! 窗口是**每一位调用者都要按它算预算**的那个数（工具结果能带多少、历史留多少），
//! 所以它一旦由型号给出，预算跟着变大而没有任何常量压回去——这条由调用点决定，
//! 这里只保证给出去的那个数带着来源。

/// 窗口这个数是谁说的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetSource {
    /// 上游条目声明的型号规格。
    Model,
    /// 还没读到规格时，配置里的种子顶着。
    Seed,
    /// 没人说过：没有界，不裁。
    Unstated,
}

impl BudgetSource {
    pub fn as_str(self) -> &'static str {
        match self {
            BudgetSource::Model => "model",
            BudgetSource::Seed => "seed",
            BudgetSource::Unstated => "unstated",
        }
    }
}

/// 这一轮真正生效的窗口，连同它的来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextBudget {
    tokens: Option<usize>,
    source: BudgetSource,
}

impl ContextBudget {
    /// 按这一轮实际用的上游取窗口：型号说了就用型号的，没说才退回种子。
    ///
    /// `seed` 为 `None` 表示这个部署没有写种子——那么窗口就是 `Unstated`，
    /// 不是 0：把「没说过」读成 0，第一条消息进来就会被裁到地板。
    pub fn resolve(model_window: Option<usize>, seed: Option<usize>) -> Self {
        match (model_window.filter(|t| *t > 0), seed.filter(|t| *t > 0)) {
            (Some(tokens), _) => Self {
                tokens: Some(tokens),
                source: BudgetSource::Model,
            },
            (None, Some(tokens)) => Self {
                tokens: Some(tokens),
                source: BudgetSource::Seed,
            },
            (None, None) => Self {
                tokens: None,
                source: BudgetSource::Unstated,
            },
        }
    }

    pub fn tokens(&self) -> Option<usize> {
        self.tokens
    }

    pub fn source(&self) -> BudgetSource {
        self.source
    }

    /// 真正生效的预算：没人说过窗口时是「没有界」。
    pub fn limit(&self) -> usize {
        self.tokens.unwrap_or(usize::MAX)
    }

    /// 型号声明的窗口比当前预算还小时应当告警。
    ///
    /// 这不是「窗口太大」的告警，恰好相反：它说的是**有人按一个比型号更小的数在裁**，
    /// 而那个数没有出处。已经由型号给出时不再是告警——那时预算就是型号说的那个。
    pub fn narrower_than(&self, in_use: usize) -> bool {
        matches!(self.source, BudgetSource::Model) && self.limit() < in_use
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 型号说了就用型号的，种子在**不改变**这个选择时也不参与。
    #[test]
    fn the_model_wins_over_the_seed() {
        let budget = ContextBudget::resolve(Some(1_000_000), Some(64_000));
        assert_eq!(budget.tokens(), Some(1_000_000));
        assert_eq!(budget.source(), BudgetSource::Model);
        assert_eq!(budget.limit(), 1_000_000, "预算跟着型号走，没有被种子压回");
    }

    /// 型号没说是种子，且来源写着 `seed`——读者要看得见「还没量过」。
    #[test]
    fn without_a_model_the_seed_stands_and_says_so() {
        let budget = ContextBudget::resolve(None, Some(64_000));
        assert_eq!(budget.tokens(), Some(64_000));
        assert_eq!(budget.source(), BudgetSource::Seed);
    }

    /// 两个都没有是「没有界」，不是 0。
    #[test]
    fn nobody_saying_anything_is_no_bound_rather_than_zero() {
        let budget = ContextBudget::resolve(None, None);
        assert_eq!(budget.tokens(), None);
        assert_eq!(budget.source(), BudgetSource::Unstated);
        assert_eq!(budget.limit(), usize::MAX, "没说过窗口就是不裁");
    }

    /// 型号窗口比在用的预算小时要能问出来：这正是「按一个更小的数在裁」的形状。
    #[test]
    fn a_model_window_below_the_budget_in_use_is_reportable() {
        let model = ContextBudget::resolve(Some(120_000), None);
        assert!(model.narrower_than(200_000));
        assert!(!model.narrower_than(100_000));
        let seed = ContextBudget::resolve(None, Some(32_000));
        assert!(
            !seed.narrower_than(200_000),
            "种子比在用预算小不是这条告警：它说的是型号说了一个更小的数"
        );
    }

    /// 0 不是窗口：型号或种子里写 0 等于没写。
    #[test]
    fn a_zero_window_is_read_as_unstated() {
        let budget = ContextBudget::resolve(Some(0), Some(0));
        assert_eq!(budget.source(), BudgetSource::Unstated);
        assert_eq!(budget.limit(), usize::MAX);
    }
}
