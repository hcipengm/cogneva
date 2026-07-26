use std::time::Duration;

/// Agent 执行预算：防止分析瘫痪的资源配额
/// 每个 Agent 在执行任务前被分配一个预算，预算耗尽则强制推进或终止。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentBudget {
    /// 诊断类动作配额（观察、读取、检查）
    pub observation_budget: u32,
    /// 修改类动作配额（编辑、写入、删除）
    pub mutation_budget: u32,
    /// 计划阶段思考时间上限
    pub thinking_time_limit: Duration,
    /// 单次任务总时间上限
    pub total_time_limit: Duration,
    /// 已消耗的诊断配额
    pub observation_consumed: u32,
    /// 已消耗的修改配额
    pub mutation_consumed: u32,
}

/// 预算消耗动作类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BudgetAction {
    /// 消耗 1 点 observation_budget
    Observation,
    /// 消耗 1 点 mutation_budget
    Mutation,
    /// 按时间消耗 thinking_time_limit（由外部计时器检查）
    Planning,
}

/// 预算消耗结果
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BudgetResult {
    Ok,
    /// 观察配额用完，必须进入 Execute 阶段
    ObservationExhausted,
    /// 修改配额用完，任务终止
    MutationExhausted,
    /// 总时间超限
    TimeExceeded,
}

impl AgentBudget {
    pub fn new(observation: u32, mutation: u32, thinking: Duration, total: Duration) -> Self {
        Self {
            observation_budget: observation,
            mutation_budget: mutation,
            thinking_time_limit: thinking,
            total_time_limit: total,
            observation_consumed: 0,
            mutation_consumed: 0,
        }
    }

    /// 尝试消耗预算
    pub fn consume(&mut self, action: BudgetAction) -> BudgetResult {
        match action {
            BudgetAction::Observation => {
                if self.observation_consumed >= self.observation_budget {
                    return BudgetResult::ObservationExhausted;
                }
                self.observation_consumed += 1;
                BudgetResult::Ok
            }
            BudgetAction::Mutation => {
                if self.mutation_consumed >= self.mutation_budget {
                    return BudgetResult::MutationExhausted;
                }
                self.mutation_consumed += 1;
                BudgetResult::Ok
            }
            BudgetAction::Planning => {
                // Planning 的时间消耗由外部调度器统一检查
                BudgetResult::Ok
            }
        }
    }

    /// 观察配额已耗尽，强制进入 Execute 阶段
    pub fn force_execute(&self) -> bool {
        self.observation_consumed >= self.observation_budget && self.mutation_budget > 0
    }

    /// 返回预算摘要（用于调试和监控）
    pub fn summary(&self) -> String {
        format!(
            "observation: {}/{}, mutation: {}/{}, thinking: {:?}, total: {:?}",
            self.observation_consumed,
            self.observation_budget,
            self.mutation_consumed,
            self.mutation_budget,
            self.thinking_time_limit,
            self.total_time_limit,
        )
    }
}

/// 预定义预算模板
pub struct BudgetTemplates;

impl BudgetTemplates {
    pub fn code_fix() -> AgentBudget {
        AgentBudget::new(3, 10, Duration::from_secs(30), Duration::from_secs(300))
    }

    pub fn doc_update() -> AgentBudget {
        AgentBudget::new(2, 20, Duration::from_secs(20), Duration::from_secs(180))
    }

    pub fn refactor() -> AgentBudget {
        AgentBudget::new(5, 5, Duration::from_secs(60), Duration::from_secs(600))
    }

    pub fn hotfix() -> AgentBudget {
        AgentBudget::new(1, 3, Duration::from_secs(10), Duration::from_secs(120))
    }
}
