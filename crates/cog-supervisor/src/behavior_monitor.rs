use std::collections::VecDeque;

use cog_core::LoopSeverity;

/// Agent 动作类型分类
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ActionType {
    /// 观察/诊断类：读取、检查、搜索（安全但无进展）
    Observational,
    /// 计划类：推理、规划（不产生副作用）
    Planning,
    /// 修改类：编辑、写入、删除（产生副作用）
    Mutational,
    /// 验证类：测试、确认（验证修改结果）
    Verification,
}

impl ActionType {
    pub fn is_observational(&self) -> bool {
        matches!(self, ActionType::Observational)
    }
}

/// 动作结果
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ActionOutcome {
    /// 获得了新信息
    NewInfo,
    /// 与之前结果重复
    Redundant,
    /// 无实质进展
    NoProgress,
}

/// Agent 行为模式监控器
/// 由 cog-supervisor 持有，订阅 Agent 动作事件流，实时检测分析瘫痪。
#[derive(Debug, Clone)]
pub struct BehaviorMonitor {
    pub agent_id: String,
    /// 最近动作历史（最多保留 20 条）
    pub action_history: VecDeque<(ActionType, ActionOutcome)>,
}

impl BehaviorMonitor {
    pub const DEFAULT_MAX_HISTORY: usize = 20;

    pub fn new(agent_id: impl Into<String>) -> Self {
        Self::with_max_history(agent_id, Self::DEFAULT_MAX_HISTORY)
    }

    pub fn with_max_history(agent_id: impl Into<String>, max_history: usize) -> Self {
        Self {
            agent_id: agent_id.into(),
            action_history: VecDeque::with_capacity(max_history),
        }
    }

    /// 记录一次动作
    pub fn record(&mut self, action: ActionType, outcome: ActionOutcome) {
        let max = self.action_history.capacity().max(1);
        if self.action_history.len() >= max {
            self.action_history.pop_front();
        }
        self.action_history.push_back((action, outcome));
    }

    /// 检测是否陷入循环
    /// 规则：
    /// - 连续 3 次观察类且无新信息 → Mild
    /// - 连续 5 次观察类 → Escalate
    /// - 最近 10 次无修改类 → Critical
    pub fn detect_loop(&self) -> LoopSeverity {
        let recent: Vec<_> = self.action_history.iter().rev().collect();

        if recent
            .iter()
            .take(3)
            .all(|(a, o)| a.is_observational() && *o != ActionOutcome::NewInfo)
        {
            return LoopSeverity::Mild;
        }

        if recent.iter().take(5).all(|(a, _)| a.is_observational()) {
            return LoopSeverity::Escalate;
        }

        if recent
            .iter()
            .take(10)
            .all(|(a, _)| !matches!(a, ActionType::Mutational))
        {
            return LoopSeverity::Critical;
        }

        LoopSeverity::None
    }

    /// 根据严重程度生成干预提示
    pub fn intervention_prompt(&self, severity: LoopSeverity) -> Option<String> {
        match severity {
            LoopSeverity::None => None,
            LoopSeverity::Mild => Some(format!(
                "[System] Agent {} has performed 3 diagnostic actions without new findings. Please proceed to the execution phase.",
                self.agent_id
            )),
            LoopSeverity::Escalate => Some(format!(
                "[System] Agent {} is in a diagnostic loop. Task will be reassigned or escalated to human.",
                self.agent_id
            )),
            LoopSeverity::Critical => Some(format!(
                "[System] Agent {} has made zero modifications after 10 actions. Task terminated.",
                self.agent_id
            )),
        }
    }
}
