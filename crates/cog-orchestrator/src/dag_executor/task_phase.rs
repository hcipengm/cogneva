/// DAG Task 执行阶段
/// 将原本宽泛的 Agent 任务拆分为四个强制阶段，防止在单一阶段无限停留。
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TaskPhase {
    /// 只允许观察类动作（grep, read, check）
    /// 默认最大迭代次数：2 次
    Diagnose,
    /// 只允许推理，不允许工具调用
    /// 默认最大迭代次数：1 次
    Plan,
    /// 只允许修改类动作（edit, write）
    /// 必须基于 Plan 的输出，禁止临时改方案
    Execute,
    /// 只允许验证类动作（test, verify）
    Verify,
}

/// 阶段退出条件
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ExitCriteria {
    /// 找到明确结论（如"5 个文件缺少字段"）
    ConclusionFound,
    /// 达到最大迭代次数，强制推进
    MaxIterationsReached,
    /// 人类审批通过
    HumanApproved,
    /// 验证通过
    VerificationPassed,
}

/// 带阶段的 Task 定义
/// 由 DagExecutor 在调度时附加到每个 task，Agent 执行时必须遵守阶段约束。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PhasedTask {
    pub phase: TaskPhase,
    pub max_iterations: u8,
    pub current_iteration: u8,
    pub exit_criteria: Vec<ExitCriteria>,
    /// 阶段产物（如 Diagnose 的结论文本）
    pub artifacts: Vec<String>,
}

impl PhasedTask {
    pub fn new(phase: TaskPhase, max_iterations: u8) -> Self {
        Self {
            phase,
            max_iterations,
            current_iteration: 0,
            exit_criteria: Self::default_criteria(&phase),
            artifacts: Vec::new(),
        }
    }

    fn default_criteria(phase: &TaskPhase) -> Vec<ExitCriteria> {
        match phase {
            TaskPhase::Diagnose => vec![
                ExitCriteria::ConclusionFound,
                ExitCriteria::MaxIterationsReached,
            ],
            TaskPhase::Plan => vec![ExitCriteria::MaxIterationsReached],
            TaskPhase::Execute => vec![ExitCriteria::MaxIterationsReached],
            TaskPhase::Verify => vec![ExitCriteria::VerificationPassed],
        }
    }

    /// 递增迭代计数
    pub fn bump_iteration(&mut self) {
        self.current_iteration += 1;
    }

    /// 检查是否应退出当前阶段
    pub fn should_exit(&self) -> bool {
        if self.current_iteration >= self.max_iterations {
            return true;
        }
        match self.phase {
            TaskPhase::Diagnose => self.artifacts.iter().any(|a| a.contains("CONCLUSION")),
            TaskPhase::Plan => !self.artifacts.is_empty(),
            TaskPhase::Execute => false, // 由外部调度器控制
            TaskPhase::Verify => self.artifacts.iter().any(|a| a.contains("PASS")),
        }
    }

    /// 推进到下一阶段
    pub fn advance(&mut self) -> Option<TaskPhase> {
        let next = match self.phase {
            TaskPhase::Diagnose => Some(TaskPhase::Plan),
            TaskPhase::Plan => Some(TaskPhase::Execute),
            TaskPhase::Execute => Some(TaskPhase::Verify),
            TaskPhase::Verify => None,
        };
        if let Some(p) = next {
            self.phase = p;
            self.current_iteration = 0;
            self.artifacts.clear();
            self.exit_criteria = Self::default_criteria(&p);
        }
        next
    }
}

/// 阶段转换规则集
pub struct PhaseTransitionRules;

impl PhaseTransitionRules {
    /// 同一 Agent 不允许连续执行两个 Diagnose task
    pub fn agent_can_accept_diagnose(_agent_id: &str, recent_tasks: &[PhasedTask]) -> bool {
        !matches!(recent_tasks.last(), Some(t) if t.phase == TaskPhase::Diagnose)
    }

    /// 根据任务类型返回默认预算配置
    pub fn default_budget_for_task_type(task_type: &str) -> (u8, u8) {
        // (observation_budget, mutation_budget)
        match task_type {
            "code_fix" => (3, 10),
            "doc_update" => (2, 20),
            "refactor" => (5, 5),
            "hotfix" => (1, 3),
            _ => (3, 5),
        }
    }
}
