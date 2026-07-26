use std::collections::VecDeque;

/// 工具输出严重程度
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Severity {
    Info,
    Warning,
    Blocking,
}

/// 工具输出摘要，保留结论、丢弃细节
#[derive(Debug, Clone)]
pub struct ToolSummary {
    pub tool_name: String,
    pub conclusion: String,
    pub severity: Severity,
}

/// 向量存储中的原始工具输出引用
/// 实际检索逻辑由使用方（sf-agent）注入，避免 cog-core 反向依赖 sf-memory。
#[derive(Debug, Clone)]
pub struct MemoryRef {
    pub embedding_id: uuid::Uuid,
    /// 存储后端标识，使用方据此选择 backend 实例
    pub backend_name: String,
}

/// Agent 工作记忆：分层存储，防止工具输出污染目标记忆
/// 核心机制：
/// - `mission` 和 `plan_summary` 是只读的，永远保留在 prompt 顶部
/// - `recent_summaries` 只保留最近 N 条（默认 10 条）
/// - 原始工具输出通过 `MemoryRef` 归档到向量存储，按需检索
#[derive(Debug, Clone)]
pub struct AgentWorkingMemory {
    /// 长期目标（只读，永不挤出工作记忆）
    pub mission: String,
    /// 当前执行计划摘要（压缩后）
    pub plan_summary: String,
    /// 最近 N 步的工具输出摘要
    pub recent_summaries: VecDeque<ToolSummary>,
    /// 原始输出的向量存储引用，按需检索
    pub raw_outputs: Option<MemoryRef>,
}

impl AgentWorkingMemory {
    pub const DEFAULT_MAX_SUMMARIES: usize = 10;

    pub fn new(mission: impl Into<String>, plan_summary: impl Into<String>) -> Self {
        Self::with_max_summaries(mission, plan_summary, Self::DEFAULT_MAX_SUMMARIES)
    }

    pub fn with_max_summaries(
        mission: impl Into<String>,
        plan_summary: impl Into<String>,
        max_summaries: usize,
    ) -> Self {
        Self {
            mission: mission.into(),
            plan_summary: plan_summary.into(),
            recent_summaries: VecDeque::with_capacity(max_summaries),
            raw_outputs: None,
        }
    }

    /// 添加工具输出摘要，自动维护上限
    pub fn add_summary(&mut self, summary: ToolSummary) {
        let max = self.recent_summaries.capacity().max(1);
        if self.recent_summaries.len() >= max {
            self.recent_summaries.pop_front();
        }
        self.recent_summaries.push_back(summary);
    }

    /// 将原始输出引用归档到向量存储
    pub fn archive_raw_output(&mut self, mem_ref: MemoryRef) {
        self.raw_outputs = Some(mem_ref);
    }

    /// 检索之前归档的原始输出（如需重新确认）
    /// 当前为 stub。Phase 2 由 sf-agent 注入实际的存储后端检索逻辑。
    pub fn retrieve_raw_output(&self) -> Option<String> {
        let mem_ref = self.raw_outputs.as_ref()?;
        Some(format!(
            "[archived output: {} from {}]",
            mem_ref.embedding_id, mem_ref.backend_name
        ))
    }

    /// 生成用于 prompt 的工作记忆文本
    pub fn render(&self) -> String {
        let mut buf = format!(
            "## Mission\n{}\n\n## Plan Summary\n{}\n\n## Recent Tool Outputs\n",
            self.mission, self.plan_summary
        );
        for s in &self.recent_summaries {
            buf.push_str(&format!(
                "- [{}] {}: {}\n",
                s.severity_label(),
                s.tool_name,
                s.conclusion
            ));
        }
        buf
    }
}

impl ToolSummary {
    fn severity_label(&self) -> &'static str {
        match self.severity {
            Severity::Info => "INFO",
            Severity::Warning => "WARN",
            Severity::Blocking => "BLOCK",
        }
    }
}
