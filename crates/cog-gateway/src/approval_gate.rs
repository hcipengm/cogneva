use std::sync::Arc;
use std::time::Duration;

/// 需要人类审批的操作类型
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ApprovalRule {
    /// 任何代码修改操作（edit, write, delete source files）
    CodeModification,
    /// Agent 运行超过设定时间无进展
    StalledTimeout(Duration),
    /// Supervisor 检测到循环
    LoopDetected,
    /// 批量操作影响 N 个以上文件
    BlastRadius(usize),
    /// 删除数据类操作
    DestructiveOperation,
}

/// 审批请求状态
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Rejected,
    Expired,
}

/// 审批请求
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ApprovalRequest {
    pub request_id: uuid::Uuid,
    pub agent_id: String,
    pub task_id: String,
    pub rule_triggered: ApprovalRule,
    pub description: String,
    pub status: ApprovalStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// Agent 动作抽象（用于审批判断）
/// 实际集成时，sf-agent 的工具调用结果需实现此 trait。
pub trait AgentAction {
    /// 是否为代码修改操作
    fn is_code_modification(&self) -> bool;
    /// 是否为破坏性操作（删除数据、drop 表等）
    fn is_destructive(&self) -> bool;
    /// 影响文件数量
    fn affected_files_count(&self) -> usize;
}

/// 人类审批网关
/// 由 cog-gateway 持有，在 Agent 执行高风险操作前进行拦截。
pub struct HumanApprovalGate {
    pub rules: Vec<ApprovalRule>,
    pub pending_approvals: Vec<ApprovalRequest>,
    pub notification_dispatcher: Option<Arc<dyn cog_core::NotificationDispatcher>>,
}

impl HumanApprovalGate {
    pub fn new(rules: Vec<ApprovalRule>) -> Self {
        Self {
            rules,
            pending_approvals: Vec::new(),
            notification_dispatcher: None,
        }
    }

    pub fn with_dispatcher(
        mut self,
        dispatcher: Arc<dyn cog_core::NotificationDispatcher>,
    ) -> Self {
        self.notification_dispatcher = Some(dispatcher);
        self
    }

    /// 检查操作是否需要审批
    pub fn requires_approval(&self, action: &dyn AgentAction) -> Option<ApprovalRule> {
        for rule in &self.rules {
            match rule {
                ApprovalRule::CodeModification if action.is_code_modification() => {
                    return Some(rule.clone())
                }
                ApprovalRule::BlastRadius(n) if action.affected_files_count() > *n => {
                    return Some(rule.clone())
                }
                ApprovalRule::DestructiveOperation if action.is_destructive() => {
                    return Some(rule.clone())
                }
                _ => {}
            }
        }
        None
    }

    /// Agent 提交审批请求，暂停执行直到人类响应
    pub async fn submit(&mut self, request: ApprovalRequest) {
        self.pending_approvals.push(request.clone());
        if let Some(ref dispatcher) = self.notification_dispatcher {
            let notification = cog_core::Notification {
                id: request.request_id.to_string(),
                title: format!("审批请求: {}", request.description),
                body: format!(
                    "Agent `{}` 触发规则 `{:?}`\n\n请求ID: `{}`\n状态: {:?}\n到期时间: {}",
                    request.agent_id,
                    request.rule_triggered,
                    request.request_id,
                    request.status,
                    request.expires_at.to_rfc3339(),
                ),
                is_read: false,
                created_at: request.created_at,
                read_at: None,
            };
            if let Err(e) = dispatcher.dispatch(&notification).await {
                tracing::warn!(error = %e, "Approval gate notification dispatch failed");
            }
        }
    }

    /// 人类审批回调
    pub fn resolve(&mut self, request_id: uuid::Uuid, approved: bool) -> Option<ApprovalRequest> {
        let idx = self
            .pending_approvals
            .iter()
            .position(|r| r.request_id == request_id)?;
        let mut req = self.pending_approvals.remove(idx);
        req.status = if approved {
            ApprovalStatus::Approved
        } else {
            ApprovalStatus::Rejected
        };
        Some(req)
    }

    /// 查询待审批列表（供 Dashboard 调用）
    pub fn pending(&self) -> &[ApprovalRequest] {
        &self.pending_approvals
    }

    /// 清理过期请求
    pub fn purge_expired(&mut self, now: chrono::DateTime<chrono::Utc>) {
        for req in &mut self.pending_approvals {
            if req.expires_at < now && req.status == ApprovalStatus::Pending {
                req.status = ApprovalStatus::Expired;
            }
        }
        self.pending_approvals
            .retain(|r| r.status == ApprovalStatus::Pending);
    }
}
