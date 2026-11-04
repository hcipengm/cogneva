//! 界面上一段对话的一轮，跑在系统唯一的那条智能体循环上。
//!
//! 这里只做接线：把编排器的控制面做成一组工具（读台账、派活、催跑、撤回），装进
//! 智能体内核的工具表，再用调用方给好的历史跑一轮。循环、守卫、预算、token 账与
//! 总线事件都在内核里发生——与一件任务的一次尝试是同一条，不是并排的两条。
//!
//! 工具集里**没有执行工具**，这是职责的位置而不是能力的裁剪：这段对话看清事实、
//! 把活派给编排器、回来读进展；真正动手的是任务图里的 squad。给它 `run_command`
//! 就等于在系统外面再开一条执行路径——那件事不进任务图、不进账、不经过任何门。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

// 工具表的写入方法在 `cog_core` 的契约 trait 上，实现是内核的注册表：两个同名，
// 这里要的是前者的方法，实现按后者取。
use cog_core::contract::tool::{Tool, ToolHandler, ToolImplementation, ToolRegistry as _};
use cog_core::{
    AgentEvent, AuditKind, AuditStream, HierarchyManager, OrchestratorControl, QuotaContext,
    RuntimeConfig, SFError, SFResult,
};
use cog_gateway::chat::{ConversationRequest, ConversationTurn};

/// 这段对话的角色名。它出现在运行清点与日志里；真正落到账上的 actor 是
/// [`WEB_CHAT_ACTOR`]——按角色名记账会把界面花的钱混进 agent 池那一族，而那正是
/// 要分开看的两件事。
const CHAT_ROLE: &str = WEB_CHAT_ACTOR;

/// 界面上那次对话在账上的 actor。取自网关声明的闭集，不在这里另写一个值。
const WEB_CHAT_ACTOR: &str = "web_chat";

/// 一段对话里最多几轮「想—用工具」。
///
/// 用完还想要工具的那一轮，内核不再给工具而直接要一个答案：问的是结论，不是又
/// 一次动作。空转比答不出来更糟——人正等着。
// cap: bound — 一段对话里允许的工具往返次数
const CHAT_ITERATIONS: u32 = 6;

/// 一次 `list_tasks` 最多回多少行。
// cap: bound — 一次读台账回多少行；整张图会把上下文和这次回答一起淹掉
const LIST_TASKS_LIMIT: usize = 40;
/// 调用方没给 `limit` 时回多少行。
// cap: bound — 默认页只有上限的一半：上限是显式要多的人能拿到的，默认值给的是「一眼扫完、还能接着问」的量
const LIST_TASKS_DEFAULT: usize = 20;

/// 装配层交给网关的那条循环：一轮对话就是内核的一次运行。
pub struct PlatformConversation {
    orchestrator: Arc<dyn OrchestratorControl>,
    /// 派活之前要过的那道准入。与 HTTP 提交面读的是同一个管理器、同一份估值：
    /// 少这一道，一个人今天用光的额度只挡得住 HTTP，挡不住界面上的一句话，
    /// 而两者要的是同一份钱。
    admission: Option<Arc<dyn HierarchyManager>>,
    /// 拒绝也要落一条。提交面拒单时写 `AuditKind::QuotaEnforcement`；界面上
    /// 同一句话被同一条规则挡住却不写，账上就只剩「有人今天没派活」。
    audit: Option<Arc<dyn AuditStream>>,
}

impl PlatformConversation {
    pub fn new(
        orchestrator: Arc<dyn OrchestratorControl>,
        admission: Option<Arc<dyn HierarchyManager>>,
        audit: Option<Arc<dyn AuditStream>>,
    ) -> Self {
        Self {
            orchestrator,
            admission,
            audit,
        }
    }
}

impl ConversationTurn for PlatformConversation {
    fn turn<'a>(
        &'a self,
        request: ConversationRequest,
        events: tokio::sync::mpsc::Sender<AgentEvent>,
    ) -> Pin<Box<dyn Future<Output = SFResult<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            let config = RuntimeConfig {
                agent_id: format!("web-chat-{}", uuid::Uuid::new_v4()),
                role: CHAT_ROLE.to_string(),
                max_iterations: CHAT_ITERATIONS,
                ..Default::default()
            };
            let tools = cog_agent::ToolRegistry::new();
            for tool in control_plane_tools(
                self.orchestrator.clone(),
                self.admission.clone(),
                self.audit.clone(),
                request.owner.clone(),
            ) {
                tools.register(tool);
            }
            let mut runtime = cog_agent::AgentRuntime::new(config, events)
                .with_tools(tools)
                .with_actor_label(WEB_CHAT_ACTOR)
                // 没有身份的连接不声明人：编一个名字会读成真有人花过这笔钱。
                .with_call_identity(
                    request.owner.clone(),
                    cog_core::LlmOrigin::Interactive.as_str(),
                );
            runtime
                .run_conversation(request.messages, request.llm.as_ref())
                .await
        })
    }
}

/// 这段对话能用的操作：**只读台账、只派活、不执行**。
///
/// 闭集写成枚举而不是一张名字表：每一项在 [`ControlPlaneOp::tool`] 里必须配一个
/// handler（match 是穷尽的），所以「提供给模型的工具」与「真的会执行的分支」不会
/// 分叉——只提供不执行在模型眼里等于没有这个工具，而它会一直试下去。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ControlPlaneOp {
    DispatchGoal,
    ListTasks,
    GetTask,
    RetryTask,
    CancelTask,
}

/// 这一段对话交给模型的全部工具，按给模型看的顺序。
const CONTROL_PLANE: &[ControlPlaneOp] = &[
    ControlPlaneOp::DispatchGoal,
    ControlPlaneOp::ListTasks,
    ControlPlaneOp::GetTask,
    ControlPlaneOp::RetryTask,
    ControlPlaneOp::CancelTask,
];

impl ControlPlaneOp {
    fn name(self) -> &'static str {
        match self {
            ControlPlaneOp::DispatchGoal => "dispatch_goal",
            ControlPlaneOp::ListTasks => "list_tasks",
            ControlPlaneOp::GetTask => "get_task",
            ControlPlaneOp::RetryTask => "retry_task",
            ControlPlaneOp::CancelTask => "cancel_task",
        }
    }

    fn description(self) -> &'static str {
        match self {
            ControlPlaneOp::DispatchGoal => {
                "把一件要做的事交给编排器：它会分解成任务、派给执行者。返回目标号——\
                 回复里引用它时必须照抄工具返回的那个值，不许自己编；分解是异步的，\
                 不要说它已经跑完。"
            }
            ControlPlaneOp::ListTasks => {
                "读任务台账：按状态或目标号筛出若干条任务，新的在前。回答「进展、谁在做、\
                 卡在哪」之前先读它，不要凭印象回答。"
            }
            ControlPlaneOp::GetTask => "读一条任务的全文，含它的输入与结果载荷。",
            ControlPlaneOp::RetryTask => "重新跑一条失败或卡住的任务。",
            ControlPlaneOp::CancelTask => {
                "撤回一条任务，并说明理由。理由会落在这条行上——没有理由的取消在读面上\
                 与「不知道谁为什么把它停了」没有分别。"
            }
        }
    }

    fn parameters(self) -> serde_json::Value {
        match self {
            ControlPlaneOp::DispatchGoal => serde_json::json!({
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "要做的事，一句话说清做什么与怎样算做完"
                    }
                },
                "required": ["goal"]
            }),
            ControlPlaneOp::ListTasks => serde_json::json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "description": "只看这一种状态（pending / scheduled / running / completed / failed / cancelled）"
                    },
                    "goal_id": {
                        "type": "string",
                        "description": "只看这一道目标下的任务"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "最多回几行，默认 20"
                    }
                }
            }),
            ControlPlaneOp::GetTask | ControlPlaneOp::RetryTask => serde_json::json!({
                "type": "object",
                "properties": {
                    "task_id": { "type": "string", "description": "任务号" }
                },
                "required": ["task_id"]
            }),
            ControlPlaneOp::CancelTask => serde_json::json!({
                "type": "object",
                "properties": {
                    "task_id": { "type": "string", "description": "任务号" },
                    "reason": { "type": "string", "description": "为什么撤回" }
                },
                "required": ["task_id", "reason"]
            }),
        }
    }

    /// 这一项装成内核认的工具：名字、说明、参数结构，以及那个一定存在的执行分支。
    fn tool(
        self,
        orchestrator: Arc<dyn OrchestratorControl>,
        admission: Option<Arc<dyn HierarchyManager>>,
        audit: Option<Arc<dyn AuditStream>>,
        owner: String,
    ) -> Tool {
        let handler = match self {
            ControlPlaneOp::DispatchGoal => dispatch_handler(orchestrator, admission, audit, owner),
            ControlPlaneOp::ListTasks => list_handler(orchestrator),
            ControlPlaneOp::GetTask => get_handler(orchestrator),
            ControlPlaneOp::RetryTask => retry_handler(orchestrator),
            ControlPlaneOp::CancelTask => cancel_handler(orchestrator),
        };
        Tool {
            name: self.name().to_string(),
            description: self.description().to_string(),
            parameters: self.parameters(),
            implementation: ToolImplementation::Native(handler),
        }
    }
}

/// 这段对话的工具集。
fn control_plane_tools(
    orchestrator: Arc<dyn OrchestratorControl>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
) -> Vec<Tool> {
    CONTROL_PLANE
        .iter()
        .map(|op| {
            op.tool(
                orchestrator.clone(),
                admission.clone(),
                audit.clone(),
                owner.clone(),
            )
        })
        .collect()
}

/// 把一次工具调用包成内核认的 handler：一个值进、一个值出。
fn native<F, Fut>(f: F) -> ToolHandler
where
    F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = SFResult<serde_json::Value>> + Send + 'static,
{
    Arc::new(move |args| Box::pin(f(args)))
}

fn require_str<'a>(args: &'a serde_json::Value, key: &str) -> SFResult<&'a str> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SFError::Validation(format!("{key} 不能为空")))
}

/// 派活之前的那道准入，与 HTTP 提交面同一条：同一个 5 级配额管理器、同一份
/// 估值口径（[`cog_gateway::tasks::estimated_goal_tokens`]）。
///
/// 拦住时回一句给人看的话，不回错误：拒绝是这一轮的结果，模型该把它说出来，
/// 而不是把同一次派发重试一遍。没有配额管理器时放行——与提交面一致，「没人配过
/// 上限」不等于「谁都不许提交」。
async fn admission_refusal(
    admission: Option<&Arc<dyn HierarchyManager>>,
    owner: &str,
    goal: &str,
) -> Option<QuotaRefusal> {
    let hierarchy = admission?;
    let context = QuotaContext {
        // 没有身份的连接不编一个人：编出来的名字会把这次提交记到别人头上。
        user_id: (!owner.is_empty()).then(|| owner.to_string()),
        workspace_id: None,
        team_id: None,
        organization_id: None,
        global_id: None,
    };
    let estimated = cog_gateway::tasks::estimated_goal_tokens(goal, 0);
    let decision = hierarchy.check(&context, estimated).await;
    if decision.allowed {
        return None;
    }
    Some(QuotaRefusal {
        estimated,
        blocked_by: decision.blocked_by.len(),
    })
}

/// 一次被准入挡下的派活。它带着读数，因为写入账面的那条要落同样的两个数。
struct QuotaRefusal {
    estimated: u64,
    blocked_by: usize,
}

impl QuotaRefusal {
    fn sentence(&self) -> String {
        format!(
            "今天的额度不够这一次提交：估计要 {} token，被 {} 层上限挡住。\
             等额度恢复再提交，或请管理员调整上限。",
            self.estimated, self.blocked_by
        )
    }
}

/// 被挡下的这一次写进审计链：与提交面同一个 kind、同一个动作名，目标号那一栏
/// 是 `-`——这一次根本没有目标，编一个 id 会让账上的拒绝看起来像一次派发。
async fn record_refusal(audit: Option<&Arc<dyn AuditStream>>, owner: &str, refusal: &QuotaRefusal) {
    let Some(stream) = audit else { return };
    let _ = stream
        .append(
            AuditKind::QuotaEnforcement,
            if owner.is_empty() { "anonymous" } else { owner },
            "-",
            "quota.dispatch_rejected",
            serde_json::json!({
                "surface": "web_chat",
                "estimated_tokens": refusal.estimated,
                "blocked_by": refusal.blocked_by,
            }),
        )
        .await;
}

fn dispatch_handler(
    orchestrator: Arc<dyn OrchestratorControl>,
    admission: Option<Arc<dyn HierarchyManager>>,
    audit: Option<Arc<dyn AuditStream>>,
    owner: String,
) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        let admission = admission.clone();
        let audit = audit.clone();
        let owner = owner.clone();
        async move {
            let goal = require_str(&args, "goal")?.to_string();
            if let Some(refusal) = admission_refusal(admission.as_ref(), &owner, &goal).await {
                // 与提交面同一格：拒绝写 `QuotaEnforcement`，目标号那一栏是
                // `-`——这一次根本没有目标，编一个 id 会让账上的拒绝看起来像
                // 一次派发。
                record_refusal(audit.as_ref(), &owner, &refusal).await;
                return Ok(serde_json::json!({
                    "status": "refused",
                    "reason": refusal.sentence(),
                    "note": "这一次没有派出去，也没有目标号。把原因说给用户听，不要重试同一次提交"
                }));
            }
            let goal_id = uuid::Uuid::new_v4().to_string();
            let row = cog_core::Task::goal_row(goal_id.clone(), &goal);
            // 分解是异步的（规划要几十秒），所以这一条不在工具调用里等：把目标号交
            // 回去，分解在后台跑，进展由台账回答。判退也是 Ok——成败读目标行本身。
            let id_for_submit = goal_id.clone();
            let submitter = orchestrator.clone();
            tokio::spawn(async move {
                if let Err(e) = submitter.submit_goal_auto(&goal, vec![row]).await {
                    tracing::warn!(goal = %id_for_submit, error = %e, "目标没能交给编排器");
                }
            });
            Ok(serde_json::json!({
                "goal_id": goal_id,
                "status": "submitted",
                "note": "已交给编排器分解。要查进展用 list_tasks，goal_id 就用这个值；\
                         这个 id 是唯一权威，回复里引用时照抄它"
            }))
        }
    })
}

fn list_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let status = args
                .get("status")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_ascii_lowercase());
            let goal = args
                .get("goal_id")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let limit = args
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| (n as usize).clamp(1, LIST_TASKS_LIMIT))
                .unwrap_or(LIST_TASKS_DEFAULT);

            let mut rows = orchestrator.get_all_tasks().await;
            if let Some(status) = status {
                rows.retain(|task| task.status.as_str() == status);
            }
            if let Some(goal) = goal {
                rows.retain(|task| task.goal_id.as_deref() == Some(goal.as_str()));
            }
            // 新的在前：问「进展」时关心的是正在动的那几行。
            rows.sort_by_key(|task| std::cmp::Reverse(task.updated_at));
            let total = rows.len();
            rows.truncate(limit);

            Ok(serde_json::json!({
                "total_matching": total,
                "returned": rows.len(),
                "tasks": rows.iter().map(task_summary).collect::<Vec<_>>(),
            }))
        }
    })
}

fn get_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let id = require_str(&args, "task_id")?.to_string();
            match orchestrator.get_task(&id).await {
                Some(task) => Ok(serde_json::to_value(&task)?),
                None => Ok(serde_json::json!({
                    "found": false,
                    "note": format!("图里没有 {id} 这条任务")
                })),
            }
        }
    })
}

fn retry_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let id = require_str(&args, "task_id")?.to_string();
            orchestrator
                .retry_task(&id)
                .await
                .map_err(|e| SFError::Agent(format!("{id} 没能重试：{e}")))?;
            Ok(serde_json::json!({ "task_id": id, "status": "retried" }))
        }
    })
}

fn cancel_handler(orchestrator: Arc<dyn OrchestratorControl>) -> ToolHandler {
    native(move |args| {
        let orchestrator = orchestrator.clone();
        async move {
            let id = require_str(&args, "task_id")?.to_string();
            let reason = require_str(&args, "reason")?.to_string();
            let cascaded = orchestrator
                .cancel_task_with_reason(&id, reason)
                .await
                .map_err(|e| SFError::Agent(format!("{id} 没能取消：{e}")))?;
            Ok(serde_json::json!({
                "task_id": id,
                "status": "cancelled",
                "also_cancelled": cascaded,
            }))
        }
    })
}

/// 台账里一行压成模型读得动的一小段。长字段（`result`、`input`）只在 `get_task`
/// 里给，否则一次列 20 行就把上下文占满了。
fn task_summary(task: &cog_core::Task) -> serde_json::Value {
    serde_json::json!({
        "id": task.id,
        "kind": task.task_type,
        "status": task.status,
        "goal_id": task.goal_id,
        "parent_task_id": task.parent_task_id,
        "agent_id": task.agent_id,
        "retry_count": task.retry_count,
        "updated_at": task.updated_at,
        "error": task.error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 工具集是闭集，而且里面没有执行工具。
    ///
    /// 出现一个 `run_command` 之类的东西，界面上就多出一条执行路径：它做的事不进
    /// 任务图、不进账、不经过任何门，而屏幕上看起来与编排器派出去的活一模一样。
    #[test]
    fn the_conversation_reads_and_dispatches_but_never_executes() {
        let names: Vec<&str> = CONTROL_PLANE.iter().map(|op| op.name()).collect();
        assert_eq!(
            names,
            vec![
                "dispatch_goal",
                "list_tasks",
                "get_task",
                "retry_task",
                "cancel_task"
            ],
            "工具集是闭集：加一项就要在这里说清楚它是读还是派活"
        );
        for name in &names {
            for banned in ["run", "exec", "shell", "bash", "write", "command", "eval"] {
                assert!(
                    !name.contains(banned),
                    "{name} 看起来是执行工具：这段对话不执行工作，活派给编排器"
                );
            }
        }
        // 每一项都要有说明与参数结构：没有说明的工具模型不会用，没有结构的参数
        // 它只能瞎填。handler 那侧由 `tool` 里的穷尽 match 保证，不在这里重判。
        for op in CONTROL_PLANE {
            assert!(!op.description().trim().is_empty(), "{}", op.name());
            assert_eq!(op.parameters()["type"], "object", "{}", op.name());
        }
    }

    /// 界面上那笔账的 actor 必须在网关声明的闭集里。
    #[test]
    fn the_chat_is_recorded_under_an_actor_the_ledger_declares() {
        assert!(cog_gateway::actor_labels::declared(WEB_CHAT_ACTOR));
    }

    /// 一个只剩硬上限的管理器：每一级都已经用完。
    struct Exhausted;

    #[async_trait::async_trait]
    impl HierarchyManager for Exhausted {
        async fn check(&self, _ctx: &QuotaContext, _tokens: u64) -> cog_core::HierarchyDecision {
            let blocked = cog_core::ScopeStatus {
                scope: cog_core::QuotaScope::User,
                target_id: "someone".into(),
                remaining: 0,
                used_today: 1_000_000,
                limits: cog_core::QuotaLimits::new(1_000_000, 1_000_000),
                blocking: true,
                warning: false,
            };
            cog_core::HierarchyDecision {
                allowed: false,
                warnings: Vec::new(),
                blocked_by: vec![blocked.clone()],
                scopes: vec![blocked],
            }
        }

        async fn set_used(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
            _tokens: u64,
        ) -> SFResult<()> {
            Ok(())
        }

        async fn history(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
            _days: u32,
        ) -> SFResult<Vec<cog_core::UsageHistoryEntry>> {
            Ok(Vec::new())
        }

        async fn limits(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
        ) -> SFResult<Option<cog_core::QuotaLimits>> {
            Ok(None)
        }

        async fn set_limits(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
            _limits: cog_core::QuotaLimits,
        ) -> SFResult<()> {
            Ok(())
        }

        async fn clear_limits(
            &self,
            _scope: cog_core::QuotaScope,
            _target_id: &str,
        ) -> SFResult<()> {
            Ok(())
        }
    }

    /// 额度用尽时，界面上的一句话也派不出去——与 HTTP 提交面同一个答案。
    ///
    /// 不加这一道，同一个人今天在 HTTP 上已经提交不了，在聊天框里却还能派；
    /// 而两条路要的是同一份钱，账上看得出来，准入上却看不见。
    #[tokio::test]
    async fn a_dispatch_over_the_day_s_quota_is_refused_before_it_reaches_the_orchestrator() {
        let admission: Arc<dyn HierarchyManager> = Arc::new(Exhausted);
        let refusal = admission_refusal(Some(&admission), "someone", "把首页的颜色改一下")
            .await
            .expect("额度用尽时必须拒绝");
        let sentence = refusal.sentence();
        assert!(sentence.contains("额度"), "{sentence}");
    }

    /// 一条只记不读的审计流，用来数拒绝有没有落账。
    #[derive(Default)]
    struct RecordingAudit(std::sync::Mutex<Vec<(AuditKind, String, String, String)>>);

    #[async_trait::async_trait]
    impl AuditStream for RecordingAudit {
        async fn append(
            &self,
            kind: AuditKind,
            actor: &str,
            target: &str,
            action: &str,
            _detail: serde_json::Value,
        ) -> SFResult<cog_core::AuditEvent> {
            self.0.lock().unwrap().push((
                kind,
                actor.to_string(),
                target.to_string(),
                action.to_string(),
            ));
            Err(SFError::Internal("只记不读".into()))
        }

        async fn read_all(&self) -> SFResult<Vec<cog_core::AuditEvent>> {
            Ok(Vec::new())
        }
    }

    /// 被挡下的派活要在审计链上留一条：提交面拒单写的那一格，界面同样写。
    ///
    /// 不写的话，同一条规则在两个入口上一个留痕一个不留——账上只看得见「今天
    /// 有人没派活」，看不出是被额度挡下的。
    #[tokio::test]
    async fn a_refused_dispatch_leaves_the_same_audit_row_the_submit_face_writes() {
        let audit = Arc::new(RecordingAudit::default());
        let refusal = QuotaRefusal {
            estimated: 128,
            blocked_by: 1,
        };
        let stream: Arc<dyn AuditStream> = audit.clone();
        record_refusal(Some(&stream), "someone", &refusal).await;

        let rows = audit.0.lock().unwrap();
        assert_eq!(rows.len(), 1, "拒绝没有落账");
        let (kind, actor, target, action) = &rows[0];
        assert_eq!(*kind, AuditKind::QuotaEnforcement);
        assert_eq!(actor, "someone");
        assert_eq!(target, "-", "这一次没有目标，账上不该编一个 id");
        assert_eq!(action, "quota.dispatch_rejected");
    }

    /// 没人配过上限时不拦：与提交面一致，「没人管」不等于「谁都不许提交」。
    #[tokio::test]
    async fn a_dispatch_without_a_quota_manager_is_not_refused() {
        assert!(admission_refusal(None, "someone", "把首页的颜色改一下")
            .await
            .is_none());
    }
}
