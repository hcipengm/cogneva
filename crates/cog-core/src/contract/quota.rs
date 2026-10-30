//!Quota and hierarchy management types.

use crate::SFResult;
use serde::{Deserialize, Serialize};

/// Scope levels for the 5-level quota hierarchy.
///
/// One name per level, in [`QuotaScope::as_str`] — the word the counters and the
/// history keys are written under. The serde impls below go through it rather
/// than through the variant names: the derive spelled `workspace` and
/// `organization` while every stored quota key said `ws` and `org`, so a reader
/// holding a serialized decision and the counter it came from had to translate
/// between two vocabularies for one level — and a level whose two spellings do
/// not meet reads as a level with no usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaScope {
    User,
    Workspace,
    Team,
    Organization,
    Global,
}

impl QuotaScope {
    pub fn cascade_order() -> [QuotaScope; 5] {
        [
            QuotaScope::User,
            QuotaScope::Workspace,
            QuotaScope::Team,
            QuotaScope::Organization,
            QuotaScope::Global,
        ]
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            QuotaScope::User => "user",
            QuotaScope::Workspace => "ws",
            QuotaScope::Team => "team",
            QuotaScope::Organization => "org",
            QuotaScope::Global => "global",
        }
    }

    /// `name` 指的那一级，或 `None`：不在 [`Self::cascade_order`] 上的名字不猜。
    pub fn parse(name: &str) -> Option<Self> {
        Self::cascade_order()
            .into_iter()
            .find(|scope| scope.as_str() == name)
    }
}

impl Serialize for QuotaScope {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// 认不出的层级名是错误，不是某一级：把它读成 `User` 会让一条来路不明的记录
/// 落进用户级的计数里，而"这一级没有用量"与"这一级没人认得"从此同形。
impl<'de> Deserialize<'de> for QuotaScope {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = <String as Deserialize>::deserialize(deserializer)?;
        Self::parse(&name).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown quota scope {name:?}; declared scopes are {:?}",
                Self::cascade_order().map(|scope| scope.as_str())
            ))
        })
    }
}

/// Soft / hard limit pair for a single scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaLimits {
    pub soft_limit: u64,
    pub hard_limit: u64,
}

impl QuotaLimits {
    pub fn new(soft_limit: u64, hard_limit: u64) -> Self {
        Self {
            soft_limit: soft_limit.min(hard_limit),
            hard_limit,
        }
    }

    pub fn from_hard(hard_limit: u64, soft_ratio: f64) -> Self {
        Self {
            soft_limit: ((hard_limit as f64) * soft_ratio) as u64,
            hard_limit,
        }
    }
}

impl Default for QuotaLimits {
    fn default() -> Self {
        Self {
            soft_limit: 80_000,
            hard_limit: 100_000,
        }
    }
}

/// Status of a single scope in the hierarchy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeStatus {
    pub scope: QuotaScope,
    pub target_id: String,
    pub remaining: u64,
    pub used_today: u64,
    pub limits: QuotaLimits,
    pub blocking: bool,
    pub warning: bool,
}

/// Context for a hierarchy quota check.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaContext {
    pub user_id: Option<String>,
    pub workspace_id: Option<String>,
    pub team_id: Option<String>,
    pub organization_id: Option<String>,
    pub global_id: Option<String>,
}

impl QuotaContext {
    pub fn target(&self, scope: QuotaScope) -> Option<&str> {
        match scope {
            QuotaScope::User => self.user_id.as_deref(),
            QuotaScope::Workspace => self.workspace_id.as_deref(),
            QuotaScope::Team => self.team_id.as_deref(),
            QuotaScope::Organization => self.organization_id.as_deref(),
            QuotaScope::Global => self.global_id.as_deref(),
        }
    }
}

/// Summary of quota usage for a target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaSummary {
    pub target_id: String,
    pub target_type: String,
    pub total_quota: u64,
    pub remaining: u64,
    pub used_today: u64,
}

/// Result of a pre-check quota request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PreCheckResult {
    pub allowed: bool,
    pub remaining: u64,
    pub estimated_cost: f64,
}

/// Aggregate result of a hierarchical check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HierarchyDecision {
    pub allowed: bool,
    pub warnings: Vec<ScopeStatus>,
    pub blocked_by: Vec<ScopeStatus>,
    pub scopes: Vec<ScopeStatus>,
}

/// Single daily usage record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageHistoryEntry {
    pub date: String,
    pub tokens_used: u64,
}

/// Token quota manager.
#[async_trait::async_trait]
pub trait QuotaManager: Send + Sync {
    async fn pre_check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult;

    async fn finalize(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
        actual_tokens: u64,
    ) -> SFResult<()>;

    async fn get_remaining(&self, user_id: &str) -> u64;
    async fn get_workspace_remaining(&self, workspace_id: &str) -> u64;
    async fn get_used_today(&self, user_id: &str) -> u64;
    async fn get_workspace_used_today(&self, workspace_id: &str) -> u64;
    async fn get_request_count_today(&self, user_id: &str) -> u64;
    async fn get_user_summary(&self, user_id: &str) -> QuotaSummary;
    async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary;

    async fn recharge(
        &self,
        user_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<()>;

    async fn recharge_workspace(
        &self,
        workspace_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<()>;
}

/// 5-level quota hierarchy manager.
#[async_trait::async_trait]
pub trait HierarchyManager: Send + Sync {
    async fn check(&self, ctx: &QuotaContext, tokens: u64) -> HierarchyDecision;
    async fn consume(&self, ctx: &QuotaContext, tokens: u64) -> SFResult<HierarchyDecision>;
    async fn refund(&self, ctx: &QuotaContext, tokens: u64) -> SFResult<()>;
    async fn pre_deduct(&self, ctx: &QuotaContext, tokens: u64) -> SFResult<HierarchyDecision>;
    async fn history(
        &self,
        scope: QuotaScope,
        target_id: &str,
        days: u32,
    ) -> SFResult<Vec<UsageHistoryEntry>>;

    /// The limits configured *for this cell*, or `None` when nobody set any.
    ///
    /// Deliberately not the same answer [`check`](Self::check) gives: a check
    /// reports the limits it evaluated with, which are the manager's defaults
    /// whenever no override exists. A reader of that answer cannot tell "an
    /// operator set exactly the default" from "nobody configured this level" —
    /// and that difference is the whole question a management face asks, since
    /// clearing an override only changes anything in the second case's absence.
    async fn limits(&self, scope: QuotaScope, target_id: &str) -> SFResult<Option<QuotaLimits>>;

    /// Configure the daily soft/hard ceilings for one `(scope, target)` cell.
    /// Set these to give one person, one workspace, or one team its own
    /// ceiling; the cascade still checks every level above it.
    async fn set_limits(
        &self,
        scope: QuotaScope,
        target_id: &str,
        limits: QuotaLimits,
    ) -> SFResult<()>;

    /// Drop the override for one `(scope, target)` cell so that the manager's
    /// default applies again. Absent is not an error: clearing twice is what a
    /// retried request does.
    async fn clear_limits(&self, scope: QuotaScope, target_id: &str) -> SFResult<()>;
}

/// Per-workspace quota source used by the supervisor.
#[async_trait::async_trait]
pub trait WorkspaceQuotaSource: Send + Sync {
    async fn workspace_remaining(&self, workspace_id: &str) -> u64;
}

/// 按任务的 token 预算：一件工作由调用方自己声明上限。
///
/// 与 [`HierarchyManager`] 的五级级联是两件事，不是它的第六级：那五级互相**包含**
/// （用户属于工作区，工作区属于团队……），检查时逐级走；任务是工作单位，属于某个
/// 租户却不与任何一级构成上下级关系。把任务塞进 `scope` 那个闭集里，`cascade_order`
/// 就变成一个混着两种关系的列表，按它逐级走的检查会把两件事读成一件。
///
/// 声明的是**今天**的上限：计数到下一个 UTC 午夜归零，被拒的那一发拿到的等待也是
/// 到那个午夜。
///
/// 写成契约而不是让网关直接依赖实现，是因为写它的（配额实现）与问它的（LLM 代理
/// 路径）在不同的 crate 里，而业务 crate 之间不直接互相依赖：两边各自认这一个 trait，
/// 由装配根把它们接起来。
#[async_trait::async_trait]
pub trait TaskBudgetGate: Send + Sync {
    /// 声明（或改写）一件任务的软/硬上限。改写不动今天的计数：改上限不是发一笔新额度。
    async fn set_budget(&self, task_id: &str, limits: QuotaLimits) -> SFResult<()>;

    /// 撤掉声明，让这件任务回到「没人管」。本来就没有不是错误。
    async fn clear_budget(&self, task_id: &str) -> SFResult<()>;

    /// 这件任务声明的上限，`None` 表示没人声明过。
    ///
    /// 与 [`Self::check`] 答的不是同一个问题：检查用的是一份会退到默认值的上限，
    /// 读那份答案的人分不出「按默认值配的」与「没人配过」。
    async fn budget(&self, task_id: &str) -> SFResult<Option<QuotaLimits>>;

    /// 所有声明过上限的任务。
    ///
    /// 管理面要回答「现在有哪些任务有上限」，而这件事按 id 问是无解的——问的人
    /// 得先知道 id。判定一个字节都不读它：清单错了，错的是清单，不是任何一次放行。
    async fn budgeted_tasks(&self) -> SFResult<Vec<String>>;

    /// 判这一发放不放行，不记账。`now_unix` 是参数而不是读时钟：日界决定了拒绝时
    /// 给出多少等待，而「午夜前一秒」那一格只有把时钟交进来才测得了。
    async fn check(&self, task_id: &str, now_unix: i64) -> SFResult<TaskBudgetVerdict>;

    /// 记一次真实花销，返回记完之后今天这件任务的累计。
    async fn charge(&self, task_id: &str, tokens: u64, now_unix: i64) -> SFResult<u64>;
}

/// 一次按任务预算的判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskBudgetVerdict {
    /// 有没有人给这件任务声明过上限。`false` 时下面三个上限字段无意义。
    pub budgeted: bool,
    /// 今天这件任务已经花掉的 token。
    pub spent_today: u64,
    /// 声明的软上限（只用来告警/响应头）。
    pub soft_limit: u64,
    /// 声明的硬上限。
    pub hard_limit: u64,
    /// 这一次放不放行。
    pub allowed: bool,
    /// 放行了，但这一发已经跨过软上限。
    pub warning: bool,
    /// 被拒时离下一次重置还有多少秒。
    pub wait_secs: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一级一个名字：键上写的、序列化写出去的、读回来的都是 `as_str`。
    ///
    /// 下面的 match 刻意穷尽，新增一级会让它编译不过——逼着人把新的一级同时
    /// 放进层叠顺序，而不是只加变体、让 `cascade_order` 少一格：少的那一格在
    /// 额度面上永远不发一档，"这一级没有限额"与"这一级没人检查"从此同形。
    #[test]
    fn every_scope_has_one_name_and_it_survives_the_wire() {
        let mut names = std::collections::BTreeSet::new();
        for scope in [
            QuotaScope::User,
            QuotaScope::Workspace,
            QuotaScope::Team,
            QuotaScope::Organization,
            QuotaScope::Global,
        ] {
            match scope {
                QuotaScope::User
                | QuotaScope::Workspace
                | QuotaScope::Team
                | QuotaScope::Organization
                | QuotaScope::Global => {}
            }
            assert!(
                QuotaScope::cascade_order().contains(&scope),
                "{scope:?} 不在层叠顺序里，这一级永远不会被检查"
            );
            assert!(
                names.insert(scope.as_str()),
                "两级共用一个名字 {:?}，额度键会互相覆盖",
                scope.as_str()
            );
            let json = serde_json::to_string(&scope).expect("scope serializes");
            assert_eq!(json, format!("\"{}\"", scope.as_str()));
            assert_eq!(serde_json::from_str::<QuotaScope>(&json).ok(), Some(scope));
        }
        // 变体名不是名字：以前序列化写的是这一套，而键上写的是另一套。
        assert!(serde_json::from_str::<QuotaScope>("\"Workspace\"").is_err());
        assert!(serde_json::from_str::<QuotaScope>("\"organization\"").is_err());
    }
}
