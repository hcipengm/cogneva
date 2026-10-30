//! 按任务的 token 预算：给一件具体的工作声明一个上限，超过就拒。
//!
//! 与 [`crate::hierarchy::HierarchyManager`] 的五级层级是两件事，不是它的第六级。
//! 那五级回答的是「这一格租户还能花多少」，它们互相是**包含**关系（用户属于工作区，
//! 工作区属于团队……），所以检查时逐级走一遍级联；任务是工作单位，属于某个租户而
//! 不与任何一级构成上下级，把任务塞进那张级联表里会让 `cascade_order` 变成一个
//! 混着两种关系的列表，而按它逐级走的那段检查会把两件事读成一件。
//!
//! 这个上限是**操作者声明的**，不是从别处推出来的。没有人声明过预算的任务就没有
//! 上限（[`TaskBudgetDecision::limits`] 为 `None`），而不是拿一个默认值去当上限：
//! 编一个界，会在一件本来没人管的任务上报出「超额」，而那句话没有下界可以引。
//!
//! 一条预算只管**当天**：计数器的 TTL 到下一个 UTC 午夜，因此日界就是重置点，
//! 拒绝时给出的等待也是到那个午夜（[`crate::day::seconds_to_next_day`]）。
//!
//! 键与层级那套同一个存储、同一套形状：
//!
//! | 键                            | 含义                     |
//! |-------------------------------|--------------------------|
//! | `quota:task:limits:<id>`      | JSON `QuotaLimits`       |
//! | `quota:task:used:<id>`        | 今天已花的 token         |
//! | `quota:task:requests:<id>`    | 今天的调用次数           |
//!
//! 限值键**不带 TTL**：预算是对这件工作整体说的，午夜不该把它抹掉。计数器带
//! TTL，这就是日置零。

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::day::seconds_to_next_day;
use crate::error::{QuotaError, QuotaResult};
use crate::store::{read_int, store_error};
use cog_core::{KeyValueStore, QuotaLimits};

/// 一件任务的预算判定。判定与它所依据的读数同一次给出，调用方不必再回读一遍。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskBudgetDecision {
    /// 被判的任务。
    pub task_id: String,
    /// 声明的预算。`None` 表示没人给这件任务声明过——这不是「上限为零」，
    /// 是「没有上限」，两者在放行与否上恰好相反。
    pub limits: Option<QuotaLimits>,
    /// 今天这件任务已经花掉的 token。
    pub spent_today: u64,
    /// 还剩多少（按硬上限减已花）。没有预算时没有这个数。
    pub remaining: Option<u64>,
    /// 这一次放不放行。
    pub allowed: bool,
    /// 放行了，但这一发会跨过软上限（只用来发告警/响应头）。
    pub warning: bool,
    /// 被拒时离下一次重置还有多少秒。放行时为 `None`。
    ///
    /// 与层级那套同一个理由：拒绝必须自带等待，否则对面只能自己猜什么时候再来，
    /// 而猜早了的那一次会被再拒一次。
    pub wait_secs: Option<u64>,
}

/// 按任务的预算管理面。
///
/// 存储走 [`cog_core::KeyValueStore`] 契约，与 [`crate::hierarchy::HierarchyManager`]
/// 是同一套（进程里通常指向同一个 Redis 连接）。这里不加读写锁之外的任何缓存：
/// 计数器是拒与不拒的唯一依据，两条路径各自缓存一份就会各自判一次。
#[derive(Clone)]
pub struct TaskBudgetManager {
    store: Arc<Mutex<Arc<dyn KeyValueStore>>>,
}

impl std::fmt::Debug for TaskBudgetManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 读者要从这个类型上读到的是「它有没有拿到存储」，不是存储的内部。
        f.debug_struct("TaskBudgetManager")
            .field("store", &"key-value store")
            .finish()
    }
}

impl TaskBudgetManager {
    pub fn new(store: Arc<dyn KeyValueStore>) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    fn key(kind: &str, task_id: &str) -> String {
        format!("quota:task:{kind}:{task_id}")
    }

    /// 声明（或改写）一件任务的预算。
    ///
    /// 只写限值，不动计数器：改上限不是给这件工作发一笔新的额度，它今天已经花掉的
    /// 那部分照样算数。要让某件任务从头开始，得换一个 task_id——一件工作的花销没有
    /// 「先前的都不算」这回事。
    pub async fn set_budget(&self, task_id: &str, limits: QuotaLimits) -> QuotaResult<()> {
        let store = self.store.lock().await;
        let payload = serde_json::to_string(&limits)?;
        store
            .set(&Self::key("limits", task_id), &payload)
            .await
            .map_err(store_error)?;
        Ok(())
    }

    /// 读一件任务声明的预算。
    ///
    /// 一条解析不了的记录是错误，不是 `None`：两者在调用方读起来不同——一个是
    /// 「没人声明过，放行」，另一个是「有个人声明了一个上限，而它现在没有任何东西
    /// 在生效」，把后者说成前者就是静默地撤掉了一条预算。
    pub async fn budget(&self, task_id: &str) -> QuotaResult<Option<QuotaLimits>> {
        let store = self.store.lock().await;
        let key = Self::key("limits", task_id);
        let Some(raw) = store.get(&key).await.map_err(store_error)? else {
            return Ok(None);
        };
        serde_json::from_str(&raw).map(Some).map_err(|e| {
            QuotaError::DatabaseError(format!(
                "the task budget stored under {key} does not parse: {e}"
            ))
        })
    }

    /// 撤掉一件任务的预算。本来就没有的时候不是错误。
    ///
    /// 删键而不是写一个「无上限」的值：写下去的那份会读作「有个人声明过这条预算」。
    pub async fn clear_budget(&self, task_id: &str) -> QuotaResult<()> {
        let store = self.store.lock().await;
        store
            .del(&Self::key("limits", task_id))
            .await
            .map_err(store_error)?;
        Ok(())
    }

    /// 今天这件任务已经花掉的 token。
    ///
    /// 读不到计数器按 0 处理，与层级那套一致：没有读数与「还没花过」在这里是同一次
    /// 检查的同一个结果，而把读失败当成「已花满」会把每一次调用都拒掉。
    pub async fn spent_today(&self, task_id: &str) -> u64 {
        let store = self.store.lock().await;
        read_int(&**store, &Self::key("used", task_id))
            .await
            .unwrap_or(0)
            .max(0) as u64
    }

    /// 判这一发放不放行，不落账。`now_unix` 是参数而不是读时钟：日界决定了拒绝时
    /// 给出多少等待，而「午夜前一秒」那一格只有把时钟交进来才测得了。
    pub async fn check(
        &self,
        task_id: &str,
        estimated_tokens: u64,
        now_unix: i64,
    ) -> QuotaResult<TaskBudgetDecision> {
        let limits = self.budget(task_id).await?;
        let spent_today = self.spent_today(task_id).await;
        Ok(verdict(
            task_id,
            limits,
            spent_today,
            estimated_tokens,
            now_unix,
        ))
    }

    /// 把一次真实花销记到这件任务上，返回记完之后今天的花销。
    ///
    /// 账只在调用真的发生之后记：预扣要配一次回滚，而回滚那条路径在中途崩溃时会留下
    /// 一笔从没发生过的花销。先放行后记账的代价是并发下的最后一发可以略微越过上限
    /// ——越过的是「同时在路上」的那几发各自的估算误差，而不是一整窗的量。
    pub async fn charge(
        &self,
        task_id: &str,
        actual_tokens: u64,
        now_unix: i64,
    ) -> QuotaResult<u64> {
        let ttl = seconds_to_next_day(now_unix);
        let store = self.store.lock().await;
        let used_key = Self::key("used", task_id);
        let req_key = Self::key("requests", task_id);
        // 加与设过期是一步：分成 `incr` + `expire` 两步时，中间崩溃留下的计数器
        // 没有 TTL，于是这件任务的花销从此永不重置——一条日上限变成了终身上限，
        // 而它读上去和「今天花得多」一模一样。
        let spent = store
            .incr_ex(&used_key, actual_tokens as i64, ttl)
            .await
            .map_err(store_error)?;
        let _ = store.incr_ex(&req_key, 1, ttl).await;
        Ok(spent.max(0) as u64)
    }

    /// 今天这件任务被放行（也含被拒）了多少次。读不到按 0。
    pub async fn requests_today(&self, task_id: &str) -> u64 {
        let store = self.store.lock().await;
        read_int(&**store, &Self::key("requests", task_id))
            .await
            .unwrap_or(0)
            .max(0) as u64
    }
}

/// 判定本身，与存储无关。
///
/// 单独成一个函数是因为这套判定要能在没有 Redis 的地方被逐格验：放行、只告警、
/// 硬拒、没有预算、以及午夜前一秒的等待。
pub fn verdict(
    task_id: &str,
    limits: Option<QuotaLimits>,
    spent_today: u64,
    estimated_tokens: u64,
    now_unix: i64,
) -> TaskBudgetDecision {
    let Some(limits) = limits else {
        return TaskBudgetDecision {
            task_id: task_id.to_string(),
            limits: None,
            spent_today,
            remaining: None,
            allowed: true,
            warning: false,
            wait_secs: None,
        };
    };
    let projected = spent_today.saturating_add(estimated_tokens);
    let allowed = projected <= limits.hard_limit;
    let remaining = limits.hard_limit.saturating_sub(spent_today);
    TaskBudgetDecision {
        task_id: task_id.to_string(),
        limits: Some(limits),
        spent_today,
        remaining: Some(remaining),
        allowed,
        // 只有放行的才有「告警」这一说：被拒的那一发已经写了它的理由，
        // 再标一个 soft 会让同一件事在读数上成为两件。
        warning: allowed && projected >= limits.soft_limit,
        wait_secs: (!allowed).then(|| seconds_to_next_day(now_unix)),
    }
}

/// 把这一面自己的错误类型翻成 core 的，分两类落到它自己的两个变体上：存储错误是
/// 缓存的问题（重连会好），记录读不出来是数据的问题（重连不会好）。两者落到同一个
/// 变体上，读日志的人就只能靠措辞分。
fn sf_error(e: QuotaError) -> cog_core::SFError {
    match e {
        QuotaError::RedisError(m) => cog_core::SFError::Redis(m),
        other => cog_core::SFError::Database(other.to_string()),
    }
}

/// 契约面：装配根用这一层把本实现交给 LLM 代理路径，两侧都不必认识对方的 crate。
#[async_trait::async_trait]
impl cog_core::TaskBudgetGate for TaskBudgetManager {
    async fn set_budget(&self, task_id: &str, limits: QuotaLimits) -> cog_core::SFResult<()> {
        TaskBudgetManager::set_budget(self, task_id, limits)
            .await
            .map_err(sf_error)
    }

    async fn clear_budget(&self, task_id: &str) -> cog_core::SFResult<()> {
        TaskBudgetManager::clear_budget(self, task_id)
            .await
            .map_err(sf_error)
    }

    async fn budget(&self, task_id: &str) -> cog_core::SFResult<Option<QuotaLimits>> {
        TaskBudgetManager::budget(self, task_id)
            .await
            .map_err(sf_error)
    }

    async fn check(
        &self,
        task_id: &str,
        now_unix: i64,
    ) -> cog_core::SFResult<cog_core::TaskBudgetVerdict> {
        // 契约这一格不带估算：代理路径在被拒之前并不知道这一发要花多少，而按「已经
        // 花掉多少」判就够了——被拒的是那一发，不是它进来之前的那几发。
        let d = TaskBudgetManager::check(self, task_id, 0, now_unix)
            .await
            .map_err(sf_error)?;
        Ok(cog_core::TaskBudgetVerdict {
            budgeted: d.limits.is_some(),
            spent_today: d.spent_today,
            soft_limit: d.limits.map(|l| l.soft_limit).unwrap_or(0),
            hard_limit: d.limits.map(|l| l.hard_limit).unwrap_or(0),
            allowed: d.allowed,
            warning: d.warning,
            wait_secs: d.wait_secs,
        })
    }

    async fn charge(&self, task_id: &str, tokens: u64, now_unix: i64) -> cog_core::SFResult<u64> {
        TaskBudgetManager::charge(self, task_id, tokens, now_unix)
            .await
            .map_err(sf_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::day::SECONDS_PER_DAY;

    const NOON: i64 = 1_600_000_000 - (1_600_000_000 % SECONDS_PER_DAY) + 12 * 3600;

    #[test]
    fn a_task_nobody_budgeted_is_allowed() {
        let d = verdict("t1", None, 10_000_000, 100_000, NOON);
        assert!(d.allowed);
        assert!(!d.warning);
        assert_eq!(d.remaining, None);
        assert_eq!(d.wait_secs, None);
    }

    #[test]
    fn a_budgeted_task_is_refused_once_the_call_would_cross_the_hard_limit() {
        let limits = QuotaLimits::new(80, 100);
        // 恰好到界放行：界是「用到这里为止」，不是「用到这里就停」。
        let at = verdict("t1", Some(limits), 100, 0, NOON);
        assert!(at.allowed);
        assert!(!at.warning);
        let over = verdict("t1", Some(limits), 100, 1, NOON);
        assert!(!over.allowed);
        assert_eq!(over.remaining, Some(0));
        assert_eq!(over.wait_secs, Some(SECONDS_PER_DAY as u64));
    }

    #[test]
    fn the_wait_is_the_rest_of_the_day_not_a_fixed_number() {
        let limits = QuotaLimits::new(10, 10);
        let last_second = NOON + SECONDS_PER_DAY - 1;
        let d = verdict("t1", Some(limits), 10, 1, last_second);
        assert!(!d.allowed);
        assert_eq!(d.wait_secs, Some(1));
    }

    #[test]
    fn a_call_that_crosses_only_the_soft_limit_is_allowed_with_a_warning() {
        let d = verdict("t1", Some(QuotaLimits::new(50, 100)), 60, 0, NOON);
        assert!(d.allowed);
        assert!(d.warning);
        assert_eq!(d.wait_secs, None);
    }

    #[test]
    fn a_refused_call_is_never_also_a_soft_warning() {
        let d = verdict("t1", Some(QuotaLimits::new(50, 100)), 100, 1, NOON);
        assert!(!d.allowed);
        assert!(!d.warning);
    }
}
