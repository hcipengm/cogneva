//! 5-level quota hierarchy: user → workspace → team → organization → global.
//! A request is allowed only when *all* applicable scopes have enough
//! remaining quota. Each scope can be configured independently with a soft
//! limit (warning) and a hard limit (block). When the soft limit is crossed
//! the [`HierarchyDecision`] reports `warnings` so callers can emit telemetry
//! or response headers; the hard limit triggers a block via
//! [`HierarchyDecision::allowed = false`].
//! ## Redis keys
//! For each `(scope, id)`:
//! | key                                   | meaning                         |
//! |---------------------------------------|---------------------------------|
//! | `quota:<scope>:remaining:<id>`        | remaining tokens (today)        |
//! | `quota:<scope>:used:<id>`             | tokens consumed today           |
//! | `quota:<scope>:requests:<id>`         | request count today             |
//! | `quota:<scope>:limits:<id>`           | JSON `QuotaLimits`              |
//! | `quota:<scope>:history:<id>:<YYYYMMDD>` | per-day used token history    |
//! All counters carry a TTL until the next UTC midnight, so daily reset is
//! automatic.

use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{QuotaError, QuotaResult};
use crate::store::{read_int, store_error};
use crate::{
    HierarchyDecision, QuotaContext, QuotaLimits, QuotaScope, ScopeStatus, UsageHistoryEntry,
};
use cog_core::KeyValueStore;

// cap: bound — 一个谁都没配过的格子读到的默认日上限。它没有实测来源，也不该有：
// 这个数只决定「没配过的格子读成什么」，决定谁被拦下的是属主为那个人声明的那一对。
/// The daily token ceiling a scope starts with before anyone configures it.
///
/// It is a bound, not a policy: enforcement on the person axis applies only to
/// cells an operator configured (see the LLM gateway's owner check), so this
/// number decides what an unconfigured cell *reads* as, never who gets cut off.
pub const PLATFORM_DAILY_TOKENS: u64 = 50_000;

/// Hierarchy checker over a key-value store.
#[derive(Clone)]
pub struct HierarchyManager {
    /// Held behind a lock so that the read-modify-write sequences below (and the
    /// per-scope cascades, which touch several keys) stay a single step for
    /// concurrent calls through this manager.
    store: Arc<Mutex<Arc<dyn KeyValueStore>>>,
    default_limits: QuotaLimits,
    /// History retention in days (default 30).
    history_days: u32,
}

impl std::fmt::Debug for HierarchyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Whether a store is held is what a reader needs from this type; the
        // store's own internals are not.
        f.debug_struct("HierarchyManager")
            .field("store", &"key-value store")
            .field("default_limits", &self.default_limits)
            .field("history_days", &self.history_days)
            .finish()
    }
}

impl HierarchyManager {
    pub fn new(store: Arc<dyn KeyValueStore>, default_limits: QuotaLimits) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            default_limits,
            history_days: 30,
        }
    }

    /// A manager on this store with the platform's own starting ceiling.
    ///
    /// Two processes build one of these on the same store -- the app holding
    /// the management face and the LLM gateway holding the enforcement point --
    /// so the number they start from has to be one definition, not two that
    /// happen to agree today. A cell an operator configured keeps its own pair
    /// either way; this is only what a cell with no override evaluates against.
    pub fn from_store(store: Arc<dyn KeyValueStore>) -> Self {
        Self::new(store, QuotaLimits::from_hard(PLATFORM_DAILY_TOKENS, 0.8))
    }

    /// Override the default history retention window (in days).
    pub fn with_history_days(mut self, days: u32) -> Self {
        self.history_days = days.max(1);
        self
    }

    fn key(scope: QuotaScope, kind: &str, id: &str) -> String {
        format!("quota:{}:{}:{}", scope.as_str(), kind, id)
    }

    fn history_key(scope: QuotaScope, id: &str, ymd: &str) -> String {
        format!("quota:{}:history:{}:{}", scope.as_str(), id, ymd)
    }

    /// How long a counter written now may live before the day turns.
    ///
    /// The bound is the rest of the UTC day, not "at least a minute": a counter
    /// written at 23:59:30 with a minute of slack would outlive the reset and
    /// charge the new day with yesterday's spend.
    fn seconds_until_midnight() -> u64 {
        crate::day::seconds_to_next_day(Utc::now().timestamp())
    }

    /// Configure the soft/hard limits for a particular `(scope, target)` pair.
    pub async fn set_limits(
        &self,
        scope: QuotaScope,
        target_id: &str,
        limits: QuotaLimits,
    ) -> QuotaResult<()> {
        self.write_limits(scope, target_id, limits).await
    }

    /// Read the soft/hard limits for `(scope, target)`. Falls back to the
    /// manager's default if no override is set.
    pub async fn get_limits(&self, scope: QuotaScope, target_id: &str) -> QuotaLimits {
        match self.override_limits(scope, target_id).await {
            Ok(Some(limits)) => limits,
            // 限值读坏了就退回默认值，而不是让整套检查失败：检查面没有报错的出口，
            // 而默认值本来就是「什么都没配」时生效的那一对。坏值以错误形式出现在
            // 管理读（`override_limits` 的调用方）上——那是能修它的那一面。
            Ok(None) | Err(_) => self.default_limits,
        }
    }

    /// Drop the override for `(scope, target)`, so that the manager's default
    /// governs that cell again. A cell that had none is not an error.
    pub async fn clear_limits(&self, scope: QuotaScope, target_id: &str) -> QuotaResult<()> {
        self.drop_limits(scope, target_id).await
    }

    /// Remove the override key. Deleting the key rather than writing the
    /// manager's current default: a written default would read back as "an
    /// operator chose this", and would keep the old ceiling for every target
    /// only until the next restart moved the default.
    async fn drop_limits(&self, scope: QuotaScope, target_id: &str) -> QuotaResult<()> {
        let store = self.store.lock().await;
        let key = Self::key(scope, "limits", target_id);
        store.del(&key).await.map_err(store_error)?;
        Ok(())
    }

    /// The override stored for `(scope, target)`, or `None` when there is none.
    ///
    /// A stored value that no longer parses is reported as an error rather than
    /// as "no override": the two would otherwise read the same at every caller,
    /// and the difference is that the first case has a limit on disk that
    /// nothing is applying.
    async fn override_limits(
        &self,
        scope: QuotaScope,
        target_id: &str,
    ) -> QuotaResult<Option<QuotaLimits>> {
        let store = self.store.lock().await;
        let key = Self::key(scope, "limits", target_id);
        let Some(raw) = store.get(&key).await.map_err(store_error)? else {
            return Ok(None);
        };
        serde_json::from_str(&raw).map(Some).map_err(|e| {
            QuotaError::DatabaseError(format!("the limits stored under {key} do not parse: {e}"))
        })
    }

    /// Write an override. One store call and no read: the cell's counters are
    /// not reset by a new ceiling, because the ceiling is a daily bound and the
    /// day is what the counters are scoped to.
    async fn write_limits(
        &self,
        scope: QuotaScope,
        target_id: &str,
        limits: QuotaLimits,
    ) -> QuotaResult<()> {
        let store = self.store.lock().await;
        let payload = serde_json::to_string(&limits)?;
        let key = Self::key(scope, "limits", target_id);
        store.set(&key, &payload).await.map_err(store_error)?;
        Ok(())
    }

    async fn used_today(&self, scope: QuotaScope, target_id: &str) -> u64 {
        let store = self.store.lock().await;
        let key = Self::key(scope, "used", target_id);
        let used = read_int(&**store, &key).await;
        used.unwrap_or(0).max(0) as u64
    }

    /// Evaluate a single `(scope, target)` cell against its limits without
    /// consuming.
    async fn evaluate_scope(
        &self,
        scope: QuotaScope,
        target_id: &str,
        estimated_tokens: u64,
    ) -> ScopeStatus {
        let limits = self.get_limits(scope, target_id).await;
        let used = self.used_today(scope, target_id).await;
        let projected = used.saturating_add(estimated_tokens);
        let remaining = limits.hard_limit.saturating_sub(used);
        let blocking = projected > limits.hard_limit;
        let warning = !blocking && projected >= limits.soft_limit;
        ScopeStatus {
            scope,
            target_id: target_id.to_string(),
            remaining,
            used_today: used,
            limits,
            blocking,
            warning,
        }
    }

    /// Walk the entire hierarchy and produce a non-mutating decision.
    pub async fn check(&self, ctx: &QuotaContext, estimated_tokens: u64) -> HierarchyDecision {
        let mut scopes = Vec::new();
        for scope in QuotaScope::cascade_order() {
            if let Some(target) = ctx.target(scope) {
                scopes.push(self.evaluate_scope(scope, target, estimated_tokens).await);
            }
        }
        let blocked_by: Vec<_> = scopes.iter().filter(|&s| s.blocking).cloned().collect();
        let warnings: Vec<_> = scopes.iter().filter(|&s| s.warning).cloned().collect();
        HierarchyDecision {
            allowed: blocked_by.is_empty(),
            warnings,
            blocked_by,
            scopes,
        }
    }

    /// Apply real consumption to every applicable scope. Returns the
    /// post-consumption decision so callers can read final remaining counts.
    pub async fn consume(
        &self,
        ctx: &QuotaContext,
        actual_tokens: u64,
    ) -> QuotaResult<HierarchyDecision> {
        let ttl = Self::seconds_until_midnight();
        let today = Utc::now().date_naive().to_string();

        for scope in QuotaScope::cascade_order() {
            if let Some(target) = ctx.target(scope) {
                let used_key = Self::key(scope, "used", target);
                let req_key = Self::key(scope, "requests", target);
                let history_key = Self::history_key(scope, target, &today);

                let store = self.store.lock().await;
                let _ = store.incr(&used_key, actual_tokens as i64).await;
                let _ = store.expire(&used_key, ttl as i64).await;
                let _ = store.incr(&req_key, 1).await;
                let _ = store.expire(&req_key, ttl as i64).await;
                let _ = store.incr(&history_key, actual_tokens as i64).await;
                let _ = store
                    .expire(&history_key, (self.history_days as i64) * 86_400)
                    .await;
            }
        }

        Ok(self.check(ctx, 0).await)
    }

    /// Refund a previous over-deduction. Equivalent to `consume(-amount)`.
    pub async fn refund(&self, ctx: &QuotaContext, tokens: u64) -> QuotaResult<()> {
        for scope in QuotaScope::cascade_order() {
            if let Some(target) = ctx.target(scope) {
                let store = self.store.lock().await;
                let used_key = Self::key(scope, "used", target);
                let _ = store.decr(&used_key, tokens as i64).await;
            }
        }
        Ok(())
    }

    /// Pre-deduct estimated tokens up-front. Returns Err if any scope is
    /// already at or past its hard limit.
    pub async fn pre_deduct(
        &self,
        ctx: &QuotaContext,
        estimated_tokens: u64,
    ) -> QuotaResult<HierarchyDecision> {
        let decision = self.check(ctx, estimated_tokens).await;
        if !decision.allowed {
            return Err(QuotaError::InsufficientQuota {
                required: estimated_tokens,
                remaining: decision
                    .blocked_by
                    .first()
                    .map(|s| s.remaining)
                    .unwrap_or(0),
            });
        }
        // No mutation in pre_deduct; counters move only in `consume`. This
        // avoids double-counting refunds when `actual ≠ estimated`.
        Ok(decision)
    }

    /// Read the recent N-day history for one `(scope, target)` cell.
    pub async fn history(
        &self,
        scope: QuotaScope,
        target_id: &str,
        days: u32,
    ) -> QuotaResult<Vec<UsageHistoryEntry>> {
        let days = days.min(self.history_days).max(1);
        let mut out = Vec::with_capacity(days as usize);
        for n in 0..days {
            let date = Utc::now().date_naive() - chrono::Duration::days(n as i64);
            let ymd = date.to_string();
            let store = self.store.lock().await;
            let key = Self::history_key(scope, target_id, &ymd);
            let used = read_int(&**store, &key).await;
            out.push(UsageHistoryEntry {
                date: ymd,
                tokens_used: used.unwrap_or(0).max(0) as u64,
            });
        }
        Ok(out)
    }
}

/// A refusal a caller can act on: which ceiling stopped it, how far past that
/// ceiling the projection went, and when the counters reset.
///
/// [`HierarchyDecision`] carries the same numbers per scope, but a caller
/// answering a request needs two things it does not: one named ceiling (the
/// first that blocked, in cascade order) and the wait until the reset. Both are
/// derived at the moment of the decision rather than stored, because both are
/// facts about that moment and not about the counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyCapRefusal {
    pub scope: QuotaScope,
    pub target_id: String,
    /// The scope's hard limit, in tokens per UTC day.
    pub cap_tokens: u64,
    /// What that scope had spent when the decision was made.
    pub spent_tokens: u64,
    /// The projection the decision was made with, passed back rather than
    /// inferred: `spent_tokens` alone cannot say how far past the ceiling the
    /// request would have gone.
    pub estimated_tokens: u64,
    /// Seconds until the counters reset, at least one.
    pub retry_after_secs: u64,
}

/// The refusal a decision names, or `None` when it allows the call.
///
/// The first blocking scope is the one named, not the largest overrun: they are
/// reported in cascade order (`user` before `workspace` before `global`), so the
/// named ceiling is the narrowest one the caller can ask its owner to raise.
pub fn refusal_for(
    decision: &HierarchyDecision,
    estimated_tokens: u64,
    now_unix: i64,
) -> Option<DailyCapRefusal> {
    let blocked = decision.blocked_by.first()?;
    Some(DailyCapRefusal {
        scope: blocked.scope,
        target_id: blocked.target_id.clone(),
        cap_tokens: blocked.limits.hard_limit,
        spent_tokens: blocked.used_today,
        estimated_tokens,
        retry_after_secs: crate::day::seconds_to_next_day(now_unix),
    })
}

#[async_trait::async_trait]
impl cog_core::HierarchyManager for HierarchyManager {
    async fn check(&self, ctx: &QuotaContext, tokens: u64) -> HierarchyDecision {
        self.check(ctx, tokens).await
    }

    async fn consume(
        &self,
        ctx: &QuotaContext,
        tokens: u64,
    ) -> cog_core::SFResult<HierarchyDecision> {
        self.consume(ctx, tokens)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn refund(&self, ctx: &QuotaContext, tokens: u64) -> cog_core::SFResult<()> {
        self.refund(ctx, tokens)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn pre_deduct(
        &self,
        ctx: &QuotaContext,
        tokens: u64,
    ) -> cog_core::SFResult<HierarchyDecision> {
        self.pre_deduct(ctx, tokens)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn history(
        &self,
        scope: QuotaScope,
        target_id: &str,
        days: u32,
    ) -> cog_core::SFResult<Vec<UsageHistoryEntry>> {
        self.history(scope, target_id, days)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn limits(
        &self,
        scope: QuotaScope,
        target_id: &str,
    ) -> cog_core::SFResult<Option<QuotaLimits>> {
        self.override_limits(scope, target_id)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn set_limits(
        &self,
        scope: QuotaScope,
        target_id: &str,
        limits: QuotaLimits,
    ) -> cog_core::SFResult<()> {
        self.write_limits(scope, target_id, limits)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn clear_limits(&self, scope: QuotaScope, target_id: &str) -> cog_core::SFResult<()> {
        self.drop_limits(scope, target_id)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }
}
