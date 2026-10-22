use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::QuotaResult;
use crate::store::{read_int, store_error};
use crate::{PreCheckResult, QuotaSummary};
use cog_core::KeyValueStore;

/// Daily usage entry.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DailyUsage {
    pub date: String, // YYYY-MM-DD
    pub tokens_used: u64,
    pub request_count: u64,
}

/// Manages per-user and per-workspace token quotas over a key-value store.
#[derive(Clone)]
pub struct QuotaManager {
    /// Held behind a lock because several methods are read-modify-write: each
    /// store operation is atomic on its own, but a read followed by a write is
    /// not, so two concurrent calls through one manager would otherwise read
    /// the same total and each write back its own deduction.
    store: Arc<Mutex<Arc<dyn KeyValueStore>>>,
    default_quota: u64,
    /// This process's own decisions, counted where they are made.
    ///
    /// Held here rather than read back out of the store at scrape time because
    /// the store is shared with every other replica: counting the fleet's
    /// traffic under one pod's name would make the reading depend on how many
    /// replicas happened to be up.
    readings: Arc<crate::observable::QuotaReadings>,
}

impl std::fmt::Debug for QuotaManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Whether a store is held is what a reader needs from this type; the
        // store's own internals are not.
        f.debug_struct("QuotaManager")
            .field("store", &"key-value store")
            .field("default_quota", &self.default_quota)
            .finish()
    }
}

impl QuotaManager {
    pub fn new(store: Arc<dyn KeyValueStore>, default_quota: u64) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            default_quota,
            readings: Arc::new(crate::observable::QuotaReadings::default()),
        }
    }

    /// This manager's decision counters, for the process's metrics face.
    ///
    /// A clone of the handle, so the reader keeps counting into the same
    /// counters the reader was built over.
    pub fn readings(&self) -> Arc<crate::observable::QuotaReadings> {
        self.readings.clone()
    }

    /// Build the Redis key for a user's remaining quota.
    fn user_key(user_id: &str) -> String {
        format!("quota:remaining:{}", user_id)
    }

    /// Build the Redis key for a workspace's remaining quota.
    fn workspace_key(workspace_id: &str) -> String {
        format!("quota:ws:remaining:{}", workspace_id)
    }

    /// Build the Redis key for a user's daily usage counter.
    fn user_used_key(user_id: &str) -> String {
        format!("quota:used:{}", user_id)
    }

    /// Build the Redis key for a workspace's daily usage counter.
    fn workspace_used_key(workspace_id: &str) -> String {
        format!("quota:used:ws:{}", workspace_id)
    }

    /// Build the Redis key for daily request count per user.
    fn user_requests_key(user_id: &str) -> String {
        format!("quota:requests:{}", user_id)
    }

    /// Get seconds until next midnight (UTC).
    ///
    /// Delegates to the crate's one day boundary: a second copy of this
    /// arithmetic is a second answer to "when does the day turn", and a key
    /// whose TTL was computed by the other copy outlives the reset.
    fn seconds_until_midnight() -> u64 {
        crate::day::seconds_to_next_day(chrono::Utc::now().timestamp())
    }

    /// Bootstrap a missing quota key to `default_quota` with TTL until midnight.
    async fn bootstrap_key(&self, store: &dyn KeyValueStore, key: &str) -> QuotaResult<()> {
        let ttl = Self::seconds_until_midnight();
        let value = self.default_quota.to_string();
        if ttl > 0 {
            store.set_ex(key, &value, ttl).await.map_err(store_error)?;
        } else {
            store.set(key, &value).await.map_err(store_error)?;
        }
        Ok(())
    }

    /// Pre-check quota before processing a request.
    /// If enough quota exists, pre-deduct the estimated amount.
    pub async fn pre_check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult {
        let store = self.store.lock().await;

        let user_key = Self::user_key(user_id);
        let user_remaining = read_int(&**store, &user_key).await;
        let user_remaining = if let Some(v) = user_remaining {
            v.max(0) as u64
        } else {
            // Bootstrap missing key with TTL
            let _ = self.bootstrap_key(&**store, &user_key).await;
            self.default_quota
        };

        let ws_remaining = if let Some(ws_id) = workspace_id {
            let ws_key = Self::workspace_key(ws_id);
            let ws = read_int(&**store, &ws_key).await;
            if let Some(v) = ws {
                v.max(0) as u64
            } else {
                let _ = self.bootstrap_key(&**store, &ws_key).await;
                self.default_quota
            }
        } else {
            u64::MAX
        };

        let remaining = user_remaining.min(ws_remaining);
        // remaining == 0 means the quota is exhausted: reject even zero-cost
        // requests so callers get a clear 429 instead of silent pass-through.
        let allowed = remaining > 0 && remaining >= estimated_tokens;

        // Counted at the decision, which is the only place that knows it: a
        // refusal reaches only the caller it refused, so this counter is the
        // whole of what an operator can learn about the rate.
        if allowed {
            self.readings.record_allowed();
        } else {
            self.readings.record_refused();
        }

        if allowed {
            // Deduct: read current (or default), compute new value, and set it.
            // The lock held above is what makes the pair a single step for
            // concurrent calls through this manager.
            let current = read_int(&**store, &user_key)
                .await
                .unwrap_or(self.default_quota as i64);
            let new_remaining = (current - estimated_tokens as i64).max(0);
            let _ = store.set(&user_key, &new_remaining.to_string()).await;

            if let Some(ws_id) = workspace_id {
                let ws_key = Self::workspace_key(ws_id);
                let current = read_int(&**store, &ws_key)
                    .await
                    .unwrap_or(self.default_quota as i64);
                let new_remaining = (current - estimated_tokens as i64).max(0);
                let _ = store.set(&ws_key, &new_remaining.to_string()).await;
            }
        }

        PreCheckResult {
            allowed,
            remaining,
            estimated_cost: 0.0,
        }
    }

    /// Finalize quota after actual consumption.
    /// Adjusts the pre-deduction to match actual usage.
    pub async fn finalize(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
        actual_tokens: u64,
    ) -> QuotaResult<()> {
        let store = self.store.lock().await;

        let diff = estimated_tokens as i64 - actual_tokens as i64;

        if diff > 0 {
            // Over-deducted: refund the difference
            let user_key = Self::user_key(user_id);
            let _ = store.incr(&user_key, diff).await;
            if let Some(ws_id) = workspace_id {
                let ws_key = Self::workspace_key(ws_id);
                let _ = store.incr(&ws_key, diff).await;
            }
        } else if diff < 0 {
            // Under-deducted: deduct additional
            let user_key = Self::user_key(user_id);
            let _ = store.decr(&user_key, -diff).await;
            if let Some(ws_id) = workspace_id {
                let ws_key = Self::workspace_key(ws_id);
                let _ = store.decr(&ws_key, -diff).await;
            }
        }

        // Record actual usage in daily counters with TTL
        let ttl = Self::seconds_until_midnight();
        if ttl > 0 {
            let user_used_key = Self::user_used_key(user_id);
            let _ = store.incr(&user_used_key, actual_tokens as i64).await;
            let _ = store.expire(&user_used_key, ttl as i64).await;

            let user_req_key = Self::user_requests_key(user_id);
            let _ = store.incr(&user_req_key, 1).await;
            let _ = store.expire(&user_req_key, ttl as i64).await;

            if let Some(ws_id) = workspace_id {
                let ws_used_key = Self::workspace_used_key(ws_id);
                let _ = store.incr(&ws_used_key, actual_tokens as i64).await;
                let _ = store.expire(&ws_used_key, ttl as i64).await;
            }
        }

        Ok(())
    }

    /// Get remaining quota for a user.
    pub async fn get_remaining(&self, user_id: &str) -> u64 {
        let store = self.store.lock().await;
        let key = Self::user_key(user_id);
        let remaining = read_int(&**store, &key).await;
        remaining.unwrap_or(self.default_quota as i64).max(0) as u64
    }

    /// Get remaining quota for a workspace.
    pub async fn get_workspace_remaining(&self, workspace_id: &str) -> u64 {
        let store = self.store.lock().await;
        let key = Self::workspace_key(workspace_id);
        let remaining = read_int(&**store, &key).await;
        remaining.unwrap_or(self.default_quota as i64).max(0) as u64
    }

    /// Get today's used tokens for a user.
    pub async fn get_used_today(&self, user_id: &str) -> u64 {
        let store = self.store.lock().await;
        let key = Self::user_used_key(user_id);
        let used = read_int(&**store, &key).await;
        used.unwrap_or(0).max(0) as u64
    }

    /// Get today's used tokens for a workspace.
    pub async fn get_workspace_used_today(&self, workspace_id: &str) -> u64 {
        let store = self.store.lock().await;
        let key = Self::workspace_used_key(workspace_id);
        let used = read_int(&**store, &key).await;
        used.unwrap_or(0).max(0) as u64
    }

    /// Get today's request count for a user.
    pub async fn get_request_count_today(&self, user_id: &str) -> u64 {
        let store = self.store.lock().await;
        let key = Self::user_requests_key(user_id);
        let count = read_int(&**store, &key).await;
        count.unwrap_or(0).max(0) as u64
    }

    /// Get a full quota summary for a user.
    pub async fn get_user_summary(&self, user_id: &str) -> QuotaSummary {
        let remaining = self.get_remaining(user_id).await;
        let used_today = self.get_used_today(user_id).await;
        let total = remaining + used_today;
        QuotaSummary {
            target_id: user_id.to_string(),
            target_type: "user".into(),
            total_quota: total,
            remaining,
            used_today,
        }
    }

    /// Get a full quota summary for a workspace.
    pub async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary {
        let remaining = self.get_workspace_remaining(workspace_id).await;
        let used_today = self.get_workspace_used_today(workspace_id).await;
        let total = remaining + used_today;
        QuotaSummary {
            target_id: workspace_id.to_string(),
            target_type: "workspace".into(),
            total_quota: total,
            remaining,
            used_today,
        }
    }

    /// Recharge a user's quota.
    pub async fn recharge(
        &self,
        user_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> QuotaResult<()> {
        let store = self.store.lock().await;
        let key = Self::user_key(user_id);

        let current = read_int(&**store, &key).await.unwrap_or(0).max(0) as u64;
        let new_amount = (current + tokens).to_string();

        let ttl = if let Some(until) = valid_until {
            let now = chrono::Utc::now();
            let diff = until.signed_duration_since(now);
            diff.num_seconds().max(0) as u64
        } else {
            Self::seconds_until_midnight()
        };

        if ttl > 0 {
            store
                .set_ex(&key, &new_amount, ttl)
                .await
                .map_err(store_error)?;
        } else {
            store.set(&key, &new_amount).await.map_err(store_error)?;
        }
        Ok(())
    }

    /// Recharge a workspace's quota.
    pub async fn recharge_workspace(
        &self,
        workspace_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> QuotaResult<()> {
        let store = self.store.lock().await;
        let key = Self::workspace_key(workspace_id);

        let current = read_int(&**store, &key).await.unwrap_or(0).max(0) as u64;
        let new_amount = (current + tokens).to_string();

        let ttl = if let Some(until) = valid_until {
            let now = chrono::Utc::now();
            let diff = until.signed_duration_since(now);
            diff.num_seconds().max(0) as u64
        } else {
            Self::seconds_until_midnight()
        };

        if ttl > 0 {
            store
                .set_ex(&key, &new_amount, ttl)
                .await
                .map_err(store_error)?;
        } else {
            store.set(&key, &new_amount).await.map_err(store_error)?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl cog_core::WorkspaceQuotaSource for QuotaManager {
    async fn workspace_remaining(&self, workspace_id: &str) -> u64 {
        self.get_workspace_remaining(workspace_id).await
    }
}

/// Lightweight quota checker for use in middleware and other contexts.
#[derive(Debug, Clone)]
pub struct QuotaChecker {
    manager: QuotaManager,
}

impl QuotaChecker {
    pub fn new(manager: QuotaManager) -> Self {
        Self { manager }
    }

    pub async fn check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult {
        self.manager
            .pre_check(user_id, workspace_id, estimated_tokens)
            .await
    }
}

#[async_trait::async_trait]
impl cog_core::QuotaManager for QuotaManager {
    async fn pre_check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult {
        self.pre_check(user_id, workspace_id, estimated_tokens)
            .await
    }

    async fn finalize(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
        actual_tokens: u64,
    ) -> cog_core::SFResult<()> {
        self.finalize(user_id, workspace_id, estimated_tokens, actual_tokens)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn get_remaining(&self, user_id: &str) -> u64 {
        self.get_remaining(user_id).await
    }

    async fn get_workspace_remaining(&self, workspace_id: &str) -> u64 {
        self.get_workspace_remaining(workspace_id).await
    }

    async fn get_used_today(&self, user_id: &str) -> u64 {
        self.get_used_today(user_id).await
    }

    async fn get_workspace_used_today(&self, workspace_id: &str) -> u64 {
        self.get_workspace_used_today(workspace_id).await
    }

    async fn get_request_count_today(&self, user_id: &str) -> u64 {
        self.get_request_count_today(user_id).await
    }

    async fn get_user_summary(&self, user_id: &str) -> QuotaSummary {
        self.get_user_summary(user_id).await
    }

    async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary {
        self.get_workspace_summary(workspace_id).await
    }

    async fn recharge(
        &self,
        user_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> cog_core::SFResult<()> {
        self.recharge(user_id, tokens, valid_until)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }

    async fn recharge_workspace(
        &self,
        workspace_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> cog_core::SFResult<()> {
        self.recharge_workspace(workspace_id, tokens, valid_until)
            .await
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }
}
