//! 「没人配过的格子读到哪一对」只有一处答案：层级库自己那一对。
//!
//! 合同里原先还带一个 `QuotaLimits::default()`（80k/100k），而没有读数的格子在
//! 充值时会从那一对起步；检查面用的却是层级库声明的那一对（`from_hard(
//! PLATFORM_DAILY_TOKENS, …)`，40k/50k）。同一格的两条路径差了一倍，且没有任何人
//! 量过其中哪一个。这个替身把层级库的答案写成一对可辨认的数，充值后写下去的必须
//! 正好是它加上额度——写成合同里的那一对，这条就红。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cog_core::{
    HierarchyDecision, HierarchyManager, QuotaContext, QuotaLimits, QuotaScope, SFResult,
    UsageHistoryEntry,
};
use cog_quota::QuotaManager;

/// 一对从没在任何地方量过、只为了认得出来而选的数。
const DECLARED: QuotaLimits = QuotaLimits {
    soft_limit: 40,
    hard_limit: 50,
};

/// 一个永远交出「没有读数」的层级库，但记得有人写下来的那一对。
struct NoReading {
    written: Mutex<Option<QuotaLimits>>,
}

impl NoReading {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            written: Mutex::new(None),
        })
    }
}

#[async_trait]
impl HierarchyManager for NoReading {
    async fn check(&self, _ctx: &QuotaContext, _tokens: u64) -> HierarchyDecision {
        HierarchyDecision {
            allowed: true,
            warnings: Vec::new(),
            blocked_by: Vec::new(),
            // 没有格子 = 这一格没有读数。充值面要落回的那一对只能来自下面这一项。
            scopes: Vec::new(),
        }
    }

    async fn set_used(&self, _scope: QuotaScope, _target_id: &str, _tokens: u64) -> SFResult<()> {
        unimplemented!("这一读不问落账")
    }

    async fn history(
        &self,
        _scope: QuotaScope,
        _target_id: &str,
        _days: u32,
    ) -> SFResult<Vec<UsageHistoryEntry>> {
        unimplemented!("这一读不问历史")
    }

    async fn limits(&self, _scope: QuotaScope, _target_id: &str) -> SFResult<Option<QuotaLimits>> {
        // 没人配过。
        Ok(None)
    }

    fn default_limits(&self) -> QuotaLimits {
        DECLARED
    }

    async fn set_limits(
        &self,
        _scope: QuotaScope,
        _target_id: &str,
        limits: QuotaLimits,
    ) -> SFResult<()> {
        *self.written.lock().unwrap() = Some(limits);
        Ok(())
    }

    async fn clear_limits(&self, _scope: QuotaScope, _target_id: &str) -> SFResult<()> {
        unimplemented!("这一读不问撤掉")
    }
}

#[tokio::test]
async fn a_cell_with_no_reading_is_raised_from_the_hierarchy_default() {
    let hierarchy = NoReading::new();
    let manager = QuotaManager::new(hierarchy.clone());

    manager
        .recharge("someone", 10, None)
        .await
        .expect("充值只碰写入面");

    let written = hierarchy
        .written
        .lock()
        .unwrap()
        .expect("充值要把抬过的上限写下去");
    assert_eq!(
        written,
        QuotaLimits::new(DECLARED.soft_limit + 10, DECLARED.hard_limit + 10),
        "没有读数的格子从层级库声明的那一对起步，不是从合同里另写的一对"
    );
}
