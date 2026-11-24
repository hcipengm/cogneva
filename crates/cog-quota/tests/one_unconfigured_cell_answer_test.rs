//! 「没人配过的格子读到哪一对」只有一处答案：**没有上限**，而且没有数可抬。
//!
//! 合同里原先还带一个 `QuotaLimits::default()`（80k/100k），而没有读数的格子在
//! 充值时会从那一对起步；检查面用的却是层级库声明的那一对（`50_000` 的日上限）。
//! 同一格的两条路径差了一倍，且没有任何人量过其中哪一个。两个数后来都删了：没被
//! 声明过的格子读到 `QuotaLimits::unlimited()`，而充值拒绝在没有声明的格子上动
//! 手——抬一个没人声明过的上限，起点只能是一个凭空写下的数。
//!
//! 这个替身把层级库的默认答案写成一对可辨认的数：充值若还敢落回它，写下去的就
//! 会是 `DECLARED + 10`，这条读得出来。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cog_core::{
    HierarchyDecision, HierarchyManager, QuotaContext, QuotaLimits, QuotaScope, QuotaManager as _,
    SFError, SFResult, UsageHistoryEntry,
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
            // 没有格子 = 这一格没有读数。
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
async fn a_cell_nobody_declared_cannot_be_raised_from_a_default() {
    let hierarchy = NoReading::new();
    let manager = QuotaManager::new(hierarchy.clone());

    let refused = manager.recharge("someone", 10, None).await;

    let error = refused.expect_err("没人声明过上限的格子没有数可抬");
    assert!(
        error.to_string().contains("还没有任何人声明过上限"),
        "拒的时候要说清缺的是哪一步：{error}"
    );
    assert!(
        hierarchy.written.lock().unwrap().is_none(),
        "被拒的充值不许留下任何一对上限：落回默认值写下去，就等于替属主声明了一个没人量过的数"
    );

    // 同一条拒绝穿过接口层（网关拿到的就是这一层的返回值）时仍要是「这份请求
    // 不成立」，而不是「服务端故障」：折成 `Internal` 的话网关只能答 500，
    // 调用方读不到缺的是「先声明一对上限」这一步。
    let through_the_trait =
        <QuotaManager as cog_core::QuotaManager>::recharge(&manager, "someone", 10, None).await;
    assert!(
        matches!(through_the_trait, Err(SFError::Validation(_))),
        "调用方错误到接口层要仍是调用方错误：{through_the_trait:?}"
    );
}
