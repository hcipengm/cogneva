//! Integration tests for the 5-level quota hierarchy.
//!
//! The manager speaks the core key-value contract, so these tests run against
//! an in-memory implementation and need no Redis: the same suite runs on a
//! developer machine, in CI and in the evolution sandbox instead of skipping
//! wherever no server happens to be up. Each test builds its own store, so
//! there is nothing to clean up between them.

use cog_core::{KeyValueStore, QuotaContext, QuotaLimits, QuotaScope};
use cog_quota::HierarchyManager;
use std::sync::{Arc, Mutex};

/// An in-memory [`KeyValueStore`] for the hierarchy suite.
///
/// Values are stored without expiry and the set and hash operations are not
/// implemented: the hierarchy manager keeps counters and JSON limit documents,
/// so those are the shapes this provides. Expiry is the driver's behaviour and
/// its tests belong with the driver.
#[derive(Default)]
struct FakeStore {
    strings: Mutex<std::collections::HashMap<String, String>>,
}

#[async_trait::async_trait]
impl KeyValueStore for FakeStore {
    async fn get(&self, key: &str) -> cog_core::SFResult<Option<String>> {
        Ok(self.strings.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> cog_core::SFResult<()> {
        self.strings
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, _ttl_secs: u64) -> cog_core::SFResult<()> {
        self.set(key, value).await
    }

    async fn del(&self, key: &str) -> cog_core::SFResult<()> {
        self.strings.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> cog_core::SFResult<i64> {
        Ok(-1)
    }

    async fn incr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        let mut strings = self.strings.lock().unwrap();
        let next = strings
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
            + by;
        strings.insert(key.to_string(), next.to_string());
        Ok(next)
    }

    async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> cog_core::SFResult<i64> {
        unreachable!("the hierarchy manager expires counters separately")
    }

    async fn decr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        self.incr(key, -by).await
    }

    async fn sadd(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use sets")
    }

    async fn srem(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use sets")
    }

    async fn smembers(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("the hierarchy manager does not use sets")
    }

    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hget(&self, _key: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hdel(&self, _key: &str, _field: &str) -> cog_core::SFResult<()> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hvals(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("the hierarchy manager does not use hash fields")
    }

    async fn hgetall(&self, _key: &str) -> cog_core::SFResult<Vec<(String, String)>> {
        unreachable!("the session store does not use whole-hash reads")
    }

    async fn hlen(&self, _key: &str) -> cog_core::SFResult<u64> {
        unreachable!("the hierarchy manager does not use hash fields")
    }
}

/// A hierarchy manager over a fresh in-memory store. Default limits are
/// soft 80 / hard 100, which the cases below are written against.
fn fresh_manager() -> HierarchyManager {
    HierarchyManager::new(Arc::new(FakeStore::default()), QuotaLimits::new(80, 100))
}

#[tokio::test]
async fn test_quota_limits_clamps_soft_above_hard() {
    let limits = QuotaLimits::new(200, 100);
    assert_eq!(limits.soft_limit, 100);
    assert_eq!(limits.hard_limit, 100);
}

#[tokio::test]
async fn test_quota_limits_from_hard_default_ratio() {
    let limits = QuotaLimits::from_hard(1000, 0.8);
    assert_eq!(limits.soft_limit, 800);
    assert_eq!(limits.hard_limit, 1000);
}

#[tokio::test]
async fn test_cascade_order_is_user_to_global() {
    let order = QuotaScope::cascade_order();
    assert_eq!(order[0], QuotaScope::User);
    assert_eq!(order[1], QuotaScope::Workspace);
    assert_eq!(order[2], QuotaScope::Team);
    assert_eq!(order[3], QuotaScope::Organization);
    assert_eq!(order[4], QuotaScope::Global);
}

#[tokio::test]
async fn test_quota_context_target_lookup() {
    let ctx = QuotaContext {
        user_id: Some("u1".into()),
        workspace_id: None,
        team_id: Some("t1".into()),
        organization_id: None,
        global_id: Some("global".into()),
    };
    assert_eq!(ctx.target(QuotaScope::User), Some("u1"));
    assert_eq!(ctx.target(QuotaScope::Workspace), None);
    assert_eq!(ctx.target(QuotaScope::Team), Some("t1"));
    assert_eq!(ctx.target(QuotaScope::Organization), None);
    assert_eq!(ctx.target(QuotaScope::Global), Some("global"));
}

#[tokio::test]
async fn test_check_allows_when_below_soft_limit() {
    let prefix = "ti1";
    let mgr = fresh_manager();
    let ctx = QuotaContext {
        user_id: Some(format!("{}:user", prefix)),
        workspace_id: Some(format!("{}:ws", prefix)),
        ..Default::default()
    };
    let decision = mgr.check(&ctx, 10).await;
    assert!(decision.allowed);
    assert!(decision.warnings.is_empty());
    assert!(decision.blocked_by.is_empty());
    assert_eq!(decision.scopes.len(), 2);
}

#[tokio::test]
async fn test_check_warns_at_soft_limit() {
    let prefix = "ti2";
    let mgr = fresh_manager();
    let user = format!("{}:user", prefix);
    let ctx = QuotaContext {
        user_id: Some(user.clone()),
        ..Default::default()
    };
    // Default soft=80, hard=100. The cell stands at 80 → the next 5 projects
    // 85 ≥ 80 but < 100, so we should warn but not block.
    mgr.project_used(QuotaScope::User, &user, 80)
        .await
        .expect("project ok");
    let decision = mgr.check(&ctx, 5).await;
    assert!(decision.allowed);
    assert_eq!(decision.warnings.len(), 1);
    assert!(decision.blocked_by.is_empty());
}

#[tokio::test]
async fn test_check_blocks_at_hard_limit() {
    let prefix = "ti3";
    let mgr = fresh_manager();
    let user = format!("{}:user", prefix);
    let ctx = QuotaContext {
        user_id: Some(user.clone()),
        ..Default::default()
    };
    mgr.project_used(QuotaScope::User, &user, 95)
        .await
        .expect("project ok");
    let decision = mgr.check(&ctx, 10).await;
    assert!(!decision.allowed);
    assert_eq!(decision.blocked_by.len(), 1);
    assert_eq!(decision.blocked_by[0].scope, QuotaScope::User);
}

#[tokio::test]
async fn test_set_limits_overrides_default() {
    let prefix = "ti4";
    let mgr = fresh_manager();
    let team = format!("{}:team", prefix);
    mgr.set_limits(QuotaScope::Team, &team, QuotaLimits::new(40, 50))
        .await
        .expect("set ok");
    let limits = mgr.get_limits(QuotaScope::Team, &team).await;
    assert_eq!(limits.soft_limit, 40);
    assert_eq!(limits.hard_limit, 50);
}

#[tokio::test]
async fn test_full_hierarchy_blocks_at_team() {
    let prefix = "ti5";
    let mgr = fresh_manager();
    let team = format!("{}:team", prefix);
    // Force the team to a tiny limit so it dominates.
    mgr.set_limits(QuotaScope::Team, &team, QuotaLimits::new(5, 10))
        .await
        .expect("set ok");
    let ctx = QuotaContext {
        user_id: Some(format!("{}:u", prefix)),
        workspace_id: Some(format!("{}:w", prefix)),
        team_id: Some(team.clone()),
        organization_id: Some(format!("{}:o", prefix)),
        global_id: Some(format!("{}:g", prefix)),
    };
    let d = mgr.check(&ctx, 12).await;
    assert!(!d.allowed);
    assert!(d.blocked_by.iter().any(|s| s.scope == QuotaScope::Team));
}

/// 投影写的是**当天累计的绝对值**，不是一笔增量：跑两遍同一份账，格子还是那个数。
///
/// 这一格是账本的投影，所以重算幂等是它成立的全部理由——丢了键，下一拍写回来；
/// 账上有了更正，下一拍把格子往回改。累加的写法两条都做不到：跑两遍翻一倍，
/// 键丢了也永远抹平不了。
#[tokio::test]
async fn projecting_the_same_ledger_twice_leaves_the_cell_at_that_value() {
    let mgr = fresh_manager();
    let user = "ti6:user";
    let ctx = QuotaContext {
        user_id: Some(user.to_string()),
        ..Default::default()
    };
    mgr.project_used(QuotaScope::User, user, 35)
        .await
        .expect("project ok");
    mgr.project_used(QuotaScope::User, user, 35)
        .await
        .expect("project ok");

    let d = mgr.check(&ctx, 0).await;
    assert_eq!(
        d.scopes[0].used_today, 35,
        "投影跑两遍还是 35；65 说明它在一笔一笔地加"
    );
    let history = mgr
        .history(QuotaScope::User, user, 1)
        .await
        .expect("history ok");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].tokens_used, 35);
}

/// 剩余额度是一个**算出来的**数，没有键：存一份剩余就是存第二个数。
///
/// 格子已经被投影接管之后，再存一个 `remaining` 就等于给同一件事留两处记录——
/// 投影覆盖了花销、没人覆盖余量，两处第一次有人手工改或者键被淘汰时就开始分叉，
/// 而分叉之后没有任何读数比得出来。判定读的是 `hard_limit - used`，两样都是当场
/// 就能读到的正本：上限在 limits 键里，花销在投影格子里。
#[tokio::test]
async fn remaining_is_derived_at_the_decision_and_never_stored() {
    let store = Arc::new(FakeStore::default());
    let mgr = HierarchyManager::new(store.clone(), QuotaLimits::new(80, 100));
    let user = "ti8:user";
    let ctx = QuotaContext {
        user_id: Some(user.to_string()),
        ..Default::default()
    };
    mgr.project_used(QuotaScope::User, user, 35)
        .await
        .expect("project ok");

    let d = mgr.check(&ctx, 0).await;
    assert_eq!(d.scopes[0].used_today, 35);
    assert_eq!(
        d.scopes[0].remaining, 65,
        "剩余额度是 hard_limit - used 算出来的，不是另存的一个数"
    );
    assert!(
        store
            .get(&format!("quota:user:remaining:{}", user))
            .await
            .expect("read ok")
            .is_none(),
        "没有任何一处写 quota:<scope>:remaining:<id>：存下来就是第二本账"
    );
}

/// 账上改小之后，格子跟着改小——这是「账本是正本」的可观测后果。
#[tokio::test]
async fn a_corrected_ledger_pulls_the_cell_down_on_the_next_pass() {
    let mgr = fresh_manager();
    let user = "ti7:user";
    let ctx = QuotaContext {
        user_id: Some(user.to_string()),
        ..Default::default()
    };
    mgr.project_used(QuotaScope::User, user, 30)
        .await
        .expect("project ok");
    mgr.project_used(QuotaScope::User, user, 10)
        .await
        .expect("project ok");
    let d = mgr.check(&ctx, 0).await;
    assert_eq!(
        d.scopes[0].used_today, 10,
        "投影写的是绝对值：账说 10，格子就得是 10"
    );
}

/// A blocked decision names the narrowest ceiling that stopped it, with the
/// figures the decision was made from and the wait until the counters reset.
///
/// Both facts are what a request-facing caller cannot derive from the decision
/// itself: the scopes are a list, and the reset is a clock fact rather than a
/// stored one. Naming the first blocking scope (cascade order) is what makes the
/// refusal point at the one allowance its owner can raise.
#[tokio::test]
async fn test_refusal_names_the_blocked_scope_and_the_reset() {
    let prefix = "ti10";
    let mgr = fresh_manager();
    let ctx = QuotaContext {
        user_id: Some(format!("{}:user", prefix)),
        workspace_id: Some(format!("{}:ws", prefix)),
        ..Default::default()
    };
    mgr.project_used(QuotaScope::User, &format!("{}:user", prefix), 95)
        .await
        .expect("project ok");

    let decision = mgr.check(&ctx, 10).await;
    assert!(!decision.allowed);
    // Noon on this process's clock: the wait below is the rest of the day, and
    // which instant that is belongs to the machine, not to the test.
    let noon = cog_quota::day_start_unix(1_600_000_000) + 12 * 3600;
    let refusal = cog_quota::refusal_for(&decision, 10, noon).expect("blocked");

    assert_eq!(
        refusal.scope,
        QuotaScope::User,
        "the narrowest blocked scope"
    );
    assert_eq!(refusal.target_id, format!("{}:user", prefix));
    assert_eq!(refusal.cap_tokens, 100);
    assert_eq!(refusal.spent_tokens, 95);
    assert_eq!(refusal.estimated_tokens, 10);
    assert_eq!(refusal.retry_after_secs, 12 * 3600);
}

/// An allowed decision has no refusal to report: `None`, not a zeroed one.
#[tokio::test]
async fn test_allowed_decision_has_no_refusal() {
    let mgr = fresh_manager();
    let ctx = QuotaContext {
        user_id: Some("ti11:user".into()),
        ..Default::default()
    };
    let decision = mgr.check(&ctx, 10).await;
    assert!(decision.allowed);
    assert_eq!(cog_quota::refusal_for(&decision, 10, 0), None);
}

// ─── 上限的读写（管理面） ───
//
// 这三条判的是契约上的那一对读法，不是 `check` 的答案。`check` 在没有覆盖时报出
// 默认值，所以「按默认值配过」与「没人配过」在它那里逐字同形；管理面要能分开，
// 否则「清除」这个动作无法预测会不会改变行为。

/// 配置过的一格读作 `Some`，没配置的读作 `None`——即使两者生效值相同。
#[tokio::test]
async fn a_configured_cell_reads_as_configured_even_when_it_matches_the_default() {
    let mgr = fresh_manager();
    let unset = "tl1:unset";
    let set_to_default = "tl1:same";

    // 没配过：管理读是 None，检查面照样报出一对默认值。
    assert_eq!(
        cog_core::HierarchyManager::limits(&mgr, QuotaScope::User, unset)
            .await
            .expect("read ok"),
        None,
        "一个从没被配置过的格子没有声明可读"
    );
    let ctx = QuotaContext {
        user_id: Some(unset.to_string()),
        ..Default::default()
    };
    let decision = mgr.check(&ctx, 10).await;
    assert_eq!(decision.scopes[0].limits, QuotaLimits::new(80, 100));

    // 配成与默认完全一样：检查面两个数一模一样，管理读是 Some。
    cog_core::HierarchyManager::set_limits(
        &mgr,
        QuotaScope::User,
        set_to_default,
        QuotaLimits::new(80, 100),
    )
    .await
    .expect("write ok");
    assert_eq!(
        cog_core::HierarchyManager::limits(&mgr, QuotaScope::User, set_to_default)
            .await
            .expect("read ok"),
        Some(QuotaLimits::new(80, 100)),
        "写成默认值也是一次声明，清除它才会改变这一格的来历"
    );
}

/// 清除之后回到默认值，检查面也回到默认值。
#[tokio::test]
async fn clearing_a_limit_puts_the_default_back() {
    let mgr = fresh_manager();
    let user = "tl2:user";
    let ctx = QuotaContext {
        user_id: Some(user.to_string()),
        ..Default::default()
    };
    cog_core::HierarchyManager::set_limits(&mgr, QuotaScope::User, user, QuotaLimits::new(5, 10))
        .await
        .expect("write ok");
    assert!(
        !mgr.check(&ctx, 12).await.allowed,
        "覆盖生效时，一次 12 个 token 的调用应当被这一格拦下"
    );

    cog_core::HierarchyManager::clear_limits(&mgr, QuotaScope::User, user)
        .await
        .expect("clear ok");
    assert_eq!(
        cog_core::HierarchyManager::limits(&mgr, QuotaScope::User, user)
            .await
            .expect("read ok"),
        None
    );
    assert!(
        mgr.check(&ctx, 12).await.allowed,
        "清除之后这一格回到默认值，12 个 token 不该再被拦"
    );
    // 清两次不是错误：重试的请求本来就会走到这里。
    cog_core::HierarchyManager::clear_limits(&mgr, QuotaScope::User, user)
        .await
        .expect("clearing an absent override is not a failure");
}

/// 存坏的限值在管理读上是错误，不是「没有覆盖」；检查面仍然答得出来。
#[tokio::test]
async fn a_stored_limit_that_does_not_parse_is_an_error_not_an_absence() {
    let store = Arc::new(FakeStore::default());
    let mgr = HierarchyManager::new(store.clone(), QuotaLimits::new(80, 100));
    let user = "tl3:user";
    store
        .set("quota:user:limits:tl3:user", "{not a limit document}")
        .await
        .expect("seed ok");

    assert!(
        cog_core::HierarchyManager::limits(&mgr, QuotaScope::User, user)
            .await
            .is_err(),
        "读不出来的声明不能读成「没配过」：前者盘上有一条没人执行的限值，后者没有"
    );

    // 检查面不能因此失败：它没有报错的出口，退回默认值是它此前一直的行为。
    let ctx = QuotaContext {
        user_id: Some(user.to_string()),
        ..Default::default()
    };
    let decision = mgr.check(&ctx, 10).await;
    assert!(decision.allowed);
    assert_eq!(decision.scopes[0].limits, QuotaLimits::new(80, 100));
}

/// 网关按人判额度的两个动作，走的正是这一格：先问「这个人配过没有」，配过才判，
/// 判的依据是记账记出来的那个数。
///
/// 这一条把两步钉在一起：`limits()` 的答案决定判不判（`None` = 没人配过，不许拿
/// 默认上限去拒人），而投影写进格子的花销必须出现在之后的判定里——两处各写
/// 各的键时，读出来永远是「一分没花」。
#[tokio::test]
async fn the_person_axis_answers_only_what_an_operator_declared_and_counts_what_was_spent() {
    let mgr = HierarchyManager::from_store(Arc::new(FakeStore::default()));
    let ctx = QuotaContext {
        user_id: Some("ti9:user".into()),
        ..Default::default()
    };

    assert!(
        cog_core::HierarchyManager::limits(&mgr, QuotaScope::User, "ti9:user")
            .await
            .expect("read ok")
            .is_none(),
        "没人配过的格子必须答「没人配过」，而不是默认那一对——网关拿它决定判不判"
    );

    cog_core::HierarchyManager::set_limits(
        &mgr,
        QuotaScope::User,
        "ti9:user",
        QuotaLimits::new(800, 1000),
    )
    .await
    .expect("set ok");
    let declared = cog_core::HierarchyManager::limits(&mgr, QuotaScope::User, "ti9:user")
        .await
        .expect("read ok")
        .expect("an override was just written");
    assert_eq!(declared.hard_limit, 1000);

    mgr.project_used(QuotaScope::User, "ti9:user", 1200)
        .await
        .expect("project ok");
    let decision = mgr.check(&ctx, 0).await;
    assert!(!decision.allowed, "记进去的花销必须出现在下一次判定里");
    assert_eq!(decision.blocked_by[0].used_today, 1200);
    assert_eq!(decision.blocked_by[0].limits.hard_limit, 1000);
}

/// 平台给未配置格子用的默认上限只有一个定义，两个进程读到的是同一个数。
#[tokio::test]
async fn the_platform_default_ceiling_is_one_definition() {
    let mgr = HierarchyManager::from_store(Arc::new(FakeStore::default()));
    let ctx = QuotaContext {
        user_id: Some("ti10:user".into()),
        ..Default::default()
    };
    let decision = mgr.check(&ctx, 0).await;
    assert_eq!(decision.scopes.len(), 1);
    assert_eq!(
        decision.scopes[0].limits,
        QuotaLimits::from_hard(cog_quota::hierarchy::PLATFORM_DAILY_TOKENS, 0.8)
    );
}
