//! 额度读数只有一本账：管理面读到的数与执法判的是同一格。
//!
//! 单级额度曾经自己记一本账：每人一个「还剩多少」的格子，请求路径上扣、充值接口上加。
//! 那个扣减的中间件因为「按请求体长度扣 token」被删掉之后，这些格子再没有写入方，而
//! 四个管理面路由与 supervisor 的按工作区闸门还在读它——读到的永远是引导时那个默认值，
//! 与「这个人今天一分没花」在读数上无法区分。一个已经花完当天额度的人与一个没花的人
//! 看到同一个数。
//!
//! 现在这条读数从额度层级里算：当天花销是账本投影到格子上的绝对值，上限是声明的那一对
//! （没人声明时是层级自己的默认值）。这个文件钉两件事：
//!
//! 1. 行为：剩余 = 上限 - 当天投影花销；旁边放一个旧账的余额格子不影响任何读数。
//! 2. 形状：生产源码里不再出现那几族旧键，第二本账要回来就得先让这条红。

use std::sync::Arc;

use cog_core::{HierarchyManager as _, KeyValueStore, QuotaLimits, QuotaScope};
use cog_quota::{HierarchyManager, QuotaManager};

fn manager() -> (
    QuotaManager,
    Arc<HierarchyManager>,
    Arc<crate::common::kv::MemoryStore>,
) {
    let store = Arc::new(crate::common::kv::MemoryStore::default());
    let hierarchy = Arc::new(HierarchyManager::new(
        store.clone(),
        QuotaLimits::new(80_000, 100_000),
    ));
    (QuotaManager::new(hierarchy.clone()), hierarchy, store)
}

#[tokio::test]
async fn the_remaining_a_route_reports_is_the_ceiling_minus_the_projected_spend() {
    let (quota, hierarchy, _store) = manager();
    hierarchy
        .set_limits(QuotaScope::User, "u1", QuotaLimits::new(800, 1_000))
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::User, "u1", 400)
        .await
        .unwrap();

    assert_eq!(quota.get_used_today("u1").await, 400);
    assert_eq!(
        quota.get_remaining("u1").await,
        600,
        "剩余只能是「声明的上限 - 投影的当天花销」"
    );

    let summary = quota.get_user_summary("u1").await;
    assert_eq!(summary.total_quota, 1_000, "上限就是声明的那一对里的硬上限");
    assert_eq!(summary.used_today, 400);
    assert_eq!(summary.remaining, 600);
}

#[tokio::test]
async fn a_legacy_balance_cell_beside_the_projection_changes_nothing() {
    let (quota, hierarchy, store) = manager();
    hierarchy
        .set_limits(QuotaScope::User, "u1", QuotaLimits::new(800, 1_000))
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::User, "u1", 400)
        .await
        .unwrap();

    // 旧账留下的键还躺在同一个存储里（升级时不会有人去删它们）。它们不许再影响任何读数：
    // 一旦有人重新读回去，这条就红，而读到的数正是「花了 400 却报 0」。
    store.set("quota:remaining:u1", "0").await.unwrap();
    store.set("quota:used:u1", "999999").await.unwrap();

    assert_eq!(quota.get_remaining("u1").await, 600, "旧余额格子不是读数");
    assert_eq!(quota.get_used_today("u1").await, 400, "旧计数格子不是读数");
}

#[tokio::test]
async fn the_check_judges_against_the_projected_cell_without_spending_it() {
    let (quota, hierarchy, _store) = manager();
    hierarchy
        .set_limits(QuotaScope::User, "u1", QuotaLimits::new(800, 1_000))
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::User, "u1", 400)
        .await
        .unwrap();

    let fits = quota.pre_check("u1", None, 100).await;
    assert!(fits.allowed, "400 + 100 没有越过 1000");
    assert_eq!(fits.remaining, 600);

    let does_not_fit = quota.pre_check("u1", None, 700).await;
    assert!(!does_not_fit.allowed, "400 + 700 越过 1000，要拒");
    assert_eq!(
        does_not_fit.remaining, 600,
        "被拒时报的仍是「还剩多少」，不是「差多少」"
    );

    assert_eq!(
        quota.get_used_today("u1").await,
        400,
        "问一次不许把估值扣进当天花销：花销只有账本落的那一笔"
    );
}

#[tokio::test]
async fn a_top_up_raises_the_ceiling_it_will_be_judged_against() {
    let (quota, hierarchy, store) = manager();
    hierarchy
        .set_limits(QuotaScope::User, "u1", QuotaLimits::new(800, 1_000))
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::User, "u1", 400)
        .await
        .unwrap();

    quota.recharge("u1", 250, None).await.unwrap();

    assert_eq!(
        quota.get_remaining("u1").await,
        850,
        "充值是抬高上限，判定与读数都跟着抬"
    );
    assert!(
        store.get("quota:user:limits:u1").await.unwrap().is_some(),
        "抬上去的上限要落在执法读的那一格"
    );
    assert!(
        store.get("quota:remaining:u1").await.unwrap().is_none(),
        "充值不许顺手记一笔第二本账"
    );

    let expiry = chrono::Utc::now() + chrono::Duration::days(2);
    assert!(
        quota.recharge("u1", 250, Some(expiry)).await.is_err(),
        "日上限在 UTC 零点归零，带过期的充值不是一个能表示的承诺"
    );
}

#[tokio::test]
async fn a_workspace_reads_the_same_way_as_a_person() {
    let (quota, hierarchy, _store) = manager();
    hierarchy
        .set_limits(QuotaScope::Workspace, "ws1", QuotaLimits::new(400, 500))
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::Workspace, "ws1", 100)
        .await
        .unwrap();

    assert_eq!(quota.get_workspace_used_today("ws1").await, 100);
    assert_eq!(quota.get_workspace_remaining("ws1").await, 400);
    assert_eq!(
        quota.get_workspace_summary("ws1").await.total_quota,
        500,
        "工作区读的是工作区那一格，不是某个人的格子"
    );
}

/// 暂停闸门只被「有人声明过的上限」触发。
///
/// 层级管理器对每个格子都有一份默认上限，那是「读起来是多少」，不是谁做的决定。
/// 拿它回答闸门的问题，等于在平台自己的花销越过这个默认数的那一天把整个调度器
/// 停下——而那不是一个任何人按下的开关。所以没人声明过的工作区答「没有约束」，
/// 声明过之后答的才是「上限减账上的花销」。
#[tokio::test]
async fn a_workspace_nobody_declared_a_ceiling_for_never_pauses_the_platform() {
    use cog_core::WorkspaceQuotaSource as _;

    let (quota, hierarchy, _store) = manager();
    hierarchy
        .set_used(QuotaScope::Workspace, "ws1", 900_000)
        .await
        .unwrap();

    assert_eq!(
        quota.workspace_remaining("ws1").await,
        u64::MAX,
        "没人声明过上限的工作区，闸门这一侧没有要拦的东西——花得再多也不许\
         由默认值来停平台"
    );
    assert_eq!(
        quota.get_workspace_summary("ws1").await.used_today,
        900_000,
        "读数面照旧报账上的数：不拦不等于看不见"
    );

    hierarchy
        .set_limits(QuotaScope::Workspace, "ws1", QuotaLimits::new(400, 500))
        .await
        .unwrap();
    assert_eq!(
        quota.workspace_remaining("ws1").await,
        0,
        "声明过上限之后，闸门读的就是上限减账上的花销"
    );
}

/// 已经退休的那几族键，不允许再从生产源码里长回来。
///
/// 判据按出现形式判：只看会随产物跑的 Rust 源（`crates/*/src` 与 crate 根的
/// `build.rs`），注释先被滤掉——文档里提到这些键的名字是解释，不是读它。测试文件不算：
/// 本文件的退路断言正是在造这些键。
#[test]
fn no_production_source_builds_the_retired_quota_keys() {
    let root = crate::common::root::repo_root();
    let mut offenders = Vec::new();
    for path in crate::common::rust_production::production_sources(&root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = crate::common::rust_production::relative_name(&root, &path);
        for line in crate::common::rust_production::production_lines(&text) {
            if RETIRED_KEYS.iter().any(|key| line.contains(key)) {
                offenders.push(format!("{file}: {}", line.trim()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "这些行还在构造已经退休的额度键（第二本账），管理面与执法会再次读两个数：{offenders:#?}"
    );
}

/// 退休的键前缀。带上引号与冒号，是因为它们只在构造键的字符串里出现；裸子串会把
/// 「讲这件事的注释」也判成违规。
const RETIRED_KEYS: [&str; 4] = [
    "\"quota:remaining:",
    "\"quota:ws:remaining:",
    "\"quota:used:",
    "\"quota:requests:",
];
