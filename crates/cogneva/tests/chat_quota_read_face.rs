//! 对话里的配额格子：带回来的必须是执法用的那一份判定。
//!
//! E28 说的是「配额余量可见，超限被拒并说明原因」。执法那一半已经在代理路径上
//! （402 + 结构化正文），这一半补的是**看得到**：一段对话的标题行上要能读出
//! 「今天已经花了多少、上限多少、还剩多少」。
//!
//! 判据只有一条原则，三种读法各自对应它的一种走样：
//!
//! 1. 有人配过上限：余量 = 声明的硬上限 − 当天投影花销，与执法面读同一格；
//! 2. 没人配过：读成「未配置」——`effective` 是 `null`，而**花销照报**。把它画成
//!    「剩余无限」是把「谁都没给这一格设过界」说成「这一格不需要界」，而执法面恰
//!    恰是「没配过就不拒」，两句话意思相反；
//! 3. 这个进程没有配额层级：503 且 `readable: false`。读不到不许冒充「没花」，
//!    也不许冒充「未配置」——那会让一个接错组件的部署永远看起来一切正常。

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{HierarchyManager as _, QuotaLimits, QuotaScope};
use cog_gateway::{create_router, GatewayState};
use cog_quota::HierarchyManager;

use crate::router_state;

/// 一份完全在内存里的层级：默认那一对上限与放上界无关，因为下面每一条都显式
/// 声明（或不声明）自己那一格。
fn hierarchy() -> (Arc<HierarchyManager>, Arc<crate::common::kv::MemoryStore>) {
    let store = Arc::new(crate::common::kv::MemoryStore::default());
    let hierarchy = Arc::new(HierarchyManager::new(
        store.clone(),
        QuotaLimits::new(80_000, 100_000),
    ));
    (hierarchy, store)
}

async fn read_quota(state: Arc<GatewayState>, token: &str) -> (StatusCode, serde_json::Value) {
    let response = create_router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/chat/quota")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("路由器接这一发");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

/// 发一个令牌并把它的 `sub` 读回来：读面按这个人查格子，套件要写进同一格。
///
/// 两样一起返回、只用这一份：再 mint 一次是另一个人（每次签发一个新 id），
/// 写进甲的格子、读乙的格子，两边都「对」，用例却什么也没证明。
async fn caller(state: &Arc<GatewayState>, name: &str) -> (String, String) {
    let token = router_state::admin_bearer(state, name).await;
    let sub = state
        .jwt_manager
        .verify_token(&token)
        .await
        .expect("这个令牌由这一进程签发")
        .sub;
    (token, sub)
}

#[tokio::test]
async fn the_remaining_a_conversation_reads_is_the_ceiling_the_proxy_refuses_on() {
    let (hierarchy, _store) = hierarchy();
    let state = router_state::with_quota_stores(
        Arc::new(router_state::UnaskedLedger),
        Some(hierarchy.clone()),
    );
    let (token, owner) = caller(&state, "chat-quota-ceiling").await;
    hierarchy
        .set_limits(QuotaScope::User, &owner, QuotaLimits::new(800, 1_000))
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::User, &owner, 400)
        .await
        .unwrap();

    let (status, body) = read_quota(state, &token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["readable"], true, "{body}");
    let effective = &body["effective"];
    assert_eq!(effective["scope"], "user", "{body}");
    assert_eq!(effective["target"], owner, "{body}");
    assert_eq!(effective["hard_limit"], 1_000, "{body}");
    assert_eq!(effective["used_today"], 400, "{body}");
    assert_eq!(
        effective["remaining"], 600,
        "余量只能是「声明的上限 - 当天投影花销」，与执法面读的同一格：{body}"
    );
    assert_eq!(body["used_today"], 400, "{body}");
    assert!(
        body["resets_at_unix"].as_i64().unwrap_or(0) > chrono::Utc::now().timestamp(),
        "恢复时刻要在未来，否则界面上那一句「什么时候回来」是假的：{body}"
    );
}

/// 越线的那一格比最窄的已声明格更宽时，报的是**会拒人的那一格**。
///
/// 两个数都是执法面自己算的：最窄的已声明格给的是一个「还剩多少」，而真正
/// 会让下一轮发不出去的是已经越线的那个更宽的格。只报前者，头行写着「剩
/// 80k」，下一轮照样被平台级拒掉——读者看到的两个答案互相矛盾，而且他会以为
/// 是自己的额度还没用完。
#[tokio::test]
async fn the_cell_a_turn_will_hit_outranks_the_narrowest_one_with_a_ceiling() {
    let (hierarchy, _store) = hierarchy();
    let state = router_state::with_quota_stores(
        Arc::new(router_state::UnaskedLedger),
        Some(hierarchy.clone()),
    );
    let (token, owner) = caller(&state, "chat-quota-wider-block").await;

    // 平台那一格的名字由网关自己定，先从读面上问出来，不在这里抄一份。
    let (_, before) = read_quota(state.clone(), &token).await;
    let global_target = before["scopes"]
        .as_array()
        .expect("判定会列出每一级")
        .iter()
        .find(|cell| cell["scope"] == "global")
        .and_then(|cell| cell["target_id"].as_str())
        .expect("全局那一级总在判定里")
        .to_string();

    // 人这一级：声明的上限还远没到。
    hierarchy
        .set_limits(QuotaScope::User, &owner, QuotaLimits::new(180_000, 200_000))
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::User, &owner, 120_000)
        .await
        .unwrap();
    // 平台那一级：已经越线，下一轮就是它拒。
    hierarchy
        .set_limits(
            QuotaScope::Global,
            &global_target,
            QuotaLimits::new(80_000, 100_000),
        )
        .await
        .unwrap();
    hierarchy
        .set_used(QuotaScope::Global, &global_target, 150_000)
        .await
        .unwrap();

    let (status, body) = read_quota(state, &token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let effective = &body["effective"];
    assert_eq!(
        effective["scope"], "global",
        "越线的那一格才是这一轮会撞上的：{body}"
    );
    assert_eq!(effective["hard_limit"], 100_000, "{body}");
    assert_eq!(
        effective["blocking"], true,
        "报出来的格子要带着「已经越线」这一格，否则头行看起来还有余量：{body}"
    );
}

#[tokio::test]
async fn a_cell_nobody_set_a_ceiling_for_reads_as_unset_not_as_unlimited() {
    let (hierarchy, _store) = hierarchy();
    let state = router_state::with_quota_stores(
        Arc::new(router_state::UnaskedLedger),
        Some(hierarchy.clone()),
    );
    let (token, owner) = caller(&state, "chat-quota-unset").await;
    // 花销照投影，上限一格都没人写。这正是「今天在花，但没人设过界」的形状。
    state
        .hierarchy_manager
        .as_ref()
        .unwrap()
        .set_used(QuotaScope::User, &owner, 1234)
        .await
        .unwrap();

    let (status, body) = read_quota(state, &token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["readable"], true, "{body}");
    assert!(
        body["effective"].is_null(),
        "没人配过上限时不许报一个余量：那会把「未配置」读成「还剩很多」甚至「无限」：{body}"
    );
    assert_eq!(
        body["used_today"], 1234,
        "没配上限不等于没花：花销是账本的投影，与有没有界无关：{body}"
    );
}

#[tokio::test]
async fn a_state_without_a_hierarchy_says_it_could_not_read_instead_of_zero() {
    let state = router_state::stateless();
    let token = router_state::admin_bearer(&state, "chat-quota-unreadable").await;

    let (status, body) = read_quota(state, &token).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "读不到要给一个读不到的回答，而不是 200 加一串零：{body}"
    );
    assert_eq!(body["readable"], false, "{body}");
    assert_eq!(body["reason"], "no_hierarchy_manager", "{body}");
    assert!(
        body.get("used_today").is_none(),
        "读不到的时候不许给出一个「今天花了 0」：{body}"
    );
}
