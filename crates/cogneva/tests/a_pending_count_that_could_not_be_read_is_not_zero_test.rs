//! A pending count that could not be read is not reported as zero.
//!
//! `GET /api/v1/admin/contribution-status` feeds the settings screen's
//! "待提交变更 N" 一格，也就是所有者决定要不要点头的那一格里唯一的数字。队列
//! 读不出来时它答 `0`：屏幕于是说「没有等你点头的变更」，而恰恰是那一拍，
//! 待批的变更最不该被当成不存在。没有接控制面（集成未启用）是同一句话——
//! 这一格没有读数，不是读到了一个零。
//!
//! 同一个答复里 `gitee_oauth_available` 早就按三态给出（`null` 是「没问出
//! 答复」），这里把计数也放回同一条口径上：`null` 表示问不到，`0` 只表示问到
//! 了且队列是空的。

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{ContributionControl, ContributionPolicy, GeneratedChange, SFError, SFResult};

use crate::router_state;

/// A control whose queue cannot be read.
struct RefusesPending;

/// A control with a readable, non-empty queue.
struct HoldsPending(usize);

fn refused() -> SFError {
    SFError::Database("the control plane did not answer".into())
}

#[async_trait::async_trait]
impl ContributionControl for RefusesPending {
    fn policy(&self) -> ContributionPolicy {
        ContributionPolicy::default()
    }

    fn set_policy(&self, _policy: ContributionPolicy) {
        unimplemented!()
    }

    async fn pending(&self) -> SFResult<Vec<GeneratedChange>> {
        Err(refused())
    }

    async fn flush_pending(&self, _change_id: Option<&str>) -> SFResult<usize> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl ContributionControl for HoldsPending {
    fn policy(&self) -> ContributionPolicy {
        ContributionPolicy::default()
    }

    fn set_policy(&self, _policy: ContributionPolicy) {
        unimplemented!()
    }

    async fn pending(&self) -> SFResult<Vec<GeneratedChange>> {
        Ok((0..self.0)
            .map(|i| GeneratedChange {
                change_id: format!("change-{i}"),
                goal: "a staged change".into(),
                ..GeneratedChange::default()
            })
            .collect())
    }

    async fn flush_pending(&self, _change_id: Option<&str>) -> SFResult<usize> {
        unimplemented!()
    }
}

async fn status_body(state: Arc<cog_gateway::GatewayState>, token: &str) -> (StatusCode, String) {
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/admin/contribution-status")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// 问不到队列：这一格是「读不到」，不是 0。
#[tokio::test]
async fn a_pending_count_that_could_not_be_read_is_not_zero() {
    let state = router_state::with_contribution_control(Arc::new(RefusesPending));
    let token = router_state::admin_bearer(&state, "contribution-probe").await;
    let (status, body) = status_body(state, &token).await;

    assert_eq!(status, StatusCode::OK, "状态屏本身照常答复：{body}");
    assert!(
        body.contains("\"pending_count\":null"),
        "队列读不出来时这一格给了一个数，读者会把它当成读到了：{body}"
    );
    assert!(
        !body.contains("\"pending_count\":0"),
        "读失败与空队列在这一行上是同一句话：{body}"
    );
}

/// 控制面没接：同一句话——这一格没有读数。
#[tokio::test]
async fn a_status_screen_without_a_control_has_no_pending_reading() {
    let state = router_state::stateless();
    let token = router_state::admin_bearer(&state, "contribution-probe").await;
    let (status, body) = status_body(state, &token).await;

    assert_eq!(status, StatusCode::OK, "状态屏本身照常答复：{body}");
    assert!(
        body.contains("\"pending_count\":null"),
        "没有控制面时这一格答 0，读者读到的是一条从不存在的读数：{body}"
    );
}

/// 问到了队列：数照常给，三态不是把「有读数」也一起藏掉。
#[tokio::test]
async fn a_readable_queue_still_reports_its_count() {
    let state = router_state::with_contribution_control(Arc::new(HoldsPending(2)));
    let token = router_state::admin_bearer(&state, "contribution-probe").await;
    let (status, body) = status_body(state, &token).await;

    assert_eq!(status, StatusCode::OK, "状态屏本身照常答复：{body}");
    assert!(
        body.contains("\"pending_count\":2"),
        "问到了两份待回流变更，答复里却没有这个数：{body}"
    );
}
