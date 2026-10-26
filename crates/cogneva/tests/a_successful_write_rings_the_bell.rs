//! 一次成功的写在总线上落一帧，其余一概不落。
//!
//! 编排器发的帧只覆盖智能体与任务。平台其余的面——设置、文件、存储、变更账、治理
//! 名册——只在有人**写**的时候变，而写过去在总线上不留痕迹。于是那些面只能靠定时
//! 重读：把间隔调成十分钟，一次保存之后屏幕一动不动，而服务端早就改完了。这一层
//! 补的就是那一步。
//!
//! 三种情形分别判：
//!
//! * 一次成功的写，帧要带着它落在哪个前缀下发出去——不发，前端订什么都等不到；
//! * 一次读，什么都不发——读了没有改动任何东西，叫醒所有面只是让它们重读同一份字节；
//! * 一次被拒的写（5xx），什么都不发——写没落地，重读只会读到写之前的那一份，
//!   而读者会把它读成「写了但没生效」。
//!
//! 只判这三条是故意的：前缀怎么从路径里取出来另有单测（`resource_domain`），
//! 这里判的是那条判断**真的挂在了请求上**，且只在写成功之后响。

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::Router;
use tower::ServiceExt;

use cog_core::AgentEvent;
use cog_gateway::resource_frames::publish_resource_frame;
use cog_gateway::GatewayState;

mod router_state;

fn state() -> Arc<GatewayState> {
    router_state::stateless()
}

/// 一个只有三条路由的小路由树：成功的写、读、失败的写。挂的层与主路由同一份。
fn app(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route(
            "/api/v1/settings/llm",
            post(|| async { StatusCode::OK }).get(|| async { StatusCode::OK }),
        )
        .route(
            "/api/v1/files/rejected",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .layer(axum::middleware::from_fn_with_state(
            state,
            publish_resource_frame,
        ))
}

/// 下一帧里如果有一条 `ResourceChanged`，返回它的前缀。
async fn next_domain(rx: &mut tokio::sync::broadcast::Receiver<AgentEvent>) -> Option<String> {
    loop {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(AgentEvent::ResourceChanged { domain, .. })) => return Some(domain),
            // 别的变体在这棵小树里不会出现；真出现了也不是这一条要判的事。
            Ok(Ok(_)) => continue,
            Ok(Err(_)) | Err(_) => return None,
        }
    }
}

#[tokio::test]
async fn a_successful_write_names_the_prefix_it_landed_under() {
    let state = state();
    let mut rx = state.event_tx.subscribe();
    let response = app(state)
        .oneshot(
            Request::post("/api/v1/settings/llm")
                .body(Body::empty())
                .expect("请求造不出来"),
        )
        .await
        .expect("路由没答");
    assert_eq!(response.status(), StatusCode::OK);

    assert_eq!(
        next_domain(&mut rx).await.as_deref(),
        Some("settings"),
        "一次成功的写没有在总线上落帧：订了 settings 的那几面永远等不到触发，\
         屏幕什么时候动只取决于它们自己那个定时器"
    );
}

#[tokio::test]
async fn a_read_leaves_the_bus_alone() {
    let state = state();
    let mut rx = state.event_tx.subscribe();
    let response = app(state)
        .oneshot(
            Request::get("/api/v1/settings/llm")
                .body(Body::empty())
                .expect("请求造不出来"),
        )
        .await
        .expect("路由没答");
    assert_eq!(response.status(), StatusCode::OK);

    assert_eq!(
        next_domain(&mut rx).await,
        None,
        "一次读也在总线上落了帧：每一面都会被读请求拖着重读，那是把轮询挪到了服务端"
    );
}

#[tokio::test]
async fn a_rejected_write_rings_nobody() {
    let state = state();
    let mut rx = state.event_tx.subscribe();
    let response = app(state)
        .oneshot(
            Request::post("/api/v1/files/rejected")
                .body(Body::empty())
                .expect("请求造不出来"),
        )
        .await
        .expect("路由没答");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    assert_eq!(
        next_domain(&mut rx).await,
        None,
        "一次被拒的写也落了帧：面重读回来的是写之前的那一份，读者会把它读成\
         「写了但没生效」"
    );
}
