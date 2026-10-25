//! 跑完的评测要留在盘上，读面才有东西可读。
//!
//! 报告读面 `/api/v1/eval/report/{id}` 找的是 `{data_dir}/eval/reports/{id}.json`，
//! 而此前**没有任何一处在写这份文件**：跑一次评测，报告只跟着那一次 HTTP 响应走掉，
//! 目录永远是空的。于是「这个部署从没跑过评测」与「评测跑过但读不回来」在界面上长得
//! 一样，而后者是真的丢了数据。
//!
//! 这一组用例只问路由看得见的事：跑完之后，报告在不在列表里、读不读得回来、第二次跑
//! 会不会把第一次盖掉。把报告送到盘上的那条路径若被删掉，第一条就会红。

mod router_state;

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_gateway::{create_router, GatewayState};

/// 一个只会交回同一份报告的评测服务。算得对不对不是这条路径的事：这里判的是
/// 「跑完之后盘上有没有它」。
struct FixedEval {
    report: serde_json::Value,
}

#[async_trait::async_trait]
impl cog_core::EvalService for FixedEval {
    async fn run_eval(&self, _dataset_path: &str) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.report.clone())
    }

    async fn compare_eval(
        &self,
        _baseline: serde_json::Value,
        _challenger: serde_json::Value,
        _baseline_name: &str,
        _challenger_name: &str,
    ) -> cog_core::SFResult<serde_json::Value> {
        Ok(serde_json::json!({}))
    }

    async fn render_report(&self, report_json: &str, format: &str) -> cog_core::SFResult<String> {
        Ok(format!("rendered {format} of {} bytes", report_json.len()))
    }
}

fn scratch_dir() -> String {
    let dir = std::env::temp_dir().join(format!("cog-eval-route-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("the scratch directory is creatable");
    dir.display().to_string()
}

async fn call(
    state: Arc<GatewayState>,
    method: &str,
    path: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json");
    let request = match body {
        Some(body) => request.body(Body::from(body.to_string())),
        None => request.body(Body::empty()),
    }
    .expect("the request is well formed");

    let response = create_router(state)
        .oneshot(request)
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn a_run_leaves_a_report_that_the_listing_and_the_reader_can_both_find() {
    let data_dir = scratch_dir();
    let state = router_state::with_eval_service(
        Arc::new(FixedEval {
            report: serde_json::json!({
                "dataset_name": "nightly probe",
                "accuracy": 0.91,
            }),
        }),
        data_dir.clone(),
    );
    let token = router_state::admin_bearer(&state, "eval-report-probe").await;

    let (status, _) = call(
        state.clone(),
        "POST",
        "/api/v1/eval/run",
        &token,
        Some(serde_json::json!({"dataset_path": "probe.jsonl"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "评测本身跑通了");

    let (status, listing) = call(state.clone(), "GET", "/api/v1/eval/reports", &token, None).await;
    assert_eq!(status, StatusCode::OK);
    let reports = listing["reports"]
        .as_array()
        .expect("the listing has a reports array");
    assert_eq!(
        reports.len(),
        1,
        "跑过一次就应该有一份报告；空列表说明那一份只跟着响应走掉了：{listing}"
    );
    assert_eq!(reports[0]["dataset_name"], "nightly probe");
    let id = reports[0]["id"]
        .as_str()
        .expect("a listing carries each report's id")
        .to_string();
    assert!(
        id.starts_with("nightly_probe_"),
        "id 里要带着数据集名，翻目录的人不必打开每一份：{id}"
    );

    let (status, report) = call(
        state,
        "GET",
        &format!("/api/v1/eval/report/{id}"),
        &token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "列表说有的那一份要读得回来");
    assert_eq!(report["dataset_name"], "nightly probe");
    assert!(
        report["report_markdown"]
            .as_str()
            .is_some_and(|md| md.starts_with("rendered")),
        "读面要给出渲染过的正文：{report}"
    );

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn a_second_run_adds_a_second_report_instead_of_replacing_the_first() {
    let data_dir = scratch_dir();
    let state = router_state::with_eval_service(
        Arc::new(FixedEval {
            report: serde_json::json!({"dataset_name": "same-second"}),
        }),
        data_dir.clone(),
    );
    let token = router_state::admin_bearer(&state, "eval-report-twice").await;

    for _ in 0..2 {
        let (status, _) = call(
            state.clone(),
            "POST",
            "/api/v1/eval/run",
            &token,
            Some(serde_json::json!({"dataset_path": "same.jsonl"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    let (_, listing) = call(state, "GET", "/api/v1/eval/reports", &token, None).await;
    let reports = listing["reports"].as_array().expect("a reports array");
    assert_eq!(
        reports.len(),
        2,
        "两次跑要留两份：覆盖会让「上一次跑成什么样」永远消失：{listing}"
    );
    assert_ne!(reports[0]["id"], reports[1]["id"]);

    std::fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn a_deployment_that_never_ran_an_eval_lists_nothing_and_names_its_directory() {
    let data_dir = scratch_dir();
    let state = router_state::with_eval_service(
        Arc::new(FixedEval {
            report: serde_json::json!({"dataset_name": "unused"}),
        }),
        data_dir.clone(),
    );
    let token = router_state::admin_bearer(&state, "eval-report-empty").await;

    let (status, listing) = call(state, "GET", "/api/v1/eval/reports", &token, None).await;
    assert_eq!(status, StatusCode::OK, "没跑过评测不是读失败");
    assert_eq!(listing["total"], 0);
    assert!(
        listing["dir"]
            .as_str()
            .is_some_and(|d| d.contains("eval/reports")),
        "空列表要说出它读的是哪个目录，读者才分得清「这里空」与「读错地方」：{listing}"
    );

    std::fs::remove_dir_all(&data_dir).ok();
}
