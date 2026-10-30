//! 按任务预算的管理面（主应用侧）：声明 / 读取 / 撤销 / 名册。
//!
//! 主应用这一层**不做判定也不碰存储**，它只把请求转给安全网关。原因与检索后备池那
//! 一面相同：判定与记账发生在网关进程里，管理面经过第二跳去写同一个地方，就多出一层
//! 「谁读到的才是执行面在用的那一份」。主应用侧也拿不到那份键值面——它有插件上下文，
//! 但预算的执行点在另一个进程里，两个进程各写一份存储才会真的把两件事读成一件。
//!
//! 这一层有实体的理由只有一个：**签名在哪一侧**。内部 HMAC 的口令只有网关进程持有，
//! 页面能到的是主应用，所以转发这一步必须在主应用里发生。

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

/// GET /api/v1/admin/task-budget — 读一件任务的预算与今天的读数；不带 `task_id` 时
/// 回名册。
///
/// 读与列合成一条，是因为页面上那是同一张表：一行一件任务。分成两条路径会让「我查
/// 的这条不在名册里」与「这条没有预算」成为两个要分别回答的问题。
pub async fn task_budget_get_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Query(query): Query<TaskBudgetQuery>,
) -> Response {
    let body = json!({ "task_id": query.task_id.unwrap_or_default() });
    forward(body).await
}

#[derive(Debug, Deserialize)]
pub struct TaskBudgetQuery {
    /// 缺省即「回名册」。
    pub task_id: Option<String>,
}

/// POST /api/v1/admin/task-budget — 声明或撤销一件任务的当日上限。
///
/// 一次只要一个动作：`clear: true` 是撤，两个上限都给是写。只给一半是 400，
/// 而「撤」与「写」同时给也是 400——两个动作都要改同一个格子，挑哪一个作数都是
/// 替调用方做决定。
pub async fn task_budget_post_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<TaskBudgetRequest>,
) -> Response {
    if let Some(refusal) = reject_two_actions(&req) {
        return refusal;
    }
    forward(serde_json::to_value(&req).unwrap_or_default()).await
}

/// 「撤」与「写」同时给出来时的拒单。抽成函数是为了能在没有 `GatewayState` 的地方
/// 逐格验：这条规则只读请求体，状态里没有任何东西参与判断。
fn reject_two_actions(req: &TaskBudgetRequest) -> Option<Response> {
    (req.clear && (req.soft_limit.is_some() || req.hard_limit.is_some())).then(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "one_action_at_a_time",
                "message": "撤销与声明不能在同一请求里：这一格只有一份状态",
            })),
        )
            .into_response()
    })
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TaskBudgetRequest {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_limit: Option<u64>,
    #[serde(default)]
    pub clear: bool,
}

/// 把请求转给网关的内部面，状态码与正文原样回去。
///
/// 不在这里改写状态码：管理面读的是「这一格现在是什么」，而网关答的就是那个。把
/// 503（这台网关没有预算门）折成 502 **会**让页面把「没装」读成「连不上」，而下一次
/// 重试仍然通向同一个 503。
async fn forward(body: Value) -> Response {
    match crate::secret_write::forward_internal_raw(
        crate::security_gateway::INTERNAL_TASK_BUDGET_PATH,
        serde_json::to_vec(&body).unwrap_or_default(),
    )
    .await
    {
        Ok((status, value)) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(value),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": crate::SECURITY_GATEWAY_UNREACHABLE, "message": e})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 「撤」与「写」同一条请求里给出来是 400，而不是挑一个执行。
    ///
    /// 挑一个执行会让另一半**静默失效**：调用方以为自己撤掉了，实际上刚写上了新的
    /// 上限（或者反过来），而这件工作下一发被拒或被放行的理由里没有一个字提到这次
    /// 请求做了哪件事。
    #[tokio::test]
    async fn clearing_and_declaring_in_one_request_is_rejected() {
        let req = TaskBudgetRequest {
            task_id: "t-1".into(),
            soft_limit: Some(10),
            hard_limit: Some(20),
            clear: true,
        };
        let resp = reject_two_actions(&req).expect("撤与写同时给出必须被拒");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["error"], "one_action_at_a_time");
    }

    /// 只带任务名的请求转发出去时不能长出两个 `null` 上限。
    ///
    /// 网关侧按「给了什么」分辨动作，而 `"soft_limit": null` 与「没给这一格」在
    /// 反序列化之后同形、在**读到一半的旧判据**里不同形。少发一格，就是不让人去
    /// 猜哪一种写法才算「只读」。
    #[test]
    fn a_read_request_carries_no_limit_fields() {
        let req: TaskBudgetRequest = serde_json::from_str(r#"{"task_id":"t-1"}"#).unwrap();
        assert!(!req.clear);
        let wire = serde_json::to_value(&req).unwrap();
        assert_eq!(wire["task_id"], "t-1");
        assert!(
            wire.get("soft_limit").is_none() && wire.get("hard_limit").is_none(),
            "读一次不该附带两个空上限：{wire}"
        );
    }
}
