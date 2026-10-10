//! 外部价值判定（taste）的接收面。
//!
//! 这是系统里唯一一处让外部的「什么算好」进入进化输入的地方：其它输入都是
//! 缺陷语义（失败的活、还在响的告警、堵住的队列），它们说哪里疼，说不出往
//! 哪走。判定由提交者署名、原样落盘，随后与任何别的信号走同一条冷却/幂等框
//! 架变成活。
//!
//! 这里只做三件事：收下提交、把它落到存储面、在审计链上留下提交者与判定类
//! 型。判断这条判定值不值得变成活，不在这里——那是把提交变成活的那一端的
//! 事，本处重复一次就会有两个答案。

use std::sync::Arc;

use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use cog_core::{AuditKind, Claims, TasteIntent, TasteIntentPayload};

/// 审计链上记这类提交的 kind。
const TASTE_AUDIT_KIND: &str = "taste_intent_submitted";

/// 审计链上的动作名：收到的是一次提交，不是一个已经做过的判定。
const TASTE_AUDIT_ACTION: &str = "submit";

/// 提交体。
///
/// 提交者、提交 id 与时标都不在体里：前两者由鉴权与本次提交决定，写进体里就
/// 成了自报的身份。`subject` 只在提交侧收一次并去空白，两边都按去空白后的形
/// 式记忆它——同一个东西写成带空格与不带空格是同一个东西，而按哪个形式记决
/// 定了重复抑制会不会把两次提交认成两回事。
#[derive(Debug, Deserialize)]
pub struct TasteIntentRequest {
    /// 这条判定针对的是什么。它随后与判定类型配对成为重复抑制的键。
    pub subject: String,
    pub payload: TasteIntentPayload,
    /// 这条判定据以作出的材料，按引用给出（raw id 或绝对 URI）。
    ///
    /// 缺省即「没给材料」，与「判定无关材料」不是一回事：每一种判定都有
    /// subject，所以这里空着只是提交者没拿出东西来。形状的判据在契约层，
    /// 不在这个面上——同一条记录日后还会从系统内部的生产者进来。
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

/// 已收下的一条提交。
#[derive(Debug, Serialize)]
pub struct TasteIntentAccepted {
    pub id: Uuid,
    pub kind: &'static str,
    pub subject: String,
    pub submitted_by: String,
    pub submitted_at: DateTime<Utc>,
    /// 落盘时记下的材料引用，原样回给提交者：读回面还没被翻到之前，这是
    /// 「我指的是那份东西」当场唯一的回执。
    pub evidence_refs: Vec<String>,
}

/// 收下一条外部提交的价值判定。
///
/// 落盘先于回应，也先于审计：提交本身就是证据，一个只回了 200 而没有落盘的
/// 提交等于凭空捏造——它的产物日后无从对照当时到底要求了什么。审计写不进去
/// 则只告警、不回退整个请求：提交已经落盘，此时回错会让提交者重发，而重发改
/// 变不了任何已经写下的事实；提交者与判定类型也在落盘的那一行里，不缺这一份。
pub async fn submit_taste_intent_handler(
    State(state): State<Arc<crate::GatewayState>>,
    claims: Option<Extension<Claims>>,
    Json(req): Json<TasteIntentRequest>,
) -> Response {
    let Some(sink) = state.taste_intent_sink.as_ref() else {
        // 存储面缺席（未配库、或它的表建不出来）时显式拒绝：
        // 这条路上「先收下再丢掉」是不可接受的降级。
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "taste intent store not configured"})),
        )
            .into_response();
    };

    // 挂载点自带鉴权层，所以 Claims 一定在；取不到说明这条路由被挂到了没有
    // 鉴权的组上，那是接线错误而不是匿名访问——不能给提交者编一个名字署名。
    let Some(Extension(claims)) = claims else {
        warn!("taste intent route reached without claims; refusing to record an unnamed submitter");
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "submission requires an authenticated caller"})),
        )
            .into_response();
    };

    let intent = TasteIntent {
        id: Uuid::new_v4(),
        subject: req.subject.trim().to_string(),
        submitted_by: claims.sub.clone(),
        submitted_at: Utc::now(),
        payload: req.payload,
        evidence_refs: req.evidence_refs,
    };
    if let Err(error) = intent.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error })),
        )
            .into_response();
    }

    if let Err(error) = sink.submit(&intent).await {
        // 提交没能落盘：这条判定没进来，提交者必须知道。
        warn!(
            subject = %intent.subject,
            kind = intent.payload.kind(),
            error = %error,
            "taste intent submission not stored"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "submission could not be stored"})),
        )
            .into_response();
    }

    if let Some(audit) = state.audit_stream.as_ref() {
        let detail = serde_json::json!({
            "kind": intent.payload.kind(),
            "subject": intent.subject,
            "payload": intent.payload,
            "evidence_refs": intent.evidence_refs,
        });
        if let Err(error) = audit
            .append(
                AuditKind::Custom(TASTE_AUDIT_KIND.to_string()),
                &intent.submitted_by,
                &intent.subject,
                TASTE_AUDIT_ACTION,
                detail,
            )
            .await
        {
            warn!(
                id = %intent.id,
                error = %error,
                "taste intent stored but not written to the audit chain"
            );
        }
    }

    (
        StatusCode::ACCEPTED,
        Json(TasteIntentAccepted {
            id: intent.id,
            kind: intent.payload.kind(),
            subject: intent.subject,
            submitted_by: intent.submitted_by,
            submitted_at: intent.submitted_at,
            evidence_refs: intent.evidence_refs,
        }),
    )
        .into_response()
}
