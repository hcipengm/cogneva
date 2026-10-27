//! Admin API for LLM provider configuration.
//!
//! The WebUI setup wizard uses these endpoints to connect the system to an
//! LLM after installation. Saving writes the upstream pool (one entry for a
//! single LLM, more for failover) into the `cogneva-secrets` Secret as the
//! `llm-upstreams` JSON entry — the gateway's only upstream source. Only the
//! gateway holds credentials; the main app and sandbox talk to the gateway,
//! so rolling the gateway is the only restart needed. The gateway fails over
//! across the pool on 429 (rate limit) / 402 (quota exhausted).

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

const SA_DIR: &str = cog_core::topology::SERVICE_ACCOUNT_DIR;
const API_BASE: &str = "https://kubernetes.default.svc";
/// 上游池条目：JSON 数组，每元素 {api_style, base_url, model, api_key}。
/// 它住带外片——这个键只被安全网关读，放内部片等于铺给持那一份的每个业务 Pod
/// （那份是整对象 get）。键位表与归属见 `secret_layout`。
const SECRET_UPSTREAMS_KEY: &str = "llm-upstreams";
/// 池的冗余下限（十进制整数，空串 = 没有声明）。它与上游池同一次写入、同一片
/// Secret：声明池需要几家、与声明池里有哪几家是同一件事，分两处写就会有一段时间
/// 里两边说的不是同一个池。
const SECRET_MIN_HEALTHY_KEY: &str = "llm-pool-min-healthy";
const GATEWAY_DEPLOYMENT: &str = cog_core::topology::SECURITY_GATEWAY;

/// 上游能力探测单次最多等多久。
///
/// 探测发的是一个最小 chat 请求，30s 覆盖跨区上游的建连加排队；等更久不会让
/// 判定更准，只会让一次配置写入卡住管理端。超时在这里读作「能力未知」，与
/// 探测失败同一个结局。
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// 协议形态探测（anthropic / openai 两种线格式）单次上限。
///
/// 比能力探测紧，因为它要串着试两种形态，墙就是两倍；8s 是健康上游往返的
/// 量级，超时读作「这一种不通」，直接换下一种。
const STYLE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

#[derive(Debug, Deserialize)]
pub struct LlmUpstreamRequest {
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    /// 协议面首猜（openai/anthropic）：探测按此顺序优先，失败自动换另一种。
    pub api_style: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LlmConfigRequest {
    /// 可选，仅作展示；网关按协议面路由，不消费该字段。
    pub provider: Option<String>,
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    /// 协议面首猜（openai/anthropic）：探测按此顺序优先，失败自动换另一种。
    pub api_style: Option<String>,
    /// 探测失败后的"仍然保存"：跳过连通验证直接落库。
    pub skip_verify: Option<bool>,
    /// 追加的故障转移上游：主上游 429/402 时按声明顺序顶上。
    pub extra_upstreams: Option<Vec<LlmUpstreamRequest>>,
    /// 声明的冗余下限：池里至少要有几家上游能承接请求，低于它就按"池不可用"
    /// 判（告警 + 暂停 LLM 依赖型派活）。缺省 = 不声明，判定回落到"一家都不供
    /// 才算不可用"。
    ///
    /// 这**不是**一个可有可无的旋钮：池的成员按构造可以互相顶替，所以"还剩几家
    /// 能供"是池自己的容量。这个字段缺省时，一个只剩一家的池读起来与四家齐供
    /// 完全一样，而下一次 429 就会把整条链停掉。
    ///
    /// 取值必须是 `1..=上游家数`：0 家是空池也能满足的界（等于"永不判不可用"），
    /// 超过池的大小则永远达不成（等于"永不判可用"），两个方向都是把笔误读成规则，
    /// 所以这一层直接拒单而不是替调用方夹一个数——它夹出来的那个值调用方看不见。
    pub min_healthy: Option<usize>,
}

/// GET /api/v1/admin/llm-status — whether the security gateway holds a
/// configured upstream pool. Never returns key material. In-cluster the
/// source of truth is the `llm-upstreams` Secret entry (the main app is
/// credential-free); outside the cluster we fall back to inspecting local
/// cogneva.json.
pub async fn llm_status_handler(State(_state): State<Arc<crate::GatewayState>>) -> Response {
    // 池的真值只注入安全网关那个 Deployment（`llm-upstreams` 在带外那一片），
    // 主应用没有读它的权限，也不该有。所以这一屏只有一条来源：问网关要派生读数。
    match forwarded_llm_status().await {
        Ok((configured, pool)) => (
            StatusCode::OK,
            Json(json!({
                "configured": configured,
                "backends": local_backends(),
                "pool": pool,
                "source": "security-gateway",
            })),
        )
            .into_response(),
        // 问不到网关时不能拿"本进程读到的"顶上去：主应用读到的是空，而空会被
        // 页面读成"没配"。这里如实报不知道，并把原因带上。
        Err(e) => (
            StatusCode::OK,
            Json(json!({
                "configured": false,
                "unknown": true,
                "read_error": e,
                "backends": local_backends(),
                "source": "unavailable",
            })),
        )
            .into_response(),
    }
}

/// 池的声明面：三个数一起印。
///
/// 这一格有两个来源——本进程直读 Secret（迁移窗里的旧路）与网关转发的派生读数
/// （目标态）。两处各拼一遍 JSON 就会漂：页面读得出一个界、判定面读不出，而两边
/// 都自报成功。所以形状只在这里定义，两个来源都经过它。
pub(crate) fn pool_policy_json(upstreams: usize, declared: Option<usize>) -> serde_json::Value {
    json!({
        "upstreams": upstreams,
        "min_healthy": declared,
        "min_healthy_source": cog_core::min_healthy_source(declared),
        "min_healthy_in_force": cog_core::pool_floor(upstreams, declared),
    })
}

/// 主应用侧：问安全网关要池的状态。
///
/// 返回的是派生读数（家数、声明下限），不是上游池本身：`llm-upstreams` 里每条都
/// 带一把 key，把它回传给零凭证的进程等于把刚搬走的凭证又发回去。签名用与
/// `secret-patch` 同一条内部 HMAC——两个进程之间本来只有这一条信任边界。
pub(crate) async fn forwarded_llm_status() -> Result<(bool, serde_json::Value), String> {
    let body = crate::secret_write::forward_internal(
        crate::security_gateway::INTERNAL_LLM_STATUS_PATH,
        Vec::new(),
    )
    .await?;
    let configured = body
        .get("configured")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| "安全网关的池状态里没有 configured".to_string())?;
    let upstreams = body.get("upstreams").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let declared = body
        .get("min_healthy")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    Ok((configured, pool_policy_json(upstreams, declared)))
}

/// Backends declared in the local cogneva.json (informational only — after
/// credential narrowing the primary backend points at the security gateway).
fn local_backends() -> Vec<serde_json::Value> {
    let path = cog_core::paths::config_file();
    match std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .and_then(|cfg| cfg.get("llm_routing")?.get("backends")?.as_array().cloned())
    {
        Some(list) => list
            .iter()
            .map(|b| {
                let key = b.get("api_key").and_then(|k| k.as_str()).unwrap_or("");
                json!({
                    "provider": b.get("provider").and_then(|v| v.as_str()).unwrap_or(""),
                    "base_url": b.get("base_url").and_then(|v| v.as_str()).unwrap_or(""),
                    "model": b.get("model").and_then(|v| v.as_str()).unwrap_or(""),
                    "api_style": b.get("api_style").and_then(|v| v.as_str()).unwrap_or("openai"),
                    "enabled": b.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false),
                    "has_key": key_resolves(key),
                })
            })
            .collect::<Vec<_>>(),
        None => Vec::new(),
    }
}

/// A key "resolves" when it is either a literal non-empty value or a
/// `${VAR}` reference whose environment variable is set and non-empty.
fn key_resolves(raw: &str) -> bool {
    let trimmed = raw.trim();
    if let Some(var) = trimmed.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        return std::env::var(var).map(|v| !v.is_empty()).unwrap_or(false);
    }
    !trimmed.is_empty()
}

/// POST /api/v1/admin/llm-config — persist provider settings and roll the
/// deployments that consume them.
pub async fn llm_config_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<LlmConfigRequest>,
) -> Response {
    let skip_verify = req.skip_verify.unwrap_or(false);
    let primary = LlmUpstreamRequest {
        base_url: req.base_url.clone(),
        model: req.model.clone(),
        api_key: req.api_key.clone(),
        api_style: req.api_style.clone(),
    };
    let mut requests = vec![primary];
    requests.extend(req.extra_upstreams.unwrap_or_default());

    // 声明的下限在落库前对**这次要落的那个池**校验：超过池大小的界永远达不成，
    // 0 的界永远满足，两者都不是运营者想写的那条规则，而它们都会安静地生效。
    if let Some(declared) = req.min_healthy {
        if declared == 0 || declared > requests.len() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "min_healthy_out_of_range",
                    "message": format!(
                        "冗余下限必须在 1..={} 之间（这次声明了 {} 个上游）",
                        requests.len(),
                        requests.len()
                    ),
                    "upstreams": requests.len(),
                    "min_healthy": declared,
                })),
            )
                .into_response();
        }
    }

    // 逐条校验 + 实证探测：地址/可达性不过关则整单拒绝，避免落下半残池；
    // 上游明确拒服（额度/限流/鉴权）不算不过关——池内条目互相独立，运行
    // 时会按序转移，这类条目照样落池，只是如实标记为未验证。
    let mut pool: Vec<serde_json::Value> = Vec::with_capacity(requests.len());
    let mut unverified: Vec<serde_json::Value> = Vec::new();
    for item in &requests {
        match resolve_upstream(item, skip_verify).await {
            Ok((entry, warning)) => {
                if let Some(reason) = warning {
                    unverified.push(json!({
                        "base_url": entry["base_url"],
                        "model": entry["model"],
                        "reason": reason,
                    }));
                }
                pool.push(entry);
            }
            Err(resp) => return *resp,
        }
    }

    let pool_json = match serde_json::to_string(&pool) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "serialize_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };

    // 两个键同一次写入：池与它的下限是同一份声明的两半。缺省写空串而不是删键
    // ——merge patch 删键要靠 null，而那与"把值写成空的 base64"在这一层分不开；
    // 空串在读者那侧就是「没有声明」（见 `min_healthy_from_env`）。旧镜像读到这个
    // 键不认，也不受影响：它按 `COGNEVA_LLM_UPSTREAMS` 一个键判池。
    let min_healthy_value = req.min_healthy.map(|n| n.to_string()).unwrap_or_default();
    if let Err(e) = crate::secret_write::forward(
        json!({
            SECRET_UPSTREAMS_KEY: pool_json,
            SECRET_MIN_HEALTHY_KEY: min_healthy_value,
        }),
        false,
        true,
    )
    .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
        )
            .into_response();
    }

    // restartedAt 注解触发网关滚动重启，新 Pod 启动时从 Secret 读
    // COGNEVA_LLM_UPSTREAMS 拿到完整上游池。
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "restarted": [GATEWAY_DEPLOYMENT],
            "upstreams": pool.len(),
            "min_healthy": req.min_healthy,
            "min_healthy_source": cog_core::min_healthy_source(req.min_healthy),
            "unverified_upstreams": unverified,
            "message": "配置已保存，安全网关正在滚动重启，约一分钟后生效",
        })),
    )
        .into_response()
}

/// 校验单条上游并定协议面：返回可写入池的 JSON 条目；验证失败返回
/// 直接可回给前端的错误响应。
async fn resolve_upstream(
    item: &LlmUpstreamRequest,
    skip_verify: bool,
) -> Result<(serde_json::Value, Option<String>), Box<Response>> {
    let base_url = item.base_url.trim();
    let model = item.model.trim();
    let api_key = item.api_key.trim();
    let style_hint = item
        .api_style
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if model.is_empty() || api_key.is_empty() {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_request", "message": "model、api_key 不能为空"})),
            )
                .into_response(),
        ));
    }
    if !(base_url.starts_with("https://") || base_url.starts_with("http://")) {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_request", "message": "base_url 必须是 http(s):// 开头的完整地址"})),
            )
                .into_response(),
        ));
    }

    // 双协议实证探测：按首猜顺序先试，失败自动换另一种协议面，
    // 胜出者即为写入网关的 api_style（Kimi 这类 base_url 看不出协议的
    // 端点也能当场测出来）。skip_verify 时采信首猜，缺省 openai。
    //
    // 上游明确应答但拒服时条目仍会落池（运行时会按序转移），所以这里把
    // 「没验证过」单独记下来回给运营者，不把拒服说成验证通过。
    let mut unverified: Option<String> = None;
    let api_style = if skip_verify {
        unverified = Some("已按 skip_verify 跳过连通验证".to_string());
        match style_hint {
            Some("anthropic") => "anthropic",
            Some(cog_llm::responses::API_STYLE) => cog_llm::responses::API_STYLE,
            _ => "openai",
        }
    } else {
        match detect_api_style(base_url, model, api_key, style_hint).await {
            ApiStyleProbe::Verified(style) => style,
            ApiStyleProbe::Refused(style, message) => {
                tracing::warn!(
                    base_url = %base_url,
                    model = %model,
                    style,
                    detail = %message,
                    "upstream refused the probe; admitting it unverified"
                );
                unverified = Some(message);
                style
            }
            ApiStyleProbe::Invalid(message) => {
                return Err(Box::new(
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(json!({"error": "llm_verify_failed", "message": message})),
                    )
                        .into_response(),
                ));
            }
        }
    };

    // function-calling 准入探测（仅 openai 协议面）：chat 能通不代表能干
    // 工具活——有的上游把工具调用写成文本而非原生 tool_calls 字段，进化/
    // 工具类负载在这种上游上空转烧钱。探测结果存进池条目，透传层据此
    // 把携带 tools 的请求路由离开证实不支持的上游。探测失败（网络抖动
    // 等）不写字段，保持未知放行。skip_verify 时跳过探测。
    let mut entry = json!({
        "api_style": api_style,
        "base_url": base_url,
        "model": model,
        "api_key": api_key,
    });
    if cog_llm::responses::is_openai_family(api_style) && !skip_verify {
        match detect_tool_call_support(base_url, model, api_key).await {
            Some(supported) => {
                entry["supports_tool_calls"] = json!(supported);
            }
            None => {
                tracing::warn!(
                    base_url = %base_url,
                    model = %model,
                    "tool-calls capability probe inconclusive; capability left unknown"
                );
            }
        }
        // temperature 准入探测：有些推理模型只接受 temperature=1，调用方发的
        // 别的值会被上游直接 400。调用方自己判断不了——它连的是网关，base URL
        // 里没有厂商身份，客户端侧按域名做的兼容探测在部署形态下永远不命中，
        // 该判据结构性不可达。网关是唯一知道真实上游的地方，探测结果存进池
        // 条目，透传层据此钳制。探测不成（配额/鉴权/网络）不写字段，保持原样。
        match detect_temperature_constraint(base_url, model, api_key).await {
            Some(requires_one) => {
                entry["requires_temperature_one"] = json!(requires_one);
            }
            None => {
                tracing::warn!(
                    base_url = %base_url,
                    model = %model,
                    "temperature constraint probe inconclusive; constraint left unknown"
                );
            }
        }
        // usage 准入探测：这台上游会不会在流里报用量。调用方判断不了（它连的是
        // 网关，客户端侧的厂商画像按域名匹配永远落兜底），而全系统的 token 计量
        // 都建在"流里有 usage 尾帧"上，所以这个判定必须问过上游而不是照抄画像。
        // 探测不成（配额/鉴权/网络）不写字段，回落画像。
        match detect_usage_in_streaming(base_url, model, api_key).await {
            Some(reports_usage) => {
                entry["supports_usage_in_streaming"] = json!(reports_usage);
            }
            None => {
                tracing::warn!(
                    base_url = %base_url,
                    model = %model,
                    "usage-in-stream probe inconclusive; capability left to the vendor profile"
                );
            }
        }
    }

    Ok((entry, unverified))
}

/// 把一次最小 temperature 探测的响应归成能力判定：`Some(false)` = 上游接受了
/// 非 1 的 temperature；`Some(true)` = 上游因 temperature 明确拒绝，只认 1；
/// `None` = 说不清（配额/鉴权/网络/与温度无关的 400），不写字段。
///
/// 只有上游报文里点名 temperature 才算判定。配额与鉴权这类 4xx 也会带着各自
/// 的措辞回来，把它们误读成"只认 1"会让所有调用方的温度被静默改写。
fn classify_temperature_probe(status: u16, body: &str) -> Option<bool> {
    if (200..300).contains(&status) {
        return Some(false);
    }
    if body.to_lowercase().contains("temperature") {
        return Some(true);
    }
    None
}

/// temperature 准入探测：发一个 temperature=0.2 的最小 chat 请求，看上游认不认。
///
/// 与用量那条一样是 `pub(crate)`：运行期补问会再问一遍，问不出结论的条目
/// （配额/鉴权/网络）与"比那次配置写入更早落下的条目"都靠这条路补上判定。
pub(crate) async fn detect_temperature_constraint(
    base_url: &str,
    model: &str,
    api_key: &str,
) -> Option<bool> {
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .ok()?;
    let resp = client
        .post(format!(
            "{}/chat/completions",
            base_url.trim_end_matches('/')
        ))
        .bearer_auth(api_key)
        .json(&json!({
            "model": model,
            "temperature": 0.2,
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "ping"}]
        }))
        .send()
        .await
        .ok()?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    classify_temperature_probe(status, &body)
}

/// 把一次 usage 探测的响应归成能力判定：`Some(true)` = 上游在流里报了用量
/// （字段被认，且尾帧有 usage）；`Some(false)` = 不会报（要么明确拒这个字段，
/// 要么收了却不报——两种情形下"剥掉"都不会丢掉任何能拿到的用量）；`None` =
/// 说不清（配额/鉴权/网络/与这个字段无关的 4xx），不写字段、回落画像。
///
/// 判 `Some(false)` 的方向是安全的：它只会让网关继续剥这个字段，即现状。
/// 反过来把"配额 403"读成"支持"会让每次调用都带一个上游可能不认的字段。
///
/// 判据是"**流帧里**有没有 usage"，不是"报文里有没有这个子串"：整块 JSON
/// 应答里也含这个子串，但那说明上游根本没按流式回，它回答的不是我们要问的
/// 那个问题（下游读的是流）。两种形态在同一条 grep 下同形，所以这里分开：
/// 有 usage 但不在任何 `data:` 帧里，判 `None` 而不是 `Some(true)`——把不是
/// 流式的应答读成"支持"，正好会把"其实不支持"钉成实测结论。
fn classify_usage_probe(status: u16, body: &str) -> Option<bool> {
    if (200..300).contains(&status) {
        let in_frame = body
            .lines()
            .any(|l| l.trim_start().starts_with("data:") && l.contains("\"usage\""));
        if in_frame {
            return Some(true);
        }
        if body.contains("\"usage\"") {
            return None;
        }
        return Some(false);
    }
    let lowered = body.to_lowercase();
    if lowered.contains("stream_options") || lowered.contains("include_usage") {
        return Some(false);
    }
    None
}

/// usage 准入探测：发一个带 `stream_options.include_usage` 的最小流式请求，
/// 看这台上游认不认、认了会不会在流里报用量。
///
/// 为什么这一条要单独问：`supports_usage_in_streaming` 的取值面就是"流里有没有
/// usage"，而 usage 出不出现取决于我们发不发这个字段。按厂商画像硬编码着剥，
/// 就把"它本来会不会报"变成了不可观测——两种解释在同一份读数上完全同形，判错
/// 也永远拿不到反证。问过这台上游，判定就不再有这个问题。
///
/// 问不出来的理由要跟着回来：这一条被调用的两个地方（管理端配置写入、运行时
/// 补问）都会把结论当"实测"用，而 `None` 在两边都读作"保持画像兜底"。只报一句
/// "inconclusive" 的话，"上游回了个不是流式的 200"与"配额墙"在同一行日志里同形，
/// 而前者要去改判据、后者只能等窗口。
pub(crate) async fn detect_usage_in_streaming(
    base_url: &str,
    model: &str,
    api_key: &str,
) -> Option<bool> {
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .ok()?;
    let resp = client
        .post(format!(
            "{}/chat/completions",
            base_url.trim_end_matches('/')
        ))
        .bearer_auth(api_key)
        .json(&json!({
            "model": model,
            "max_tokens": 8,
            "stream": true,
            "stream_options": {"include_usage": true},
            "messages": [{"role": "user", "content": "ping"}]
        }))
        .send()
        .await
        .ok()?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    let verdict = classify_usage_probe(status, &body);
    // 问到了什么形态跟着判定一起记：`None` 的两个成因（不是流式的 200、
    // 配额/鉴权类拒服）只能从这三个数分开，而它们指向完全不同的下一步。
    tracing::info!(
        base_url = %base_url,
        model = %model,
        status,
        sse_frames = body.lines().filter(|l| l.trim_start().starts_with("data:")).count(),
        names_usage = body.contains("\"usage\""),
        verdict = ?verdict,
        "usage-in-stream probe answered"
    );
    verdict
}

/// function-calling 实证探测：发一个带 tools 的最小 chat 请求，强制模型
/// 调工具；响应的 `message.tool_calls` 非空才算支持原生协议。返回 None
/// 表示探测本身失败（网络/非 2xx），能力未知。文本协议变体（工具调用
/// 写在 content 里）判定为不支持——那正是要挡在工具类负载门外的形态。
pub(crate) async fn detect_tool_call_support(
    base_url: &str,
    model: &str,
    api_key: &str,
) -> Option<bool> {
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .ok()?;
    let resp = client
        .post(format!(
            "{}/chat/completions",
            base_url.trim_end_matches('/')
        ))
        .bearer_auth(api_key)
        .json(&json!({
            "model": model,
            "max_tokens": 128,
            "messages": [{
                "role": "user",
                "content": "What is the weather in Paris right now? You must use the get_weather tool to answer."
            }],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get the current weather for a city",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "city": {"type": "string", "description": "City name"}
                        },
                        "required": ["city"]
                    }
                }
            }],
            "tool_choice": "auto"
        }))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = resp.json().await.ok()?;
    let has_native_tool_calls = body
        .pointer("/choices/0/message/tool_calls")
        .and_then(|t| t.as_array())
        .is_some_and(|t| !t.is_empty());
    Some(has_native_tool_calls)
}

/// 一次协议面探测的结局。判据只有「上游到底怎么应答」这一条，不按状态码
/// 推病因：同一件事（额度耗尽）各家给的码不同（429/402/403/451），按码表判
/// 必然漏。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeOutcome {
    /// 探针被 2xx 接受：这个协议面当场就能干活。
    Accepted,
    /// 这个协议面上没有该端点，换另一个协议面试。
    NoSuchEndpoint,
    /// 上游在这条协议面上明确应答了，只是拒绝服务（额度/限流/鉴权）。
    Refused,
}

fn classify_probe(status: u16) -> ProbeOutcome {
    if (200..300).contains(&status) {
        ProbeOutcome::Accepted
    } else if status == 404 {
        ProbeOutcome::NoSuchEndpoint
    } else {
        ProbeOutcome::Refused
    }
}

/// 协议探测的判决。
#[derive(Debug)]
enum ApiStyleProbe {
    /// 探针当场通了这个协议面。
    Verified(&'static str),
    /// 上游在首猜协议面上明确应答但拒绝服务，按首猜入池（未验证）。
    ///
    /// 判据与运行时池内转移一致（`security_gateway` 对首字节前的任何非 2xx
    /// 都换池内下一个）：拒绝服务是上游当下的健康问题，不是「这条不该在
    /// 池里」。早先的判据把 401/403 整单判死，后果是**能干活的上游进不来**
    /// ——不实现 `/models` 的端点被读成「密钥无效」，配额耗尽的被读成「地址
    /// 或模型名错」——池因此长期为空，整个系统 503 空转。
    Refused(&'static str, String),
    /// 两个协议面都没拿到可归因的应答：地址写错，或根本连不上。
    Invalid(String),
}

/// 保存前用用户手填的真 key 做实证协议探测（每种协议超时 8 秒）：
/// 按首猜顺序各发一个最小 chat 请求（openai 风格 `/chat/completions`，
/// anthropic 风格 `/v1/messages`），哪个当场通了就返回哪个协议面，
/// base_url 看不出协议的端点（如 Kimi coding）也能当场测出。
///
/// 探针走的是**真实调用路径**而不是 `GET /models`：后者是可选端点，
/// 不实现它的 OpenAI 兼容网关会把可用的上游读成「密钥无效」。
///
/// 拒绝服务不整单判死（见 `ApiStyleProbe::Refused`），只有「两个协议面都
/// 没有该端点」或「连不上」才拒收。前端仍可带 skip_verify 直接采信首猜。
/// The address a protocol face is probed at.
///
/// Composed by the same functions the runtime uses, not by a second copy of
/// the rule. A probe that checks one address and a client that calls another
/// accepts a configuration that then fails every request, and the failure
/// reads as a wrong key rather than as a wrong address: this used to probe
/// `{base}/v1/messages` while the provider called `{base}/messages`, so an
/// Anthropic entry saved as `https://api.anthropic.com` passed the wizard and
/// 404'd in production.
fn protocol_probe_url(style: &str, base_url: &str) -> String {
    match style {
        "openai" => cog_llm::openai::chat_completions_url(base_url),
        cog_llm::responses::API_STYLE => cog_llm::responses::responses_url(base_url),
        _ => cog_llm::anthropic::messages_url(base_url),
    }
}

async fn detect_api_style(
    base_url: &str,
    model: &str,
    api_key: &str,
    style_hint: Option<&str>,
) -> ApiStyleProbe {
    let client = match reqwest::Client::builder()
        .timeout(STYLE_PROBE_TIMEOUT)
        .build()
    {
        Ok(client) => client,
        Err(e) => return ApiStyleProbe::Invalid(format!("HTTP 客户端构建失败: {e}")),
    };
    let base = base_url.trim_end_matches('/');
    let first_anthropic = style_hint == Some("anthropic");

    let mut refused: Option<(&'static str, String)> = None;
    // 在 Responses 面上被明确拒服的那一次。它不能进 `refused`：那个面是**偏好**
    // 而不是唯一候选，一次 400/429 可能只是"这个端点不认这个体"，而同一台上游
    // 的 chat 面完全可能当场通——先把它按 chat 面入池才对。只有整个族都没有一个
    // 面给过 2xx 时，它才作为这个族的拒服证据拿出来。
    let mut responses_refusal: Option<String> = None;
    let mut unreachable: Option<String> = None;

    // 顺序就是偏好：OpenAI 族里先问 Responses 面，答得通就记那个面。这是**同一台
    // 上游的两个端点**，先问新的那个不会让旧的消失（池条目按族匹配请求面），而
    // 反过来先问 chat 面会把这个能力整个问不出来。
    let order: [&'static str; 3] = if first_anthropic {
        ["anthropic", cog_llm::responses::API_STYLE, "openai"]
    } else {
        [cog_llm::responses::API_STYLE, "openai", "anthropic"]
    };

    for style in order {
        let result = match style {
            cog_llm::responses::API_STYLE => {
                client
                    .post(protocol_probe_url(style, base))
                    .bearer_auth(api_key)
                    .json(&json!({
                        "model": model,
                        "max_output_tokens": 16,
                        "input": "ping"
                    }))
                    .send()
                    .await
            }
            "openai" => {
                client
                    .post(protocol_probe_url(style, base))
                    .bearer_auth(api_key)
                    .json(&json!({
                        "model": model,
                        "max_tokens": 1,
                        "messages": [{"role": "user", "content": "ping"}]
                    }))
                    .send()
                    .await
            }
            _ => {
                client
                    .post(protocol_probe_url(style, base))
                    .header("x-api-key", api_key)
                    .header("anthropic-version", "2023-06-01")
                    .json(&json!({
                        "model": model,
                        "max_tokens": 1,
                        "messages": [{"role": "user", "content": "ping"}]
                    }))
                    .send()
                    .await
            }
        };
        match result {
            Ok(resp) => match classify_probe(resp.status().as_u16()) {
                ProbeOutcome::Accepted => return ApiStyleProbe::Verified(style),
                ProbeOutcome::NoSuchEndpoint => continue,
                ProbeOutcome::Refused => {
                    let status = resp.status();
                    let detail: String = resp
                        .text()
                        .await
                        .unwrap_or_default()
                        .chars()
                        .take(160)
                        .collect();
                    let message = format!("{base_url} 返回 HTTP {status}: {detail}");
                    if style == cog_llm::responses::API_STYLE {
                        tracing::info!(
                            base_url = %base_url,
                            status = %status,
                            "responses probe was refused; asking the chat face before drawing a conclusion"
                        );
                        if responses_refusal.is_none() {
                            responses_refusal = Some(message);
                        }
                        continue;
                    }
                    if refused.is_none() {
                        refused = Some((style, message));
                    }
                }
            },
            Err(e) => {
                // 连接都建立不了，换协议面也一样，但仍试完另一面——base_url
                // 的路径可能只对其中一种协议面成立。
                if unreachable.is_none() {
                    unreachable = Some(format!("无法连接 {base_url}（{e}），请检查地址与网络"));
                }
            }
        }
    }

    if let Some((style, message)) = refused {
        return ApiStyleProbe::Refused(style, message);
    }
    if let Some(message) = responses_refusal {
        // 只有 Responses 面答过话（而且拒绝了）：按最不张扬的判定入池——chat 面
        // 没验证过，不能记成 Responses 面。
        return ApiStyleProbe::Refused("openai", message);
    }
    if let Some(message) = unreachable {
        return ApiStyleProbe::Invalid(message);
    }
    ApiStyleProbe::Invalid(format!(
        "{base_url} 上没有可用的协议端点（三个面都返回 404）。base_url 要写到\n\
         版本段为止、不要写到最后的资源段：平台只往后接 `/chat/completions`（OpenAI 兼容）\n\
         或 `/responses`（OpenAI Responses）或 `/messages`（Anthropic），所以 Anthropic 官方地址是\n\
         https://api.anthropic.com/v1，DeepSeek 的 Anthropic 面是 https://api.deepseek.com/anthropic/v1"
    ))
}

/// apiserver 的判决里，哪些码明确定性为「这个进程不持有该权限」。
///
/// 只认 401/403：它们是权限与凭证的判决，与故障无关。其余非 2xx 都留有余地
/// （404 资源还没建、5xx/429 apiserver 侧的问题），归「还要继续观察」——
/// 属主不能因为一次启动抖动就被判成「这不是它的活」而静默停摆。
fn apiserver_denies_ownership(status: u16) -> bool {
    matches!(status, 401 | 403)
}

/// Minimal in-cluster Kubernetes API client (service account token + CA).
pub(crate) struct KubeClient {
    http: reqwest::Client,
    token: String,
    namespace: String,
}

impl KubeClient {
    pub(crate) fn in_cluster() -> Result<Self, String> {
        let read = |name: &str| {
            std::fs::read(format!("{SA_DIR}/{name}"))
                .map_err(|e| format!("无法读取 {SA_DIR}/{name}（不在 K8s 集群内？）: {e}"))
        };
        let token = String::from_utf8_lossy(&read("token")?).trim().to_string();
        let namespace = read("namespace")
            .map(|b| String::from_utf8_lossy(&b).trim().to_string())
            .unwrap_or_else(|_| cog_core::topology::namespace());
        let ca = reqwest::Certificate::from_pem(&read("ca.crt")?)
            .map_err(|e| format!("集群 CA 证书解析失败: {e}"))?;
        let http = reqwest::Client::builder()
            .add_root_certificate(ca)
            .build()
            .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?;
        Ok(Self {
            http,
            token,
            namespace,
        })
    }

    /// GET an apiserver path and return the parsed JSON body.
    pub(crate) async fn get_json(&self, path: &str) -> Result<serde_json::Value, String> {
        let resp = self
            .http
            .get(format!("{API_BASE}{path}"))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("请求 apiserver 失败: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("apiserver 返回 {}", resp.status()));
        }
        resp.json()
            .await
            .map_err(|e| format!("apiserver 响应解析失败: {e}"))
    }

    /// 当前命名空间。
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    /// 这个客户端本身，给同一进程里**别的**集群内读数复用。
    ///
    /// 它是本 crate 唯一一份配过集群 CA 的客户端，再建一个就是第二份设置；
    /// 各读数要的期限不同（轮询的读数只有几秒，apiserver 的往返走默认），
    /// 所以期限挂在请求上，而不是每个读数各建一个客户端。
    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// 本进程**能不能** GET 该 apiserver 路径——一次能力探测，不取数据。
    ///
    /// 用来回答「这份资源是不是本进程的活」：同一份资源在同一个二进制里被多个
    /// deployment 共享，能碰到它的只有一个（凭 ServiceAccount 的角色绑定）。判据
    /// 必须是 apiserver 自己的权限判决，不能是「集群里存在这份资源」这种每个进程
    /// 都成立的事实。
    pub(crate) async fn can_get(&self, path: &str) -> Result<bool, String> {
        let resp = self
            .http
            .get(format!("{API_BASE}{path}"))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("请求 apiserver 失败: {e}"))?;
        Ok(!apiserver_denies_ownership(resp.status().as_u16()))
    }

    /// POST one object, returning what the apiserver made of it.
    ///
    /// The body comes back rather than being discarded: a create answers with
    /// the object it created, and the things that matter about it (the name
    /// the apiserver settled on, the uid a later read or delete has to name)
    /// exist only in that answer.
    pub(crate) async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let resp = self
            .http
            .post(format!("{API_BASE}{path}"))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("请求 apiserver 失败: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("apiserver 返回 {status}: {text}"));
        }
        serde_json::from_str(&text).map_err(|e| format!("apiserver 响应解析失败: {e}"))
    }

    pub(crate) async fn patch(&self, path: &str, body: serde_json::Value) -> Result<(), String> {
        let resp = self
            .http
            .patch(format!("{API_BASE}{path}"))
            .bearer_auth(&self.token)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/strategic-merge-patch+json",
            )
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("请求 apiserver 失败: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("apiserver 返回 {status}: {text}"));
        }
        Ok(())
    }

    /// 池的声明面：池里有几家、声明的冗余下限是几、以及这个值是从哪来的。
    ///
    /// 三个数一起给，因为「界」这个读数的全部意义就在这三个数的关系上：只有池的
    /// 大小看不出该在哪停下来，只有下限看不出它是不是一句被写下来的话。**没有声明**
    /// 与「声明为 1」在这一面必须分得开——两者算出的界相同，而一个运营者看得出来
    /// 的区别是：前者是他还没决定的，后者是他已经决定过的。
    ///
    /// `min_healthy_in_force` 是那份声明生效时的界，按 `cog_core::pool_floor` 一处
    /// 的定义算出来（含夹取），不是这一面自己再算一遍——两面各夹一次就会出现"页面
    /// 上说 3、实际按 4 判"这种没有一处报错的漂移。
    ///
    /// 读不到 Secret 时返回 `Ok(None)` 而不是一个默认值：这一面无话可说与「池里
    /// 一家都没有」是两件事，把前者印成后者等于替一次读失败编一个池。
    pub(crate) async fn secret_pool_policy(&self) -> Result<Option<serde_json::Value>, String> {
        let resp = self
            .http
            .get(format!(
                "{API_BASE}{}",
                crate::secret_layout::oob_secret_api_path(&self.namespace)
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("读取 secret 失败: {e}"))?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let secret: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("secret 响应解析失败: {e}"))?;
        let Some(data) = secret.get("data") else {
            return Ok(None);
        };
        let decode = |key: &str| -> Option<String> {
            let raw = data.get(key)?.as_str()?;
            use base64::Engine as _;
            let bytes = base64::engine::general_purpose::STANDARD.decode(raw).ok()?;
            String::from_utf8(bytes).ok()
        };
        let Some(upstreams_raw) = decode(SECRET_UPSTREAMS_KEY) else {
            // 池那个键都读不出来（没有、或不是我们写的形状）：这一面无话可说。
            return Ok(None);
        };
        let upstreams = serde_json::from_str::<Vec<serde_json::Value>>(&upstreams_raw)
            .map(|list| list.len())
            .unwrap_or(0);
        // 同一条语法由判定那一路定义：这一面若自己再写一遍"trim、非空、大于 0"，
        // 两处就会漂——配置面读得出一个界、判定面读不出，而两边都自报成功。
        let declared = decode(SECRET_MIN_HEALTHY_KEY)
            .as_deref()
            .and_then(crate::security_gateway::parse_min_healthy);
        Ok(Some(pool_policy_json(upstreams, declared)))
    }

    /// 滚动重启安全网关：上游池整体在 Secret 里（env 引用 Secret 条目，
    /// 内容变化不会触发重启），restartedAt 注解保证每次保存都滚出新 Pod
    /// 读取新池。贡献通道 token 同理（env 从 Secret 注入），保存后同样
    /// 需要滚动重启才能生效。
    pub(crate) async fn restart_gateway(&self) -> Result<(), String> {
        let restarted_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default();
        self.patch(
            &format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}",
                self.namespace, GATEWAY_DEPLOYMENT
            ),
            json!({
                "spec": {"template": {
                    "metadata": {"annotations": {"cogneva.io/restartedAt": restarted_at}},
                }}
            }),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apiserver_denies_ownership, classify_probe, classify_temperature_probe,
        classify_usage_probe, detect_api_style, protocol_probe_url, ApiStyleProbe, ProbeOutcome,
    };

    /// 向导里每个预设的 base_url，拼出来必须是各家文档给出的那个端点。
    ///
    /// 这条判据盯的是**地址的形状**：平台只往后接最后一段，版本段因此属于
    /// base_url。少一段的写法能过旧探测（它自己补 `/v1`）却在运行时 404，
    /// 读起来像密钥不对。预设值改错、或者有人把拼法改成别的形状，这里当场红。
    #[test]
    fn each_vendor_preset_composes_the_endpoint_that_vendor_documents() {
        for (style, base_url, want) in [
            // DeepSeek 文档给的 base_url 不带 `/v1`，拼出来的就是文档示例打到的那一个。
            (
                "openai",
                "https://api.deepseek.com",
                "https://api.deepseek.com/chat/completions",
            ),
            (
                "openai",
                "https://api.openai.com/v1",
                "https://api.openai.com/v1/chat/completions",
            ),
            (
                "anthropic",
                "https://api.anthropic.com/v1",
                "https://api.anthropic.com/v1/messages",
            ),
            // Anthropic 协议的第三方面（DeepSeek 的 Anthropic 兼容端点）。
            (
                "anthropic",
                "https://api.deepseek.com/anthropic/v1",
                "https://api.deepseek.com/anthropic/v1/messages",
            ),
            (
                "openai",
                "https://api.kimi.com/coding/v1",
                "https://api.kimi.com/coding/v1/chat/completions",
            ),
            (
                "openai",
                "https://dashscope.aliyuncs.com/compatible-mode/v1",
                "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
            ),
            (
                "openai",
                "https://open.bigmodel.cn/api/paas/v4",
                "https://open.bigmodel.cn/api/paas/v4/chat/completions",
            ),
            (
                "openai",
                "https://api.minimaxi.com/v1",
                "https://api.minimaxi.com/v1/chat/completions",
            ),
            (
                "openai",
                "https://ark.cn-beijing.volces.com/api/v3",
                "https://ark.cn-beijing.volces.com/api/v3/chat/completions",
            ),
        ] {
            assert_eq!(protocol_probe_url(style, base_url), want, "{base_url}");
        }
    }

    /// A vendor that answers on `/responses` is admitted on that face, and a
    /// vendor that only answers chat completions is admitted on the older one.
    ///
    /// The Responses face is the preference, not an assumption: asking it first
    /// must not deny the chat face to a vendor that has not implemented it, and
    /// answering it must not be recorded as anything the vendor proved. Each
    /// stub serves exactly one path, which is how the two outcomes are told
    /// apart here.
    #[tokio::test]
    async fn the_admitted_face_is_the_one_the_vendor_answered_on() {
        async fn spawn_stub(served: &'static [&'static str]) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = axum::Router::new().fallback(axum::routing::any(
                move |uri: axum::http::Uri| async move {
                    if served.contains(&uri.path()) {
                        (axum::http::StatusCode::OK, "{}")
                    } else {
                        (axum::http::StatusCode::NOT_FOUND, "{}")
                    }
                },
            ));
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            format!("http://{addr}")
        }

        for (served, want) in [
            (&["/v1/responses"][..], cog_llm::responses::API_STYLE),
            (&["/v1/chat/completions"][..], "openai"),
            (
                &["/v1/responses", "/v1/chat/completions"][..],
                cog_llm::responses::API_STYLE,
            ),
        ] {
            let base = spawn_stub(served).await;
            match detect_api_style(&format!("{base}/v1"), "m", "k", None).await {
                ApiStyleProbe::Verified(style) => {
                    assert_eq!(style, want, "stub served {served:?}")
                }
                other => panic!("stub served {served:?}, expected {want}: {other:?}"),
            }
        }
    }

    /// 末尾带斜杠是同一把钥匙：不许因此拼出 `//messages`。
    #[test]
    fn a_trailing_slash_does_not_change_the_address() {
        assert_eq!(
            protocol_probe_url("anthropic", "https://api.anthropic.com/v1/"),
            protocol_probe_url("anthropic", "https://api.anthropic.com/v1")
        );
    }

    #[test]
    fn probe_outcome_follows_the_upstream_answer_not_a_status_table() {
        // 2xx 是当场通了。
        for status in [200, 201, 204] {
            assert_eq!(classify_probe(status), ProbeOutcome::Accepted);
        }
        // 404 是「这个协议面上没有该端点」，换另一个协议面再试。
        assert_eq!(classify_probe(404), ProbeOutcome::NoSuchEndpoint);
        // 其余一律是「上游明确应答但拒服」：配额各家用不同码
        // （429/402/403/451），鉴权与限流也在其中，按码表判必然漏。
        // 这类拒服要入池（运行时会转移），不是「这条不该在池里」。
        for status in [400, 401, 402, 403, 409, 422, 429, 451, 500, 503] {
            assert_eq!(
                classify_probe(status),
                ProbeOutcome::Refused,
                "status {status} must be read as a refusal, not as a dead endpoint"
            );
        }
    }

    #[test]
    fn only_permission_verdicts_deny_ownership() {
        // 权限与凭证的判决：明确定性为「不是本进程的活」。
        assert!(apiserver_denies_ownership(401));
        assert!(apiserver_denies_ownership(403));
        // 其余一律不算：资源还没建、apiserver 侧的故障、限流，都不能把属主
        // 判成非属主——那会让刷新任务在属主进程里被静默停掉。
        for status in [200, 201, 404, 409, 429, 500, 503] {
            assert!(
                !apiserver_denies_ownership(status),
                "status {status} must not be read as a permission verdict"
            );
        }
    }

    #[test]
    fn temperature_probe_reads_the_upstream_verdict() {
        // 通了就是"任意温度都行"。
        assert_eq!(classify_temperature_probe(200, "{}"), Some(false));
        // 上游点名 temperature 拒绝：只认 1。
        assert_eq!(
            classify_temperature_probe(
                400,
                r#"{"error":{"message":"invalid temperature: only 1 is allowed for this model"}}"#
            ),
            Some(true)
        );
        // 配额/鉴权/服务端故障与温度无关：不能读成"只认 1"，否则所有调用方
        // 的温度会被静默改写；也不能读成"任意值都行"，那会把约束漏掉。
        assert_eq!(
            classify_temperature_probe(
                429,
                r#"{"error":{"code":"AccountQuotaExceeded","message":"monthly quota"}}"#
            ),
            None
        );
        assert_eq!(
            classify_temperature_probe(403, r#"{"error":{"type":"access_terminated_error"}}"#),
            None
        );
        assert_eq!(
            classify_temperature_probe(401, r#"{"error":{"message":"invalid api key"}}"#),
            None
        );
        assert_eq!(
            classify_temperature_probe(400, r#"{"error":{"message":"invalid request body"}}"#),
            None
        );
    }

    #[test]
    fn usage_probe_reads_whether_the_stream_carries_a_usage_tail() {
        // 2xx 且流里出现 usage 尾帧：这台上游认这个字段、也确实报用量。
        assert_eq!(
            classify_usage_probe(
                200,
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                 data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n\
                 data: [DONE]\n\n"
            ),
            Some(true)
        );
        // 2xx 但流里没有 usage：字段被接受、却换不回来用量。这时剥与不剥都不
        // 会有用量，判"不会报"是安全方向——它只会让网关保持现状。
        assert_eq!(
            classify_usage_probe(
                200,
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
            ),
            Some(false)
        );
        // 2xx 带 usage 但整块体不是流：上游没按流式回，它答的不是"流里会不会
        // 有 usage"。读成"支持"等于用一个不流式的应答去断言流式行为。
        assert_eq!(
            classify_usage_probe(
                200,
                r#"{"choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#
            ),
            None
        );
        // 上游明确点名这个字段拒绝：剥掉它是保护性的。
        assert_eq!(
            classify_usage_probe(
                400,
                r#"{"error":{"message":"unknown field stream_options","type":"invalid_request_error"}}"#
            ),
            Some(false)
        );
        assert_eq!(
            classify_usage_probe(
                400,
                r#"{"error":{"message":"include_usage is not supported"}}"#
            ),
            Some(false)
        );
        // 与这个字段无关的拒服一律不写结论：读成"支持"会让每次调用都带一个
        // 上游可能不认的字段；读成"不支持"则会把"其实支持"永久钉死。
        assert_eq!(
            classify_usage_probe(
                429,
                r#"{"error":{"code":"AccountQuotaExceeded","message":"monthly quota"}}"#
            ),
            None
        );
        assert_eq!(
            classify_usage_probe(403, r#"{"error":{"type":"access_terminated_error"}}"#),
            None
        );
        assert_eq!(
            classify_usage_probe(401, r#"{"error":{"message":"invalid api key"}}"#),
            None
        );
        assert_eq!(
            classify_usage_probe(400, r#"{"error":{"message":"invalid request body"}}"#),
            None
        );
    }
}
