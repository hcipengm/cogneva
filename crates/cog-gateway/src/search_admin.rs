//! 检索/抓取后备池的设置面：前端填的 key 从这里落盘，网关滚动自己生效。
//!
//! 池的形状与 `llm-upstreams` 同构（一个 JSON 数组住在带外片），所以读写的形状
//! 也照抄那份：写成功之后改 `restartedAt` 注解滚出新的网关 Pod，新 Pod 启动时
//! 从 Secret 读到新池。**生效的判据是新 Pod**，不是这次 HTTP 返回 200——env
//! 引用 Secret 条目的部署里，改内容不会触发滚动，所以滚动是这一步的一部分。
//!
//! 两条不做的事：
//!   - **不回显凭证**：读面只给 `has_key`，写面缺席 `api_key` 就是"保留原来那把"。
//!     前端若把读到的值再写回来，那它就成了凭证的第二份副本，而副本会在轮换后
//!     变成 401——那会被读成"这家挂了"。
//!   - **不自己翻译条目**：写面把候选条目序列化之后用运行期同一个解析器
//!     （[`crate::search_fallback::parse_upstreams`]）过一遍，条目数对不上就整单
//!     拒绝。两边各写一套判据的话，设置页会保存下一份运行期要丢掉一半的配置，
//!     而两个界面都自报成功。

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::llm_admin::KubeClient;
use crate::search_fallback::{from_live_llm, parse_upstreams, Capability, ResponseShape};

/// 池所在的那个带外键。与 `secret_layout::OOB_SECRET_KEYS` 同一张表，两边都要写
/// 是因为一侧在编译期分派、一侧在运行期读写，而漏写一侧的表现是"页面保存成功、
/// 网关重启后池是空的"。
const SECRET_KEY: &str = "search-fallback-upstreams";

/// 现网 LLM 上游那一份配置的环境变量。读它只为在页面上把"跟着 LLM 走"的两层
/// 显示出来——它们的值不在这里，也写不进来（见 [`crate::search_fallback::from_live_llm`]）。
const LLM_UPSTREAMS_ENV: &str = "COGNEVA_LLM_UPSTREAMS";

#[derive(Debug, Deserialize)]
pub struct SearchUpstreamRequest {
    /// 报给调用方与日志的名字。缺省取形状名；重名整单拒绝。
    pub id: Option<String>,
    /// 应答形状：tavily / exa / qianfan。deepseek / kimi 会被拒——那两家跟着
    /// 现网在用的 LLM 走，不经过这份配置。
    pub shape: String,
    pub base_url: String,
    /// **缺席 = 保留这一条原来那把**；空串 = 明确改成不带凭证。
    ///
    /// 这个区分是这条 API 的全部要点：页面读回来的 entry 不带 key，用户只改顺序
    /// 或地址时前端原样提交，缺席那一条的凭证必须原封不动地活下来。
    pub api_key: Option<String>,
    /// `search` / `fetch` / `both`。缺省 `search`：抓取按 URL 另计费。
    pub capability: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SearchFallbackRequest {
    pub upstreams: Vec<SearchUpstreamRequest>,
}

/// GET /api/v1/admin/search-fallback — 池里有哪几条、哪几条是从现网 LLM 折出来的。
///
/// 读的是集群里那份 Secret，不是本进程的环境变量：本进程的环境是**上一次滚动时**
/// 的快照，刚保存完还没滚时它会落后于页面，而页面上显示"已保存但没生效"这种状态
/// 需要一个能读出"文件里现在是什么"的入口。
pub async fn search_fallback_status_handler(
    State(_state): State<Arc<crate::GatewayState>>,
) -> Response {
    let raw = match KubeClient::in_cluster() {
        Ok(kube) => read_secret(&kube).await,
        Err(e) => Err(e),
    };
    let (upstreams, configured, read_error) = match raw {
        Ok(Some(text)) => {
            let parsed = parse_upstreams(&text);
            let declared = serde_json::from_str::<Vec<Value>>(&text)
                .map(|list| list.len())
                .unwrap_or(parsed.len());
            let entries = parsed
                .iter()
                .map(|u| {
                    json!({
                        "id": u.id,
                        "shape": u.shape.as_str(),
                        "base_url": u.base_url,
                        "capability": u.capability.as_str(),
                        "has_key": !u.keyless(),
                    })
                })
                .collect::<Vec<_>>();
            (
                entries,
                true,
                if declared > parsed.len() {
                    // 落盘的那份里有运行期会丢掉的条目：不在这里复述原因（解析器
                    // 已经按条名点过日志），只把"存了几条、能用几条"摆出来。
                    Some(format!(
                        "落盘的配置有 {declared} 条，其中 {} 条能用；其余的会被网关丢弃并在日志里点名",
                        parsed.len()
                    ))
                } else {
                    None
                },
            )
        }
        Ok(None) => (Vec::new(), false, None),
        Err(e) => (Vec::new(), false, Some(e)),
    };
    (
        StatusCode::OK,
        Json(json!({
            "configured": configured,
            "upstreams": upstreams,
            // 这两层不是配置出来的，页面只展示、不可编辑：它们跟着现网在用的
            // LLM（同一台主机、同一把 key、同一个轮换）。
            "from_llm": llm_shaped_entries(),
            "read_error": read_error,
            // 页面能选的两列都从枚举自己的表里取：写面收哪些形状 / 哪些能力由
            // `ResponseShape::parse` 与 `Capability::parse` 决定，回包里手抄一份
            // 就等于给同一件事留了第二个答案，而两个答案只在加分项的那天才对不上。
            "shapes": ResponseShape::CONFIGURABLE
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            "capabilities": Capability::ALL
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>(),
        })),
    )
        .into_response()
}

/// 现网 LLM 池里那些带上网工具的上游，按它们在池里的顺序。
fn llm_shaped_entries() -> Vec<Value> {
    let Ok(raw) = std::env::var(LLM_UPSTREAMS_ENV) else {
        return Vec::new();
    };
    let Ok(list) = serde_json::from_str::<Vec<Value>>(&raw) else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|entry| {
            let text = |k: &str| {
                entry
                    .get(k)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string()
            };
            let (base_url, api_key) = (text("base_url"), text("api_key"));
            // 与运行期同一条准入：缺 key 的上游折不出来（那是一次必然 401 的请求）。
            let derived = from_live_llm(&base_url, &api_key)?;
            Some(json!({
                "id": derived.id,
                "shape": derived.shape.as_str(),
                "base_url": derived.base_url,
                "capability": derived.capability.as_str(),
                "from_llm": true,
            }))
        })
        .collect()
}

/// POST /api/v1/admin/search-fallback — 落盘整份池，滚动网关。
pub async fn search_fallback_config_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<SearchFallbackRequest>,
) -> Response {
    // 现有的那把 key 要在合并时读到：页面不回声，所以缺席的那几条必须从落盘的
    // 那份里取回自己的凭证。读不到落盘那份也没关系——那不是"没有 key"，而是
    // "没有旧值可保留"，两件事的结局都由下面那一行决定。
    let existing = match KubeClient::in_cluster() {
        Ok(kube) => read_secret(&kube).await.ok().flatten(),
        Err(_) => None,
    };
    let existing = existing.as_deref().map(parse_upstreams).unwrap_or_default();

    let merged = match merge_with_existing(&req.upstreams, &existing) {
        Ok(list) => list,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid_request", "message": message })),
            )
                .into_response()
        }
    };

    let raw = match serde_json::to_string(&merged) {
        Ok(text) => text,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "serialize_failed", "message": e.to_string()})),
            )
                .into_response()
        }
    };

    // 存进去的那份必须与运行期读出来的那份逐条相同：解析器丢掉的每一条，都是
    // 用户以为保存了、网关却不认的一条。
    let accepted = parse_upstreams(&raw);
    if accepted.len() != merged.len() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "not_accepted_by_the_runtime",
                "message": format!(
                    "这份配置有 {} 条，网关只会认其中 {} 条；被丢掉的那几条的运行期理由见网关日志",
                    merged.len(),
                    accepted.len()
                ),
            })),
        )
            .into_response();
    }

    if let Err(e) = crate::secret_write::forward(json!({ SECRET_KEY: raw }), false, true).await {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
        )
            .into_response();
    }

    let from_llm = llm_shaped_entries().len();
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "restarted": ["cogneva-security-gateway"],
            "upstreams": accepted.len(),
            "from_llm": from_llm,
            "message": format!(
                "配置已保存，安全网关正在滚动重启，约一分钟后生效；另有 {from_llm} 条跟着现网在用的 LLM 走，不在这次配置里"
            ),
        })),
    )
        .into_response()
}

/// 把请求里的条目并进池里，缺席的 key 从同名的旧条目取回。
///
/// 抽成纯函数，是因为"保留旧 key"这条规则一旦写错，症状是某一家在下一次保存后
/// 拿到 401，而那被读成"这家挂了"——一条只在保存路径上出现的静默故障，没有别处
/// 能把它读出来。
pub(crate) fn merge_with_existing(
    requested: &[SearchUpstreamRequest],
    existing: &[crate::search_fallback::SearchUpstream],
) -> Result<Vec<Value>, String> {
    let mut out: Vec<Value> = Vec::with_capacity(requested.len());
    for item in requested {
        let shape = ResponseShape::parse(&item.shape).ok_or_else(|| {
            format!(
                "形状 {} 不认识（这份配置只收 tavily / exa / qianfan）",
                item.shape
            )
        })?;
        if matches!(shape, ResponseShape::DeepSeek | ResponseShape::Kimi) {
            return Err(format!(
                "{} 的检索与抓取跟着现网在用的那条 LLM 走（同一台主机、同一把 key），\
                 不写进这份配置：写进来就是同一把 key 的第二份副本，轮换之后必然有一份是旧的",
                shape.as_str()
            ));
        }
        let base_url = item.base_url.trim();
        if base_url.is_empty() {
            return Err("base_url 不能为空".to_string());
        }
        if !(base_url.starts_with("https://") || base_url.starts_with("http://")) {
            return Err(format!("{base_url} 不是 http(s):// 开头的完整地址"));
        }
        let capability_raw = item.capability.as_deref().unwrap_or("");
        let capability = Capability::parse(capability_raw).ok_or_else(|| {
            format!(
                "能力写法 {} 不认识（可写 search / fetch / both）",
                capability_raw
            )
        })?;
        let id = item
            .id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| shape.as_str().to_string());
        if out.iter().any(|entry| entry["id"] == json!(id)) {
            return Err(format!(
                "同名两条（{id}）：`source` 读不出是哪一家应答的，请给其中一条改名"
            ));
        }
        let api_key = match item.api_key.as_deref() {
            Some(key) => key.trim().to_string(),
            // 缺席 = 保留：先按 id 找，找不到再按"同一形状 + 同一主机"找——用户
            // 改名不该让它丢掉凭证。
            None => existing
                .iter()
                .find(|u| u.id == id)
                .or_else(|| {
                    existing.iter().find(|u| {
                        u.shape == shape
                            && u.base_url.trim_end_matches('/') == base_url.trim_end_matches('/')
                    })
                })
                .map(|u| u.api_key.clone())
                .unwrap_or_default(),
        };
        out.push(json!({
            "id": id,
            "shape": shape.as_str(),
            "base_url": base_url,
            "api_key": api_key,
            "capability": capability.as_str(),
        }));
    }
    Ok(out)
}

/// 读带外片里的池；键不在就是 `Ok(None)`（空池是合法部署形态）。
async fn read_secret(kube: &KubeClient) -> Result<Option<String>, String> {
    let path = crate::secret_layout::oob_secret_api_path(kube.namespace());
    let secret = kube.get_json(&path).await?;
    let Some(encoded) = secret
        .get("data")
        .and_then(|d| d.get(SECRET_KEY))
        .and_then(|v| v.as_str())
    else {
        return Ok(None);
    };
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| format!("Secret 条目解码失败: {e}"))?;
    let text = String::from_utf8(bytes).map_err(|e| format!("Secret 条目不是 UTF-8: {e}"))?;
    if text.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(text))
}

/// 形状名认不认得，供前端在提交前先问一次——与 [`ResponseShape::parse`] 同一条
/// 判据，所以页面不会在客户端放行一个服务端必然拒绝的值。
pub fn shape_is_accepted(raw: &str) -> bool {
    ResponseShape::parse(raw).is_some_and(|s| ResponseShape::CONFIGURABLE.contains(&s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_fallback::SearchUpstream;

    fn request(
        id: Option<&str>,
        shape: &str,
        base_url: &str,
        api_key: Option<&str>,
        capability: Option<&str>,
    ) -> SearchUpstreamRequest {
        SearchUpstreamRequest {
            id: id.map(str::to_string),
            shape: shape.to_string(),
            base_url: base_url.to_string(),
            api_key: api_key.map(str::to_string),
            capability: capability.map(str::to_string),
        }
    }

    fn stored(id: &str, shape: ResponseShape, base_url: &str, key: &str) -> SearchUpstream {
        SearchUpstream {
            id: id.to_string(),
            shape,
            capability: Capability::Search,
            base_url: base_url.to_string(),
            api_key: key.to_string(),
            key_from: None,
        }
    }

    #[test]
    fn a_save_that_omits_the_key_keeps_the_one_already_stored() {
        // 页面不回声凭证，所以"只改了顺序"的提交带来的条目是不带 key 的。这一条
        // 若丢了 key，下一次调用会 401，而那被读成"这家挂了"。
        let existing = vec![stored(
            "exa",
            ResponseShape::Exa,
            "https://api.exa.ai",
            "k1",
        )];
        let merged = merge_with_existing(
            &[request(
                Some("exa"),
                "exa",
                "https://api.exa.ai",
                None,
                None,
            )],
            &existing,
        )
        .unwrap();
        assert_eq!(merged[0]["api_key"], json!("k1"));
        // 改名也保留：按"同一形状 + 同一主机"兜底。
        let merged = merge_with_existing(
            &[request(
                Some("my-exa"),
                "exa",
                "https://api.exa.ai/",
                None,
                None,
            )],
            &existing,
        )
        .unwrap();
        assert_eq!(merged[0]["api_key"], json!("k1"));
        assert_eq!(merged[0]["id"], json!("my-exa"));
        // 空串是**明确的**"改成不带凭证"，不许被当成缺席去取回旧值。
        let merged = merge_with_existing(
            &[request(
                Some("exa"),
                "exa",
                "https://api.exa.ai",
                Some(""),
                None,
            )],
            &existing,
        )
        .unwrap();
        assert_eq!(merged[0]["api_key"], json!(""));
    }

    #[test]
    fn an_entry_the_runtime_would_drop_is_refused_by_name() {
        // 每条拒绝都要点出是谁：整单报"参数不对"会让用户在一份十条的池里找一个
        // 自己看不见的错字。
        let cases = [
            (
                request(None, "serper", "https://x", Some("k"), None),
                "serper",
            ),
            (
                request(None, "exa", "api.exa.ai", Some("k"), None),
                "api.exa.ai",
            ),
            (
                request(None, "exa", "https://x", Some("k"), Some("crawl")),
                "crawl",
            ),
        ];
        for (case, named) in cases {
            let err = merge_with_existing(&[case], &[]).unwrap_err();
            assert!(err.contains(named), "报错要带上它拒绝的那个值: {err}");
        }
        // 重名是整单拒绝而不是悄悄合并。
        let err = merge_with_existing(
            &[
                request(Some("same"), "exa", "https://a", Some("k"), None),
                request(Some("same"), "tavily", "https://b", Some("k"), None),
            ],
            &[],
        )
        .unwrap_err();
        assert!(err.contains("same"), "{err}");
    }

    #[test]
    fn the_two_vendors_that_follow_the_llm_cannot_be_saved_here() {
        // 存进来就是同一把 key 的第二份副本；轮换之后它 401，读起来像"这家挂了"。
        for shape in ["deepseek", "kimi", "moonshot"] {
            let err = merge_with_existing(
                &[request(
                    None,
                    shape,
                    "https://api.deepseek.com/v1",
                    Some("k"),
                    None,
                )],
                &[],
            )
            .unwrap_err();
            assert!(err.contains("现网在用的"), "{shape}: {err}");
        }
    }

    #[test]
    fn the_saved_pool_is_the_one_the_runtime_reads_back() {
        // 写面与读面之间只有这一个判据：序列化之后用运行期解析器读回来，条目与
        // 主键都得对上。两边各写一套判据时，页面会保存下一份运行期丢一半的配置。
        let merged = merge_with_existing(
            &[
                request(
                    None,
                    "tavily",
                    "https://api.tavily.com",
                    Some("tv"),
                    Some("both"),
                ),
                request(
                    Some("baidu"),
                    "qianfan",
                    "https://qianfan.baidubce.com/v2",
                    None,
                    None,
                ),
            ],
            &[],
        )
        .unwrap();
        let raw = serde_json::to_string(&merged).unwrap();
        let back = parse_upstreams(&raw);
        assert_eq!(back.len(), merged.len());
        assert_eq!(back[0].shape.as_str(), "tavily");
        assert_eq!(back[0].capability.as_str(), "both");
        // 没给 key 的那条照样进池（keyless 是合法档），前端据此显示"未配置 key"。
        assert!(back[1].keyless());
        assert_eq!(back[1].id, "baidu");
    }

    #[test]
    fn the_page_offers_only_shapes_the_pool_can_hold() {
        for shape in ["tavily", "exa", "qianfan"] {
            assert!(shape_is_accepted(shape), "{shape}");
        }
        for shape in ["deepseek", "kimi", "serper", ""] {
            assert!(!shape_is_accepted(shape), "{shape}");
        }
    }

    /// 菜单就是写面能收的那一集，两个方向都要对上。
    ///
    /// 少一格的表现是"能存、页面上选不到"；多一格的表现是"页面上能选、保存被拒"。
    /// 两种都只在用户手里才现形，所以在这里各钉一侧：菜单上的每一项都得真能存进去，
    /// 而写面拒掉的那种拼法不许出现在菜单上。
    #[test]
    fn the_menu_is_exactly_what_the_write_side_accepts() {
        for shape in ResponseShape::CONFIGURABLE {
            for capability in Capability::ALL {
                let saved = merge_with_existing(
                    &[request(
                        None,
                        shape.as_str(),
                        "https://example.test",
                        Some("k"),
                        Some(capability.as_str()),
                    )],
                    &[],
                )
                .unwrap_or_else(|e| panic!("{} + {}: {e}", shape.as_str(), capability.as_str()));
                assert_eq!(saved.len(), 1);
            }
        }
        // 写面拒的那一种拼法不许借菜单混进来：它今天恰好不在表上，这条钉住的是
        // "以后加一项时先想清楚写面收不收"，不是"crawl 这个名字特殊"。
        let err = merge_with_existing(
            &[request(
                None,
                "tavily",
                "https://example.test",
                Some("k"),
                Some("crawl"),
            )],
            &[],
        )
        .unwrap_err();
        assert!(err.contains("crawl"), "{err}");
        assert!(
            !Capability::ALL.iter().any(|c| c.as_str() == "crawl"),
            "写面不收的拼法不能出现在菜单上"
        );
    }
}
