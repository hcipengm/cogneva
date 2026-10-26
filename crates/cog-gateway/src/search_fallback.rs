//! 检索后备的池子：**一个槽**换成**一条有序列表**。
//!
//! 单槽的问题是它在最需要后备的那一刻自己变成一个没有后备的部署：免费档
//! 有额度，额度会在「自建检索全沉默、每条查询都落到这里」的那些天里用完，
//! 而用完之后的形状与「没有配置后备」一模一样——都是没有答案。把几家排在
//! 一起并不能保证永远有答案（它们各自也会挂），但它把「同时用完」从默认
//! 情形变成需要巧合的情形。
//!
//! 形状与 `llm-upstreams` 同构，理由也一样：加一家上游应当是一次配置写入，
//! 不是一次代码改动。LLM 那侧已经这么做了，池条目住在一个 JSON 环境变量里，
//! 由带外片注入；这里照抄那个形状，于是「换索引」不需要重新发版。
//!
//! 三件事在下面分开写，好让每一件都能被单独断言：
//!   - 解析：哪些条目算数、顺序是什么（[`parse_upstreams`]）；
//!   - 请求：往哪台主机、带什么认证头、请求体是什么（[`build_request`]）；
//!   - 应答：怎么把它折成与自建那层相同的 `results` 形状（[`parse_response`]）。
//!
//! **应答解析宁可报错也不许返回空**：这个模块存在的前提是"沉默"和"没有
//! 命中"是两个不同的答案，所以凡是它读不懂的应答——换了一版信封、返回了
//! `error` 键、结果字段是 null——都必须变成一条点名了原始应答的错误，而不是
//! 一条空列表。空列表是这条链路上唯一会被读成"网上确实没有"的形状。

use serde_json::Value;

/// 池子所在的环境变量。它住的还是带外片——与代码平台的 token 同一个对象、
/// 同一个理由：业务 Pod 零持有，能发出凭证的只有网关这一个进程。
pub const SEARCH_FALLBACK_UPSTREAMS_ENV: &str = "COGNEVA_SG_SEARCH_FALLBACK_UPSTREAMS";

/// 一家后备索引的**应答形状**。
///
/// 名字是厂商名，因为形状本身就是厂商给的：同一个厂商的接口换了版本，形状
/// 会跟着换，而"某某家的搜索接口"没有别的准确名字。它出现在代码里是应该的，
/// 出现在部署清单里不是——清单侧用的是厂商中立的 `/search-fallback` 与
/// `search-fallback-key`，所以换一家不需要动任何一份清单。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResponseShape {
    /// `{"results": [{"title", "url", "content"}]}`
    Tavily,
    /// `{"results": [{"title", "url", "text"}]}`
    Exa,
    /// 百度千帆：`{"references": [...]}` 或 `{"search_results": [...]}`。
    Qianfan,
}

impl ResponseShape {
    pub fn as_str(self) -> &'static str {
        match self {
            ResponseShape::Tavily => "tavily",
            ResponseShape::Exa => "exa",
            ResponseShape::Qianfan => "qianfan",
        }
    }

    /// 没见过的形状返回 `None`，由调用方丢弃**这一个条目**而不是整份配置。
    ///
    /// 丢弃一条与丢弃整份的差别在于可见性：只丢一条时其余几家照常工作，
    /// 而那条被丢的会在日志里点名；整份丢掉则表现为"池子空了"，那是另一个
    /// 已经合法且更难分辨的状态（这个部署没配后备）。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "tavily" => Some(ResponseShape::Tavily),
            "exa" => Some(ResponseShape::Exa),
            "qianfan" => Some(ResponseShape::Qianfan),
            _ => None,
        }
    }
}

/// 池子里的一家里条目。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchUpstream {
    /// 报给调用方与日志的名字。调用方从 `source` 读到的是它，所以它必须
    /// 是写配置的人认得的东西；没有单独写时用形状名，因为那时池子里
    /// 通常只有这一家——两家的形状名相同才会撞，解析时会把撞的那条丢掉。
    pub id: String,
    pub shape: ResponseShape,
    /// 根地址，不含 `/search` 之类的路径：路径由形状决定，不由配置决定。
    pub base_url: String,
    /// 空串是一个**合法的、有意义的**取值：这家走 keyless 档。
    pub api_key: String,
}

impl SearchUpstream {
    fn keyless(&self) -> bool {
        self.api_key.trim().is_empty()
    }
}

/// 读环境变量。解析失败或没配都读成空池——空池是合法部署形态：这个部署
/// 选择让查询不出自己这棵树。
pub fn upstreams_from_env() -> Vec<SearchUpstream> {
    match std::env::var(SEARCH_FALLBACK_UPSTREAMS_ENV) {
        Ok(raw) => parse_upstreams(&raw),
        Err(_) => Vec::new(),
    }
}

/// 解析池子配置。顺序即询问顺序：写在前面的先问，所以顺序是配置的一部分，
/// 不能排序、不能去重后重排。
///
/// 丢弃规则只有两条，且都要在日志里点名：缺 `base_url`（没有主机可问）、
/// 形状不认识（不知道问什么路径）。缺 `api_key` **不是**丢弃理由。
pub fn parse_upstreams(raw: &str) -> Vec<SearchUpstream> {
    let Ok(list) = serde_json::from_str::<Vec<Value>>(raw) else {
        tracing::warn!("{SEARCH_FALLBACK_UPSTREAMS_ENV} 不是合法 JSON 数组，按未配置处理");
        return Vec::new();
    };
    let mut out: Vec<SearchUpstream> = Vec::with_capacity(list.len());
    for value in list.iter() {
        let text = |k: &str| {
            value
                .get(k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string()
        };
        let base_url = text("base_url");
        let shape_raw = text("shape");
        let Some(shape) = ResponseShape::parse(&shape_raw) else {
            tracing::warn!(
                base_url = %base_url,
                shape = %shape_raw,
                "后备索引条目的形状不认识，已丢弃这一条"
            );
            continue;
        };
        if base_url.is_empty() {
            tracing::warn!(
                shape = shape.as_str(),
                "后备索引条目没有 base_url，已丢弃这一条"
            );
            continue;
        }
        let declared_id = text("id");
        let id = if declared_id.is_empty() {
            shape.as_str().to_string()
        } else {
            declared_id
        };
        if out.iter().any(|u| u.id == id) {
            // 同名两条会让 `source` 读不出是哪一家应答的，而那个字段的全部
            // 用处就是回答这个。丢后面那条并点名，比悄悄合并或悄悄覆盖好。
            tracing::warn!(id = %id, "后备索引条目重名，已丢弃后出现的那一条");
            continue;
        }
        out.push(SearchUpstream {
            id,
            shape,
            base_url,
            api_key: text("api_key"),
        });
    }
    out
}

/// 一次询问的形状：发到哪、带什么头、请求体是什么。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Value,
}

impl SearchRequest {
    /// 认证头里有没有这一条，测试与调用面都用它读，不靠翻 headers 数组。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// 把一次查询折成某一家要的形状。
///
/// 路径是常量而不是拼出来的：调用方贡献的是一个**查询词**，不是一段路径，
/// 所以没有任何输入能改变这次请求发往哪台主机。
pub fn build_request(upstream: &SearchUpstream, query: &str, limit: u64) -> SearchRequest {
    let base = upstream.base_url.trim_end_matches('/');
    let mut headers = vec![
        ("content-type".to_string(), "application/json".to_string()),
        ("accept".to_string(), "application/json".to_string()),
    ];
    let (path, body) = match upstream.shape {
        ResponseShape::Tavily => (
            "/search",
            serde_json::json!({
                "query": query,
                // 深度选 basic：后备要快，深度检索是主路径的活。
                "search_depth": "basic",
                "max_results": limit,
            }),
        ),
        ResponseShape::Exa => (
            "/search",
            serde_json::json!({
                "query": query,
                "numResults": limit,
                // 只要一句摘要，不要全文：正文由本地的 `web_fetch` 去取，那一步
                // 有地址守卫，而这里每多要一页正文就多一份不经过守卫的字节。
                "contents": { "summary": true },
            }),
        ),
        ResponseShape::Qianfan => (
            "/ai_search",
            serde_json::json!({
                "messages": [{ "role": "user", "content": query }],
                "stream": false,
                "resource_type_filter": [{ "type": "web", "top_k": limit }],
            }),
        ),
    };
    // 没有 key 就不发认证头，而不是发一个空串：空串在有些上游那里是 401，
    // 在另一些那里是"匿名"，而同一份配置在这两处的表现不该不同。
    if !upstream.keyless() {
        let key = upstream.api_key.trim();
        match upstream.shape {
            ResponseShape::Tavily | ResponseShape::Qianfan => {
                headers.push(("authorization".to_string(), format!("Bearer {key}")));
            }
            ResponseShape::Exa => {
                headers.push(("x-api-key".to_string(), key.to_string()));
            }
        }
    }
    SearchRequest {
        url: format!("{base}{path}"),
        headers,
        body,
    }
}

/// 一条折好后的命中，与自建那层 `results` 的字段同名。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchHit {
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub published: Option<String>,
}

/// 一家应答的结局。三种，刻意分开：
///   - `Hits`：问到了东西；
///   - `Empty`：这一家**明确地**答了"没有命中"（这个形状存在，且列表是空的）；
///   - `Broken`：应答读不懂、报错、或缺字段——这一家没有给出任何读数。
///
/// 区分 `Empty` 与 `Broken` 是这一整条链路的要点：前者可以说"网上没有"，
/// 后者只能说"这一家没答"。把它们合并成一个空列表，就是把这次要消除的
/// 混淆又接回池子里。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    Hits(Vec<SearchHit>),
    Empty,
    Broken(String),
}

/// 把一家的原始应答折成上面那三种结局之一。
pub fn parse_response(shape: ResponseShape, raw: &Value, limit: u64) -> Answer {
    // 能读出来的错误先读：多数上游在限额与鉴权失败时返回 200 + 一个错误体，
    // 于是"没看错误键"的表现是"这一家没有命中"，而不是"这一家拒绝了"。
    if let Some(message) = error_message(raw) {
        return Answer::Broken(message);
    }
    let entries = match shape {
        ResponseShape::Tavily | ResponseShape::Exa => raw.get("results").and_then(|v| v.as_array()),
        ResponseShape::Qianfan => raw
            .get("search_results")
            .and_then(|v| v.as_array())
            .or_else(|| raw.get("references").and_then(|v| v.as_array())),
    };
    let Some(entries) = entries else {
        return Answer::Broken(format!(
            "应答里没有结果字段（{} 形状要 results 或 search_results/references），\
             原文开头：{}",
            shape.as_str(),
            head_of(raw)
        ));
    };
    let hits: Vec<SearchHit> = entries
        .iter()
        .filter_map(|entry| {
            let text = |k: &str| {
                entry
                    .get(k)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string()
            };
            let url = text("url");
            // 没有链接的条目没有用处：调用方拿它做不了任何事，而它会占掉
            // 一个名额，让真正能读的那一条挤进来。
            if url.is_empty() {
                return None;
            }
            let snippet = first_non_empty(&[
                text("content"),
                text("text"),
                text("summary"),
                text("snippet"),
                text("description"),
            ]);
            Some(SearchHit {
                title: text("title"),
                url,
                snippet: snippet.unwrap_or_default(),
                published: first_non_empty(&[
                    text("published_date"),
                    text("publishedDate"),
                    text("date"),
                ]),
            })
        })
        .take(usize::try_from(limit).unwrap_or(usize::MAX))
        .collect();
    if hits.is_empty() {
        // 到了这里，「条目全都没有链接」与「列表本来就是空的」都会落成 Empty，
        // 这是这两件里唯一可以合并的一处：两种情况下这一家都没有给出可用的东西，
        // 而池子会继续问下一家。
        Answer::Empty
    } else {
        Answer::Hits(hits)
    }
}

/// 上游的报错键。三家用的名字不同，所以按名字找不到就再看一眼有没有
/// `error` 键——它的值可能是字符串，也可能是一个对象，两种都取出来。
fn error_message(raw: &Value) -> Option<String> {
    for key in ["error", "detail", "message", "errmsg", "msg"] {
        let Some(value) = raw.get(key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        // `message` 在正常应答里常常是 `"ok"` 一类的东西，所以只有它在**没有
        // 结果字段**时才算错误；其余几个键出现即报错。这一条判断放在下面
        // 由调用顺序保证：先看结果字段有没有，有就不把 message 当错误。
        if key == "message" && raw.get("results").is_some() {
            continue;
        }
        let rendered = match value {
            Value::String(s) => s.trim().to_string(),
            other => other.to_string(),
        };
        if !rendered.is_empty() && rendered != "ok" && rendered != "success" {
            return Some(format!("上游报错（{key}）：{rendered}"));
        }
    }
    None
}

fn first_non_empty(values: &[String]) -> Option<String> {
    values.iter().find(|v| !v.is_empty()).cloned()
}

/// 错误信息里带上原始应答的开头：读不懂一个信封时，下一个读懂它的人
/// 需要它。长度上限按字符截，不按字节切——按字节切会把一个多字节字符
/// 切成两半，而那段字节会进日志。
fn head_of(raw: &Value) -> String {
    let text = raw.to_string();
    text.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(shape: ResponseShape, key: &str) -> SearchUpstream {
        SearchUpstream {
            id: shape.as_str().to_string(),
            shape,
            base_url: "http://gateway:8081/search-fallback".to_string(),
            api_key: key.to_string(),
        }
    }

    #[test]
    fn a_keyless_entry_sends_no_credential_at_all() {
        let request = build_request(&upstream(ResponseShape::Tavily, ""), "qdrant", 5);
        assert_eq!(request.header("authorization"), None);
        assert_eq!(request.header("x-api-key"), None);
    }

    #[test]
    fn each_shape_carries_its_key_in_the_header_that_shape_reads() {
        let tavily = build_request(&upstream(ResponseShape::Tavily, "k1"), "q", 5);
        assert_eq!(tavily.header("authorization"), Some("Bearer k1"));
        let exa = build_request(&upstream(ResponseShape::Exa, "k2"), "q", 5);
        assert_eq!(exa.header("x-api-key"), Some("k2"));
        assert_eq!(exa.header("authorization"), None);
        let qianfan = build_request(&upstream(ResponseShape::Qianfan, "k3"), "q", 5);
        assert_eq!(qianfan.header("authorization"), Some("Bearer k3"));
    }

    #[test]
    fn the_caller_contributes_a_query_and_never_a_path() {
        let request = build_request(&upstream(ResponseShape::Tavily, "k"), "../../admin", 5);
        assert_eq!(request.url, "http://gateway:8081/search-fallback/search");
        assert_eq!(request.body["query"], "../../admin");
    }

    #[test]
    fn the_order_written_is_the_order_asked() {
        let raw = r#"[
            {"shape":"exa","base_url":"http://a","api_key":"x","id":"first"},
            {"shape":"tavily","base_url":"http://b","id":"second"},
            {"shape":"qianfan","base_url":"http://c","api_key":"z"}
        ]"#;
        let pool = parse_upstreams(raw);
        let ids: Vec<&str> = pool.iter().map(|u| u.id.as_str()).collect();
        assert_eq!(ids, ["first", "second", "qianfan"]);
        // 中间那家没写 key：它仍然在池子里，只是走 keyless 档。
        assert!(pool[1].keyless());
    }

    #[test]
    fn a_broken_entry_is_dropped_by_name_and_the_rest_still_answer() {
        let raw = r#"[
            {"shape":"serper","base_url":"http://a","api_key":"x"},
            {"shape":"exa"},
            {"shape":"exa","base_url":"http://b","api_key":"y","id":"good"},
            {"shape":"exa","base_url":"http://c","api_key":"z","id":"good"}
        ]"#;
        let pool = parse_upstreams(raw);
        // 形状不认识、没有 base_url、重名——三条各丢一条，剩下一条。
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].id, "good");
        assert_eq!(pool[0].base_url, "http://b");
    }

    #[test]
    fn exa_and_qianfan_answers_fold_into_the_self_hosted_result_shape() {
        let exa = serde_json::json!({
            "results": [
                {"title":"One","url":"https://e/1","text":"body one","publishedDate":"2026-01-02"},
                {"title":"No link","text":"dropped"}
            ]
        });
        match parse_response(ResponseShape::Exa, &exa, 10) {
            Answer::Hits(hits) => {
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].url, "https://e/1");
                assert_eq!(hits[0].snippet, "body one");
                assert_eq!(hits[0].published.as_deref(), Some("2026-01-02"));
            }
            other => panic!("expected hits, got {other:?}"),
        }
        let qianfan = serde_json::json!({
            "search_results": [
                {"title":"Two","url":"https://q/2","content":"body two"}
            ]
        });
        match parse_response(ResponseShape::Qianfan, &qianfan, 10) {
            Answer::Hits(hits) => assert_eq!(hits[0].url, "https://q/2"),
            other => panic!("expected hits, got {other:?}"),
        }
    }

    #[test]
    fn an_envelope_this_code_cannot_read_is_broken_not_empty() {
        // 换了一版信封、或者代理换了个中间件：读不懂必须报错并带上原文。
        let unknown = serde_json::json!({ "webPages": { "value": [] } });
        match parse_response(ResponseShape::Tavily, &unknown, 10) {
            Answer::Broken(reason) => {
                assert!(reason.contains("results"), "{reason}");
                assert!(reason.contains("webPages"), "{reason}");
            }
            other => panic!("an unreadable envelope must not read as no matches: {other:?}"),
        }
    }

    #[test]
    fn an_error_body_is_not_a_reading_of_the_web() {
        // 限额与鉴权失败常常是 200 + 错误体；不读错误键就会读成"没有命中"。
        let quota = serde_json::json!({
            "error": "You have exceeded your monthly quota",
            "results": []
        });
        match parse_response(ResponseShape::Tavily, &quota, 10) {
            Answer::Broken(reason) => assert!(reason.contains("quota"), "{reason}"),
            other => panic!("a quota error must not read as no matches: {other:?}"),
        }
    }

    #[test]
    fn an_answer_that_really_says_nothing_is_empty_and_says_so() {
        let nothing = serde_json::json!({ "results": [] });
        assert_eq!(
            parse_response(ResponseShape::Tavily, &nothing, 10),
            Answer::Empty
        );
    }
}
