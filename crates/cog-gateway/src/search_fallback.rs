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
//! 池子有两段来源，但它们不是两套机制：
//!   - **配出来的**：第三方检索/抓取服务，key 由人在设置页里填；
//!   - **折出来的**：DeepSeek / Kimi 那种"LLM 自己带上网工具"的，**跟着现网
//!     在用的那条 LLM 上游走**——同一台主机、同一把 key、同一个轮换。它不进
//!     配置，因为进配置就意味着同一把 key 存两份，而轮换后必然有一份是旧的。
//!     见 [`from_live_llm`]。
//!
//! 三件事在下面分开写，好让每一件都能被单独断言：
//!   - 解析：哪些条目算数、顺序是什么（[`parse_upstreams`]）；
//!   - 请求：往哪台主机、带什么认证头、请求体是什么（[`build_request`]、
//!     [`build_fetch_request`]）；
//!   - 应答：怎么把它折成与自建那层相同的形状（[`parse_response`]、
//!     [`parse_fetch_response`]）。
//!
//! 检索与抓取是**两个能力**，不是一件事：它们各自计费、各自挂、各自有
//! 没有接口的家庭成员（千帆只有检索面）。所以每个条目声明它能承接哪一种，
//! 抓取这一侧找不到能接的后备时就如实报错，不拿检索接口去充当。
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
/// `search-fallback-upstreams`，整份池是带外片里的一个值，所以换一家、加一家
/// 都不需要动任何一份清单。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResponseShape {
    /// `{"results": [{"title", "url", "content"}]}`
    Tavily,
    /// `{"results": [{"title", "url", "text"}]}`
    Exa,
    /// 百度千帆：`{"references": [...]}` 或 `{"search_results": [...]}`。
    Qianfan,
    /// DeepSeek 的 `/tools/*`：搜索 `{"results": [...]}`，抓取 `{"title","content"}`。
    DeepSeek,
    /// Kimi 的 `/tools/*`：搜索 `{"search_results": [...]}`，抓取 `{"title","content"}`。
    Kimi,
}

impl ResponseShape {
    pub fn as_str(self) -> &'static str {
        match self {
            ResponseShape::Tavily => "tavily",
            ResponseShape::Exa => "exa",
            ResponseShape::Qianfan => "qianfan",
            ResponseShape::DeepSeek => "deepseek",
            ResponseShape::Kimi => "kimi",
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
            "deepseek" => Some(ResponseShape::DeepSeek),
            "kimi" | "moonshot" => Some(ResponseShape::Kimi),
            _ => None,
        }
    }
}

/// 这一家能承接哪一种动作。
///
/// 分成两个能力而不是一个"是不是搜索后端"，理由是它们各自失败、各自计费：
/// 抓取按 URL 收费而搜索按查询收费，把一家只声明了搜索的条目拿去抓取，
/// 等于让一份没打算花的钱悄悄花出去。所以缺省是**搜索**，要用抓取得写出来。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Capability {
    Search,
    Fetch,
    Both,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Capability::Search => "search",
            Capability::Fetch => "fetch",
            Capability::Both => "both",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "search" => Some(Capability::Search),
            "fetch" => Some(Capability::Fetch),
            "both" | "search,fetch" | "fetch,search" => Some(Capability::Both),
            _ => None,
        }
    }

    pub fn search(self) -> bool {
        matches!(self, Capability::Search | Capability::Both)
    }

    pub fn fetch(self) -> bool {
        matches!(self, Capability::Fetch | Capability::Both)
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
    pub capability: Capability,
    /// 根地址，不含 `/search` 之类的路径：路径由形状决定，不由配置决定。
    pub base_url: String,
    /// 空串是一个**合法的、有意义的**取值：这家走 keyless 档。
    pub api_key: String,
    /// 这一条的 key 是从哪来的：`None` = 配置里写的那把；`Some(主机)` =
    /// 这条不是配出来的，是从现网在用的那条 LLM 上游折出来的，key 就是那把。
    ///
    /// 它不影响运行期——折出来的条目自带已经解析好的 key。它的用处是让读日志
    /// 与读状态面的人分得出"这家是有人配的"与"这家是跟着 LLM 走的"，后者
    /// 在配置里找不到，也不该去找。
    pub key_from: Option<String>,
}

impl SearchUpstream {
    pub fn keyless(&self) -> bool {
        self.api_key.trim().is_empty()
    }
}

/// 一个 LLM 上游主机有没有 `/tools/*` 面，以及面长什么样。
///
/// 判据只能是主机名：池条目里没有厂商字段，也不该为这件事加一个——现网在用
/// 的那条 LLM 上游已经是权威，再加一个字段就等于给"这家是谁"造出第二处可以
/// 写错的地方，而写错的表现是每次调用都 404。
///
/// 认主机而不是认域名后缀，是因为部署常常在中间加一层代理，而一个 URL 里
/// 写着 deepseek 的代理出口确实就是 DeepSeek 面。
pub fn shape_for_llm_host(base_url: &str) -> Option<ResponseShape> {
    let lowered = base_url.trim().to_ascii_lowercase();
    if lowered.contains("deepseek") {
        Some(ResponseShape::DeepSeek)
    } else if lowered.contains("moonshot") || lowered.contains("kimi") {
        Some(ResponseShape::Kimi)
    } else {
        None
    }
}

/// 把现网在用的那条 LLM 上游折成一条后备条目——**第三层的全部**。
///
/// 这里没有新配置、没有第二份密钥：它用的就是这条路正在用的那把 key 与那台
/// 主机。所以 LLM 换了一家，这一层跟着换；key 轮换之后这一层不会拿着旧 key
/// 去撞 401（那会被读成"这家挂了"，而真正的原因是它在拿旧凭证问）。
///
/// 主机认不出是哪一家时返回 `None`：那一层就是没有，而不是猜一个形状去问。
pub fn from_live_llm(base_url: &str, api_key: &str) -> Option<SearchUpstream> {
    let shape = shape_for_llm_host(base_url)?;
    let base_url = base_url.trim();
    let api_key = api_key.trim();
    if base_url.is_empty() || api_key.is_empty() {
        return None;
    }
    Some(SearchUpstream {
        // 名字里带主机：池里可能同时有 deepseek 与 kimi，而 `source` 的全部
        // 用处就是回答"是哪一家答的"。
        id: format!("llm:{}", host_of(base_url)),
        shape,
        // 这两家的 `/tools/fetch` 与 `/tools/search` 是同一份文档里的两个面。
        capability: Capability::Both,
        base_url: base_url.to_string(),
        api_key: api_key.to_string(),
        key_from: Some(base_url.to_string()),
    })
}

/// `https://api.deepseek.com/v1` 里的 `api.deepseek.com`。
fn host_of(base_url: &str) -> &str {
    let without_scheme = match base_url.split_once("://") {
        Some((_, rest)) => rest,
        None => base_url,
    };
    without_scheme.split('/').next().unwrap_or(without_scheme)
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
        // 这两家不写进这份配置：它们的搜索/抓取面跟着**现网在用的那条 LLM**
        // 走（见 [`from_live_llm`]），用的就是那把 key。写在这里等于让同一个
        // 凭证在第二个地方存一份，轮换时必然有一份是旧的——而旧 key 的表现
        // 是 401，会被读成"这家挂了"。
        if matches!(shape, ResponseShape::DeepSeek | ResponseShape::Kimi) {
            tracing::warn!(
                shape = shape.as_str(),
                "这家跟着现网在用的 LLM 走，不写进后备池配置；已丢弃这一条"
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
        let capability_raw = text("capability");
        let Some(capability) = Capability::parse(&capability_raw) else {
            tracing::warn!(
                id = %id,
                capability = %capability_raw,
                "后备索引条目的能力写法不认识，已丢弃这一条"
            );
            continue;
        };
        let key_from = text("key_from");
        out.push(SearchUpstream {
            id,
            shape,
            capability,
            base_url,
            api_key: text("api_key"),
            key_from: if key_from.is_empty() {
                None
            } else {
                Some(key_from)
            },
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
        // 这两家是同一个形状家族，但字段名不同，所以按家分列而不是合并：
        // 合并之后要按 id 猜，而猜错的表现是"每一次都 400"，不是解析失败。
        ResponseShape::DeepSeek => (
            "/tools/search",
            serde_json::json!({
                "query": query,
                "count": limit,
                // 不要正文：正文按 URL 另计一次费，而本地 `web_fetch` 才是取正文本的路。
                "need_content": false,
            }),
        ),
        ResponseShape::Kimi => (
            "/tools/search",
            serde_json::json!({
                "text_query": query,
                "limit": limit,
                "include_content": false,
            }),
        ),
    };
    // 没有 key 就不发认证头，而不是发一个空串：空串在有些上游那里是 401，
    // 在另一些那里是"匿名"，而同一份配置在这两处的表现不该不同。
    if !upstream.keyless() {
        let key = upstream.api_key.trim();
        match upstream.shape {
            ResponseShape::Tavily
            | ResponseShape::Qianfan
            | ResponseShape::DeepSeek
            | ResponseShape::Kimi => {
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

/// 抓一页的请求。`None` 表示这个形状**没有抓取面**（千帆只做检索），
/// 调用方跳过这一家就行了——拼一个猜出来的路径比跳过更糟，因为它在
/// 运行期才表现成 404，而 404 会被读成"这家挂了"。
pub fn build_fetch_request(upstream: &SearchUpstream, target: &str) -> Option<SearchRequest> {
    let base = upstream.base_url.trim_end_matches('/');
    let mut headers = vec![
        ("content-type".to_string(), "application/json".to_string()),
        ("accept".to_string(), "application/json".to_string()),
    ];
    let (path, body) = match upstream.shape {
        ResponseShape::Tavily => ("/extract", serde_json::json!({ "urls": [target] })),
        ResponseShape::Exa => (
            "/contents",
            // `text: {}` 才是"要正文"；不给这个键，Exa 只回元数据。
            serde_json::json!({ "urls": [target], "text": {} }),
        ),
        ResponseShape::DeepSeek | ResponseShape::Kimi => {
            ("/tools/fetch", serde_json::json!({ "url": target }))
        }
        ResponseShape::Qianfan => return None,
    };
    if !upstream.keyless() {
        let key = upstream.api_key.trim();
        match upstream.shape {
            ResponseShape::Tavily | ResponseShape::DeepSeek | ResponseShape::Kimi => {
                headers.push(("authorization".to_string(), format!("Bearer {key}")));
            }
            ResponseShape::Exa => {
                headers.push(("x-api-key".to_string(), key.to_string()));
            }
            // 上面已经 return 了，这里只是让穷尽性检查成立。
            ResponseShape::Qianfan => {}
        }
    }
    Some(SearchRequest {
        url: format!("{base}{path}"),
        headers,
        body,
    })
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

/// 抓到的一页。`title` 可以为空——很多站点没有 `<title>`，没有标题的
/// 正文仍然是有用的正文，而按标题把这一页丢掉会让后备层看起来全挂。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedPage {
    pub title: String,
    pub content: String,
}

/// 一家抓取应答的结局，三分法与 [`Answer`] 同构。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetchAnswer {
    Page(FetchedPage),
    Empty,
    Broken(String),
}

/// 把一家的抓取应答折成上面三种结局之一。
pub fn parse_fetch_response(shape: ResponseShape, raw: &Value) -> FetchAnswer {
    if let Some(message) = error_message(raw) {
        return FetchAnswer::Broken(message);
    }
    match shape {
        // 这两家把这一页放在 `results[0]` 里。
        ResponseShape::Tavily | ResponseShape::Exa => {
            if shape == ResponseShape::Tavily {
                // `failed_results` 是"这一页抓不下来"，与"这一页是空的"是
                // 两件事：前者要换下一家，后者换了也一样。混起来会让
                // 全部上游都被记成"抓到了空页"。
                if let Some(failures) = raw.get("failed_results").and_then(|v| v.as_array()) {
                    if let Some(first) = failures.first() {
                        return FetchAnswer::Broken(format!(
                            "上游报抓取失败：{first}（原文开头：{}）",
                            head_of(raw)
                        ));
                    }
                }
            }
            let Some(entries) = raw.get("results").and_then(|v| v.as_array()) else {
                return FetchAnswer::Broken(format!(
                    "应答里没有结果字段（{} 抓取要 results），原文开头：{}",
                    shape.as_str(),
                    head_of(raw)
                ));
            };
            match entries.first() {
                Some(entry) => page_of(entry),
                None => FetchAnswer::Empty,
            }
        }
        // 这两家的正文就在顶层，没有 results 包一层。
        ResponseShape::DeepSeek | ResponseShape::Kimi => page_of(raw),
        // 没有抓取面却被问到抓取：这是配置写错了，必须出声。
        ResponseShape::Qianfan => {
            FetchAnswer::Broken("千帆这个形状只做检索，没有抓取面".to_string())
        }
    }
}

fn page_of(entry: &Value) -> FetchAnswer {
    let text = |k: &str| {
        entry
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string()
    };
    let content =
        first_non_empty(&[text("raw_content"), text("text"), text("content")]).unwrap_or_default();
    if content.is_empty() {
        return FetchAnswer::Empty;
    }
    FetchAnswer::Page(FetchedPage {
        title: text("title"),
        content,
    })
}

/// 把一家的原始应答折成上面那三种结局之一。
pub fn parse_response(shape: ResponseShape, raw: &Value, limit: u64) -> Answer {
    // 能读出来的错误先读：多数上游在限额与鉴权失败时返回 200 + 一个错误体，
    // 于是"没看错误键"的表现是"这一家没有命中"，而不是"这一家拒绝了"。
    if let Some(message) = error_message(raw) {
        return Answer::Broken(message);
    }
    // 每家的结果键不同，而"上游换了版信封"与"这段代码读错了键"在现象上
    // 一模一样，所以已知的键全列出来，一个都没有才判 Broken。
    let candidates: &[&str] = match shape {
        ResponseShape::Tavily | ResponseShape::Exa => &["results"],
        ResponseShape::DeepSeek => &["results", "search_results"],
        ResponseShape::Qianfan => &["search_results", "references"],
        ResponseShape::Kimi => &["search_results", "references", "results"],
    };
    let entries = candidates
        .iter()
        .find_map(|key| raw.get(*key).and_then(|v| v.as_array()));
    let Some(entries) = entries else {
        return Answer::Broken(format!(
            "应答里没有结果字段（{} 形状要 {}），原文开头：{}",
            shape.as_str(),
            candidates.join(" 或 "),
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

/// 应答体里除了那句 `message` 之外还带着东西吗——即那句话是注解，
/// 还是它本身就是全部内容。
///
/// 只看"有没有 `results` 键"不够：限额错误常常是 `{"message": "quota
/// exceeded", "results": []}`，键在、东西不在，那种应答里 `message`
/// 就是唯一的内容，必须当错误读。
fn carries_a_payload(raw: &Value) -> bool {
    const PAYLOAD: [&str; 5] = [
        "results",
        "search_results",
        "references",
        "content",
        "raw_content",
    ];
    PAYLOAD.iter().any(|key| match raw.get(*key) {
        Some(Value::Array(items)) => !items.is_empty(),
        Some(Value::String(s)) => !s.trim().is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => true,
    })
}

/// 上游的报错键。几家用的名字不同，所以按名字一个个找。
///
/// `message` 单独处理：它在正常应答里常常是 `"ok"` 一类的东西，所以只有
/// 应答体里**没有别的可读内容**时才算错误。其余几个键出现即报错。
fn error_message(raw: &Value) -> Option<String> {
    for key in ["error", "detail", "message", "errmsg", "msg"] {
        let Some(value) = raw.get(key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        if key == "message" && carries_a_payload(raw) {
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
            capability: Capability::Both,
            base_url: "http://gateway:8081/search-fallback".to_string(),
            api_key: key.to_string(),
            key_from: None,
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

    #[test]
    fn an_entry_that_names_no_capability_only_does_search() {
        // 抓取按 URL 计费，所以"没说"必须是"只做检索"：反过来的缺省
        // 会让一份只打算检索的配置在抓取那一侧悄悄花钱。
        let raw = r#"[{"shape":"tavily","base_url":"http://a","api_key":"x"}]"#;
        let pool = parse_upstreams(raw);
        assert!(pool[0].capability.search());
        assert!(!pool[0].capability.fetch());
        let raw = r#"[{"shape":"tavily","base_url":"http://a","capability":"both"}]"#;
        let pool = parse_upstreams(raw);
        assert!(pool[0].capability.search() && pool[0].capability.fetch());
        // 写法不认识是配置错误：丢这一条并点名，不猜成 search。
        let raw = r#"[{"shape":"tavily","base_url":"http://a","capability":"crawl"}]"#;
        assert!(parse_upstreams(raw).is_empty());
    }

    #[test]
    fn the_llm_shaped_upstreams_ask_for_no_body_text() {
        // 这两家按 URL 收正文的钱，而正文本地取得到：搜索只要链接与摘要，
        // 正文交给带地址守卫的 `web_fetch`。
        let deepseek = build_request(&upstream(ResponseShape::DeepSeek, "k"), "qdrant", 3);
        assert_eq!(
            deepseek.url,
            "http://gateway:8081/search-fallback/tools/search"
        );
        assert_eq!(deepseek.body["need_content"], serde_json::json!(false));
        assert_eq!(deepseek.header("authorization"), Some("Bearer k"));
        let kimi = build_request(&upstream(ResponseShape::Kimi, "k"), "qdrant", 3);
        assert_eq!(kimi.body["text_query"], "qdrant");
        assert_eq!(kimi.body["include_content"], serde_json::json!(false));
        // 两家的字段名不同，所以形状名必须分清：合成一个的话这里会 400。
        assert!(kimi.body.get("query").is_none());
        assert!(deepseek.body.get("text_query").is_none());
    }

    #[test]
    fn kimi_folds_search_results_like_the_others() {
        let raw = serde_json::json!({
            "search_results": [
                {"title":"K","url":"https://k/1","content":"body k"}
            ]
        });
        match parse_response(ResponseShape::Kimi, &raw, 10) {
            Answer::Hits(hits) => {
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].url, "https://k/1");
                assert_eq!(hits[0].snippet, "body k");
            }
            other => panic!("expected hits, got {other:?}"),
        }
    }

    #[test]
    fn each_shape_fetches_with_its_own_body_and_the_url_untouched() {
        let target = "https://example.com/a?b=1";
        let tavily = build_fetch_request(&upstream(ResponseShape::Tavily, "k1"), target).unwrap();
        assert_eq!(tavily.url, "http://gateway:8081/search-fallback/extract");
        assert_eq!(tavily.body["urls"], serde_json::json!([target]));
        assert_eq!(tavily.header("authorization"), Some("Bearer k1"));
        let exa = build_fetch_request(&upstream(ResponseShape::Exa, "k2"), target).unwrap();
        assert_eq!(exa.url, "http://gateway:8081/search-fallback/contents");
        assert_eq!(exa.body["urls"], serde_json::json!([target]));
        // `text: {}` 才是"要正文"；没有这个键 Exa 只回元数据。
        assert!(exa.body.get("text").is_some());
        assert_eq!(exa.header("x-api-key"), Some("k2"));
        let deepseek =
            build_fetch_request(&upstream(ResponseShape::DeepSeek, "k3"), target).unwrap();
        assert_eq!(
            deepseek.url,
            "http://gateway:8081/search-fallback/tools/fetch"
        );
        assert_eq!(deepseek.body["url"], target);
        // 千帆没有抓取面：跳过它，而不是猜一条路径。
        assert!(build_fetch_request(&upstream(ResponseShape::Qianfan, "k4"), target).is_none());
    }

    #[test]
    fn a_fetched_page_folds_out_of_both_envelope_families() {
        let tavily = serde_json::json!({
            "results": [{"url":"https://e/1","raw_content":"full text"}]
        });
        assert_eq!(
            parse_fetch_response(ResponseShape::Tavily, &tavily),
            FetchAnswer::Page(FetchedPage {
                title: String::new(),
                content: "full text".to_string()
            })
        );
        let exa = serde_json::json!({
            "results": [{"url":"https://e/1","title":"T","text":"body"}]
        });
        assert_eq!(
            parse_fetch_response(ResponseShape::Exa, &exa),
            FetchAnswer::Page(FetchedPage {
                title: "T".to_string(),
                content: "body".to_string()
            })
        );
        // DeepSeek / Kimi 的正文在顶层，没有 results 包一层。
        let top = serde_json::json!({ "title": "D", "content": "top text" });
        assert_eq!(
            parse_fetch_response(ResponseShape::DeepSeek, &top),
            FetchAnswer::Page(FetchedPage {
                title: "D".to_string(),
                content: "top text".to_string()
            })
        );
    }

    #[test]
    fn a_failed_extraction_is_broken_not_an_empty_page() {
        // 「这一页抓不下来」与「这一页是空的」是两件事：前者要换下一家。
        let failed = serde_json::json!({
            "results": [],
            "failed_results": [{"url":"https://e/1","error":"403 Forbidden"}]
        });
        match parse_fetch_response(ResponseShape::Tavily, &failed) {
            FetchAnswer::Broken(reason) => assert!(reason.contains("403"), "{reason}"),
            other => panic!("a failed extraction must not read as an empty page: {other:?}"),
        }
        // 信封读不懂同样是错误，且要带上原文。
        let unknown = serde_json::json!({ "webPages": {} });
        match parse_fetch_response(ResponseShape::Exa, &unknown) {
            FetchAnswer::Broken(reason) => assert!(reason.contains("webPages"), "{reason}"),
            other => panic!("an unreadable envelope must not read as an empty page: {other:?}"),
        }
    }

    #[test]
    fn the_server_side_layer_is_derived_from_the_live_llm_not_configured() {
        // 现网在用的是 DeepSeek：这一层就是那台主机、那把 key，一个字节都不是
        // 抄来的第二份。所以 LLM 轮换 key 之后它不可能还拿着旧的。
        let derived =
            from_live_llm("https://api.deepseek.com/v1", "sk-live").expect("deepseek 有工具面");
        assert_eq!(derived.shape, ResponseShape::DeepSeek);
        assert_eq!(derived.base_url, "https://api.deepseek.com/v1");
        assert_eq!(derived.api_key, "sk-live");
        assert_eq!(derived.id, "llm:api.deepseek.com");
        assert_eq!(
            derived.key_from.as_deref(),
            Some("https://api.deepseek.com/v1")
        );
        let request = build_request(&derived, "qdrant", 3);
        // base_url 带着版本前缀，工具路径接在后面：/v1/tools/search。
        assert_eq!(request.url, "https://api.deepseek.com/v1/tools/search");

        let kimi = from_live_llm("https://api.moonshot.cn/v1", "sk-k").expect("kimi 有工具面");
        assert_eq!(kimi.shape, ResponseShape::Kimi);
        // 主机认不出是哪一家就没有这一层，而不是猜一个形状去问。
        assert!(from_live_llm("https://api.openai.com/v1", "sk-o").is_none());
        // 没有 key 的 LLM 上游也折不出来：那是一次必然 401 的请求。
        assert!(from_live_llm("https://api.deepseek.com/v1", "").is_none());
    }

    #[test]
    fn an_entry_that_follows_the_llm_is_not_written_into_the_pool() {
        // 写进配置就等于存第二份 key；轮换之后那份旧的会 401，而 401 会被
        // 读成"这家挂了"。所以它必须被丢掉，且不是悄悄丢。
        let raw = r#"[
            {"shape":"deepseek","base_url":"https://api.deepseek.com/v1","api_key":"sk-old"},
            {"shape":"moonshot","base_url":"https://api.moonshot.cn/v1","api_key":"sk-old"},
            {"shape":"tavily","base_url":"https://api.tavily.com","api_key":"tv"}
        ]"#;
        let pool = parse_upstreams(raw);
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].shape, ResponseShape::Tavily);
    }

    #[test]
    fn a_quota_message_with_an_empty_result_list_is_still_an_error() {
        // 只看"有没有 results 键"会把这种应答读成"没有命中"：键在、东西不在，
        // 那句 message 就是这条应答的全部内容。
        let quota = serde_json::json!({
            "message": "monthly quota exceeded",
            "results": []
        });
        match parse_response(ResponseShape::Kimi, &quota, 10) {
            Answer::Broken(reason) => assert!(reason.contains("quota"), "{reason}"),
            other => panic!("a quota message must not read as no matches: {other:?}"),
        }
    }
}
