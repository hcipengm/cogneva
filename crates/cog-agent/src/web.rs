//! Free web access: search through a self-hosted SearXNG, and read one page.
//!
//! Both tools exist so a run can look things up on the open web without
//! spending an upstream vendor's server-side search/fetch quota. The search
//! half talks to a SearXNG instance inside the cluster -- no account, no
//! per-request billing, and the query never leaves the deployment's own
//! metasearch fan-out. The fetch half downloads the page itself and extracts
//! its text in-process, which is the part a hosted "fetch this URL" tool used to
//! do for a fee.
//!
//! Three properties are decided here rather than left to the caller:
//!
//! - **Where the model may point the fetch.** A run is driven by text it did
//!   not write, so a URL in a tool call is untrusted input. Loopback, private,
//!   link-local, cluster-internal names and non-HTTP schemes are refused before
//!   a request is built. The refusal names the reason, because "this address is
//!   inside the deployment" and "the page did not answer" are different facts
//!   and a caller that folds them retries the wrong one.
//! - **How many bytes one page may cost.** The download stops at the cap rather
//!   than truncating a fully transferred body: the bill for an oversized page
//!   is paid in transfer, so a cap applied afterwards is a report, not a limit.
//! - **What the caller sees when the search backend is not wired.** The tools
//!   stay registered and fail with the name of the missing setting. Leaving
//!   them out would make "this deployment has no web access" and "this tool does
//!   not exist" the same answer, which is the failure the built-in list's
//!   one-constructor rule is there to prevent.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cog_core::{HttpClient, HttpRequest, SFError, SFResult, Tool, ToolImplementation};
use futures::StreamExt;
use url::Url;

/// Hits returned when the caller does not say how many it wants. Ten is one
/// screen of titles and excerpts: enough to choose a page to read, small enough
/// that a turn is not spent on links it will not follow.
const DEFAULT_SEARCH_RESULTS: u64 = 10;

/// The most hits one call may ask for. Every hit is context the run pays for on
/// every later turn, so a caller that wants more pages should read the ones it
/// has rather than page through a result list it cannot use.
const MAX_SEARCH_RESULTS: u64 = 50;

/// Characters of extracted text returned when the caller does not ask for a
/// size. Roughly one long article; the caller can raise it for a document it
/// means to read whole.
const DEFAULT_FETCH_CHARS: u64 = 20_000;

/// The most text one fetch may return. Above this the answer stops being a turn
/// and becomes a document, and a document belongs in the archive rather than in
/// the conversation.
const MAX_FETCH_CHARS: u64 = 200_000;

/// Bytes one page may transfer. Below the point where a single turn is worth
/// anything, the page cannot be read anyway; above it, an accidental video or
/// an infinite stream costs the pod's memory before extraction ever runs.
const MAX_DOWNLOAD_BYTES: usize = 5 * 1024 * 1024;

/// How long one fetch may take, headers included. A page that has not started
/// answering in this window is not going to make the turn useful.
const FETCH_TIMEOUT_SECS: u64 = 30;

/// How long one search may take. The metasearch fan-out answers from several
/// engines at once and waits for the slowest; this is the point past which the
/// slowest is not worth the turn.
const SEARCH_TIMEOUT_SECS: u64 = 20;

/// How many times a search whose engines all stayed silent is asked again.
///
/// Only that one outcome is repeated: an engine that timed out once often
/// answers the next request, while a non-200 or a body that is not JSON is a
/// verdict about the request itself and asking again returns the same verdict
/// for the same wait.
const SEARCH_ATTEMPTS: u32 = 2;

/// The environment variable that names the SearXNG base URL.
///
/// Named here, next to the tool that reads it, so the deployment's env table and
/// the tool cannot disagree about the spelling. Empty means "this deployment
/// chose not to wire web search", which the tool reports at call time rather
/// than hiding by being absent from the list.
pub const WEB_SEARCH_BASE_ENV: &str = "COGNEVA_WEB_SEARCH_BASE";

/// The environment variable that names the rendering backend for `web_fetch`.
///
/// Empty means "extract in this process", which is the cheaper path and the one
/// a deployment without a browser gets. Non-empty is a promise: the page is
/// rendered by a real browser before it is read, so a client-side-rendered page
/// returns its text instead of an empty shell. The two are not interchangeable
/// — the in-process path cannot run scripts — so a configured backend that does
/// not answer is an error naming the backend, never a silent downgrade to the
/// path that returns nothing for the pages the backend exists for.
pub const WEB_FETCH_BASE_ENV: &str = "COGNEVA_WEB_FETCH_BASE";

/// How long one rendered fetch may take. A browser has to start, load, run the
/// page's scripts and settle; the in-process deadline would cut that short.
const RENDER_TIMEOUT_SECS: u64 = 60;

/// How long one rendered page stays servable out of the backend's own cache.
///
/// The backend keys its render cache on the revision the request carries, and
/// its default revision is one bucket for the life of the process. A render
/// that caught a client-rendered page before its scripts ran is then handed to
/// every later call for that URL: the caller gets the shell again and again,
/// with nothing in the answer that says it is stale.
///
/// The window is one render's own deadline, not a number of its own: a render
/// may run that long (measured 3s on a plain page, the whole budget on a heavy
/// one), so a shorter window could never be hit by the next render and a longer
/// one would keep an answer alive past the budget that produced it.
const RENDER_CACHE_SECS: u64 = RENDER_TIMEOUT_SECS;

/// How many times a page the backend failed on is asked for again.
///
/// A renderer answers 5xx when its own page budget ran out mid-render, which is
/// what a cold browser on a heavy page looks like; the second attempt starts
/// from a warm one and usually returns the page. Only a 5xx is repeated: a 4xx
/// is a verdict about this URL and repeating it costs the caller the same wait
/// for the same answer.
const RENDER_ATTEMPTS: u32 = 2;

fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The SearXNG base URL, or an error naming the setting that would supply it.
fn search_base(base: Option<&str>) -> SFResult<String> {
    base.filter(|b| !b.trim().is_empty())
        .map(|b| b.trim_end_matches('/').to_string())
        .ok_or_else(|| {
            SFError::Config(format!(
                "web search has no backend configured: set {WEB_SEARCH_BASE_ENV} to the \
                 SearXNG base URL this deployment runs"
            ))
        })
}

fn http_client(client: Option<Arc<dyn HttpClient>>) -> SFResult<Arc<dyn HttpClient>> {
    client.ok_or_else(|| SFError::Config("no HTTP client is available for web access".into()))
}

/// Refuse an address that is not on the public web.
///
/// The check is on the URL as written, before any request exists, and it covers
/// the shapes a model can plausibly be talked into: metadata endpoints, the
/// pod's own loopback, the cluster's service DNS, and schemes that read local
/// files. It cannot resolve a hostname that looks public and points inward --
/// that is what the pod's egress policy is for -- so it is the cheap half of
/// the guard, applied at the only place the request is built.
fn check_public_url(raw: &str) -> SFResult<Url> {
    let url = Url::parse(raw.trim())
        .map_err(|e| SFError::Validation(format!("not a URL: {raw:?} ({e})")))?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(SFError::Validation(format!(
                "only http and https may be fetched; {other:?} is refused"
            )))
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(SFError::Validation(
            "a URL carrying credentials is refused: the page would be read with them".into(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| SFError::Validation(format!("{raw:?} has no host")))?;
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        let inside = match ip {
            std::net::IpAddr::V4(v4) => {
                v4.is_loopback()
                    || v4.is_private()
                    || v4.is_link_local()
                    || v4.is_unspecified()
                    || v4.is_broadcast()
                    || v4.is_multicast()
                    || v4.is_documentation()
            }
            std::net::IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    // IPv4-mapped forms such as ::ffff:127.0.0.1 reach loopback.
                    || v6.to_ipv4_mapped().is_some_and(|v4| {
                        v4.is_loopback() || v4.is_private() || v4.is_link_local()
                    })
            }
        };
        if inside {
            return Err(SFError::Validation(format!(
                "{host} is an address inside this network, not on the public web; the \
                 fetch is refused"
            )));
        }
        return Ok(url);
    }
    let lowered = host.to_ascii_lowercase();
    let internal = lowered == "localhost"
        || lowered.ends_with(".localhost")
        || lowered.ends_with(".local")
        || lowered.ends_with(".internal")
        || lowered.ends_with(".svc")
        || lowered.contains(".svc.")
        || lowered.ends_with(".cluster.local");
    if internal {
        return Err(SFError::Validation(format!(
            "{host} is a name this deployment resolves internally, not a public host; \
             the fetch is refused"
        )));
    }
    Ok(url)
}

/// Read at most `cap` bytes of a response, stopping the transfer at the cap.
///
/// The stream is dropped as soon as the cap is crossed, which is what stops the
/// download; collecting the body first and checking its length afterwards would
/// transfer the whole page and then call it bounded.
struct Body {
    status: u16,
    content_type: String,
    bytes: Vec<u8>,
    too_large: bool,
}

async fn fetch_capped(client: &Arc<dyn HttpClient>, url: &Url, cap: usize) -> SFResult<Body> {
    let mut req = HttpRequest::new("GET", url.as_str());
    req.timeout_secs = Some(FETCH_TIMEOUT_SECS);
    req.headers.insert(
        "Accept".into(),
        "text/html,application/xhtml+xml,text/plain;q=0.9,application/json;q=0.8,*/*;q=0.5".into(),
    );
    // A page that answers with an identity that blocks scripts is cheaper to
    // read and cannot hand the extraction a client-side-only shell.
    req.headers
        .insert("Accept-Language".into(), "zh-CN,zh;q=0.9,en;q=0.8".into());
    let mut resp = client.execute_stream(req).await?;
    let content_type = resp
        .headers
        .get("content-type")
        .or_else(|| resp.headers.get("Content-Type"))
        .cloned()
        .unwrap_or_default();
    let status = resp.status;
    let mut bytes = Vec::new();
    let mut too_large = false;
    while let Some(chunk) = resp.stream.next().await {
        let chunk = chunk?;
        if bytes.len() + chunk.len() > cap {
            // Keep the prefix: it is enough to say the page is too large by
            // showing what it is, and it is not billed twice.
            bytes.extend_from_slice(&chunk[..cap.saturating_sub(bytes.len())]);
            too_large = true;
            break;
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(Body {
        status,
        content_type,
        bytes,
        too_large,
    })
}

/// The text of a page, with the page's own chrome removed.
struct Extracted {
    title: String,
    text: String,
    byline: Option<String>,
    site_name: Option<String>,
    published: Option<String>,
    /// Which extraction produced the text, so a thin answer can be traced to the
    /// path that produced it instead of looking like a thin page.
    how: &'static str,
}

fn extract(html: &str, url: &str) -> Extracted {
    if let Ok(mut readability) = dom_smoothie::Readability::new(html, Some(url), None) {
        if let Ok(article) = readability.parse() {
            let text = normalise(&article.text_content);
            if !text.trim().is_empty() {
                return Extracted {
                    title: article.title.trim().to_string(),
                    text,
                    byline: article.byline.filter(|b| !b.trim().is_empty()),
                    site_name: article.site_name.filter(|s| !s.trim().is_empty()),
                    published: article.published_time.filter(|p| !p.trim().is_empty()),
                    how: "readability",
                };
            }
        }
    }
    // Readability refuses pages that are not articles -- a list, a bare
    // specification, a table. Those still have text worth returning, so the
    // fallback strips tags instead of reporting an empty page.
    Extracted {
        title: html_title(html).unwrap_or_default(),
        text: normalise(&strip_html(html)),
        byline: None,
        site_name: None,
        published: None,
        how: "stripped",
    }
}

/// Collapse runs of blanks to one space, keep line breaks, and drop blank lines.
fn normalise(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut last_was_space = false;
    let mut blank_lines = 0;
    for ch in text.chars() {
        match ch {
            '\n' => {
                if out.ends_with(' ') {
                    out.pop();
                }
                blank_lines += 1;
                if blank_lines <= 2 {
                    out.push('\n');
                }
                last_was_space = false;
            }
            c if c.is_whitespace() => {
                if !last_was_space && !out.ends_with('\n') {
                    out.push(' ');
                }
                last_was_space = true;
            }
            c => {
                out.push(c);
                last_was_space = false;
                blank_lines = 0;
            }
        }
    }
    out.trim().to_string()
}

fn html_title(html: &str) -> Option<String> {
    let lowered = html.to_ascii_lowercase();
    let start = lowered.find("<title")?;
    let open_end = lowered[start..].find('>')? + start + 1;
    let close = lowered[open_end..].find("</title")? + open_end;
    let title = decode_entities(&html[open_end..close]);
    let title = normalise(&title);
    (!title.is_empty()).then_some(title)
}

/// Tags whose content is never prose. Their closing tag is looked up rather
/// than trusted as a structure, because the input is whatever the page sent.
const NON_TEXT_TAGS: [&str; 6] = ["script", "style", "noscript", "svg", "template", "head"];

/// Remove markup, keeping the words, for pages readability will not take.
fn strip_html(html: &str) -> String {
    let lowered = html.to_ascii_lowercase();
    let bytes = html.as_bytes();
    let mut out = String::with_capacity(html.len() / 3);
    let mut i = 0;
    while i < bytes.len() {
        if html[i..].starts_with("<!--") {
            match html[i + 4..].find("-->") {
                Some(end) => i += 4 + end + 3,
                None => break,
            }
            continue;
        }
        if html[i..].starts_with('<') {
            let Some(rel) = html[i..].find('>') else {
                break;
            };
            let tag_body = &html[i + 1..i + rel];
            let name: String = tag_body
                .trim_start_matches('/')
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect::<String>()
                .to_ascii_lowercase();
            if !tag_body.starts_with('/') && NON_TEXT_TAGS.contains(&name.as_str()) {
                let closing = format!("</{name}");
                match lowered[i + rel..].find(&closing) {
                    Some(end) => {
                        let after = i + rel + end;
                        i = lowered[after..]
                            .find('>')
                            .map_or(bytes.len(), |e| after + e + 1);
                    }
                    None => break,
                }
                continue;
            }
            // Block boundaries survive as line breaks so paragraphs do not run
            // together into one unreadable line. Only the closing tag emits
            // one: an opening tag that did too would turn every paragraph
            // break into a blank line.
            if (tag_body.starts_with('/') || name == "br")
                && matches!(
                    name.as_str(),
                    "p" | "div"
                        | "br"
                        | "li"
                        | "tr"
                        | "h1"
                        | "h2"
                        | "h3"
                        | "h4"
                        | "h5"
                        | "h6"
                        | "section"
                        | "article"
                        | "blockquote"
                        | "pre"
                )
            {
                out.push('\n');
            }
            i += rel + 1;
            continue;
        }
        let next = html[i..].find('<').map_or(bytes.len(), |n| i + n);
        out.push_str(&html[i..next]);
        i = next;
    }
    decode_entities(&out)
}

/// Decode the entities that carry meaning in prose. Numeric forms are covered
/// generically; the named ones are the set that actually reaches text.
fn decode_entities(text: &str) -> String {
    if !text.contains('&') {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        let semi = tail[..tail.len().min(12)].find(';');
        let decoded = semi.map(|end| {
            let entity = &tail[1..end];
            let ch = match entity {
                "amp" => Some('&'),
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" | "#39" => Some('\''),
                "nbsp" => Some(' '),
                "mdash" => Some('—'),
                "ndash" => Some('–'),
                "hellip" => Some('…'),
                "rsquo" => Some('’'),
                "lsquo" => Some('‘'),
                "ldquo" => Some('“'),
                "rdquo" => Some('”'),
                _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                    u32::from_str_radix(&entity[2..], 16)
                        .ok()
                        .and_then(char::from_u32)
                }
                _ if entity.starts_with('#') => {
                    entity[1..].parse::<u32>().ok().and_then(char::from_u32)
                }
                _ => None,
            };
            match ch {
                Some(c) => (c, end + 1),
                None => ('&', 0),
            }
        });
        match decoded {
            Some((c, consumed)) if consumed > 0 => {
                out.push(c);
                rest = &tail[consumed..];
            }
            _ => {
                out.push('&');
                rest = &tail[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    match text.char_indices().nth(max_chars) {
        Some((cut, _)) => (text[..cut].to_string(), true),
        None => (text.to_string(), false),
    }
}

/// Search the open web through the deployment's own SearXNG instance.
pub fn web_search(client: Option<Arc<dyn HttpClient>>, base: Option<String>) -> Tool {
    Tool {
        name: "web_search".into(),
        description: "Search the open web and return ranked results with title, URL and \
                      excerpt. Runs against this deployment's self-hosted metasearch \
                      backend, so it costs no per-request vendor fee. Use it before \
                      guessing at a current fact or an unfamiliar API; follow it with \
                      web_fetch to read a result that matters."
            .into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to search for, in the words a person would use"
                },
                "limit": {
                    "type": "integer",
                    "description": "How many results to return (default 10, max 50)"
                },
                "language": {
                    "type": "string",
                    "description": "Result language such as zh-CN or en; omit for the backend's default"
                },
                "categories": {
                    "type": "string",
                    "description": "SearXNG category set such as general, news, it (default general)"
                }
            },
            "required": ["query"]
        }),
        implementation: ToolImplementation::Native(Arc::new(move |args| {
            let client = client.clone();
            let base = base.clone();
            Box::pin(async move {
                let query = args["query"]
                    .as_str()
                    .map(str::trim)
                    .filter(|q| !q.is_empty())
                    .ok_or_else(|| SFError::Validation("query required".into()))?
                    .to_string();
                let limit = args["limit"]
                    .as_u64()
                    .unwrap_or(DEFAULT_SEARCH_RESULTS)
                    .clamp(1, MAX_SEARCH_RESULTS);
                let base = search_base(base.as_deref())?;
                let client = http_client(client)?;
                let mut url = format!(
                    "{base}/search?q={}&format=json&limit={limit}&safesearch=1",
                    encode_component(&query)
                );
                if let Some(language) = args["language"].as_str().filter(|l| !l.trim().is_empty()) {
                    url.push_str(&format!("&language={}", encode_component(language.trim())));
                }
                let categories = args["categories"]
                    .as_str()
                    .filter(|c| !c.trim().is_empty())
                    .unwrap_or("general");
                url.push_str(&format!("&categories={}", encode_component(categories)));

                // A search that came back empty because the engines were
                // silent is asked again, because that is the one failure here
                // with a good chance of not repeating: measured on this
                // deployment, one query returned zero hits with the engine
                // timing out and the very next query answered normally. One
                // extra request buys the caller an answer instead of a "the web
                // has nothing on this" that is wrong. Everything else -- a
                // non-200, a body that is not JSON -- is a verdict about the
                // request and is not repeated.
                let mut attempt = 0;
                let answer = loop {
                    attempt += 1;
                    let mut req = HttpRequest::new("GET", url.clone());
                    req.timeout_secs = Some(SEARCH_TIMEOUT_SECS);
                    req.headers
                        .insert("Accept".into(), "application/json".into());
                    let resp = client.execute(req).await.map_err(|e| {
                        SFError::Agent(format!("web search backend at {base} is unreachable: {e}"))
                    })?;
                    let answer = search_answer(&resp, &base, limit)?;
                    if !answer.results.is_empty()
                        || answer.broken.is_empty()
                        || attempt >= SEARCH_ATTEMPTS
                    {
                        break answer;
                    }
                };
                // Zero hits means two different things depending on who is
                // reporting it. From an engine that answered, it says the query
                // was asked and nothing matched -- paraphrasing is the right
                // next move. From a fan-out where every engine that could have
                // answered went silent, it says the query was never asked --
                // retrying is the right next move, and rewriting the question
                // throws away the one thing that was working. Returning both as
                // `count: 0` leaves the caller to pick between those without
                // the fact it needs, so the silent one is an error that names
                // the engines instead of a result set.
                if answer.results.is_empty() && !answer.broken.is_empty() {
                    let names: Vec<String> = answer.broken.iter().map(|e| e.to_string()).collect();
                    return Err(SFError::Agent(format!(
                        "web search came back empty after {attempt} attempt(s) because no \
                         engine answered, which is not the same answer as 'nothing \
                         matched': the query was never put to anything. Engines that \
                         stayed silent: {}. Retry the same query rather than rephrasing \
                         it",
                        names.join(", ")
                    )));
                }
                let mut out = serde_json::json!({
                    "query": query,
                    "limit": limit,
                    "count": answer.results.len(),
                    "results": answer.results,
                });
                // Answers and suggestions come from the engines' own knowledge
                // cards; when present they answer without a second fetch. They
                // are kept out of an empty answer: a card-only reply would read
                // as a result set that happens to have no hits.
                for (key, value) in answer.extras {
                    out[key] = value;
                }
                if !answer.broken.is_empty() {
                    out["unresponsive_engines"] = serde_json::Value::Array(answer.broken);
                }
                Ok(out)
            })
        })),
    }
}

/// One answer from the search backend, split into what the caller acts on.
///
/// The three parts are kept apart because they mean different things: `results`
/// is what the engines found, `broken` is which of them never answered, and
/// `extras` are the knowledge cards that answer without a second fetch. Folding
/// `broken` into `results` is what makes an outage read as an empty web.
struct SearchAnswer {
    results: Vec<serde_json::Value>,
    broken: Vec<serde_json::Value>,
    extras: Vec<(String, serde_json::Value)>,
}

fn search_answer(resp: &cog_core::HttpResponse, base: &str, limit: u64) -> SFResult<SearchAnswer> {
    if !resp.is_success() {
        let body = String::from_utf8_lossy(&resp.body);
        let hint = if resp.status == 403 || body.contains("format") {
            " -- SearXNG refuses JSON by default; add `json` to `search.formats` in its settings"
        } else {
            ""
        };
        return Err(SFError::Agent(format!(
            "web search backend answered {}{hint}: {}",
            resp.status,
            body.chars().take(300).collect::<String>()
        )));
    }
    let body: serde_json::Value = serde_json::from_slice(&resp.body).map_err(|e| {
        SFError::Agent(format!(
            "web search backend at {base} did not answer with JSON ({e}); a \
             backend serving HTML only needs `json` added to its `search.formats`",
        ))
    })?;
    let results: Vec<serde_json::Value> = body["results"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .take(limit as usize)
        .map(|hit| {
            serde_json::json!({
                "title": hit["title"],
                "url": hit["url"],
                "snippet": hit["content"],
                "engine": hit["engine"],
                "published": hit["publishedDate"],
            })
        })
        .collect();
    let broken: Vec<serde_json::Value> = body
        .get("unresponsive_engines")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let extras: Vec<(String, serde_json::Value)> = ["answers", "suggestions", "infoboxes"]
        .into_iter()
        .filter_map(|key| {
            body.get(key)
                .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
                .map(|v| (key.to_string(), v.clone()))
        })
        .collect();
    Ok(SearchAnswer {
        results,
        broken,
        extras,
    })
}

/// The rendering backend, or `None` when this deployment reads pages in process.
fn render_base(base: Option<&str>) -> Option<String> {
    base.map(str::trim)
        .filter(|b| !b.is_empty())
        .map(|b| b.trim_end_matches('/').to_string())
}

/// The cache revision the backend is asked for at `now`, in buckets of
/// [`RENDER_CACHE_SECS`]. Reads the clock so it is a function of the time only,
/// which is what lets a test say when a stale render expires.
fn render_cache_revision(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    (secs / RENDER_CACHE_SECS).to_string()
}

/// Read one page through the rendering backend and return its Markdown.
///
/// The backend is asked for the *filtered* Markdown (`f: fit`): the raw body of
/// a page includes navigation, cookie banners and footer links, which is the
/// part readability strips and the part that makes a fetched page cost more
/// context than the question it was fetched for.
///
/// Every way this can fail returns an error that names the backend. A caller
/// that got a thin answer could not tell "the page is empty" from "the renderer
/// never ran", and the two call for opposite repairs.
async fn fetch_rendered(
    client: &Arc<dyn HttpClient>,
    base: &str,
    url: &Url,
    max_chars: usize,
) -> SFResult<serde_json::Value> {
    let endpoint = format!("{base}/md");
    let body = serde_json::to_vec(&serde_json::json!({
        "url": url.as_str(),
        "f": "fit",
        "c": render_cache_revision(SystemTime::now()),
    }))
    .map_err(SFError::Serialization)?;
    let mut attempt = 0;
    let resp = loop {
        attempt += 1;
        let mut req = HttpRequest::new("POST", endpoint.clone());
        req.headers
            .insert("Content-Type".into(), "application/json".into());
        req.timeout_secs = Some(RENDER_TIMEOUT_SECS);
        req.body = Some(body.clone());
        let resp = client.execute(req).await.map_err(|e| {
            SFError::Agent(format!(
                "the rendering backend at {base} did not answer ({e}); the page was not read \
                 in process instead, because this deployment asked for rendering"
            ))
        })?;
        if (200..300).contains(&resp.status) {
            break resp;
        }
        let detail = truncate_chars(&String::from_utf8_lossy(&resp.body), 300).0;
        if !(500..600).contains(&resp.status) || attempt >= RENDER_ATTEMPTS {
            let tries = if attempt == 1 {
                String::new()
            } else {
                format!(" on {attempt} attempts")
            };
            return Err(SFError::Agent(format!(
                "the rendering backend at {base} refused {url} with HTTP {}{tries}: {detail}",
                resp.status
            )));
        }
    };
    let mut parsed: serde_json::Value = serde_json::from_slice(&resp.body).map_err(|e| {
        SFError::Agent(format!(
            "the rendering backend at {base} answered non-JSON: {e}"
        ))
    })?;
    if parsed.get("success").and_then(|s| s.as_bool()) == Some(false) {
        let detail = parsed
            .get("detail")
            .or_else(|| parsed.get("error"))
            .map(|v| v.to_string())
            .unwrap_or_else(|| "(no reason given)".into());
        return Err(SFError::Agent(format!(
            "the rendering backend at {base} could not read {url}: {detail}"
        )));
    }
    let markdown = parsed
        .get("markdown")
        .and_then(|m| m.as_str())
        .ok_or_else(|| {
            SFError::Agent(format!(
                "the rendering backend at {base} answered without a `markdown` field"
            ))
        })?
        .to_string();
    if markdown.trim().is_empty() {
        // A 200 with nothing in it is the shape a document gets here: the
        // renderer opened it and found no HTML to reduce. Returning the empty
        // string would read as "this page says nothing", which is a different
        // fact and calls for a different repair.
        return Err(SFError::Agent(format!(
            "the rendering backend at {base} opened {url} but extracted no text; the \
             renderer reads HTML pages, so a binary document or a page that draws itself \
             outside the document tree comes back empty"
        )));
    }
    let (text, truncated) = truncate_chars(&markdown, max_chars);
    let mut out = serde_json::json!({
        "url": url.as_str(),
        "chars": text.chars().count(),
        "truncated": truncated,
        "extraction": "rendered",
        "format": "markdown",
        "text": text,
    });
    if let Some(title) = markdown_title(&markdown) {
        out["title"] = serde_json::json!(title);
        // The title is also a field on the answer, not only a line in the body:
        // a caller that lists pages should not have to parse Markdown to name
        // the one it read.
        parsed["title"] = serde_json::json!(title);
    }
    Ok(out)
}

/// The first ATX heading of a Markdown body, used as the page title.
fn markdown_title(markdown: &str) -> Option<String> {
    markdown
        .lines()
        .find_map(|line| line.trim_start().strip_prefix("# ").map(str::trim))
        .filter(|t| !t.is_empty())
        .map(str::to_string)
}

/// Download one page and return its readable text.
///
/// With `base` set the page is rendered by a real browser first (see
/// [`fetch_rendered`]); without it the page is read in process. The difference
/// matters on client-side-rendered pages, where the in-process path returns the
/// shell of a page whose text arrives only after scripts run.
pub fn web_fetch(client: Option<Arc<dyn HttpClient>>, base: Option<String>) -> Tool {
    Tool {
        name: "web_fetch".into(),
        description: "Fetch one public web page and return its readable text, with the \
                      page title. When this deployment runs a rendering backend the page \
                      is rendered in a real browser first and returned as filtered \
                      Markdown, so client-side-rendered pages come back with the text \
                      their scripts produced; otherwise HTML is reduced to its article \
                      content in process. Plain text and JSON come back as they are. \
                      Binary files, including PDFs, are reported rather than returned: \
                      the backend renders pages, it does not extract documents. Private, \
                      loopback and cluster-internal addresses are refused."
            .into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "Absolute http or https URL of a public page"
                },
                "max_chars": {
                    "type": "integer",
                    "description": "Cap on returned characters (default 20000, max 200000)"
                }
            },
            "required": ["url"]
        }),
        implementation: ToolImplementation::Native(Arc::new(move |args| {
            let client = client.clone();
            let base = base.clone();
            Box::pin(async move {
                let raw = args["url"]
                    .as_str()
                    .ok_or_else(|| SFError::Validation("url required".into()))?;
                let url = match check_public_url(raw) {
                    Ok(url) => url,
                    Err(e) => {
                        return Err(e);
                    }
                };
                let client = http_client(client)?;
                let max_chars = args["max_chars"]
                    .as_u64()
                    .unwrap_or(DEFAULT_FETCH_CHARS)
                    .clamp(1, MAX_FETCH_CHARS) as usize;
                // Rendering is a property of the deployment, not of the call:
                // a run that could ask for it would ask for it every time, and
                // the deployment that has a browser has it for exactly the
                // pages the cheap path cannot read.
                if let Some(base) = render_base(base.as_deref()) {
                    return fetch_rendered(&client, &base, &url, max_chars).await;
                }
                let body = fetch_capped(&client, &url, MAX_DOWNLOAD_BYTES)
                    .await
                    .map_err(|e| SFError::Agent(format!("fetching {url} failed: {e}")))?;
                if body.too_large {
                    return Ok(serde_json::json!({
                        "url": url.as_str(),
                        "status": body.status,
                        "content_type": body.content_type,
                        "limit_bytes": MAX_DOWNLOAD_BYTES,
                        "too_large": true,
                    }));
                }
                if !(200..300).contains(&body.status) {
                    return Ok(serde_json::json!({
                        "url": url.as_str(),
                        "status": body.status,
                        "content_type": body.content_type,
                        "bytes": body.bytes.len(),
                        "refused": true,
                    }));
                }
                let content_type = body.content_type.to_ascii_lowercase();
                let is_html = content_type.contains("html");
                let is_textual = is_html
                    || content_type.starts_with("text/")
                    || content_type.contains("json")
                    || content_type.contains("xml");
                if !is_textual {
                    return Ok(serde_json::json!({
                        "url": url.as_str(),
                        "status": body.status,
                        "content_type": body.content_type,
                        "bytes": body.bytes.len(),
                        "binary": true,
                    }));
                }
                let text = String::from_utf8_lossy(&body.bytes).to_string();
                let extracted = if is_html {
                    extract(&text, url.as_str())
                } else {
                    Extracted {
                        title: String::new(),
                        text: normalise(&text),
                        byline: None,
                        site_name: None,
                        published: None,
                        how: "plain",
                    }
                };
                let (text, truncated) = truncate_chars(&extracted.text, max_chars);
                let mut out = serde_json::json!({
                    "url": url.as_str(),
                    "status": body.status,
                    "content_type": body.content_type,
                    "chars": text.chars().count(),
                    "truncated": truncated,
                    "extraction": extracted.how,
                    "text": text,
                });
                if !extracted.title.is_empty() {
                    out["title"] = serde_json::json!(extracted.title);
                }
                for (key, value) in [
                    ("byline", extracted.byline),
                    ("site_name", extracted.site_name),
                    ("published", extracted.published),
                ] {
                    if let Some(value) = value {
                        out[key] = serde_json::json!(value);
                    }
                }
                Ok(out)
            })
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A client that answers from a script and remembers what it was asked, so
    /// a test can state both halves: what came back, and whether a request was
    /// made at all.
    #[derive(Debug, Default)]
    struct Scripted {
        answers: Mutex<Vec<HttpResponse>>,
        asked: Mutex<Vec<HttpRequest>>,
    }

    #[derive(Debug, Clone)]
    struct HttpResponse {
        status: u16,
        content_type: String,
        body: Vec<u8>,
    }

    impl Scripted {
        fn answering(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(vec![HttpResponse {
                    status,
                    content_type: content_type.to_string(),
                    body: body.into(),
                }]),
                asked: Mutex::new(Vec::new()),
            })
        }

        /// Answers in the order the requests arrive, first request first.
        fn answering_in_order(answers: Vec<(u16, &str, String)>) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(
                    answers
                        .into_iter()
                        .rev()
                        .map(|(status, content_type, body)| HttpResponse {
                            status,
                            content_type: content_type.to_string(),
                            body: body.into_bytes(),
                        })
                        .collect(),
                ),
                asked: Mutex::new(Vec::new()),
            })
        }

        fn asked(&self) -> Vec<HttpRequest> {
            self.asked.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for Scripted {
        async fn execute(&self, req: HttpRequest) -> SFResult<cog_core::HttpResponse> {
            self.asked.lock().unwrap().push(req);
            let answer = self
                .answers
                .lock()
                .unwrap()
                .pop()
                .ok_or_else(|| SFError::Agent("no scripted answer left".into()))?;
            let mut headers = HashMap::new();
            headers.insert("content-type".to_string(), answer.content_type);
            Ok(cog_core::HttpResponse {
                status: answer.status,
                headers,
                body: answer.body,
            })
        }
    }

    async fn call(tool: &Tool, args: serde_json::Value) -> SFResult<serde_json::Value> {
        match &tool.implementation {
            ToolImplementation::Native(f) => f(args).await,
            _ => panic!("web tools are native"),
        }
    }

    fn client_of(scripted: &Arc<Scripted>) -> Option<Arc<dyn HttpClient>> {
        Some(scripted.clone() as Arc<dyn HttpClient>)
    }

    #[tokio::test]
    async fn search_asks_the_backend_for_json_and_names_the_hits() {
        let scripted = Scripted::answering(
            200,
            "application/json",
            r#"{"results":[
                 {"title":"Rust 2024","url":"https://example.com/rust","content":"edition notes","engine":"duckduckgo"},
                 {"title":"Async","url":"https://example.com/async","content":"tokio","engine":"bing"}],
               "answers":[],"suggestions":["rust async"]}"#,
        );
        let tool = web_search(
            client_of(&scripted),
            Some("http://searxng.cogneva.svc.cluster.local:8080/".into()),
        );
        let out = call(&tool, serde_json::json!({"query":"rust 2024", "limit": 1}))
            .await
            .expect("a healthy backend answers");

        let asked = scripted.asked();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].method, "GET");
        assert!(
            asked[0]
                .url
                .starts_with("http://searxng.cogneva.svc.cluster.local:8080/search?"),
            "the base is used as given, with its trailing slash removed: {}",
            asked[0].url
        );
        for needed in ["q=rust%202024", "format=json", "limit=1"] {
            assert!(
                asked[0].url.contains(needed),
                "{} missing from {}",
                needed,
                asked[0].url
            );
        }
        assert_eq!(out["count"], 1, "limit caps the list: {out}");
        assert_eq!(out["results"][0]["url"], "https://example.com/rust");
        assert_eq!(out["results"][0]["snippet"], "edition notes");
        assert_eq!(out["suggestions"][0], "rust async");
        assert!(
            out.get("answers").is_none(),
            "an empty list is left out rather than shown as an empty answer: {out}"
        );
    }

    /// A fan-out where nothing answered is a failed lookup, not an empty web.
    ///
    /// The backend reports both the same way -- HTTP 200 and a `results` array
    /// -- and only `unresponsive_engines` says which of the two happened. A
    /// caller that reads `count: 0` as "nothing matched" rephrases a question
    /// that was never asked, and the retry that would have worked never
    /// happens. This is the reading a single-engine deployment produces every
    /// time that engine times out, which is exactly how it looked in
    /// production.
    #[tokio::test]
    async fn search_says_nothing_answered_rather_than_nothing_matched() {
        let silent = r#"{"query":"rust lang","results":[],"answers":[],"suggestions":[],
               "unresponsive_engines":[["bing","timeout"]]}"#
            .to_string();
        let scripted = Scripted::answering_in_order(vec![
            (200, "application/json", silent.clone()),
            (200, "application/json", silent),
        ]);
        let tool = web_search(client_of(&scripted), Some("http://searxng:8080".into()));
        let err = call(&tool, serde_json::json!({"query":"rust lang"}))
            .await
            .expect_err("an empty answer from silent engines is not a result set");
        let msg = err.to_string();
        assert!(
            msg.contains("bing") && msg.contains("timeout"),
            "the error has to name the engine and its own reason, otherwise the caller \
             cannot tell a dead engine from a reachable one that found nothing: {msg}"
        );
        assert!(
            msg.contains("Retry"),
            "the error has to say what to do instead, because the move that looks \
             obvious (rephrase) is the wrong one here: {msg}"
        );
        assert_eq!(
            scripted.asked().len(),
            SEARCH_ATTEMPTS as usize,
            "the silence has to be re-asked before it is reported: one timed-out engine \
             often answers the next request, and this deployment has only one engine"
        );
    }

    /// Silence that clears on the second ask is an answer, not an error.
    ///
    /// This is the case the retry exists for and the one measured on the
    /// deployment: the first query came back empty with the engine timing out,
    /// and the request right after it returned the hits. Reporting the first
    /// one would hand the caller an outage it would then have to retry itself,
    /// through a model turn, at the cost of a whole turn.
    #[tokio::test]
    async fn search_asks_again_when_the_first_ask_found_nothing_and_nobody_answered() {
        let scripted = Scripted::answering_in_order(vec![
            (
                200,
                "application/json",
                r#"{"results":[],"unresponsive_engines":[["bing","timeout"]]}"#.to_string(),
            ),
            (
                200,
                "application/json",
                r#"{"results":[{"title":"Rust","url":"https://rust-lang.org/","content":"fast",
                     "engine":"bing"}],"unresponsive_engines":[]}"#
                    .to_string(),
            ),
        ]);
        let tool = web_search(client_of(&scripted), Some("http://searxng:8080".into()));
        let out = call(&tool, serde_json::json!({"query":"rust"}))
            .await
            .expect("the second ask answered, so this is a hit and not an outage");
        assert_eq!(out["count"], 1);
        assert_eq!(out["results"][0]["url"], "https://rust-lang.org/");
        assert_eq!(
            scripted.asked().len(),
            2,
            "exactly one extra ask: the retry is not a loop that keeps the caller waiting"
        );
    }

    /// One engine down but another one answering is still an answer.
    ///
    /// The distinction above must not swallow this case: hits arrived, so the
    /// query was asked and the caller can act on what came back. The dead
    /// engine rides along as a field rather than turning a usable answer into
    /// an error.
    #[tokio::test]
    async fn search_returns_hits_when_one_engine_failed_and_another_answered() {
        let scripted = Scripted::answering(
            200,
            "application/json",
            r#"{"results":[
                 {"title":"Rust","url":"https://rust-lang.org/","content":"fast","engine":"bing"}],
               "unresponsive_engines":[["sogou","crash"]]}"#,
        );
        let tool = web_search(client_of(&scripted), Some("http://searxng:8080".into()));
        let out = call(&tool, serde_json::json!({"query":"rust"}))
            .await
            .expect("an engine answered, so the query was asked");
        assert_eq!(out["count"], 1);
        assert_eq!(
            out["unresponsive_engines"][0][0], "sogou",
            "the dead engine is still reported next to the usable answer: {out}"
        );
    }

    /// Engines that answered and found nothing is a real empty result.
    ///
    /// Without this case the rule above could be written as "an empty list is
    /// always an error", which would turn a legitimate no-match into a retry
    /// loop.
    #[tokio::test]
    async fn search_reports_an_empty_list_when_every_engine_answered() {
        let scripted = Scripted::answering(
            200,
            "application/json",
            r#"{"query":"q","results":[],"unresponsive_engines":[]}"#,
        );
        let tool = web_search(client_of(&scripted), Some("http://searxng:8080".into()));
        let out = call(&tool, serde_json::json!({"query":"q"}))
            .await
            .expect("engines answered; the web simply had nothing");
        assert_eq!(out["count"], 0);
        assert!(
            out.get("unresponsive_engines").is_none(),
            "an empty list is left out rather than reported as an outage: {out}"
        );
    }

    #[tokio::test]
    async fn search_without_a_backend_names_the_missing_setting() {
        let scripted = Scripted::answering(200, "application/json", "{}");
        let tool = web_search(client_of(&scripted), Some("   ".into()));
        let err = call(&tool, serde_json::json!({"query":"x"}))
            .await
            .expect_err("an unwired deployment cannot search");
        assert!(
            err.to_string().contains(WEB_SEARCH_BASE_ENV),
            "the error must name the setting that is missing: {err}"
        );
        assert!(
            scripted.asked().is_empty(),
            "no request is made when there is nowhere to send it"
        );
    }

    #[tokio::test]
    async fn search_explains_a_backend_that_refuses_json() {
        let scripted = Scripted::answering(403, "text/html", "Forbidden");
        let tool = web_search(client_of(&scripted), Some("http://searx:8080".into()));
        let err = call(&tool, serde_json::json!({"query":"x"}))
            .await
            .expect_err("403 is not an answer");
        let msg = err.to_string();
        assert!(msg.contains("403"), "{msg}");
        assert!(
            msg.contains("search.formats"),
            "the commonest cause is a deployment detail the caller can fix: {msg}"
        );
    }

    #[tokio::test]
    async fn fetch_returns_the_article_and_drops_the_chrome() {
        let html = r#"<html><head><title>On Caches</title><style>.x{}</style></head>
            <body><nav>Home | About</nav>
            <article><h1>On Caches</h1>
            <p>A cache is a promise you keep with a clock.</p>
            <p>Invalidation is the clock.</p></article>
            <script>track()</script></body></html>"#;
        let scripted = Scripted::answering(200, "text/html; charset=utf-8", html);
        let tool = web_fetch(client_of(&scripted), None);
        let out = call(
            &tool,
            serde_json::json!({"url":"https://example.com/caches"}),
        )
        .await
        .expect("a public page is fetched");
        let text = out["text"].as_str().unwrap_or_default();
        assert!(text.contains("promise you keep"), "{out}");
        assert!(text.contains("Invalidation is the clock"), "{out}");
        assert!(!text.contains("track()"), "script text is not prose: {out}");
        assert!(
            !text.contains("Home | About"),
            "navigation is not the page: {out}"
        );
        assert_eq!(out["status"], 200);
        assert_eq!(out["truncated"], false);
    }

    #[tokio::test]
    async fn fetch_refuses_addresses_inside_this_network() {
        let scripted = Scripted::answering(200, "text/html", "secret");
        let tool = web_fetch(client_of(&scripted), None);
        for url in [
            "http://127.0.0.1:8080/api/v1/agents",
            "http://169.254.169.254/latest/meta-data/",
            "http://10.1.2.3/internal",
            "http://192.168.1.1/",
            "http://[::1]/",
            "http://localhost:5432/",
            "http://cogneva.cogneva.svc.cluster.local:8080/api",
            "http://postgres.svc/",
            "http://gateway.internal/",
            "file:///etc/passwd",
            "ftp://example.com/x",
            "https://user:pw@example.com/",
        ] {
            let answer = call(&tool, serde_json::json!({ "url": url })).await;
            assert!(
                answer.is_err(),
                "{url} is not on the public web and must be refused, got {answer:?}"
            );
        }
        assert!(
            scripted.asked().is_empty(),
            "a refused address must not produce a request: {:?}",
            scripted.asked()
        );
    }

    #[tokio::test]
    async fn fetch_stops_at_the_byte_cap_and_says_so() {
        let big = "a".repeat(MAX_DOWNLOAD_BYTES + 1024);
        let scripted = Scripted::answering(200, "text/html", big);
        let tool = web_fetch(client_of(&scripted), None);
        let out = call(&tool, serde_json::json!({"url":"https://example.com/huge"}))
            .await
            .expect("an oversized page is an outcome, not a failure");
        assert_eq!(out["too_large"], true, "{out}");
        assert_eq!(out["limit_bytes"], MAX_DOWNLOAD_BYTES as u64);
        assert!(
            out.get("text").is_none(),
            "no prefix is returned; the caller needs a different address, not a teaser: {out}"
        );
    }

    #[tokio::test]
    async fn fetch_returns_plain_text_and_reports_binary_as_such() {
        let scripted = Scripted::answering(200, "text/plain", "line one\nline two");
        let tool = web_fetch(client_of(&scripted), None);
        let out = call(
            &tool,
            serde_json::json!({"url":"https://example.com/notes.txt"}),
        )
        .await
        .expect("plain text is fetched");
        assert_eq!(out["extraction"], "plain");
        assert!(out["text"].as_str().unwrap().contains("line two"));

        let scripted = Scripted::answering(200, "application/pdf", vec![0u8, 1, 2, 3]);
        let tool = web_fetch(client_of(&scripted), None);
        let out = call(
            &tool,
            serde_json::json!({"url":"https://example.com/doc.pdf"}),
        )
        .await
        .expect("a binary is reported, not refused");
        assert_eq!(out["binary"], true, "{out}");
        assert!(out.get("text").is_none(), "{out}");
    }

    #[tokio::test]
    async fn fetch_truncates_long_text_at_the_requested_size() {
        let body = format!("<html><body><p>{}</p></body></html>", "word ".repeat(200));
        let scripted = Scripted::answering(200, "text/html", body);
        let tool = web_fetch(client_of(&scripted), None);
        let out = call(
            &tool,
            serde_json::json!({"url":"https://example.com/long", "max_chars": 50}),
        )
        .await
        .expect("a long page is fetched");
        assert_eq!(out["truncated"], true, "{out}");
        assert_eq!(
            out["text"].as_str().unwrap().chars().count(),
            50,
            "the returned text is exactly the requested size: {out}"
        );
    }

    /// A configured rendering backend is used, and what comes back is the
    /// backend's Markdown rather than a readability pass over it.
    ///
    /// The `f: fit` filter is asserted because it is the half of the request
    /// that decides how much of the page reaches the model: asking for the raw
    /// body returns navigation and footers, which is exactly what the backend
    /// exists to strip.
    #[tokio::test]
    async fn a_configured_backend_renders_the_page_and_names_the_extraction() {
        let scripted = Scripted::answering(
            200,
            "application/json",
            serde_json::json!({
                "url": "https://example.com/article",
                "filter": "fit",
                "success": true,
                "markdown": "# The Rendered Title\n\nBody text from the browser.",
            })
            .to_string(),
        );
        let tool = web_fetch(
            client_of(&scripted),
            Some("http://render.invalid:11235/".into()),
        );
        let out = call(
            &tool,
            serde_json::json!({"url":"https://example.com/article"}),
        )
        .await
        .expect("a reachable backend renders the page");

        let asked = scripted.asked();
        assert_eq!(asked.len(), 1, "one request, to the backend");
        assert_eq!(asked[0].method, "POST");
        assert_eq!(
            asked[0].url, "http://render.invalid:11235/md",
            "the trailing slash of the base is dropped and `/md` is appended"
        );
        let sent: serde_json::Value =
            serde_json::from_slice(asked[0].body.as_deref().unwrap_or_default())
                .expect("the backend is asked in JSON");
        assert_eq!(sent["url"], "https://example.com/article");
        assert_eq!(
            sent["f"], "fit",
            "the filtered body, not the raw one: {sent}"
        );
        assert!(
            sent["c"].is_string() && !sent["c"].as_str().unwrap().is_empty(),
            "the request carries a cache revision of its own; the backend's default \
             bucket outlives every page it serves: {sent}"
        );

        assert_eq!(out["extraction"], "rendered", "{out}");
        assert_eq!(out["format"], "markdown", "{out}");
        assert_eq!(out["title"], "The Rendered Title", "{out}");
        assert!(out["text"]
            .as_str()
            .unwrap()
            .contains("Body text from the browser."));
        assert_eq!(out["truncated"], false, "{out}");
    }

    /// A backend that was asked for and does not answer is an error, not a
    /// quiet fall back to reading the page in process.
    ///
    /// The fallback is the tempting repair and it is the wrong one: the page a
    /// deployment asked to have rendered is usually the one that reads empty
    /// without a browser, so a silent in-process read returns a thin answer
    /// that looks like a thin page. The assertion on the request count is the
    /// other half — the page itself is never fetched behind the backend's back.
    #[tokio::test]
    async fn an_unreachable_backend_fails_naming_it_instead_of_reading_in_process() {
        let scripted = Arc::new(Scripted::default());
        let tool = web_fetch(
            client_of(&scripted),
            Some("http://render.invalid:11235".into()),
        );
        let err = call(&tool, serde_json::json!({"url":"https://example.com/x"}))
            .await
            .expect_err("a deployment that asked for rendering must not get a raw read");
        let msg = err.to_string();
        assert!(
            msg.contains("http://render.invalid:11235"),
            "the error names the backend so the repair is obvious: {msg}"
        );
        assert_eq!(
            scripted.asked().len(),
            1,
            "exactly one attempt, to the backend; the page is not fetched directly"
        );
    }

    /// A backend that answers but could not read the page carries its reason
    /// through: `success: false` is a verdict about this page, not a transport
    /// failure, and the detail is the only thing that says which.
    #[tokio::test]
    async fn a_backend_that_could_not_read_the_page_returns_its_reason() {
        let scripted = Scripted::answering(
            200,
            "application/json",
            serde_json::json!({
                "url": "https://example.com/paywalled",
                "success": false,
                "detail": "navigation timed out after 30s",
            })
            .to_string(),
        );
        let tool = web_fetch(
            client_of(&scripted),
            Some("http://render.invalid:11235".into()),
        );
        let err = call(
            &tool,
            serde_json::json!({"url":"https://example.com/paywalled"}),
        )
        .await
        .expect_err("a page the backend could not read must not read as an empty page");
        let msg = err.to_string();
        assert!(
            msg.contains("navigation timed out after 30s"),
            "the backend's own reason reaches the caller: {msg}"
        );
        assert!(
            msg.contains("http://render.invalid:11235"),
            "and the answer still names who gave it: {msg}"
        );
    }

    /// A render that came back thin must not be served forever, which means the
    /// revision the request carries has to move without anyone remembering to
    /// move it.
    #[test]
    fn a_renders_cache_revision_moves_on_its_own() {
        let at = |secs: u64| UNIX_EPOCH + std::time::Duration::from_secs(secs);
        let window = RENDER_CACHE_SECS;
        assert_eq!(
            render_cache_revision(at(10 * window)),
            render_cache_revision(at(10 * window + window - 1)),
            "within one bucket the revision is stable, so repeat fetches stay cheap"
        );
        assert_ne!(
            render_cache_revision(at(10 * window)),
            render_cache_revision(at(11 * window)),
            "one window later it is a different revision, so a stale render expires"
        );
        assert_ne!(
            render_cache_revision(UNIX_EPOCH),
            render_cache_revision(at(window)),
            "the first bucket is not the empty string the backend treats as unset"
        );
    }

    /// A 5xx is the renderer failing on its own budget, and one more ask gets the
    /// page. Without the retry the caller sees the failure the backend had
    /// while its browser was cold and reads it as a page that cannot be fetched.
    #[tokio::test]
    async fn a_render_the_backend_failed_on_is_asked_for_again() {
        let scripted = Scripted::answering_in_order(vec![
            (500, "text/plain", "render budget exhausted".into()),
            (
                200,
                "application/json",
                serde_json::json!({
                    "url": "https://example.com/heavy",
                    "success": true,
                    "markdown": "# Heavy Page\n\nIt rendered the second time.",
                })
                .to_string(),
            ),
        ]);
        let tool = web_fetch(
            client_of(&scripted),
            Some("http://render.invalid:11235".into()),
        );
        let out = call(
            &tool,
            serde_json::json!({"url":"https://example.com/heavy"}),
        )
        .await
        .expect("a page the backend failed on once is asked for again");
        assert_eq!(
            scripted.asked().len(),
            2,
            "the failure is repeated once, then the page comes back"
        );
        assert!(out["text"]
            .as_str()
            .unwrap()
            .contains("rendered the second time"));
    }

    /// A page the backend keeps failing on carries how many times it was asked,
    /// so a reader can tell a flaky renderer from a page that never renders.
    #[tokio::test]
    async fn a_render_that_keeps_failing_says_how_many_attempts_it_took() {
        let failing = || (500u16, "text/plain", "render budget exhausted".to_string());
        let scripted = Scripted::answering_in_order(vec![failing(), failing()]);
        let tool = web_fetch(
            client_of(&scripted),
            Some("http://render.invalid:11235".into()),
        );
        let err = call(
            &tool,
            serde_json::json!({"url":"https://example.com/heavy"}),
        )
        .await
        .expect_err("two failures are still a failure");
        let msg = err.to_string();
        assert_eq!(scripted.asked().len(), 2, "the retry is bounded at two");
        assert!(
            msg.contains("on 2 attempts") && msg.contains("render budget exhausted"),
            "the error names the attempt count and the backend's own words: {msg}"
        );
    }

    /// A render that produced no text is a failure, not an empty page.
    ///
    /// This is the shape a PDF takes through the renderer: it opens fine and has
    /// no HTML to reduce. Returning `chars: 0` would be indistinguishable from a
    /// page that genuinely says nothing.
    #[tokio::test]
    async fn a_render_with_no_text_is_an_error_not_an_empty_page() {
        let scripted = Scripted::answering(
            200,
            "application/json",
            serde_json::json!({
                "url": "https://example.com/paper.pdf",
                "success": true,
                "markdown": "\n",
            })
            .to_string(),
        );
        let tool = web_fetch(
            client_of(&scripted),
            Some("http://render.invalid:11235".into()),
        );
        let err = call(
            &tool,
            serde_json::json!({"url":"https://example.com/paper.pdf"}),
        )
        .await
        .expect_err("an empty render is not an empty page");
        let msg = err.to_string();
        assert!(
            msg.contains("extracted no text") && msg.contains("http://render.invalid:11235"),
            "the error says what happened and who did it: {msg}"
        );
    }

    /// A 4xx is a verdict about this URL. Repeating it would spend the same wait
    /// to be told the same thing.
    #[tokio::test]
    async fn a_refused_url_is_not_asked_again() {
        let scripted = Scripted::answering(404, "text/plain", "no such path");
        let tool = web_fetch(
            client_of(&scripted),
            Some("http://render.invalid:11235".into()),
        );
        let err = call(
            &tool,
            serde_json::json!({"url":"https://example.com/missing"}),
        )
        .await
        .expect_err("a 404 from the backend is an error");
        assert_eq!(scripted.asked().len(), 1, "asked once, then reported");
        assert!(err.to_string().contains("HTTP 404"));
    }

    #[test]
    fn entity_decoding_covers_the_forms_pages_use() {
        assert_eq!(
            decode_entities("a &amp; b &lt;c&gt; &#65; &#x42; &nbsp;d"),
            "a & b <c> A B  d"
        );
        assert_eq!(
            decode_entities("100% &unknown; here"),
            "100% &unknown; here"
        );
    }

    #[test]
    fn stripping_keeps_block_boundaries() {
        let text = strip_html("<div>one</div><div>two</div><p>three</p>");
        let normalised = normalise(&text);
        assert_eq!(normalised, "one\ntwo\nthree");
    }
}
