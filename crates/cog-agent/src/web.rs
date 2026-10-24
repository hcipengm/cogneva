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

/// The environment variable that names the SearXNG base URL.
///
/// Named here, next to the tool that reads it, so the deployment's env table and
/// the tool cannot disagree about the spelling. Empty means "this deployment
/// chose not to wire web search", which the tool reports at call time rather
/// than hiding by being absent from the list.
pub const WEB_SEARCH_BASE_ENV: &str = "COGNEVA_WEB_SEARCH_BASE";

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

                let mut req = HttpRequest::new("GET", url.clone());
                req.timeout_secs = Some(SEARCH_TIMEOUT_SECS);
                req.headers
                    .insert("Accept".into(), "application/json".into());
                let resp = client.execute(req).await.map_err(|e| {
                    SFError::Agent(format!("web search backend at {base} is unreachable: {e}"))
                })?;
                if !resp.is_success() {
                    let body = String::from_utf8_lossy(&resp.body);
                    let hint = if resp.status == 403 || body.contains("format") {
                        " -- SearXNG refuses JSON by default; add `json` to `search.formats` \
                         in its settings"
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
                let mut out = serde_json::json!({
                    "query": query,
                    "limit": limit,
                    "count": results.len(),
                    "results": results,
                });
                // Answers and suggestions come from the engines' own knowledge
                // cards; when present they answer without a second fetch.
                for key in ["answers", "suggestions", "infoboxes"] {
                    if let Some(value) = body
                        .get(key)
                        .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
                    {
                        out[key] = value.clone();
                    }
                }
                if let Some(broken) = body
                    .get("unresponsive_engines")
                    .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
                {
                    out["unresponsive_engines"] = broken.clone();
                }
                Ok(out)
            })
        })),
    }
}

/// Download one page and return its readable text.
pub fn web_fetch(client: Option<Arc<dyn HttpClient>>) -> Tool {
    Tool {
        name: "web_fetch".into(),
        description: "Fetch one public web page and return its readable text, with the \
                      page title. HTML is reduced to its article content; plain text and \
                      JSON come back as they are. Binary files are reported, not \
                      returned. Private, loopback and cluster-internal addresses are \
                      refused."
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
        let tool = web_fetch(client_of(&scripted));
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
        let tool = web_fetch(client_of(&scripted));
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
        let tool = web_fetch(client_of(&scripted));
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
        let tool = web_fetch(client_of(&scripted));
        let out = call(
            &tool,
            serde_json::json!({"url":"https://example.com/notes.txt"}),
        )
        .await
        .expect("plain text is fetched");
        assert_eq!(out["extraction"], "plain");
        assert!(out["text"].as_str().unwrap().contains("line two"));

        let scripted = Scripted::answering(200, "application/pdf", vec![0u8, 1, 2, 3]);
        let tool = web_fetch(client_of(&scripted));
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
        let tool = web_fetch(client_of(&scripted));
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
