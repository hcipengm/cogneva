//! The model station: the one place where a document body leaves this process, and the
//! shape of the request that carries it.
//!
//! Name rules decide what they can, and the rest of a folder stays where it is -- which is
//! the right default and also the reason this station exists at all: a file called
//! `IMG_0421` has no extension to be read, and a person looking at the folder afterwards
//! cannot tell whether it was skipped on purpose. What this station adds is one question
//! per folder, asked over the channel that audits what goes out.
//!
//! Three things are deliberately not here. **No credentials**: this process holds none
//! (`automountServiceAccountToken: false`, no Secret mounted), so the request carries a
//! placeholder model name and an actor label, and the gateway injects the real upstream
//! and rewrites the model. **No streaming**: one non-streaming answer per run, because the
//! caller needs a finished mapping and nothing else -- streaming would add a second shape
//! of response to parse for no gain. **No restructuring of the answer**: a bucket name the
//! table does not contain is refused downstream, not sanitized here.
//!
//! The whole module is shaped so that the parts that can be tested without a network are
//! pure functions: building the prompt, reading the completion envelope, parsing the
//! mapping. Only [`AuditedChannel::ask`] touches the wire, and the tests that exercise it
//! run against a local fake upstream.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;
use tracing::warn;

/// Where the audited channel lives, e.g.
/// `http://cogneva-security-gateway:8083/v1/chat/completions`. Absent or empty means this
/// station is not wired up, and a run then does what the rules alone can do.
pub const URL_ENV: &str = "HOST_DOCS_AUDITED_LLM_URL";
/// How many bytes of one body the question carries.
pub const ASSIST_MAX_BODY_BYTES_ENV: &str = "HOST_DOCS_ASSIST_BODY_BYTES";
/// How many files one question may cover.
pub const ASSIST_MAX_CANDIDATES_ENV: &str = "HOST_DOCS_ASSIST_MAX_CANDIDATES";

/// A body is evidence of what a file is, not the whole of it: a couple of kilobytes of a
/// text file name the subject as well as the whole file does, and the cost of the call
/// grows with every byte of it.
pub const DEFAULT_ASSIST_MAX_BODY_BYTES: usize = 2048;
/// Past a few dozen unclassified files, the folder is telling the operator something about
/// the rule table rather than about the files.
pub const DEFAULT_ASSIST_MAX_CANDIDATES: usize = 50;
/// An answer to one question. Long enough for a slow upstream, short enough that a run
/// cannot hang on a channel that accepted the connection and then went quiet.
const REQUEST_TIMEOUT_SECS: u64 = 120;

/// The model name this module sends. **It is a placeholder by contract**: the gateway
/// rewrites the model to whatever the current upstream serves, so this string exists only
/// because the OpenAI request shape has a field there.
const PLACEHOLDER_MODEL: &str = "document-organizer";
/// Who spent the tokens. Sent so that the usage this costs is attributable to the document
/// organizer rather than landing in the unattributed remainder of the token reading.
const ACTOR: &str = "sandbox-document-organizer";

/// What the model station did, one cell per decision path; every cell is published, zeros
/// included.
///
/// The cells are cut the same way the audited channel's are -- by **what has to change** --
/// because that is the question an operator brings to this counter:
/// - `switch_off`: bodies may not leave the cluster, so no question was asked. The action
///   is to turn the switch on (see the read face) or to accept that rules alone decide.
/// - `unconfigured`: the switch is on but no channel URL is configured. The action is a
///   deployment change, not a switch.
/// - `no_candidates`: the rules placed everything, so there was nothing to ask about.
///   **This cell is the healthy one**: a table that fits the folder shows up here.
/// - `over_candidates`: there were more unclassified files than one question covers, so
///   nothing was asked. The action is to widen the rule table (or raise the ceiling): a
///   partial answer would make the plan depend on which files happened to fit.
/// - `read_refused`: at least one candidate body could not be obtained. Nothing was asked,
///   for the same reason as the previous cell -- and which body and why is on the read
///   counter, so this cell stays a decision about this station.
/// - `request_failed`: the channel refused, was unreachable, timed out, or answered with
///   something that is not a completion. The action is to look at the channel.
/// - `unparsed`: the completion arrived but did not contain a JSON object of path →
///   bucket. The action is to look at the model (or at the prompt), not at the channel.
/// - `answered`: a mapping came back. It may still be empty -- a model can answer "none of
///   these belong to any of those folders", which is a legitimate answer and reads as
///   `moved_model` staying at zero.
pub const ASSIST_OUTCOMES: [&str; 8] = [
    "switch_off",
    "unconfigured",
    "no_candidates",
    "over_candidates",
    "read_refused",
    "request_failed",
    "unparsed",
    "answered",
];

/// The client for the audited channel. It holds a URL and an HTTP client and nothing else:
/// no key, no upstream, no retry policy. A retry here would be this process deciding how
/// often to send someone's document out, which is not a decision it should be making.
pub struct AuditedChannel {
    url: String,
    http: reqwest::Client,
}

impl AuditedChannel {
    pub fn new(url: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .unwrap_or_default();
        Self { url, http }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Ask one question and read the mapping out of the answer.
    ///
    /// The error is the cell to count, not a message to show: the caller counts it and
    /// carries on with what the rules decided, because a folder that could not be asked
    /// about is a folder whose files stay put -- the fail-safe direction, and one the
    /// operator sees as a reading rather than as a failed organize run.
    pub async fn ask(&self, prompt: &str) -> Result<BTreeMap<String, String>, &'static str> {
        let response = self
            .http
            .post(&self.url)
            .header("content-type", "application/json")
            .header(cog_core::LLM_ACTOR_HEADER, ACTOR)
            .body(request_body(prompt))
            .send()
            .await
            .map_err(|e| {
                warn!(url = %self.url, error = %e, "the audited channel call failed");
                "request_failed"
            })?;
        let status = response.status();
        let raw = response.text().await.map_err(|e| {
            warn!(error = %e, "the audited channel answer could not be read");
            "request_failed"
        })?;
        if !status.is_success() {
            // The body is logged, not returned: it is the upstream's error text, which is
            // what an operator needs to see, and it is not something a caller can act on.
            warn!(status = %status, body = %truncate_for_log(&raw), "the audited channel refused");
            return Err("request_failed");
        }
        let content = extract_content(&raw).ok_or("unparsed")?;
        parse_mapping(&content).ok_or("unparsed")
    }
}

/// The OpenAI completion request this module sends, as a JSON string.
///
/// `stream` is false because the mapping is needed whole; the gateway forwards the shape
/// as given and rewrites the model.
pub fn request_body(prompt: &str) -> String {
    serde_json::json!({
        "model": PLACEHOLDER_MODEL,
        "stream": false,
        "messages": [{"role": "user", "content": prompt}],
    })
    .to_string()
}

/// Build the question. Pure, so the shape of what leaves the process can be read in a
/// test rather than inferred from a live call.
///
/// The file paths and the folder names are the only things in it: no host paths, no scope
/// root, no timestamps -- what leaves is exactly the bodies and the vocabulary they are to
/// be sorted into.
pub fn build_prompt(buckets: &[String], bodies: &[(String, usize, String)]) -> String {
    let mut prompt = String::from(
        "Sort the following files into the listed folders. Answer with one JSON object and \
         nothing else: each key is a file path exactly as written below, each value is one \
         of the folder names. Leave out any file that fits none of them.\n\nFolders: ",
    );
    prompt.push_str(&buckets.join(", "));
    prompt.push_str("\n\n");
    for (path, bytes, body) in bodies {
        prompt.push_str(&format!("--- {path} ({bytes} bytes) ---\n{body}\n\n"));
    }
    prompt
}

/// Cut a body down to what one question carries, on a character boundary.
///
/// Returns whether it was cut, because a prefix is worth saying: the answer is about the
/// beginning of a file, and an operator comparing it against the file itself should know
/// that.
pub fn truncate_body(body: &str, max_bytes: usize) -> (&str, bool) {
    if body.len() <= max_bytes {
        return (body, false);
    }
    let mut end = max_bytes;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    (&body[..end], true)
}

/// The assistant's text out of a completion envelope. `None` when there is no envelope to
/// read, which the caller counts as `unparsed`.
pub fn extract_content(raw: &str) -> Option<String> {
    let parsed: CompletionResponse = serde_json::from_str(raw).ok()?;
    let message = parsed.choices.into_iter().next()?.message?;
    let text = message.content?;
    if text.trim().is_empty() {
        return None;
    }
    Some(text)
}

/// The mapping out of the assistant's text. `None` means the text held no JSON object at
/// all -- that is a different thing from an object that turned out to name nothing (an
/// empty answer is a legitimate answer, and both lead to the same plan: nothing moves).
///
/// Entries whose value is not a string, or is blank, are dropped. What is *not* lenient is
/// the bucket names: this function does not know the table, so it hands back whatever was
/// said and the caller refuses names that are not configured -- one judgement, on the side
/// that holds the table.
pub fn parse_mapping(text: &str) -> Option<BTreeMap<String, String>> {
    let value = first_json_object(text)?;
    let object = value.as_object()?;
    Some(
        object
            .iter()
            .filter_map(|(k, v)| {
                v.as_str()
                    .map(|bucket| (k.clone(), bucket.trim().to_string()))
            })
            .filter(|(_, bucket)| !bucket.is_empty())
            .collect(),
    )
}

/// The first JSON object in the text: a model that wrapped its answer in prose or a code
/// fence is answering the question that was asked, and the fence is formatting.
fn first_json_object(text: &str) -> Option<serde_json::Value> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text.trim()) {
        return Some(value);
    }
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(&text[start..=end]).ok()
}

/// Enough of a body to make an operator's log readable, and short enough that a stray
/// answer cannot put the conversation into a log line.
fn truncate_for_log(text: &str) -> &str {
    truncate_body(text, 512).0
}

#[derive(Debug, Deserialize)]
struct CompletionResponse {
    #[serde(default)]
    choices: Vec<CompletionChoice>,
}

#[derive(Debug, Deserialize)]
struct CompletionChoice {
    #[serde(default)]
    message: Option<CompletionMessage>,
}

#[derive(Debug, Deserialize)]
struct CompletionMessage {
    #[serde(default)]
    content: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hostdocs_organizer::{OrganizeRules, DEFAULT_RULES};

    fn buckets() -> Vec<String> {
        OrganizeRules::parse(DEFAULT_RULES)
            .buckets()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn the_prompt_carries_the_folders_and_the_bodies_and_nothing_else() {
        let prompt = build_prompt(&buckets(), &[("mystery".into(), 4, "data".into())]);
        assert!(prompt.contains("Folders: documents, spreadsheets"));
        assert!(prompt.contains("--- mystery (4 bytes) ---\ndata"));
        // One JSON object, nothing else: the request shape is what carries the answer back.
        let body: serde_json::Value = serde_json::from_str(&request_body(&prompt)).unwrap();
        assert_eq!(body["stream"], false);
        assert_eq!(body["messages"][0]["content"], prompt);
        // No credentials, no upstream, no host path: a placeholder the gateway replaces.
        assert_eq!(body["model"], PLACEHOLDER_MODEL);
        assert!(body.as_object().unwrap().len() == 3);
    }

    #[test]
    fn a_body_longer_than_the_ceiling_is_cut_on_a_character_boundary() {
        let body = "文档".repeat(4); // 12 bytes, four three-byte characters
        let (cut, truncated) = truncate_body(&body, 11);
        assert!(truncated);
        assert_eq!(cut, "文档文"); // 9 bytes: the boundary, not the ceiling
        assert!(body.starts_with(cut));
        // Cutting mid-character would produce a string Rust would refuse to build; this
        // walks back to the boundary instead.
        assert_eq!(truncate_body(&body, 1).0, "");
        let (whole, truncated) = truncate_body("short", 4096);
        assert_eq!((whole, truncated), ("short", false));
    }

    #[test]
    fn the_answer_is_read_out_of_a_completion_envelope() {
        let raw =
            r#"{"choices":[{"message":{"role":"assistant","content":"{\"a\":\"images\"}"}}]}"#;
        assert_eq!(extract_content(raw).unwrap(), r#"{"a":"images"}"#);
        // Nothing to read: no envelope, no choices, no message, empty text.
        assert!(extract_content("not json").is_none());
        assert!(extract_content(r#"{"choices":[]}"#).is_none());
        assert!(extract_content(r#"{"choices":[{}]}"#).is_none());
        assert!(extract_content(r#"{"choices":[{"message":{"content":"  "}}]}"#).is_none());
    }

    #[test]
    fn the_mapping_is_read_from_a_bare_object_or_one_inside_prose() {
        let bare = parse_mapping(r#"{"a":"images","b":"code"}"#).unwrap();
        assert_eq!(bare["a"], "images");
        assert_eq!(bare["b"], "code");
        // A code fence is formatting, and the answer inside it is the answer.
        let fenced = parse_mapping("Sure!\n```json\n{\"a\":\"images\"}\n```\n").unwrap();
        assert_eq!(fenced["a"], "images");
        // No object at all: that is `unparsed`, and it stays apart from an object that named
        // nothing -- one is the model not answering the question, the other is it answering
        // "none of these".
        assert!(parse_mapping("I cannot help with that.").is_none());
        assert!(parse_mapping("").is_none());
        assert!(parse_mapping(r#"["images"]"#).is_none());
        assert!(parse_mapping(r#"{"a":["images"]}"#).unwrap().is_empty());
        assert!(parse_mapping(r#"{"a":"  "}"#).unwrap().is_empty());
        // A name the table does not have is handed back as said: refusing it belongs to the
        // side that knows the table, so the two halves cannot disagree about it.
        assert_eq!(
            parse_mapping(r#"{"a":"../../etc"}"#).unwrap()["a"],
            "../../etc"
        );
    }
}
