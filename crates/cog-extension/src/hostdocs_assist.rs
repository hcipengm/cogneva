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
/// The most one question may put on the wire: the audited channel's own audit
/// bound, handed to this pod from the same chart value the channel enforces.
/// Absent means this deployment does not state one; the plan then has only what
/// it has measured itself.
pub const AUDIT_BUDGET_ENV: &str = "HOST_DOCS_ASSIST_AUDIT_BUDGET_BYTES";
/// The smallest prefix a question gives one body. Not a ceiling: the plan may
/// give a body more (see [`question_plan`]); this is the amount below which
/// this deployment has said a prefix stops being evidence of what a file is.
pub const ASSIST_MAX_BODY_BYTES_ENV: &str = "HOST_DOCS_ASSIST_BODY_BYTES";
/// The smallest number of files one question covers. Also a seed rather than a
/// ceiling: a folder that needs more asks for more (again [`question_plan`]).
pub const ASSIST_MAX_CANDIDATES_ENV: &str = "HOST_DOCS_ASSIST_MAX_CANDIDATES";

/// A body is evidence of what a file is, not the whole of it: a couple of kilobytes of a
/// text file name the subject as well as the whole file does, and the cost of the call
/// grows with every byte of it.
// cap: bound — 几 KB 正文足以说明这是什么文件，不必整份
pub const DEFAULT_ASSIST_MAX_BODY_BYTES: usize = 2048;
/// Past a few dozen unclassified files, the folder is telling the operator something about
/// the rule table rather than about the files.
// cap: bound — 几十个未分类文件说明的是规则表的问题，不是这些文件的问题
pub const DEFAULT_ASSIST_MAX_CANDIDATES: usize = 50;

/// The two numbers one question runs under, and how they were reached.
///
/// `body_bytes` is a **wire** budget: the bytes a body prefix costs inside the
/// JSON request (see [`wire_len`]), because that is the quantity both the
/// plan's arithmetic and the channel's bound are made of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuestionPlan {
    pub body_bytes: usize,
    /// How many files one question covers in force: this folder's own count
    /// when it can be asked about in full, and otherwise the capacity at this
    /// prefix -- the number the caller compares the folder against to decide
    /// whether to ask at all.
    pub max_candidates: usize,
    /// This question is deliberately larger than anything the channel has
    /// answered yet: the folder asks for more than the measured window, and
    /// nothing has failed at this size. Answering it raises the window;
    /// failing it caps the next one.
    pub probe: bool,
    /// At least one question has been answered or failed, so the numbers are a
    /// function of that rather than of the deployment's seeds.
    pub derived: bool,
}

/// The plan for one question.
///
/// The window is what one question is expected to carry without probing. It is
/// **at least the deployment's own practice** -- the two seeds multiplied into
/// a request, plus the prompt's fixed text -- so derivation never makes the
/// deployment ask less than it declared, and the first folders behave exactly
/// as they did before this was derived. An answered request raises the window
/// to its size; a stated audit bound caps it at what the channel will accept.
/// A failure is the only evidence the bytes were too many for what sits behind
/// the channel, and a network blip looks exactly like an overflow from here, so
/// a failure never shrinks the window -- it only caps a probe at one byte under
/// the size that failed.
///
/// The body prefix is at least the declared seed and at most what a typical
/// file in this folder needs, so the question carries whole files when it can
/// and never less of a body than the deployment called evidence. When the whole
/// folder does not fit the window at that prefix, the question is asked anyway
/// **if** the evidence allows it: that probe is how the window grows to meet
/// the folders this deployment actually has. What "allows it" means is the
/// audit bound when the deployment states one, and otherwise the largest size
/// the evidence itself supports -- one byte under a request that failed. With
/// neither, the deployment's own practice is the ceiling: a bigger question
/// would be a guess about what sits behind the channel. When the question
/// cannot be asked, `max_candidates` is the capacity and the caller refuses --
/// a partial answer would make the plan depend on which files happened to fit.
pub fn question_plan(
    seed_body_bytes: usize,
    seed_candidates: usize,
    audit_budget_bytes: Option<usize>,
    scaffold_bytes: usize,
    sizes: &[usize],
    accepted_wire_bytes: Option<usize>,
    failed_wire_bytes: Option<usize>,
) -> QuestionPlan {
    let seed_body = seed_body_bytes.max(1);
    let seed_candidates = seed_candidates.max(1);
    let n = sizes.len().max(1);
    let seed_window =
        scaffold_bytes.saturating_add(seed_body.saturating_mul(seed_candidates));
    // A floor can only be lowered by the channel's own stated bound: evidence
    // about one request is not evidence that the deployment's practice is
    // wrong, and a failure it cannot explain (a blip) must not leave the
    // station refusing every folder for the length of the window.
    let window = accepted_wire_bytes
        .unwrap_or(seed_window)
        .max(seed_window)
        .min(audit_budget_bytes.unwrap_or(usize::MAX));
    let typical = median(sizes).unwrap_or(seed_body);
    let per_file = window.saturating_sub(scaffold_bytes) / n;
    let body_bytes = seed_body.max(typical.min(per_file)).max(1);
    let capacity = window.saturating_sub(scaffold_bytes) / body_bytes;
    // What this folder actually costs: a body shorter than the prefix costs its
    // own length, so two small files can fit a window that the prefix's full
    // price would call too small. Estimating from the prefix alone would make
    // the plan probe a size it has already been answered at.
    let needed = scaffold_bytes.saturating_add(
        sizes
            .iter()
            .map(|size| (*size).min(body_bytes))
            .sum::<usize>(),
    );
    // A probe may not repeat a size that already failed (`failed - 1` is the
    // largest one still allowed), and may not exceed a stated audit bound.
    // Without either, the practice window is the ceiling.
    let probe_cap = match (audit_budget_bytes, failed_wire_bytes) {
        (Some(budget), Some(failed)) => budget.min(failed.saturating_sub(1)),
        (Some(budget), None) => budget,
        (None, Some(failed)) => failed.saturating_sub(1),
        (None, None) => window,
    };
    let fits = needed <= window;
    let probe = !fits && needed <= probe_cap;
    QuestionPlan {
        body_bytes,
        max_candidates: if fits || probe { n } else { capacity },
        probe,
        derived: accepted_wire_bytes.is_some() || failed_wire_bytes.is_some(),
    }
}

/// The upper median of a set of sizes; `None` for an empty set.
fn median(sizes: &[usize]) -> Option<usize> {
    if sizes.is_empty() {
        return None;
    }
    let mut sorted = sizes.to_vec();
    sorted.sort_unstable();
    sorted.get(sorted.len() / 2).copied()
}

/// What one character costs inside the JSON request.
///
/// `serde_json` escapes quotes and backslashes with one extra byte, the short
/// control characters as two (`\n`), and the rest as six (`\u00XX`); everything
/// else is its UTF-8 length. The channel's bound counts the request body, so
/// the plan has to count the same bytes the wire will carry.
fn escaped_len(ch: char) -> usize {
    match ch {
        '"' | '\\' => 2,
        '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 2,
        c if (c as u32) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// How many wire bytes a body costs.
pub fn wire_len(body: &str) -> usize {
    body.chars().map(escaped_len).sum()
}

/// Cut a body to a wire budget, on a character boundary.
///
/// Returns whether it was cut, for the same reason [`truncate_body`] does: the
/// answer was about the beginning of a file, and an operator comparing it
/// against the file should know that.
pub fn truncate_to_wire(body: &str, max_wire_bytes: usize) -> (&str, bool) {
    let mut used = 0;
    for (index, ch) in body.char_indices() {
        let cost = escaped_len(ch);
        if used + cost > max_wire_bytes {
            return (&body[..index], true);
        }
        used += cost;
    }
    (body, false)
}
/// An answer to one question. Long enough for a slow upstream, short enough that a run
/// cannot hang on a channel that accepted the connection and then went quiet.
// cap: bound — 一次问答的墙：够慢上游，又不至于挂在接了连接不说话的通道上
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
/// - `over_candidates`: there were more unclassified files than one question covers at
///   the prefix they need (the plan in force is published beside this counter), so
///   nothing was asked. The action is to widen the rule table, or to raise the seeds
///   while the plan is still the declaration: a partial answer would make the plan depend
///   on which files happened to fit.
/// - `read_refused`: at least one candidate body could not be obtained. Nothing was asked,
///   for the same reason as the previous cell -- and which body and why is on the read
///   counter, so this cell stays a decision about this station.
/// - `refused_by_channel`: the channel judged this request and refused it, and said under
///   which of its own cells. The action is to look at the document (or at the switch), not
///   at the network -- which the log line names, because the channel's cell is logged
///   rather than mapped into a second table here. Telling this cell apart from
///   `request_failed` is why the channel names its refusals on the response: the status is
///   a 403 either way, so the status alone would fold "a security decision was made" into
///   "the network is unhappy".
/// - `request_failed`: nothing judged the request -- unreachable, timed out, or answered
///   with something that is not a completion. The action is to look at the channel.
/// - `unparsed`: the completion arrived but did not contain a JSON object of path →
///   bucket. The action is to look at the model (or at the prompt), not at the channel.
/// - `answered`: a mapping came back. It may still be empty -- a model can answer "none of
///   these belong to any of those folders", which is a legitimate answer and reads as
///   `moved_model` staying at zero.
pub const ASSIST_OUTCOMES: [&str; 9] = [
    "switch_off",
    "unconfigured",
    "no_candidates",
    "over_candidates",
    "read_refused",
    "refused_by_channel",
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
            .header("content-type", cog_core::CONTENT_TYPE_JSON)
            .header(cog_core::LLM_ACTOR_HEADER, ACTOR)
            .body(request_body(prompt))
            .send()
            .await
            .map_err(|e| {
                warn!(url = %self.url, error = %e, "the audited channel call failed");
                "request_failed"
            })?;
        let status = response.status();
        // Read the channel's cell before the body, because reading the body consumes the
        // response. Its absence is itself information: nothing named a reason, so nothing
        // judged the request and this is not the channel's refusal to explain.
        let channel_cell = response
            .headers()
            .get(cog_core::host_documents::AUDIT_CELL_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let raw = response.text().await.map_err(|e| {
            warn!(error = %e, "the audited channel answer could not be read");
            "request_failed"
        })?;
        if !status.is_success() {
            // The body is logged, not returned: it is the upstream's error text, which is
            // what an operator needs to see, and it is not something a caller can act on.
            // The channel's cell is logged rather than translated: this side would need a
            // table of the other side's vocabulary to translate it, and a stale copy of
            // that table reads as a confident wrong answer.
            let Some(cell) = channel_cell else {
                warn!(status = %status, body = %truncate_for_log(&raw), "the audited channel call failed without a judgement");
                return Err("request_failed");
            };
            warn!(
                status = %status,
                channel_cell = %cell,
                body = %truncate_for_log(&raw),
                "the audited channel refused this request"
            );
            return Err("refused_by_channel");
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

/// An upper bound on the fixed part of a request for these paths: the prompt's
/// own text and one header per file, with every body empty and every printed
/// byte count at the widest a `usize` gets. It exists so a caller can bound how
/// much of each body it has to keep before the plan -- which needs the exact
/// counts, and so the bodies themselves -- is known.
pub fn scaffold_upper_bound(buckets: &[String], paths: &[String]) -> usize {
    let widest: Vec<(String, usize, String)> = paths
        .iter()
        .map(|path| (path.clone(), usize::MAX, String::new()))
        .collect();
    request_body(&build_prompt(buckets, &widest)).len()
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

    /// The plan counts the bytes the channel's bound counts: the serialized
    /// request, looked at from both sides. If these two disagreed, a plan could
    /// call a request "inside the window" while the channel refused it.
    #[test]
    fn the_plan_counts_the_bytes_the_wire_will_carry() {
        assert_eq!(wire_len("a"), 1);
        assert_eq!(wire_len("\""), 2, "a quote is escaped with one extra byte");
        assert_eq!(wire_len("\n"), 2, "a short control character costs two");
        assert_eq!(wire_len("\u{1}"), 6, "the rest of the controls cost six");
        assert_eq!(wire_len("文"), 3, "everything else is its UTF-8 length");

        let body = "a\"b\nc\u{1}文".to_string();
        let with = request_body(&build_prompt(&buckets(), &[("f".into(), 4, body.clone())]));
        let without = request_body(&build_prompt(&buckets(), &[("f".into(), 4, String::new())]));
        assert_eq!(
            with.len() - without.len(),
            wire_len(&body),
            "the body's cost inside the request is what wire_len says it is"
        );

        // Cutting on that budget walks to a character boundary and reports it.
        assert_eq!(truncate_to_wire("aa\"bb", 3), ("aa", true));
        assert_eq!(truncate_to_wire("\"ab", 2), ("\"", true));
        assert_eq!(truncate_to_wire("文档", 3), ("文", true));
        assert_eq!(truncate_to_wire("short", 5), ("short", false));
        assert_eq!(truncate_to_wire("", 0), ("", false));

        // The pre-plan bound on the fixed part is a bound on the real one.
        let bound = scaffold_upper_bound(&buckets(), &["mystery".to_string()]);
        let actual = request_body(&build_prompt(
            &buckets(),
            &[("mystery".into(), 4, String::new())],
        ))
        .len();
        assert!(bound >= actual);
    }

    /// The plan is a function of what the channel answered: before anything was
    /// measured it is the deployment's practice, after an answer it covers what
    /// that answer showed the channel can carry. Nothing restarts.
    #[test]
    fn the_plan_follows_the_question_the_channel_answered() {
        let (seed_body, seed_candidates, scaffold) = (100, 2, 10);
        // Practice alone: two files of 100 wire bytes, plus 10 bytes of prompt.
        let quiet = question_plan(
            seed_body,
            seed_candidates,
            None,
            scaffold,
            &[50, 60],
            None,
            None,
        );
        assert_eq!(
            (quiet.body_bytes, quiet.max_candidates, quiet.probe, quiet.derived),
            (100, 2, false, false),
            "with nothing measured the declaration is the plan, and the reading says so"
        );

        // The channel answered a 1500-byte question: the next question may be
        // that large, and the same three files now fit in one question.
        let answered = question_plan(
            seed_body,
            seed_candidates,
            None,
            scaffold,
            &[700, 800],
            Some(1500),
            None,
        );
        assert_eq!(
            (answered.body_bytes, answered.max_candidates),
            (745, 2),
            "a body may now take its even share of the answered window"
        );
        assert!(answered.derived && !answered.probe);

        let third = question_plan(
            seed_body,
            seed_candidates,
            None,
            scaffold,
            &[700, 800, 900],
            Some(1500),
            None,
        );
        assert_eq!(third.max_candidates, 3);
        assert!(!third.probe);
        assert!(
            third.body_bytes < answered.body_bytes,
            "three files share the same window"
        );
    }

    /// A question larger than the evidence is a probe, and a probe only runs
    /// when something says how large the channel may be: its stated audit
    /// bound, or a request that failed above the size being tried. Without
    /// either, the practice window is the ceiling and the folder is refused.
    #[test]
    fn a_probe_needs_a_bound_or_a_failure_to_stand_on() {
        let (seed_body, seed_candidates, scaffold) = (100, 2, 10);
        let sizes = [1000, 1000, 1000];
        let unprompted = question_plan(
            seed_body,
            seed_candidates,
            None,
            scaffold,
            &sizes,
            None,
            None,
        );
        assert!(!unprompted.probe);
        assert_eq!(unprompted.max_candidates, 2, "the seed's capacity");
        // Three files do not fit two: the caller refuses rather than ask part
        // of the folder.
        assert!(sizes.len() > unprompted.max_candidates);

        // A stated audit bound lets the probe through...
        let bounded = question_plan(
            seed_body,
            seed_candidates,
            Some(400),
            scaffold,
            &sizes,
            None,
            None,
        );
        assert!(bounded.probe);
        assert_eq!(bounded.max_candidates, 3);
        assert_eq!(bounded.body_bytes, 100, "the declared prefix is the floor");

        // ...but not up to a size that already failed: one byte under it is
        // the most the evidence allows.
        let failed_below = question_plan(
            seed_body,
            seed_candidates,
            Some(400),
            scaffold,
            &sizes,
            None,
            Some(250),
        );
        assert!(!failed_below.probe);
        assert_eq!(failed_below.max_candidates, 2);

        // Without a stated bound a failure is itself the bound: 310 bytes is
        // under the 400 that failed, so the probe runs.
        let failure_only = question_plan(
            seed_body,
            seed_candidates,
            None,
            scaffold,
            &sizes,
            None,
            Some(400),
        );
        assert!(failure_only.probe);

        // A tiny stated bound is the one thing that can take the window below
        // the deployment's practice -- and even then the prefix is not cut
        // below what the deployment called evidence.
        let tiny = question_plan(
            seed_body,
            seed_candidates,
            Some(50),
            scaffold,
            &[1000, 1000],
            None,
            None,
        );
        assert_eq!(tiny.body_bytes, seed_body);
        assert_eq!(tiny.max_candidates, 0);
        assert!(2 > tiny.max_candidates);

        // The prefix is a ceiling per body, not a bill per file: two files
        // shorter than the prefix fit a window the prefix's full price would
        // call too small. Reading it as an overflow would probe a size the
        // channel has already answered.
        let short = question_plan(8, 1, None, 0, &[8, 7], Some(15), None);
        assert_eq!((short.max_candidates, short.probe), (2, false));
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
