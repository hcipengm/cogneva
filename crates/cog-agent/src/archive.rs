//! Writing every tool call into the platform's memory Raw layer, and the
//! readings that say what became of it.
//!
//! A tool call's arguments and its whole result are the most expensive
//! information in a run and the least reproducible: replaying the conversation
//! re-buys the model calls but never re-runs the tools. They therefore get a
//! permanent copy the moment the call returns, before the result is handed to
//! the model — the statement, the arguments and the result, one object per
//! call. This is the same bargain the event stream makes, with the difference
//! that the event log is a bounded ring (it sweeps by row count) and the Raw
//! layer is not: once the ring passes a call, this object is the only copy left.
//!
//! The same write also covers the case the cut needs. When the result does not
//! fit the context window the model is shown a prefix and a marker, and the
//! marker carries the `artifact://` reference of the object just written — so
//! what the model did not see is one fetch away instead of gone. Archiving only
//! the oversized ones would leave every shorter result with no permanent copy
//! at all, which is the loss this module exists to remove.
//!
//! The write cannot go through an in-process [`cog_core::MemoryBackend`]: the
//! process that runs the loops holds no memory dataset on purpose (it is
//! switched off there), so a handle resolved in-process would be absent exactly
//! where the loops run. It goes over HTTP to the platform memory API instead,
//! which is also the only route a credential-free pod has. The base URL points
//! at the API's **internal, zero-credential face** — a port that serves the
//! memory ingest and raw routes and nothing else — not the operator API, whose
//! memory routes are gated by role: a caller with no token is refused there
//! before any handler runs, so the call would never land. On this face there is
//! no token to carry and no claims to read, so the write lands in the default
//! namespace by construction.
//!
//! The readings live on this handle rather than beside it because they are the
//! fates of one write: `archived` counts the records that landed and
//! `archive_failure` the ones that did not, under the cause. The two have to
//! add up — every call either landed or is counted as a failure under some
//! cause — or the share of calls whose record landed reads as a ceiling rather
//! than a share. An unconfigured base URL is one of those causes, not an
//! exemption: with nowhere to write, the call has no permanent copy and that is
//! what the reading must say. `truncated` and `truncated_chars` are about the
//! conversation rather than the record — how often a result had to be cut and
//! how much of it the model did not see.

use std::collections::HashMap;
use std::sync::Arc;

use cog_core::metric_names::{
    TOOL_CALL_ARCHIVED_TOTAL, TOOL_OUTPUT_ARCHIVE_FAILED_TOTAL, TOOL_OUTPUT_TRUNCATED_CHARS_TOTAL,
    TOOL_OUTPUT_TRUNCATED_TOTAL,
};
use cog_core::{
    artifact_uri, HttpClient, HttpRequest, MetricsBackend, SFError, SFResult,
    DEFAULT_MEMORY_NAMESPACE, MEMORY_API_BASE_ENV, MEMORY_INGEST_PATH,
};

/// Why an archive attempt did not land. A closed set: the label value names the
/// cause, and a reader choosing between "set the base URL", "fix the route to
/// the memory API", and "read the API's refusal" is choosing between these
/// three. It is closed on purpose — every call's record either lands or is
/// counted under exactly one of these, so the archived counter against the
/// failure counter is the share of calls whose record landed.
pub mod cause {
    /// No base URL is configured, so nothing was attempted.
    pub const UNCONFIGURED: &str = "unconfigured";
    /// The call did not complete: the memory API was unreachable.
    pub const UNREACHABLE: &str = "unreachable";
    /// The memory API answered, and refused the write.
    pub const REFUSED: &str = "refused";
}

/// The surface a run uses to give one tool call a permanent copy, and to record
/// the readings that say what became of it.
#[async_trait::async_trait]
pub trait ToolOutputArchive: Send + Sync + std::fmt::Debug {
    /// Store one tool call's record — its arguments and its whole result — as
    /// the raw source `id`, and return its `artifact://` reference.
    ///
    /// Called for every call, not only for results too large for the window: a
    /// result that fits still exists nowhere else once the event log's ring
    /// passes it. The attempt publishes its own outcome readings — the cause is
    /// known only here, and a caller left to classify the error would either
    /// guess or fold "misconfigured" and "the API refused" into one cell. The
    /// `Err` is still returned so the caller can say in its own log why the
    /// truncation marker carries no reference.
    async fn archive(
        &self,
        tool: &str,
        id: &str,
        content_type: &str,
        text: &str,
    ) -> SFResult<String>;

    /// Record that `tool`'s output was truncated to fit the context window, and
    /// how many characters of it the model will not see.
    ///
    /// The two go in together because they are one event: a caller that counted
    /// the cut but not its size would publish a reading that cannot tell a
    /// result cut by ten characters from one cut by a hundred thousand, and the
    /// character count is only available where the cut is decided.
    async fn record_truncation(&self, tool: &str, dropped_chars: usize);
}

/// Archive tool outputs by POSTing them to the platform memory API.
pub struct HttpToolOutputArchive {
    client: Arc<dyn HttpClient>,
    /// The memory API base. `None` when the deployment names none, which makes
    /// [`Self::archive`] fail under [`cause::UNCONFIGURED`] — the truncation is
    /// still counted, so a deployment that cannot archive says so instead of
    /// looking like one whose outputs all fit.
    base: Option<String>,
    /// Where the two readings go. Absent in embedded and test use, where there
    /// is no metrics backend to hold them; the archive itself does not depend on
    /// it.
    metrics: Option<Arc<dyn MetricsBackend>>,
}

// The metrics backend is not `Debug`, so the derived impl does not compile. The
// two fields worth reading in a log line are spelled out instead.
impl std::fmt::Debug for HttpToolOutputArchive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpToolOutputArchive")
            .field("base", &self.base)
            .field("has_metrics", &self.metrics.is_some())
            .finish()
    }
}

/// Why one archive POST did not produce a reference. The three are the
/// deployment-shaped failures [`cause`] names; a caller that reports its own
/// readings maps them itself, a caller with none only needs to know the write
/// did not land.
#[derive(Debug)]
pub enum WriteFailure {
    /// No base URL was configured, so no request was made.
    Unconfigured,
    /// The request did not complete.
    Unreachable(SFError),
    /// The API answered and refused the write.
    Refused(SFError),
}

impl WriteFailure {
    /// The error to hand back to a caller that has no reading of its own.
    pub fn into_error(self) -> SFError {
        match self {
            WriteFailure::Unconfigured => SFError::Config(format!(
                "{MEMORY_API_BASE_ENV} is not set: nothing can be archived under this deployment"
            )),
            WriteFailure::Unreachable(e) => e,
            WriteFailure::Refused(e) => e,
        }
    }
}

/// POST one raw source to the platform memory API and return the reference a
/// reader fetches it back by.
///
/// One delivery shape for every caller that stores raw bytes from a pod: this
/// is the same request the tool-output archive makes, so when the API's ingest
/// contract moves there is one place to change rather than one per producer.
/// It records nothing. The fates of a tool call's record are the readings of
/// the handle below, and they are a closed pair over *calls* -- a producer
/// writing something that is not a tool call has no cell there to land in, and
/// adding its writes to those counters would make the share of calls that
/// landed unreadable.
pub async fn post_raw(
    client: &dyn HttpClient,
    base: Option<&str>,
    id: &str,
    content_type: &str,
    text: &str,
) -> Result<String, WriteFailure> {
    let Some(base) = base.filter(|b| !b.trim().is_empty()) else {
        return Err(WriteFailure::Unconfigured);
    };
    let url = format!("{}{MEMORY_INGEST_PATH}", base.trim_end_matches('/'));
    // `purpose: "archive"` is written here rather than taken from the caller:
    // every call site of this function stores bytes and none of them learned
    // anything from them, so the intent belongs to the delivery shape. Without
    // it the API reads the body as a learning payload and answers with a
    // summary of the first 200 characters, which is a preview presented as a
    // claim.
    let body = serde_json::json!({
        "id": id,
        "content_type": content_type,
        "text": text,
        "purpose": "archive",
    });
    // A body that will not serialize is a bug in this function, not a property
    // of the deployment, and there is no cause for it.
    let req = match HttpRequest::post(url)
        .json(&body)
        .map(|r| r.timeout(ARCHIVE_TIMEOUT_SECS))
    {
        Ok(req) => req,
        Err(e) => return Err(WriteFailure::Unreachable(e)),
    };

    let resp = client
        .execute(req)
        .await
        .map_err(WriteFailure::Unreachable)?;
    if !resp.is_success() {
        return Err(WriteFailure::Refused(SFError::Config(format!(
            "memory API refused the archive of {id:?}: HTTP {}",
            resp.status
        ))));
    }
    // The reference is spelling, not a value the API returns: the pod is
    // anonymous, so the API filed the source under the default namespace, and
    // the reference says where a reader should ask for it.
    Ok(artifact_uri(DEFAULT_MEMORY_NAMESPACE, id))
}

impl HttpToolOutputArchive {
    pub fn new(
        client: Arc<dyn HttpClient>,
        base: Option<String>,
        metrics: Option<Arc<dyn MetricsBackend>>,
    ) -> Self {
        Self {
            client,
            base,
            metrics,
        }
    }

    async fn count(&self, name: cog_core::MetricName, value: f64, labels: HashMap<String, String>) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        // A reading that cannot be written does not fail the call it describes:
        // the truncation already happened, and the archive may well have
        // succeeded. Losing the count is worth a line, not the output.
        if let Err(e) = metrics.record_counter(name, value, labels).await {
            tracing::warn!(error = %e, "tool-output archive: could not record outcome");
        }
    }
}

#[async_trait::async_trait]
impl ToolOutputArchive for HttpToolOutputArchive {
    async fn archive(
        &self,
        tool: &str,
        id: &str,
        content_type: &str,
        text: &str,
    ) -> SFResult<String> {
        let base = self.base.as_deref();
        let resp = match post_raw(self.client.as_ref(), base, id, content_type, text).await {
            Ok(uri) => {
                self.count(TOOL_CALL_ARCHIVED_TOTAL, 1.0, tool_labels(tool))
                    .await;
                return Ok(uri);
            }
            Err(f) => f,
        };
        let (cause, err) = match resp {
            WriteFailure::Unconfigured => (
                cause::UNCONFIGURED,
                SFError::Config(format!(
                    "{MEMORY_API_BASE_ENV} is not set: a truncated tool output has nowhere to be archived"
                )),
            ),
            WriteFailure::Unreachable(e) => (cause::UNREACHABLE, e),
            WriteFailure::Refused(e) => (cause::REFUSED, e),
        };
        self.record_failure(tool, cause).await;
        Err(err)
    }

    async fn record_truncation(&self, tool: &str, dropped_chars: usize) {
        let labels = tool_labels(tool);
        self.count(TOOL_OUTPUT_TRUNCATED_TOTAL, 1.0, labels.clone())
            .await;
        self.count(
            TOOL_OUTPUT_TRUNCATED_CHARS_TOTAL,
            dropped_chars as f64,
            labels,
        )
        .await;
    }
}

impl HttpToolOutputArchive {
    async fn record_failure(&self, tool: &str, cause: &str) {
        let mut labels = tool_labels(tool);
        labels.insert(crate::LABEL_CAUSE.to_string(), cause.to_string());
        self.count(TOOL_OUTPUT_ARCHIVE_FAILED_TOTAL, 1.0, labels)
            .await;
    }
}

/// The label set every reading in this module carries. One function so the
/// truncation count, its character total and the failure family cannot end up
/// keyed by different things -- three series that disagree about their labels
/// cannot be subtracted from one another, and the landed share is a subtraction.
fn tool_labels(tool: &str) -> HashMap<String, String> {
    HashMap::from([(crate::LABEL_TOOL.to_string(), tool.to_string())])
}

/// How long one archive POST may take before it is abandoned.
///
/// The call sits on the tool-result path, so its budget is a share of a turn
/// rather than a background job's: long enough for a healthy API to store an
/// oversized payload, short enough that an API that stopped answering delays the
/// turn instead of wedging it.
// cap: bound — 长到容得下一份过大的载荷，短到让一个不再应答的 API 拖慢这一回合而不是把它卡死
// wall: 等=归档 API 收下一次写入 / 等不到=这一回合的归档失败，账上记一条失败；**没有半份载荷被当成已归档** / 续跑=能，重来是一次新的归档请求
const ARCHIVE_TIMEOUT_SECS: u64 = 20;

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{HttpResponse, MetricName};
    use std::sync::Mutex;

    #[derive(Debug)]
    struct Recording {
        status: u16,
        fail: bool,
        calls: Mutex<Vec<HttpRequest>>,
    }

    #[async_trait::async_trait]
    impl HttpClient for Recording {
        async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
            self.calls.lock().unwrap().push(req);
            if self.fail {
                return Err(SFError::IO("connection refused".into()));
            }
            Ok(HttpResponse {
                status: self.status,
                headers: HashMap::new(),
                body: b"{}".to_vec(),
            })
        }
    }

    /// 一次计数：名字、增量、标签集。
    type Reading = (MetricName, f64, HashMap<String, String>);

    #[derive(Debug, Default)]
    struct Counters(Mutex<Vec<Reading>>);

    #[async_trait::async_trait]
    impl MetricsBackend for Counters {
        async fn record_gauge(
            &self,
            _name: MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn record_counter(
            &self,
            name: MetricName,
            value: f64,
            labels: HashMap<String, String>,
        ) -> SFResult<()> {
            self.0.lock().unwrap().push((name, value, labels));
            Ok(())
        }
        async fn record_histogram(
            &self,
            _name: MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_gauge_latest(&self, _name: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_totals(&self, _name: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }
        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn health_check(&self) -> SFResult<()> {
            Ok(())
        }
    }

    fn client(status: u16, fail: bool) -> Arc<Recording> {
        Arc::new(Recording {
            status,
            fail,
            calls: Mutex::new(Vec::new()),
        })
    }

    #[tokio::test]
    async fn archive_posts_the_full_text_and_returns_an_artifact_reference() {
        let c = client(200, false);
        let archiver =
            HttpToolOutputArchive::new(c.clone(), Some("http://cogneva:8080".into()), None);
        let uri = archiver
            .archive(
                "read_file",
                "task-0001-0002",
                cog_core::CONTENT_TYPE_JSON,
                "{\"big\":true}",
            )
            .await
            .expect("archive succeeds");
        assert_eq!(uri, "artifact://default/task-0001-0002");

        let calls = c.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let req = &calls[0];
        assert_eq!(req.method, "POST");
        assert_eq!(req.url, "http://cogneva:8080/api/v1/memory/ingest");
        let body = String::from_utf8(req.body.clone().unwrap()).unwrap();
        assert!(body.contains("\"id\":\"task-0001-0002\""));
        assert!(body.contains("{\\\"big\\\":true}"));
        // Dropping this field turns the archive back into a learning write:
        // the API would answer with a 200-character preview of a tool output
        // and store it as a summary.
        assert!(body.contains("\"purpose\":\"archive\""));
    }

    #[tokio::test]
    async fn each_attempt_publishes_a_cause_of_its_own() {
        // A refusal and an unreachable API are the same `Err` to the caller but
        // different repairs, so the label has to be read from the attempt.
        for (status, fail, expected) in [
            (400u16, false, cause::REFUSED),
            (200, true, cause::UNREACHABLE),
        ] {
            let c = client(status, fail);
            let counters = Arc::new(Counters::default());
            let archiver = HttpToolOutputArchive::new(
                c,
                Some("http://cogneva:8080".into()),
                Some(counters.clone()),
            );
            assert!(archiver
                .archive("run_command", "x", "text/plain", "y")
                .await
                .is_err());
            let recorded = counters.0.lock().unwrap();
            assert_eq!(recorded.len(), 1, "one attempt, one failure reading");
            assert_eq!(recorded[0].0, TOOL_OUTPUT_ARCHIVE_FAILED_TOTAL);
            assert_eq!(
                recorded[0].2.get(crate::LABEL_TOOL).map(String::as_str),
                Some("run_command")
            );
            assert_eq!(
                recorded[0].2.get(crate::LABEL_CAUSE).map(String::as_str),
                Some(expected)
            );
        }
    }

    #[tokio::test]
    async fn without_a_base_url_no_request_is_made_and_the_failure_names_the_variable() {
        let c = client(200, false);
        let counters = Arc::new(Counters::default());
        let archiver = HttpToolOutputArchive::new(c.clone(), None, Some(counters.clone()));
        let err = archiver
            .archive("read_file", "x", "text/plain", "y")
            .await
            .expect_err("unconfigured");
        assert!(err.to_string().contains(MEMORY_API_BASE_ENV));
        assert!(c.calls.lock().unwrap().is_empty());
        let recorded = counters.0.lock().unwrap();
        assert_eq!(
            recorded[0].2.get(crate::LABEL_CAUSE).map(String::as_str),
            Some(cause::UNCONFIGURED)
        );
    }

    #[tokio::test]
    async fn a_successful_archive_records_the_archived_call_and_no_failure() {
        let c = client(200, false);
        let counters = Arc::new(Counters::default());
        let archiver = HttpToolOutputArchive::new(
            c,
            Some("http://cogneva:8080".into()),
            Some(counters.clone()),
        );
        assert!(archiver
            .archive("read_file", "x", "text/plain", "y")
            .await
            .is_ok());
        let recorded = counters.0.lock().unwrap();
        assert_eq!(recorded.len(), 1, "落地这一次只写成功那一格");
        assert_eq!(recorded[0].0, TOOL_CALL_ARCHIVED_TOTAL);
        assert_eq!(recorded[0].1, 1.0);
        assert_eq!(
            recorded[0].2.get(crate::LABEL_TOOL).map(String::as_str),
            Some("read_file")
        );
        assert!(
            !recorded[0].2.contains_key(crate::LABEL_CAUSE),
            "成功没有 cause：带着一个空的 cause 标签，读的人会以为那也是失败族里的一格"
        );
    }

    #[tokio::test]
    async fn the_truncation_reading_names_the_tool_and_its_size() {
        let c = client(200, false);
        let counters = Arc::new(Counters::default());
        let archiver = HttpToolOutputArchive::new(
            c,
            Some("http://cogneva:8080".into()),
            Some(counters.clone()),
        );
        archiver.record_truncation("run_command", 4096).await;
        let recorded = counters.0.lock().unwrap();
        assert_eq!(recorded.len(), 2, "一次裁剪既是「一条」，也带着「多少字」");
        assert_eq!(recorded[0].0, TOOL_OUTPUT_TRUNCATED_TOTAL);
        assert_eq!(recorded[0].1, 1.0, "计数那一格每次加一");
        assert_eq!(recorded[1].0, TOOL_OUTPUT_TRUNCATED_CHARS_TOTAL);
        assert_eq!(recorded[1].1, 4096.0, "字符量那一格加的是丢掉的字数");
        for (_, _, labels) in recorded.iter() {
            assert_eq!(
                labels.get(crate::LABEL_TOOL).map(String::as_str),
                Some("run_command"),
                "两格必须按同一个 tool 标签写，否则「几个结果被剪」与「剪掉多少字」对不上同一批调用"
            );
        }
    }
}
