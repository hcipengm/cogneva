//! Writing a truncated tool output into the platform's memory Raw layer, and
//! the two readings that say what happened to it.
//!
//! The tool result is cut to fit the context window, and the cut bytes are the
//! only copy of what the model did not see. They are written back where the
//! read side can reach them by reference — the truncation marker carries an
//! `artifact://` URI — so the loss is addressable rather than silent.
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
//! Both readings live on this handle rather than beside it because they are the
//! two fates of one event: a tool output that did not fit. `truncated` counts
//! the event; `archive_failure` counts the ones whose tail could not be stored,
//! under the cause. An unconfigured base URL is one of those causes, not an
//! exemption — with nowhere to write, a truncated output is lost, and that is
//! what the reading must say.

use std::collections::HashMap;
use std::sync::Arc;

use cog_core::metric_names::{TOOL_OUTPUT_ARCHIVE_FAILED_TOTAL, TOOL_OUTPUT_TRUNCATED_TOTAL};
use cog_core::{
    artifact_uri, HttpClient, HttpRequest, MetricsBackend, SFError, SFResult,
    DEFAULT_MEMORY_NAMESPACE, MEMORY_API_BASE_ENV, MEMORY_INGEST_PATH,
};

/// Why an archive attempt did not produce a reference. A closed set: the label
/// value names the cause, and a reader choosing between "set the base URL",
/// "fix the route to the memory API", and "read the API's refusal" is choosing
/// between these three.
pub mod cause {
    /// No base URL is configured, so nothing was attempted.
    pub const UNCONFIGURED: &str = "unconfigured";
    /// The call did not complete: the memory API was unreachable.
    pub const UNREACHABLE: &str = "unreachable";
    /// The memory API answered, and refused the write.
    pub const REFUSED: &str = "refused";
}

/// The surface a run uses to archive an oversized tool output and to record the
/// two readings that say what became of it.
#[async_trait::async_trait]
pub trait ToolOutputArchive: Send + Sync + std::fmt::Debug {
    /// Archive `tool`'s `text` as the raw source `id` and return its
    /// `artifact://` reference.
    ///
    /// The attempt publishes its own failure reading — the cause is known only
    /// here, and a caller left to classify the error would either guess or fold
    /// "misconfigured" and "the API refused" into one cell. The `Err` is still
    /// returned so the caller can say in its own log why the marker carries no
    /// reference.
    async fn archive(
        &self,
        tool: &str,
        id: &str,
        content_type: &str,
        text: &str,
    ) -> SFResult<String>;

    /// Record that `tool`'s output was truncated to fit the context window.
    async fn record_truncation(&self, tool: &str);
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
        let Some(base) = self.base.as_deref().filter(|b| !b.trim().is_empty()) else {
            let err = SFError::Config(format!(
                "{MEMORY_API_BASE_ENV} is not set: a truncated tool output has nowhere to be archived"
            ));
            self.record_failure(tool, cause::UNCONFIGURED).await;
            return Err(err);
        };
        let url = format!("{}{MEMORY_INGEST_PATH}", base.trim_end_matches('/'));
        let body = serde_json::json!({
            "id": id,
            "content_type": content_type,
            "text": text,
        });
        // A body that will not serialize is a bug in this function, not a
        // property of the deployment, so it is not one of the archive causes.
        let req = HttpRequest::post(url)
            .json(&body)?
            .timeout(ARCHIVE_TIMEOUT_SECS);

        let resp = match self.client.execute(req).await {
            Ok(resp) => resp,
            Err(e) => {
                self.record_failure(tool, cause::UNREACHABLE).await;
                return Err(e);
            }
        };
        if !resp.is_success() {
            self.record_failure(tool, cause::REFUSED).await;
            return Err(SFError::Config(format!(
                "memory API refused the archive of {id:?}: HTTP {}",
                resp.status
            )));
        }
        // The reference is spelling, not a value the API returns: the pod is
        // anonymous, so the API filed the source under the default namespace,
        // and the reference says where a reader should ask for it.
        Ok(artifact_uri(DEFAULT_MEMORY_NAMESPACE, id))
    }

    async fn record_truncation(&self, tool: &str) {
        let mut labels = HashMap::new();
        labels.insert("tool".to_string(), tool.to_string());
        self.count(TOOL_OUTPUT_TRUNCATED_TOTAL, 1.0, labels).await;
    }
}

impl HttpToolOutputArchive {
    async fn record_failure(&self, tool: &str, cause: &str) {
        let mut labels = HashMap::new();
        labels.insert("tool".to_string(), tool.to_string());
        labels.insert("cause".to_string(), cause.to_string());
        self.count(TOOL_OUTPUT_ARCHIVE_FAILED_TOTAL, 1.0, labels)
            .await;
    }
}

/// How long one archive POST may take before it is abandoned.
///
/// The call sits on the tool-result path, so its budget is a share of a turn
/// rather than a background job's: long enough for a healthy API to store an
/// oversized payload, short enough that an API that stopped answering delays the
/// turn instead of wedging it.
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

    #[derive(Debug, Default)]
    struct Counters(Mutex<Vec<(MetricName, HashMap<String, String>)>>);

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
            _value: f64,
            labels: HashMap<String, String>,
        ) -> SFResult<()> {
            self.0.lock().unwrap().push((name, labels));
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
                "application/json",
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
                recorded[0].1.get("tool").map(String::as_str),
                Some("run_command")
            );
            assert_eq!(
                recorded[0].1.get("cause").map(String::as_str),
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
            recorded[0].1.get("cause").map(String::as_str),
            Some(cause::UNCONFIGURED)
        );
    }

    #[tokio::test]
    async fn a_successful_archive_records_no_failure() {
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
        assert!(counters.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_truncation_reading_names_the_tool() {
        let c = client(200, false);
        let counters = Arc::new(Counters::default());
        let archiver = HttpToolOutputArchive::new(
            c,
            Some("http://cogneva:8080".into()),
            Some(counters.clone()),
        );
        archiver.record_truncation("run_command").await;
        let recorded = counters.0.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, TOOL_OUTPUT_TRUNCATED_TOTAL);
        assert_eq!(
            recorded[0].1.get("tool").map(String::as_str),
            Some("run_command")
        );
    }
}
