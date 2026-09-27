//! Audited LLM channel: "a body is looked at before it leaves" is placed on the
//! channel itself rather than on its callers.
//!
//! Each of the other two egress faces is missing half. `/proxy` (8080) audits the
//! body per request but injects a forwarding credential rather than an LLM
//! upstream one, so a model call cannot go through it; the LLM passthrough (8081)
//! injects the upstream credential and does not look at a byte of the request
//! body. "Document bodies leave only through an audited channel" therefore needs
//! a third face that has **both**. Adding the LLM routes to 8080 was the
//! alternative, and it was rejected for widening the reachable surface: the
//! sandbox pods' egress allowlist explicitly admits 8080, so routes there amount
//! to handing them the ability to call the upstream directly. A separate port
//! keeps the reachable surface down to the one workload that organizes documents;
//! both sides are stronger than the literal reading of the design.
//!
//! The outcome vocabulary unfolds along the **decision path** (see
//! `AUDITED_OUTCOMES`): one cause per cell, each with its own action. A narrow
//! vocabulary is a trap here: with only "passed / refused / blocked" a reader
//! cannot tell "the body contains something shaped like a credential" (a
//! security event) from "the body is over the auditable bound" (a knob or a
//! caller problem), and the two point in opposite directions.
//!
//! The audit **refuses rather than scrubs**: a body that matches a credential
//! shape fails the whole request. Scrubbing it and sending on would let the
//! caller believe the full text left, which is worse than stopping it -- a
//! "successful" call carrying a body with a hole in it leaves nothing to trace
//! back to.
//!
//! The audited surface is the request body, not the response: what must not leave
//! is the body, and the bytes coming back have already been outside the cluster.

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use cog_core::{MetricName, MetricsBackend};
use futures::StreamExt;

/// The audited channel's closed set of outcomes, **every one published** (zeros
/// included).
///
/// The order is the order of the decision, so reading the vocabulary is reading
/// the decision path: the switch is outermost (a closed switch never reads the
/// body), then the body is read (over bound / read failure), then judged (not
/// text / matched), then released. Each cell is a different cause with a
/// different action:
///
/// - `blocked_by_switch`: the channel is configured off. The action is to flip
///   the switch, not to look at the caller.
/// - `refused_over_bound`: the body is over the auditable bound. The action is to
///   change the bound or to stop the caller sending this much -- this cell once
///   read the same as the previous one (both "the channel is not working"), and
///   they are apart because **either side's configuration can be wrong** and only
///   its own cell can say which.
/// - `refused_unreadable`: the body was not read to the end (client aborted, ...).
///   The action is to look at the link.
/// - `refused_not_text`: read in full but not text, so the judge cannot conclude.
///   The action is to look at the caller's encoding.
/// - `refused_by_audit`: the judge matched a credential shape in the body. **This
///   cell is a security event.**
/// - `released`: let through to the upstream handler. It answers "how many times
///   the gate opened"; upstream success and failure are accounted separately by
///   `llm_calls_total`.
///
/// Why zeros are published too: see `AuditedGate::publish_vocabulary` -- absent
/// and zero are two different things.
pub const AUDITED_OUTCOMES: [&str; 6] = [
    "blocked_by_switch",
    "refused_over_bound",
    "refused_unreadable",
    "refused_not_text",
    "refused_by_audit",
    "released",
];

/// The series name the channel counts under. One series per `outcome`.
pub const AUDITED_REQUESTS: MetricName = cog_core::metric_names::AUDITED_LLM_REQUESTS_TOTAL;

/// The switch's variable name and its reading both come from the shared contract.
///
/// The meaning is **"host document bodies may leave the cluster"**, off by
/// default. The name and its reading are defined once, in
/// `cog_core::host_documents`: both sides read the same switch, the gateway
/// refusing requests by it and the executor deciding by it whether to read a body
/// at all. Two implementations would eventually disagree on some default, and
/// both shapes of that disagreement ("one side open, one side closed") read as
/// "the capability is not on" from either side's own reading.
///
/// The gateway stays the only side that can refuse: a judgement has to be
/// enforced where it is made. The executor reads it merely to avoid sending
/// requests that are certain to be refused; the asymmetry is deliberate.
pub use cog_core::host_documents::switch_enabled;
pub use cog_core::host_documents::AUDIT_CELL_HEADER;
pub use cog_core::host_documents::BODY_EGRESS_ENV;

/// Variable name of the auditable request-body bound
/// (`COGNEVA_SG_AUDITED_MAX_BODY_BYTES`).
pub const MAX_BODY_BYTES_ENV: &str = "COGNEVA_SG_AUDITED_MAX_BODY_BYTES";

/// Default for the auditable request-body bound: 8 MiB.
///
/// Above the bound nothing is audited, and **what cannot be audited is refused**,
/// so this number is not a throughput parameter: it says how much body may be
/// sent out in one call. It deliberately does **not** share a value with the
/// document write bound: that one bounds the bytes a single operation writes down
/// (the storage face), this one bounds the text entering one model call (the
/// context face). They being of a similar magnitude is a coincidence, not the
/// same quantity, and tying them together would quietly drag one side along
/// whenever the other is adjusted. The smaller side is observable: requests it
/// stops are counted under `refused_over_bound`, not dropped in silence.
pub const DEFAULT_MAX_AUDITED_BODY_BYTES: usize = 8 * 1024 * 1024;

/// When the audit cannot pass a body, which kind of "cannot" it was.
///
/// A type rather than a string: the caller picks a reading cell by it, and string
/// comparison would let a new shape fall silently into some `_ =>` branch.
#[derive(Debug, PartialEq, Eq)]
pub enum AuditRefusal {
    /// The body is not UTF-8 text, so the text judge cannot conclude on it.
    NotText,
    /// The judge matched; the name is the matched shape's (same table as the
    /// egress proxy's).
    Credential(&'static str),
}

/// Audit one request body: refuse on a credential shape and name the shape.
///
/// The judge reuses the egress proxy's (`security_gateway::contains_secret`)
/// rather than keeping a second table -- two judges drift eventually, and the
/// direction they drift in is "this channel is looser than that one".
///
/// A non-UTF-8 body is always refused: the text judge cannot conclude on binary
/// bytes, and the side that cannot conclude has to refuse. On this LLM path a
/// request body is JSON by definition, so a legitimate request never lands here.
pub fn audit_body(bytes: &[u8]) -> Result<(), AuditRefusal> {
    let text = std::str::from_utf8(bytes).map_err(|_| AuditRefusal::NotText)?;
    match crate::security_gateway::contains_secret(text) {
        Some(kind) => Err(AuditRefusal::Credential(kind)),
        None => Ok(()),
    }
}

/// One verdict from reading the body. Three states rather than a `Result`: hitting
/// the bound and failing to read are different things (a knob versus a link), and
/// folding them into one error makes them indistinguishable forever after.
enum BodyRead {
    Full(Bytes),
    OverBound,
    Failed(axum::Error),
}

/// Read the whole body under a bound, with a distinguishable verdict for over
/// bound and for read failure.
///
/// Not `axum::body::to_bytes`: it folds both failures into one `Error`, and
/// separating them again would mean unpacking the error's source (and pulling in
/// a crate to do it). Accumulating chunk by chunk keeps the boundary here.
async fn read_within_limit(body: Body, limit: usize) -> BodyRead {
    let mut stream = body.into_data_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => {
                if buf.len().saturating_add(chunk.len()) > limit {
                    return BodyRead::OverBound;
                }
                buf.extend_from_slice(&chunk);
            }
            Err(e) => return BodyRead::Failed(e),
        }
    }
    BodyRead::Full(Bytes::from(buf))
}

/// The audited channel's gate: switch state plus the reading outlet.
///
/// Decoupled from `AppState` so the decision can be tested on its own -- all six
/// outcomes have to be walkable in a test with no upstream pool and no database,
/// otherwise "a match is refused" can only be believed by reading the code.
#[derive(Clone)]
pub struct AuditedGate {
    enabled: bool,
    max_body_bytes: usize,
    metrics: Arc<dyn MetricsBackend>,
}

impl AuditedGate {
    pub fn new(enabled: bool, max_body_bytes: usize, metrics: Arc<dyn MetricsBackend>) -> Self {
        Self {
            enabled,
            max_body_bytes,
            metrics,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }

    /// Record one cell. A failed write degrades to debug: the reading channel
    /// breaking must not take the request path down with it.
    async fn record(&self, outcome: &str, value: f64) {
        let labels =
            std::collections::HashMap::from([("outcome".to_string(), outcome.to_string())]);
        if let Err(e) = self
            .metrics
            .record_counter(AUDITED_REQUESTS, value, labels)
            .await
        {
            tracing::debug!(error = %e, outcome, "audited channel count did not reach the metrics face");
        }
    }

    /// Publish the whole vocabulary at zero.
    ///
    /// The channel exists from deployment on (its listener is always bound), so
    /// "every cell present, all of them zero" is a true reading: the channel is
    /// there and nobody has called it yet. Building the channel only when the
    /// switch is on would make these cells either absent or appear only after the
    /// switch was flipped once, and "the channel was never built" and "it was
    /// built and nobody called" are exactly the two things to keep apart.
    pub async fn publish_vocabulary(&self) {
        for outcome in AUDITED_OUTCOMES {
            self.record(outcome, 0.0).await;
        }
    }
}

/// A refusal, carrying the cell it was recorded under.
///
/// The cell goes in a header as well as into the counter and the log, because a
/// refusal is a 403 either way: without it the caller has one word for all of
/// them and reads "the channel refused this body" as "the call failed", which
/// points at the network instead of at the document.
fn refusal(cell: &'static str, message: String) -> Response {
    let mut response = (StatusCode::FORBIDDEN, message).into_response();
    match HeaderValue::from_str(cell) {
        Ok(value) => {
            response.headers_mut().insert(AUDIT_CELL_HEADER, value);
        }
        // Unreachable for a cell name, and staying silent about it would make a
        // typo'd name look like a caller that was told nothing.
        Err(e) => {
            tracing::warn!(cell, error = %e, "audited channel: cell name would not go in a header")
        }
    }
    response
}

/// The audited channel's middleware: switch -> read body -> audit -> release.
///
/// The order is deliberate: the switch is outermost. With it closed the body need
/// not be read at all -- that request did not fail the audit, it asked a channel
/// that is not serving today, and the two must be separate cells.
pub async fn enforce(State(gate): State<AuditedGate>, req: Request<Body>, next: Next) -> Response {
    if !gate.enabled {
        gate.record("blocked_by_switch", 1.0).await;
        tracing::warn!(
            switch = BODY_EGRESS_ENV,
            path = %req.uri().path(),
            "audited channel closed: a request carrying a document body stopped at the switch"
        );
        return refusal(
            "blocked_by_switch",
            format!(
                "audited channel not enabled: {BODY_EGRESS_ENV} is off, so document bodies do not leave the cluster"
            ),
        );
    }

    let (parts, body) = req.into_parts();
    let limit = gate.max_body_bytes();
    let bytes = match read_within_limit(body, limit).await {
        BodyRead::Full(bytes) => bytes,
        BodyRead::OverBound => {
            gate.record("refused_over_bound", 1.0).await;
            tracing::warn!(
                limit,
                path = %parts.uri.path(),
                "audited channel: request body over the auditable bound, refused"
            );
            return refusal(
                "refused_over_bound",
                format!("request body over the auditable bound ({limit} bytes), refused"),
            );
        }
        BodyRead::Failed(e) => {
            gate.record("refused_unreadable", 1.0).await;
            tracing::warn!(
                error = %e,
                path = %parts.uri.path(),
                "audited channel: request body not read to the end, refused (an unread body cannot be audited)"
            );
            return refusal(
                "refused_unreadable",
                "request body not read to the end, refused (what cannot be audited is not released)"
                    .to_string(),
            );
        }
    };

    if let Err(audit_refusal) = audit_body(&bytes) {
        // A match names the shape and a non-text body says which kind it is, but
        // **neither echoes the body**: the matched run of bytes is the suspected
        // credential, and copying it into a log or a response stores the very
        // thing this is meant to keep out.
        let (cell, reason) = match audit_refusal {
            AuditRefusal::NotText => ("refused_not_text", "is not UTF-8 text".to_string()),
            AuditRefusal::Credential(kind) => (
                "refused_by_audit",
                format!("contains a credential-shaped value ({kind})"),
            ),
        };
        gate.record(cell, 1.0).await;
        tracing::warn!(
            cell,
            reason = %reason,
            path = %parts.uri.path(),
            "audited channel: request body refused by the audit (not scrubbed)"
        );
        return refusal(cell, format!("request body {reason}, refused"));
    }

    // The release cell is recorded on **the gate's own decision**, not on the
    // upstream's answer: this reading answers "how many times did the audited
    // channel let something through", while upstream success and failure are
    // accounted by llm_calls_total. Counting by the upstream's outcome would make
    // an upstream 5xx look like the audit stopped something.
    gate.record("released", 1.0).await;
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn audited_gate(enabled: bool) -> (AuditedGate, Arc<cog_storage::mem::MemoryMetricsBackend>) {
        let metrics = Arc::new(cog_storage::mem::MemoryMetricsBackend::new());
        (
            AuditedGate::new(enabled, DEFAULT_MAX_AUDITED_BODY_BYTES, metrics.clone()),
            metrics,
        )
    }

    #[test]
    fn the_switch_reading_comes_from_the_shared_definition() {
        // The reading lives in `cog_core::host_documents`, with its own value table
        // and its own test. What this pins is only that the gateway uses that one
        // copy: there is no second implementation here, so renaming it or swapping
        // it has to touch this test first.
        assert!(switch_enabled(Some("true")));
        assert!(!switch_enabled(Some("enabled")));
        assert!(!switch_enabled(None));
    }

    #[test]
    fn a_body_carrying_a_credential_shape_is_refused_and_named() {
        // One case per shape: the judge is a table, and testing a single shape lets
        // the remaining rows fail silently.
        let bodies = [
            (
                r#"{"content":"sk-ant-aaaaaaaaaaaaaaaaaaaaaaaa"}"#,
                "anthropic_api_key",
            ),
            (
                r#"{"content":"sk-abcdefghijklmnopqrstuvwx"}"#,
                "openai_api_key",
            ),
            (
                r#"{"content":"ghp_abcdefghijklmnopqrstuvwx"}"#,
                "github_token",
            ),
            (r#"{"content":"AKIAIOSFODNN7EXAMPLE"}"#, "aws_access_key"),
            (r#"{"content":"xoxb-1234567890-abcdefghij"}"#, "slack_token"),
            (
                r#"{"note":"api_key: abcdefghijklmnopqrst"}"#,
                "generic_credential",
            ),
        ];
        for (body, expected) in bodies {
            assert_eq!(
                audit_body(body.as_bytes()),
                Err(AuditRefusal::Credential(expected)),
                "this body must be refused naming {expected}"
            );
        }
    }

    #[test]
    fn a_document_without_credential_shapes_is_released() {
        assert_eq!(audit_body("An ordinary document body.".as_bytes()), Ok(()));
        assert_eq!(audit_body(b"{\"messages\":[{\"content\":\"hi\"}]}"), Ok(()));
        // Non-UTF-8 yields no text verdict, so the direction has to be refuse, and
        // the refusal has to say this is the reason.
        assert_eq!(
            audit_body(&[0xff, 0xfe, 0x00]),
            Err(AuditRefusal::NotText),
            "not being readable as text and matching the judge are two different \
             things and must not land in the same cell"
        );
    }

    /// The whole vocabulary has to sit on the metric surface, at zero.
    ///
    /// This does not test "what one request recorded". It tests that **the channel
    /// can be read even when nobody has called it**, because "the channel is wired
    /// up and idle" and "the channel was never wired up" have to be distinguishable
    /// in the reading -- and the latter is exactly what a closed channel looks like
    /// most of the time.
    #[tokio::test]
    async fn the_gate_publishes_its_whole_vocabulary_at_zero() {
        let (gate, metrics) = audited_gate(false);
        gate.publish_vocabulary().await;

        let totals = metrics
            .query_counter_totals(AUDITED_REQUESTS.as_str())
            .await
            .expect("published counter reading");
        let published: BTreeSet<&str> = totals
            .iter()
            .map(|s| s.labels.get("outcome").map(String::as_str).unwrap_or(""))
            .collect();
        let expected: BTreeSet<&str> = AUDITED_OUTCOMES.iter().copied().collect();
        assert_eq!(published, expected, "every outcome is a series of its own");
        assert!(
            totals.iter().all(|s| s.value == 0.0),
            "the vocabulary lands at zero; requests are what add to it"
        );
    }

    /// Drive the middleware through a real axum router: one case per way out of the
    /// judgement, each checking which cell the reading lands in.
    ///
    /// Calling `enforce` directly is not possible (`Next` can only be built inside a
    /// router), and the whole value of this channel is whether the request really
    /// stopped before the inner handler -- testing only the switch reading and the
    /// judge function would leave a broken wiring entirely outside the test.
    mod wiring {
        use super::*;
        use axum::routing::post;
        use axum::Router;
        use tower::ServiceExt;

        /// The inner handler. Its response text is the evidence that a request made
        /// it upstream.
        async fn upstream() -> &'static str {
            "reached the upstream handler"
        }

        fn app(gate: AuditedGate) -> Router {
            Router::new()
                .route("/v1/chat", post(upstream))
                .layer(axum::middleware::from_fn_with_state(gate, enforce))
        }

        async fn call_with(app: Router, body: Body) -> (StatusCode, String) {
            let (status, _named, body) = call_with_cell(app, body).await;
            (status, body)
        }

        /// One call, keeping the name the channel put on the response: the only thing a
        /// caller has to tell a judgement apart from a transport failure, since a refusal
        /// is a 403 either way.
        async fn call_with_cell(app: Router, body: Body) -> (StatusCode, Option<String>, String) {
            let resp = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/chat")
                        .header("content-type", "application/json")
                        .body(body)
                        .expect("request is constructible"),
                )
                .await
                .expect("router is callable");
            let status = resp.status();
            let named = resp
                .headers()
                .get(AUDIT_CELL_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .expect("response body is readable");
            (status, named, String::from_utf8_lossy(&bytes).to_string())
        }

        async fn call(app: Router, body: &str) -> (StatusCode, String) {
            call_with(app, Body::from(body.to_string())).await
        }

        /// Current value of one `outcome` cell; None when absent.
        async fn cell(
            metrics: &Arc<cog_storage::mem::MemoryMetricsBackend>,
            outcome: &str,
        ) -> Option<f64> {
            metrics
                .query_counter_totals(AUDITED_REQUESTS.as_str())
                .await
                .expect("published counter reading")
                .iter()
                .find(|s| s.labels.get("outcome").map(String::as_str) == Some(outcome))
                .map(|s| s.value)
        }

        #[tokio::test]
        async fn a_closed_switch_never_reaches_the_handler() {
            let (gate, metrics) = audited_gate(false);
            gate.publish_vocabulary().await;

            let (status, body) = call(
                app(gate),
                r#"{"messages":[{"content":"a perfectly ordinary document"}]}"#,
            )
            .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
            assert!(
                !body.contains("reached the upstream handler"),
                "with the switch closed the body must not reach the inner handler: {body}"
            );
            assert!(
                body.contains("not enabled"),
                "the refusal has to point at the switch: {body}"
            );
            assert_eq!(cell(&metrics, "blocked_by_switch").await, Some(1.0));
            assert_eq!(cell(&metrics, "released").await, Some(0.0));
        }

        #[tokio::test]
        async fn a_body_with_a_credential_shape_is_refused_not_washed() {
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;

            let (status, body) = call(
                app(gate),
                r#"{"messages":[{"content":"token ghp_abcdefghijklmnopqrstuvwx"}]}"#,
            )
            .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
            assert!(
                !body.contains("reached the upstream handler"),
                "a request that hit the audit must not be forwarded: {body}"
            );
            assert!(
                body.contains("github_token"),
                "the refusal has to name the shape it matched (without echoing the body): {body}"
            );
            assert!(
                !body.contains("ghp_abcdefghijklmnopqrstuvwx"),
                "the refusal must not echo the matched span"
            );
            assert_eq!(cell(&metrics, "refused_by_audit").await, Some(1.0));
            assert_eq!(cell(&metrics, "released").await, Some(0.0));
        }

        #[tokio::test]
        async fn a_clean_body_is_released_to_the_handler() {
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;

            let (status, body) =
                call(app(gate), r#"{"messages":[{"content":"ordinary body"}]}"#).await;

            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, "reached the upstream handler");
            assert_eq!(cell(&metrics, "released").await, Some(1.0));
            assert_eq!(cell(&metrics, "refused_by_audit").await, Some(0.0));
        }

        #[tokio::test]
        async fn a_body_too_large_to_audit_is_refused() {
            let metrics = Arc::new(cog_storage::mem::MemoryMetricsBackend::new());
            // The bound is turned down to something one call can exceed: this branch
            // judges "what cannot be audited is not released", which is independent of
            // the bound's value, so the test gives it a small one.
            let gate = AuditedGate::new(true, 16, metrics.clone());
            gate.publish_vocabulary().await;

            let (status, body) = call(
                app(gate),
                r#"{"messages":["this body is longer than sixteen bytes"]}"#,
            )
            .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
            assert!(!body.contains("reached the upstream handler"));
            assert!(
                body.contains("bound"),
                "the refusal has to point at the bound: {body}"
            );
            assert_eq!(cell(&metrics, "refused_over_bound").await, Some(1.0));
            // A bound problem must not be recorded as a judge hit: that would read as
            // "a credential was found in the body".
            assert_eq!(cell(&metrics, "refused_by_audit").await, Some(0.0));
        }

        #[tokio::test]
        async fn a_body_that_is_not_text_is_refused_as_such() {
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;

            let (status, body) = call_with(app(gate), Body::from(vec![0xffu8, 0xfe, 0x00])).await;

            assert_eq!(status, StatusCode::FORBIDDEN);
            assert!(!body.contains("reached the upstream handler"));
            assert!(
                body.contains("UTF-8"),
                "the refusal has to say it could not be read as text: {body}"
            );
            assert_eq!(cell(&metrics, "refused_not_text").await, Some(1.0));
            assert_eq!(cell(&metrics, "refused_by_audit").await, Some(0.0));
        }

        #[tokio::test]
        async fn a_body_that_cannot_be_read_is_refused_as_unreadable() {
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;

            let (status, body) = call_with(app(gate), broken_body()).await;

            assert_eq!(status, StatusCode::FORBIDDEN);
            assert!(!body.contains("reached the upstream handler"));
            assert_eq!(cell(&metrics, "refused_unreadable").await, Some(1.0));
            // A read failure is neither a judge hit nor an oversized body: the three
            // cells have to stay distinguishable.
            assert_eq!(cell(&metrics, "refused_by_audit").await, Some(0.0));
            assert_eq!(cell(&metrics, "refused_over_bound").await, Some(0.0));
        }

        /// After one scenario, **exactly one** cell on the reading surface is
        /// non-zero; returns that cell's name.
        ///
        /// More than one means "a single request recorded two cells"; fewer means
        /// "this request recorded nothing". Both are defects of the judgement
        /// surface, so this asserts "exactly one" instead of just looking at the
        /// cell it wants.
        async fn sole_nonzero_cell(
            metrics: &Arc<cog_storage::mem::MemoryMetricsBackend>,
        ) -> String {
            let nonzero: Vec<String> = metrics
                .query_counter_totals(AUDITED_REQUESTS.as_str())
                .await
                .expect("published counter reading")
                .into_iter()
                .filter(|s| s.value > 0.0)
                .filter_map(|s| s.labels.get("outcome").cloned())
                .collect();
            assert_eq!(
                nonzero.len(),
                1,
                "one request lands in exactly one cell: {nonzero:?}"
            );
            nonzero.into_iter().next().expect("checked non-empty above")
        }

        fn broken_body() -> Body {
            // The body stream itself fails (a client disconnecting looks like this):
            // there are no bytes to judge.
            let broken = futures::stream::iter(vec![Err::<Bytes, std::io::Error>(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed"),
            )]);
            Body::from_stream(broken)
        }

        /// One scenario end to end: the cell the request was counted under, and the name
        /// the same request put on its response.
        async fn scenario(
            gate: AuditedGate,
            metrics: &Arc<cog_storage::mem::MemoryMetricsBackend>,
            body: Body,
        ) -> (String, Option<String>) {
            let (_status, named, _body) = call_with_cell(app(gate), body).await;
            (sole_nonzero_cell(metrics).await, named)
        }

        /// The counted name and the travelled name are the same word.
        ///
        /// The caller logs one and the operator reads the other. If they can differ, the
        /// log line that a person acts on cannot be checked against any reading on the
        /// channel -- and the two would be read as one refusal until somebody compares
        /// them by hand.
        fn assert_named(counted: &str, named: Option<String>) {
            assert_eq!(
                named.as_deref(),
                Some(counted),
                "a refusal has to travel named: counted under {counted}, the response said {named:?}"
            );
        }

        /// Every cell of the vocabulary has to be actually reachable.
        ///
        /// A judgement surface narrower than the set of causes reads two causes the
        /// same way; **wider than that set, with one cell never reachable**, is the
        /// other kind of lie: the vocabulary promises an outcome that no path
        /// produces. This walks all six cells and collects each one's **measured**
        /// landing cell (not a hard-coded expectation), so a missing one is reported
        /// as a missing one.
        ///
        /// The walk also pins the other half of every refusal: the name that travels
        /// back on the response is the name the request was counted under, and the one
        /// outcome that is not a refusal carries no name at all. A caller cannot
        /// separate "a judgement was made" from "the transport failed" without it.
        #[tokio::test]
        async fn every_published_outcome_is_reachable() {
            let mut seen: BTreeSet<String> = BTreeSet::new();

            // Closed: the switch stops it
            let (gate, metrics) = audited_gate(false);
            gate.publish_vocabulary().await;
            let (counted, named) = scenario(gate, &metrics, Body::from("{}")).await;
            assert_named(&counted, named);
            seen.insert(counted);

            // Open + an ordinary body: released -- and named by nobody, because nothing
            // refused it and a name here would make the caller report a judgement that
            // never happened.
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;
            let (counted, named) = scenario(
                gate,
                &metrics,
                Body::from(r#"{"messages":[{"content":"ordinary body"}]}"#),
            )
            .await;
            assert_eq!(counted, "released");
            assert!(
                named.is_none(),
                "a released request must not be labelled as a refusal: {named:?}"
            );
            seen.insert(counted);

            // Open + a credential shape: the judge hit
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;
            let (counted, named) = scenario(
                gate,
                &metrics,
                Body::from(r#"{"content":"ghp_abcdefghijklmnopqrstuvwx"}"#),
            )
            .await;
            assert_named(&counted, named);
            seen.insert(counted);

            // Open + not text
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;
            let (counted, named) = scenario(gate, &metrics, Body::from(vec![0xffu8, 0xfe])).await;
            assert_named(&counted, named);
            seen.insert(counted);

            // Open + over the bound (turned down to 4 bytes)
            let metrics = Arc::new(cog_storage::mem::MemoryMetricsBackend::new());
            let gate = AuditedGate::new(true, 4, metrics.clone());
            gate.publish_vocabulary().await;
            let (counted, named) = scenario(
                gate,
                &metrics,
                Body::from(r#"{"a":"this string is longer than four bytes"}"#),
            )
            .await;
            assert_named(&counted, named);
            seen.insert(counted);

            // Open + a failed read
            let (gate, metrics) = audited_gate(true);
            gate.publish_vocabulary().await;
            let (counted, named) = scenario(gate, &metrics, broken_body()).await;
            assert_named(&counted, named);
            seen.insert(counted);

            let expected: BTreeSet<String> =
                AUDITED_OUTCOMES.iter().map(|c| c.to_string()).collect();
            assert_eq!(
                seen, expected,
                "the vocabulary and the reachable outcomes must line up cell by cell"
            );
        }
    }

    #[tokio::test]
    async fn each_outcome_lands_in_its_own_cell() {
        let (gate, metrics) = audited_gate(true);
        gate.publish_vocabulary().await;
        gate.record("released", 1.0).await;
        gate.record("released", 1.0).await;
        gate.record("refused_by_audit", 1.0).await;

        let totals = metrics
            .query_counter_totals(AUDITED_REQUESTS.as_str())
            .await
            .expect("published counter reading");
        let value = |outcome: &str| {
            totals
                .iter()
                .find(|s| s.labels.get("outcome").map(String::as_str) == Some(outcome))
                .map(|s| s.value)
        };
        assert_eq!(value("released"), Some(2.0));
        assert_eq!(value("refused_by_audit"), Some(1.0));
        assert_eq!(
            value("blocked_by_switch"),
            Some(0.0),
            "a cell nothing was blocked in reads zero, not absent -- absent means \
             nobody published this cell, which is a different thing from this cell \
             being zero"
        );
    }
}
