//! The MCP HTTP+SSE transport, against a stub server that speaks the wire.
//!
//! What these cases check is the part of the transport that only exists on the
//! wire: the `endpoint` event naming where requests go, the handshake that has to
//! happen before anything else, replies arriving on the **event stream** while the
//! POST that asked for them answers `202` with an empty body, and those replies
//! being matched by their JSON-RPC `id` rather than by the order they arrive.
//!
//! Each of these is a way a plausible implementation reads as working while
//! needing nothing: a transport that takes the POST's body as the answer would
//! read an empty 202 as `null`; one that matched replies by arrival order would
//! hand a call the answer to another call whenever two are in flight; one that
//! waited forever on a stream that had closed would report a hang where the truth
//! is "it is not coming".
//!
//! The stub is a real HTTP server on a loopback port, so the transport's own
//! client, headers and URL handling are under test too.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderValue, Response, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures::stream::Stream;
use tokio::sync::{mpsc, Mutex};

use cog_protocol::{McpTransport, SseTransport, MCP_PROTOCOL_VERSION};

/// One stub server's state: the open stream's sender and every request it has
/// accepted, in the order it accepted them.
#[derive(Clone, Default)]
struct Shared {
    /// `None` until a client opens the event stream.
    stream: Arc<Mutex<Option<mpsc::UnboundedSender<Bytes>>>>,
    /// Every JSON-RPC body the server accepted, and the path it arrived on.
    seen: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    /// What the server does on the stream the client is about to open.
    behaviour: Behaviour,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum Behaviour {
    /// Name the endpoint, then answer requests on the stream.
    #[default]
    Answers,
    /// Name the endpoint and end the stream; nothing that is asked afterwards
    /// can ever be answered.
    EndsAfterEndpoint,
    /// Name a message path, and answer each request only after the one behind it
    /// has been answered.
    AnswersOutOfOrder,
}

async fn sse_handler(State(shared): State<Shared>) -> Response<Body> {
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
    if shared.behaviour == Behaviour::EndsAfterEndpoint {
        // Nothing keeps the channel open, so the body ends as soon as the
        // endpoint event has been read -- and no sender is left for the POST
        // handler to answer on, which is what a stream that ended looks like
        // from the server's side too.
        *shared.stream.lock().await = None;
    } else {
        *shared.stream.lock().await = Some(tx.clone());
    }
    // The endpoint event is relative on purpose: the transport has to resolve it
    // against the stream's own URL, and a server that sends an absolute one would
    // not exercise that.
    let _ = tx.send(sse_frame("endpoint", "/messages"));
    drop(tx);
    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, Infallible>> + Send>> =
        Box::pin(unfold_receiver(rx));
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        )
        .body(Body::from_stream(stream))
        .expect("build the event stream response")
}

fn unfold_receiver(
    rx: mpsc::UnboundedReceiver<Bytes>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    futures::stream::unfold(rx, |mut rx| async move {
        let next = rx.recv().await.map(Ok);
        next.map(|item| (item, rx))
    })
}

async fn not_a_stream_handler() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )
        .body(Body::from("{\"error\":\"this is not an event stream\"}"))
        .expect("build the wrong-content-type response")
}

/// Accept one JSON-RPC message, answer `202`, and put the reply on the stream.
///
/// The `202` with an empty body is the whole point: a client that reads the POST's
/// own body as the answer gets nothing, and the reply it is waiting for is on the
/// event stream.
async fn post_handler(
    State(shared): State<Shared>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> StatusCode {
    shared
        .seen
        .lock()
        .await
        .push(("/messages".to_string(), body.clone()));
    let Some(tx) = shared.stream.lock().await.clone() else {
        return StatusCode::ACCEPTED;
    };
    reply(&shared, &tx, &body).await;
    StatusCode::ACCEPTED
}

async fn reply(shared: &Shared, tx: &mpsc::UnboundedSender<Bytes>, body: &serde_json::Value) {
    let Some(id) = body.get("id").cloned() else {
        // A notification: the protocol gives it no answer, and sending one would
        // be a reply to a message that has no id to match it to.
        return;
    };
    let method = body.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let result = match method {
        "initialize" => serde_json::json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "probe", "version": "0"},
        }),
        "tools/list" => serde_json::json!({
            "tools": [{
                "name": "echo",
                "description": "echoes its argument back",
                "inputSchema": {"type": "object"},
            }],
        }),
        "tools/call" => {
            let q = body["params"]["arguments"]["q"]
                .as_str()
                .unwrap_or("")
                .to_string();
            if shared.behaviour == Behaviour::AnswersOutOfOrder && q == "first" {
                // Held back until the request that came after it has been
                // answered, so a client matching by arrival order hands the
                // second call's answer to the first.
                let tx = tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    let _ = tx.send(sse_frame(
                        "message",
                        &jsonrpc_result(
                            &id,
                            serde_json::json!({
                                "content": [{"type": "text", "text": "echo:first"}],
                                "isError": false,
                            }),
                        )
                        .to_string(),
                    ));
                });
                return;
            }
            serde_json::json!({
                "content": [{"type": "text", "text": format!("echo:{q}")}],
                "isError": false,
            })
        }
        other => serde_json::json!({"probe says": other}),
    };
    let _ = tx.send(sse_frame(
        "message",
        &jsonrpc_result(&id, result).to_string(),
    ));
}

fn jsonrpc_result(id: &serde_json::Value, result: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn sse_frame(event: &str, data: &str) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {data}\n\n"))
}

/// Start the stub and answer its base URL.
async fn stub(behaviour: Behaviour) -> (String, Shared) {
    let shared = Shared {
        behaviour,
        ..Default::default()
    };
    let app = Router::new()
        .route("/sse", get(sse_handler))
        .route("/messages", post(post_handler))
        .route("/not-a-stream", get(not_a_stream_handler))
        .with_state(shared.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let addr = listener.local_addr().expect("read the bound port");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), shared)
}

/// A `reqwest`-backed client for the tests: the transport is handed its client
/// through the same contract the plugin uses, so the stub has to be reachable
/// through that contract and not through a private one.
///
/// No overall deadline of its own (a bare `reqwest::Client`), so a case that
/// asserts a hang can only fail by hanging, not by a timeout this test set.
#[derive(Debug)]
struct TestHttpClient(reqwest::Client);

impl TestHttpClient {
    fn build(&self, req: &cog_core::HttpRequest) -> reqwest::RequestBuilder {
        let method = reqwest::Method::from_bytes(req.method.as_bytes())
            .expect("the transport builds its own methods");
        let mut builder = self.0.request(method, &req.url);
        for (k, v) in &req.headers {
            builder = builder.header(k, v);
        }
        if let Some(body) = &req.body {
            builder = builder.body(body.clone());
        }
        if let Some(secs) = req.timeout_secs {
            builder = builder.timeout(Duration::from_secs(secs));
        }
        builder
    }
}

fn headers_of(resp: &reqwest::Response) -> std::collections::HashMap<String, String> {
    resp.headers()
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
        .collect()
}

#[async_trait::async_trait]
impl cog_core::HttpClient for TestHttpClient {
    async fn execute(
        &self,
        req: cog_core::HttpRequest,
    ) -> cog_core::SFResult<cog_core::HttpResponse> {
        let resp = self
            .build(&req)
            .send()
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("test client: {e}")))?;
        let status = resp.status().as_u16();
        let headers = headers_of(&resp);
        let body = resp
            .bytes()
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("test client: {e}")))?
            .to_vec();
        Ok(cog_core::HttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn execute_stream(
        &self,
        req: cog_core::HttpRequest,
    ) -> cog_core::SFResult<cog_core::HttpStreamResponse> {
        use futures::TryStreamExt;
        let resp = self
            .build(&req)
            .send()
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("test client: {e}")))?;
        let status = resp.status().as_u16();
        let headers = headers_of(&resp);
        let stream = resp
            .bytes_stream()
            .map_err(|e| cog_core::SFError::Agent(format!("test client stream: {e}")));
        Ok(cog_core::HttpStreamResponse {
            status,
            headers,
            stream: Box::pin(stream),
        })
    }
}

/// The transport under test, handed a client the way the plugin hands it one.
fn transport(base: &str) -> SseTransport {
    SseTransport::new(
        format!("{base}/sse"),
        Arc::new(TestHttpClient(reqwest::Client::new())),
        Duration::from_secs(30),
    )
    .expect("the stub URL is a URL")
}

async fn paths(shared: &Shared) -> Vec<String> {
    shared
        .seen
        .lock()
        .await
        .iter()
        .map(|(p, _)| p.clone())
        .collect()
}

async fn methods(shared: &Shared) -> Vec<String> {
    shared
        .seen
        .lock()
        .await
        .iter()
        .map(|(_, body)| body["method"].as_str().unwrap_or("").to_string())
        .collect()
}

/// The handshake runs before any tool work, requests go where the `endpoint`
/// event said, and the call's answer comes back off the stream.
#[tokio::test]
async fn a_call_follows_the_endpoint_event_after_handshaking() {
    let (base, shared) = stub(Behaviour::Answers).await;
    let transport = transport(&base);

    let tools = transport.list_tools().await.expect("list the tools");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "echo");

    let result = transport
        .call_tool(cog_protocol::mcp::McpToolCall {
            name: "echo".into(),
            arguments: [("q".to_string(), serde_json::json!("hi"))]
                .into_iter()
                .collect(),
        })
        .await
        .expect("call the tool");
    assert!(!result.is_error);
    match &result.content[0] {
        cog_protocol::mcp::McpContent::Text { text } => assert_eq!(text, "echo:hi"),
        other => panic!("expected text content, got {other:?}"),
    }

    assert_eq!(
        methods(&shared).await,
        vec![
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/call"
        ],
        "the handshake comes first, and only it is answered"
    );
    let init = shared.seen.lock().await[0].1.clone();
    assert_eq!(
        init["params"]["protocolVersion"],
        serde_json::json!(MCP_PROTOCOL_VERSION)
    );
    assert_eq!(
        paths(&shared).await,
        vec!["/messages"; 4],
        "every request goes to the path the endpoint event named, not to the stream's own"
    );
    let call = shared.seen.lock().await.last().cloned().expect("the call");
    assert_eq!(
        call.1["params"],
        serde_json::json!({"name": "echo", "arguments": {"q": "hi"}}),
        "a tool call carries the name and the arguments as MCP defines them"
    );
}

/// Two calls in flight, answered back to front: each gets its own reply.
#[tokio::test]
async fn replies_are_matched_by_id_and_not_by_arrival_order() {
    let (base, _shared) = stub(Behaviour::AnswersOutOfOrder).await;
    let transport = transport(&base);

    let call = |q: &'static str| {
        let transport = &transport;
        async move {
            transport
                .call_tool(cog_protocol::mcp::McpToolCall {
                    name: "echo".into(),
                    arguments: [("q".to_string(), serde_json::json!(q))]
                        .into_iter()
                        .collect(),
                })
                .await
                .expect("call the tool")
        }
    };
    let (first, second) = tokio::join!(call("first"), call("second"));

    let text = |r: &cog_protocol::mcp::McpToolResult| match &r.content[0] {
        cog_protocol::mcp::McpContent::Text { text } => text.clone(),
        other => panic!("expected text content, got {other:?}"),
    };
    assert_eq!(
        text(&first),
        "echo:first",
        "the reply that arrived last is the one this call asked for"
    );
    assert_eq!(text(&second), "echo:second");
}

/// A stream that ends leaves nothing to answer the call: the call fails rather
/// than waiting for a reply that cannot come.
#[tokio::test]
async fn a_stream_that_ends_fails_the_waiting_call_instead_of_hanging() {
    let (base, _shared) = stub(Behaviour::EndsAfterEndpoint).await;
    let transport = transport(&base);

    let outcome = tokio::time::timeout(Duration::from_secs(5), transport.list_tools())
        .await
        .expect("the call has to come back on its own, not be cut off by this test");
    let error = outcome.expect_err("there is no reply, so this cannot be Ok");
    assert!(
        error.to_string().contains("closed") || error.to_string().contains("channel"),
        "the failure names the stream ending: {error}"
    );
}

/// A GET that answers with something other than an event stream is refused
/// instead of being read as a stream with no events in it.
#[tokio::test]
async fn a_response_that_is_not_an_event_stream_is_refused() {
    let (base, shared) = stub(Behaviour::Answers).await;
    let transport = SseTransport::new(
        format!("{base}/not-a-stream"),
        Arc::new(TestHttpClient(reqwest::Client::new())),
        Duration::from_secs(30),
    )
    .expect("the stub URL is a URL");

    let error = transport
        .list_tools()
        .await
        .expect_err("JSON is not an event stream");
    assert!(
        error.to_string().contains("text/event-stream"),
        "the failure says what the server answered with: {error}"
    );
    assert!(
        shared.seen.lock().await.is_empty(),
        "nothing may be POSTed to a stream that was never opened"
    );
}
