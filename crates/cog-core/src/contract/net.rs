//!HTTP client configuration types and trait.
//!`cog-core` defines the [`HttpClient`] trait and request/response types so
//!that business crates never depend on a specific HTTP implementation
//!(reqwest, hyper, etc.).  The `cogneva` assembly layer injects the concrete
//!backend — usually `cog_net::ReqwestHttpClient`.

use crate::{SFError, SFResult};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

/// The media type of a JSON body, on the wire.
///
/// Written here once because both ends of every JSON request in this workspace
/// have to agree on it: a gateway answering with a different spelling, or a
/// provider sending one, still parses locally — the disagreement only shows up
/// when something in the middle picks a decoder by that word and picks the wrong
/// one. Nothing in this tree compares two spellings of a header value, so the
/// second copy would be free to drift.
pub const CONTENT_TYPE_JSON: &str = "application/json";

/// A stream of HTTP response body chunks.
pub type HttpBodyStream = Pin<Box<dyn Stream<Item = Result<Bytes, SFError>> + Send>>;

/// An HTTP response with a streaming body.
/// Callers should check [`Self::status`] before consuming [`Self::stream`].
pub struct HttpStreamResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub stream: HttpBodyStream,
}

impl std::fmt::Debug for HttpStreamResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpStreamResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("stream", &"<HttpBodyStream>")
            .finish()
    }
}

impl HttpStreamResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Consume the stream and collect the full body as a UTF-8 string.
    pub async fn drain_text(mut self) -> String {
        let mut body = Vec::new();
        while let Some(chunk) = self.stream.next().await {
            match chunk {
                Ok(bytes) => body.extend_from_slice(&bytes),
                Err(_) => break,
            }
        }
        String::from_utf8_lossy(&body).into_owned()
    }
}

// ─── Configuration ──────────────────────────────────────────────────────────

// ─── Request / Response ─────────────────────────────────────────────────────

/// The request deadline the published [`HttpClient`] was built with.
///
/// A consumer that has to bound a wait of its own — a protocol handshake, or a
/// reply that arrives on a different connection than the request went out on —
/// reads this instead of defining a second number. The deadline is one setting
/// with one value; two copies of it that are supposed to agree are two copies
/// that drift, and the drift shows up as one side timing out while the other
/// waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRequestTimeout(pub std::time::Duration);

/// An [`HttpClient`] that carries no overall request deadline of its own, for a
/// connection meant to outlive a short exchange — a server-sent-events stream,
/// a long poll.
///
/// It is a distinct published service rather than a second trait because the
/// deadline lives in the client, not in the request: a per-request timeout can
/// only shorten the client's, so a consumer holding the ordinary client cannot
/// open a connection that outlives it. One client handed out for both jobs is
/// how a stream gets cut at the short-exchange deadline and the server is
/// blamed for it; the choice of which client to hold *is* the choice, so the
/// two are published separately. A bare request that needs a deadline still
/// sets it per request ([`HttpRequest::timeout`]).
#[derive(Debug, Clone)]
pub struct StreamingHttpClient(pub Arc<dyn HttpClient>);

/// An HTTP request description.
/// Callers build requests with the builder-style methods (`get`, `post`,
/// `header`, `json`, `timeout`) and pass the finished value to
/// [`HttpClient::execute`].
#[derive(Debug, Clone, Default)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub body: Option<Vec<u8>>,
    pub timeout_secs: Option<u64>,
}

impl HttpRequest {
    pub fn new(method: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: "GET".into(),
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn post(url: impl Into<String>) -> Self {
        Self {
            method: "POST".into(),
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn put(url: impl Into<String>) -> Self {
        Self {
            method: "PUT".into(),
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn delete(url: impl Into<String>) -> Self {
        Self {
            method: "DELETE".into(),
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn head(url: impl Into<String>) -> Self {
        Self {
            method: "HEAD".into(),
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn patch(url: impl Into<String>) -> Self {
        Self {
            method: "PATCH".into(),
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Serialize `value` to JSON and set the body and `Content-Type` header.
    pub fn json(mut self, value: &impl Serialize) -> SFResult<Self> {
        let bytes = serde_json::to_vec(value).map_err(SFError::Serialization)?;
        self.headers
            .insert("Content-Type".into(), "application/json".into());
        self.body = Some(bytes);
        Ok(self)
    }

    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.body = Some(body);
        self
    }

    pub fn timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }
}

/// An HTTP response.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Deserialize the response body as JSON.
    pub fn json<T: for<'de> Deserialize<'de>>(&self) -> SFResult<T> {
        serde_json::from_slice(&self.body).map_err(SFError::Serialization)
    }

    /// Decode the response body as UTF-8 text.
    pub fn text(&self) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.body.clone())
    }
}

// ─── Trait ──────────────────────────────────────────────────────────────────

/// Abstraction over an HTTP client.
/// Business crates depend on `Arc<dyn HttpClient>` (or `&dyn HttpClient`) so
/// that they are not tied to `reqwest`, `hyper`, or any other implementation.
#[async_trait::async_trait]
pub trait HttpClient: Send + Sync + std::fmt::Debug {
    async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse>;

    /// Execute a request and return a streaming response.
    /// The default implementation falls back to [`Self::execute`] and yields the
    /// full body as a single chunk.  Backends that support true streaming
    /// (e.g. `reqwest`) should override this.
    async fn execute_stream(&self, req: HttpRequest) -> SFResult<HttpStreamResponse> {
        let resp = self.execute(req).await?;
        let chunk = Result::<Bytes, SFError>::Ok(Bytes::from(resp.body));
        let stream: HttpBodyStream = Box::pin(futures::stream::iter(vec![chunk]));
        Ok(HttpStreamResponse {
            status: resp.status,
            headers: resp.headers,
            stream,
        })
    }
}

// ─── WebSocket abstraction ──────────────────────────────────────────────────

/// A WebSocket message.
#[derive(Debug, Clone)]
pub enum WsMessage {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}

/// A single WebSocket connection.
/// Callers receive this from [`WebSocketClient::connect`] and use it to
/// send/receive messages until they call [`Self::close`].
#[async_trait::async_trait]
pub trait WebSocketConnection: Send + Sync {
    async fn send(&mut self, msg: WsMessage) -> SFResult<()>;
    async fn receive(&mut self) -> SFResult<Option<WsMessage>>;
    async fn close(&mut self) -> SFResult<()>;
}

/// Abstraction over a WebSocket client.
/// Business crates depend on `Arc<dyn WebSocketClient>` so that they are not
/// tied to `tokio-tungstenite` or any other implementation.
#[async_trait::async_trait]
pub trait WebSocketClient: Send + Sync + std::fmt::Debug {
    async fn connect(
        &self,
        url: &str,
        headers: HashMap<String, String>,
    ) -> SFResult<Box<dyn WebSocketConnection>>;
}
