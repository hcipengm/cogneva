//! WebSocket client implementation backed by [`tokio-tungstenite`].
//! Use [`TungsteniteWebSocketClient`] when you need to satisfy
//! [`cog_core::WebSocketClient`] in tests or generic code.

use cog_core::{SFError, SFResult, WebSocketClient, WebSocketConnection, WsMessage};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use tokio_tungstenite::tungstenite::protocol::Message as TungsteniteMessage;

/// A [`cog_core::WebSocketClient`] implementation backed by [`tokio-tungstenite`].
#[derive(Debug, Clone, Default)]
pub struct TungsteniteWebSocketClient;

impl TungsteniteWebSocketClient {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl WebSocketClient for TungsteniteWebSocketClient {
    async fn connect(
        &self,
        url: &str,
        headers: HashMap<String, String>,
    ) -> SFResult<Box<dyn WebSocketConnection>> {
        let parsed_url = url
            .parse::<http::Uri>()
            .map_err(|e| SFError::Agent(format!("invalid websocket url '{}': {}", url, e)))?;

        let host = parsed_url.host().unwrap_or("localhost").to_string();
        let mut builder = tokio_tungstenite::tungstenite::handshake::client::Request::builder()
            .uri(url)
            .header("Host", host)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13");

        for (k, v) in headers {
            builder = builder.header(k, v);
        }

        let request = builder
            .body(())
            .map_err(|e| SFError::Agent(format!("websocket request build failed: {}", e)))?;

        let (ws_stream, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| SFError::Agent(format!("websocket connect failed: {}", e)))?;

        Ok(Box::new(TungsteniteConnection { inner: ws_stream }))
    }
}

struct TungsteniteConnection {
    inner: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

#[async_trait::async_trait]
impl WebSocketConnection for TungsteniteConnection {
    async fn send(&mut self, msg: WsMessage) -> SFResult<()> {
        let tmsg = match msg {
            WsMessage::Text(t) => TungsteniteMessage::Text(t.into()),
            WsMessage::Binary(b) => TungsteniteMessage::Binary(b.into()),
            WsMessage::Ping(d) => TungsteniteMessage::Ping(d.into()),
            WsMessage::Pong(d) => TungsteniteMessage::Pong(d.into()),
            WsMessage::Close => TungsteniteMessage::Close(None),
        };
        self.inner
            .send(tmsg)
            .await
            .map_err(|e| SFError::Agent(format!("websocket send failed: {}", e)))
    }

    async fn receive(&mut self) -> SFResult<Option<WsMessage>> {
        let msg = self
            .inner
            .next()
            .await
            .transpose()
            .map_err(|e| SFError::Agent(format!("websocket receive failed: {}", e)))?;
        Ok(msg.map(|m| match m {
            TungsteniteMessage::Text(t) => WsMessage::Text(t.to_string()),
            TungsteniteMessage::Binary(b) => WsMessage::Binary(b.to_vec()),
            TungsteniteMessage::Ping(d) => WsMessage::Ping(d.to_vec()),
            TungsteniteMessage::Pong(d) => WsMessage::Pong(d.to_vec()),
            TungsteniteMessage::Close(_) => WsMessage::Close,
            _ => WsMessage::Close,
        }))
    }

    async fn close(&mut self) -> SFResult<()> {
        self.inner
            .close(None)
            .await
            .map_err(|e| SFError::Agent(format!("websocket close failed: {}", e)))
    }
}
