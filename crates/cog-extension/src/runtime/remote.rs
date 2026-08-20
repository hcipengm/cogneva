//! Remote sandbox executor client: routes `Command` payloads to the executor
//! pod over HTTP and streams output back as NDJSON [`CommandEvent`] lines.

use async_trait::async_trait;
use cog_core::{
    CommandEvent, CommandEventStream, SFResult, SandboxBackend, SandboxPayload, SandboxRequest,
    SandboxResult,
};
use futures::StreamExt;

/// HTTP client for the cluster sandbox executor (`cogneva sandbox-executor`).
pub struct RemoteExecutor {
    base_url: String,
    http: reqwest::Client,
}

impl RemoteExecutor {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            // No total timeout: long-running commands are bounded by the
            // server-side timeout, the client must not cut the stream early.
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl SandboxBackend for RemoteExecutor {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        Ok(super::local::collect_stream(self.execute_stream(req).await?).await)
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(cog_core::SFError::Agent(
            "RemoteExecutor does not compile WASM modules".into(),
        ))
    }

    async fn execute_stream(&self, req: &SandboxRequest) -> SFResult<CommandEventStream> {
        if matches!(req.payload, SandboxPayload::Wasm { .. }) {
            return Err(cog_core::SFError::Agent(
                "RemoteExecutor only executes environment payloads".into(),
            ));
        }
        let url = format!("{}/execute", self.base_url.trim_end_matches('/'));
        let response = self
            .http
            .post(&url)
            .json(&serde_json::json!({
                "payload": req.payload,
                "timeout_ms": req.timeout.as_millis(),
                "task_id": req.task_id,
                "agent_id": req.agent_id,
            }))
            .send()
            .await
            .map_err(|e| {
                cog_core::SFError::Agent(format!("sandbox executor unreachable: {}", e))
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(cog_core::SFError::Agent(format!(
                "sandbox executor returned {}: {}",
                status, body
            )));
        }

        // Decode the NDJSON stream: buffer bytes, split on newlines, parse
        // each complete line as a CommandEvent.
        let state = (response.bytes_stream(), String::new());
        let stream = futures::stream::unfold(state, |(mut bytes, mut buf)| async move {
            loop {
                if let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].to_string();
                    buf.drain(..=pos);
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<CommandEvent>(&line) {
                        Ok(event) => return Some((event, (bytes, buf))),
                        Err(e) => {
                            tracing::warn!("sandbox executor sent malformed event: {}", e);
                            continue;
                        }
                    }
                }
                match bytes.next().await {
                    Some(Ok(chunk)) => buf.push_str(&String::from_utf8_lossy(&chunk)),
                    Some(Err(e)) => {
                        tracing::warn!("sandbox executor stream error: {}", e);
                        return None;
                    }
                    None => return None,
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal NDJSON server on an ephemeral port speaking the executor
    /// wire protocol; no HTTP framework needed for a canned response.
    async fn serve_once(lines: Vec<String>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Drain request headers.
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let body = lines.join("\n") + "\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{}", addr)
    }

    fn cmd_req(command: &str) -> SandboxRequest {
        SandboxRequest {
            task_id: "t".into(),
            agent_id: "test".into(),
            payload: SandboxPayload::Command {
                command: command.into(),
            },
            input: serde_json::json!({}),
            timeout: std::time::Duration::from_secs(5),
            limits: Default::default(),
        }
    }

    #[tokio::test]
    async fn remote_executor_decodes_ndjson_stream() {
        let url = serve_once(vec![
            serde_json::to_string(&CommandEvent::Stdout {
                data: "hello".into(),
            })
            .unwrap(),
            serde_json::to_string(&CommandEvent::Exit { code: 0 }).unwrap(),
        ])
        .await;
        let executor = RemoteExecutor::new(url);
        let result = executor.execute(&cmd_req("echo hello")).await.unwrap();
        assert_eq!(result.stdout, "hello");
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn remote_executor_surfaces_http_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            socket
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 3\r\n\r\nbad")
                .await
                .unwrap();
        });
        let executor = RemoteExecutor::new(format!("http://{}", addr));
        let err = executor.execute(&cmd_req("echo hi")).await.unwrap_err();
        assert!(err.to_string().contains("500"));
    }

    #[tokio::test]
    async fn remote_executor_rejects_wasm_payload() {
        let executor = RemoteExecutor::new("http://127.0.0.1:1");
        let mut req = cmd_req("echo hi");
        req.payload = SandboxPayload::Wasm {
            bytes: vec![],
            entry: "main".into(),
        };
        assert!(executor.execute(&req).await.is_err());
    }
}
