//! Unix Domain Socket heartbeat channel.
//! Used for same-machine deployments (K8s same Node) where latency < 1ms
//! is required and Redis failure must not affect health detection.

use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use cog_core::{AgentEvent, AgentRegistry, SFResult};

/// Supervisor-side Unix Socket heartbeat listener.
pub struct UnixHeartbeatServer;

impl UnixHeartbeatServer {
    /// Bind to  and spawn an accept loop.
    /// Each accepted connection reads newline-delimited JSON heartbeat
    /// packets and calls .
    pub async fn spawn<P: AsRef<Path>>(
        path: P,
        registry: Arc<dyn AgentRegistry>,
        event_tx: Option<tokio::sync::broadcast::Sender<AgentEvent>>,
    ) -> SFResult<tokio::task::JoinHandle<()>> {
        let path = path.as_ref().to_owned();
        // Remove stale socket file
        let _ = tokio::fs::remove_file(&path).await;
        let listener = UnixListener::bind(&path)
            .map_err(|e| cog_core::SFError::IO(format!("Unix bind failed: {}", e)))?;

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut stream, _addr)) => {
                        let reg = registry.clone();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            let mut buf = vec![0u8; 1024];
                            match stream.read(&mut buf).await {
                                Ok(n) if n > 0 => {
                                    if let Ok(packet) =
                                        serde_json::from_slice::<serde_json::Value>(&buf[..n])
                                    {
                                        if let Some(agent_id) =
                                            packet.get("agent_id").and_then(|v| v.as_str())
                                        {
                                            let _ = reg.heartbeat(agent_id).await;
                                            if let Some(ref tx) = tx {
                                                let _ = tx.send(AgentEvent::Heartbeat {
                                                    agent_id: agent_id.into(),
                                                    timestamp: chrono::Utc::now(),
                                                });
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("Unix accept error: {}", e);
                    }
                }
            }
        });
        Ok(handle)
    }
}

/// Agent-side Unix Socket heartbeat client.
pub struct UnixHeartbeatClient {
    socket_path: std::path::PathBuf,
    agent_id: String,
}

impl UnixHeartbeatClient {
    pub fn new(socket_path: impl Into<std::path::PathBuf>, agent_id: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            agent_id: agent_id.into(),
        }
    }

    /// Spawn a loop that connects and sends a heartbeat every .
    pub fn spawn(
        &self,
        interval_secs: u64,
        cancel: cog_core::ShutdownSignal,
    ) -> tokio::task::JoinHandle<()> {
        let path = self.socket_path.clone();
        let agent_id = self.agent_id.clone();
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_secs.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let packet = serde_json::json!({
                            "agent_id": &agent_id,
                            "ts": chrono::Utc::now().timestamp(),
                        });
                        match UnixStream::connect(&path).await {
                            Ok(mut stream) => {
                                let _ = stream.write_all(packet.to_string().as_bytes()).await;
                            }
                            Err(e) => {
                                tracing::warn!(agent_id=%agent_id, "Unix heartbeat connect failed: {}", e);
                            }
                        }
                    }
                    _ = cancel.wait() => break,
                }
            }
        })
    }
}
