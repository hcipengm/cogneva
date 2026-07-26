//! UDP heartbeat channel for cross-machine lightweight health checks.
//! Provides a [`UdpHeartbeatServer`] (Supervisor-side) that listens on a UDP
//! socket and updates an [`AgentRegistry`] on each received heartbeat, and a
//! [`UdpHeartbeatClient`] (Agent-side) that periodically sends redundant
//! heartbeat packets to the server.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

use cog_core::{AgentRegistry, SFResult, ShutdownSignal};

/// Heartbeat packet payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HeartbeatPacket {
    pub agent_id: String,
    pub ts: i64,
}

/// Supervisor-side UDP heartbeat listener.
pub struct UdpHeartbeatServer {
    socket: UdpSocket,
}

impl UdpHeartbeatServer {
    /// Bind to the given address (e.g. `"0.0.0.0:8765"`).
    pub async fn new(bind_addr: &str) -> SFResult<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self { socket })
    }

    /// Returns the local address the socket is bound to.
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.socket.local_addr()
    }

    /// Spawn an async task that receives heartbeat packets and calls
    /// [`AgentRegistry::heartbeat`] for the enclosed `agent_id`.
    pub fn spawn(self, registry: Arc<dyn AgentRegistry>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                match self.socket.recv_from(&mut buf).await {
                    Ok((len, _src)) => {
                        let payload = &buf[..len];
                        match serde_json::from_slice::<HeartbeatPacket>(payload) {
                            Ok(packet) => {
                                if let Err(e) = registry.heartbeat(&packet.agent_id).await {
                                    tracing::warn!(
                                        agent_id = %packet.agent_id,
                                        "udp heartbeat registry update failed: {e}"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!("udp heartbeat parse error: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("udp recv error: {e}");
                        break;
                    }
                }
            }
        })
    }
}

/// Agent-side UDP heartbeat sender.
pub struct UdpHeartbeatClient {
    server_addr: String,
    agent_id: String,
}

impl UdpHeartbeatClient {
    pub fn new(server_addr: String, agent_id: String) -> Self {
        Self {
            server_addr,
            agent_id,
        }
    }

    /// Spawn an async task that sends heartbeat packets every `interval_secs`,
    /// with 3 redundant packets per interval (spaced 10 ms apart).
    pub fn spawn(self, interval_secs: u64, cancel: ShutdownSignal) -> JoinHandle<()> {
        tokio::spawn(async move {
            let socket = match UdpSocket::bind("0.0.0.0:0").await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("udp client bind failed: {e}");
                    return;
                }
            };

            if let Err(e) = socket.connect(&self.server_addr).await {
                tracing::error!("udp client connect failed: {e}");
                return;
            }

            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_secs.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let packet = HeartbeatPacket {
                            agent_id: self.agent_id.clone(),
                            ts: chrono::Utc::now().timestamp(),
                        };
                        let payload = match serde_json::to_vec(&packet) {
                            Ok(p) => p,
                            Err(e) => {
                                tracing::warn!("heartbeat serialization failed: {e}");
                                continue;
                            }
                        };
                        // Send 3 redundant packets with 10 ms spacing.
                        for i in 0..3 {
                            if i > 0 {
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            }
                            if let Err(e) = socket.send(&payload).await {
                                tracing::warn!("udp heartbeat send failed: {e}");
                            }
                        }
                    }
                    _ = cancel.wait() => {
                        break;
                    }
                }
            }
        })
    }
}
