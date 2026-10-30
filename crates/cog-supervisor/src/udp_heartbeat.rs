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

/// 心跳包发几份：3。
///
/// UDP 不重传，冗余发送就是这里唯一的重传；三份覆盖「单包丢弃」这个常态，
/// 更多只是把心跳流量乘以份数。
// cap: bound — UDP 不重传，冗余发送就是这里唯一的重传；三份覆盖「单包丢弃」这个常态，更多只是把心跳流量乘以份数
const REDUNDANT_PACKETS: u32 = 3;

/// 冗余包之间的间隔：10ms。
///
/// 太密会被接收端当成重复包一次丢掉，太疏则三份落在同一个拥塞窗口里，
/// 冗余随之失去意义。
// cap: bound — 太密会被接收端当成重复包一次丢掉，太疏则三份落进同一个拥塞窗口，冗余随之失去意义
const REDUNDANT_PACKET_SPACING: std::time::Duration = std::time::Duration::from_millis(10);

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
        // Registered before the socket is set up: a heartbeat loop whose client
        // never came up is not a loop that is fine, so the early return below has
        // to read as the loop having ended.
        let loop_name = format!("supervisor_udp_heartbeat[{}]", self.agent_id);
        let server_addr = self.server_addr;
        let agent_id = self.agent_id;
        cog_core::loop_health::spawn(
            loop_name,
            cog_core::loop_health::Cadence::Periodic(std::time::Duration::from_secs(
                interval_secs.max(1),
            )),
            cancel.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let server_addr = server_addr.clone();
                let agent_id = agent_id.clone();
                let cancel = cancel.clone();
                async move {
                    let socket = match UdpSocket::bind("0.0.0.0:0").await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("udp client bind failed: {e}");
                            return;
                        }
                    };

                    if let Err(e) = socket.connect(&server_addr).await {
                        tracing::error!("udp client connect failed: {e}");
                        return;
                    }

                    let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(
                        interval_secs.max(1),
                    ));
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    ticker.tick().await;

                    loop {
                        beat.beat();
                        tokio::select! {
                        _ = ticker.tick() => {
                            let packet = HeartbeatPacket {
                                agent_id: agent_id.clone(),
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
                            for i in 0..REDUNDANT_PACKETS {
                                if i > 0 {
                                    tokio::time::sleep(REDUNDANT_PACKET_SPACING).await;
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
                }
            },
        )
    }
}
