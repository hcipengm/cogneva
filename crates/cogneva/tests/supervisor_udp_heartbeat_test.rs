use cog_core::{AgentRegistration, AgentRegistry, ResourceInfo, ShutdownSignal};
use cog_storage::MemoryAgentRegistry;
use cog_supervisor::udp_heartbeat::{HeartbeatPacket, UdpHeartbeatClient, UdpHeartbeatServer};
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::test]
async fn udp_heartbeat_end_to_end() {
    let registry: Arc<dyn AgentRegistry> = Arc::new(MemoryAgentRegistry::new());
    let reg = AgentRegistration::new(
        "agent-1",
        "host-1",
        "10.0.0.1",
        "planner",
        "ws-1",
        vec!["code".into()],
        ResourceInfo {
            cpu_cores: 4,
            memory_gb: 8,
        },
    );
    registry.register(&reg).await.unwrap();

    let server = UdpHeartbeatServer::new("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let server_handle = server.spawn(registry.clone());

    let cancel = ShutdownSignal::new();
    let client = UdpHeartbeatClient::new(server_addr.to_string(), reg.agent_id.clone());
    let client_handle = client.spawn(1, cancel.clone());

    // Wait for at least one heartbeat cycle (3 redundant packets).
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let after = registry.get(&reg.agent_id).await.unwrap().unwrap();
    assert!(
        after.last_heartbeat > reg.last_heartbeat,
        "udp heartbeat must update last_heartbeat"
    );

    cancel.trigger();
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), client_handle).await;
    server_handle.abort();
}

#[tokio::test]
async fn udp_heartbeat_packet_serde_roundtrip() {
    let packet = HeartbeatPacket {
        agent_id: "agent-42".into(),
        ts: 1234567890,
    };
    let json = serde_json::to_string(&packet).unwrap();
    assert_eq!(json, r#"{"agent_id":"agent-42","ts":1234567890}"#);

    let decoded: HeartbeatPacket = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, packet);
}

#[tokio::test]
async fn udp_heartbeat_server_ignores_malformed_payload() {
    let registry: Arc<dyn AgentRegistry> = Arc::new(MemoryAgentRegistry::new());
    let server = UdpHeartbeatServer::new("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let _server_handle = server.spawn(registry.clone());

    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.connect(server_addr).await.unwrap();

    // Send garbage.
    socket.send(b"not-json").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Server should still be alive — no panic, no error propagated.
    let all = registry.list().await.unwrap();
    assert!(all.is_empty());
}
