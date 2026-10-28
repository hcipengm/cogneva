//! 一个已注册的 agent，两个网关报文契约都要它。
//!
//! 两处各写了一遍同一个结构体字面量（连 `probe-host`、`127.0.0.1` 这些占位值都
//! 逐字相同）。这不是「各自造各自的数据」：两份夹具问的是同一个网关对同一个已注册
//! agent 的回答，一边改了角色或工作区而另一边没改，两份判据说的就不是同一件事了。

use chrono::Utc;
use cog_core::{AgentRegistration, ResourceInfo};

/// `agent_id` 已经注册、心跳就在此刻的一条注册记录。
//
// 每个测试二进制各编一次 `common`，只有两份夹具用它。
#[allow(dead_code)]
pub fn registration(agent_id: &str) -> AgentRegistration {
    let now = Utc::now();
    AgentRegistration {
        agent_id: agent_id.into(),
        hostname: "probe-host".into(),
        pod_ip: "127.0.0.1".into(),
        role: "worker".into(),
        workspace_id: "default".into(),
        capabilities: Vec::new(),
        resources: ResourceInfo::default(),
        registered_at: now,
        last_heartbeat: now,
    }
}
