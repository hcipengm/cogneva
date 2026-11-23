//! 监督面两份夹具共用的替身：同一个 `AgentInfo` 写两遍时，改一处另一处不会红，
//! 而读的人分不出那两个「agent」是不是同一件事。

use chrono::Utc;
use cog_supervisor::registry::AgentInfo;

/// 一个刚注册、没有角色、没有队伍的 agent。
pub fn make_agent(id: &str) -> AgentInfo {
    let now = Utc::now();
    AgentInfo {
        agent_id: id.into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: Vec::new(),
        last_heartbeat: now,
        state_since: now,
        registered_at: now,
    }
}
