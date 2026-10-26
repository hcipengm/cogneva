use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::{AgentState, StateBackend};

use crate::error::SupervisorResult;
use crate::registry::{AgentInfo, AgentRegistry};
use cog_core::HealthIssue;

/// Aggregate result of a single health-check pass.
#[derive(Debug, Clone, Default)]
pub struct HealthReport {
    pub healthy: Vec<String>,
    pub suspect: Vec<(String, HealthIssue)>,
    pub dead: Vec<(String, HealthIssue)>,
    pub stuck: Vec<(String, HealthIssue)>,
    /// 这一拍判过、而舰队名单里已经没有的 id。
    ///
    /// 判决本身留在上面三栏：要动手的一方（回收、重试、续跑）仍然需要知道「这个
    /// agent 不出声了」，走了不等于不用管。但「离开舰队」与「掉线」不是同一件事，
    /// 而名单是这里唯一能区分它们的东西——一次性角色干完活会自己注销，注册记录
    /// 到期也会从名单里消失，两者对读者都是「名单里没有它」。
    ///
    /// 实测 2026-10-26：`agent_unhealthy` 当下 firing 的 233 行里 222 行的 id 已经
    /// 不在注册表的索引里（正常注销过），只有 11 行是到期消失、3 行还有活记录。
    /// 也就是说这条规则报的几乎全是「已经走了」，而不是「还挂着但不说话」——真正
    /// 需要人看的那条常驻 `worker` 行被埋在中间 40 分钟。
    pub departed: Vec<String>,
}

impl HealthReport {
    pub fn is_clean(&self) -> bool {
        self.suspect.is_empty() && self.dead.is_empty() && self.stuck.is_empty()
    }

    pub fn unhealthy_total(&self) -> usize {
        self.suspect.len() + self.dead.len() + self.stuck.len()
    }

    /// Return the ids of the agents this pass classified as dead,
    /// in the order they were recorded.
    pub fn dead_ids(&self) -> Vec<String> {
        self.dead.iter().map(|(id, _)| id.clone()).collect()
    }
}

/// Configuration for the [`HealthChecker`] component.
#[derive(Debug, Clone)]
pub struct HealthCheckerConfig {
    /// How long without a heartbeat before marking an Agent as Suspect.
    pub suspect_after: Duration,
    /// How long without a heartbeat before marking an Agent as Dead.
    pub dead_after: Duration,
    /// How long an Agent may stay in `Active` without progress before
    /// being flagged as Stuck.
    pub stuck_after: Duration,
}

impl Default for HealthCheckerConfig {
    fn default() -> Self {
        Self {
            suspect_after: Duration::from_secs(15),
            dead_after: Duration::from_secs(60),
            stuck_after: Duration::from_secs(600),
        }
    }
}

impl From<cog_core::HealthCheckerConfig> for HealthCheckerConfig {
    fn from(c: cog_core::HealthCheckerConfig) -> Self {
        Self {
            suspect_after: Duration::from_secs(c.suspect_after_secs),
            dead_after: Duration::from_secs(c.dead_after_secs),
            stuck_after: Duration::from_secs(c.stuck_after_secs),
        }
    }
}

/// Polls the agent registry + StateBackend to identify unhealthy
/// workers.  Pure observation; no side-effects beyond returning a
/// [`HealthReport`].
pub struct HealthChecker {
    registry: Arc<AgentRegistry>,
    state_backend: Arc<dyn StateBackend>,
    cfg: HealthCheckerConfig,
}

impl HealthChecker {
    pub fn new(
        registry: Arc<AgentRegistry>,
        state_backend: Arc<dyn StateBackend>,
        cfg: HealthCheckerConfig,
    ) -> Self {
        Self {
            registry,
            state_backend,
            cfg,
        }
    }

    /// Run a single health check pass over the registry.
    ///
    /// The population is refreshed from the shared registry first, because
    /// that is where an agent writes its own presence and the supervisor's map
    /// only holds what something told this process. Judging the map alone
    /// reported an empty fleet while the workers were alive and beating.
    pub async fn check(&self) -> SupervisorResult<HealthReport> {
        let mut report = HealthReport::default();
        let now = Utc::now();
        // 共享注册表是「这个舰队里有谁」的权威，所以它的名单也决定这一拍可以忘掉谁
        // （见下面的退休一段）。读失败时 `listed` 留在 `None`：过期的种群是比没有
        // 更差的读数，但一次读不到绝不能读成「所有人都走了」。
        let mut listed: Option<HashSet<String>> = None;
        match self.registry.sync_from_shared().await {
            Ok(ids) => {
                // 没挂共享注册表时 `list` 也返回空表——那个空表说的是「本进程没有
                // 意见」，不是「舰队空了」，所以只有挂上时它才是名单。
                if self.registry.shares_a_registry() {
                    listed = Some(ids.into_iter().collect());
                }
            }
            Err(e) => {
                // The pass still runs over whatever this process already tracks:
                // a stale population is a worse reading than none, but a failed
                // read of the shared registry must not turn into a failed pass.
                tracing::warn!(
                    error = %e,
                    "health pass could not read the shared agent registry; judging only locally tracked agents"
                );
            }
        }
        let agents = self.registry.agents();

        for agent in &agents {
            // 1. Check the durable state first — explicit Dead beats heartbeat.
            let backend_state = self
                .state_backend
                .get_agent_state(&agent.agent_id)
                .await
                .ok()
                .flatten();

            if let Some(AgentState::Dead) = backend_state {
                report.dead.push((
                    agent.agent_id.clone(),
                    HealthIssue::Dead {
                        last_seen: agent.last_heartbeat,
                    },
                ));
                continue;
            }

            // 2. Heartbeat liveness based on registry-recorded timestamps.
            let elapsed = (now - agent.last_heartbeat)
                .to_std()
                .unwrap_or(Duration::from_secs(0));

            if elapsed >= self.cfg.dead_after {
                report.dead.push((
                    agent.agent_id.clone(),
                    HealthIssue::Dead {
                        last_seen: agent.last_heartbeat,
                    },
                ));
                continue;
            }

            if elapsed >= self.cfg.suspect_after {
                let missed_beats = (elapsed.as_secs() / self.cfg.suspect_after.as_secs().max(1))
                    .min(u32::MAX as u64) as u32;
                report.suspect.push((
                    agent.agent_id.clone(),
                    HealthIssue::Suspect { missed_beats },
                ));
                continue;
            }

            // 3. Stuck detection — Agent has been in Active too long.
            if matches!(backend_state, Some(AgentState::Active)) {
                let active_for = (now - agent.state_since)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0));
                if active_for >= self.cfg.stuck_after {
                    report.stuck.push((
                        agent.agent_id.clone(),
                        HealthIssue::Stuck {
                            stuck_seconds: active_for.as_secs(),
                        },
                    ));
                    continue;
                }
            }

            report.healthy.push(agent.agent_id.clone());
        }

        // 判过的 id 里，哪些已经不在名单上了。名单不可读时（`listed` 为 None）这
        // 一格留空：没有名单就没有「离开」，把一个读不到名单的一拍读成「全体离队」
        // 会把整条规则静音。
        if let Some(listed) = &listed {
            report.departed = report
                .suspect
                .iter()
                .chain(report.dead.iter())
                .chain(report.stuck.iter())
                .map(|(id, _)| id)
                .filter(|id| !listed.contains(*id))
                .cloned()
                .collect();
        }

        // 退休：舰队名单里已经没有、而它的死亡已经有机会被报出去的 id，从本进程的
        // 表里删掉。
        //
        // 为什么必须删：这张表只增不减，于是每一个曾经存在过的 id（短命 squad 成员、
        // 每次进化都换名的 agent）都被永久判死——每一拍重新判一次死、重新点亮一次
        // 告警行，种群与告警行都不收敛。实测 2026-10-26：`agent_unhealthy` 一张表里
        // 两万行、当下 firing 的 754 行全部由已经不在名单上的 id 撑着，而真正的舰队
        // 只有个位数 agent，于是「告警太多没人看」把真正要人动手的那两三条埋掉。
        //
        // 为什么等一个死判决的窗口再删：删得太早会让「它掉线了」这件事一次都不被报出
        // ——不在名单里就没人问、没人报，而这条规则存在的理由正是报它。`dead_after`
        // 之后它已经被判死过至少一拍（真实 TTL 远大于这个窗口），再留一个窗口是为了
        // 让那一拍真的跑过，而不是刚好错过。
        if let Some(listed) = listed {
            let forget_after = self.cfg.dead_after * 2;
            let gone: Vec<String> = agents
                .iter()
                .filter(|a| !listed.contains(&a.agent_id))
                .filter(|a| {
                    (now - a.last_heartbeat)
                        .to_std()
                        .unwrap_or(Duration::from_secs(0))
                        >= forget_after
                })
                .map(|a| a.agent_id.clone())
                .collect();
            for id in &gone {
                self.registry.unregister_agent(id);
            }
            if !gone.is_empty() {
                tracing::debug!(
                    retired = gone.len(),
                    "forgot agents the shared registry no longer lists; nothing would ever \
                     have refreshed their verdict again"
                );
            }
        }

        Ok(report)
    }

    /// Reset the heartbeat timestamp for an Agent that was healthy
    /// during this pass — the caller is responsible for invoking this
    /// after observing live activity (e.g. fresh broadcast events).
    pub fn record_recovery(&self, agent: &AgentInfo) {
        self.registry.touch(&agent.agent_id);
    }

    pub fn config(&self) -> &HealthCheckerConfig {
        &self.cfg
    }
}
