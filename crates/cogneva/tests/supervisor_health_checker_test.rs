use chrono::Utc;
use cog_core::{AgentState, StateBackend};
use cog_storage::MemoryStateBackend;
use cog_supervisor::health_checker::{HealthChecker, HealthCheckerConfig};
use cog_supervisor::registry::{AgentInfo, AgentRegistry};
use std::sync::Arc;

fn fresh_registry() -> Arc<AgentRegistry> {
    Arc::new(AgentRegistry::new())
}

fn fresh_backend() -> Arc<dyn StateBackend> {
    Arc::new(MemoryStateBackend::new())
}

fn make_agent(id: &str, secs_since_heartbeat: i64) -> AgentInfo {
    let now = Utc::now();
    AgentInfo {
        agent_id: id.into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: Vec::new(),
        last_heartbeat: now - chrono::Duration::seconds(secs_since_heartbeat),
        state_since: now,
        registered_at: now,
    }
}

#[tokio::test]
async fn healthy_agent_yields_clean_report() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 1));
    let checker = HealthChecker::new(reg, fresh_backend(), HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert!(report.is_clean(), "{:?}", report);
    assert_eq!(report.healthy, vec!["a-1".to_string()]);
}

#[tokio::test]
async fn suspect_threshold_triggers() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 20));
    let checker = HealthChecker::new(reg, fresh_backend(), HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.suspect.len(), 1);
    assert!(report.dead.is_empty());
}

#[tokio::test]
async fn dead_threshold_triggers() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 90));
    let checker = HealthChecker::new(reg, fresh_backend(), HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.dead.len(), 1);
    assert!(report.suspect.is_empty());
}

#[tokio::test]
async fn backend_dead_state_short_circuits() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 1));
    let backend = Arc::new(MemoryStateBackend::new());
    backend
        .set_agent_state("a-1", &AgentState::Dead)
        .await
        .unwrap();
    let checker = HealthChecker::new(reg, backend, HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.dead.len(), 1);
}

#[tokio::test]
async fn stuck_active_agent_detected() {
    let reg = fresh_registry();
    let mut agent = make_agent("a-1", 1);
    agent.state_since = Utc::now() - chrono::Duration::seconds(700);
    reg.register_agent(agent);

    let backend = Arc::new(MemoryStateBackend::new());
    backend
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .unwrap();
    let checker = HealthChecker::new(reg, backend, HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.stuck.len(), 1);
}

// ─── 舰队名单决定这一拍可以忘掉谁 ────────────────────────────────────
//
// 这张表只增不减时，每一个曾经存在过的 id 都被永久判死：每拍重新判一次死、重新点亮
// 一次告警行。实测（2026-10-26）`agent_unhealthy` 的两万行里，当下 firing 的 754 行
// 全部由已经不在名单上的 id 撑着，而真实舰队只有个位数 agent——真正要人动手的那两三条
// 就被这堆永远不收敛的行埋掉了。所以「名单里没有」这件事必须能被表记住。

use cog_core::{AgentRegistration, ResourceInfo, SFResult};
use std::sync::Mutex;

/// 共享注册表的替身：名单由测试改，`list` 说什么就是什么。
struct ListingRegistry {
    listed: Mutex<Vec<AgentRegistration>>,
}

impl ListingRegistry {
    fn with(ids: &[(&str, i64)]) -> Arc<Self> {
        let now = Utc::now();
        Arc::new(Self {
            listed: Mutex::new(
                ids.iter()
                    .map(|(id, secs_since_beat)| AgentRegistration {
                        agent_id: (*id).into(),
                        role: "worker".into(),
                        workspace_id: "ws".into(),
                        capabilities: Vec::new(),
                        resources: ResourceInfo {
                            cpu_cores: 1,
                            memory_gb: 1,
                        },
                        hostname: "host".into(),
                        pod_ip: "10.0.0.1".into(),
                        registered_at: now,
                        last_heartbeat: now - chrono::Duration::seconds(*secs_since_beat),
                    })
                    .collect(),
            ),
        })
    }

    fn forget(&self, id: &str) {
        self.listed
            .lock()
            .unwrap()
            .retain(|registration| registration.agent_id != id);
    }
}

#[async_trait::async_trait]
impl cog_core::AgentRegistry for ListingRegistry {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
        self.listed.lock().unwrap().push(registration.clone());
        Ok(())
    }

    async fn heartbeat(&self, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> SFResult<()> {
        self.forget(agent_id);
        Ok(())
    }

    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        Ok(self
            .listed
            .lock()
            .unwrap()
            .iter()
            .find(|registration| registration.agent_id == agent_id)
            .cloned())
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        Ok(self.listed.lock().unwrap().clone())
    }

    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(self
            .listed
            .lock()
            .unwrap()
            .iter()
            .filter(|registration| registration.role == role)
            .cloned()
            .collect())
    }

    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(self
            .listed
            .lock()
            .unwrap()
            .iter()
            .filter(|registration| registration.capabilities.iter().any(|c| c == capability))
            .cloned()
            .collect())
    }
}

fn has_agent(reg: &AgentRegistry, id: &str) -> bool {
    reg.agents().iter().any(|agent| agent.agent_id == id)
}

/// 掉线之后被报出来、然后被忘掉：三种结局缺一不可。
///
/// 报在前面是这条判据的一半：删得太早会让「它掉线了」一次都不被报出——不在名单里就
/// 没人问、没人报，而这条规则存在的理由正是报它。
#[tokio::test]
async fn an_agent_the_fleet_no_longer_lists_is_forgotten_after_its_death_was_reported() {
    let shared = ListingRegistry::with(&[("gone", 300)]);
    let reg = Arc::new(AgentRegistry::new().with_agent_registry(shared.clone()));
    let checker = HealthChecker::new(
        Arc::clone(&reg),
        fresh_backend(),
        HealthCheckerConfig::default(),
    );

    // 还在名单里：判死，但不许忘——它仍是舰队的一部分，只是不出声。
    let first = checker.check().await.unwrap();
    assert_eq!(first.dead.len(), 1, "{first:?}");
    assert!(has_agent(&reg, "gone"), "名单里还有它，不该忘");

    // 名单里没有了：这一拍仍然判死（报出去），同一拍把表里的那行收掉。
    shared.forget("gone");
    let second = checker.check().await.unwrap();
    assert_eq!(second.dead.len(), 1, "忘掉之前必须先报一次：{second:?}");
    assert!(!has_agent(&reg, "gone"), "名单里没有它，表里不该再留着");

    // 下一拍就干净了：没人会再提起这个 id，也不该再有人替它报警。
    let third = checker.check().await.unwrap();
    assert!(third.is_clean(), "{third:?}");
}

/// 名单里还有、但心跳停了：一直判死一直留着。舰队没把它踢出去，本进程就不许替它
/// 做主——「还在名单里」是唯一一条能证明还有人会刷新它的证据。
#[tokio::test]
async fn an_agent_still_in_the_fleet_keeps_its_dead_verdict_pass_after_pass() {
    let shared = ListingRegistry::with(&[("quiet", 3600)]);
    let reg = Arc::new(AgentRegistry::new().with_agent_registry(shared));
    let checker = HealthChecker::new(
        Arc::clone(&reg),
        fresh_backend(),
        HealthCheckerConfig::default(),
    );

    for round in 0..3 {
        let report = checker.check().await.unwrap();
        assert_eq!(report.dead.len(), 1, "第 {round} 拍：{report:?}");
        assert!(has_agent(&reg, "quiet"), "第 {round} 拍：不该忘");
    }
}

/// 没挂共享注册表时不判「离队」：那个空名单说的是「本进程没有意见」，不是「舰队空了」。
#[tokio::test]
async fn without_a_shared_registry_a_silent_agent_is_never_forgotten() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 3600));
    let checker = HealthChecker::new(
        Arc::clone(&reg),
        fresh_backend(),
        HealthCheckerConfig::default(),
    );

    for round in 0..3 {
        let report = checker.check().await.unwrap();
        assert_eq!(report.dead.len(), 1, "第 {round} 拍：{report:?}");
        assert!(has_agent(&reg, "a-1"), "第 {round} 拍：不该忘");
    }
}
