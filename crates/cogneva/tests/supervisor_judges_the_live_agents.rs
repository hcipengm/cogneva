//! The health pass judges the agents that are actually alive.
//!
//! Presence is written by the agent itself: a worker calls the gateway's
//! register route, beats every few seconds, and its record is dropped when the
//! beats stop. The supervisor keeps a map of its own, and in the deployed
//! topology nothing ever filled it — so the pass walked an empty list and
//! reported a fleet of zero, which is the same shape a genuinely empty
//! deployment prints. The status route and the process view both read that
//! report, so three live workers showed up as `healthy_agents: 0` and as
//! `health: unknown` next to every roster entry.
//!
//! These cases attach the supervisor's map to the shared registry the way the
//! plugin does and ask what the pass sees.

use std::sync::Arc;

use cog_core::{AgentRegistration, AgentRegistry, ResourceInfo, StateBackend};
use cog_storage::{MemoryAgentRegistry, MemoryStateBackend};
use cog_supervisor::health_checker::{HealthChecker, HealthCheckerConfig};

/// One live unit in the shared registry, registered the way a worker does.
async fn registered(shared: &MemoryAgentRegistry, agent_id: &str) {
    shared
        .register(&AgentRegistration::new(
            agent_id,
            "worker-pod",
            "10.42.0.10",
            "worker",
            "cogneva",
            Vec::new(),
            ResourceInfo {
                cpu_cores: 1,
                memory_gb: 1,
            },
        ))
        .await
        .expect("the shared registry takes a registration");
}

fn checker(shared: Arc<MemoryAgentRegistry>) -> HealthChecker {
    let registry = Arc::new(cog_supervisor::AgentRegistry::new().with_agent_registry(shared));
    let state: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    HealthChecker::new(registry, state, HealthCheckerConfig::default())
}

/// The premise: nothing populates the supervisor's own map in the deployed
/// topology. If that ever changes this case stops describing the bug, and the
/// one below would pass for the wrong reason.
#[tokio::test]
async fn the_supervisors_own_map_starts_empty() {
    let shared = Arc::new(MemoryAgentRegistry::new());
    registered(&shared, "cogneva-worker-0").await;
    let registry = cog_supervisor::AgentRegistry::new().with_agent_registry(shared.clone());
    assert_eq!(
        registry.agent_count(),
        0,
        "the supervisor's map is filled by the pass, not by registration"
    );
}

/// A live worker registered in the shared registry is healthy in the pass —
/// not missing from it. This is the case that printed zero.
#[tokio::test]
async fn an_agent_alive_in_the_shared_registry_is_judged_healthy() {
    let shared = Arc::new(MemoryAgentRegistry::new());
    registered(&shared, "cogneva-worker-0").await;
    registered(&shared, "cogneva-worker-1").await;

    let report = checker(shared).check().await.expect("the pass runs");

    assert!(
        report.healthy.contains(&"cogneva-worker-0".to_string()),
        "a registered, beating agent must be judged, not skipped: {report:?}"
    );
    assert!(report.healthy.contains(&"cogneva-worker-1".to_string()));
    assert!(
        report.dead.is_empty(),
        "a fresh registration is not dead: {report:?}"
    );
}

/// The population is refreshed on every pass, so an agent that registers after
/// the first pass is judged by the second one. A sync done once at startup
/// would pass the case above and miss every worker that came up later.
#[tokio::test]
async fn an_agent_that_registers_later_is_judged_by_the_next_pass() {
    let shared = Arc::new(MemoryAgentRegistry::new());
    let checker = checker(shared.clone());

    let first = checker.check().await.expect("first pass");
    assert!(first.healthy.is_empty(), "nobody has registered yet");

    registered(&shared, "cogneva-worker-late").await;
    let second = checker.check().await.expect("second pass");
    assert!(
        second.healthy.contains(&"cogneva-worker-late".to_string()),
        "the pass must read presence again, not reuse the first listing: {second:?}"
    );
}
