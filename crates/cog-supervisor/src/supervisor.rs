use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::{AgentEvent, ObservabilityGateway, OrchestratorControl, StateBackend};
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

use crate::autonomous::{AutonomousCollaborator, AutonomousConfig, AUTONOMOUS_EVENT_LOOP};

/// Loop name reported through the background-loop liveness family.
pub const SUPERVISOR_LOOP: &str = "supervisor_main";

/// How long an agent whose heartbeat has stopped stays in the registry before
/// it is dropped.
///
/// Counted from the last heartbeat, and long enough that the recovery paths
/// finish first: the respawner reads the dead set on every pass to retry the
/// crew an agent belonged to, and a crew is retried more than once before it is
/// given up on. Reaping earlier would delete the evidence while the attempt to
/// bring the crew back was still running; reaping later costs nothing but the
/// announcement each pass makes about an agent that is already gone.
// cap: bound
const REAP_AFTER_DEAD: Duration = Duration::from_secs(1800);

/// The verdicts a pass may announce as an unhealthy agent.
///
/// A verdict about an id the fleet no longer lists is a departure, not an
/// unhealthy agent. Both look the same from the registry's side -- a one-shot
/// role that finishes deregisters itself, and a record that stops being renewed
/// expires -- so the distinction cannot be recovered afterwards; it has to be
/// applied where the pass still knows whether the id was listed.
///
/// Announcing departures was the rule's whole output: measured 2026-10-26, 222
/// of the 233 ids `agent_unhealthy` held firing had already left the registry's
/// index (orderly deregistrations), leaving the one resident row that had been
/// firing for forty minutes buried under them. What a departure loses by not
/// being announced here is covered where the work itself is: a one-shot that
/// dies mid-task leaves its DAG task stalled (`dag_task_stalled_after_ready`
/// fires on it), and a resident process that dies is a pod event.
fn announceable(
    report: &crate::HealthReport,
) -> impl Iterator<Item = (&String, &cog_core::HealthIssue)> {
    report
        .suspect
        .iter()
        .chain(report.dead.iter())
        .chain(report.stuck.iter())
        .filter_map(move |(agent_id, issue)| {
            report.announces(agent_id).then_some((agent_id, issue))
        })
}

/// Publish one `AgentUnhealthy` event per verdict the pass may announce.
///
/// Kept as its own function so the question "does a departure reach the event
/// stream?" can be asked of a real channel instead of a hand-built report: the
/// events are what the alert bridge turns into rows, and a filter that only
/// exists in a report field is a filter no reader ever sees.
fn announce_verdicts(report: &crate::HealthReport, event_tx: &broadcast::Sender<SupervisorEvent>) {
    for (agent_id, issue) in announceable(report) {
        let _ = event_tx.send(SupervisorEvent::AgentUnhealthy {
            agent_id: agent_id.clone(),
            issue: issue.clone(),
            timestamp: Utc::now(),
        });
    }
}
use crate::control_plane::{ControlPlaneClient, HttpControlPlaneClient, SupervisorStatus};
use crate::error::SupervisorResult;
use crate::event_aggregator::{EventAggregator, EVENT_AGGREGATOR_LOOP};
use crate::health_checker::{HealthChecker, HealthCheckerConfig};
use crate::lifecycle_coordinator::LifecycleCoordinator;
use crate::quota_enforcer::QuotaEnforcer;
use crate::registry::AgentRegistry;
use crate::respawner::Respawner;
use crate::scheduler_gate::SchedulerGate;
use crate::task_rebalancer::{TaskRebalancer, TaskRebalancerConfig};
use cog_core::{SupervisorEvent, WorkspaceQuotaSource};

/// Periodic intervals for Supervisor sub-systems.
/// - Health: 10s (a slower beat than the per-Agent 5s lifecycle heartbeat)
/// - Quota: 60s
/// - Rebalance: 300s (5 minutes)
/// - Event aggregation flush: 30s
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub health_interval: Duration,
    pub quota_interval: Duration,
    pub rebalance_interval: Duration,
    pub event_window: Duration,
    pub broadcast_capacity: usize,
    pub health_checker: HealthCheckerConfig,
    pub task_rebalancer: TaskRebalancerConfig,
    pub autonomous: AutonomousConfig,
    /// Workspace quota threshold below which the scheduler is paused.
    pub quota_threshold: u64,
    /// Interval between control plane status reports.
    pub control_plane_interval: Duration,
    /// Optional control plane URL. If Some, an HttpControlPlaneClient is created.
    pub control_plane_url: Option<String>,
    /// Maximum action history entries per behavior monitor.
    pub behavior_history_max: usize,
    /// Maximum heartbeat records retained per agent in the registry.
    pub heartbeat_history_max: usize,
    /// Maximum alerts retained in the alert store.
    pub alert_history_max: usize,
}

/// 健康巡检周期：10s。
///
/// 判据（可疑 15s、死亡 60s）都在秒级，巡检必须比它们密，否则判据被巡检自己
/// 拖粗。
// cap: bound
const HEALTH_INTERVAL: Duration = Duration::from_secs(10);

/// 配额巡检周期：60s。
///
/// 配额是按分钟算的额度，一分钟读一次就够；更密只是重复读同一批计数。
// cap: bound
const QUOTA_INTERVAL: Duration = Duration::from_secs(60);

/// 任务再平衡周期：300s。
///
/// 再平衡会挪动在跑的任务，代价远高于一次巡检；五分钟一拍让舰队形状稳定下来，
/// 而不是跟着一次抖动来回搬。
// cap: bound
const REBALANCE_INTERVAL: Duration = Duration::from_secs(300);

/// 事件窗口：30s。
///
/// 告警去重与突发判定都看这个窗口；它要长到能盖住一次真实的故障脉冲，短到
/// 不会把两次独立故障并成一条。
// cap: bound
const EVENT_WINDOW: Duration = Duration::from_secs(30);

/// 控制面巡检周期：30s。
///
/// 控制面地址是可选的外部依赖，读它的代价在网络往返上；30s 一拍足够发现
/// 「控制面不在了」，而不会把监控面变成对它的压力源。
// cap: bound
const CONTROL_PLANE_INTERVAL: Duration = Duration::from_secs(30);

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            health_interval: HEALTH_INTERVAL,
            quota_interval: QUOTA_INTERVAL,
            rebalance_interval: REBALANCE_INTERVAL,
            event_window: EVENT_WINDOW,
            broadcast_capacity: 256,
            health_checker: HealthCheckerConfig::default(),
            task_rebalancer: TaskRebalancerConfig::default(),
            autonomous: AutonomousConfig::default(),
            quota_threshold: 1_000,
            control_plane_interval: CONTROL_PLANE_INTERVAL,
            control_plane_url: None,
            behavior_history_max: 20,
            heartbeat_history_max: 1_000,
            alert_history_max: 10_000,
        }
    }
}

impl From<cog_core::SupervisorConfig> for SupervisorConfig {
    fn from(c: cog_core::SupervisorConfig) -> Self {
        Self {
            health_interval: Duration::from_secs(c.health_interval_secs),
            quota_interval: Duration::from_secs(c.quota_interval_secs),
            rebalance_interval: Duration::from_secs(c.rebalance_interval_secs),
            event_window: Duration::from_secs(c.event_window_secs),
            broadcast_capacity: c.broadcast_capacity,
            health_checker: c.health_checker.into(),
            task_rebalancer: c.task_rebalancer.into(),
            autonomous: AutonomousConfig::default(),
            quota_threshold: c.quota_threshold,
            control_plane_interval: Duration::from_secs(c.control_plane_interval_secs),
            control_plane_url: c.control_plane_url,
            behavior_history_max: c.behavior_history_max,
            heartbeat_history_max: c.heartbeat_history_max,
            alert_history_max: c.alert_history_max,
        }
    }
}

/// Top-level Supervisor coordinating health, retry, quota, rebalance,
/// and event aggregation sub-systems.
/// The Supervisor is constructed with shared `Arc` references to the
/// orchestrator, state backend, observability gateway, quota manager,
/// and scheduler gate.  It exposes a single [`Self::run`] entry point that
/// drives all periodic tasks until a shutdown signal fires.
pub struct Supervisor {
    pub config: std::sync::RwLock<SupervisorConfig>,
    pub registry: Arc<AgentRegistry>,
    pub orchestrator: Arc<dyn OrchestratorControl>,
    pub gate: Arc<SchedulerGate>,
    pub health: HealthChecker,
    pub respawner: Respawner,
    pub quota: QuotaEnforcer,
    pub rebalancer: TaskRebalancer,
    pub aggregator: Arc<EventAggregator>,
    pub lifecycle: LifecycleCoordinator,
    pub autonomous: Arc<AutonomousCollaborator>,
    agent_event_rx: broadcast::Receiver<AgentEvent>,
    event_tx: broadcast::Sender<SupervisorEvent>,
    control_plane: Option<Arc<dyn ControlPlaneClient>>,
    last_rebalance: tokio::sync::Mutex<Option<chrono::DateTime<chrono::Utc>>>,
    /// Behavior monitors for anti-loop detection, keyed by agent_id.
    pub behavior_monitors: std::sync::Mutex<
        std::collections::HashMap<String, crate::behavior_monitor::BehaviorMonitor>,
    >,
    /// The command stream into the agents. `cog-protocol` implements
    /// [`cog_core::AgentLifecycleServer`] over its gRPC command router and
    /// publishes it; taking it here is what turns `kill_agent` /
    /// `restart_agent` / `checkpoint_agent` from a broadcast-only notice into
    /// a real dispatch. Unset only where a Supervisor is built without the
    /// protocol plugin.
    lifecycle_server: Option<Arc<dyn cog_core::AgentLifecycleServer>>,
    /// Optional watch receiver for dynamic config reload.
    config_rx: Option<watch::Receiver<SupervisorConfig>>,
    /// Where each pass of the loop reports how it ended. Unset when the metrics
    /// backend was not attached: the pass then leaves only the log line it
    /// always left.
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
}

/// The arms of the supervisor loop, one per supervised pass.
///
/// Each loop iteration fires exactly one of these, so a round is one pass. The
/// loop's own beat is driven by the shortest of the six intervals, which means a
/// starved pass stays hidden behind a beat the others keep stamping unless each
/// pass reports its own rounds.
#[derive(Debug, Clone, Copy)]
enum SupervisorPass {
    Health,
    Quota,
    Rebalance,
    Event,
    Autonomous,
    ControlPlane,
}

impl SupervisorPass {
    fn as_cell(&self) -> &'static str {
        match self {
            SupervisorPass::Health => "health",
            SupervisorPass::Quota => "quota",
            SupervisorPass::Rebalance => "rebalance",
            SupervisorPass::Event => "event",
            SupervisorPass::Autonomous => "autonomous",
            SupervisorPass::ControlPlane => "control_plane",
        }
    }
}

impl Supervisor {
    /// Build a Supervisor from production-grade dependencies.
    /// Components like the [`cog_core::QuotaManager`] are wrapped behind their
    /// trait equivalents so that test setups can substitute mocks.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: SupervisorConfig,
        registry: Arc<AgentRegistry>,
        state_backend: Arc<dyn StateBackend>,
        orchestrator: Arc<dyn OrchestratorControl>,
        quota: Arc<dyn WorkspaceQuotaSource>,
        gateway: Arc<dyn ObservabilityGateway>,
        gate: Arc<SchedulerGate>,
        agent_events: broadcast::Sender<AgentEvent>,
        meta_learning: Option<Arc<dyn cog_core::MetaLearning>>,
    ) -> Self {
        // Subscribed here rather than by the caller: the supervisor needs the
        // sender anyway -- the lifecycle coordinator announces state
        // transitions on this bus, and a caller that only ever hands over a
        // receiver cannot give it one.
        let agent_event_rx = agent_events.subscribe();
        let health = HealthChecker::new(
            registry.clone(),
            state_backend.clone(),
            config.health_checker.clone(),
        );

        let (event_tx, _rx) = broadcast::channel(config.broadcast_capacity);
        let respawner =
            Respawner::new(registry.clone(), orchestrator.clone()).with_event_tx(event_tx.clone());

        let quota_enforcer = QuotaEnforcer::new(quota, config.quota_threshold, gate.clone());

        let rebalancer = TaskRebalancer::new(
            registry.clone(),
            state_backend.clone(),
            orchestrator.clone(),
            config.task_rebalancer.clone(),
        );

        let autonomous_rx = agent_event_rx.resubscribe();
        let aggregator =
            Arc::new(EventAggregator::new(agent_event_rx).with_gateway(gateway.clone()));

        let mut autonomous_builder = AutonomousCollaborator::new(
            config.autonomous.clone(),
            gate.clone(),
            state_backend.clone(),
            orchestrator.clone(),
            event_tx.clone(),
        );
        if let Some(ref meta) = meta_learning {
            autonomous_builder = autonomous_builder.with_meta_learning(meta.clone());
        }
        let autonomous = Arc::new(autonomous_builder);

        let lifecycle =
            LifecycleCoordinator::new(registry.clone(), state_backend, event_tx.clone())
                .with_agent_events(agent_events);

        let control_plane: Option<Arc<dyn ControlPlaneClient>> =
            config.control_plane_url.as_ref().map(|url| {
                Arc::new(HttpControlPlaneClient::new(url.clone())) as Arc<dyn ControlPlaneClient>
            });

        Self {
            config: std::sync::RwLock::new(config),
            registry,
            orchestrator: orchestrator.clone(),
            gate,
            health,
            respawner,
            quota: quota_enforcer,
            rebalancer,
            aggregator,
            lifecycle,
            autonomous,
            agent_event_rx: autonomous_rx,
            event_tx,
            control_plane,
            last_rebalance: tokio::sync::Mutex::new(None),
            behavior_monitors: std::sync::Mutex::new(std::collections::HashMap::new()),
            lifecycle_server: None,
            config_rx: None,
            metrics: None,
        }
    }

    /// Attach the command stream used to actually dispatch lifecycle commands
    /// to the agents. Without it the three lifecycle methods can only report
    /// that they did nothing.
    pub fn with_lifecycle_server(
        mut self,
        server: Arc<dyn cog_core::AgentLifecycleServer>,
    ) -> Self {
        self.lifecycle_server = Some(server);
        self
    }

    /// Attach a [`watch::Receiver`] so the Supervisor can react to config
    /// reloads at runtime.  Call this before [`Self::run`].
    pub fn with_config_watch(mut self, rx: watch::Receiver<SupervisorConfig>) -> Self {
        self.config_rx = Some(rx);
        self
    }

    /// Attach the sink for each pass's per-round outcome.
    ///
    /// The owner attaches it after the metrics backend has been published; a
    /// `None` sink is not an error -- the pass keeps its log line -- so it is
    /// simply left unset.
    pub fn with_metrics(mut self, metrics: Option<Arc<dyn cog_core::MetricsBackend>>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Record how one arm of the loop ended.
    ///
    /// Every arm routes its round through here, so the reading covers all six
    /// the same way. The `None` sink and a backend error are both swallowed to a
    /// WARN: a missing reading must not take the supervision loop down with it.
    async fn record_pass(&self, pass: SupervisorPass, ok: bool) {
        self.record_pass_outcome(pass, if ok { "ok" } else { "failed" })
            .await;
    }

    /// Record a round of one arm under an outcome the caller names.
    ///
    /// Separate from [`Self::record_pass`] for the arm whose ending is neither
    /// success nor failure: with no control plane URL configured the
    /// control-plane arm is idle, not fine, and [`crate::OUTCOME_SKIPPED`] is
    /// the only cell that says so. Before it existed that arm wrote nothing,
    /// and nothing is what a reader sees for an arm whose rounds all succeed.
    async fn record_pass_outcome(&self, pass: SupervisorPass, outcome: &str) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let mut labels = std::collections::HashMap::new();
        labels.insert(crate::LABEL_PASS.to_string(), pass.as_cell().to_string());
        labels.insert(crate::LABEL_OUTCOME.to_string(), outcome.to_string());
        if let Err(e) = metrics
            .record_counter(cog_core::metric_names::SUPERVISOR_PASS_TOTAL, 1.0, labels)
            .await
        {
            warn!(error = %e, "supervisor pass: could not record outcome");
        }
    }

    /// Attach a root-cause classifier so exhausted-retry decisions carry a
    /// cause-matched recovery recommendation.
    pub fn with_fault_classifier(mut self, classifier: Arc<dyn cog_core::FaultClassifier>) -> Self {
        self.respawner.set_fault_classifier(classifier);
        self
    }

    /// Subscribe to the Supervisor's event broadcast channel.
    pub fn subscribe(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.event_tx.subscribe()
    }

    /// Track a workspace for quota enforcement.  Pass-through to the
    /// [`QuotaEnforcer`].
    pub fn track_workspace(&self, workspace_id: impl Into<String>) {
        self.quota.track_workspace(workspace_id);
    }

    /// Fetch a clone of the broadcast sender.  Useful for component
    /// composition where another module needs to publish a Supervisor
    /// event without owning the Supervisor itself.
    pub fn event_sender(&self) -> broadcast::Sender<SupervisorEvent> {
        self.event_tx.clone()
    }

    /// Record an agent action for anti-loop detection.
    pub fn record_agent_action(
        &self,
        agent_id: impl Into<String>,
        action: crate::behavior_monitor::ActionType,
        outcome: crate::behavior_monitor::ActionOutcome,
    ) {
        let id = agent_id.into();
        let mut monitors = self
            .behavior_monitors
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let max = self
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .behavior_history_max;
        let monitor = monitors.entry(id.clone()).or_insert_with(|| {
            crate::behavior_monitor::BehaviorMonitor::with_max_history(id.clone(), max)
        });
        monitor.record(action, outcome);
        let severity = monitor.detect_loop();
        if severity != cog_core::LoopSeverity::None {
            if let Some(prompt) = monitor.intervention_prompt(severity) {
                tracing::warn!("{}", prompt);
                let _ = self.event_tx.send(SupervisorEvent::AgentRuntimeDetected {
                    agent_id: id,
                    severity,
                    timestamp: Utc::now(),
                });
            }
        }
    }

    /// Drive a single health-check pass and emit events for any
    /// unhealthy agents.  Returns the underlying [`crate::HealthReport`].
    pub async fn run_health_pass(&self) -> SupervisorResult<crate::HealthReport> {
        let report = self.health.check().await?;

        // Drive lifecycle state transitions and checkpoint recovery.
        let lifecycle_report = self.lifecycle.handle_health_report(&report).await?;
        if !lifecycle_report.transitioned.is_empty() {
            debug!(
                "Lifecycle transitions: {} succeeded, {} failed, {} checkpoints recovered",
                lifecycle_report.transitioned.len(),
                lifecycle_report.failed.len(),
                lifecycle_report.recovered_checkpoints.len(),
            );
        }

        // If checkpoints were recovered for dead agents, build a recovery
        // plan so the orchestrator can re-queue those tasks.
        if !lifecycle_report.recovered_checkpoints.is_empty() {
            let recovery_plan = self
                .rebalancer
                .recovery_plan(&lifecycle_report.recovered_checkpoints)
                .await?;
            if !recovery_plan.checkpoint_recoveries.is_empty() {
                let _ = self.event_tx.send(SupervisorEvent::Rebalance {
                    ready_tasks: recovery_plan.ready_tasks,
                    active_agents: recovery_plan.available_agents,
                    plan_size: recovery_plan.assignments.len(),
                    checkpoint_recoveries: recovery_plan.checkpoint_recoveries.len(),
                    timestamp: Utc::now(),
                });
            }
        }

        // Legacy event emission for backward compatibility with subscribers
        // that expect AgentUnhealthy events from the Supervisor directly.
        announce_verdicts(&report, &self.event_tx);

        if !report.dead.is_empty() {
            let dead_ids = report.dead_ids();
            let respawn = self.respawner.handle_dead_agents(&dead_ids).await?;
            for action in &respawn.retried_crews {
                let _ = self.event_tx.send(SupervisorEvent::CrewRetried {
                    crew_id: action.crew_id.clone(),
                    task_ids: action.task_ids.clone(),
                    retried: action.retried,
                    timestamp: Utc::now(),
                });
            }
            for action in &respawn.respawn_requested {
                let _ = self.event_tx.send(SupervisorEvent::SquadRespawnRequested {
                    crew_id: action.crew_id.clone(),
                    squad_id: None,
                    reason: action.reason.clone(),
                    timestamp: Utc::now(),
                });
            }
        }

        self.reap_agents_gone(&report.dead);

        Ok(report)
    }

    /// Drop agents whose heartbeat has been gone past the grace period, and
    /// say so on the event stream so the alert row they left behind is settled.
    ///
    /// Without this the registry is a ledger of every agent that ever sent a
    /// heartbeat: `touch` and `record_heartbeat` insert unknown ids, and nothing
    /// in the platform calls `unregister_agent`. A respawn registers a new id,
    /// so the predecessor never comes back, and each health pass then announces
    /// every one of those ids as dead. The volume a supervisor event subscriber
    /// has to carry is therefore proportional to the number of agents that have
    /// *ever* existed, and every subscriber falls behind it in that proportion
    /// — the alerts among them stop being written while looking exactly like a
    /// quiet period. Dropping the entry is what ends the announcement; the
    /// event is the other half of it, because an agent that no longer exists
    /// cannot be unhealthy and its row must not stay firing with a clock that
    /// nothing moves.
    ///
    /// The grace is measured from the moment the agent was last seen, not from
    /// when this pass noticed: a supervisor that restarts has no memory of the
    /// previous pass, and a grace counted from first observation would restart
    /// with it.
    fn reap_agents_gone(&self, dead: &[(String, cog_core::HealthIssue)]) {
        if dead.is_empty() {
            return;
        }
        let dead_after = self
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .health_checker
            .dead_after;
        let grace = chrono::Duration::from_std(dead_after).unwrap_or_default()
            + chrono::Duration::from_std(REAP_AFTER_DEAD).unwrap_or_default();
        let now = Utc::now();
        for (agent_id, issue) in dead {
            let last_seen = match issue {
                cog_core::HealthIssue::Dead { last_seen } => *last_seen,
                // Not a heartbeat verdict: fall back to the registry's own
                // clock, and keep the agent when there is no entry left to
                // read (another pass already reaped it).
                _ => match self.registry.get_agent(agent_id) {
                    Some(info) => info.last_heartbeat,
                    None => continue,
                },
            };
            let gone_for = now.signed_duration_since(last_seen);
            if gone_for < grace {
                continue;
            }
            self.registry.unregister_agent(agent_id);
            warn!(
                agent_id = %agent_id,
                gone_secs = gone_for.num_seconds(),
                "reaped an agent that has been dead past its grace"
            );
            let _ = self.event_tx.send(SupervisorEvent::AgentKilled {
                agent_id: agent_id.clone(),
                reason: format!("reaped: no heartbeat for {}s", gone_for.num_seconds()),
                timestamp: now,
            });
        }
    }

    /// Drive a single quota-enforcement pass and emit events.
    pub async fn run_quota_pass(&self) -> SupervisorResult<()> {
        let report = self.quota.enforce().await?;
        for snap in &report.snapshots {
            if snap.breached {
                let _ = self.event_tx.send(SupervisorEvent::QuotaThresholdBreached {
                    workspace_id: snap.workspace_id.clone(),
                    remaining: snap.remaining,
                    threshold: snap.threshold,
                    scheduler_paused: report.paused || self.gate.is_paused(),
                    timestamp: Utc::now(),
                });
            } else if snap.recovered {
                let _ = self.event_tx.send(SupervisorEvent::QuotaRecovered {
                    workspace_id: snap.workspace_id.clone(),
                    remaining: snap.remaining,
                    timestamp: Utc::now(),
                });
            }
        }
        Ok(())
    }

    /// Drive a single rebalance pass and emit events for the resulting
    /// plan, if any.
    pub async fn run_rebalance_pass(&self) -> SupervisorResult<()> {
        let plan = self.rebalancer.plan().await?;

        // Direct execution: assign tasks to agents per the rebalance plan.
        if !plan.assignments.is_empty() {
            for (agent_id, task_id) in &plan.assignments {
                if let Err(e) = self.orchestrator.assign_task(task_id, agent_id).await {
                    warn!(
                        "Supervisor rebalance: failed to assign task {} to agent {}: {}",
                        task_id, agent_id, e
                    );
                }
            }
        }

        if plan.ready_tasks > 0 || !plan.is_empty() {
            let _ = self.event_tx.send(SupervisorEvent::Rebalance {
                ready_tasks: plan.ready_tasks,
                active_agents: plan.available_agents,
                plan_size: plan.assignments.len(),
                checkpoint_recoveries: plan.checkpoint_recoveries.len(),
                timestamp: Utc::now(),
            });
        }
        Ok(())
    }

    /// Report what the aggregation loop consumed since the last pass.
    ///
    /// The events are taken by [`EventAggregator::wait_and_drain`], which wakes
    /// on the channel; this pass keeps the window's cadence because it is the
    /// half that writes. Making one pass do both is what put a ceiling on the
    /// consumer.
    pub async fn run_event_pass(&self) -> SupervisorResult<()> {
        let stats = self.aggregator.take_stats().await;
        // 取出来的是**自上一趟报告以来**的累计，所以这里的数就是这一窗丢掉的条数，
        // 累加进计数器正是「每丢掉一条加一」的语义。计数放在这里而不是消费环里，
        // 是因为聚合器自己不带后端，而调它的这一层（supervisor）带；消费环每唤醒
        // 一次就写一次计数器则是一趟一条写放大。
        if stats.dropped > 0 {
            crate::consumer_lag::record_consumer_lag(
                &self.metrics,
                crate::consumer_lag::LAG_CONSUMER_SUPERVISOR_EVENT_AGGREGATOR,
                stats.dropped,
            )
            .await;
        }
        // 每趟都记，包括都是 0 的那一趟：`taken=0` 是一条 0 样本，序列不动才是
        // 「没跑」——把两者印成同一种安静，正是这条读数要拆开的那一对。
        crate::consumer_lag::record_handoff(
            &self.metrics,
            crate::consumer_lag::LAG_CONSUMER_SUPERVISOR_EVENT_AGGREGATOR,
            stats.received,
            stats.republished,
        )
        .await;
        if let Some(event) = EventAggregator::build_supervisor_event(
            &stats,
            self.config
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .event_window
                .as_secs(),
        ) {
            let _ = self.event_tx.send(event);
        }
        Ok(())
    }

    /// Run the Supervisor until `shutdown` resolves.
    pub async fn run<F>(self: Arc<Self>, shutdown: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let cfg = self
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut health = tokio::time::interval(cfg.health_interval);
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut quota = tokio::time::interval(cfg.quota_interval);
        quota.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut rebalance = tokio::time::interval(cfg.rebalance_interval);
        rebalance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut events = tokio::time::interval(cfg.event_window);
        events.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut autonomous_tick =
            tokio::time::interval(Duration::from_secs(cfg.autonomous.decision_interval_secs));
        autonomous_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut control_plane_tick = tokio::time::interval(cfg.control_plane_interval);
        control_plane_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // The loop beats whenever any of its tickers fires, so the cadence it can
        // be held to is the shortest of them: one stall threshold for all of them
        // has to be the tightest one, or a slow arm would read as a stall.
        let beat_period = [
            cfg.health_interval,
            cfg.quota_interval,
            cfg.rebalance_interval,
            cfg.event_window,
            Duration::from_secs(cfg.autonomous.decision_interval_secs),
            cfg.control_plane_interval,
        ]
        .into_iter()
        .min()
        .unwrap_or(cfg.health_interval);
        // The caller's future is this loop's stop condition, so the exit it leads
        // to is the intended one and must not be counted as a death. It is not
        // Clone, so it cannot be rebuilt with the body; it is folded into a
        // signal once, and every attempt shares that signal as its stop.
        let supervisor_stop = cog_core::ShutdownSignal::new();
        let stop_from_caller = supervisor_stop.clone();
        let waited = shutdown;
        tokio::spawn(async move {
            waited.await;
            stop_from_caller.trigger();
        });

        // Spawn autonomous event loop. Outside the supervised body: it is a loop
        // of its own, and rebuilding it per attempt would leave the previous one
        // running with nothing holding its handle.
        //
        // 登记进循环表：它以前是个裸 `tokio::spawn`，退出之后读数面上什么都没有
        // ——交接与失败恢复从此不再被驱动，而看起来与「没有值得协调的事」同形。
        // 停机信号与下面那条 supervisor 环同一个：停机时退出的那一次不算「死了」。
        // 订阅一次、跨重启共用：body 每次尝试都重建，重新订阅会落在通道当前的
        // 尾巴上，环死期间到达的事件就再也补不回来——而那正是它最该补上的那一段。
        let autonomous_rx = Arc::new(tokio::sync::Mutex::new(self.agent_event_rx.resubscribe()));
        // 这条环的停条件也取自 `supervisor_stop`，但那个信号下面那条环还要用，
        // 所以这里先克隆一份再移进闭包。
        let autonomous_stop = supervisor_stop.clone();
        // 登记名写成裸标识符（用上面导入的常量）：门要求登记名来自一个 `*_LOOP`
        // 声明，路径形式的 `crate::…::NAME` 会被读成「在调用点现写的字面量」。
        let _autonomous_handle = cog_core::loop_health::spawn(
            AUTONOMOUS_EVENT_LOOP,
            cog_core::loop_health::Cadence::EventDriven,
            autonomous_stop.clone(),
            {
                let this = Arc::clone(&self);
                move |beat| {
                    let this = Arc::clone(&this);
                    let rx = Arc::clone(&autonomous_rx);
                    let stop = autonomous_stop.clone();
                    async move {
                        crate::autonomous::run_autonomous_event_loop(
                            Arc::clone(&this.autonomous),
                            rx,
                            this.metrics.clone(),
                            stop,
                            beat,
                        )
                        .await;
                    }
                }
            },
        );

        // Spawn the aggregator's consumer, for the same reason the autonomous
        // loop lives outside the supervised body: it is a loop of its own, and
        // rebuilding it per attempt would leave the previous one running with
        // nothing holding its handle. It is also why it is not driven from the
        // supervisor's select loop — a consumer that only reads on a ticker has
        // the ticker's throughput.
        let aggregator_stop = supervisor_stop.clone();
        let _aggregator_handle = cog_core::loop_health::spawn(
            EVENT_AGGREGATOR_LOOP,
            cog_core::loop_health::Cadence::EventDriven,
            aggregator_stop.clone(),
            {
                let aggregator = Arc::clone(&self.aggregator);
                move |beat| {
                    let aggregator = Arc::clone(&aggregator);
                    let stop = aggregator_stop.clone();
                    async move {
                        crate::event_aggregator::run_event_aggregator_loop(aggregator, stop, beat)
                            .await;
                    }
                }
            },
        );

        info!(
            "Supervisor starting (intervals: health={:?}, quota={:?}, rebalance={:?}, event={:?})",
            cfg.health_interval, cfg.quota_interval, cfg.rebalance_interval, cfg.event_window,
        );

        let _ = cog_core::loop_health::spawn(
            SUPERVISOR_LOOP,
            cog_core::loop_health::Cadence::Periodic(beat_period),
            supervisor_stop.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let this = Arc::clone(&self);
                let cfg = cfg.clone();
                let supervisor_stop = supervisor_stop.clone();
                async move {
                    let mut health = tokio::time::interval(cfg.health_interval);
                    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut quota = tokio::time::interval(cfg.quota_interval);
                    quota.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut rebalance = tokio::time::interval(cfg.rebalance_interval);
                    rebalance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut events = tokio::time::interval(cfg.event_window);
                    events.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut autonomous_tick = tokio::time::interval(Duration::from_secs(
                        cfg.autonomous.decision_interval_secs,
                    ));
                    autonomous_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut control_plane_tick = tokio::time::interval(cfg.control_plane_interval);
                    control_plane_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                    // Counting restarts at one per attempt: a cycle number is a
                    // reading of this run of the loop, not of the process.
                    let mut cycle: u64 = 0;

                    // Take the optional config receiver so we can listen to hot-reloads.
                    // Cloned per attempt from the same source: a watch receiver clone
                    // carries the position its source had.
                    let mut config_rx = this.config_rx.clone();

        loop {
            beat.beat();
            let config_tick = async {
                match &mut config_rx {
                    Some(rx) => {
                        if rx.changed().await.is_err() {
                            // Sender dropped — prevent busy-loop by pending forever.
                            std::future::pending::<()>().await;
                        }
                    }
                    None => {
                        std::future::pending::<()>().await;
                    }
                }
            };
            tokio::select! {
                _ = health.tick() => {
                    cycle = cycle.saturating_add(1);
                    let _ = this.event_tx.send(SupervisorEvent::Tick { timestamp: Utc::now(), cycle });
                    let ok = match this.run_health_pass().await {
                        Ok(report) => {
                            if !report.is_clean() {
                                warn!(
                                    "Supervisor health pass: suspect={}, dead={}, stuck={}",
                                    report.suspect.len(),
                                    report.dead.len(),
                                    report.stuck.len(),
                                );
                            } else {
                                debug!("Supervisor health pass clean ({} agents)", report.healthy.len());
                            }
                            true
                        }
                        Err(e) => {
                            warn!("Supervisor health pass failed: {}", e);
                            false
                        }
                    };
                    this.record_pass(SupervisorPass::Health, ok).await;
                }
                _ = quota.tick() => {
                    let ok = match this.run_quota_pass().await {
                        Ok(()) => true,
                        Err(e) => {
                            warn!("Supervisor quota pass failed: {}", e);
                            false
                        }
                    };
                    this.record_pass(SupervisorPass::Quota, ok).await;
                }
                _ = rebalance.tick() => {
                    let ok = match this.run_rebalance_pass().await {
                        Ok(()) => {
                            *this.last_rebalance.lock().await = Some(Utc::now());
                            true
                        }
                        Err(e) => {
                            warn!("Supervisor rebalance pass failed: {}", e);
                            false
                        }
                    };
                    this.record_pass(SupervisorPass::Rebalance, ok).await;
                }
                _ = events.tick() => {
                    let ok = match this.run_event_pass().await {
                        Ok(()) => true,
                        Err(e) => {
                            warn!("Supervisor event pass failed: {}", e);
                            false
                        }
                    };
                    this.record_pass(SupervisorPass::Event, ok).await;
                }
                _ = autonomous_tick.tick() => {
                    this.autonomous.run_decision_pass().await;
                    // The decision pass reports no error by construction: a
                    // failure inside it is logged there and a panic ends the
                    // loop attempt, which the liveness family counts.
                    this.record_pass(SupervisorPass::Autonomous, true).await;
                }
                _ = control_plane_tick.tick() => {
                    if let Some(ref client) = this.control_plane {
                        let report = match this.run_health_pass().await {
                            Ok(r) => r,
                            Err(e) => {
                                warn!("Control plane tick: health pass failed: {}", e);
                                crate::health_checker::HealthReport::default()
                            }
                        };
                        let pending_handoffs = this.autonomous.pending_count().await;
                        let last_rebalance = *this.last_rebalance.lock().await;
                        let status = SupervisorStatus {
                            cycle,
                            healthy_agents: report.healthy.len(),
                            dead_agents: report.dead.len(),
                            pending_handoffs,
                            last_rebalance,
                            timestamp: Utc::now(),
                        };
                        let ok = match client.report_status(status).await {
                            Ok(()) => true,
                            Err(e) => {
                                warn!("Control plane report failed: {}", e);
                                false
                            }
                        };
                        this.record_pass(SupervisorPass::ControlPlane, ok).await;
                    } else {
                        // Nothing to report to, so the round is skipped rather
                        // than left unwritten: the failing rule selects
                        // `pass="control_plane", outcome="failed"`, and an arm
                        // that writes no cell at all keeps that rule silent for
                        // the same reason a healthy arm does. An operator who
                        // lost a control plane URL in an upgrade would read the
                        // silence as health.
                        this.record_pass_outcome(
                            SupervisorPass::ControlPlane,
                            crate::OUTCOME_SKIPPED,
                        )
                        .await;
                    }
                }
                _ = config_tick => {
                    if let Some(ref mut rx) = config_rx {
                        let new_config = rx.borrow_and_update().clone();
                        health = tokio::time::interval(new_config.health_interval);
                        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        quota = tokio::time::interval(new_config.quota_interval);
                        quota.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        rebalance = tokio::time::interval(new_config.rebalance_interval);
                        rebalance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        events = tokio::time::interval(new_config.event_window);
                        events.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        autonomous_tick = tokio::time::interval(Duration::from_secs(new_config.autonomous.decision_interval_secs));
                        autonomous_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        control_plane_tick = tokio::time::interval(new_config.control_plane_interval);
                        control_plane_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        *this.config.write().unwrap_or_else(|e| e.into_inner()) = new_config.clone();
                        info!("Supervisor config reloaded (intervals: health={:?}, quota={:?}, rebalance={:?}, event={:?})",
                            new_config.health_interval,
                            new_config.quota_interval,
                            new_config.rebalance_interval,
                            new_config.event_window,
                        );
                    }
                }
                _ = supervisor_stop.wait() => {
                    // No need to trigger anything here: the death criterion the
                    // supervisor holds is this very signal, it has fired, and an
                    // exit now is not counted as a death.
                    info!("Supervisor shutdown signal received");
                    break;
                }
            }
        }
                }
            },
        )
        .await;
    }
}

#[async_trait::async_trait]
impl cog_core::Supervisor for Supervisor {
    async fn run_health_pass(&self) -> cog_core::SFResult<cog_core::HealthReport> {
        let report = Supervisor::run_health_pass(self)
            .await
            .map_err(|e| cog_core::SFError::IO(e.to_string()))?;
        Ok(cog_core::HealthReport {
            healthy: report.healthy,
            dead: report.dead.into_iter().map(|(id, _)| id).collect(),
            suspect: report.suspect.into_iter().map(|(id, _)| id).collect(),
            stuck: report.stuck.into_iter().map(|(id, _)| id).collect(),
        })
    }

    fn gate(&self) -> Arc<dyn cog_core::SchedulerGate> {
        self.gate.clone()
    }

    async fn autonomous_pending_count(&self) -> usize {
        self.autonomous.pending_count().await
    }

    async fn autonomous_retry_count(&self) -> usize {
        self.autonomous.retry_count().await
    }

    async fn orchestrator_dlq_len(&self) -> cog_core::SFResult<usize> {
        self.orchestrator
            .dlq_len()
            .await
            .map_err(|e| cog_core::SFError::IO(e.to_string()))
    }

    fn control_plane_url(&self) -> Option<String> {
        self.config
            .read()
            .ok()
            .and_then(|c| c.control_plane_url.clone())
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::SupervisorEvent> {
        self.event_tx.subscribe()
    }

    fn event_sender(&self) -> tokio::sync::broadcast::Sender<cog_core::SupervisorEvent> {
        self.event_tx.clone()
    }

    async fn kill_agent(&self, agent_id: &str, reason: &str) -> cog_core::SFResult<bool> {
        let found = self.registry.get_agent(agent_id).is_some();
        if !found {
            return Ok(false);
        }
        // Push into the agent's command stream. A push failure (the agent is in
        // the registry but not subscribed to the command stream) is a real
        // error, not `false`: the caller named a known agent and the command
        // did not reach it, and reporting a dispatch that never happened is the
        // exact bug this replaces.
        let dispatched = self
            .lifecycle_server
            .as_ref()
            .ok_or_else(|| {
                cog_core::SFError::Config(
                    "no AgentLifecycleServer published: cannot dispatch kill".into(),
                )
            })?
            .kill(agent_id, reason)
            .await?;
        let _ = self.event_tx.send(cog_core::SupervisorEvent::AgentKilled {
            agent_id: agent_id.into(),
            reason: reason.into(),
            timestamp: chrono::Utc::now(),
        });
        Ok(dispatched)
    }

    async fn restart_agent(
        &self,
        agent_id: &str,
        preserve_context: bool,
    ) -> cog_core::SFResult<bool> {
        let found = self.registry.get_agent(agent_id).is_some();
        if !found {
            return Ok(false);
        }
        let dispatched = self
            .lifecycle_server
            .as_ref()
            .ok_or_else(|| {
                cog_core::SFError::Config(
                    "no AgentLifecycleServer published: cannot dispatch restart".into(),
                )
            })?
            .restart(agent_id, preserve_context)
            .await?;
        let _ = self
            .event_tx
            .send(cog_core::SupervisorEvent::AgentRestarted {
                agent_id: agent_id.into(),
                preserve_context,
                timestamp: chrono::Utc::now(),
            });
        Ok(dispatched)
    }

    async fn checkpoint_agent(&self, agent_id: &str, task_id: &str) -> cog_core::SFResult<String> {
        let found = self.registry.get_agent(agent_id).is_some();
        if !found {
            return Err(cog_core::SFError::Agent(format!(
                "agent {} not found in registry",
                agent_id
            )));
        }
        // The checkpoint id now comes from the dispatch path rather than from a
        // locally minted UUID that no agent ever received.
        let checkpoint_id = self
            .lifecycle_server
            .as_ref()
            .ok_or_else(|| {
                cog_core::SFError::Config(
                    "no AgentLifecycleServer published: cannot dispatch checkpoint".into(),
                )
            })?
            .checkpoint(agent_id, task_id)
            .await?;
        let _ = self
            .event_tx
            .send(cog_core::SupervisorEvent::CheckpointRequested {
                agent_id: agent_id.into(),
                task_id: task_id.into(),
                checkpoint_id: checkpoint_id.clone(),
                timestamp: chrono::Utc::now(),
            });
        Ok(checkpoint_id)
    }
}

#[cfg(test)]
mod announceable_tests {
    //! 「离队」不进事件流：一条已经不在舰队名单上的判决不许报成「这个 agent 不健康」。
    //! 门的另一半在 `cogneva/tests/supervisor_health_checker_test.rs`（谁进 `departed`）。

    use super::*;
    use cog_core::HealthIssue;

    fn dead(id: &str) -> (String, HealthIssue) {
        (
            id.to_string(),
            HealthIssue::Dead {
                last_seen: Utc::now(),
            },
        )
    }

    #[test]
    fn a_departure_is_not_announced() {
        let mut report = crate::HealthReport::default();
        report.dead.push(dead("squad:one-shot-planner"));
        report.departed.push("squad:one-shot-planner".into());
        assert_eq!(
            announceable(&report).count(),
            0,
            "注销过的一次性角色已经走了，不是掉线的常驻 agent"
        );
    }

    #[test]
    fn a_verdict_about_a_listed_agent_is_announced() {
        let mut report = crate::HealthReport::default();
        report.dead.push(dead("worker"));
        assert_eq!(announceable(&report).count(), 1);
    }

    #[test]
    fn a_departure_does_not_swallow_the_rest_of_the_pass() {
        let mut report = crate::HealthReport::default();
        report.dead.push(dead("squad:one-shot-planner"));
        report
            .suspect
            .push(("worker".into(), HealthIssue::Suspect { missed_beats: 3 }));
        report.departed.push("squad:one-shot-planner".into());
        let announced: Vec<&str> = announceable(&report).map(|(id, _)| id.as_str()).collect();
        assert_eq!(announced, vec!["worker"]);
    }

    /// 同一件事，问的是真正被读走的那一头：告警桥把事件变成行，所以「离队不报」
    /// 必须落在事件流上，不能只落在一个没人读的字段里。
    #[test]
    fn a_departure_never_reaches_the_event_stream() {
        let (tx, mut rx) = broadcast::channel::<SupervisorEvent>(16);
        let mut report = crate::HealthReport::default();
        report.dead.push(dead("squad:one-shot-planner"));
        report.dead.push(dead("resident"));
        report.departed.push("squad:one-shot-planner".into());

        announce_verdicts(&report, &tx);

        let mut announced = Vec::new();
        while let Ok(SupervisorEvent::AgentUnhealthy { agent_id, .. }) = rx.try_recv() {
            announced.push(agent_id);
        }
        assert_eq!(announced, vec!["resident".to_string()]);
    }
}
