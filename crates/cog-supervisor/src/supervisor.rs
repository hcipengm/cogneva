use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::{AgentEvent, ObservabilityGateway, OrchestratorControl, StateBackend};
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

use crate::autonomous::{AutonomousCollaborator, AutonomousConfig};
use crate::control_plane::{ControlPlaneClient, HttpControlPlaneClient, SupervisorStatus};
use crate::error::SupervisorResult;
use crate::event_aggregator::EventAggregator;
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

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            health_interval: Duration::from_secs(10),
            quota_interval: Duration::from_secs(60),
            rebalance_interval: Duration::from_secs(300),
            event_window: Duration::from_secs(30),
            broadcast_capacity: 256,
            health_checker: HealthCheckerConfig::default(),
            task_rebalancer: TaskRebalancerConfig::default(),
            autonomous: AutonomousConfig::default(),
            quota_threshold: 1_000,
            control_plane_interval: Duration::from_secs(30),
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
/// and scheduler gate.  It exposes a single [`run`] entry point that
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
    pub aggregator: EventAggregator,
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
    /// Optional watch receiver for dynamic config reload.
    config_rx: Option<watch::Receiver<SupervisorConfig>>,
}

impl Supervisor {
    /// Build a Supervisor from production-grade dependencies.
    /// Components like the [`QuotaManager`] are wrapped behind their
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
        agent_event_rx: broadcast::Receiver<AgentEvent>,
        meta_learning: Option<Arc<dyn cog_core::MetaLearning>>,
    ) -> Self {
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
        let aggregator = EventAggregator::new(agent_event_rx).with_gateway(gateway.clone());

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
            LifecycleCoordinator::new(registry.clone(), state_backend, event_tx.clone());

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
            config_rx: None,
        }
    }

    /// Attach a [`watch::Receiver`] so the Supervisor can react to config
    /// reloads at runtime.  Call this before [`run`].
    pub fn with_config_watch(mut self, rx: watch::Receiver<SupervisorConfig>) -> Self {
        self.config_rx = Some(rx);
        self
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
    /// unhealthy agents.  Returns the underlying [`HealthReport`].
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
        for (agent_id, issue) in report
            .suspect
            .iter()
            .chain(report.dead.iter())
            .chain(report.stuck.iter())
        {
            let event = SupervisorEvent::AgentUnhealthy {
                agent_id: agent_id.clone(),
                issue: issue.clone(),
                timestamp: Utc::now(),
            };
            let _ = self.event_tx.send(event);
        }

        if !report.dead.is_empty() {
            let dead_ids: Vec<String> = report.dead.iter().map(|(id, _)| id.clone()).collect();
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

        Ok(report)
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

    /// Drive a single event-aggregation pass.
    pub async fn run_event_pass(&self) -> SupervisorResult<()> {
        let stats = self.aggregator.drain(1024).await?;
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

        let mut cycle: u64 = 0;
        let shutdown = std::pin::pin!(shutdown);
        let mut shutdown = shutdown;

        // Take the optional config receiver so we can listen to hot-reloads.
        let mut config_rx = self.config_rx.clone();

        // Spawn autonomous event loop
        let _autonomous_handle = tokio::spawn({
            let collaborator = Arc::clone(&self.autonomous);
            let rx = self.agent_event_rx.resubscribe();
            async move {
                crate::autonomous::run_autonomous_event_loop(collaborator, rx).await;
            }
        });

        info!(
            "Supervisor starting (intervals: health={:?}, quota={:?}, rebalance={:?}, event={:?})",
            cfg.health_interval, cfg.quota_interval, cfg.rebalance_interval, cfg.event_window,
        );

        loop {
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
                    let _ = self.event_tx.send(SupervisorEvent::Tick { timestamp: Utc::now(), cycle });
                    match self.run_health_pass().await {
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
                        }
                        Err(e) => warn!("Supervisor health pass failed: {}", e),
                    }
                }
                _ = quota.tick() => {
                    if let Err(e) = self.run_quota_pass().await {
                        warn!("Supervisor quota pass failed: {}", e);
                    }
                }
                _ = rebalance.tick() => {
                    if let Err(e) = self.run_rebalance_pass().await {
                        warn!("Supervisor rebalance pass failed: {}", e);
                    } else {
                        *self.last_rebalance.lock().await = Some(Utc::now());
                    }
                }
                _ = events.tick() => {
                    if let Err(e) = self.run_event_pass().await {
                        warn!("Supervisor event pass failed: {}", e);
                    }
                }
                _ = autonomous_tick.tick() => {
                    self.autonomous.run_decision_pass().await;
                }
                _ = control_plane_tick.tick() => {
                    if let Some(ref client) = self.control_plane {
                        let report = match self.run_health_pass().await {
                            Ok(r) => r,
                            Err(e) => {
                                warn!("Control plane tick: health pass failed: {}", e);
                                crate::health_checker::HealthReport::default()
                            }
                        };
                        let pending_handoffs = self.autonomous.pending_count().await;
                        let last_rebalance = *self.last_rebalance.lock().await;
                        let status = SupervisorStatus {
                            cycle,
                            healthy_agents: report.healthy.len(),
                            dead_agents: report.dead.len(),
                            pending_handoffs,
                            last_rebalance,
                            timestamp: Utc::now(),
                        };
                        if let Err(e) = client.report_status(status).await {
                            warn!("Control plane report failed: {}", e);
                        }
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
                        *self.config.write().unwrap_or_else(|e| e.into_inner()) = new_config.clone();
                        info!("Supervisor config reloaded (intervals: health={:?}, quota={:?}, rebalance={:?}, event={:?})",
                            new_config.health_interval,
                            new_config.quota_interval,
                            new_config.rebalance_interval,
                            new_config.event_window,
                        );
                    }
                }
                _ = &mut shutdown => {
                    info!("Supervisor shutdown signal received");
                    break;
                }
            }
        }
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
        let _ = self.event_tx.send(cog_core::SupervisorEvent::AgentKilled {
            agent_id: agent_id.into(),
            reason: reason.into(),
            timestamp: chrono::Utc::now(),
        });
        Ok(true)
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
        let _ = self
            .event_tx
            .send(cog_core::SupervisorEvent::AgentRestarted {
                agent_id: agent_id.into(),
                preserve_context,
                timestamp: chrono::Utc::now(),
            });
        Ok(true)
    }

    async fn checkpoint_agent(&self, agent_id: &str, task_id: &str) -> cog_core::SFResult<String> {
        let found = self.registry.get_agent(agent_id).is_some();
        if !found {
            return Err(cog_core::SFError::Agent(format!(
                "agent {} not found in registry",
                agent_id
            )));
        }
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
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
