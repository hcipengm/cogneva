//! gRPC control-plane implementation for AgentLifecycle.
//! - `AgentLifecycleGrpcHandler` implements the tonic-generated server trait.
//! - `GrpcAgentLifecycleServer` implements `cog_core::AgentLifecycleServer`.
//! - `GrpcAgentLifecycleClient` implements `cog_core::AgentLifecycleClient`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use cog_core::{
    AgentCommand, AgentEvent, AgentLifecycleClient, AgentLifecycleServer, AgentRegistry, SFError,
    SFResult, SupervisorEvent,
};

use crate::agent_lifecycle::agent_lifecycle_client::AgentLifecycleClient as TonicClient;
use crate::agent_lifecycle::agent_lifecycle_server::AgentLifecycle;
use crate::agent_lifecycle::{
    CheckpointCmd, CheckpointReq, CheckpointResp, Command, ConfigUpdate, HeartbeatReq,
    HeartbeatResp, KillCmd, KillReq, KillResp, QueryStateReq, QueryStateResp, ReportEventReq,
    ReportEventResp, RestartCmd, RestartReq, RestartResp, SubscribeCommandsReq, UploadEventsReq,
    UploadEventsResp,
};

// ─── Shared command router ───

#[derive(Clone)]
pub struct AgentCommandRouter {
    senders: Arc<Mutex<HashMap<String, mpsc::Sender<Command>>>>,
}

impl Default for AgentCommandRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentCommandRouter {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, agent_id: String, sender: mpsc::Sender<Command>) {
        self.senders.lock().unwrap().insert(agent_id, sender);
    }

    pub fn unregister(&self, agent_id: &str) {
        self.senders.lock().unwrap().remove(agent_id);
    }

    pub fn push(&self, agent_id: &str, command: Command) -> SFResult<()> {
        let senders = self.senders.lock().unwrap();
        let sender = senders
            .get(agent_id)
            .ok_or_else(|| SFError::Agent(format!("agent {} not connected", agent_id)))?;
        sender
            .try_send(command)
            .map_err(|_| SFError::Agent("agent command channel full".into()))?;
        Ok(())
    }

    pub fn connected_agents(&self) -> Vec<String> {
        self.senders.lock().unwrap().keys().cloned().collect()
    }
}

// ─── Tonic server stream wrapper ───

pub struct CommandStream {
    rx: mpsc::Receiver<Command>,
    agent_id: String,
    router: AgentCommandRouter,
}

impl CommandStream {
    fn new(rx: mpsc::Receiver<Command>, agent_id: String, router: AgentCommandRouter) -> Self {
        Self {
            rx,
            agent_id,
            router,
        }
    }
}

impl Stream for CommandStream {
    type Item = Result<Command, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx).map(|opt| opt.map(Ok))
    }
}

impl Drop for CommandStream {
    fn drop(&mut self) {
        self.router.unregister(&self.agent_id);
    }
}

// ─── Tonic server handler ───

pub struct AgentLifecycleGrpcHandler {
    agent_registry: Arc<dyn AgentRegistry>,
    event_tx: broadcast::Sender<SupervisorEvent>,
    router: AgentCommandRouter,
    /// The state each agent last reported in its heartbeat, by agent id.
    ///
    /// It lives here rather than in the registry because the registry's
    /// contract has no state column: an agent sends its state on every
    /// heartbeat and the field had nowhere to land, so the server took the
    /// agent id and dropped the rest. This is the one place on that path that
    /// can hold it without a cross-crate contract change.
    ///
    /// The trade is that the answer is "what this process was told" rather than
    /// a durable fact: after a restart it reads unknown until the next
    /// heartbeat, which is exactly what it knows.
    states: std::sync::Mutex<HashMap<String, String>>,
}

impl AgentLifecycleGrpcHandler {
    pub fn new(
        agent_registry: Arc<dyn AgentRegistry>,
        event_tx: broadcast::Sender<SupervisorEvent>,
        router: AgentCommandRouter,
    ) -> Self {
        Self {
            agent_registry,
            event_tx,
            router,
            states: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl AgentLifecycle for AgentLifecycleGrpcHandler {
    type SubscribeCommandsStream = CommandStream;

    async fn heartbeat(
        &self,
        request: Request<HeartbeatReq>,
    ) -> Result<Response<HeartbeatResp>, Status> {
        let req = request.into_inner();
        let _ = self.agent_registry.heartbeat(&req.agent_id).await;
        // The state is the only part of a heartbeat the registry cannot take,
        // and the only part that answers "what is it doing": record it here,
        // where the RPC that asks is answered. An empty state is not a state,
        // so it leaves the previous one standing rather than erasing it.
        if !req.state.is_empty() {
            self.states
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(req.agent_id.clone(), req.state.clone());
        }
        Ok(Response::new(HeartbeatResp {
            agent_id: req.agent_id,
            accepted: true,
        }))
    }

    async fn subscribe_commands(
        &self,
        request: Request<SubscribeCommandsReq>,
    ) -> Result<Response<Self::SubscribeCommandsStream>, Status> {
        let req = request.into_inner();
        let agent_id = req.agent_id.clone();
        let (tx, rx) = mpsc::channel::<Command>(128);
        self.router.register(agent_id.clone(), tx);
        info!("Agent {} subscribed to commands", req.agent_id);
        Ok(Response::new(CommandStream::new(
            rx,
            req.agent_id,
            self.router.clone(),
        )))
    }

    async fn kill(&self, request: Request<KillReq>) -> Result<Response<KillResp>, Status> {
        let req = request.into_inner();
        info!("gRPC Kill received for agent {}", req.agent_id);
        let found = self
            .agent_registry
            .get(&req.agent_id)
            .await
            .ok()
            .flatten()
            .is_some();
        if !found {
            warn!("gRPC Kill: agent {} not found in registry", req.agent_id);
            return Ok(Response::new(KillResp {
                agent_id: req.agent_id,
                success: false,
            }));
        }
        let _ = self.event_tx.send(SupervisorEvent::AgentKilled {
            agent_id: req.agent_id.clone(),
            reason: req.reason.clone(),
            timestamp: chrono::Utc::now(),
        });
        // Synchronously push the kill command to the agent's command stream.
        let cmd = Command {
            payload: Some(crate::agent_lifecycle::command::Payload::Kill(KillCmd {
                reason: req.reason,
            })),
        };
        if let Err(e) = self.router.push(&req.agent_id, cmd) {
            warn!(
                "gRPC Kill: failed to push command to agent {}: {}",
                req.agent_id, e
            );
            return Ok(Response::new(KillResp {
                agent_id: req.agent_id,
                success: false,
            }));
        }
        Ok(Response::new(KillResp {
            agent_id: req.agent_id,
            success: true,
        }))
    }

    async fn restart(&self, request: Request<RestartReq>) -> Result<Response<RestartResp>, Status> {
        let req = request.into_inner();
        info!("gRPC Restart received for agent {}", req.agent_id);
        let found = self
            .agent_registry
            .get(&req.agent_id)
            .await
            .ok()
            .flatten()
            .is_some();
        if !found {
            warn!("gRPC Restart: agent {} not found in registry", req.agent_id);
            return Ok(Response::new(RestartResp {
                agent_id: req.agent_id,
                success: false,
            }));
        }
        let _ = self.event_tx.send(SupervisorEvent::AgentRestarted {
            agent_id: req.agent_id.clone(),
            preserve_context: req.preserve_context,
            timestamp: chrono::Utc::now(),
        });
        // Synchronously push the restart command to the agent's command stream.
        let cmd = Command {
            payload: Some(crate::agent_lifecycle::command::Payload::Restart(
                RestartCmd {
                    preserve_context: req.preserve_context,
                },
            )),
        };
        if let Err(e) = self.router.push(&req.agent_id, cmd) {
            warn!(
                "gRPC Restart: failed to push command to agent {}: {}",
                req.agent_id, e
            );
            return Ok(Response::new(RestartResp {
                agent_id: req.agent_id,
                success: false,
            }));
        }
        Ok(Response::new(RestartResp {
            agent_id: req.agent_id,
            success: true,
        }))
    }

    async fn checkpoint(
        &self,
        request: Request<CheckpointReq>,
    ) -> Result<Response<CheckpointResp>, Status> {
        let req = request.into_inner();
        info!("gRPC Checkpoint received for agent {}", req.agent_id);
        let found = self
            .agent_registry
            .get(&req.agent_id)
            .await
            .ok()
            .flatten()
            .is_some();
        if !found {
            warn!(
                "gRPC Checkpoint: agent {} not found in registry",
                req.agent_id
            );
            return Ok(Response::new(CheckpointResp {
                agent_id: req.agent_id,
                checkpoint_id: String::new(),
                success: false,
            }));
        }
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let _ = self.event_tx.send(SupervisorEvent::CheckpointRequested {
            agent_id: req.agent_id.clone(),
            task_id: req.task_id.clone(),
            checkpoint_id: checkpoint_id.clone(),
            timestamp: chrono::Utc::now(),
        });
        // Push it like kill and restart do. The event alone never reaches the
        // agent: it goes to whoever subscribes to supervisor events, so a
        // remote caller used to get `success: true` and a checkpoint id while
        // the agent was never told to take one.
        let cmd = Command {
            payload: Some(crate::agent_lifecycle::command::Payload::Checkpoint(
                CheckpointCmd {
                    task_id: req.task_id,
                },
            )),
        };
        if let Err(e) = self.router.push(&req.agent_id, cmd) {
            warn!(
                "gRPC Checkpoint: failed to push command to agent {}: {}",
                req.agent_id, e
            );
            return Ok(Response::new(CheckpointResp {
                agent_id: req.agent_id,
                checkpoint_id: String::new(),
                success: false,
            }));
        }
        Ok(Response::new(CheckpointResp {
            agent_id: req.agent_id,
            checkpoint_id,
            success: true,
        }))
    }

    async fn query_state(
        &self,
        request: Request<QueryStateReq>,
    ) -> Result<Response<QueryStateResp>, Status> {
        let req = request.into_inner();
        let info = self.agent_registry.get(&req.agent_id).await.ok().flatten();
        // What the agent last said about itself answers the question better
        // than the fact that it is registered, so it is preferred. When there
        // is no reported state the registration stands in -- it is a weaker
        // answer than a state word, but it is not a wrong one.
        let reported = self
            .states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&req.agent_id)
            .cloned();
        let state = match (info, reported) {
            (_, Some(reported)) => reported,
            (Some(agent), None) => {
                format!("registered (last_heartbeat: {:?})", agent.last_heartbeat)
            }
            (None, None) => "unknown".into(),
        };
        Ok(Response::new(QueryStateResp {
            agent_id: req.agent_id,
            state,
            iteration: 0,
        }))
    }

    async fn report_event(
        &self,
        request: Request<ReportEventReq>,
    ) -> Result<Response<ReportEventResp>, Status> {
        let req = request.into_inner();
        let event: AgentEvent = serde_json::from_str(&req.event_json)
            .map_err(|e| Status::invalid_argument(format!("invalid event_json: {}", e)))?;
        let _ = self.event_tx.send(SupervisorEvent::AgentEventReported {
            agent_id: req.agent_id.clone(),
            event: event.clone(),
            timestamp: chrono::Utc::now(),
        });
        info!("gRPC ReportEvent from agent {}: {:?}", req.agent_id, event);
        Ok(Response::new(ReportEventResp {
            agent_id: req.agent_id,
            accepted: true,
        }))
    }

    async fn upload_events(
        &self,
        request: Request<Streaming<UploadEventsReq>>,
    ) -> Result<Response<UploadEventsResp>, Status> {
        let mut stream = request.into_inner();
        let mut accepted_count = 0i32;
        let mut agent_id = String::new();
        while let Some(req) = stream.message().await? {
            agent_id = req.agent_id.clone();
            if let Ok(event) = serde_json::from_str::<AgentEvent>(&req.event_json) {
                let _ = self.event_tx.send(SupervisorEvent::AgentEventReported {
                    agent_id: req.agent_id.clone(),
                    event,
                    timestamp: chrono::Utc::now(),
                });
                accepted_count += 1;
            } else {
                warn!(
                    "gRPC UploadEvents: invalid event_json from agent {}",
                    req.agent_id
                );
            }
        }
        info!(
            "gRPC UploadEvents from agent {}: {} events accepted",
            agent_id, accepted_count
        );
        Ok(Response::new(UploadEventsResp {
            agent_id,
            accepted_count,
        }))
    }
}

// ─── cog_core::AgentLifecycleServer implementation ───

pub struct GrpcAgentLifecycleServer {
    router: AgentCommandRouter,
    /// The process's `SupervisorEvent` broadcast, attached once there is one.
    ///
    /// Empty until then, because this server is constructed and published
    /// during the protocol plugin's `init` while the sender is published by the
    /// supervisor plugin, whose descriptor requires this one -- so the two ends
    /// of the same process meet after construction, not at it.
    events: std::sync::RwLock<Option<broadcast::Sender<SupervisorEvent>>>,
}

impl GrpcAgentLifecycleServer {
    pub fn new(router: AgentCommandRouter) -> Self {
        Self {
            router,
            events: std::sync::RwLock::new(None),
        }
    }

    /// Attach the process's `SupervisorEvent` broadcast.
    pub fn attach_events(&self, tx: broadcast::Sender<SupervisorEvent>) {
        *self.events.write().unwrap_or_else(|e| e.into_inner()) = Some(tx);
    }

    /// The router this server dispatches through, so the process that built
    /// both does not have to keep a second copy of it.
    pub fn router(&self) -> AgentCommandRouter {
        self.router.clone()
    }

    /// Put reported events on the broadcast, or say why they cannot go.
    ///
    /// A trait that answers `Ok` with nothing behind it reads to the caller as
    /// accepted events, and this one answered `Ok(0)` -- a count of events
    /// accepted that had been dropped on the floor. Failing instead is the only
    /// answer that cannot be mistaken for delivery.
    fn report(&self, agent_id: &str, events: Vec<AgentEvent>) -> SFResult<u32> {
        let guard = self.events.read().unwrap_or_else(|e| e.into_inner());
        let Some(tx) = guard.as_ref() else {
            return Err(SFError::Agent(format!(
                "no SupervisorEvent channel in this process, so events from {agent_id} would reach nobody"
            )));
        };
        let accepted = events.len() as u32;
        for event in events {
            let _ = tx.send(SupervisorEvent::AgentEventReported {
                agent_id: agent_id.to_string(),
                event,
                timestamp: chrono::Utc::now(),
            });
        }
        Ok(accepted)
    }
}

#[async_trait]
impl AgentLifecycleServer for GrpcAgentLifecycleServer {
    async fn push_command(&self, agent_id: &str, command: AgentCommand) -> SFResult<()> {
        let proto_cmd = match command {
            AgentCommand::Kill { reason } => Command {
                payload: Some(crate::agent_lifecycle::command::Payload::Kill(KillCmd {
                    reason,
                })),
            },
            AgentCommand::Restart { preserve_context } => Command {
                payload: Some(crate::agent_lifecycle::command::Payload::Restart(
                    RestartCmd { preserve_context },
                )),
            },
            AgentCommand::Checkpoint { task_id } => Command {
                payload: Some(crate::agent_lifecycle::command::Payload::Checkpoint(
                    CheckpointCmd { task_id },
                )),
            },
            AgentCommand::ConfigUpdate { config_json } => Command {
                payload: Some(crate::agent_lifecycle::command::Payload::Config(
                    ConfigUpdate { config_json },
                )),
            },
        };
        self.router.push(agent_id, proto_cmd)
    }

    async fn connected_agents(&self) -> SFResult<Vec<String>> {
        Ok(self.router.connected_agents())
    }

    async fn report_event(&self, agent_id: &str, event: &AgentEvent) -> SFResult<()> {
        // The tonic handler has a caller per RPC; this face has whatever holds
        // the trait object, and it goes onto the same broadcast, so both faces
        // of one process report the same way.
        self.report(agent_id, vec![event.clone()]).map(|_| ())
    }

    async fn upload_events(&self, agent_id: &str, events: Vec<AgentEvent>) -> SFResult<u32> {
        self.report(agent_id, events)
    }
}

// ─── cog_core::AgentLifecycleClient implementation ───

pub struct GrpcAgentLifecycleClient {
    inner: tokio::sync::Mutex<TonicClient<tonic::transport::Channel>>,
}

impl GrpcAgentLifecycleClient {
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let client = TonicClient::connect(dst).await?;
        Ok(Self {
            inner: tokio::sync::Mutex::new(client),
        })
    }
}

#[async_trait]
impl AgentLifecycleClient for GrpcAgentLifecycleClient {
    async fn heartbeat(&self, agent_id: &str, state: &str) -> SFResult<()> {
        let req = HeartbeatReq {
            agent_id: agent_id.into(),
            timestamp: chrono::Utc::now().timestamp(),
            state: state.into(),
        };
        let mut client = self.inner.lock().await;
        client
            .heartbeat(req)
            .await
            .map_err(|e| SFError::Agent(e.to_string()))?;
        Ok(())
    }

    async fn subscribe_commands(
        &self,
        agent_id: &str,
    ) -> SFResult<futures::stream::BoxStream<'static, AgentCommand>> {
        let req = SubscribeCommandsReq {
            agent_id: agent_id.into(),
        };
        let mut client = self.inner.lock().await;
        let response = client
            .subscribe_commands(req)
            .await
            .map_err(|e| SFError::Agent(e.to_string()))?;
        let stream = response.into_inner();
        let mapped = futures::stream::unfold(stream, |mut s| async move {
            match s.message().await {
                Ok(Some(cmd)) => {
                    let core_cmd = map_proto_command(cmd);
                    Some((core_cmd, s))
                }
                _ => None,
            }
        });
        Ok(mapped.boxed())
    }

    async fn report_event(&self, agent_id: &str, event: &AgentEvent) -> SFResult<()> {
        let event_json = serde_json::to_string(event)
            .map_err(|e| SFError::Agent(format!("serialize event failed: {}", e)))?;
        let req = ReportEventReq {
            agent_id: agent_id.into(),
            event_json,
        };
        let mut client = self.inner.lock().await;
        client
            .report_event(req)
            .await
            .map_err(|e| SFError::Agent(e.to_string()))?;
        Ok(())
    }

    async fn upload_events(&self, agent_id: &str, events: Vec<AgentEvent>) -> SFResult<u32> {
        let mut client = self.inner.lock().await;
        let proto_events: Vec<UploadEventsReq> = events
            .into_iter()
            .filter_map(|ev| {
                serde_json::to_string(&ev)
                    .ok()
                    .map(|event_json| UploadEventsReq {
                        agent_id: agent_id.into(),
                        event_json,
                    })
            })
            .collect();
        let stream = futures::stream::iter(proto_events);
        let resp = client
            .upload_events(stream)
            .await
            .map_err(|e| SFError::Agent(e.to_string()))?;
        Ok(resp.into_inner().accepted_count as u32)
    }
}

fn map_proto_command(cmd: Command) -> AgentCommand {
    use crate::agent_lifecycle::command::Payload;
    match cmd.payload {
        Some(Payload::Kill(k)) => AgentCommand::Kill { reason: k.reason },
        Some(Payload::Restart(r)) => AgentCommand::Restart {
            preserve_context: r.preserve_context,
        },
        Some(Payload::Checkpoint(c)) => AgentCommand::Checkpoint { task_id: c.task_id },
        Some(Payload::Config(c)) => AgentCommand::ConfigUpdate {
            config_json: c.config_json,
        },
        None => AgentCommand::Kill {
            reason: "unknown command".into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{AgentRegistration, ResourceInfo};

    /// Holds what it was given. The handlers answer "is this agent known"
    /// before they push anything, so a test of the push path needs a registry
    /// that can say yes.
    #[derive(Default)]
    struct StubRegistry {
        agents: Mutex<HashMap<String, AgentRegistration>>,
    }

    #[async_trait]
    impl AgentRegistry for StubRegistry {
        async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
            self.agents
                .lock()
                .unwrap()
                .insert(registration.agent_id.clone(), registration.clone());
            Ok(())
        }

        async fn heartbeat(&self, _agent_id: &str) -> SFResult<()> {
            Ok(())
        }

        async fn deregister(&self, agent_id: &str) -> SFResult<()> {
            self.agents.lock().unwrap().remove(agent_id);
            Ok(())
        }

        async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
            Ok(self.agents.lock().unwrap().get(agent_id).cloned())
        }

        async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
            Ok(self.agents.lock().unwrap().values().cloned().collect())
        }

        async fn list_by_role(&self, _role: &str) -> SFResult<Vec<AgentRegistration>> {
            Ok(Vec::new())
        }

        async fn list_by_capability(&self, _capability: &str) -> SFResult<Vec<AgentRegistration>> {
            Ok(Vec::new())
        }
    }

    fn registration(agent_id: &str) -> AgentRegistration {
        AgentRegistration::new(
            agent_id,
            "host-1",
            "10.0.0.1",
            "worker",
            "ws-1",
            Vec::new(),
            ResourceInfo::default(),
        )
    }

    fn event() -> AgentEvent {
        AgentEvent::AgentStart {
            agent_id: "a-1".into(),
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        }
    }

    async fn registered_handler() -> AgentLifecycleGrpcHandler {
        let registry = Arc::new(StubRegistry::default());
        registry.register(&registration("a-1")).await.unwrap();
        let (tx, _rx) = broadcast::channel(16);
        AgentLifecycleGrpcHandler::new(
            registry as Arc<dyn AgentRegistry>,
            tx,
            AgentCommandRouter::new(),
        )
    }

    /// The checkpoint used to answer `success: true` and an id while the only
    /// thing it sent was a supervisor event — the agent was never told to take
    /// one, and kill and restart next door both push.
    #[tokio::test]
    async fn a_checkpoint_reaches_the_agent_it_names() {
        let handler = registered_handler().await;
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(8);
        handler.router.register("a-1".into(), cmd_tx);

        let resp = handler
            .checkpoint(Request::new(CheckpointReq {
                agent_id: "a-1".into(),
                task_id: "t-9".into(),
            }))
            .await
            .expect("the call answers")
            .into_inner();

        assert!(resp.success);
        assert!(
            !resp.checkpoint_id.is_empty(),
            "the id it answers with is the one the caller tracks the checkpoint by"
        );
        match cmd_rx.try_recv().expect("the agent was told to checkpoint") {
            Command {
                payload: Some(crate::agent_lifecycle::command::Payload::Checkpoint(c)),
            } => assert_eq!(c.task_id, "t-9"),
            other => panic!("expected a checkpoint command, got {other:?}"),
        }
    }

    /// No subscription means no stream to push to. The caller has to be able to
    /// tell that apart from a checkpoint the agent actually took.
    #[tokio::test]
    async fn a_checkpoint_for_an_agent_with_no_command_stream_is_not_a_success() {
        let handler = registered_handler().await;

        let resp = handler
            .checkpoint(Request::new(CheckpointReq {
                agent_id: "a-1".into(),
                task_id: "t-9".into(),
            }))
            .await
            .expect("the call answers")
            .into_inner();

        assert!(!resp.success, "nobody was listening, so nothing was taken");
        assert!(resp.checkpoint_id.is_empty());
    }

    /// An agent the registry has never seen is not a checkpoint either.
    #[tokio::test]
    async fn a_checkpoint_for_an_unknown_agent_is_not_a_success() {
        let handler = registered_handler().await;

        let resp = handler
            .checkpoint(Request::new(CheckpointReq {
                agent_id: "nobody".into(),
                task_id: "t-9".into(),
            }))
            .await
            .expect("the call answers")
            .into_inner();

        assert!(!resp.success);
    }

    /// The trait face answered `Ok(())` and `Ok(0)` for both of these: events
    /// the caller believed were reported, and a count of events accepted, with
    /// nothing behind either. Refusing is the only answer that cannot be read
    /// as delivery.
    #[tokio::test]
    async fn events_reported_without_a_channel_are_refused_rather_than_accepted() {
        let server = GrpcAgentLifecycleServer::new(AgentCommandRouter::new());

        let single = server.report_event("a-1", &event()).await;
        assert!(single.is_err(), "no channel, so nothing was reported");

        let batch = server.upload_events("a-1", vec![event(), event()]).await;
        assert!(
            batch.is_err(),
            "no channel, so accepted_count is not a number"
        );
    }

    /// With the channel attached the same two calls deliver, on the same
    /// broadcast the RPC face uses, so a process that holds this trait object
    /// reports events the supervisor can see.
    #[tokio::test]
    async fn attached_events_reach_the_broadcast() {
        let (tx, mut rx) = broadcast::channel(16);
        let server = GrpcAgentLifecycleServer::new(AgentCommandRouter::new());
        server.attach_events(tx);

        server
            .report_event("a-1", &event())
            .await
            .expect("the channel is attached");
        let accepted = server
            .upload_events("a-1", vec![event(), event()])
            .await
            .expect("the channel is attached");
        assert_eq!(accepted, 2, "the count is what went onto the channel");

        let mut seen = 0;
        while let Ok(reported) = rx.try_recv() {
            match reported {
                SupervisorEvent::AgentEventReported { agent_id, .. } => {
                    assert_eq!(agent_id, "a-1");
                    seen += 1;
                }
                other => panic!("unexpected event {other:?}"),
            }
        }
        assert_eq!(seen, 3, "one reported singly and two in a batch");
    }

    /// A free port nothing is listening on: bind it, read what the OS chose,
    /// let it go.
    fn free_port() -> std::net::SocketAddr {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("the OS hands out a port");
        let addr = probe.local_addr().expect("a bound listener has an address");
        drop(probe);
        addr
    }

    /// The state an agent reports has to come back out of the query. It used to
    /// be read off the wire and dropped, so the field answered with a
    /// registration timestamp and the agent's own word -- the only part of a
    /// heartbeat that says what it is doing -- went nowhere.
    ///
    /// Driven over a real socket rather than by calling the two handlers: the
    /// claim is about what a client of this service gets, and the two calls
    /// travel in opposite directions (one in, one out) through the generated
    /// code either way.
    #[tokio::test]
    async fn the_state_an_agent_reports_is_the_state_the_query_answers() {
        let addr = free_port();
        let handler = {
            let registry = Arc::new(StubRegistry::default());
            registry.register(&registration("a-1")).await.unwrap();
            let (tx, _rx) = broadcast::channel(16);
            AgentLifecycleGrpcHandler::new(
                registry as Arc<dyn AgentRegistry>,
                tx,
                AgentCommandRouter::new(),
            )
        };
        let server =
            crate::agent_lifecycle::agent_lifecycle_server::AgentLifecycleServer::new(handler);
        let (_handle, _stop) = crate::plugin::spawn_grpc_server(server, addr);

        // The bind is inside the spawned task, so connecting once here would
        // race it: wait until something answers on the port before talking.
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let mut client =
            crate::agent_lifecycle::agent_lifecycle_client::AgentLifecycleClient::connect(format!(
                "http://{addr}"
            ))
            .await
            .expect("the server is listening on the port it was given");

        client
            .heartbeat(HeartbeatReq {
                agent_id: "a-1".into(),
                timestamp: chrono::Utc::now().timestamp(),
                state: "active".into(),
            })
            .await
            .expect("the heartbeat is accepted");

        let answered = client
            .query_state(QueryStateReq {
                agent_id: "a-1".into(),
            })
            .await
            .expect("the query answers")
            .into_inner();
        assert_eq!(
            answered.state, "active",
            "the query answers with what the agent said, not with a timestamp"
        );

        // An agent that never sent one is not given a state it never had.
        let stranger = client
            .query_state(QueryStateReq {
                agent_id: "nobody".into(),
            })
            .await
            .expect("the query answers")
            .into_inner();
        assert_eq!(stranger.state, "unknown");
    }
}
