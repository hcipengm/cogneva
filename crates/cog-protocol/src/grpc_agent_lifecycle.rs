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
            task_id: req.task_id,
            checkpoint_id: checkpoint_id.clone(),
            timestamp: chrono::Utc::now(),
        });
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
        let state = match info {
            Some(agent) => format!("registered (last_heartbeat: {:?})", agent.last_heartbeat),
            None => "unknown".into(),
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
}

impl GrpcAgentLifecycleServer {
    pub fn new(router: AgentCommandRouter) -> Self {
        Self { router }
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

    async fn report_event(&self, _agent_id: &str, _event: &AgentEvent) -> SFResult<()> {
        // Server-side `report_event` is consumed by Supervisor via the gRPC handler
        // above; this trait method is a no-op because the actual event processing
        // happens in the tonic server which has access to the broadcast channel.
        Ok(())
    }

    async fn upload_events(&self, _agent_id: &str, _events: Vec<AgentEvent>) -> SFResult<u32> {
        // Same as report_event — the tonic handler processes the stream directly.
        Ok(0)
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
