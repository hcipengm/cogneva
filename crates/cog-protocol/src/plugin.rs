//! Protocol plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// Protocol plugin that provides MCP, A2A, and gRPC lifecycle services.
pub struct ProtocolPlugin {
    mcp_client: Option<Arc<dyn cog_core::McpClient>>,
    a2a_client: Option<Arc<crate::A2aClient>>,
    /// The lifecycle server published at init. `start` keeps the handle so it
    /// can hand the server the process's event channel, which is published by
    /// the supervisor plugin after this one's init has already run.
    grpc_server: Option<Arc<crate::grpc_agent_lifecycle::GrpcAgentLifecycleServer>>,
}

impl ProtocolPlugin {
    /// Create a plugin that will build clients during `init`.
    pub fn new() -> Self {
        Self {
            mcp_client: None,
            a2a_client: None,
            grpc_server: None,
        }
    }

    /// Create a plugin that wraps existing clients.
    pub fn from_clients(
        mcp_client: Option<Arc<dyn cog_core::McpClient>>,
        a2a_client: Arc<crate::A2aClient>,
    ) -> Self {
        Self {
            mcp_client,
            a2a_client: Some(a2a_client),
            grpc_server: None,
        }
    }
}

impl Default for ProtocolPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for ProtocolPlugin {
    fn name(&self) -> &'static str {
        "protocol"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // ── MCP client ──
        if self.mcp_client.is_none() {
            if let Ok(ref mcp_endpoint) = std::env::var("COGNEVA_MCP_ENDPOINT") {
                // Built here rather than at the first call: the client comes from
                // the `http_client` config section, and an endpoint or a config
                // that cannot be used is a startup finding, not something to
                // discover as a failed tool call much later.
                let transport = Box::new(crate::SseTransport::new(mcp_endpoint)?);
                let client: Arc<dyn cog_core::McpClient> =
                    Arc::new(crate::McpClient::new(transport));
                self.mcp_client = Some(client.clone());
                ctx.publish_service(client);
                info!("ProtocolPlugin MCP client initialized: {}", mcp_endpoint);
            } else {
                info!("ProtocolPlugin MCP client not configured");
            }
        }

        // ── A2A client ──
        if self.a2a_client.is_none() {
            let a2a = Arc::new(crate::A2aClient::new());
            self.a2a_client = Some(a2a.clone());
            ctx.publish(a2a);
            info!("ProtocolPlugin A2A client initialized");
        }

        // ── gRPC lifecycle server handle (command router) ──
        let router = crate::grpc_agent_lifecycle::AgentCommandRouter::new();
        let server = Arc::new(crate::grpc_agent_lifecycle::GrpcAgentLifecycleServer::new(
            router,
        ));
        ctx.publish_service(server.clone() as Arc<dyn cog_core::AgentLifecycleServer>);
        self.grpc_server = Some(server);
        info!("ProtocolPlugin AgentLifecycleServer published");

        // ── gRPC lifecycle client ──
        if let Ok(addr) = std::env::var("COGNEVA_SUPERVISOR_GRPC_ADDR") {
            match crate::grpc_agent_lifecycle::GrpcAgentLifecycleClient::connect(addr.clone()).await
            {
                Ok(client) => {
                    let client_arc: Arc<dyn cog_core::AgentLifecycleClient> = Arc::new(client);
                    ctx.publish_service(client_arc);
                    info!("ProtocolPlugin gRPC client connected to {}", addr);
                }
                Err(e) => {
                    warn!(
                        "ProtocolPlugin gRPC client connection to {} failed: {}",
                        addr, e
                    );
                }
            }
        }

        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // Start gRPC server only when running in a supervisor context
        // (AgentRegistry + SupervisorEvent sender are available).
        if let Some(server) = self.grpc_server.clone() {
            let router = server.router();
            if let Some(agent_registry) = ctx.consume_service::<dyn cog_core::AgentRegistry>() {
                if let Some(event_tx) =
                    ctx.consume::<tokio::sync::broadcast::Sender<cog_core::SupervisorEvent>>()
                {
                    // The trait face reports events onto the same channel the
                    // RPC face does. Attached here and not at construction
                    // because the sender is published by the supervisor plugin,
                    // which this one cannot wait for during init.
                    server.attach_events((*event_tx).clone());
                    let bind_addr = std::env::var("COGNEVA_GRPC_BIND_ADDR")
                        .unwrap_or_else(|_| "0.0.0.0:50051".into());
                    let socket_addr: std::net::SocketAddr = bind_addr.parse().map_err(|e| {
                        cog_core::SFError::Config(format!(
                            "invalid COGNEVA_GRPC_BIND_ADDR '{}': {}",
                            bind_addr, e
                        ))
                    })?;

                    let handler = crate::grpc_agent_lifecycle::AgentLifecycleGrpcHandler::new(
                        agent_registry,
                        (*event_tx).clone(),
                        router,
                    );
                    let server =
                        crate::agent_lifecycle::agent_lifecycle_server::AgentLifecycleServer::new(
                            handler,
                        );

                    let handle = tokio::spawn(async move {
                        if let Err(e) = tonic::transport::Server::builder()
                            .add_service(server)
                            .serve(socket_addr)
                            .await
                        {
                            warn!("gRPC server exited with error: {}", e);
                        }
                    });

                    // We cannot store handle because `start` takes `&self`.
                    // Drop it into a background task; the OS will clean up on process exit.
                    tokio::spawn(async move {
                        let _ = handle.await;
                    });

                    info!("ProtocolPlugin gRPC server started on {}", bind_addr);
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("ProtocolPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "protocol",
    requires: &[],
    optional_requires: &[],
    factory: || Box::new(ProtocolPlugin::new()),
};
