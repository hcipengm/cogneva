//! Protocol plugin — implements [`cog_core::SystemPlugin`].

use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// Protocol plugin that provides MCP, A2A, and gRPC lifecycle services.
pub struct ProtocolPlugin {
    mcp_client: Option<Arc<dyn cog_core::McpClient>>,
    /// The lifecycle server published at init. `start` keeps the handle so it
    /// can hand the server the process's event channel, which is published by
    /// the supervisor plugin after this one's init has already run.
    grpc_server: Option<Arc<crate::grpc_agent_lifecycle::GrpcAgentLifecycleServer>>,
    /// The two halves of stopping the listener: the signal that asks it to
    /// stop, and the task that ends once it has.
    ///
    /// Held rather than dropped because `shutdown` takes `&self`: a handle
    /// dropped in `start` leaves the plugin with nothing to do but log, so the
    /// port stays open until the process exits and takes the in-flight calls
    /// with it -- which is what a peer sees as a truncated connection, not as
    /// a shutdown.
    grpc_shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    grpc_serve: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ProtocolPlugin {
    /// Create a plugin that will build clients during `init`.
    pub fn new() -> Self {
        Self {
            mcp_client: None,
            grpc_server: None,
            grpc_shutdown: Mutex::new(None),
            grpc_serve: Mutex::new(None),
        }
    }

    /// Create a plugin that wraps existing clients.
    pub fn from_clients(mcp_client: Option<Arc<dyn cog_core::McpClient>>) -> Self {
        Self {
            mcp_client,
            grpc_server: None,
            grpc_shutdown: Mutex::new(None),
            grpc_serve: Mutex::new(None),
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
            // Built here rather than at the first call: the client comes from
            // the `http_client` config section, and an endpoint or a config
            // that cannot be used is a startup finding, not something to
            // discover as a failed tool call much later. The same goes for the
            // stdio side, whose command is spawned on the same terms.
            //
            // Both the client and the deadline come from `net`: the streaming
            // client (an MCP session outlives a short exchange, so it cannot be
            // the one carrying the request deadline) and the request timeout
            // that bounds each handshake, POST and reply wait.
            let http = ctx.require::<cog_core::StreamingHttpClient>()?.0.clone();
            let request_timeout = ctx.require::<cog_core::HttpRequestTimeout>()?.0;
            let selection = crate::transport_from_config(
                std::env::var("COGNEVA_MCP_ENDPOINT").ok(),
                std::env::var("COGNEVA_MCP_COMMAND").ok(),
                std::env::var("COGNEVA_MCP_ARGS").ok(),
                http,
                request_timeout,
            )?;
            match selection {
                Some((transport, how)) => {
                    let client: Arc<dyn cog_core::McpClient> =
                        Arc::new(crate::McpClient::new(transport));
                    self.mcp_client = Some(client.clone());
                    ctx.publish_service(client);
                    info!("ProtocolPlugin MCP client initialized: {how}");
                }
                None => info!("ProtocolPlugin MCP client not configured"),
            }
        }

        // ── A2A client ──
        //
        // This used to build an `A2aClient` during init and publish it, and
        // nothing in the workspace ever read that service: the process
        // advertised a capability, took one of the `HttpClient` pin's readers
        // on its behalf, and never called it. The client itself stays --
        // `A2aClient` is still exported, and `new` now takes the `HttpClient`
        // as a required argument, so whoever needs one builds it at the call
        // site and an unwired instance cannot be constructed at all.

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

                    let (handle, stop) = spawn_grpc_server(server, socket_addr);
                    *self.grpc_shutdown.lock().unwrap_or_else(|e| e.into_inner()) = Some(stop);
                    *self.grpc_serve.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);

                    info!("ProtocolPlugin gRPC server started on {}", bind_addr);
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        // Stopping the listener is the one thing this plugin owns that
        // outlives it otherwise: without the signal below the port stays open
        // until the process exits, and a rolling update then truncates whatever
        // was in flight instead of draining it.
        let stop = self
            .grpc_shutdown
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(stop) = stop {
            // A closed receiver means the task already ended (binding failed,
            // say) -- nothing left to stop, which is not an error here.
            let _ = stop.send(());
            let serve = self
                .grpc_serve
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(serve) = serve {
                match tokio::time::timeout(std::time::Duration::from_secs(5), serve).await {
                    Ok(Ok(())) => info!("gRPC server stopped"),
                    Ok(Err(e)) => warn!("gRPC server task ended with error: {e}"),
                    Err(_) => {
                        warn!("gRPC server did not stop within 5s; it is still holding its port")
                    }
                }
            }
        }
        info!("ProtocolPlugin shutdown");
        Ok(())
    }
}

/// Serve the lifecycle RPC service until asked to stop, and hand back both ends
/// of that: the task to await and the signal that ends it.
///
/// The two are returned together because a listener nobody can stop is not a
/// service the process owns -- and the signal has to exist before the first
/// call arrives, not when someone remembers to add a shutdown path.
pub(crate) fn spawn_grpc_server(
    server: crate::agent_lifecycle::agent_lifecycle_server::AgentLifecycleServer<
        crate::grpc_agent_lifecycle::AgentLifecycleGrpcHandler,
    >,
    socket_addr: std::net::SocketAddr,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let outcome = tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_shutdown(socket_addr, async move {
                let _ = stopped.await;
            })
            .await;
        if let Err(e) = outcome {
            warn!("gRPC server exited with error: {}", e);
        }
    });
    (handle, stop)
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "protocol",
    // The MCP transport is built during `init` from `net`'s streaming client and
    // request deadline, so `net` has to have run first.
    requires: &["net"],
    optional_requires: &[],
    factory: || Box::new(ProtocolPlugin::new()),
};

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SystemPlugin;
    use std::time::Duration;

    /// The handler answers "is this agent known" before it pushes anything; it
    /// only has to exist here, so every lookup comes back empty.
    struct StubRegistry;

    #[async_trait::async_trait]
    impl cog_core::AgentRegistry for StubRegistry {
        async fn register(
            &self,
            _registration: &cog_core::AgentRegistration,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn heartbeat(&self, _agent_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn deregister(&self, _agent_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn get(
            &self,
            _agent_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::AgentRegistration>> {
            Ok(None)
        }
        async fn list(&self) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(Vec::new())
        }
        async fn list_by_role(
            &self,
            _role: &str,
        ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(Vec::new())
        }
        async fn list_by_capability(
            &self,
            _capability: &str,
        ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(Vec::new())
        }
    }

    fn lifecycle_service() -> crate::agent_lifecycle::agent_lifecycle_server::AgentLifecycleServer<
        crate::grpc_agent_lifecycle::AgentLifecycleGrpcHandler,
    > {
        let (event_tx, _rx) = tokio::sync::broadcast::channel::<cog_core::SupervisorEvent>(4);
        let handler = crate::grpc_agent_lifecycle::AgentLifecycleGrpcHandler::new(
            Arc::new(StubRegistry) as Arc<dyn cog_core::AgentRegistry>,
            event_tx,
            crate::grpc_agent_lifecycle::AgentCommandRouter::new(),
        );
        crate::agent_lifecycle::agent_lifecycle_server::AgentLifecycleServer::new(handler)
    }

    /// A port nothing is listening on: bind it, read what the OS chose, let it
    /// go. The window between the release and the server's own bind is the cost
    /// of testing on a real socket, and is why the port is chosen this way
    /// rather than written down.
    fn free_port() -> std::net::SocketAddr {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("the OS hands out a port");
        let addr = probe.local_addr().expect("a bound listener has an address");
        drop(probe);
        addr
    }

    /// Wait until the server answers, so that "it stopped" below is a change
    /// and not a race the test won.
    async fn wait_until_serving(addr: std::net::SocketAddr) {
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the server never started listening on {addr}");
    }

    /// The claim: shutting the plugin down closes the port it opened, rather
    /// than leaving it open until the process exits. A listener nobody stops is
    /// what a peer sees as a truncated connection during a rolling update.
    #[tokio::test]
    async fn shutting_the_plugin_down_closes_the_port_it_served_on() {
        let addr = free_port();
        let plugin = ProtocolPlugin::new();
        let (handle, stop) = spawn_grpc_server(lifecycle_service(), addr);
        *plugin.grpc_shutdown.lock().unwrap() = Some(stop);
        *plugin.grpc_serve.lock().unwrap() = Some(handle);

        wait_until_serving(addr).await;

        plugin.shutdown().await.expect("shutdown answers");

        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "nothing may answer on {addr} once the plugin is shut down"
        );
        // And the port is the OS's again: a listener that were still held would
        // make this bind fail with EADDRINUSE.
        let rebound = std::net::TcpListener::bind(addr).expect("the port was released");
        drop(rebound);
    }

    /// Shutting down a plugin that never got a server (no registry, no event
    /// sender -- a single-process deployment) is a no-op, not a hang.
    #[tokio::test]
    async fn shutting_down_a_plugin_that_never_served_is_not_a_hang() {
        let plugin = ProtocolPlugin::new();
        tokio::time::timeout(Duration::from_secs(5), plugin.shutdown())
            .await
            .expect("shutdown returns without a server to stop")
            .expect("and it is not an error");
    }
}
