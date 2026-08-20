//! `cog-extension` — Extension discovery, verification, registry,
//! and execution runtime for WASM and Rhai.
//! This crate consolidates the former `cog-plugin` (discovery + registry)
//! and `cog-sandbox` (WASM + Rhai execution) into a single crate.
//! | Feature | Runtime | Use-case |
//! |---------|---------|----------|
//! | `wasm`  | Wasmtime | Untrusted third-party code (plugins) |
//! | `rhai`  | Rhai     | Trusted inline scripting (prompt logic, glue) |

pub mod command_server;
pub mod executor;
pub mod plugin;
pub mod registry;
pub mod runtime;

#[cfg(feature = "rhai")]
pub use runtime::script::RhaiRuntime;
#[cfg(feature = "wasm")]
pub use runtime::wasm::WasmRuntime;
pub use runtime::{CompositeSandbox, LocalExecutor, RemoteExecutor};

use std::sync::Arc;

/// Execute a sandbox task (`WasmSkill`) through the given backend.
/// When `plugin_registry` is provided, the WASM bytecode is resolved via
/// [`cog_core::PluginRegistry::fetch_by_id`]. Otherwise the caller must
/// ensure a registry is available or the call returns an error.
pub async fn execute_task(
    backend: &dyn cog_core::SandboxBackend,
    task: &cog_core::Task,
    plugin_registry: Option<&dyn cog_core::PluginRegistry>,
) -> cog_core::SFResult<serde_json::Value> {
    let payload = match task.task_type {
        cog_core::TaskType::WasmSkill => {
            let plugin_id = task
                .input
                .get("plugin_id")
                .and_then(|v| v.as_str())
                .unwrap_or(&task.id);
            let bytes = if let Some(registry) = plugin_registry {
                registry.fetch_by_id(plugin_id).await?
            } else {
                return Err(cog_core::SFError::Agent(
                    "PluginRegistry not configured for WasmSkill".into(),
                ));
            };
            let entry = task
                .input
                .get("entry")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .into();
            cog_core::SandboxPayload::Wasm { bytes, entry }
        }
        _ => {
            return Err(cog_core::SFError::Validation(
                "execute_task called with non-sandbox task type".into(),
            ))
        }
    };

    let req = cog_core::SandboxRequest {
        task_id: task.id.clone(),
        agent_id: task.agent_id.clone().unwrap_or_default(),
        payload,
        input: task.input.clone(),
        timeout: std::time::Duration::from_secs(task.timeout_seconds),
        limits: Default::default(),
    };

    let result = backend.execute(&req).await?;
    Ok(result.into_json())
}

/// Convenience constructor that builds the appropriate backend from config.
pub fn build_extension_backend() -> Arc<dyn cog_core::SandboxBackend> {
    #[cfg(feature = "wasm")]
    {
        Arc::new(WasmRuntime::new())
    }
    #[cfg(not(feature = "wasm"))]
    {
        panic!("wasm feature not enabled — no extension backend available");
    }
}

pub use executor::TaskSandboxExecutor;
