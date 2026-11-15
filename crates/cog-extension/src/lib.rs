//! `cog-extension` — Extension discovery, verification, registry,
//! and execution runtime for WASM and Rhai.
//! This crate consolidates the former `cog-plugin` (discovery + registry)
//! and `cog-sandbox` (WASM + Rhai execution) into a single crate.
//! | Feature | Runtime | Use-case |
//! |---------|---------|----------|
//! | `wasm`  | Wasmtime | Untrusted third-party code (plugins) |
//! | `rhai`  | Rhai     | Trusted inline scripting (prompt logic, glue) |

pub mod build_cache;
pub mod checkpoints;
pub mod command_server;
pub mod edit;
pub mod executor;
pub mod executor_lease;
pub mod hostdocs;
pub mod hostdocs_assist;
pub mod hostdocs_organizer;
pub mod lane_sizing;
pub mod plugin;
pub mod registry;
pub mod runtime;
pub mod workdir;

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

/// 环境类工具（shell 命令、文件读写）落在哪个后端上，由配置里的那一个地址决定：
/// 配了地址就发给独立的执行器 Pod（生产形态），没配就在本进程里跑（嵌入式/开发）。
///
/// 地址可以用逗号分成多个，一个执行器副本一个：那时一条 lane 钉在一副副本上
/// （见 [`executor_lease`]），因为同一副工作树只在一副副本的卷上。一个地址的
/// 部署与从前完全一样，钉与不钉都落到它身上。
///
/// 只有这一处做这个判断。第二个调用点各写一份三元判断的下场是两边对「没配地址」
/// 给出不同答案——而它们都自称在跑环境工具。
pub fn executor_backend(
    url: Option<String>,
) -> cog_core::SFResult<Arc<dyn cog_core::SandboxBackend>> {
    Ok(match url {
        Some(url) => {
            let endpoints: Vec<String> = url
                .split(',')
                .map(str::trim)
                .filter(|one| !one.is_empty())
                .map(str::to_string)
                .collect();
            match endpoints.len() {
                0 => Arc::new(LocalExecutor::new()),
                1 => Arc::new(RemoteExecutor::new(
                    endpoints.into_iter().next().unwrap_or_default(),
                )?),
                _ => Arc::new(RemoteExecutor::with_endpoints(endpoints)?),
            }
        }
        None => Arc::new(LocalExecutor::new()),
    })
}

pub use executor::TaskSandboxExecutor;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_address_and_an_empty_address_both_mean_the_local_backend() {
        assert!(executor_backend(None).is_ok());
        assert!(executor_backend(Some(String::new())).is_ok());
        // 一个逗号、两边都是空的：与不配地址同义，不该被当成「配了一个空地址」。
        assert!(executor_backend(Some(" , ".into())).is_ok());
    }

    #[test]
    fn a_comma_separated_list_is_one_endpoint_per_replica() {
        let backend = executor_backend(Some("http://a:9090, http://b:9090".into()));
        assert!(backend.is_ok(), "多个地址应当被接受");
        let single = executor_backend(Some("http://a:9090".into()));
        assert!(single.is_ok(), "一个地址仍然是生产形态的远端执行器");
    }
}
