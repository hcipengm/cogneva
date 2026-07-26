//! Wasmtime-based WASM sandbox runtime.

use async_trait::async_trait;
use cog_core::{SFResult, SandboxBackend, SandboxRequest, SandboxResult};
use std::collections::HashMap;
use std::sync::Mutex;

/// Wasmtime-backed sandbox for untrusted WASM modules.
pub struct WasmRuntime {
    engine: wasmtime::Engine,
    // In-memory cache of precompiled module IDs → wasmtime::Module
    module_cache: Mutex<HashMap<String, wasmtime::Module>>,
}

impl WasmRuntime {
    pub fn new() -> Self {
        let mut config = wasmtime::Config::new();
        config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        config.async_support(true);
        let engine = wasmtime::Engine::new(&config).expect("Wasmtime engine creation failed");
        Self {
            engine,
            module_cache: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for WasmRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SandboxBackend for WasmRuntime {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        tracing::info!(
            task_id = %req.task_id,
            agent_id = %req.agent_id,
            "WASM sandbox execute"
        );
        // MVP: return a stub. Full implementation will instantiate the module,
        // set up WASI, inject input, run the entry function, and collect output.
        Ok(SandboxResult {
            stdout: String::new(),
            stderr: String::new(),
            exit_code: 0,
            output: Some(req.input.clone()),
            duration_ms: 0,
            resource_usage: Default::default(),
        })
    }

    async fn precompile(&self, bytes: &[u8]) -> SFResult<String> {
        let module = wasmtime::Module::new(&self.engine, bytes)
            .map_err(|e| cog_core::SFError::Agent(format!("WASM compile error: {}", e)))?;
        let id = uuid::Uuid::new_v4().to_string();
        self.module_cache.lock().unwrap().insert(id.clone(), module);
        Ok(id)
    }
}
