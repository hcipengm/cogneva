//! Wasmtime-based WASM sandbox runtime.
//!
//! This build compiles a module and stops there: nothing here instantiates it,
//! binds WASI, or calls an entry point. That is why [`WasmRuntime::execute`]
//! refuses instead of returning a result. It used to answer with a successful
//! [`SandboxResult`] whose `output` was the request's own input -- so a
//! `wasm_skill` task landed in the ledger as having run while the module never
//! executed a single instruction, and "the module produced nothing" became
//! indistinguishable from "the module was never run". A refusal keeps those two
//! readings apart; an unimplemented execution path that reports success is the
//! worst of the three.

use async_trait::async_trait;
use cog_core::{SFResult, SandboxBackend, SandboxRequest, SandboxResult};

/// Wasmtime-backed sandbox for untrusted WASM modules.
pub struct WasmRuntime {
    engine: wasmtime::Engine,
}

impl WasmRuntime {
    pub fn new() -> Self {
        let mut config = wasmtime::Config::new();
        config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        config.async_support(true);
        let engine = wasmtime::Engine::new(&config).expect("Wasmtime engine creation failed");
        Self { engine }
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
        tracing::warn!(
            task_id = %req.task_id,
            agent_id = %req.agent_id,
            "WASM 执行还没有实现，拒绝这条请求"
        );
        Err(cog_core::SFError::Agent(format!(
            "WASM 执行还没有实现：这个构建只编译模块（`precompile`），不实例化、不调用入口，\
             任务 {} 的模块没有运行",
            req.task_id
        )))
    }

    /// Compile without keeping the result.
    ///
    /// Compiling is all this backend does today, and it is worth doing because
    /// a module that cannot compile must be refused here rather than at the
    /// call site. The cache this used to fill was written once per call, keyed
    /// by a fresh id, and read by nothing -- after `execute` learned to refuse
    /// there is no reader at all, so keeping it would only grow a process's
    /// memory for a result nobody asks for again.
    async fn precompile(&self, bytes: &[u8]) -> SFResult<String> {
        wasmtime::Module::new(&self.engine, bytes)
            .map_err(|e| cog_core::SFError::Agent(format!("WASM compile error: {e}")))?;
        Ok(uuid::Uuid::new_v4().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SandboxPayload;

    fn request() -> SandboxRequest {
        SandboxRequest {
            task_id: "task-wasm-stub".into(),
            agent_id: "agent-a".into(),
            payload: SandboxPayload::Wasm {
                bytes: b"\0asm\x01\0\0\0".to_vec(),
                entry: "main".into(),
            },
            input: serde_json::json!({"answer": 42}),
            limits: Default::default(),
        }
    }

    /// 没跑就是没跑：返回 `Ok` 会让这条任务在账上记成跑过，而模块一行没执行。
    #[tokio::test]
    async fn the_runtime_refuses_to_report_a_module_it_did_not_run() {
        let runtime = WasmRuntime::new();
        let error = runtime
            .execute(&request())
            .await
            .expect_err("一条没有执行模块的请求不许以成功收场");
        let text = error.to_string();
        assert!(
            text.contains("没有运行") && text.contains("task-wasm-stub"),
            "拒绝要说出是哪条任务没跑：{text}"
        );
    }

    /// 这台后端今天提供的全部能力就是编译：好模块过，坏字节被拒。
    #[tokio::test]
    async fn precompile_compiles_a_module_and_rejects_bytes_that_are_not_one() {
        let runtime = WasmRuntime::new();
        let id = runtime
            .precompile(b"\0asm\x01\0\0\0")
            .await
            .expect("最小的空模块是合法模块");
        assert!(!id.is_empty(), "编译成功要回一个非空的 id");
        let error = runtime
            .precompile(b"not a wasm module")
            .await
            .expect_err("不是模块的字节不许编译通过");
        assert!(
            error.to_string().contains("WASM compile error"),
            "拒绝要说清是编译失败：{error}"
        );
    }
}
