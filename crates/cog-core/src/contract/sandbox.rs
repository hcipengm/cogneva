//!Sandbox execution contracts — `cog-core` Domain Kernel.
//!Two independent traits for two different execution domains:
// - [`SandboxBackend`] — binary sandbox (WASM), for untrusted third-party code.
// - [`ScriptEngine`] — embedded script evaluation (Rhai), for trusted inline logic.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ==========================================================================
// SandboxBackend — WASM binary execution
// ==========================================================================

/// Sandbox execution request.
#[derive(Debug, Clone)]
pub struct SandboxRequest {
    pub task_id: String,
    pub agent_id: String,
    /// WASM bytecode.
    pub payload: SandboxPayload,
    /// Single-run input arguments.
    pub input: serde_json::Value,
    /// Maximum execution duration.
    pub timeout: std::time::Duration,
    /// Resource ceiling.
    pub limits: ResourceLimits,
}

#[derive(Debug, Clone)]
pub enum SandboxPayload {
    Wasm { bytes: Vec<u8>, entry: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub max_cpu_millicores: u32, // 1000 = 1 core
    pub max_memory_mb: u32,
    pub max_disk_mb: u32,
    pub allow_network: bool,
}

/// Sandbox execution result.
#[derive(Debug, Clone)]
pub struct SandboxResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub output: Option<serde_json::Value>,
    pub duration_ms: u64,
    pub resource_usage: ResourceUsage,
}

impl SandboxResult {
    pub fn into_json(self) -> serde_json::Value {
        self.output.unwrap_or_else(|| {
            serde_json::json!({
                "stdout": self.stdout,
                "stderr": self.stderr,
                "exit_code": self.exit_code,
            })
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    pub cpu_millicores_used: u32,
    pub memory_mb_peak: u32,
    pub disk_mb_written: u32,
}

#[async_trait]
pub trait SandboxBackend: Send + Sync {
    async fn execute(&self, req: &SandboxRequest) -> crate::SFResult<SandboxResult>;
    /// Pre-compile WASM module and cache, reducing repeated compilation cost.
    async fn precompile(&self, bytes: &[u8]) -> crate::SFResult<String>;
}

// ==========================================================================
// ScriptEngine — Rhai / trusted inline script evaluation
// ==========================================================================

/// Script evaluation request.
#[derive(Debug, Clone)]
pub struct ScriptRequest {
    pub script_id: String,
    pub source: String,
    pub input: serde_json::Value,
    pub timeout: std::time::Duration,
}

/// Script evaluation result.
#[derive(Debug, Clone)]
pub struct ScriptResult {
    pub output: serde_json::Value,
    pub duration_ms: u64,
}

/// Embedded script engine for trusted inline logic.
/// Use-cases:
/// - Dynamic prompt selection / preprocessing.
/// - Lightweight agent behaviour branching.
/// - Prompt-chain glue logic.
#[async_trait]
pub trait ScriptEngine: Send + Sync {
    /// Evaluate a script with the given input context.
    async fn eval(&self, req: &ScriptRequest) -> crate::SFResult<ScriptResult>;
    /// Compile a script for repeated execution (optional optimisation).
    async fn compile(&self, source: &str) -> crate::SFResult<String>;
}
