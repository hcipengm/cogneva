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

/// Sandbox payload. Serializable: it is also the wire format between the
/// remote executor client and the executor pod (`POST /execute`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SandboxPayload {
    Wasm {
        bytes: Vec<u8>,
        entry: String,
    },
    /// Shell command executed by a command-capable backend (remote executor
    /// in cluster deployments, in-process for embedded use).
    Command {
        command: String,
    },
    /// Read a file from the executor's filesystem.
    ReadFile {
        path: String,
    },
    /// Write a file on the executor's filesystem.
    WriteFile {
        path: String,
        content: String,
    },
    /// Replace one exact string in a file, where the file lives.
    ///
    /// A whole-file write is the only other way to change a line, and it costs
    /// the caller everything the file contains to say something about one line
    /// of it -- in the request, and again in whatever the model had to read
    /// first to reproduce the rest faithfully. The executor already has the
    /// bytes; it does the read, the count and the write, and answers with how
    /// many replacements it made and where.
    EditFile {
        path: String,
        old_string: String,
        new_string: String,
        /// Replace every occurrence instead of insisting on exactly one.
        #[serde(default)]
        replace_all: bool,
    },
}

/// One chunk of streaming command output. Also serves as the wire format
/// between executor server and client (NDJSON, one event per line).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CommandEvent {
    /// The checkout this call is running in, as an absolute path on the
    /// executor's machine.
    ///
    /// It travels with the reply rather than in the caller's prompt because
    /// only the executor knows it: the caller composes the lane's name, the
    /// executor turns that name into a directory. A model that has to run
    /// `pwd` to learn where it is spends a round trip finding out something the
    /// reply already knew.
    Workdir {
        path: String,
    },
    Stdout {
        data: String,
    },
    Stderr {
        data: String,
    },
    Exit {
        code: i32,
    },
}

/// Stream of command output events, boxed for object safety.
pub type CommandEventStream = std::pin::Pin<Box<dyn futures::Stream<Item = CommandEvent> + Send>>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub max_cpu_millicores: u32, // 1000 = 1 core
    pub max_memory_mb: u32,
    pub max_disk_mb: u32,
    pub allow_network: bool,
}

/// Sandbox execution result.
#[derive(Debug, Clone, Default)]
pub struct SandboxResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub output: Option<serde_json::Value>,
    pub duration_ms: u64,
    pub resource_usage: ResourceUsage,
    /// Where the call ran, as the executor reported it. `None` is a local or
    /// unnamed run: those have no checkout to name.
    pub workdir: Option<String>,
}

impl SandboxResult {
    pub fn into_json(self) -> serde_json::Value {
        self.output.unwrap_or_else(|| {
            let mut reply = serde_json::json!({
                "stdout": self.stdout,
                "stderr": self.stderr,
                "exit_code": self.exit_code,
            });
            if let Some(workdir) = self.workdir {
                reply["workdir"] = serde_json::Value::String(workdir);
            }
            reply
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

    /// Stream command output as it happens. The default implementation
    /// buffers `execute` into a single terminal sequence so existing
    /// non-streaming backends (WASM) compile unchanged.
    async fn execute_stream(&self, req: &SandboxRequest) -> crate::SFResult<CommandEventStream> {
        let result = self.execute(req).await?;
        Ok(Box::pin(futures::stream::iter(vec![
            CommandEvent::Stdout {
                data: result.stdout,
            },
            CommandEvent::Stderr {
                data: result.stderr,
            },
            CommandEvent::Exit {
                code: result.exit_code,
            },
        ])))
    }
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
