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
    /// Remember the worktree as it is now, under `key`.
    ///
    /// The copy is taken on the side that holds the files, not by shipping
    /// them here: a conversation's tree is shared with whatever else that
    /// employee is doing, and a check point that only existed in the caller's
    /// memory would be gone the moment the caller restarted.
    CaptureCheckpoint {
        key: String,
    },
    /// What returning to `key` would change, asked before anything is touched.
    PlanCheckpoint {
        key: String,
    },
    /// Put the worktree back the way it was at `key`.
    ///
    /// Only ever sent because a person asked for it: a caller that could roll
    /// the tree back as a side effect of something else would be able to undo
    /// somebody's work without anybody seeing it happen.
    RestoreCheckpoint {
        key: String,
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

/// 命令与写盘实际被什么管住：一个维度一行。
///
/// `state` 的三种回答分得开三件事，因为把它们写成一件事是这张表最容易犯的错：
/// `enforced` 是这一层真的会拒，拒绝时说得出范围；`declared` 是**部署**声明了这条
/// 边界、这一层读不到它执行与否；`none` 是这一层根本不设边界。默认部署里没有独立
/// 执行器时，网络与资源两行就该是 `none`——把它写成 `enforced` 会让不隔离的部署
/// 读起来和隔离部署一样。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryLine {
    pub dimension: String,
    pub state: String,
    pub detail: String,
}

/// 对话里的命令跑在什么样的环境里，由跑它的那一层自己回答。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxBoundary {
    /// 执行环境的名字，对端自报：`executor`（独立执行器）或 `local`（进程内）。
    pub kind: String,
    /// 是否隔着一次进程/容器边界。`false` 时「沙箱」只是同一个进程里跑命令。
    pub isolated: bool,
    /// 这条命令落在哪棵树上；没有车道或进程内运行时没有这一格。
    pub workdir: Option<String>,
    pub lines: Vec<BoundaryLine>,
}

#[async_trait]
pub trait SandboxBackend: Send + Sync {
    async fn execute(&self, req: &SandboxRequest) -> crate::SFResult<SandboxResult>;

    /// 这次调用落在什么样的环境里。
    ///
    /// 默认回答「说不出来」，而不是给一份空边界：说不出边界的后端与边界为空的后端
    /// 是两件事，前者不该被读成后者，更不该被读成「哪里都能写」。
    async fn boundary(&self) -> crate::SFResult<SandboxBoundary> {
        Err(crate::SFError::Agent("这个后端说不出自己的边界".into()))
    }
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
