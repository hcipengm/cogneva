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

/// How the tree a command is running in came to exist on that replica.
///
/// The executor resolves this while routing the command, so it costs nothing
/// to say. It is what lets a caller distinguish a tree that carries the lane's
/// earlier work from one cut for this call: the two read identically on disk,
/// and the difference only matters after a move, when the question is whether
/// anything the previous replica wrote is in this tree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorktreeOrigin {
    /// The lane's tree was already on this replica and healthy, so it carries
    /// whatever earlier commands on this replica left in it.
    Existing,
    /// This replica cut the tree ahead of demand and handed it out now. It was
    /// never run in before this call.
    Warm,
    /// This replica cut the tree for this call.
    Created,
    /// An executor that does not report where the tree came from.
    #[default]
    Unknown,
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
        /// How this tree came to exist on the replica the command landed on.
        ///
        /// Only the executor knows this, and it is what separates "this tree
        /// has the lane's earlier work" from "this tree was cut for this
        /// call". A caller that can only guess ends up unable to say whether
        /// unpushed work survived a move between replicas; with this it can.
        /// An executor that predates the field sends nothing, hence the
        /// default -- less said, never a claim guessed.
        #[serde(default)]
        origin: WorktreeOrigin,
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

/// 编译队此刻的名单：几个位子、谁占着、谁在等（按轮到它的顺序）。
///
/// 它不只是执行器的内部状态，也是界面上「在等执行槽」那句话的唯一出处：等的人排在
/// 第几只有队知道，编排侧自己不排这条队。`capacity: None` 是「说不出来」——没有编译队
/// 的后端答不出容量，这与「容量是一」是两件事，读的人不能把前者读成后者。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildLaneReading {
    pub capacity: Option<usize>,
    pub holding: Vec<LaneHolder>,
    pub waiting: Vec<LaneWaiter>,
    /// 正按本副本的内存余量在跑、与被余量挡在门外的命令。
    ///
    /// 与编译位子的 `holding`/`waiting` 分开，因为它们等的是两样东西：位子，和
    /// 余量。扩缩要的是「这一副上还有多少工作没跑」，两份都得算；少了这一格，
    /// 一副命令全在等余量的副本会被读成闲着。
    ///
    /// `None` 是「答话的这版执行器不报这一格」，不是「零」：滚动升级期间两种
    /// 执行器同时在跑，把没报读成零会让新旧两版在同一张表上给出不同的答案。
    #[serde(default)]
    pub room: Option<RoomQueue>,
}

/// 按内存余量记的队：在跑几条、在等几条。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomQueue {
    pub running: usize,
    pub waiting: usize,
}

/// 一个正占着编译位子的人。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneHolder {
    pub agent: String,
    pub held_ms: u64,
}

/// 一个在队里等的人，与它此刻的名次（从 1 数）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneWaiter {
    pub place: usize,
    pub agent: String,
    pub tool: String,
    pub waited_ms: u64,
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
    /// 这条后端此刻的编译队。
    ///
    /// 默认回答「说不出来」，与 `boundary` 同一个理由：说不出队列的后端与队列是空的
    /// 后端是两件事，前者不该被读成「没有人等」。
    async fn lane(&self) -> crate::SFResult<BuildLaneReading> {
        Err(crate::SFError::Agent("这个后端说不出自己的编译队".into()))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 旧一版执行器不报余量这一格：它答的话照样读得出来，而「没报」与
    /// 「报了零」是两件事——前者不能读成「没有人在等」。
    #[test]
    fn a_lane_reading_without_room_is_unreported_not_zero() {
        let old: BuildLaneReading =
            serde_json::from_str(r#"{"capacity":2,"holding":[],"waiting":[]}"#)
                .expect("an older executor's answer still reads");
        assert_eq!(old.room, None);
        let new: BuildLaneReading = serde_json::from_str(
            r#"{"capacity":2,"holding":[],"waiting":[],"room":{"running":1,"waiting":3}}"#,
        )
        .expect("a reading with room reads");
        assert_eq!(
            new.room,
            Some(RoomQueue {
                running: 1,
                waiting: 3
            })
        );
    }
}
