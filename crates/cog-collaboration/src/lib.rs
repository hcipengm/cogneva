//! Multi-Agent collaboration layer — actors, squads, and orchestration.
//! Built on top of `sf-agent` (Agent kernel).  Consumers that only need a
//! single Agent loop should depend on `sf-agent` directly.

pub mod actors;
pub mod collaboration_executor;
pub mod config;
pub mod hierarchy;
pub mod ipc;
pub mod meta_features;
pub mod observable;
pub mod profile;
pub mod resume;
pub mod squad;

/// The label naming how a reading ended.
///
/// Every family here with more than one ending carries this label, and the
/// readers that count one ending select by it. It is declared once per crate
/// that writes it, under the same identifier, and a gate compares the
/// spellings: one edited copy publishes a family under a name every existing
/// selector reads as empty, which is what "this family never ran" looks like.
pub const LABEL_OUTCOME: &str = "outcome";

/// The label naming where a reading's value came from.
///
/// The same label carries the source of a build cap, of an upstream mirror and
/// of a configured period, so the name is one word across those families.
/// Declared here under the same identifier the other writers use, with a gate
/// comparing the spellings.
pub const LABEL_SOURCE: &str = "source";

/// 读数按「哪一份」归属时用的标签键。
pub const LABEL_PART: &str = "part";

/// 读数带上「为什么是这个值」时用的标签键。
pub const LABEL_REASON: &str = "reason";

/// 读数按「走到哪一步」归属时用的标签键。
pub const LABEL_STAGE: &str = "stage";

/// 读数按「按哪一条判据」归属时用的标签键。
pub const LABEL_CRITERIA: &str = "criteria";

/// 读数按「这一格给出什么结局」归属时用的标签键。
pub const LABEL_VERDICT: &str = "verdict";

/// 读数按「用哪种模式跑」归属时用的标签键。
pub const LABEL_MODE: &str = "mode";

/// 读数按「哪一档」归属时用的标签键。
pub const LABEL_TIER: &str = "tier";

/// 读数按「哪一路输入」归属时用的标签键。
pub const LABEL_INPUT: &str = "input";

/// 读数按「走哪条路线」归属时用的标签键。
pub const LABEL_ROUTE: &str = "route";

/// 读数按「哪一类对象」归属时用的标签键。
pub const LABEL_CLASS: &str = "class";

/// 读数按「哪个 agent」归属时用的标签键。
pub const LABEL_AGENT_ID: &str = "agent_id";

/// 读数按「哪个维度」归属时用的标签键。
pub const LABEL_DIMENSION: &str = "dimension";

pub use actors::{ModeratorDecision, ModeratorOutput};
pub use collaboration_executor::CollaborationExecutor;
pub use config::{
    BoundaryConfig, PgeSettings, RalphSettings, SelfReviewSettings, DEFAULT_LOCAL_REPAIR_MAX,
};
pub use hierarchy::{
    cross_squad_notify, AgentIdent, BroadcastRouter, HierarchicalCommunication,
    HierarchicalMessage, InterSquadMessage, RoutingStrategy, SquadId, TopicName,
};
pub use ipc::{FileSystemIpc, IpcChannel, IpcMessage};
pub use observable::{ChangeYieldOutcome, CollaborationObservable};
pub use profile::{
    change_tier, complexity_score, declaration_inputs, declared_scale, derive_task_profile,
    scale_label, select_mode, ChangeTier, DeclarationInput, DeclarationInputs, DeclaredScale,
    PgeMode, RouteDecision, RouteStage, TaskProfile, PIPELINE_SCORE_THRESHOLD, SCALE_LABELS,
};
pub use squad::pge::{
    parse_evaluation_result, parse_generator_output, parse_planner_output, Artifact, ContextBoard,
    Criterion, EvaluationResult, GeneratorOutput, InMemoryContextBoard, KeyValueContextBoard,
    PgePipeline, PgePipelineAttempt, PgePipelineConfig, PgePipelineResult, PgeRoundtable,
    PgeRoundtableConfig, PgeRoundtableIteration, PgeRoundtableResult, PlannerOutput, TaskSpec,
    Verdict,
};
pub use squad::ralph::{
    FailureAnalysis, RalphIteration, RalphLoop, RalphLoopConfig, RalphVerdict, ResetStrategy,
};
pub use squad::{Squad, SquadConfig, SquadExecutor, SquadResult};

pub mod plugin;

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;
