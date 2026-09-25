//! Stage 3 — SquadExecutor: 直接执行任务到 Squad。
//! - Task 直接映射到 Squad，Squad 是原子任务的执行单元。
//! - Squad 的三层循环：Ralph Loop → PGE (Pipeline/Roundtable) → SelfReview。
//! - Ralph Loop 判定"不可修复"时，触发 Squad 级自动重试（默认 3 次）。
//! - 重试耗尽后原子任务终止。

use std::sync::Arc;

use crate::actors::{EvaluatorActor, GeneratorActor, PlannerActor};
use crate::hierarchy::{HierarchicalCommunication, HierarchicalMessage};
use crate::profile::PgeMode;
use crate::squad::pge::pipeline::{PgePipeline, PgePipelineConfig};
use crate::squad::pge::roundtable::{PgeRoundtable, PgeRoundtableConfig};
use crate::squad::ralph::ralph_loop::{RalphLoop, RalphLoopConfig, RalphVerdict};
use cog_core::{BroadcastScope, SquadStatus};
use cog_core::{HookEngine, HookEvent, HookTrigger};

/// 创建 Squad 所需的配置。
#[derive(Debug, Clone)]
pub struct SquadConfig {
    pub goal: String,
    pub context: serde_json::Value,
    /// 这次运行实际会用的拓扑。它是"跑到哪几个角色"的唯一来源：PlanOnly 表示
    /// 只跑 Planner（分解任务的交付物就是它产出的任务列表），另外两个值表示
    /// Planner → Generator → Evaluator 的两种拓扑。
    pub pge_mode: PgeMode,
    /// Squad 被 Ralph Loop 判定不可修复后的最大策略升级次数。
    /// 0 = 只做一次尝试（不升级），1 = 允许一次 Pipeline → Roundtable 升级。
    pub max_retries: u32,
    /// 可选的上下文窗口大小，用于策略升级时动态扩容。
    pub context_window_size: Option<usize>,
    /// 可选的任务 profile，用于在没有 MetaLearningEngine 时通过静态规则选择 PGE 模式。
    pub profile: Option<crate::profile::TaskProfile>,
    /// 可选的 BoundaryConfig，注入到 Evaluator Agent 用于动态边界维度评估。
    pub boundary_config: Option<crate::BoundaryConfig>,
    /// true = 这是一个 self_evolution 任务；使用更激进的短路径以控制延迟。
    pub is_self_evolution: bool,
    /// 通过 Skill ID 指定 Planner 角色的 prompt/schema 来源（None = 内置默认）。
    pub planner_skill_id: Option<String>,
    /// 通过 Skill ID 指定 Generator 角色的 prompt/schema 来源。
    pub generator_skill_id: Option<String>,
    /// 通过 Skill ID 指定 Evaluator 角色的 prompt/schema 来源。
    pub evaluator_skill_id: Option<String>,
}

impl Default for SquadConfig {
    fn default() -> Self {
        Self {
            goal: String::new(),
            context: serde_json::json!({}),
            pge_mode: PgeMode::Pipeline,
            max_retries: 1,
            context_window_size: None,
            profile: None,
            boundary_config: None,
            is_self_evolution: false,
            planner_skill_id: None,
            generator_skill_id: None,
            evaluator_skill_id: None,
        }
    }
}

/// 单个 Squad 实例。
#[derive(Debug, Clone)]
pub struct Squad {
    pub id: String,
    pub task_id: String,
    pub config: SquadConfig,
    pub status: SquadStatus,
    pub result: Option<serde_json::Value>,
    /// 当前 Squad 已被替换过几次（0 = 首次运行）。
    pub retry_count: u32,
    /// 上一次失败时继承的快照 ID（用于从 ObjectBackend 检索）。
    pub snapshot_id: Option<String>,
    /// 上一次失败时继承的快照上下文（完整 Snapshot 结构）。
    pub snapshot: Option<cog_core::AgentCheckpoint>,
    /// 连续 Pipeline 失败次数。当达到
    /// [`PIPELINE_FAILURES_BEFORE_UPGRADE`] 时自动升级到 Roundtable。
    pub consecutive_pipeline_failures: u32,
}

/// 连续 Pipeline 失败多少次后自动升级到 Roundtable 模式。
pub const PIPELINE_FAILURES_BEFORE_UPGRADE: u32 = 2;

/// 单个 Squad 执行结果。
#[derive(Debug, Clone, serde::Serialize)]
pub struct SquadResult {
    pub squad_id: String,
    pub success: bool,
    pub result: Option<serde_json::Value>,
    pub retry_count: u32,
    pub error: Option<String>,
    /// The PGE mode that was actually used for this squad's execution.
    pub pge_mode: PgeMode,
    /// Squad-level reflection output (populated when reflection is configured).
    #[serde(skip)]
    pub reflection: Option<cog_core::SquadReflectionResult>,
}

/// Squad 执行器 —— 直接执行 Task → Squad。
#[derive(Default)]
pub struct SquadExecutor {
    hook_engine: Option<Arc<dyn HookEngine>>,
    object_backend: Option<Arc<dyn cog_core::ObjectBackend>>,
    agent_manager: Option<Arc<dyn cog_core::AgentManager>>,
    llm_provider: Option<Arc<dyn cog_core::LlmClient>>,
    squad_reflection: Option<Arc<dyn cog_core::SquadReflection>>,
    meta_learning: Option<Arc<dyn cog_core::MetaLearning>>,
    hierarchy: Option<Arc<HierarchicalCommunication>>,
    knowledge_backend: Option<Arc<dyn cog_core::KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    pge_schemas: Option<std::collections::HashMap<String, serde_json::Value>>,
    /// Skill registry — 解析 SquadConfig 中 *_skill_id 指定的 prompt skill。
    skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    /// State backend — 持久化 RalphLoop 迭代历史（ContextBoard 载体），重启后可续跑。
    state_backend: Option<Arc<dyn cog_core::StateBackend>>,
    /// Ralph Loop 预算与停滞窗口（来自配置面 `ralph` 段，默认见 RalphLoopConfig）。
    ralph: RalphLoopConfig,
    /// 局部修复预算（来自配置面 `pge` 段）。None = 由
    /// [`crate::DEFAULT_LOCAL_REPAIR_MAX`] 兜底。
    local_repair_max: Option<u32>,
}

/// Optional service dependencies shared by squad execution stages.
#[derive(Clone, Default)]
pub(crate) struct SquadDeps {
    pub hook_engine: Option<Arc<dyn HookEngine>>,
    pub object_backend: Option<Arc<dyn cog_core::ObjectBackend>>,
    pub agent_manager: Option<Arc<dyn cog_core::AgentManager>>,
    pub llm_provider: Option<Arc<dyn cog_core::LlmClient>>,
    pub squad_reflection: Option<Arc<dyn cog_core::SquadReflection>>,
    pub knowledge_backend: Option<Arc<dyn cog_core::KnowledgeBackend>>,
    pub self_review: Option<cog_core::SelfReviewConfig>,
    pub pge_schemas: Option<std::collections::HashMap<String, serde_json::Value>>,
    pub skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    pub state_backend: Option<Arc<dyn cog_core::StateBackend>>,
    pub ralph: RalphLoopConfig,
    /// 局部修复预算（配置面 `pge` 段）。None = [`crate::DEFAULT_LOCAL_REPAIR_MAX`]。
    pub local_repair_max: Option<u32>,
}

impl SquadExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_hook_engine(mut self, engine: Arc<dyn HookEngine>) -> Self {
        self.hook_engine = Some(engine);
        self
    }

    /// Inject an object backend for snapshot persistence.
    pub fn with_object_backend(mut self, backend: Arc<dyn cog_core::ObjectBackend>) -> Self {
        self.object_backend = Some(backend);
        self
    }

    /// Inject an LLM provider so that actors can use real inference instead of stubs.
    pub fn with_agent_manager(mut self, manager: Arc<dyn cog_core::AgentManager>) -> Self {
        self.agent_manager = Some(manager);
        self
    }

    /// Inject an LLM provider used when creating agent workers via the agent manager.
    pub fn with_llm_provider(mut self, provider: Arc<dyn cog_core::LlmClient>) -> Self {
        self.llm_provider = Some(provider);
        self
    }

    /// Attach a squad-level reflection engine for intra-squad disagreement detection.
    pub fn with_squad_reflection(mut self, reflection: Arc<dyn cog_core::SquadReflection>) -> Self {
        self.squad_reflection = Some(reflection);
        self
    }

    /// Attach a meta-learning engine for predictive PGE mode selection.
    pub fn with_meta_learning(mut self, engine: Arc<dyn cog_core::MetaLearning>) -> Self {
        self.meta_learning = Some(engine);
        self
    }

    /// Attach a hierarchical communication layer for Squad/Agent broadcasts.
    pub fn with_hierarchy(mut self, comm: Arc<HierarchicalCommunication>) -> Self {
        self.hierarchy = Some(comm);
        self
    }

    /// Attach a unified knowledge backend for historical pattern retrieval.
    pub fn with_knowledge_backend(mut self, backend: Arc<dyn cog_core::KnowledgeBackend>) -> Self {
        self.knowledge_backend = Some(backend);
        self
    }

    /// Enable the self-review quality gate for all PGE actors in this squad.
    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Attach operator-configured JSON Schemas for PGE actor outputs,
    /// keyed by actor name ("planner", "generator", "evaluator",
    /// "moderator", "merger").
    pub fn with_pge_schemas(
        mut self,
        schemas: std::collections::HashMap<String, serde_json::Value>,
    ) -> Self {
        self.pge_schemas = Some(schemas);
        self
    }

    /// Inject a skill registry for resolving prompt skills
    /// (`SquadConfig.{planner,generator,evaluator}_skill_id`)。
    pub fn with_skill_registry(
        mut self,
        registry: Arc<dyn cog_core::ExternalSkillRegistry>,
    ) -> Self {
        self.skill_registry = Some(registry);
        self
    }

    /// Inject a state backend so RalphLoop can persist iteration history
    /// across restarts (stored on the task's ContextBoard).
    pub fn with_state_backend(mut self, backend: Arc<dyn cog_core::StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// Override Ralph Loop budget/stagnation knobs (from the `ralph` config
    /// section). Unset = `RalphLoopConfig::default()`.
    pub fn with_ralph_config(mut self, config: RalphLoopConfig) -> Self {
        self.ralph = config;
        self
    }

    /// Override the local-repair budget (from the `pge` config section).
    /// Unset = [`crate::DEFAULT_LOCAL_REPAIR_MAX`].
    pub fn with_local_repair_max(mut self, max: u32) -> Self {
        self.local_repair_max = Some(max);
        self
    }

    /// 直接执行一个 Squad，返回执行结果。
    pub async fn execute_squad(&self, task_id: String, config: SquadConfig) -> SquadResult {
        let squad_id = format!("squad:{}", task_id);
        let squad = Squad {
            id: squad_id.clone(),
            task_id: task_id.clone(),
            config,
            status: SquadStatus::Pending,
            result: None,
            retry_count: 0,
            snapshot_id: None,
            snapshot: None,
            consecutive_pipeline_failures: 0,
        };

        let hook_engine = self.hook_engine.clone();
        let hierarchy = self.hierarchy.clone();

        // Broadcast squad start via hierarchical communication layer.
        if let Some(ref comm) = hierarchy {
            let msg = HierarchicalMessage::new(
                uuid::Uuid::new_v4().to_string(),
                BroadcastScope::Squad {
                    squad_id: squad_id.clone(),
                },
                "squad_started",
                &squad_id,
                serde_json::json!({"task_id": &task_id}),
            );
            let _ = comm.broadcast(&msg).await;
        }

        // Mode selection is done upstream by ModeSelectorAgent (LLM semantic
        // judgment + keyword heuristic). SquadExecutor respects that decision
        // and does not override it with meta-learning or static rules.
        // Meta-learning data is still recorded at the end for future improvement.
        tracing::info!(
            squad_id = %squad.id,
            mode = ?squad.config.pge_mode,
            "Respecting upstream mode selection from ModeSelectorAgent"
        );

        let squad_start = std::time::Instant::now();
        let deps = SquadDeps {
            hook_engine: hook_engine.clone(),
            object_backend: self.object_backend.clone(),
            agent_manager: self.agent_manager.clone(),
            llm_provider: self.llm_provider.clone(),
            squad_reflection: self.squad_reflection.clone(),
            knowledge_backend: self.knowledge_backend.clone(),
            self_review: self.self_review.clone(),
            pge_schemas: self.pge_schemas.clone(),
            skill_registry: self.skill_registry.clone(),
            state_backend: self.state_backend.clone(),
            ralph: self.ralph,
            local_repair_max: self.local_repair_max,
        };
        let result = Self::run_squad_with_retries(squad, deps).await;
        let squad_latency_ms = squad_start.elapsed().as_millis() as u64;
        crate::observable::global_observable()
            .record_turnaround(&result.squad_id, squad_latency_ms)
            .await;
        crate::observable::global_observable().record_message();

        // Meta-learning: record the actual outcome so the model improves.
        //
        // 只记真的跑过 PGE 拓扑的那些运行。分解路径没有拓扑可言，把它记成一次
        // 模式试验就是往学习数据里写一条"这个模式跑过、结果如此"的假记录——
        // 模式选择的样本会被一批从未发生过的试验污染，而污染的表现是模型对
        // 真实模式越来越有把握。
        if let Some(ref engine) = self.meta_learning {
            let decision = crate::meta_features::squad_decision();
            if let Some(mode_str) = result.pge_mode.as_learnt_mode() {
                let _ = engine
                    .record_outcome(
                        &decision.group,
                        &decision.features,
                        mode_str,
                        result.success,
                        if result.success { 1.0 } else { 0.0 },
                        0,
                    )
                    .await;
            }
        }

        // Broadcast squad completion via hierarchical communication layer.
        if let Some(ref comm) = hierarchy {
            let msg = HierarchicalMessage::new(
                uuid::Uuid::new_v4().to_string(),
                BroadcastScope::Squad {
                    squad_id: squad_id.clone(),
                },
                "squad_completed",
                &squad_id,
                serde_json::json!({
                    "task_id": &task_id,
                    "success": result.success,
                }),
            );
            let _ = comm.broadcast(&msg).await;
        }

        result
    }

    /// 执行一次 Ralph Loop（Pipeline 或 Roundtable），通过 AgentManager 创建 Agent。
    async fn run_squad_once(
        squad: &Squad,
        ralph: &mut RalphLoop,
        deps: &SquadDeps,
    ) -> RalphVerdict {
        let agent_manager = &deps.agent_manager;
        let llm_provider = &deps.llm_provider;
        let knowledge_backend = &deps.knowledge_backend;
        let self_review = &deps.self_review;
        let pge_schemas = &deps.pge_schemas;
        let skill_registry = &deps.skill_registry;
        let schema_for = |actor: &str| -> Option<serde_json::Value> {
            pge_schemas.as_ref()?.get(actor).cloned()
        };
        // 解析 SquadConfig 指定的 prompt skills（解析失败回退内置 prompt）。
        let planner_skill = crate::actors::resolve_prompt_skill(
            skill_registry.as_ref(),
            squad.config.planner_skill_id.as_deref(),
            "planner",
        )
        .await;
        let generator_skill = crate::actors::resolve_prompt_skill(
            skill_registry.as_ref(),
            squad.config.generator_skill_id.as_deref(),
            "generator",
        )
        .await;
        let evaluator_skill = crate::actors::resolve_prompt_skill(
            skill_registry.as_ref(),
            squad.config.evaluator_skill_id.as_deref(),
            "evaluator",
        )
        .await;
        // 分解路径只跑 Planner：这条路上没有生成器也没有评估器，凭空创建两个
        // 不会被调用的 agent 是白花，还会让"谁参与了这次运行"变得不可读。
        if matches!(squad.config.pge_mode, PgeMode::PlanOnly) {
            let (Some(manager), Some(llm)) = (agent_manager.as_ref(), llm_provider.as_ref()) else {
                return RalphVerdict::Unrecoverable {
                    reason: "AgentManager not available to create the Planner agent".into(),
                    iterations: 0,
                    history: Vec::new(),
                };
            };
            let planner = match manager
                .create_agent(&format!("{}-planner", squad.id), "planner", llm.clone())
                .await
            {
                Ok(agent) => agent,
                Err(e) => {
                    tracing::warn!("Failed to create the decomposition planner agent: {}", e);
                    return RalphVerdict::Unrecoverable {
                        reason: format!("Failed to create the Planner agent: {e}"),
                        iterations: 0,
                        history: Vec::new(),
                    };
                }
            };
            let mut planner_actor = PlannerActor::new(planner);
            if let Some(ref kb) = knowledge_backend {
                planner_actor = planner_actor.with_knowledge(kb.clone());
            }
            if let Some(ref cfg) = self_review {
                planner_actor = planner_actor.with_self_review(cfg.clone());
            }
            if let Some(s) = schema_for("planner") {
                planner_actor = planner_actor.with_output_schema(s);
            }
            if let Some(ref sk) = planner_skill {
                planner_actor = planner_actor.with_prompt_skill(sk.clone());
            }
            return ralph
                .run_plan(
                    &squad.config.goal,
                    squad.config.context.clone(),
                    &planner_actor,
                )
                .await;
        }

        // 声明式范围路径：请求自己说清了要改哪些文件，于是没有 planner 阶段，
        // 计划由请求的声明充当。生成器与评估器都在，逐条装配与别的路径相同——
        // 省的是一个阶段，不是 PGE 的判据面。
        if matches!(squad.config.pge_mode, PgeMode::Direct) {
            let (Some(manager), Some(llm)) = (agent_manager.as_ref(), llm_provider.as_ref()) else {
                return RalphVerdict::Unrecoverable {
                    reason: "AgentManager not available to create the Generator agent".into(),
                    iterations: 0,
                    history: Vec::new(),
                };
            };
            let generator = match manager
                .create_agent(&format!("{}-generator", squad.id), "generator", llm.clone())
                .await
            {
                Ok(agent) => agent,
                Err(e) => {
                    tracing::warn!("Failed to create the declared-scope generator agent: {}", e);
                    return RalphVerdict::Unrecoverable {
                        reason: format!("Failed to create the Generator agent: {e}"),
                        iterations: 0,
                        history: Vec::new(),
                    };
                }
            };
            let evaluator = match manager
                .create_agent(&format!("{}-evaluator", squad.id), "evaluator", llm.clone())
                .await
            {
                Ok(agent) => agent,
                Err(e) => {
                    tracing::warn!("Failed to create the declared-scope evaluator agent: {}", e);
                    return RalphVerdict::Unrecoverable {
                        reason: format!("Failed to create the Evaluator agent: {e}"),
                        iterations: 0,
                        history: Vec::new(),
                    };
                }
            };
            let mut generator_actor = GeneratorActor::new(generator);
            let mut evaluator_actor = EvaluatorActor::new(evaluator);
            if let Some(ref kb) = knowledge_backend {
                generator_actor = generator_actor.with_knowledge(kb.clone());
                evaluator_actor = evaluator_actor.with_knowledge(kb.clone());
            }
            if let Some(ref cfg) = self_review {
                generator_actor = generator_actor.with_self_review(cfg.clone());
                evaluator_actor = evaluator_actor.with_self_review(cfg.clone());
            }
            if let Some(s) = schema_for("generator") {
                generator_actor = generator_actor.with_output_schema(s);
            }
            if let Some(s) = schema_for("evaluator") {
                evaluator_actor = evaluator_actor.with_output_schema(s);
            }
            if let Some(ref sk) = generator_skill {
                generator_actor = generator_actor.with_prompt_skill(sk.clone());
            }
            if let Some(ref sk) = evaluator_skill {
                evaluator_actor = evaluator_actor.with_prompt_skill(sk.clone());
            }
            // 范围取自已算好的 profile：路由是按它判的，计划就必须按同一个读数
            // 写，否则"为什么走这条路径"与"计划说了什么"会各自成立却对不上。
            let scale = squad
                .config
                .profile
                .map(|p| p.declared_scale)
                .unwrap_or_default();
            let declared = crate::squad::pge::pipeline::declared_plan(&squad.config.goal, scale);
            let mut pipeline_config = PgePipelineConfig::default();
            if squad.config.is_self_evolution {
                // 与 Pipeline 同一条：自进化任务对延迟敏感，一次尝试交给外层
                // Ralph 做全局重置就够。
                pipeline_config.max_retries = 1;
            }
            pipeline_config.local_repair_max = deps
                .local_repair_max
                .unwrap_or(crate::DEFAULT_LOCAL_REPAIR_MAX);
            let pipeline = PgePipeline::new(pipeline_config);
            return ralph
                .run_declared_scope(
                    &squad.config.goal,
                    squad.config.context.clone(),
                    &pipeline,
                    &declared,
                    &generator_actor,
                    &evaluator_actor,
                )
                .await;
        }

        let (planner, generator, evaluator, moderator) = match (agent_manager, llm_provider) {
            (Some(manager), Some(llm)) => {
                let planner_id = format!("{}-planner", squad.id);
                let generator_id = format!("{}-generator", squad.id);
                let evaluator_id = format!("{}-evaluator", squad.id);
                let moderator_id = format!("{}-moderator", squad.id);

                let planner = manager
                    .create_agent(&planner_id, "planner", llm.clone())
                    .await;
                let generator = manager
                    .create_agent(&generator_id, "generator", llm.clone())
                    .await;
                let evaluator = manager
                    .create_agent(&evaluator_id, "evaluator", llm.clone())
                    .await;
                // Moderator is optional — failure to create it is non-fatal.
                let moderator = manager
                    .create_agent(&moderator_id, "moderator", llm.clone())
                    .await
                    .ok();

                match (planner, generator, evaluator) {
                    (Ok(p), Ok(g), Ok(e)) => (Some(p), Some(g), Some(e), moderator),
                    (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                        tracing::warn!("Failed to create squad agents via AgentManager: {}", e);
                        (None, None, None, None)
                    }
                }
            }
            _ => (None, None, None, None),
        };

        match squad.config.pge_mode {
            // 分解在上面的早返回里处理掉了：走到这里必然是有拓扑的模式。
            PgeMode::PlanOnly => RalphVerdict::Unrecoverable {
                reason: "PlanOnly squads run before the PGE dispatch".into(),
                iterations: 0,
                history: Vec::new(),
            },
            // 同上：声明式范围也在早返回里跑完了，它不需要下面这套
            // planner/generator/evaluator 的装配。
            PgeMode::Direct => RalphVerdict::Unrecoverable {
                reason: "Declared-scope squads run before the PGE dispatch".into(),
                iterations: 0,
                history: Vec::new(),
            },
            PgeMode::Pipeline => {
                let mut pipeline_config = PgePipelineConfig::default();
                if squad.config.is_self_evolution {
                    // Self-evolution tasks are latency-sensitive: one attempt per
                    // Ralph iteration is enough; rely on Ralph for global reset.
                    pipeline_config.max_retries = 1;
                }
                // The repair loop is what lets the evaluator's feedback reach the
                // generator without paying for a new plan, so it is set for every
                // mode rather than only the non-self-evolution ones: the run that
                // matters most here is the one whose artifact failed a structural
                // gate that already named what to change.
                pipeline_config.local_repair_max = deps
                    .local_repair_max
                    .unwrap_or(crate::DEFAULT_LOCAL_REPAIR_MAX);
                let pipeline = PgePipeline::new(pipeline_config);
                match (planner.as_ref(), generator.as_ref(), evaluator.as_ref()) {
                    (Some(p), Some(g), Some(e)) => {
                        let mut planner_actor = PlannerActor::new(Arc::clone(p));
                        let mut generator_actor = GeneratorActor::new(Arc::clone(g));
                        let mut evaluator_actor = EvaluatorActor::new(Arc::clone(e));
                        if let Some(ref kb) = knowledge_backend {
                            planner_actor = planner_actor.with_knowledge(kb.clone());
                            generator_actor = generator_actor.with_knowledge(kb.clone());
                            evaluator_actor = evaluator_actor.with_knowledge(kb.clone());
                        }
                        if let Some(ref cfg) = self_review {
                            planner_actor = planner_actor.with_self_review(cfg.clone());
                            generator_actor = generator_actor.with_self_review(cfg.clone());
                            evaluator_actor = evaluator_actor.with_self_review(cfg.clone());
                        }
                        if let Some(s) = schema_for("planner") {
                            planner_actor = planner_actor.with_output_schema(s);
                        }
                        if let Some(s) = schema_for("generator") {
                            generator_actor = generator_actor.with_output_schema(s);
                        }
                        if let Some(s) = schema_for("evaluator") {
                            evaluator_actor = evaluator_actor.with_output_schema(s);
                        }
                        if let Some(ref sk) = planner_skill {
                            planner_actor = planner_actor.with_prompt_skill(sk.clone());
                        }
                        if let Some(ref sk) = generator_skill {
                            generator_actor = generator_actor.with_prompt_skill(sk.clone());
                        }
                        if let Some(ref sk) = evaluator_skill {
                            evaluator_actor = evaluator_actor.with_prompt_skill(sk.clone());
                        }
                        ralph
                            .run_pipeline(
                                &squad.config.goal,
                                squad.config.context.clone(),
                                &pipeline,
                                &planner_actor,
                                &generator_actor,
                                &evaluator_actor,
                            )
                            .await
                    }
                    _ => RalphVerdict::Unrecoverable {
                        reason: "AgentManager not available to create Pipeline agents".into(),
                        iterations: 0,
                        history: Vec::new(),
                    },
                }
            }
            PgeMode::Roundtable => match (planner, generator, evaluator) {
                (Some(p), Some(g), Some(e)) => {
                    let mut rt_config = PgeRoundtableConfig {
                        agent_manager: agent_manager.clone(),
                        llm_provider: llm_provider.clone(),
                        ..Default::default()
                    };
                    if let Some(m) = moderator {
                        let mut moderator_actor = crate::actors::ModeratorActor::new(m);
                        if let Some(ref kb) = knowledge_backend {
                            moderator_actor = moderator_actor.with_knowledge(kb.clone());
                        }
                        if let Some(ref cfg) = self_review {
                            moderator_actor = moderator_actor.with_self_review(cfg.clone());
                        }
                        if let Some(s) = schema_for("moderator") {
                            moderator_actor = moderator_actor.with_output_schema(s);
                        }
                        rt_config.moderator = Some(moderator_actor);
                    }
                    if matches!(
                        rt_config.branch_merge_strategy,
                        crate::squad::pge::types::BranchMergeStrategy::Custom
                    ) {
                        if let (Some(manager), Some(llm)) =
                            (agent_manager.as_ref(), llm_provider.as_ref())
                        {
                            let merger_id = format!("{}-merger", squad.id);
                            match manager
                                .create_agent(&merger_id, "merger", llm.clone())
                                .await
                            {
                                Ok(agent) => {
                                    let mut merger_actor = crate::actors::MergerActor::new(agent);
                                    if let Some(ref cfg) = self_review {
                                        merger_actor = merger_actor.with_self_review(cfg.clone());
                                    }
                                    if let Some(s) = schema_for("merger") {
                                        merger_actor = merger_actor.with_output_schema(s);
                                    }
                                    rt_config.merger = Some(merger_actor);
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to create merger agent: {}", e);
                                }
                            }
                        }
                    }
                    let mut planner_actor = PlannerActor::new(p);
                    let mut generator_actor = GeneratorActor::new(g);
                    let mut evaluator_actor = EvaluatorActor::new(e);
                    if let Some(ref kb) = knowledge_backend {
                        planner_actor = planner_actor.with_knowledge(kb.clone());
                        generator_actor = generator_actor.with_knowledge(kb.clone());
                        evaluator_actor = evaluator_actor.with_knowledge(kb.clone());
                    }
                    if let Some(ref cfg) = self_review {
                        planner_actor = planner_actor.with_self_review(cfg.clone());
                        generator_actor = generator_actor.with_self_review(cfg.clone());
                        evaluator_actor = evaluator_actor.with_self_review(cfg.clone());
                    }
                    if let Some(s) = schema_for("planner") {
                        planner_actor = planner_actor.with_output_schema(s);
                    }
                    if let Some(s) = schema_for("generator") {
                        generator_actor = generator_actor.with_output_schema(s);
                    }
                    if let Some(s) = schema_for("evaluator") {
                        evaluator_actor = evaluator_actor.with_output_schema(s);
                    }
                    if let Some(ref sk) = planner_skill {
                        planner_actor = planner_actor.with_prompt_skill(sk.clone());
                    }
                    if let Some(ref sk) = generator_skill {
                        generator_actor = generator_actor.with_prompt_skill(sk.clone());
                    }
                    if let Some(ref sk) = evaluator_skill {
                        evaluator_actor = evaluator_actor.with_prompt_skill(sk.clone());
                    }
                    let roundtable = PgeRoundtable::new(
                        rt_config,
                        planner_actor,
                        generator_actor,
                        evaluator_actor,
                    );
                    ralph
                        .run_roundtable(
                            &squad.config.goal,
                            squad.config.context.clone(),
                            &roundtable,
                        )
                        .await
                }
                _ => RalphVerdict::Unrecoverable {
                    reason: "AgentManager not available to create Roundtable agents".into(),
                    iterations: 0,
                    history: Vec::new(),
                },
            },
        }
    }

    /// 运行单个 Squad。
    /// Ralph Loop 由配置面的迭代预算与停滞窗口止损（默认 50 轮/5 轮停滞窗），
    /// Squad 层不再做无意义重试。
    /// 只做一次策略升级：Pipeline 失败后自动切换到 Roundtable，再试一次。
    async fn run_squad_with_retries(mut squad: Squad, deps: SquadDeps) -> SquadResult {
        let hook_engine = &deps.hook_engine;
        let object_backend = &deps.object_backend;
        let squad_reflection = &deps.squad_reflection;
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: if squad.config.is_self_evolution {
                2
            } else {
                deps.ralph.max_iterations
            },
            stagnation_window: deps.ralph.stagnation_window,
        });
        if let Some(ref llm) = deps.llm_provider {
            ralph = ralph.with_llm_provider(llm.clone());
        }
        if let Some(ref backend) = deps.state_backend {
            ralph = ralph.with_history_store(squad.task_id.clone(), backend.clone());
        }

        // 若 Squad 已携带快照，先从后端恢复
        if let (Some(ref backend), Some(ref snap_id)) = (&object_backend, &squad.snapshot_id) {
            match backend.get(snap_id).await {
                Ok(Some(bytes)) => {
                    if let Ok(snapshot) =
                        serde_json::from_slice::<cog_core::AgentCheckpoint>(&bytes)
                    {
                        squad.snapshot = Some(snapshot);
                    }
                }
                Ok(None) => {
                    tracing::warn!("Snapshot {} not found in backend", snap_id);
                }
                Err(e) => {
                    tracing::warn!("Failed to load snapshot {}: {}", snap_id, e);
                }
            }
        }

        // === 尝试 1：初始 PGE 模式 ===
        squad.status = SquadStatus::Running;
        let verdict = Self::run_squad_once(&squad, &mut ralph, &deps).await;

        if let RalphVerdict::Passed { ref result, .. } = verdict {
            squad.status = SquadStatus::Complete;
            squad.result = Some(result.clone());
            if let Some(ref engine) = hook_engine {
                let event = HookEvent::new(HookTrigger::OnRalphPass)
                    .with_task_id(&squad.task_id)
                    .with_squad_id(&squad.id)
                    .with_payload(serde_json::json!({
                        "squad_id": squad.id,
                        "retry_count": 0,
                        "result": result,
                    }));
                let engine = engine.clone();
                tokio::spawn(async move {
                    let _ = engine.emit(event).await;
                });
            }
            let reflection = run_squad_reflection(squad_reflection, &verdict, &squad).await;
            return SquadResult {
                squad_id: squad.id,
                success: true,
                result: squad.result,
                retry_count: 0,
                error: None,
                pge_mode: squad.config.pge_mode,
                reflection,
            };
        }

        // === 尝试 1 失败：运行 Reflection，决定是否升级策略 ===
        let reflection = run_squad_reflection(squad_reflection, &verdict, &squad).await;

        // 只有 Pipeline 有升级目标，所以这条判据同时排除两件事：Roundtable
        // 失败后没有下一个拓扑，分解（PlanOnly）更没有——它跑的不是拓扑，
        // 升级只会把生成器重新请回一条不需要它的路径上。
        let is_pipeline = matches!(squad.config.pge_mode, PgeMode::Pipeline);
        // Deterministic environment/protocol failures and stall-detected
        // degenerate loops cannot be fixed by a strategy upgrade — Roundtable
        // would burn the same tokens to the same empty/flat result.
        let is_terminal = matches!(
            &verdict,
            RalphVerdict::Unrecoverable { reason, .. }
                if reason.starts_with(cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX)
                    || reason.starts_with(cog_core::contract::outcome::DEGENERATE_LOOP_PREFIX)
        );
        let can_upgrade = squad.config.max_retries > 0 && !is_terminal;
        let should_upgrade = can_upgrade
            && if let Some(ref r) = reflection {
                if r.upgrade_recommended && is_pipeline {
                    tracing::info!(
                        squad_id = %squad.id,
                        "Squad reflection recommends upgrade → forcing Pipeline → Roundtable"
                    );
                    true
                } else {
                    is_pipeline
                }
            } else {
                is_pipeline
            };

        if !should_upgrade {
            // Roundtable 也失败，直接返回
            squad.status = SquadStatus::Failed;
            if let Some(ref engine) = hook_engine {
                let event = HookEvent::new(HookTrigger::OnRalphUnrecoverable)
                    .with_task_id(&squad.task_id)
                    .with_squad_id(&squad.id)
                    .with_payload(serde_json::json!({
                        "squad_id": squad.id,
                        "retry_count": 0,
                        "reason": match &verdict {
                            RalphVerdict::Unrecoverable { reason, .. } => reason,
                            _ => "unknown",
                        },
                    }));
                let engine2 = engine.clone();
                tokio::spawn(async move {
                    let _ = engine2.emit(event).await;
                });
            }
            return SquadResult {
                squad_id: squad.id,
                success: false,
                result: None,
                retry_count: 0,
                error: match verdict {
                    RalphVerdict::Unrecoverable { reason, .. } => Some(reason),
                    _ => None,
                },
                pge_mode: squad.config.pge_mode,
                reflection,
            };
        }

        // === 升级策略 ===
        squad.config.pge_mode = PgeMode::Roundtable;
        if let Some(ref mut size) = squad.config.context_window_size {
            *size = size.saturating_add(1024);
        }
        squad.retry_count = 1;
        squad.status = SquadStatus::Retrying;

        if let Some(ref engine) = hook_engine {
            let event = HookEvent::new(HookTrigger::OnSquadRetry)
                .with_task_id(&squad.task_id)
                .with_squad_id(&squad.id)
                .with_payload(serde_json::json!({
                    "squad_id": squad.id,
                    "retry_count": 1,
                    "reason": match &verdict {
                        RalphVerdict::Unrecoverable { reason, .. } => reason,
                        _ => "unknown",
                    },
                    "snapshot_id": squad.snapshot_id,
                    "escalated": true,
                    "strategy": "Pipeline → Roundtable",
                }));
            let engine = engine.clone();
            tokio::spawn(async move {
                let _ = engine.emit(event).await;
            });
        }

        // === 尝试 2：升级后的 Roundtable 模式 ===
        squad.status = SquadStatus::Running;
        let verdict2 = Self::run_squad_once(&squad, &mut ralph, &deps).await;

        match verdict2 {
            RalphVerdict::Passed { ref result, .. } => {
                squad.status = SquadStatus::Complete;
                squad.result = Some(result.clone());
                if let Some(ref engine) = hook_engine {
                    let event = HookEvent::new(HookTrigger::OnRalphPass)
                        .with_task_id(&squad.task_id)
                        .with_squad_id(&squad.id)
                        .with_payload(serde_json::json!({
                            "squad_id": squad.id,
                            "retry_count": 1,
                            "result": result,
                        }));
                    let engine = engine.clone();
                    tokio::spawn(async move {
                        let _ = engine.emit(event).await;
                    });
                }
                let reflection2 = run_squad_reflection(squad_reflection, &verdict2, &squad).await;
                SquadResult {
                    squad_id: squad.id,
                    success: true,
                    result: squad.result,
                    retry_count: 1,
                    error: None,
                    pge_mode: squad.config.pge_mode,
                    reflection: reflection2,
                }
            }
            RalphVerdict::Unrecoverable { ref reason, .. } => {
                squad.status = SquadStatus::Failed;
                if let Some(ref engine) = hook_engine {
                    let event = HookEvent::new(HookTrigger::OnRalphUnrecoverable)
                        .with_task_id(&squad.task_id)
                        .with_squad_id(&squad.id)
                        .with_payload(serde_json::json!({
                            "squad_id": squad.id,
                            "retry_count": 1,
                            "reason": reason,
                        }));
                    let engine2 = engine.clone();
                    tokio::spawn(async move {
                        let _ = engine2.emit(event).await;
                    });

                    let handoff = HookEvent::new(HookTrigger::OnSquadRetry)
                        .with_task_id(&squad.task_id)
                        .with_squad_id(&squad.id)
                        .with_payload(serde_json::json!({
                            "squad_id": squad.id,
                            "retry_count": 1,
                            "handoff": true,
                            "reason": "Roundtable also failed",
                        }));
                    let engine2 = engine.clone();
                    tokio::spawn(async move {
                        let _ = engine2.emit(handoff).await;
                    });
                }
                let reflection2 = run_squad_reflection(squad_reflection, &verdict2, &squad).await;
                SquadResult {
                    squad_id: squad.id,
                    success: false,
                    result: None,
                    retry_count: 1,
                    error: Some(reason.clone()),
                    pge_mode: squad.config.pge_mode,
                    reflection: reflection2,
                }
            }
        }
    }
}

/// 从 RalphVerdict 构造 AgentSquadContribution 列表，供 SquadReflection 使用。
///
/// 评估结论必须回流为结构化 learnings/errors：只带 result 快照时，
/// 反思层第一步（收集个体学习）恒为空，squad 永远产出 0 learnings。
/// 判定非 Pass 时把评估反馈与评分细则转成 generator 的 Correction 学习；
/// 整个 loop 被判不可修复时记一条 ErrorEntry，供复发匹配与模式检测消费。
fn build_contributions_from_verdict(
    verdict: &RalphVerdict,
    squad_id: &str,
) -> Vec<cog_core::AgentSquadContribution> {
    let (snapshot, unrecoverable_reason) = match verdict {
        RalphVerdict::Passed { result, .. } => (result.clone(), None),
        RalphVerdict::Unrecoverable {
            reason, history, ..
        } => (
            history
                .last()
                .map(|h| h.snapshot.clone())
                .unwrap_or(serde_json::Value::Null),
            Some(reason.clone()),
        ),
    };

    let evaluation = extract_evaluation(&snapshot);
    let learnings = match unrecoverable_reason.as_deref() {
        // 终止原因是这一轮真正的结论，最后一条迭代的反馈只是过程。用过程
        // 当摘要会把死因写错：预算耗尽被记成"生成器没有产出"，于是整条
        // 自省链路照着错误的方向找病根。
        Some(reason) => vec![learning_from_termination(reason, evaluation.as_ref())],
        None => evaluation
            .as_ref()
            .map(learnings_from_evaluation)
            .unwrap_or_default(),
    };
    let errors = unrecoverable_reason
        .map(|reason| {
            vec![cog_core::ErrorEntry::new(
                cog_core::Priority::High,
                "ralph loop judged the goal unrecoverable",
                reason,
                format!("squad:{squad_id}"),
                "escalate to a human or reframe the goal",
            )]
        })
        .unwrap_or_default();

    // Pipeline 快照在顶层包含 plan / generation / evaluation
    let has_pipeline_shape = snapshot.get("plan").is_some() || snapshot.get("final_plan").is_some();

    if has_pipeline_shape {
        let plan = snapshot.get("final_plan").or(snapshot.get("plan")).cloned();
        let generation = snapshot
            .get("final_generation")
            .or(snapshot.get("generation"))
            .cloned();
        let evaluation_value = snapshot
            .get("final_evaluation")
            .or(snapshot.get("evaluation"))
            .cloned();

        vec![
            cog_core::AgentSquadContribution {
                agent_id: format!("{}:planner", squad_id),
                role: "planner".into(),
                learnings: Vec::new(),
                errors: Vec::new(),
                result: plan,
            },
            cog_core::AgentSquadContribution {
                agent_id: format!("{}:generator", squad_id),
                role: "generator".into(),
                learnings,
                errors,
                result: generation,
            },
            cog_core::AgentSquadContribution {
                agent_id: format!("{}:evaluator", squad_id),
                role: "evaluator".into(),
                learnings: Vec::new(),
                errors: Vec::new(),
                result: evaluation_value,
            },
        ]
    } else {
        // Roundtable 模式：所有角色共享同一个 roundtable 结果
        let rt = snapshot.get("roundtable").cloned().or(Some(snapshot));
        vec![
            cog_core::AgentSquadContribution {
                agent_id: format!("{}:planner", squad_id),
                role: "planner".into(),
                learnings: Vec::new(),
                errors: Vec::new(),
                result: rt.clone(),
            },
            cog_core::AgentSquadContribution {
                agent_id: format!("{}:generator", squad_id),
                role: "generator".into(),
                learnings,
                errors,
                result: rt.clone(),
            },
            cog_core::AgentSquadContribution {
                agent_id: format!("{}:evaluator", squad_id),
                role: "evaluator".into(),
                learnings: Vec::new(),
                errors: Vec::new(),
                result: rt,
            },
        ]
    }
}

/// 迭代快照与通过结果对评估结果的落点不同（`evaluation` / `final_evaluation`），
/// 两个键都试。解析失败按无评估处理，不让反思因快照形状变化而 panic。
fn extract_evaluation(
    snapshot: &serde_json::Value,
) -> Option<crate::squad::pge::types::EvaluationResult> {
    let raw = snapshot
        .get("final_evaluation")
        .or_else(|| snapshot.get("evaluation"))?;
    serde_json::from_value(raw.clone()).ok()
}

/// 非 Pass 判定 → 一条 Correction 学习：摘要取反馈，细节带全部评分细则，
/// 让复发匹配能按内容聚合同类失败。Pass 没有需要纠正的东西，不产出学习。
fn learnings_from_evaluation(
    evaluation: &crate::squad::pge::types::EvaluationResult,
) -> Vec<cog_core::Learning> {
    use crate::squad::pge::types::Verdict;

    if matches!(evaluation.verdict, Verdict::Pass) {
        return Vec::new();
    }
    let priority = match evaluation.verdict {
        Verdict::Fail => cog_core::Priority::High,
        _ => cog_core::Priority::Medium,
    };
    let criteria_detail = if evaluation.criteria.is_empty() {
        String::new()
    } else {
        let lines = evaluation
            .criteria
            .iter()
            .map(|c| format!("- {}: {}/100 — {}", c.name, c.score, c.comment))
            .collect::<Vec<_>>()
            .join("\n");
        format!("\ncriteria:\n{lines}")
    };
    let mut learning = cog_core::Learning::new(
        cog_core::LearningCategory::Correction,
        priority,
        cog_core::Area::Backend,
        evaluation.feedback.chars().take(200).collect::<String>(),
        format!(
            "{:?}:{}{}",
            evaluation.verdict, evaluation.feedback, criteria_detail
        ),
        &evaluation.feedback,
        cog_core::LearningSource::SelfReview,
    );
    learning.tags.push("squad-reflection".into());
    vec![learning]
}

/// 终止型判定（不可修复 / 预算耗尽 / 停滞 / 确定性环境失败）对应的学习：
/// 摘要与建议动作取**终止原因**，最后一条迭代的评分细则作为过程证据附在
/// 细节里。复发匹配按摘要聚合，摘要写错就等于把同类失败归错了类。
fn learning_from_termination(
    reason: &str,
    last_evaluation: Option<&crate::squad::pge::types::EvaluationResult>,
) -> cog_core::Learning {
    let context_detail = last_evaluation
        .filter(|e| !e.criteria.is_empty())
        .map(|e| {
            let lines = e
                .criteria
                .iter()
                .map(|c| format!("- {}: {}/100 — {}", c.name, c.score, c.comment))
                .collect::<Vec<_>>()
                .join("\n");
            format!("\nlast iteration criteria:\n{lines}")
        })
        .unwrap_or_default();
    let mut learning = cog_core::Learning::new(
        cog_core::LearningCategory::Correction,
        cog_core::Priority::High,
        cog_core::Area::Backend,
        reason.chars().take(200).collect::<String>(),
        format!("{reason}{context_detail}"),
        reason,
        cog_core::LearningSource::SelfReview,
    );
    learning.tags.push("squad-reflection".into());
    learning
}

/// 运行 Squad 级反思（若配置了 reflection 引擎）。
async fn run_squad_reflection(
    squad_reflection: &Option<Arc<dyn cog_core::SquadReflection>>,
    verdict: &RalphVerdict,
    squad: &Squad,
) -> Option<cog_core::SquadReflectionResult> {
    let reflection = squad_reflection.as_ref()?;
    let contributions = build_contributions_from_verdict(verdict, &squad.id);
    match reflection
        .reflect(&squad.id, &squad.task_id, &contributions, squad.retry_count)
        .await
    {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::warn!("Squad reflection failed for {}: {}", squad.id, e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::squad::pge::types::{Criterion, EvaluationResult};
    use crate::squad::ralph::ralph_loop::{RalphIteration, ResetStrategy};

    fn evaluation_json(verdict: &str, feedback: &str) -> serde_json::Value {
        serde_json::to_value(EvaluationResult {
            verdict: serde_json::from_value(serde_json::json!(verdict)).unwrap(),
            feedback: feedback.into(),
            score: Some(40),
            criteria: vec![Criterion {
                name: "tests pass".into(),
                score: 0,
                comment: "no test artifacts produced".into(),
            }],
            details: None,
        })
        .unwrap()
    }

    fn pipeline_snapshot(verdict: &str, feedback: &str) -> serde_json::Value {
        serde_json::json!({
            "plan": {"steps": []},
            "generation": {"content": "diff"},
            "evaluation": evaluation_json(verdict, feedback),
        })
    }

    #[test]
    fn failed_evaluation_becomes_generator_correction_learning() {
        let verdict = RalphVerdict::Unrecoverable {
            reason: "stagnation".into(),
            iterations: 3,
            history: vec![RalphIteration {
                iteration: 3,
                reset_strategy: ResetStrategy::Identical,
                pge_passed: false,
                feedback: "evaluator: criteria unmet".into(),
                snapshot: pipeline_snapshot("fail", "evaluator: criteria unmet"),
                progress: None,
            }],
        };
        let contributions = build_contributions_from_verdict(&verdict, "squad-x");
        let generator = contributions
            .iter()
            .find(|c| c.role == "generator")
            .expect("generator contribution");
        assert_eq!(
            generator.learnings.len(),
            1,
            "failed verdict must yield a learning"
        );
        let learning = &generator.learnings[0];
        assert!(matches!(
            learning.category,
            cog_core::LearningCategory::Correction
        ));
        assert!(matches!(learning.priority, cog_core::Priority::High));
        assert!(learning.details.contains("tests pass"));
        assert_eq!(
            generator.errors.len(),
            1,
            "unrecoverable verdict must yield an error entry"
        );
        assert_eq!(generator.errors[0].error_message, "stagnation");
    }

    #[test]
    fn passed_verdict_yields_no_learnings_or_errors() {
        let verdict = RalphVerdict::Passed {
            result: serde_json::json!({
                "final_plan": {"steps": []},
                "final_generation": {"content": "ok"},
                "final_evaluation": evaluation_json("pass", "looks good"),
            }),
            iterations: 1,
            history: vec![],
        };
        let contributions = build_contributions_from_verdict(&verdict, "squad-y");
        for c in &contributions {
            assert!(c.learnings.is_empty(), "pass must not invent learnings");
            assert!(c.errors.is_empty(), "pass must not invent errors");
        }
    }

    #[test]
    fn roundtable_snapshot_also_feeds_learnings() {
        let verdict = RalphVerdict::Unrecoverable {
            reason: "max iterations".into(),
            iterations: 50,
            history: vec![RalphIteration {
                iteration: 50,
                reset_strategy: ResetStrategy::Identical,
                pge_passed: false,
                feedback: "roundtable rejected".into(),
                snapshot: serde_json::json!({
                    "roundtable": {"transcript": []},
                    "evaluation": evaluation_json("partial", "roundtable rejected"),
                }),
                progress: None,
            }],
        };
        let contributions = build_contributions_from_verdict(&verdict, "squad-z");
        let generator = contributions
            .iter()
            .find(|c| c.role == "generator")
            .expect("generator contribution");
        // 摘要取自终止原因，不是最后一轮的反馈；第二轮桌的快照形状（无 plan、
        // 只有 roundtable + evaluation）仍然要能贡献评分细则。
        assert_eq!(generator.learnings.len(), 1);
        assert_eq!(generator.learnings[0].summary, "max iterations");
        assert!(matches!(
            generator.learnings[0].priority,
            cog_core::Priority::High
        ));
        assert!(generator.learnings[0].details.contains("tests pass"));
    }

    #[test]
    fn malformed_evaluation_snapshot_still_yields_termination_learning() {
        let verdict = RalphVerdict::Unrecoverable {
            reason: "broken".into(),
            iterations: 1,
            history: vec![RalphIteration {
                iteration: 1,
                reset_strategy: ResetStrategy::Identical,
                pge_passed: false,
                feedback: String::new(),
                snapshot: serde_json::json!({"plan": {}, "evaluation": "not-an-object"}),
                progress: None,
            }],
        };
        let contributions = build_contributions_from_verdict(&verdict, "squad-w");
        let generator = contributions
            .iter()
            .find(|c| c.role == "generator")
            .expect("generator contribution");
        // 终止原因是这一轮真正的结论，与快照能否解析无关：解析失败只让细则
        // 为空，摘要仍如实写"broken"。若摘要转而依赖快照，这类记录就会退回
        // 到用过程冒充死因的老毛病。
        assert_eq!(generator.learnings.len(), 1);
        assert_eq!(generator.learnings[0].summary, "broken");
        assert_eq!(generator.learnings[0].details, "broken");
        assert_eq!(generator.errors.len(), 1);
    }
}
