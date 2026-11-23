use std::sync::Arc;

use cog_core::{Agent, KnowledgeBackend, Task};

use crate::squad::pge::types::PlannerOutput;

/// 把一句随数据走的说明接到契约上：已经有了就接在后面，没有就自成一段。
///
/// 接而不是替，是因为这段说明与 operator 的 schema、prompt skill 各自独立：
/// 它们说的是答案的形状，这一句说的是这一份数据是什么。
// prompt: not-an-instruction — 本体只是拼接：文字全部由调用方给，这里没有一个字是写给模型的
fn append_instruction(contract: &mut serde_json::Value, note: &str) {
    let text = match contract["instructions"].as_str() {
        Some(existing) => format!("{existing} {note}"),
        None => note.to_string(),
    };
    contract["instructions"] = serde_json::json!(text);
}

/// The files a rework hand-off named, and which way the next attempt has to read
/// them.
///
/// Read from the payload of the task the submit flow queues when it hands a
/// refused change back: `hand_off_change_rework` (in `cog-reflection`) builds a
/// `change_rework` task and writes `named_files` — the paths that gate's verdict
/// carried — and `rejection_cause` into it. Read here because this is the actor
/// that turns a request into the paths the change is held to. An empty list and
/// an absent field are the same case — a hand-off that named nothing has not
/// told the planner anything it did not already know, and a `required_targets`
/// of zero entries would ask the model to name nothing.
fn rework_named_files(task: &Task) -> Option<RefusedFiles> {
    let files: Vec<String> = task
        .input
        .get("named_files")?
        .as_array()?
        .iter()
        .filter_map(|f| f.as_str())
        .filter(|f| !f.trim().is_empty())
        .map(|f| f.to_string())
        .collect();
    if files.is_empty() {
        return None;
    }
    Some(
        match task.input.get("rejection_cause").and_then(|c| c.as_str()) {
            Some(cause) if cause == cog_core::RejectionCause::ForbiddenPath.as_str() => {
                RefusedFiles::OutOfReach(files)
            }
            _ => RefusedFiles::Targets(files),
        },
    )
}

/// What the refusing gate's file list is to the plan that follows.
///
/// Almost every refusal names where the defect is, and the plan is held to
/// those files. A path refusal is the one that inverts: it names the files
/// *because this instance may not write to them*, so a plan that touches any of
/// them is refused again before it is ever compiled. The hand-off that builds
/// this task already words its goal that way; without this split the planner
/// undid that wording one step later, telling the generator that every refused
/// path had to appear in its targets — which is how a rework of a path refusal
/// spends its whole budget re-deriving a verdict that was already in hand.
enum RefusedFiles {
    /// The defect is in these files, so the change has to touch them.
    Targets(Vec<String>),
    /// These are the files this instance may not contribute to at all.
    OutOfReach(Vec<String>),
}

/// 上一轮尝试的记录，Planner 在重试时照原样读回。
///
/// 这四格一起出现、一起去（重试要么带着上一轮的判词与分数，要么什么都不带），
/// 所以它们是一项输入而不是四项：分开传时每加一格都要再动一次签名，而签名宽到
/// 某一步以后，读的人已经看不出哪些参数是一起的。这里的名字与
/// `cog_core::TaskContextInput` 的字段一一对应，摆在一起就是它被填进去的样子。
struct PreviousAttempt<'a> {
    feedback: Option<&'a str>,
    score: Option<u32>,
    generation: Option<&'a serde_json::Value>,
    context_board: Option<&'a serde_json::Value>,
}

/// Planner Actor — semantic wrapper around a `dyn Agent` created via
/// [`AgentManager`](cog_core::AgentManager).
///
/// Responsible for:
/// 1. Querying historical decomposition patterns from [`KnowledgeBackend`].
/// 2. Constructing Planner context.
/// 3. Invoking the underlying agent and parsing strict-schema output.
#[derive(Clone)]
pub struct PlannerActor {
    agent: Arc<dyn Agent>,
    knowledge: Option<Arc<dyn KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    output_schema: Option<serde_json::Value>,
    prompt_skill: Option<cog_core::PromptSkillDef>,
    context_builder: Option<Arc<dyn cog_core::TaskContextBuilder>>,
    /// 还没被派生成任务的意图。planner 的那一次调用把它们一起读进来判——哪几条只是
    /// 回答、哪几条是活，都在这一次里定，不另买一次调用的 token。
    intent_reader: Option<Arc<dyn cog_core::IntentReader>>,
}

impl PlannerActor {
    pub fn new(agent: Arc<dyn Agent>) -> Self {
        Self {
            agent,
            knowledge: None,
            self_review: None,
            output_schema: None,
            prompt_skill: None,
            context_builder: None,
            intent_reader: None,
        }
    }

    /// 装上意图的读面：这一批还没派生的意图会随计划的请求一起交进去。
    pub fn with_intent_reader(mut self, reader: Arc<dyn cog_core::IntentReader>) -> Self {
        self.intent_reader = Some(reader);
        self
    }

    pub fn with_knowledge(mut self, knowledge: Arc<dyn KnowledgeBackend>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Attach a JSON Schema constraining the planner output. When set, the
    /// schema is injected into the prompt context (taking precedence over
    /// the built-in self-evolution schema) and the raw LLM output is
    /// validated against it; failures are logged and lenient parsing applies.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Attach a prompt skill (SKILL.md 模板 + 可选 schema 指导)。
    /// 算子显式配置的 output_schema 优先于 skill 声明的 schema。
    pub fn with_prompt_skill(mut self, skill: cog_core::PromptSkillDef) -> Self {
        self.prompt_skill = Some(skill);
        self
    }

    /// Override the prompt context builder (defaults to
    /// [`crate::actors::StandardTaskContextBuilder`]).
    pub fn with_context_builder(mut self, builder: Arc<dyn cog_core::TaskContextBuilder>) -> Self {
        self.context_builder = Some(builder);
        self
    }

    /// Run the Planner phase: produce a structured plan and sub-tasks.
    pub async fn plan(
        &self,
        task: &Task,
        attempt: u32,
        previous_feedback: Option<&str>,
        previous_score: Option<u32>,
        previous_generation: Option<&serde_json::Value>,
        context_board: Option<&serde_json::Value>,
    ) -> PlannerOutput {
        self.plan_inner(
            task,
            attempt,
            PreviousAttempt {
                feedback: previous_feedback,
                score: previous_score,
                generation: previous_generation,
                context_board,
            },
            false,
        )
        .await
    }

    /// Run the Planner phase for a caller that will judge the plan with a
    /// program criterion and hand the verdict back to the next round.
    ///
    /// The decomposition loop reads the plan it just parsed against
    /// `crate::squad::plan::judge_plan` — a structural gate over
    /// the same bytes the self-review would be asked to read — and its refusal
    /// is the next round's prompt. Buying an LLM review there is a second judge
    /// for one artifact, and its rewritten text would be re-judged by the
    /// program gate anyway. The skip is counted under its own reason rather
    /// than `disabled`: nothing was turned off, a gate that already exists took
    /// the judgement over.
    pub async fn plan_judged_downstream(
        &self,
        task: &Task,
        attempt: u32,
        previous_feedback: Option<&str>,
        previous_score: Option<u32>,
        previous_generation: Option<&serde_json::Value>,
        context_board: Option<&serde_json::Value>,
    ) -> PlannerOutput {
        self.plan_inner(
            task,
            attempt,
            PreviousAttempt {
                feedback: previous_feedback,
                score: previous_score,
                generation: previous_generation,
                context_board,
            },
            true,
        )
        .await
    }

    // The seven are one round's inputs, and they line up positionally with the
    // public entry points that forward them; a struct here would split the same
    // set into a type whose only job is to be taken apart again one line later.
    #[allow(clippy::too_many_arguments)]
    // prompt: contract — enforced by crates/cog-collaboration/tests/actor_prompt_contract_test.rs::every_stable_half_carries_the_answer_shape_and_nothing_else
    async fn plan_inner(
        &self,
        task: &Task,
        attempt: u32,
        previous: PreviousAttempt<'_>,
        program_judged: bool,
    ) -> PlannerOutput {
        let goal = task
            .input
            .get("goal")
            .and_then(|g| g.as_str())
            .unwrap_or("");

        let default_builder;
        let builder: &dyn cog_core::TaskContextBuilder = match self.context_builder.as_ref() {
            Some(b) => b.as_ref(),
            None => {
                default_builder = crate::actors::StandardTaskContextBuilder;
                &default_builder
            }
        };
        let mut ctx = cog_core::TaskContextBuilder::build(
            builder,
            cog_core::PgeRole::Planner,
            &cog_core::TaskContextInput {
                task: Some(task),
                attempt,
                generation: previous.generation,
                previous_feedback: previous.feedback,
                previous_score: previous.score,
                context_board: previous.context_board,
                ..Default::default()
            },
        );

        // 还没派生的意图随这一次请求一起进去：它们是意图这一侧的输入，而「哪几条只是
        // 回答、哪几条是活」正由这一次调用一并判。读不到就说出来——把读失败读成「没有
        // 意图」，会让每一次计划都在没有输入的情况下启动。
        if let Some(reader) = self.intent_reader.as_ref() {
            match reader.undispatched(i64::MAX).await {
                Ok(rows) if !rows.is_empty() => {
                    let list: Vec<serde_json::Value> = rows
                        .iter()
                        .map(|r| {
                            serde_json::json!({
                                "source": r.source,
                                "external_id": r.external_id,
                                "text": r.text,
                                "parent": r.parent,
                            })
                        })
                        .collect();
                    ctx["intents"] = serde_json::json!(list);
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "intents could not be read; the plan has none of them")
                }
            }
        }

        // The stable half: the answer contract (this role's instructions, output
        // schema, format). None of it depends on this attempt, so it must be
        // assembled apart from `ctx` — flattened into one map, key order sorts
        // `attempt` to the front and the contract never reaches the cacheable
        // prefix. See `actor_input`.
        let mut contract = serde_json::json!({});

        // The stable half carries the answer's shape, not a rulebook for arriving
        // at it. What the plan has to satisfy structurally — a leaf that says how
        // it is done, one end the whole goal converges on, dependencies that name
        // sub-tasks of this plan, unique ids — is judged by `squad::plan::check_plan`
        // and the defects come back named in the next attempt; what coverage the
        // targets get is judged on the change and comes back as per-target
        // criteria. Restating either here would be a second copy of the judge,
        // one that cannot go red and drifts from it.
        let is_self_evolution = task.is_self_evolution();

        if is_self_evolution {
            contract[cog_core::INPUT_EVOLUTION_MODE] =
                serde_json::json!(cog_core::EVOLUTION_MODE_GENERATE_CHANGE);
            contract["response_format"] = serde_json::json!("json");
            contract["output_schema"] = serde_json::json!({
                "summary": "string: concise plan summary",
                "plan": "object: structured plan details (may be empty for self-evolution execution)",
                "sub_tasks": "array: empty for self-evolution execution, otherwise TaskSpec objects",
                "targets": "array of strings: repository-relative paths the change must touch"
            });
        } else if self.output_schema.is_none() && self.prompt_skill.is_none() {
            // Built-in contract for standard goal decomposition. Lowest
            // precedence: operator schema > prompt skill > built-in.
            contract["response_format"] = serde_json::json!("json");
            contract["output_schema"] = serde_json::json!({
                "summary": "string: one-sentence summary of the plan",
                "plan": {"approach": "string", "steps": ["string"]},
                "sub_tasks": [{
                    "id": "string: unique task id like t1, t2",
                    "name": "string: short task name",
                    "task_type": "string: the id of the executor this sub-task should be handed to, from task.input.executors, or a generic executor type if none of them fits",
                    "input": {"query": "string: everything the executor needs to accomplish this task"},
                    "blocked_by": ["string: ids of tasks that must finish first, empty if none"],
                    "acceptance": ["string: this sub-task's own criteria, each checkable true/false"],
                    "delivers": "string: what this sub-task hands back, one of plan|change|judgement|answer|work_product"
                }],
                "acceptance_criteria": ["string: one verifiable criterion per entry; each must be checkable as true/false against the final output"]
            });
        }

        // A configured output schema takes precedence over the built-in
        // self-evolution schema: operators own the contract.
        if let Some(ref schema) = self.output_schema {
            contract["output_schema"] = schema.clone();
            contract["response_format"] = serde_json::json!("json");
        }

        // Prompt skill（SKILL.md 模板 + schema 指导）：算子 schema 优先于 skill schema。
        if let Some(ref skill) = self.prompt_skill {
            crate::actors::apply_prompt_skill(&mut contract, skill, self.output_schema.as_ref());
        }

        // 上一次尝试被门禁拒绝时，判词已经点名了它反对的文件，而这条再生成任务把它
        // 带在载荷里（`named_files`，由 `hand_off_change_rework` 写入）。此前没有任何
        // 读者：plan 又从检出里重新找了一遍文件，而答案就在请求里。把它交给 plan，
        // targets 就是那张表——生成侧的契约已经写着「你点名的路径而 diff 没碰，判退」，
        // 所以这一条不需要新判据，只是把已有的那一条接上。这一句说明是随数据走的
        // 插头：字段名自己说不出「必须碰它」，而这条要求是这道门唯一会判的东西。
        //
        // 路径判退要反过来读：它点名的正是这一实例碰不得的路径（见 [`RefusedFiles`]）。
        // 那里的清单不进 `required_targets`，而是写成一段禁令——照着原样交给 plan，
        // 等于要求一次注定被同一道门拒掉的尝试。
        //
        // 这份清单整条任务不变，所以它进稳定半边，与其余答案契约一起被缓存；
        // 没有这个字段的任务（每一个不是回流的自进化任务）字节不变。
        match rework_named_files(task) {
            Some(RefusedFiles::Targets(files)) => {
                contract["required_targets"] = serde_json::json!(files);
                let note = "required_targets names the repository-relative paths the check that \
                            refused the previous attempt objected to. Every one of them must \
                            appear in your targets: the defect is in those files, so the change \
                            has to touch them. Do not re-derive the list by searching the \
                            checkout -- it is given.";
                append_instruction(&mut contract, note);
            }
            Some(RefusedFiles::OutOfReach(files)) => {
                append_instruction(
                    &mut contract,
                    &format!(
                        "The check that refused the previous attempt named {named}. Those paths \
                         are outside the surface this instance may contribute to at all \
                         ({surfaces}), so a change that touches any of them is refused before it \
                         is compiled. Meet the goal from inside that surface, and leave those \
                         paths out of your targets entirely.",
                        named = files.join(", "),
                        surfaces = cog_core::CONTRIBUTABLE_SURFACES,
                    ),
                );
            }
            None => {}
        }

        // Inject historical decomposition patterns if knowledge backend is wired.
        if let Some(ref k) = self.knowledge {
            // The class, not the goal, is what the entries are keyed on, and it
            // is read from the goal's own carrier: the decomposition loop hands
            // the planner a synthetic task that holds nothing but the goal text,
            // so a class taken from this task's type would name the loop instead
            // of the work and aggregate every goal under one row.
            let class = cog_core::GoalClass::of(task);
            crate::observable::global_observable().record_goal_class_source(class.source.as_str());
            match k
                .retrieve_similar_decompositions(&class.value, goal, 3)
                .await
            {
                Ok(patterns) if !patterns.is_empty() => {
                    ctx["historical_decompositions"] = serde_json::json!(patterns);
                }
                Err(e) => {
                    tracing::warn!("Planner knowledge query failed: {}", e);
                }
                _ => {}
            }
        }

        let input = crate::actors::actor_input(task, contract, ctx);

        let mut output = match self.agent.prompt_for_task(&task.id, input).await {
            Ok(result) => {
                // 校验 schema：算子显式配置优先，否则用 skill 声明的 schema（均仅告警）。
                let effective_schema = self.output_schema.as_ref().or_else(|| {
                    self.prompt_skill
                        .as_ref()
                        .and_then(|s| s.output_schema.as_ref())
                });
                if let Some(schema) = effective_schema {
                    crate::actors::validate_against_schema(
                        schema,
                        &result.to_string(),
                        cog_core::roles::PLANNER,
                    );
                }
                crate::squad::pge::parse_planner_output(&result, goal)
            }
            Err(e) => {
                tracing::warn!("Planner prompt failed: {}", e);
                // 没到上游的 prompt 不是 planner 判断"无事可做"。空计划在下游
                // 与真计划完全同形，吞掉错误就等于把一次传输失败静默降级成一个
                // 合法的空任务集，整条链照常往下跑。把真因带在 content 里：线格
                // 上的标记让外层把它认成终止性环境失败，反馈与学习链指向真实
                // 原因，而不是一个根本不存在的 planner 缺陷。
                PlannerOutput {
                    summary: format!("Planner prompt did not reach its upstream: {e}"),
                    plan: serde_json::Value::String(
                        cog_core::contract::outcome::environment_error_content(&e),
                    ),
                    sub_tasks: Vec::new(),
                    acceptance_criteria: Vec::new(),
                    targets: Vec::new(),
                    items: Vec::new(),
                }
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        // 上游已经失败时不能再走自审：那会为同一个不可用的上游再买一次调用，
        // 而且改写的输出会把上面带下来的真因覆盖掉，降级重新变得无声。理由
        // 交给自审漏斗，于是「这次没审」和「这次审了」一样有读数。
        //
        // 自进化任务的 plan 也不审，与生成侧同一条理由（`generator.rs` 对
        // `is_self_evolution` 的跳过）：这条路上要的是能被下游确定性解析的 JSON
        // 形状，plan 在到这里之前已经解析过，而只会说人话的模型会让改写的自审
        // 跑到超时才返回——买到的是一段改不动解析结果的散文，代价是一整个超时。
        // 跳过用同一个具名理由，省下的调用因此是读数而不是缺席。
        //
        // 第三种：调用方自己会用程序判据判这份计划（分解循环就是这样，判词还会
        // 回到下一轮）。那时自审是给同一个产物买第二个判官，理由同样具名。
        let review_basis = crate::actors::review_basis(
            task,
            crate::actors::ReviewFacts {
                upstream_failed: output.is_terminal_env_failure(),
                self_evolution: is_self_evolution,
                program_judged,
            },
            &[],
        );
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            cog_core::roles::PLANNER,
            review_basis,
            &task.id,
        )
        .await
        {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                output = crate::squad::pge::parse_planner_output(&value, goal);
            }
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A task shaped the way the submit flow queues one: an id, the rework task
    /// kind, and the fields the gate's verdict left behind.
    fn rework_task(input: serde_json::Value) -> Task {
        Task::new(
            "rework-change-1",
            cog_core::TaskType::Custom("change_rework".into()),
            input,
        )
    }

    /// A hand-off that named nothing has told the planner nothing: an absent
    /// field and a list with no paths in it are the same case, and a
    /// `required_targets` of zero entries would ask the model to name nothing.
    #[test]
    fn a_hand_off_that_named_no_files_reads_as_nothing() {
        assert!(
            rework_named_files(&rework_task(serde_json::json!({}))).is_none(),
            "a task with no `named_files` was read as a file list"
        );
        assert!(
            rework_named_files(&rework_task(serde_json::json!({"named_files": []}))).is_none(),
            "an empty file list was read as a file list"
        );
        assert!(
            rework_named_files(&rework_task(
                serde_json::json!({"named_files": ["", "   "], "rejection_cause": "apply_failed"})
            ))
            .is_none(),
            "entries that hold no path were read as paths"
        );
    }

    /// Every cause but a path refusal names where the defect is, so the plan is
    /// held to those files.
    #[test]
    fn a_defect_refusal_reads_as_targets() {
        let named = rework_named_files(&rework_task(serde_json::json!({
            "named_files": ["crates/cogneva/src/main.rs", "", "crates/cog-core/src/lib.rs"],
            "rejection_cause": cog_core::RejectionCause::LintIntroduced.as_str(),
        })))
        .expect("a hand-off that named files was read as naming nothing");
        match named {
            RefusedFiles::Targets(files) => assert_eq!(
                files,
                vec![
                    "crates/cogneva/src/main.rs".to_string(),
                    "crates/cog-core/src/lib.rs".to_string()
                ],
                "the named files were not read as the paths the change has to touch"
            ),
            RefusedFiles::OutOfReach(files) => {
                panic!("a defect refusal was read as paths to stay out of: {files:?}")
            }
        }

        // The cause is not what makes a list a target list: a hand-off that
        // carries no cause at all is still a defect refusal.
        let without_cause =
            rework_named_files(&rework_task(serde_json::json!({"named_files": ["a/b.rs"]})))
                .expect("a hand-off that named files was read as naming nothing");
        match without_cause {
            RefusedFiles::Targets(files) => assert_eq!(files, vec!["a/b.rs".to_string()]),
            RefusedFiles::OutOfReach(files) => {
                panic!("a refusal with no cause was read as a path refusal: {files:?}")
            }
        }
    }

    /// A path refusal inverts its list: those paths are the ones this instance
    /// may not write, so they must never become `required_targets`.
    #[test]
    fn a_path_refusal_reads_as_out_of_reach() {
        let named = rework_named_files(&rework_task(serde_json::json!({
            "named_files": ["crates/change_ledger/tests/principle_ledger.rs"],
            "rejection_cause": cog_core::RejectionCause::ForbiddenPath.as_str(),
        })))
        .expect("a hand-off that named files was read as naming nothing");
        match named {
            RefusedFiles::OutOfReach(files) => assert_eq!(
                files,
                vec!["crates/change_ledger/tests/principle_ledger.rs".to_string()],
                "the refused paths were not read as the ones to stay out of"
            ),
            RefusedFiles::Targets(files) => panic!(
                "a path this instance may not write was read as a required target: {files:?}"
            ),
        }
    }
}
