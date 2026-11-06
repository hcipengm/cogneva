use std::sync::Arc;

use cog_core::{Agent, KnowledgeBackend, Task};

use crate::squad::pge::types::GeneratorOutput;

/// Context from a previous generation attempt, fed back into repair
/// iterations so the Generator fixes its own output instead of starting over.
#[derive(Debug, Clone, Copy, Default)]
pub struct PreviousAttempt<'a> {
    /// Last evaluation (verdict/feedback/score), if any.
    pub evaluation: Option<&'a serde_json::Value>,
    /// Last generation output, if any.
    pub generation: Option<&'a serde_json::Value>,
    /// Evaluator feedback targeted at repair, if any.
    pub repair_feedback: Option<&'a str>,
}

/// Generator Actor — semantic wrapper around a `dyn Agent` created via
/// [`AgentManager`](cog_core::AgentManager).
///
/// Responsible for:
/// 1. Querying historical implementations from [`KnowledgeBackend`].
/// 2. Constructing Generator context.
/// 3. Invoking the underlying agent and parsing strict-schema output.
#[derive(Clone)]
pub struct GeneratorActor {
    agent: Arc<dyn Agent>,
    knowledge: Option<Arc<dyn KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
    output_schema: Option<serde_json::Value>,
    prompt_skill: Option<cog_core::PromptSkillDef>,
    context_builder: Option<Arc<dyn cog_core::TaskContextBuilder>>,
}

impl GeneratorActor {
    pub fn new(agent: Arc<dyn Agent>) -> Self {
        Self {
            agent,
            knowledge: None,
            self_review: None,
            output_schema: None,
            prompt_skill: None,
            context_builder: None,
        }
    }

    pub fn with_knowledge(mut self, knowledge: Arc<dyn KnowledgeBackend>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Attach a JSON Schema constraining the generator output. When set, the
    /// schema is injected into the prompt context and the raw LLM output is
    /// validated against it; failures are logged and lenient parsing applies.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Attach a prompt skill（算子显式 output_schema 优先于 skill schema）。
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

    /// Run the Generator phase: execute the plan and produce artifacts.
    pub async fn generate(
        &self,
        task: &Task,
        plan: &serde_json::Value,
        attempt: u32,
        previous: PreviousAttempt<'_>,
        context_board: Option<&serde_json::Value>,
    ) -> GeneratorOutput {
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
            cog_core::PgeRole::Generator,
            &cog_core::TaskContextInput {
                task: Some(task),
                attempt,
                plan: Some(plan),
                generation: previous.generation,
                previous_evaluation: previous.evaluation,
                repair_feedback: previous.repair_feedback,
                context_board,
                ..Default::default()
            },
        );

        // The stable half: the answer contract. The change-format contract is the
        // heaviest part of it here (it spells out how the gate will judge the
        // diff), and it is identical byte for byte across attempts and across
        // tasks — yet flattened into the same map as `attempt` it never reached
        // the cacheable prefix. See `actor_input`.
        let mut contract = serde_json::json!({});

        // Inject self-evolution change-generation instructions when requested.
        let is_self_evolution = task.is_self_evolution();

        if is_self_evolution {
            contract["change_generation"] = change_generation_contract();
        } else if self.output_schema.is_none() && self.prompt_skill.is_none() {
            // Built-in contract for standard execution. Lowest precedence:
            // operator schema > prompt skill > built-in.
            contract["response_format"] = serde_json::json!("json");
            contract["output_schema"] = serde_json::json!({
                "content": "string: the produced result — the actual answer or deliverable for the goal",
                "artifacts": [{"name": "string", "content": "string", "artifact_type": "string"}]
            });
            contract["instructions"] = serde_json::json!(
                "Execute the plan in context.plan against the goal in context.goal and produce the deliverable. \
                 Put the primary result in content (the real answer, not a description of what you would do). \
                 Use artifacts only for named files/deliverables; an empty array is fine. \
                 Emit ONLY a single JSON object matching output_schema. No markdown, no code fences, no commentary."
            );
        }

        // A configured output schema takes precedence over built-in prompt
        // contracts: operators own the contract.
        if let Some(ref schema) = self.output_schema {
            contract["output_schema"] = schema.clone();
            contract["response_format"] = serde_json::json!("json");
        }

        // Prompt skill（SKILL.md 模板 + schema 指导）：算子 schema 优先于 skill schema。
        if let Some(ref skill) = self.prompt_skill {
            crate::actors::apply_prompt_skill(&mut contract, skill, self.output_schema.as_ref());
        }

        // 项目自己的原则，随任务带进来。集群内的生成端此前看不见任何一条：事后
        // 有形状门，事前只有这份手写的变更格式契约。这一次该看哪几条由提交方说了
        // 算——只有它知道这条改动的面与要动哪些路径——所以这里不挑，只如实摆放；
        // 任务没带就是不摆，不补一段泛泛的提醒（那种提醒读起来像原则，实际不指着
        // 任何一条，也就没有一条会在它被违反时变红）。
        //
        // 进的是稳定半边：同一件任务的重试之间这一份逐字节相同，与变更格式契约
        // 一样该落在可缓存的那一半里，而不是每一轮重新付一遍。
        if let Some(block) = principles_block(&task.input) {
            contract["principles"] = block;
        }

        // Inject reference implementations if knowledge backend is wired.
        if let Some(ref k) = self.knowledge {
            let task_type = format!("{:?}", task.task_type);
            let input_summary = serde_json::to_string(&task.input).unwrap_or_default();
            match k
                .retrieve_similar_implementations(&task_type, &input_summary, 3)
                .await
            {
                Ok(examples) if !examples.is_empty() => {
                    ctx["reference_implementations"] = serde_json::json!(examples);
                }
                Err(e) => {
                    tracing::warn!("Generator knowledge query failed: {}", e);
                }
                _ => {}
            }
        }

        let input = crate::actors::actor_input(task, contract, ctx);
        // 自进化的一轮要在收尾时问工作树「它改了什么」，问不到就得让这一轮
        // 重来一次；重来的请求是这一次的请求加一句，所以留一份。别的角色不留。
        let repair_base = is_self_evolution.then(|| input.clone());

        let (mut output, review_basis) = match self.agent.prompt_for_task(&task.id, input).await {
            Ok(result) => {
                let effective_schema = self.output_schema.as_ref().or_else(|| {
                    self.prompt_skill
                        .as_ref()
                        .and_then(|s| s.output_schema.as_ref())
                });
                if let Some(schema) = effective_schema {
                    crate::actors::validate_against_schema(
                        schema,
                        &result.to_string(),
                        cog_core::roles::GENERATOR,
                    );
                }
                let output = crate::squad::pge::parse_generator_output(&result);
                // An output whose own content names a deterministic cause is not
                // the generator's answer: it is either this actor's fallback or a
                // spent iteration budget, and the pipeline reads it as terminal
                // and stops. Grading that placeholder would buy a review of a
                // sentence nobody wrote in answer to the task, so it is skipped
                // under the same named reason the planner uses — the calls this
                // saves stay a reading rather than an absence.
                let basis = crate::actors::review_basis(
                    task,
                    crate::actors::ReviewFacts {
                        upstream_failed: output.is_terminal_env_failure(),
                        self_evolution: is_self_evolution,
                        ..Default::default()
                    },
                    &[],
                );
                (output, basis)
            }
            Err(e) => {
                tracing::warn!("Generator prompt failed: {}", e);
                // A prompt that never reached its upstream is not the generator
                // choosing to produce nothing. Carry the real error through
                // content: the in-band token keeps is_terminal_env_failure true,
                // so the pipeline still stops instead of paying for identical
                // retries, while feedback and the learning chain name the actual
                // cause instead of a generator defect that is not there.
                (
                    GeneratorOutput {
                        content: serde_json::Value::String(
                            cog_core::contract::outcome::environment_error_content(&e),
                        ),
                        artifacts: Vec::new(),
                    },
                    crate::actors::ReviewBasis::Passed(
                        crate::observable::SELF_REVIEW_DECIDED_BY_UPSTREAM_UNAVAILABLE,
                    ),
                )
            }
        };
        let output_str = serde_json::to_string_pretty(&output).unwrap_or_default();
        if let Some(revised) = crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            cog_core::roles::GENERATOR,
            review_basis,
            &task.id,
        )
        .await
        {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&revised) {
                output = crate::squad::pge::parse_generator_output(&value);
            }
        }
        // Last, so it also covers a revision the self-review wrote: whatever
        // artifact the round ends up with is the one the tree is asked about.
        if is_self_evolution {
            let (out, source, checkout_read) =
                prefer_workspace_change(self.agent.as_ref(), &task.id, output).await;
            output = match (source, repair_base) {
                // 只有「检出读得到且读回来没有改动」才重问。一个根本读不到的
                // 检出（这一轮没有工作区、读失败）与一个没有改动的检出，在产物
                // 来源上同形——都退回模型给的 diff。前者的成因是这里没有树可写，
                // 再问一次仍然没有树可写，只会白付一通生成。
                (Some(crate::observable::CHANGE_DIFF_SOURCE_MODEL), Some(base))
                    if checkout_read =>
                {
                    self.repair_untouched_checkout(task, base, out).await
                }
                _ => out,
            };
        }
        output
    }

    /// 树没动的时候，让这一轮把改动真的写进检出，而不是把 diff 写在答复里。
    ///
    /// 生成端拿到文件工具也可以一次都不调用：它读几个文件，然后把一份凭记忆
    /// 手写的 unified diff 当成产物交上来。手写的 hunk 计数与上下文行对不上，
    /// `git apply --check` 就挂，这一轮的生成、评审、执行全部白付。从树里取
    /// 改动的那条路没有这个问题——树是它自己写的。
    ///
    /// 所以只在这一个条件下重问一次：这一轮**产出了变更产物**、工作树读得到、
    /// 而读回来没有改动。重问的话只说一件它能做的事——把改动写进检出，然后从
    /// `git diff` 取产物。第二次也没动树的，仍然收第一份答复：这里只补一次机会，
    /// 不改判据，也不把一轮答复换成一无所有。
    async fn repair_untouched_checkout(
        &self,
        task: &Task,
        base: serde_json::Value,
        first: GeneratorOutput,
    ) -> GeneratorOutput {
        let repair = checkout_repair_request(base);
        let result = match self.agent.prompt_for_task(&task.id, repair).await {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(
                    task_id = %task.id,
                    error = %e,
                    "the round's checkout was untouched and asking again failed; keeping the answer it gave"
                );
                return first;
            }
        };
        let second = crate::squad::pge::parse_generator_output(&result);
        if !has_change_artifact(&second) {
            tracing::warn!(
                task_id = %task.id,
                "the round's checkout was untouched and the second answer carried no change; keeping the first"
            );
            return first;
        }
        // Whether the re-read found a checkout is not asked here: this branch is
        // reached only because a checkout was readable a moment ago, and the
        // question it settles is where the second answer's diff came from.
        let (out, source, _) = prefer_workspace_change(self.agent.as_ref(), &task.id, second).await;
        if source == Some(crate::observable::CHANGE_DIFF_SOURCE_TREE) {
            return out;
        }
        tracing::warn!(
            task_id = %task.id,
            "the round's checkout was still untouched after asking again; keeping the first answer"
        );
        first
    }
}

/// 重问时要加进请求的那一句话，以及它落在请求的哪个位置。
///
/// 落在 `context` 里而不是契约里：契约是逐字节相同的那一半，随每次尝试变化的
/// 东西放进去就不再可缓存。这一句只属于这一轮。
fn checkout_repair_request(mut base: serde_json::Value) -> serde_json::Value {
    let sentence = serde_json::Value::String(
        "Your checkout is unchanged: this round answered with a diff it wrote from memory, \
         and only the checkout is read back -- a diff in the answer is not applied for you. \
         Make the change in the checkout itself with your tools (write_file, or a shell \
         command that edits the file), then answer again with the same artifact, taking its \
         content from `git --no-pager diff`. If the change you described is not the right \
         one, change your mind and write the right one into the checkout instead."
            .to_string(),
    );
    match base.get_mut("context").and_then(|c| c.as_object_mut()) {
        Some(context) => {
            context.insert("checkout_unchanged".to_string(), sentence);
        }
        None => {
            base["context"] = serde_json::json!({ "checkout_unchanged": sentence });
        }
    }
    base
}

/// Take the change from the run's checkout when the checkout holds one.
///
/// The artifact the model writes is prose about a file, and the form defects
/// the apply gate refuses — a path header naming a directory the file is not
/// in, a context line remembered with a token missing, a hunk header whose
/// count disagrees with its body — are properties of that prose, not of the
/// change. A run that edited its checkout has the same change on disk, where
/// git derived all three from the file itself. This reads that back and swaps
/// it in.
///
/// The typed artifact is kept whenever the tree cannot be read — no checkout,
/// no backend, or a run that left its tree untouched — so this can only replace
/// a diff with one the tree produced, never lose a change the round would have
/// had. Which end was used is counted rather than inferred: a harvest that
/// silently never fires and a model that never needed one leave the same
/// absence otherwise.
/// Returns the output to carry on with, where its diff came from, and whether
/// the checkout behind it could be read at all.
///
/// The last piece is not implied by the second: a diff that came from the model
/// is both what an unchanged checkout produces and what an unreadable one
/// produces, and the two call for opposite answers -- one is a round that should
/// be asked to write into the tree, the other is a run that has no tree to write
/// into.
async fn prefer_workspace_change(
    agent: &dyn Agent,
    task_id: &str,
    output: GeneratorOutput,
) -> (GeneratorOutput, Option<&'static str>, bool) {
    // A round with no change artifact is never asked about, so a report-only
    // round does not pay a sandbox command to be told there is nothing to swap.
    if !has_change_artifact(&output) {
        return (output, None, false);
    }
    let excluded = artifact_files_to_exclude(&output);
    let (harvested, checkout_read) = match agent.workspace_change(task_id, &excluded).await {
        Ok(diff) => (diff, true),
        Err(e) => {
            tracing::warn!(
                task_id,
                error = %e,
                "could not read the run's checkout; keeping the generated diff"
            );
            (None, false)
        }
    };
    let (output, source) = apply_workspace_change(output, harvested);
    if let Some(source) = source {
        crate::observable::global_observable()
            .record_change_diff_source(source)
            .await;
    }
    (output, source, checkout_read)
}

fn has_change_artifact(output: &GeneratorOutput) -> bool {
    output.artifacts.iter().any(|artifact| artifact.is_change())
}

/// The files the round's own answer would have left in the checkout.
///
/// A change artifact's name is what the round was told to call its answer, and
/// a run that took that answer from `git --no-pager diff` may have dropped the
/// output into the tree under exactly that name. The tree then reads back as a
/// change that creates the file, which is a change nobody wrote. Naming it out
/// of the read is the only place it can be told apart from a file the round
/// meant to add.
///
/// Only names that read as a diff are collected. A change artifact named like a
/// source file — the contract asks for `.diff`, but the type alone is enough
/// for an artifact to count as a change — would otherwise take a real file out
/// of the diff, and a change missing a file it edited is worse than one whose
/// diff is the model's own.
fn artifact_files_to_exclude(output: &GeneratorOutput) -> Vec<String> {
    output
        .artifacts
        .iter()
        .filter(|artifact| artifact.is_change())
        .map(|artifact| artifact.name.clone())
        .filter(|name| name.to_lowercase().ends_with(".diff"))
        .collect()
}

/// Drop every section that adds a file whose own content is a diff.
///
/// The read names the artifact files out by name, which is exact only for the
/// name the contract asks for. A round that writes its answer somewhere else
/// leaves a file nothing about the name says is not a source file, and the one
/// thing that does say so is the file's content: a change that adds a file
/// holding a diff is a change nothing derived from the repository.
///
/// Deliberately narrow. Only a creation counts, and only its first added line,
/// so a test fixture or a document that merely contains a diff — not as the
/// whole file, not from its first line — is left alone. A section that is not
/// dropped is passed through byte for byte.
fn without_answer_files(diff: &str) -> String {
    let mut kept = String::new();
    let mut section = String::new();
    let mut drop = false;
    let mut creation = false;
    let mut seen_first_added = false;

    for line in diff.split_inclusive('\n') {
        if line.starts_with("diff --git ") {
            if !drop {
                kept.push_str(&section);
            }
            section.clear();
            drop = false;
            creation = false;
            seen_first_added = false;
        } else if !drop {
            if line.starts_with("new file mode ") {
                creation = true;
            } else if creation
                && !seen_first_added
                && line.starts_with('+')
                && !line.starts_with("+++")
            {
                seen_first_added = true;
                drop = line.starts_with("+diff --git ");
            }
        }
        section.push_str(line);
    }
    if !drop {
        kept.push_str(&section);
    }
    kept
}

/// Swap the tree's diff in for the typed one, and say which end was used.
///
/// `None` from this is "there was nothing to decide": a round with no change
/// artifact has no diff whose source could be counted, and counting one anyway
/// would report a choice that was never made. A harvested string that is not a
/// diff at all (no `diff --git` in it) is a tree that answered with something
/// else, and it is refused here rather than handed to the gate as a change.
fn apply_workspace_change(
    mut output: GeneratorOutput,
    harvested: Option<String>,
) -> (GeneratorOutput, Option<&'static str>) {
    if !has_change_artifact(&output) {
        return (output, None);
    }
    let Some(diff) = harvested.filter(|diff| diff.contains("diff --git")) else {
        return (output, Some(crate::observable::CHANGE_DIFF_SOURCE_MODEL));
    };
    // Second cut at the run's own answer, for the name the read was not told
    // about. Stripping it can leave nothing, and a tree that held only the
    // answer held no change: the typed diff is what stays.
    let diff = without_answer_files(&diff);
    if !diff.contains("diff --git") {
        return (output, Some(crate::observable::CHANGE_DIFF_SOURCE_MODEL));
    }
    for artifact in &mut output.artifacts {
        // The contract asks for exactly one change artifact. A second one is a
        // different defect, and filling it with the same diff would hide it
        // behind a duplicate.
        if artifact.is_change() {
            artifact.content = diff.clone();
            break;
        }
    }
    (output, Some(crate::observable::CHANGE_DIFF_SOURCE_TREE))
}

/// The prompt contract the Generator gets for a self-evolution task.
///
/// The change this asks for is judged deterministically: the diff runs against a
/// checkout of the repository through `git apply --check` and then compiles. So
/// the contract has to state the things the gate will check, in the terms the
/// gate checks them — read before writing, hunk counts equal to the body, the
/// diff terminated, context matching the file byte for byte. A diff written from
/// memory is the most expensive failure on this path: the whole generation and
/// evaluation round is paid for and nothing is produced.
///
/// `refusal_causes` is the map that keeps this promise honest as the gate grows:
/// one entry per [`cog_core::RejectionCause`], naming the field here that
/// answers it, and a test that holds the two sets equal. A cause the gate gains
/// without a clause here is a round the generator is invited to spend and lose,
/// and that has already happened once — the lint criterion added on 2026-10-01
/// had no clause for seven days.
///
/// Naming the gate's causes is not the same as naming everything the gate
/// refuses. `tests_failed` answers a suite, and part of that suite is this
/// repository's own contract tests — conventions stated only in the tests that
/// judge them, which no enumeration of refusal causes can carry. On 2026-10-04 a
/// change that added two metric names died on the census test that walks the
/// closed metric set, after a round had been paid for in full; the diff had
/// added the publishing half and never the reading half. `repo_contracts` is
/// where those conventions are written down, because "the test suite runs" tells
/// the writer that it will be judged and not what it will be judged against.
/// 任务带来的原则，排成生成端读得懂的那一块；任务没带就是 `None`。
///
/// 一条都没有与没有带是两件事，读法却应该一样：空数组不是「这次不受任何原则
/// 约束」，只是提交方没有东西可放。两种都返回 `None`，于是「契约里没有这一块」
/// 永远只表示一件事——这一次没有原则被递进来。
///
/// 只认带 `statement` 或 `lead` 的行。一个只有 slug 的条目对模型是一串不认识
/// 的标识符，摆进去只会让它以为自己看见了规则。
fn principles_block(input: &serde_json::Value) -> Option<serde_json::Value> {
    let rows: Vec<&serde_json::Value> = input
        .get(cog_core::INPUT_PRINCIPLES)
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter()
                .filter(|row| {
                    row.get("statement")
                        .or_else(|| row.get("lead"))
                        .and_then(|v| v.as_str())
                        .is_some_and(|s| !s.trim().is_empty())
                })
                .collect()
        })
        .unwrap_or_default();
    if rows.is_empty() {
        return None;
    }
    Some(serde_json::json!({
        "binding": "These are this project's own recorded principles for the work you are about to do. They are binding: a change that violates one is refused by a gate that names it, and the refusal cites the principle rather than a style rule. Read every one of them before you write code, and treat a conflict between one of them and your own judgement as a reason to look again at the code, not at the principle.",
        "rows": rows,
    }))
}

fn change_generation_contract() -> serde_json::Value {
    serde_json::json!({
        "output_format": "unified_diff",
        "response_format": "json",
        "schema": {
            "content": "string: concise summary of the change",
            "artifacts": [
                {
                    "artifact_type": "change",
                    "name": "changes.diff",
                    "content": "valid git unified diff starting with 'diff --git'"
                }
            ]
        },
        "artifact_instructions": "Output exactly one artifact: artifact_type='change', name='changes.diff', content being the raw unified diff whose first line is 'diff --git a/<path> b/<path>'. Produce that content by making the edit in the checkout and then pasting what `git --no-pager diff` prints there, verbatim: the diff is read off the files, not reconstructed from memory. Retyping it by hand is accepted only when the checkout cannot be read at all, and a hand-written diff is the shape the apply gate rejects. The change is read back from the checkout's own diff, so put nothing in the checkout that is not part of the change. No markdown fences, no commentary inside content.",
        "grounding": "Your diff is validated with `git apply --check` against a checkout of this repository, then applied to it and compiled. It must therefore describe the files as they actually are, not as you remember them. You have file tools in this run — use them before writing: list the directories you intend to touch, then read every file you change in full (read_file, or a shell command such as `sed -n '1,400p' <path>`). Then make the edit in the checkout itself (write_file, or a shell command) and take this artifact's content from `git --no-pager diff`, so the headers, the context lines and the hunk counts are the ones git derives from the files. Address files by repository-relative path (for example crates/cog-core/src/lib.rs) — the same path that appears in the '+++ b/' line — and those paths resolve against the checkout. Never guess a path: a path that is not in the checkout is rejected unless the diff itself declares it as created.",
        "diff_grammar": "Every hunk header '@@ -<start>,<count> +<start>,<count> @@' must declare exactly the number of lines its body carries, and the diff must end with a newline. Every context line and every removed line has to match the file byte for byte, including indentation and trailing whitespace, and the start line numbers must be the real line numbers in the file you read. Keep hunks narrow and anchor them on context that is unique in the file: one hunk whose context cannot be located fails the entire change.",
        "creating_a_file": "To add a file, declare it as a creation: 'diff --git a/<path> b/<path>', then 'new file mode 100644', '--- /dev/null', '+++ b/<path>', and a hunk header '@@ -0,0 +1,<n> @@' whose body is n '+' lines. Creating a file that already exists fails, and so does rewriting a file that does not exist without declaring it as a creation.",
        "scope": "Stay inside the checkout, and prefer paths under crates/*/src/ and crates/*/tests/. A crate's integration tests are a first-class place for this work: a missing check is often repaired by writing the check, and cargo builds and the suite runs crates/<member>/tests/*.rs exactly as it does src. The gate rejects changes to build and deployment manifests (Cargo.toml, Cargo.lock, Dockerfile, Containerfile, docker-compose.yml, setup.sh), to configuration or credential files (cogneva.json, .env, .envrc, *.pem, *.key, *.crt, *.p12), and to anything inside a directory named `eval` — that directory holds the suite the change is measured against, so a change that edits it would be moving its own exam; deletions are judged by the same rules as edits. A diff far larger than the change it makes is refused as too large to review, so keep the change to the lines it needs. The applied change is compiled and tested, so it must be complete and self-consistent — no placeholder or unimplemented bodies.",
        "plan_targets": "The plan you are handed may name the repository-relative paths the change must touch. Every path it names has to be a target of your diff: the change may touch more files than the plan lists, but a path the plan names and your diff never touches is refused. If the plan names a file that already exists, change that file — do not create a new one beside it and leave the named file alone.",
        "formatting": "Before anything is compiled, the applied tree is checked the way CI checks it: `cargo fmt --all -- --check`. Your change has to be exactly what the workspace's formatter (plain rustfmt) produces — do not hand-format, and do not reflow or realign lines you did not need to touch, because the formatter's version of them is not the one that is in the file. A hunk whose result differs from the formatter's is refused (formatting_differs) no matter how good the code is.",
        "lint": "The applied tree is then linted the way CI lints it: `cargo clippy --workspace --all-targets` with warnings denied (`-D warnings`). Test targets are part of that scope, so code you write under `tests/` or in a `#[cfg(test)]` module is judged by the same rules as code under `src/`. Only the lines your diff writes are judged — a clippy diagnostic whose span lands on a line you added refuses the change (lint_introduced). Lints the tree already carried are not counted against you, so do not fix unrelated warnings: copy neither them nor the style of the line they report on. Write the new line so clippy has nothing to say about it.",
        "verification": "The applied change is compiled and the workspace test suite is run. A test that passed on the tree before your change and fails after it refuses the change (tests_failed). A suite that could not be run to a verdict at all (test_run_unavailable) is not your diff's fault and is not held against it, though it does cost the round. The suite is wider than the tests that sit beside the code you edit: this repository also carries contract tests that pin conventions a diff cannot state, and they are ordinary tests, so nothing in your diff announces that one of them judges it. Look for them before you write -- grep the test directories for every symbol you add or change (repo_contracts names the ones this path has already lost a round to) -- and satisfy what you find. That search is finished when every symbol your diff adds or changes has been looked up once: it is a lookup, not an exploration, and running it again with different arguments does not turn up anything the first pass did not, so a search that keeps going is a turn spent finding out nothing.",
        "reproduction": "A change raised to fix an upstream CI failure owes a reproduction, and it is judged on a pair of readings from the same command rather than on one: with the change taken back off the tree that command has to fail, and with the change on the tree the same command has to pass. A green run on its own is refused (not_reproduced) — a change that turns the tree green without touching what was red is precisely the round an upstream CI pays for and does not get back. So before you write the diff, run the failing check in this checkout and note which test names it reports; after the diff, run that same command again over the changed tree. The command is the project's own suite at full scope (`cargo test --workspace --no-fail-fast` here), never narrowed to the crates you edited: the red may live in a crate your diff never touches, and a narrowed run is green for exactly that reason. If the red does not reproduce here at all, say so and change nothing — a change pushed against a red this machine cannot show is a guess, and it is refused before it costs an upstream round.",
        "effect": "The change has to reach a running process. A configuration document is deserialized into the type that reads it, and the type's `impl Default for *Config` is only reached for a key the document leaves out — so a change whose every line is a literal in such an `impl`, for a key every shipped document already writes, is refused as no change at all (unreachable_default): the value it moves is one nothing reads. Change what the document carries, or the code that consumes the value, not a fallback no deployment falls back to.",
        "repo_contracts": "Conventions this repository's own tests enforce, and the half a diff that ignores them forgets. The metric name set (cog_core::metric_names::ALL) is closed: a contract test walks it against the alert rules and the dashboard panels and fails for any name that is read by neither and is not registered in that test's census with its own written verdict. A diff that adds a metric name and stops there is therefore incomplete by construction -- it has added the publishing half without the reading half. Add both: the reader (a rule or a panel that consumes the series) or the census entry, and whatever count the test holds against its own census. Treat the same shape as general wherever this repository pins a closed set: the entry you add is judged together with the evidence that reads it.",
        "refusal_causes": {
            "malformed_diff": "diff_grammar",
            "promotion_gate_refused": "scope",
            "forbidden_path": "scope",
            "intent_mismatch": "plan_targets",
            "context_does_not_apply": "diff_grammar",
            "apply_failed": "grounding",
            "formatting_differs": "formatting",
            "test_run_unavailable": "no_diff_can_avoid_it: the verification suite needs the host's build slot; when it cannot be had the change is refused without being judged, which no wording of a diff can change",
            "tests_failed": "verification",
            "not_reproduced": "reproduction",
            "lint_introduced": "lint",
            "lint_refused": "no_diff_can_avoid_it: the linter this deployment declares reports in a form the gate cannot compare against the lines a diff writes, so no diff can be shown not to have caused its findings; the deployment's declaration is what decides this, not the wording of a change",
            "unreachable_default": "effect",
            "deploy_admission_failed": "no_diff_can_avoid_it: a change that reaches deploy/** is admitted only when the project declares executable admission conditions (render, parity, server-side dry-run) in its own configuration document, and that declaration is read from the tree the change lands on — no wording of a diff can supply it, and a family with no conditions is refused as the strictest tier rather than sent to a person; the declared commands themselves, and a rollback round-trip that must leave the tree byte-identical, decide whether the diff is admitted",
            "project_kind_unusable": "no_diff_can_avoid_it: the landing recipe a project is dispatched under is declared by the project itself, in its own configuration document, and that document is read from the tree the change is applied to before the change is applied — so nothing a diff contains or writes can decide this, and a project whose declaration cannot be read is refused before its diff is looked at"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every rule here is one the gate enforces, so dropping any of them costs a
    /// whole paid round for nothing. The prose is free to change; these claims
    /// are not.
    #[test]
    fn the_change_contract_states_the_rules_the_gate_judges() {
        let contract = change_generation_contract();
        let text = contract.to_string();

        for (claim, needle) in [
            (
                "the diff is checked against a checkout",
                "git apply --check",
            ),
            (
                "the model must read before writing",
                "read every file you change in full",
            ),
            ("paths are repository-relative", "repository-relative path"),
            ("a guessed path is rejected", "Never guess a path"),
            (
                "hunk counts must equal the body",
                "number of lines its body carries",
            ),
            ("the diff must be terminated", "must end with a newline"),
            (
                "context must match byte for byte",
                "match the file byte for byte",
            ),
            ("creating a file has its own shape", "--- /dev/null"),
            (
                "the creation shape names the mode line",
                "new file mode 100644",
            ),
            (
                "the creation shape names its hunk header",
                "@@ -0,0 +1,<n> @@",
            ),
            (
                "an existing file may not be recreated",
                "Creating a file that already exists fails",
            ),
            (
                "an absent file may not be rewritten silently",
                "rewriting a file that does not exist without declaring it as a creation",
            ),
            (
                "the protected-file set is named",
                "configuration or credential files",
            ),
            (
                "the build manifests are named",
                "build and deployment manifests",
            ),
            (
                "the deploy manifests are named",
                "Dockerfile, Containerfile",
            ),
            ("the deploy manifests are named", "setup.sh"),
            (
                "line numbers must be the file's real ones",
                "must be the real line numbers in the file you read",
            ),
            (
                "hunks must be narrow and uniquely anchored",
                "narrow and anchor them on context that is unique in the file",
            ),
            (
                "deletions are judged as edits are",
                "deletions are judged by the same rules as edits",
            ),
            (
                "the directories must be listed before reading",
                "list the directories you intend to touch",
            ),
            (
                "every path the plan names must be a target of the diff",
                "has to be a target of your diff",
            ),
            (
                "a named existing file is changed, not bypassed",
                "do not create a new one beside it",
            ),
            (
                "an oversized diff is refused before it is applied",
                "too large to review",
            ),
            (
                "formatting is judged by the formatter's own output",
                "cargo fmt --all -- --check",
            ),
            (
                "the formatting rule forbids reflowing untouched lines",
                "do not reflow or realign lines you did not need to touch",
            ),
            (
                "linting runs with warnings denied",
                "cargo clippy --workspace --all-targets",
            ),
            (
                "the lint rule is scoped to the lines the change writes",
                "Only the lines your diff writes are judged",
            ),
            (
                "tests that passed before the change are the criterion",
                "passed on the tree before your change and fails after it",
            ),
            (
                "the suite is wider than the tests beside the file",
                "contract tests that pin conventions a diff cannot state",
            ),
            (
                "the writer is told to look for those tests itself",
                "grep the test directories for every symbol you add or change",
            ),
            (
                "the search for those tests is told where it ends",
                "That search is finished when every symbol your diff adds or changes has been looked up once",
            ),
            (
                "the closed metric name set is named",
                "The metric name set (cog_core::metric_names::ALL) is closed",
            ),
            (
                "adding a metric is stated to need its reader",
                "added the publishing half without the reading half",
            ),
            (
                "the census ratchet is stated",
                "whatever count the test holds against its own census",
            ),
        ] {
            assert!(
                text.contains(needle),
                "contract no longer says {claim}: {needle}"
            );
        }

        // The old wording told the model to only ever edit existing sources
        // under src/, which is narrower than the gate: creations are allowed and
        // so is anything else inside the checkout.
        assert!(
            !text.contains("include only source file modifications"),
            "the contract is back to forbidding everything the gate allows"
        );
    }

    /// The contract names every file the gate refuses, derived from the policy
    /// rather than restated here.
    ///
    /// The test above pins the wording the model reads; this one pins the list
    /// against the gate it is a promise about. They fail in different
    /// directions and neither replaces the other: a name added to the policy
    /// without a word in the contract is a change the generator is invited to
    /// write and the gate then refuses — a round paid for nothing, and the
    /// exact divergence that let a contract-legal change die at evaluation.
    #[test]
    fn the_change_contract_names_every_file_the_policy_refuses() {
        let text = change_generation_contract().to_string();

        // A needle that is a prefix of another entry (`.env` inside `.envrc`)
        // must be followed by something other than a name character, or `.envrc`
        // alone would satisfy the check for `.env` and hide the omission.
        let names = |needle: &str| {
            text.match_indices(needle).any(|(start, _)| {
                match text[start + needle.len()..].chars().next() {
                    Some(c) => !c.is_ascii_alphanumeric(),
                    None => true,
                }
            })
        };

        for name in cog_core::PROTECTED_FILE_NAMES {
            assert!(
                names(name),
                "the contract never names protected file {name}"
            );
        }
        for ext in cog_core::PROTECTED_FILE_EXTENSIONS {
            assert!(
                names(ext),
                "the contract never names protected extension .{ext}"
            );
        }
        // The directory axis is the one a reader misses by pattern: the two
        // lists above are read as file names, so an entry that is a directory
        // looks like a name that happens to be short. It is checked here for
        // the same reason as the others — a change the contract invites and the
        // gate refuses costs a round and buys nothing.
        for segment in cog_core::PROTECTED_DIR_SEGMENTS {
            assert!(
                names(segment),
                "the contract never names protected directory {segment}/"
            );
        }
    }

    /// The marker a cause carries when no wording of a diff can avoid it.
    ///
    /// Such a cause still gets an entry and still gets words: an omission and
    /// an unthought-about cause read the same from here, and the model is owed
    /// the reason either way.
    const NOT_WRITABLE: &str = "no_diff_can_avoid_it: ";

    /// The contract answers the gate's whole axis, and the axis it is held to
    /// is the gate's own — not a list restated here that could drift beside it.
    ///
    /// The two tests above pin the wording and the protected-file names; this
    /// one pins coverage. Without it the expensive direction of the drift is
    /// silent: a cause added to the gate leaves the generator writing diffs the
    /// gate refuses, and nothing goes red until a round has already been paid
    /// for — which is how a lint criterion added on 10-01 cost seven days of
    /// landings before anyone could see it.
    #[test]
    fn the_change_contract_answers_every_refusal_cause() {
        let contract = change_generation_contract();
        let answers = contract["refusal_causes"]
            .as_object()
            .expect("the contract carries no refusal_causes map");

        for cause in cog_core::RejectionCause::ALL {
            let key = cause.as_str();
            let answer = answers
                .get(key)
                .and_then(|value| value.as_str())
                .unwrap_or_else(|| panic!("the contract answers no refusal cause {key}"));
            match answer.strip_prefix(NOT_WRITABLE) {
                Some(reason) => assert!(
                    !reason.trim().is_empty(),
                    "refusal cause {key} is declared unavoidable with no reason"
                ),
                None => assert!(
                    contract.get(answer).is_some(),
                    "refusal cause {key} points at contract field {answer}, which the contract does not carry"
                ),
            }
        }

        // The other direction: a key left behind by a renamed variant would
        // read as coverage for a cause the gate can no longer reach.
        for key in answers.keys() {
            assert!(
                cog_core::RejectionCause::ALL
                    .iter()
                    .any(|cause| cause.as_str() == key),
                "the contract answers {key}, which is not a cause the gate can reach"
            );
        }
    }

    /// A change artifact carrying the diff the model typed, with a path header
    /// that names a directory the file is not in — the defect the tree's own
    /// diff cannot have.
    fn typed_change_output() -> GeneratorOutput {
        GeneratorOutput {
            content: serde_json::json!("added the reader"),
            artifacts: vec![crate::squad::pge::types::Artifact {
                name: "changes.diff".into(),
                artifact_type: "change".into(),
                content: "diff --git a/crates/cogneta/src/a.rs b/crates/cogneta/src/a.rs\n\
                          --- a/crates/cogneva/src/a.rs\n+++ b/crates/cogneta/src/a.rs\n\
                          @@ -1 +1 @@\n-old\n+new\n"
                    .into(),
            }],
        }
    }

    const TREE_DIFF: &str = "diff --git a/crates/cogneva/src/a.rs b/crates/cogneva/src/a.rs\n\
                            --- a/crates/cogneva/src/a.rs\n+++ b/crates/cogneva/src/a.rs\n\
                            @@ -1 +1 @@\n-old\n+new\n";

    #[test]
    fn the_diff_the_run_left_in_its_checkout_replaces_the_typed_one() {
        let (output, source) =
            apply_workspace_change(typed_change_output(), Some(TREE_DIFF.into()));
        assert_eq!(source, Some("tree"));
        assert_eq!(output.artifacts[0].content, TREE_DIFF);
    }

    #[test]
    fn a_round_the_tree_cannot_answer_for_keeps_the_typed_diff() {
        let typed = typed_change_output();
        let (output, source) = apply_workspace_change(typed.clone(), None);
        assert_eq!(source, Some("model"));
        assert_eq!(output.artifacts[0].content, typed.artifacts[0].content);
    }

    #[test]
    fn a_checkout_that_answers_with_something_other_than_a_diff_is_not_swapped_in() {
        let typed = typed_change_output();
        let (output, source) =
            apply_workspace_change(typed.clone(), Some("nothing changed\n".into()));
        assert_eq!(source, Some("model"));
        assert_eq!(output.artifacts[0].content, typed.artifacts[0].content);
    }

    /// 重问的那一句要落在这一轮的上下文里，并且**不动**原来那半：这一轮的
    /// 目标、计划、上一轮反馈都还在，模型才有东西可改。
    #[test]
    fn the_repair_round_carries_the_original_request_and_says_what_was_wrong() {
        let base = serde_json::json!({
            "task": {"id": "t1", "goal": "fix the thing"},
            "context": {"goal": "fix the thing", "plan": {"steps": ["a"]}},
            "change_generation": {"output_format": "unified_diff"},
        });
        let repair = checkout_repair_request(base.clone());
        assert_eq!(repair["task"], base["task"]);
        assert_eq!(repair["context"]["goal"], base["context"]["goal"]);
        assert_eq!(repair["context"]["plan"], base["context"]["plan"]);
        assert_eq!(
            repair["change_generation"], base["change_generation"],
            "契约那半必须逐字节不变，否则这一轮的重试就不再可缓存"
        );
        let sentence = repair["context"]["checkout_unchanged"]
            .as_str()
            .expect("重问要说明树没动");
        assert!(
            sentence.contains("checkout is unchanged"),
            "要先说清事实：{sentence}"
        );
        assert!(
            sentence.contains("git --no-pager diff"),
            "要给出取产物的那一条命令：{sentence}"
        );
    }

    /// 请求里没有上下文那一半也不能把话丢掉：`context` 整个建出来。
    #[test]
    fn a_request_with_no_context_still_gets_the_repair_sentence() {
        let repair = checkout_repair_request(serde_json::json!({"task": {"id": "t1"}}));
        assert!(repair["context"]["checkout_unchanged"].is_string());
        assert_eq!(repair["task"]["id"], "t1");
    }

    /// A round with no change artifact has no diff whose source could be
    /// counted; counting one anyway reports a choice nobody made. The other
    /// artifact must also survive verbatim — the swap touches change artifacts
    /// and nothing else.
    #[test]
    fn a_round_with_no_change_artifact_decides_nothing() {
        let output = GeneratorOutput {
            content: serde_json::json!("a report"),
            artifacts: vec![crate::squad::pge::types::Artifact {
                name: "report.md".into(),
                artifact_type: "report".into(),
                content: "@@ not a diff @@".into(),
            }],
        };
        let (output, source) = apply_workspace_change(output, Some(TREE_DIFF.into()));
        assert_eq!(source, None);
        assert_eq!(output.artifacts[0].content, "@@ not a diff @@");
    }

    /// A round that writes its answer out under a name the read was not told
    /// about is caught by what the file holds instead. Only the section that
    /// adds a diff goes: the round's real edit is in the same diff and has to
    /// survive the strip.
    #[test]
    fn a_creation_that_holds_a_diff_is_dropped_and_the_rest_kept() {
        let harvested = format!(
            "{TREE_DIFF}\
             diff --git a/patch.txt b/patch.txt\nnew file mode 100644\n\
             index 0000000..6332604\n--- /dev/null\n+++ b/patch.txt\n@@ -0,0 +1,3 @@\n\
             +diff --git a/x.rs b/x.rs\n+--- a/x.rs\n++++ b/x.rs\n"
        );
        assert_eq!(without_answer_files(&harvested), TREE_DIFF);
    }

    /// Narrow on purpose: a file the round really added is a file the change
    /// means to write, and a document that mentions a diff is not a diff. Both
    /// shapes are common enough that a wider rule would eat real changes.
    #[test]
    fn a_new_source_file_and_a_document_that_mentions_a_diff_both_survive() {
        let harvested = "\
diff --git a/crates/cogneva/src/new.rs b/crates/cogneva/src/new.rs\n\
new file mode 100644\nindex 0000000..1111111\n--- /dev/null\n+++ b/crates/cogneva/src/new.rs\n\
@@ -0,0 +1,2 @@\n+// a new module\n+pub fn f() {}\n\
diff --git a/prompts/notes.md b/prompts/notes.md\nnew file mode 100644\n\
index 0000000..2222222\n--- /dev/null\n+++ b/prompts/notes.md\n@@ -0,0 +1,2 @@\n\
+Example:\n+diff --git a/x.rs b/x.rs\n";
        assert_eq!(without_answer_files(harvested), harvested);
    }

    /// The file the round wrote its answer into is left out of the read. Any
    /// artifact named like a diff counts as a change artifact — a name ending in
    /// `.diff` is half of what `is_change` is — so all of them are named out; a
    /// name that is a source file is not, because dropping a file the round
    /// really edited is worse than keeping a diff the model typed out.
    #[test]
    fn only_the_artifacts_that_read_as_diffs_are_left_out_of_the_read() {
        let mut output = typed_change_output();
        output.artifacts.push(crate::squad::pge::types::Artifact {
            name: "notes.diff".into(),
            artifact_type: "report".into(),
            content: "a second answer of the same shape".into(),
        });
        output.artifacts.push(crate::squad::pge::types::Artifact {
            name: "report.md".into(),
            artifact_type: "report".into(),
            content: "what the round says it did".into(),
        });
        assert_eq!(
            artifact_files_to_exclude(&output),
            vec!["changes.diff".to_string(), "notes.diff".to_string()]
        );

        let source_named = GeneratorOutput {
            content: serde_json::json!("edited the file"),
            artifacts: vec![crate::squad::pge::types::Artifact {
                name: "lib.rs".into(),
                artifact_type: "change".into(),
                content: TREE_DIFF.into(),
            }],
        };
        assert!(artifact_files_to_exclude(&source_named).is_empty());
    }

    /// 带原则来的任务，原则真的排在这一块里；每一条都带着它自己那句话。
    #[test]
    fn principles_the_task_carries_are_laid_out_with_their_own_sentences() {
        let block = principles_block(&serde_json::json!({
            cog_core::INPUT_PRINCIPLES: [
                {"slug": "feedback_a", "lead": "先给结论"},
                {"slug": "feedback_b", "statement": "写不出门的记忆不新开文件"},
            ]
        }))
        .expect("两条原则都带正文，应该有这一块");

        let rows = block["rows"].as_array().expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["lead"], "先给结论");
        assert_eq!(rows[1]["statement"], "写不出门的记忆不新开文件");
        assert!(
            block["binding"]
                .as_str()
                .unwrap_or_default()
                .contains("binding"),
            "没有一句话说清它们是必须遵守的，模型读到的就只是一份参考资料"
        );
    }

    /// 只有 slug 的条目对模型是一串不认识的标识符，摆进去等于让它以为自己
    /// 看见了规则。一条都排不出来时整块不出现——「没有这一块」于是永远只表示
    /// 一件事：这一次没有原则被递进来。
    #[test]
    fn a_row_with_no_sentence_is_not_a_principle_the_model_can_follow() {
        for input in [
            serde_json::json!({}),
            serde_json::json!({ cog_core::INPUT_PRINCIPLES: [] }),
            serde_json::json!({ cog_core::INPUT_PRINCIPLES: [{"slug": "feedback_a"}] }),
            serde_json::json!({ cog_core::INPUT_PRINCIPLES: [{"slug": "x", "lead": "   "}] }),
            serde_json::json!({ cog_core::INPUT_PRINCIPLES: "not a list" }),
        ] {
            assert!(
                principles_block(&input).is_none(),
                "这一份不该排出原则块：{input}"
            );
        }
    }
}
