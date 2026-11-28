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

/// 把计划交出来的定义摆到生成侧的上下文里：完成标准不只是判者的输入。
///
/// 计划的 `definition` 说的是「这一类是什么、怎样算做完」。判者已经按它判；生成侧
/// 若只在 `plan` 那一格里顺带看见它，执行就仍是在照每片输入填。这里把它提成上下文
/// 的一等格，模型的答案契约因此有一条明确的完成标准；空定义不摆（不是一条标准）。
fn state_the_completion_standard(ctx: &mut serde_json::Value, plan: &serde_json::Value) {
    let Some(definition) = plan
        .get("definition")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|definition| !definition.is_empty())
    else {
        return;
    };
    ctx["completion_standard"] = serde_json::json!(definition);
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

        // The stable half: the answer contract. What it carries is the plug — the
        // field the answer goes in, where the content comes from, which surfaces
        // may be touched — not a rule book: the gate judges the rules and prints
        // its own verdict, so restating them here would only say them twice.
        let mut contract = serde_json::json!({});

        // Inject self-evolution change-generation instructions when requested.
        let is_self_evolution = task.is_self_evolution();

        if is_self_evolution {
            contract["change_generation"] = change_generation_contract();
        } else if self.output_schema.is_none() && self.prompt_skill.is_none() {
            // Built-in contract for standard execution. Lowest precedence:
            // operator schema > prompt skill > built-in. Only the answer's shape:
            // the plan and the goal are the work, and the deliverable is what
            // this actor was called to produce.
            contract["response_format"] = serde_json::json!("json");
            contract["output_schema"] = serde_json::json!({
                "content": "string: the produced result — the actual answer or deliverable for the goal",
                "artifacts": [{"name": "string", "content": "string", "artifact_type": "string"}]
            });
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
            let input_summary = serde_json::to_string(&task.input).unwrap_or(Default::default());
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

        // 定义的来源是计划，不是任务输入：同一份计划下的每一片都按同一条完成标准做。
        state_the_completion_standard(&mut ctx, plan);
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
        let output_str = serde_json::to_string_pretty(&output).unwrap_or(Default::default());
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
        .unwrap_or(Default::default());
    if rows.is_empty() {
        return None;
    }
    Some(serde_json::json!({
        // 只报这一块是什么，不报该怎么对待它：哪些原则真的挂着门、挂在哪些路径上，
        // 是逐条不同的事实（`collaboration_executor` 才知道这一次带进来的哪几条有门），
        // 一句「违反了会被门拒绝」在这里是把逐条事实压成一句总话，说错了没人会红。
        "binding": "This project's own recorded principles, and they are binding for this work.",
        "rows": rows,
    }))
}

// prompt: contract — enforced by crates/cog-collaboration/src/actors/generator.rs::the_contract_states_the_plug_and_leaves_the_judging_to_the_gate
fn change_generation_contract() -> serde_json::Value {
    // 只剩插头：答案放在哪个字段、内容从哪儿取、这次能在哪个面上动手。
    //
    // 规则一条都不写——不是压缩，是不写。门判完会把自己说的话原样交回去，两条通道都
    // 在：同一轮内的重试走 `context.repair_feedback`（evaluator 把 `change_validation`
    // 的正文照抄成 feedback），跨任务返工走返工目标的 `Evidence:`（`cog-reflection` 的
    // `details` 就是门的原文），受保护的路径也由那条通道点名。我们再用中文说一遍「fmt
    // 怎么跑、clippy 怎么跑、哪些路径不许碰」，是同一件事说两遍：第一遍机器说，永远是
    // 准的；第二遍我们说，只在它过期之前是准的。
    serde_json::json!({
        "output_format": "unified_diff",
        "response_format": "json",
        "schema": {
            "content": "string: concise summary of the change",
            "artifacts": [
                {
                    "artifact_type": "change",
                    "name": "changes.diff",
                    "content": "the unified diff git prints for your checkout"
                }
            ]
        },
        "artifact_instructions": "Output exactly one artifact: artifact_type='change', name='changes.diff', content = what `git --no-pager diff` prints in your checkout, verbatim.",
        "surface": format!("Your checkout is {surfaces}.", surfaces = cog_core::CONTRIBUTABLE_SURFACES),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 计划的定义进生成侧的上下文，作为一条明确写成 `completion_standard` 的完成标准。
    #[test]
    fn the_plan_definition_becomes_the_generation_standard() {
        let mut ctx = serde_json::json!({"goal": "g"});
        state_the_completion_standard(
            &mut ctx,
            &serde_json::json!({"definition": "  做完＝每一片都说得出终态  "}),
        );
        assert_eq!(
            ctx["completion_standard"],
            serde_json::json!("做完＝每一片都说得出终态"),
            "定义没有被摆成生成侧的完成标准"
        );
        // 没有定义（或者只有空白）时不摆一格：空标准会把「没写」读成「随便」。
        let mut bare = serde_json::json!({});
        state_the_completion_standard(&mut bare, &serde_json::json!({"definition": "   "}));
        assert!(bare.get("completion_standard").is_none());
    }

    /// The stable contract carries the plug, not the rule book.
    ///
    /// What the model cannot infer belongs here: which artifact, where its content
    /// comes from, which surfaces may be touched, which plan paths must be hit.
    /// What it *can* infer — or read off the gate's own message when a round is
    /// refused — does not: those nine sections were sent on every round,
    /// including the ones nothing refused, and every sentence in them was a
    /// verdict some gate prints for itself.
    #[test]
    fn the_contract_states_the_plug_and_leaves_the_judging_to_the_gate() {
        let contract = change_generation_contract();
        let text = contract.to_string();

        for (claim, needle) in [
            (
                "the answer is one change artifact named changes.diff",
                "name='changes.diff'",
            ),
            (
                "its content is the checkout's own diff, not a retyped one",
                "git --no-pager diff",
            ),
            (
                "the checkout a change is written in is named",
                cog_core::CONTRIBUTABLE_SURFACES,
            ),
        ] {
            assert!(
                text.contains(needle),
                "the contract no longer says {claim}: {needle}"
            );
        }

        // The other direction, and the one that keeps the deletion from growing
        // back: the checks the gate runs are named by the gate, not by the
        // contract. A round that is never refused pays for none of them.
        for (claim, needle) in [
            ("the formatter's command", "cargo fmt --all"),
            ("the linter's command", "cargo clippy --workspace"),
            ("the apply check", "git apply --check"),
            (
                "the suite's pre/post criterion",
                "passed on the tree before your change",
            ),
            ("the metric-name convention", "The metric name set"),
            ("the reproduction pair", "not_reproduced"),
            ("how to paste a diff", "do not wrap it in markdown fences"),
        ] {
            assert!(
                !text.contains(needle),
                "the contract is restating what a gate prints ({claim}: {needle}) — that \
                 sentence costs every round and is already in the refusal it is about"
            );
        }

        // Nor by another route: a table keyed by refusal cause is the same prose
        // with a smaller blast radius, and it is the shape this deletion first
        // grew back into. The gate's answer reaches the model as the gate's own
        // text (`context.repair_feedback`, or the rework goal's `Evidence:`),
        // so the contract never needs to spell a cause.
        // Nor the protected paths. They were once enumerated here so a round
        // would not be spent learning them; the refusal names the path it
        // refused (`related_files` carries into the rework goal), so the
        // enumeration is a second copy of the same fact, in prose that goes
        // stale the moment the policy list changes.
        for name in cog_core::PROTECTED_FILE_NAMES
            .iter()
            .chain(cog_core::PROTECTED_FILE_EXTENSIONS.iter())
        {
            assert!(
                !text.contains(name),
                "the contract names protected {name} — the refusal already names \
                 the path it refused, and this copy outlives the policy it copies"
            );
        }

        for cause in cog_core::RejectionCause::ALL {
            assert!(
                !text.contains(cause.as_str()),
                "the contract names the refusal cause {} — the refusal it describes \
                 already carries that word, in the gate's own sentence",
                cause.as_str()
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
