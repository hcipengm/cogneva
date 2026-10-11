//! 反思式提示词进化 ＋ Pareto 搜索。
//!
//! 一行是「把提示词当基因组来进化」：候选提示词各跑一次，拿到一段**自然语言的反思**
//! （不需要金标——这是这个方法自己的前提），按质量与简洁两个目标取 Pareto 前沿，
//! 再从前沿变异出下一代，最后用最好的那次尝试的答案交卷。
//!
//! 内层那一次尝试（一次带工具的 rollout）是这个外壳**自己的**：不借用别行的循环。
//! 两行共用同一条循环会让它们变成同一个实现，表里的差就只剩提示词了。

use std::time::Instant;

use async_trait::async_trait;
use cog_core::{Message, Usage};

use super::{accumulate, chat_options, observe_call, take_turn, task_text};
use crate::dataset::EvalCase;
use crate::metric::StepRecord;
use crate::scaffold::{AgentOutput, AgentScaffold, FinishReason, SolveContext};

const BASE_PROMPT: &str = "\
You are an agent solving a single task. Use the tools you are given when they help, and \
finish by stating the final answer plainly.";

const CRITIQUE_PROMPT: &str = "\
You are reviewing an attempt at a task. Say in two sentences what was strong and what was \
missing, then end with a line `SCORE: <a number between 0 and 1>`.";

const MUTATE_PROMPT: &str = "\
You are improving the instructions given to an agent. Rewrite them so the weaknesses in the \
review are addressed. Reply with the new instructions only.";

/// 搜索的规模。
///
/// 这两个数决定这一行**每题花多少次上游调用**，所以它们必须和 harness 的其它旋钮
/// 一起钉进实验元数据：同一格换个规模就是另一个实验。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GepaConfig {
    /// 每一代保留多少个候选。
    pub population: usize,
    /// 跑多少代。
    pub generations: usize,
}

impl Default for GepaConfig {
    fn default() -> Self {
        Self {
            population: 3,
            generations: 2,
        }
    }
}

pub struct Gepa {
    config: GepaConfig,
}

impl Gepa {
    pub fn new(config: GepaConfig) -> Self {
        Self {
            config: GepaConfig {
                population: config.population.max(1),
                generations: config.generations.max(1),
            },
        }
    }
}

impl Default for Gepa {
    fn default() -> Self {
        Self::new(GepaConfig::default())
    }
}

/// 从反思里读回分数。
///
/// 读不出来就是 `None`，由调用方当成 0——**不能**当成 1，也不能拿一段散文去排序：
/// 排序键要是读错，Pareto 前沿就选错了，而且没有任何症状。
pub(crate) fn parse_score(text: &str) -> Option<f64> {
    let mut found = None;
    for line in text.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("SCORE:") else {
            continue;
        };
        if let Ok(v) = rest.trim().parse::<f64>() {
            found = Some(v.clamp(0.0, 1.0));
        }
    }
    found
}

/// 一次尝试：用这份提示词跑一遍（带工具），最多 `max_turns` 轮。
struct Attempt {
    answer: String,
    trace: Vec<StepRecord>,
    tokens: Usage,
    turns: usize,
}

impl Gepa {
    async fn rollout(
        &self,
        case: &EvalCase,
        ctx: &SolveContext,
        prompt: &str,
    ) -> anyhow::Result<Attempt> {
        let options = chat_options(ctx);
        let mut messages = vec![
            Message::system(prompt.to_string()),
            Message::user(task_text(case)),
        ];
        let mut trace = Vec::new();
        let mut tokens = Usage::default();

        for turn in 0..ctx.budget.max_steps {
            let started = Instant::now();
            let turn_result = take_turn(ctx, &messages, &options).await?;
            accumulate(&mut tokens, &turn_result.usage);
            if turn_result.calls.is_empty() {
                return Ok(Attempt {
                    answer: turn_result.text,
                    trace,
                    tokens,
                    turns: turn + 1,
                });
            }
            messages.push(Message::assistant(turn_result.content.clone()));
            for (id, name, arguments) in &turn_result.calls {
                let (observation, ok) = observe_call(ctx, &mut messages, id, name, arguments).await;
                trace.push(StepRecord {
                    step_index: turn,
                    action_type: name.clone(),
                    action_params: arguments.clone(),
                    thought: Some(turn_result.text.clone()).filter(|t| !t.trim().is_empty()),
                    duration_ms: started.elapsed().as_millis() as u64,
                    success: ok,
                    tool_calls: vec![name.clone()],
                });
                let _ = observation;
            }
        }
        Ok(Attempt {
            answer: String::new(),
            trace,
            tokens,
            turns: ctx.budget.max_steps,
        })
    }

    /// 一段反思：给这次尝试的自然语言评价与分数。
    async fn critique(
        &self,
        case: &EvalCase,
        ctx: &SolveContext,
        answer: &str,
    ) -> anyhow::Result<(String, f64, Usage)> {
        let options = chat_options(ctx);
        let messages = vec![
            Message::system(CRITIQUE_PROMPT),
            Message::user(format!("Task:\n{}\n\nAttempt:\n{answer}", task_text(case))),
        ];
        let turn = take_turn(ctx, &messages, &options).await?;
        let score = parse_score(&turn.text).unwrap_or(0.0);
        Ok((turn.text, score, turn.usage))
    }

    /// 从前沿变异出下一代的一份提示词。
    async fn mutate(
        &self,
        ctx: &SolveContext,
        prompt: &str,
        critique: &str,
    ) -> anyhow::Result<(String, Usage)> {
        let options = chat_options(ctx);
        let messages = vec![
            Message::system(MUTATE_PROMPT),
            Message::user(format!(
                "Current instructions:\n{prompt}\n\nReview:\n{critique}"
            )),
        ];
        let turn = take_turn(ctx, &messages, &options).await?;
        let next = if turn.text.trim().is_empty() {
            prompt.to_string()
        } else {
            turn.text
        };
        Ok((next, turn.usage))
    }
}

/// Pareto 前沿：质量与简洁都不差的那些人。
///
/// 「简洁」用提示词的字符数当代理——它是这一行唯一能被我们量出来的第二个目标，
/// 而它确实是 GEPA 要压的东西之一。代理不是原物：论文里对 Pareto 的第二个目标
/// 另有定义，这一点要在报告里说清。
fn pareto_front(candidates: &[(String, f64, String)]) -> Vec<&(String, f64, String)> {
    candidates
        .iter()
        .filter(|(prompt_a, score_a, _)| {
            !candidates.iter().any(|(prompt_b, score_b, _)| {
                (score_b > score_a
                    || (score_b == score_a && prompt_b.chars().count() < prompt_a.chars().count()))
                    && (score_b >= score_a && prompt_b.chars().count() <= prompt_a.chars().count())
            })
        })
        .collect()
}

#[async_trait]
impl AgentScaffold for Gepa {
    fn name(&self) -> &str {
        "gepa"
    }

    async fn solve(&self, case: &EvalCase, ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
        let mut population = vec![BASE_PROMPT.to_string()];
        let mut trace: Vec<StepRecord> = Vec::new();
        let mut tokens = Usage::default();
        let mut best: Option<(f64, String)> = None;
        let mut reached_budget = false;

        for generation in 0..self.config.generations {
            let mut scored: Vec<(String, f64, String)> = Vec::new();
            for prompt in &population {
                let attempt = self.rollout(case, ctx, prompt).await?;
                accumulate(&mut tokens, &attempt.tokens);
                trace.extend(attempt.trace.clone());
                if attempt.answer.is_empty() && attempt.turns >= ctx.budget.max_steps {
                    reached_budget = true;
                }
                let (critique, score, usage) = self.critique(case, ctx, &attempt.answer).await?;
                accumulate(&mut tokens, &usage);
                if best
                    .as_ref()
                    .is_none_or(|(best_score, _)| score > *best_score)
                {
                    best = Some((score, attempt.answer.clone()));
                }
                scored.push((prompt.clone(), score, critique));
            }

            // 最后一代不再变异：那一次的调用不会影响答案，白花上游的钱。
            if generation + 1 >= self.config.generations {
                break;
            }
            let front = pareto_front(&scored);
            let mut next: Vec<String> = Vec::new();
            for (prompt, _, critique) in front {
                if next.len() >= self.config.population {
                    break;
                }
                let (mutated, usage) = self.mutate(ctx, prompt, critique).await?;
                accumulate(&mut tokens, &usage);
                next.push(mutated);
            }
            population = next;
        }

        let (_, answer) = best.unwrap_or((0.0, String::new()));
        Ok(AgentOutput {
            final_answer: answer,
            trace,
            tokens,
            finish: if reached_budget {
                FinishReason::StepBudget
            } else {
                FinishReason::Answered
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{Budget, NoEnv};
    use crate::scaffolds::test_support::{code_tool, system_of, ScriptedLlm, ToolRecorder};
    use std::sync::Arc;

    fn case() -> EvalCase {
        EvalCase {
            id: "c1".into(),
            name: "c1".into(),
            input: serde_json::json!("how many?"),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: Default::default(),
        }
    }

    fn ctx_of(llm: Arc<ScriptedLlm>, recorder: &ToolRecorder) -> SolveContext {
        SolveContext {
            llm,
            tools: Arc::new(code_tool(recorder)),
            env: Arc::new(NoEnv),
            budget: Budget::default(),
            seed: 5,
        }
    }

    #[test]
    fn a_score_is_read_from_the_end_of_the_review_and_nothing_else_is_one() {
        assert_eq!(parse_score("good\nSCORE: 0.75"), Some(0.75));
        // 没有那一行就是 None：散文不能当分数，四舍五入成 1 更不行。
        assert_eq!(parse_score("looks good to me"), None);
        // 越界的数按边界夹住，不反过来放大。
        assert_eq!(parse_score("SCORE: 3"), Some(1.0));
        assert_eq!(parse_score("SCORE: -1"), Some(0.0));
    }

    #[tokio::test]
    async fn one_generation_keeps_the_best_attempt_as_the_answer() {
        // 人口 1、一代：rollout 一次（直接给答案）＋ 反思一次。
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::text("the answer is 7"),
            ScriptedLlm::text("solid\nSCORE: 0.8"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), &recorder);
        let gepa = Gepa::new(GepaConfig {
            population: 1,
            generations: 1,
        });

        let out = gepa.solve(&case(), &ctx).await.unwrap();

        assert_eq!(out.final_answer, "the answer is 7");
        assert_eq!(out.finish, FinishReason::Answered);
        assert_eq!(llm.calls(), 2, "一次尝试加一次反思");
    }

    #[tokio::test]
    async fn the_next_generation_runs_the_mutated_instructions() {
        // 人口 1、两代：rollout+反思 → 变异 → rollout+反思，共 5 次。
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::text("first attempt"),
            ScriptedLlm::text("weak\nSCORE: 0.2"),
            ScriptedLlm::text("Use a calculator before answering."),
            ScriptedLlm::text("second attempt"),
            ScriptedLlm::text("better\nSCORE: 0.9"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), &recorder);
        let gepa = Gepa::new(GepaConfig {
            population: 1,
            generations: 2,
        });

        let out = gepa.solve(&case(), &ctx).await.unwrap();

        assert_eq!(llm.calls(), 5);
        // 分数高的那一代赢：0.9 的那次答案交卷。
        assert_eq!(out.final_answer, "second attempt");
        // 第二代真用上了变异出来的提示词。
        let second_gen_request = system_of(&llm.requests()[3]);
        assert!(
            second_gen_request.contains("calculator"),
            "{second_gen_request}"
        );
    }

    #[tokio::test]
    async fn a_tool_turn_inside_a_rollout_is_observed_and_continues() {
        let llm = Arc::new(ScriptedLlm::new(vec![
            ScriptedLlm::tool_call("python", serde_json::json!({"code": "1+1"})),
            ScriptedLlm::text("2"),
            ScriptedLlm::text("ok\nSCORE: 1.0"),
        ]));
        let recorder = ToolRecorder::new();
        let ctx = ctx_of(llm.clone(), &recorder);
        let gepa = Gepa::new(GepaConfig {
            population: 1,
            generations: 1,
        });

        let out = gepa.solve(&case(), &ctx).await.unwrap();

        assert_eq!(out.final_answer, "2");
        assert_eq!(recorder.count(), 1);
        assert_eq!(out.trace.len(), 1);
        assert!(out.trace[0].success);
    }

    #[test]
    fn the_front_drops_candidates_that_are_worse_on_both_objectives() {
        let candidates = vec![
            ("short and good".to_string(), 0.9, "a".to_string()),
            (
                "a much longer prompt that is also worse".to_string(),
                0.5,
                "b".to_string(),
            ),
        ];
        let front = pareto_front(&candidates);
        assert_eq!(front.len(), 1);
        assert_eq!(front[0].1, 0.9);
    }
}
