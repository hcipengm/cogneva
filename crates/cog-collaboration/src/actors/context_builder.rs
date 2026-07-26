//! Standard [`cog_core::TaskContextBuilder`] implementation for PGE actors.
//!
//! Design: `docs/20250607_context_engineering_action_plan.md` P0-2 / P1-4 /
//! P1-5. Centralizes the prompt context JSON that Planner / Generator /
//! Evaluator previously hand-assembled inline. The per-role field sets below
//! are the standardized context schemas; actor-specific extras
//! (self-evolution mode, output schema overrides, prompt skills, knowledge
//! retrieval) are layered on top by the actors themselves.

use cog_core::{PgeRole, Task, TaskContextBuilder, TaskContextInput};
use serde_json::{json, Value};

/// Default context builder used by all PGE actors unless overridden.
#[derive(Debug, Clone, Copy, Default)]
pub struct StandardTaskContextBuilder;

impl StandardTaskContextBuilder {
    /// Fields every role shares: goal, task identity, hierarchy.
    fn base(task: &Task) -> Value {
        json!({
            "goal": task.input.get("goal").cloned().unwrap_or(json!(task.task_type)),
            "task_type": format!("{:?}", task.task_type),
            "is_executable": task.is_executable,
            "task_id": task.id,
        })
    }

    fn with_board(mut ctx: Value, board: Option<&Value>) -> Value {
        if let Some(board) = board {
            ctx["context_board"] = board.clone();
        }
        ctx
    }

    fn planner(input: &TaskContextInput<'_>) -> Value {
        let task = input.task.expect("planner requires a task");
        let mut ctx = Self::base(task);
        ctx["attempt"] = json!(input.attempt);
        ctx["parent_task_id"] = json!(task.parent_task_id);

        if let Some(ref meta) = task.action_planner_meta {
            ctx["action_planner_meta"] = json!({
                "verified": meta.verified,
                "confidence": meta.confidence,
                "version": meta.version,
            });
        }
        if let Some(feedback) = input.previous_feedback {
            ctx["previous_feedback"] = json!(feedback);
        }
        if let Some(score) = input.previous_score {
            ctx["previous_score"] = json!(score);
        }
        if let Some(gen) = input.generation {
            ctx["previous_generation"] = gen.clone();
        }
        Self::with_board(ctx, input.context_board)
    }

    fn generator(input: &TaskContextInput<'_>) -> Value {
        let task = input.task.expect("generator requires a task");
        let mut ctx = Self::base(task);
        ctx["plan"] = input.plan.cloned().unwrap_or(Value::Null);
        ctx["attempt"] = json!(input.attempt);
        ctx["parent_task_id"] = json!(task.parent_task_id);
        ctx["input"] = task.input.clone();

        if let Some(eval) = input.previous_evaluation {
            ctx["previous_evaluation"] = eval.clone();
        }
        if let Some(gen) = input.generation {
            ctx["previous_generation"] = gen.clone();
        }
        if let Some(feedback) = input.repair_feedback {
            ctx["repair_feedback"] = json!(feedback);
        }
        Self::with_board(ctx, input.context_board)
    }

    fn evaluator(input: &TaskContextInput<'_>) -> Value {
        let task = input.task.expect("evaluator requires a task");
        let mut ctx = Self::base(task);
        ctx["plan"] = input.plan.cloned().unwrap_or(Value::Null);
        ctx["generation"] = input.generation.cloned().unwrap_or(Value::Null);
        ctx["history"] = json!(input.history);

        if !input.criteria.is_empty() {
            ctx["criteria"] = Value::Array(input.criteria.iter().map(|c| json!(c)).collect());
        }
        if let Some(ref meta) = task.action_planner_meta {
            if let Some(confidence) = meta.confidence {
                ctx["quality_threshold"] = json!(confidence);
            }
        }
        Self::with_board(ctx, input.context_board)
    }
}

impl TaskContextBuilder for StandardTaskContextBuilder {
    fn build(&self, role: PgeRole, input: &TaskContextInput<'_>) -> Value {
        match role {
            PgeRole::Planner => Self::planner(input),
            PgeRole::Generator => Self::generator(input),
            PgeRole::Evaluator => Self::evaluator(input),
            // Moderator and Merger assemble session-shaped contexts that do
            // not share the PGE base schema; actors keep their own assembly
            // for those roles and only fall back to the shared base fields.
            PgeRole::Moderator | PgeRole::Merger => {
                let task = input.task.expect("moderator/merger requires a task");
                Self::with_board(Self::base(task), input.context_board)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task() -> Task {
        let mut t = Task::new(
            "42",
            cog_core::TaskType::Custom("test".into()),
            json!({"goal": "improve latency"}),
        );
        t.parent_task_id = Some("7".into());
        t
    }

    #[test]
    fn planner_context_matches_legacy_shape() {
        let t = task();
        let board = json!({"k": "v"});
        let prev_gen = json!({"content": "old"});
        let input = TaskContextInput {
            task: Some(&t),
            attempt: 2,
            generation: Some(&prev_gen),
            previous_feedback: Some("fix it"),
            previous_score: Some(60),
            context_board: Some(&board),
            ..Default::default()
        };
        let ctx = StandardTaskContextBuilder.build(PgeRole::Planner, &input);

        assert_eq!(ctx["goal"], json!("improve latency"));
        assert_eq!(ctx["task_id"], json!("42"));
        assert_eq!(ctx["parent_task_id"], json!("7"));
        assert_eq!(ctx["attempt"], json!(2));
        assert_eq!(ctx["previous_feedback"], json!("fix it"));
        assert_eq!(ctx["previous_score"], json!(60));
        assert_eq!(ctx["previous_generation"], prev_gen);
        assert_eq!(ctx["context_board"], board);
        assert_eq!(ctx["is_executable"], json!(true));
    }

    #[test]
    fn generator_context_matches_legacy_shape() {
        let t = task();
        let plan = json!({"approach": "x"});
        let prev_eval = json!({"verdict": "fail"});
        let prev_gen = json!({"content": "old"});
        let input = TaskContextInput {
            task: Some(&t),
            attempt: 3,
            plan: Some(&plan),
            generation: Some(&prev_gen),
            previous_evaluation: Some(&prev_eval),
            repair_feedback: Some("repair hint"),
            ..Default::default()
        };
        let ctx = StandardTaskContextBuilder.build(PgeRole::Generator, &input);

        assert_eq!(ctx["plan"], plan);
        assert_eq!(ctx["input"], json!({"goal": "improve latency"}));
        assert_eq!(ctx["attempt"], json!(3));
        assert_eq!(ctx["previous_evaluation"], prev_eval);
        assert_eq!(ctx["previous_generation"], prev_gen);
        assert_eq!(ctx["repair_feedback"], json!("repair hint"));
        assert!(ctx.get("context_board").is_none());
    }

    #[test]
    fn evaluator_context_matches_legacy_shape() {
        let t = task();
        let plan = json!({"a": 1});
        let generation = json!({"content": "code"});
        let history = vec![json!({"verdict": "fail"})];
        let criteria = ["correctness", "tests"];
        let board = json!({"notes": []});
        let input = TaskContextInput {
            task: Some(&t),
            plan: Some(&plan),
            generation: Some(&generation),
            history: &history,
            criteria: &criteria,
            context_board: Some(&board),
            ..Default::default()
        };
        let ctx = StandardTaskContextBuilder.build(PgeRole::Evaluator, &input);

        assert_eq!(ctx["goal"], json!("improve latency"));
        assert_eq!(ctx["plan"], plan);
        assert_eq!(ctx["generation"], generation);
        assert_eq!(ctx["history"], json!(history));
        assert_eq!(ctx["criteria"], json!(["correctness", "tests"]));
        assert_eq!(ctx["context_board"], board);
        // Evaluator context never carries attempt/parent_task_id.
        assert!(ctx.get("attempt").is_none());
        assert!(ctx.get("parent_task_id").is_none());
    }

    #[test]
    fn evaluator_omits_empty_criteria() {
        let t = task();
        let input = TaskContextInput {
            task: Some(&t),
            ..Default::default()
        };
        let ctx = StandardTaskContextBuilder.build(PgeRole::Evaluator, &input);
        assert!(ctx.get("criteria").is_none());
        assert_eq!(ctx["history"], json!([]));
    }

    #[test]
    fn planner_omits_optional_fields_when_absent() {
        let t = task();
        let input = TaskContextInput {
            task: Some(&t),
            attempt: 1,
            ..Default::default()
        };
        let ctx = StandardTaskContextBuilder.build(PgeRole::Planner, &input);
        assert!(ctx.get("previous_feedback").is_none());
        assert!(ctx.get("previous_score").is_none());
        assert!(ctx.get("previous_generation").is_none());
        assert!(ctx.get("action_planner_meta").is_none());
    }
}
