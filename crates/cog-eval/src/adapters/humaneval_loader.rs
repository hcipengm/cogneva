//! HumanEval loader: the release shard in `eval_datasets/` -> `EvalDataset`.
//!
//! The fixed suite an evaluation gate compares two builds against has to be a
//! file that ships with the repository: a suite the evaluated side chooses is
//! not a measurement of that side. This is that file's reader.
//!
//! Why this dataset is the first one: every problem carries its own executable
//! assertion (`test`) and the name the assertion calls (`entry_point`), so a run
//! is scored by running Python and reading an exit status. A suite whose ground
//! truth is a model's opinion would put an unvalidated judge inside the gate
//! this suite exists to feed — which is the shape being replaced.
//!
//! The vendored file is the official shard byte for byte (164 problems, 214315
//! bytes, sha256 b2adeeae8b383b6f0c615746a397f25c8abe44da4afb29cd10e6c4bbf0463fd3,
//! MIT licensed, from the OpenAI HumanEval release). The fields keep their
//! official names: the grader hands `test` to a Python interpreter together with
//! the candidate's code, and a translation layer between the dataset and the
//! only thing that scores it is a place for the two to disagree.
//!
//! One thing the release leaves to its own harness and that is therefore *not*
//! in the file: a problem's `test` block defines `check(candidate)` and never
//! names the function under test — the official harness appends
//! `check(<entry_point>)` before running it. [`HumanEvalLoader::problem_to_case`]
//! bakes that call into the case, so a run is scored by executing one
//! self-contained program. Leaving it out would put a convention that only the
//! release's own runner knows between the suite and the thing that grades it —
//! and the failure it produces is a plain `NameError` that reads as a wrong
//! answer.
//!
//! Where the file has to be: at the repository root (`eval_datasets/`), because
//! the run that scores against it happens in a checkout of the repository. It is
//! deliberately *not* copied into the built image — the image ships a binary,
//! the suite is a property of a revision, and a run has to say which revision it
//! scored. The unit test below reads it from the checkout it is compiled in.

use std::path::Path;

use serde::Deserialize;

use crate::dataset::{EvalCase, EvalDataset};
use crate::metric::EvalMetric;

/// One problem, in the release's own field names.
#[derive(Debug, Deserialize)]
pub struct HumanEvalProblem {
    /// The release's identifier, `HumanEval/<n>`.
    pub task_id: String,
    /// The function signature and docstring the candidate completes.
    pub prompt: String,
    /// The reference solution. Not run by the grader; it is what makes the
    /// harness checkable without a model, see [`HumanEvalLoader::problem_to_case`].
    pub canonical_solution: String,
    /// The assertion block, appended to a candidate's code before running it.
    pub test: String,
    /// The function name `test` calls.
    pub entry_point: String,
}

pub struct HumanEvalLoader;

impl HumanEvalLoader {
    /// Read the shard at `path`.
    pub fn load_problems(path: &Path) -> anyhow::Result<EvalDataset> {
        let content = std::fs::read_to_string(path)?;
        let mut dataset = EvalDataset::new(
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        );
        dataset
            .metadata
            .insert("benchmark".into(), "humaneval".into());

        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let problem: HumanEvalProblem = serde_json::from_str(line).map_err(|e| {
                anyhow::anyhow!("{} 第 {} 行解析失败: {e}", path.display(), lineno + 1)
            })?;
            dataset.add_case(Self::problem_to_case(&problem));
        }
        Ok(dataset)
    }

    pub fn problem_to_case(problem: &HumanEvalProblem) -> EvalCase {
        EvalCase {
            // The release's own id, with the one character that cannot survive
            // being used as a name replaced. `task_id` travels back from a run
            // as `EvalOutcome::task_id`, so it has to be a string a run can
            // report verbatim; two spellings of one problem would make the
            // before/after join drop the row instead of comparing it.
            id: problem.task_id.replace('/', "-"),
            name: problem.task_id.clone(),
            input: serde_json::json!({
                "prompt": problem.prompt,
                "entry_point": problem.entry_point,
            }),
            // The canonical solution rides along so the suite can be checked
            // without a model: feeding it back must score every problem as
            // passed, and anything less means the harness is broken rather than
            // the candidate. A suite that cannot pass its own reference
            // solutions measures the harness, silently and in the direction of
            // "the model got worse".
            //
            // `test` is completed with the call the release's own harness adds:
            // the block defines `check(candidate)` and never names the function
            // under test, so a run that skipped this step would raise
            // `NameError` and be read as a wrong answer.
            expected_output: Some(serde_json::json!({
                "test": format!(
                    "{}\n\ncheck({})\n",
                    problem.test.trim_end(),
                    problem.entry_point
                ),
                "canonical_solution": problem.canonical_solution,
            })),
            expected_tools: None,
            tags: vec!["humaneval".to_string(), "code-generation".to_string()],
            // Every assertion in `test` has to hold; there is no partial credit
            // in the release's own scoring, and a threshold below 1.0 here would
            // be this crate inventing a grading rule the dataset does not have.
            metrics: vec![EvalMetric::TaskSuccessRate { threshold: 1.0 }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The shard as it is vendored in this repository.
    fn vendored_shard() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../eval_datasets/humaneval.jsonl")
    }

    /// The shipped file parses, completely, and every problem carries what the
    /// grader needs. This is the only reader of the vendored shard, so a file
    /// that stopped matching the loader fails here rather than in the gate.
    #[test]
    fn the_vendored_shard_loads_every_problem_with_its_assertions() {
        let dataset = HumanEvalLoader::load_problems(&vendored_shard()).expect("load the shard");
        assert_eq!(
            dataset.cases.len(),
            164,
            "the release shard has 164 problems; a different count means the file changed"
        );

        let mut ids: Vec<&str> = dataset.cases.iter().map(|c| c.id.as_str()).collect();
        ids.sort_unstable();
        let unique = {
            let mut deduped = ids.clone();
            deduped.dedup();
            deduped
        };
        assert_eq!(
            ids, unique,
            "two problems sharing an id would report against each other's outcome"
        );

        for case in &dataset.cases {
            let prompt = case.input.get("prompt").and_then(|v| v.as_str());
            let entry_point = case.input.get("entry_point").and_then(|v| v.as_str());
            let test = case
                .expected_output
                .as_ref()
                .and_then(|v| v.get("test"))
                .and_then(|v| v.as_str());
            assert!(
                prompt.is_some_and(|p| !p.is_empty()),
                "{}: no prompt to hand to the candidate",
                case.id
            );
            assert!(
                test.is_some_and(|t| !t.is_empty()),
                "{}: no assertion block, so nothing could score this problem",
                case.id
            );
            let entry_point = entry_point.unwrap_or_default();
            assert!(
                !entry_point.is_empty(),
                "{}: the assertions call a function by name and the name is missing",
                case.id
            );
            // The property that makes the case runnable on its own: the grading
            // program ends by calling the function the prompt asks for. The
            // release's file does not contain that call -- its own harness adds
            // it -- so this asserts the loader's half of the contract, not the
            // dataset's. Textual, because nothing in this crate runs Python.
            let test = test.unwrap_or_default();
            assert!(
                test.contains("def check("),
                "{}: the block that defines the checker is gone",
                case.id
            );
            assert!(
                test.trim_end().ends_with(&format!("check({entry_point})")),
                "{}: the grading program must end by calling `check({entry_point})`, \
                 or a run raises NameError and reports a wrong answer",
                case.id
            );
            assert!(
                case.id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
                "{}: the id has to survive being reported as a plain string",
                case.id
            );
        }

        // Both ends, so a truncated file is not mistaken for a shorter suite.
        let first = &dataset.cases[0];
        assert_eq!(first.id, "HumanEval-0");
        assert_eq!(
            first.input.get("entry_point").and_then(|v| v.as_str()),
            Some("has_close_elements")
        );
        assert_eq!(dataset.cases[163].id, "HumanEval-163");
    }
}
