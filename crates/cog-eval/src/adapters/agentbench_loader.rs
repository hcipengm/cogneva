//! AgentBench 数据加载器：8 环境 task JSONL → EvalDataset。
//! 兼容官方格式（每行一个 task：id/index + env + instruction/prompt + expected/answer）。

use std::path::Path;

use serde::Deserialize;

use crate::dataset::{EvalCase, EvalDataset};
use crate::metric::EvalMetric;

#[derive(Debug, Deserialize)]
struct AgentBenchTask {
    #[serde(alias = "index", alias = "task_id")]
    id: serde_json::Value,
    #[serde(default)]
    env: Option<String>,
    #[serde(alias = "prompt", alias = "instruction", alias = "question")]
    instruction: String,
    #[serde(default, alias = "answer", alias = "expected", alias = "ground_truth")]
    expected: Option<serde_json::Value>,
}

pub struct AgentBenchLoader;

impl AgentBenchLoader {
    /// 加载 AgentBench task JSONL（可以是单环境文件或多环境合并文件）。
    pub fn load_tasks(path: &Path) -> anyhow::Result<EvalDataset> {
        let content = std::fs::read_to_string(path)?;
        let mut dataset = EvalDataset::new(
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        );
        dataset
            .metadata
            .insert("benchmark".into(), "agentbench".into());

        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let task: AgentBenchTask = serde_json::from_str(line).map_err(|e| {
                anyhow::anyhow!("{} 第 {} 行解析失败: {e}", path.display(), lineno + 1)
            })?;
            let env = task.env.clone().unwrap_or_else(|| "unknown".into());
            let id = match &task.id {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            dataset.add_case(EvalCase {
                id: format!("agentbench-{env}-{id}"),
                name: format!("{env} task {id}"),
                input: serde_json::json!({ "instruction": task.instruction, "env": env }),
                expected_output: task.expected,
                expected_tools: None,
                tags: vec!["agentbench".into(), env],
                metrics: vec![
                    EvalMetric::TaskSuccessRate { threshold: 0.5 },
                    EvalMetric::StepAccuracy { threshold: 0.5 },
                ],
            });
        }
        Ok(dataset)
    }

    /// 环境分布统计（用于检查 8 环境覆盖）。
    pub fn env_distribution(dataset: &EvalDataset) -> std::collections::HashMap<String, usize> {
        let mut dist = std::collections::HashMap::new();
        for case in &dataset.cases {
            let env = case
                .tags
                .iter()
                .find(|t| *t != "agentbench")
                .cloned()
                .unwrap_or_else(|| "unknown".into());
            *dist.entry(env).or_default() += 1;
        }
        dist
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_jsonl_with_aliases() {
        let dir = std::env::temp_dir().join(format!("agentbench-test-{}", uuid_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("os.jsonl");
        std::fs::write(
            &path,
            concat!(
                r#"{"index": 1, "env": "os", "instruction": "list files", "answer": "ok"}"#,
                "\n",
                r#"{"task_id": "db-7", "prompt": "select 1", "expected": "1"}"#,
                "\n",
            ),
        )
        .unwrap();
        let dataset = AgentBenchLoader::load_tasks(&path).unwrap();
        assert_eq!(dataset.cases.len(), 2);
        assert_eq!(dataset.cases[0].id, "agentbench-os-1");
        assert_eq!(dataset.cases[1].id, "agentbench-unknown-db-7");
        let dist = AgentBenchLoader::env_distribution(&dataset);
        assert_eq!(dist.get("os"), Some(&1));
        assert_eq!(dist.get("unknown"), Some(&1));
        std::fs::remove_dir_all(&dir).ok();
    }

    fn uuid_v4() -> String {
        format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}
