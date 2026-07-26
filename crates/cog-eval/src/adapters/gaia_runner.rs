//! GAIA 数据加载器与评分器：多步推理问题 JSONL → EvalDataset，
//! 以及 GAIA 官方 quasi-exact-match 评分逻辑（归一化后比对）。

use std::path::Path;

use serde::Deserialize;

use crate::dataset::{EvalCase, EvalDataset};
use crate::metric::EvalMetric;

#[derive(Debug, Deserialize)]
struct GaiaQuestion {
    task_id: String,
    #[serde(rename = "Question")]
    question: String,
    #[serde(rename = "Level", default)]
    level: Option<serde_json::Value>,
    #[serde(rename = "Final answer", default)]
    final_answer: Option<String>,
    #[serde(default)]
    file_name: Option<String>,
}

pub struct GaiaRunner;

impl GaiaRunner {
    /// 加载 GAIA JSONL（validation / test 元数据文件）。
    pub fn load_questions(path: &Path) -> anyhow::Result<EvalDataset> {
        let content = std::fs::read_to_string(path)?;
        let mut dataset = EvalDataset::new(
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        );
        dataset.metadata.insert("benchmark".into(), "gaia".into());

        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let q: GaiaQuestion = serde_json::from_str(line).map_err(|e| {
                anyhow::anyhow!("{} 第 {} 行解析失败: {e}", path.display(), lineno + 1)
            })?;
            let level = q
                .level
                .as_ref()
                .map(|l| match l {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| "unknown".into());
            dataset.add_case(EvalCase {
                id: format!("gaia-{}", q.task_id),
                name: format!("GAIA level {level} {}", q.task_id),
                input: serde_json::json!({
                    "question": q.question,
                    "file_name": q.file_name,
                }),
                expected_output: q.final_answer.map(serde_json::Value::String),
                expected_tools: None,
                tags: vec!["gaia".into(), format!("level-{level}")],
                metrics: vec![EvalMetric::ExactMatch],
            });
        }
        Ok(dataset)
    }

    /// GAIA 评分：归一化后精确匹配（数字按数值比对，字符串去冠词/标点/大小写）。
    /// 返回 1.0（正确）或 0.0（错误）。
    pub fn evaluate_answer(prediction: &str, ground_truth: &str) -> f64 {
        if normalize_number(prediction).is_some()
            && normalize_number(prediction) == normalize_number(ground_truth)
        {
            return 1.0;
        }
        if normalize_string(prediction) == normalize_string(ground_truth) {
            return 1.0;
        }
        // 逗号分隔列表：集合相等
        let pred_set = split_list(prediction);
        let gt_set = split_list(ground_truth);
        if !gt_set.is_empty() && pred_set == gt_set {
            return 1.0;
        }
        0.0
    }
}

fn normalize_number(s: &str) -> Option<f64> {
    let cleaned: String = s.trim().replace([',', '$', '%'], "");
    cleaned.parse::<f64>().ok()
}

fn normalize_string(s: &str) -> String {
    let lower = s.to_lowercase();
    let no_punct: String = lower
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect();
    no_punct
        .split_whitespace()
        .filter(|w| !matches!(*w, "a" | "an" | "the"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn split_list(s: &str) -> std::collections::BTreeSet<String> {
    s.split(',')
        .map(normalize_string)
        .filter(|p| !p.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_string_with_formatting_differences() {
        assert_eq!(
            GaiaRunner::evaluate_answer("The Eiffel Tower", "eiffel tower"),
            1.0
        );
        assert_eq!(GaiaRunner::evaluate_answer("Paris", "London"), 0.0);
    }

    #[test]
    fn numeric_equivalence() {
        assert_eq!(GaiaRunner::evaluate_answer("1,000", "1000"), 1.0);
        assert_eq!(GaiaRunner::evaluate_answer("$42.0", "42"), 1.0);
    }

    #[test]
    fn list_set_equality() {
        assert_eq!(
            GaiaRunner::evaluate_answer("apple, banana", "banana, apple"),
            1.0
        );
        assert_eq!(
            GaiaRunner::evaluate_answer("apple, cherry", "banana, apple"),
            0.0
        );
    }

    #[test]
    fn loads_gaia_jsonl() {
        let dir = std::env::temp_dir().join(format!(
            "gaia-test-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("metadata.jsonl");
        std::fs::write(
            &path,
            r#"{"task_id": "abc-1", "Question": "What is 2+2?", "Level": 1, "Final answer": "4", "file_name": ""}"#,
        )
        .unwrap();
        let dataset = GaiaRunner::load_questions(&path).unwrap();
        assert_eq!(dataset.cases.len(), 1);
        assert_eq!(dataset.cases[0].id, "gaia-abc-1");
        assert_eq!(
            dataset.cases[0].expected_output,
            Some(serde_json::Value::String("4".into()))
        );
        assert!(dataset.cases[0].tags.contains(&"level-1".to_string()));
        std::fs::remove_dir_all(&dir).ok();
    }
}
