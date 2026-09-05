//! SWE-bench 数据加载器：instance JSONL → EvalDataset，支持 Rust 子集过滤。
//! 兼容官方 SWE-bench 格式（instance_id / repo / base_commit / problem_statement / change）。

use std::path::Path;

use serde::Deserialize;

use crate::dataset::{EvalCase, EvalDataset};
use crate::metric::EvalMetric;

#[derive(Debug, Deserialize)]
pub struct SweBenchInstance {
    pub instance_id: String,
    pub repo: String,
    #[serde(default)]
    pub base_commit: Option<String>,
    #[serde(default)]
    pub problem_statement: Option<String>,
    /// SWE-bench 官方字段：gold patch（unified diff 文本）。外部数据集字段名，勿改。
    #[serde(default)]
    pub patch: Option<String>,
    /// 官方字段：修复后应通过的测试（JSON 字符串数组）。
    #[serde(rename = "FAIL_TO_PASS", default)]
    pub fail_to_pass: Option<serde_json::Value>,
    #[serde(rename = "PASS_TO_PASS", default)]
    pub pass_to_pass: Option<serde_json::Value>,
}

pub struct SweBenchRunner;

/// 已知 Rust 仓库前缀（SWE-bench 官方无 Rust 子集，自建数据集按 repo 识别）。
const KNOWN_RUST_REPOS: &[&str] = &[
    "tokio-rs/",
    "rust-lang/",
    "serde-rs/",
    "clap-rs/",
    "BurntSushi/",
    "rayon-rs/",
    "rust-analyzer/",
    "sharkdp/",
    "cross-rs/",
];

impl SweBenchRunner {
    /// 加载 SWE-bench instance JSONL。
    pub fn load_instances(path: &Path) -> anyhow::Result<EvalDataset> {
        let content = std::fs::read_to_string(path)?;
        let mut dataset = EvalDataset::new(
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        );
        dataset
            .metadata
            .insert("benchmark".into(), "swe-bench".into());

        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let inst: SweBenchInstance = serde_json::from_str(line).map_err(|e| {
                anyhow::anyhow!("{} 第 {} 行解析失败: {e}", path.display(), lineno + 1)
            })?;
            dataset.add_case(Self::instance_to_case(&inst));
        }
        Ok(dataset)
    }

    pub fn instance_to_case(inst: &SweBenchInstance) -> EvalCase {
        let is_rust = Self::is_rust_repo(&inst.repo);
        let mut tags = vec!["swe-bench".to_string(), inst.repo.clone()];
        if is_rust {
            tags.push("rust".into());
        }
        EvalCase {
            id: format!("swebench-{}", inst.instance_id),
            name: format!("{} @ {}", inst.repo, inst.instance_id),
            input: serde_json::json!({
                "repo": inst.repo,
                "base_commit": inst.base_commit,
                "problem_statement": inst.problem_statement,
                "fail_to_pass": inst.fail_to_pass,
                "pass_to_pass": inst.pass_to_pass,
            }),
            // 参考 gold patch（diff）用于评估比对，不作为 agent 输入
            expected_output: inst.patch.clone().map(serde_json::Value::String),
            expected_tools: None,
            tags,
            metrics: vec![
                EvalMetric::ResolveRate { threshold: 1.0 },
                EvalMetric::RegressionRate { threshold: 0.05 },
            ],
        }
    }

    fn is_rust_repo(repo: &str) -> bool {
        KNOWN_RUST_REPOS.iter().any(|p| repo.starts_with(p))
    }

    /// Rust 子集过滤：保留 Rust 仓库的 case（自建 Rust SWE-bench 的核心）。
    pub fn filter_rust_instances(dataset: &EvalDataset) -> EvalDataset {
        let mut out = EvalDataset::new(format!("{}-rust", dataset.name));
        out.description = format!("Rust subset of {}", dataset.name);
        out.metadata = dataset.metadata.clone();
        out.metadata.insert("subset".into(), "rust".into());
        out.cases = dataset
            .cases
            .iter()
            .filter(|c| c.tags.iter().any(|t| t == "rust"))
            .cloned()
            .collect();
        out
    }

    /// 解析 docker 内 `cargo test` 输出，统计 fail_to_pass 测试中通过的比例。
    /// `passed_tests` 为 cargo test 输出中状态为 ok 的测试名列表。
    pub fn resolve_score(instance_json: &serde_json::Value, passed_tests: &[String]) -> f64 {
        let fail_to_pass: Vec<String> = instance_json
            .get("fail_to_pass")
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    serde_json::from_str(s).ok()
                } else {
                    serde_json::from_value(v.clone()).ok()
                }
            })
            .unwrap_or_default();
        if fail_to_pass.is_empty() {
            return 0.0;
        }
        let passed: std::collections::HashSet<&String> = passed_tests.iter().collect();
        let resolved = fail_to_pass.iter().filter(|t| passed.contains(t)).count();
        resolved as f64 / fail_to_pass.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_jsonl(lines: &[&str]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "swebench-test-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("instances.jsonl");
        std::fs::write(&path, lines.join("\n")).unwrap();
        path
    }

    #[test]
    fn loads_and_filters_rust_subset() {
        let path = write_jsonl(&[
            r#"{"instance_id": "tokio-1", "repo": "tokio-rs/tokio", "problem_statement": "fix bug", "change": "diff..."}"#,
            r#"{"instance_id": "django-1", "repo": "django/django", "problem_statement": "fix bug"}"#,
        ]);
        let dataset = SweBenchRunner::load_instances(&path).unwrap();
        assert_eq!(dataset.cases.len(), 2);
        let rust = SweBenchRunner::filter_rust_instances(&dataset);
        assert_eq!(rust.cases.len(), 1);
        assert_eq!(rust.cases[0].id, "swebench-tokio-1");
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn resolve_score_counts_fail_to_pass() {
        let inst = serde_json::json!({
            "fail_to_pass": "[\"test_a\", \"test_b\", \"test_c\"]"
        });
        let score =
            SweBenchRunner::resolve_score(&inst, &["test_a".to_string(), "test_b".to_string()]);
        assert!((score - 2.0 / 3.0).abs() < 1e-9);
    }
}
