//! Eval 数据集管理。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 单条评估用例。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalCase {
    pub id: String,
    pub name: String,
    pub input: serde_json::Value,
    pub expected_output: Option<serde_json::Value>,
    pub expected_tools: Option<Vec<String>>,
    pub tags: Vec<String>,
    pub metrics: Vec<crate::metric::EvalMetric>,
}

/// 评估数据集。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EvalDataset {
    pub name: String,
    pub description: String,
    pub cases: Vec<EvalCase>,
    pub metadata: HashMap<String, String>,
}

impl EvalDataset {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            cases: vec![],
            metadata: HashMap::new(),
        }
    }

    pub fn add_case(&mut self, case: EvalCase) {
        self.cases.push(case);
    }

    pub fn load_from_jsonl(path: &std::path::Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut cases = vec![];
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let case: EvalCase = serde_json::from_str(line)?;
            cases.push(case);
        }
        Ok(Self {
            name: path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            description: String::new(),
            cases,
            metadata: HashMap::new(),
        })
    }

    pub fn save_to_jsonl(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let mut content = String::new();
        for case in &self.cases {
            content.push_str(&serde_json::to_string(case)?);
            content.push('\n');
        }
        std::fs::write(path, content)?;
        Ok(())
    }
}
