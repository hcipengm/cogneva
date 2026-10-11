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
    /// 基准自己带的旋钮：来源 rev、镜像名、`fail_to_pass`、可用工具清单等。
    /// 与 `input` 分开是因为 `input` 是**题面**（喂给外壳的那段），这些是**评测台**
    /// 起环境、判分时要读的东西——外壳不该看见它们（§1.3 第 4 条：外壳看不到判分器）。
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl EvalCase {
    /// 交给外壳的那一份：题面与它的身份，别的都留在外面。
    ///
    /// 答案键（`expected_output`）、期望工具（`expected_tools`）、评测台起环境与判分用的
    /// 旋钮（`metadata`：镜像名、来源 rev、`fail_to_pass`、可用工具清单）以及计分表
    /// （`tags` / `metrics`）都只给评测台读。外壳拿到的那个值里就没有这些字段的内容，
    /// 所以「看不到答案」不靠外壳自觉，也不靠谁记得别读——判分器那一侧仍读**原来那一份**，
    /// 收窄的是外壳的输入，不是判分的依据。
    pub fn shell_view(&self) -> Self {
        Self {
            id: self.id.clone(),
            name: self.name.clone(),
            input: self.input.clone(),
            expected_output: None,
            expected_tools: None,
            tags: Vec::new(),
            metrics: Vec::new(),
            metadata: HashMap::new(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_case_written_before_metadata_existed_still_reads() {
        // 加键不改旧形状：判分/起环境要读的旋钮是新加的，但盘上已经躺着的
        // 语料没有这一项，读不动就等于把老语料全废掉。
        let old = r#"{
            "id": "hle_00001",
            "name": "hle_00001",
            "input": "Question text",
            "expected_output": "Answer",
            "expected_tools": null,
            "tags": ["hle"],
            "metrics": []
        }"#;
        let case: EvalCase = serde_json::from_str(old).unwrap();
        assert_eq!(case.id, "hle_00001");
        assert!(case.metadata.is_empty());

        // 新写的这条把旋钮带上，读回来要还是同一份。
        let mut with = case.clone();
        with.metadata.insert("base_commit".into(), "abc123".into());
        let json = serde_json::to_string(&with).unwrap();
        let back: EvalCase = serde_json::from_str(&json).unwrap();
        assert_eq!(back.metadata.get("base_commit").unwrap(), "abc123");
    }
}
