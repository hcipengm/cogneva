//! Prompt 版本管理 — SemVer + diff + 回滚。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Prompt 版本。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptVersion {
    pub version: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub author: String,
    pub changelog: String,
}

/// 版本历史。
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct VersionHistory {
    pub key: String,
    pub versions: Vec<PromptVersion>,
}

impl VersionHistory {
    pub fn new(key: String) -> Self {
        Self {
            key,
            versions: vec![],
        }
    }

    pub fn add(&mut self, version: PromptVersion) {
        self.versions.push(version);
        // 按版本号排序（简单字符串排序，生产环境应使用 semver crate）
        self.versions.sort_by(|a, b| b.version.cmp(&a.version));
    }

    pub fn latest(&self) -> Option<&PromptVersion> {
        self.versions.first()
    }

    pub fn get(&self, version: &str) -> Option<&PromptVersion> {
        self.versions.iter().find(|v| v.version == version)
    }

    /// 生成两个版本之间的 diff（行级）。
    pub fn diff(&self, old: &str, new: &str) -> Option<String> {
        let old_content = self.get(old)?.content.clone();
        let new_content = self.get(new)?.content.clone();
        Some(generate_diff(&old_content, &new_content))
    }
}

fn generate_diff(old: &str, new: &str) -> String {
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();

    let mut result = String::new();
    let max_len = old_lines.len().max(new_lines.len());

    for i in 0..max_len {
        match (old_lines.get(i), new_lines.get(i)) {
            (Some(o), Some(n)) if o != n => {
                result.push_str(&format!("- {}\n+ {}\n", o, n));
            }
            (None, Some(n)) => {
                result.push_str(&format!("+ {}\n", n));
            }
            (Some(o), None) => {
                result.push_str(&format!("- {}\n", o));
            }
            _ => {}
        }
    }

    result
}
