//! Prompt 注册中心 — 按 `domain:purpose` 索引所有 prompt。
//! 支持按 `task_type:action` 命名空间查询，例如：
//! - `self_review:critique` — SelfReview 的 critique prompt
//! - `skill_extractor:extraction` — SkillExtractor 的提取 prompt
//! - `evaluator:system` — EvaluatorAgent 的 system prompt

use crate::version::{PromptVersion, VersionHistory};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Prompt 来源。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptSource {
    FileSystem { path: String },
    Database { table: String, id: String },
    Remote { url: String },
}

/// 单条 prompt 条目。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptEntry {
    /// 唯一标识，格式 `domain:purpose`（如 `self_review:critique`）。
    pub key: String,
    /// Prompt 正文（支持 Jinja2 模板语法）。
    pub content: String,
    /// 版本号（SemVer）。
    pub version: String,
    /// 描述（人类可读）。
    pub description: String,
    /// 适用领域标签（如 `["backend", "tests"]`）。
    pub tags: Vec<String>,
    /// 来源。
    pub source: PromptSource,
    /// 是否激活（A/B 测试时可能有多版本，仅一个激活）。
    pub active: bool,
    /// A/B 测试分组（可选）。
    pub ab_group: Option<String>,
}

/// Prompt 注册中心。
#[derive(Debug, Default)]
pub struct PromptRegistry {
    entries: HashMap<String, PromptEntry>,
    /// 每个 prompt key 的版本历史。
    version_histories: HashMap<String, VersionHistory>,
}

impl PromptRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一条 prompt，同时追加到版本历史。
    pub fn register(&mut self, entry: PromptEntry) {
        let key = entry.key.clone();
        // 追加版本历史
        let history = self
            .version_histories
            .entry(key.clone())
            .or_insert_with(|| VersionHistory::new(key.clone()));
        history.add(PromptVersion {
            version: entry.version.clone(),
            content: entry.content.clone(),
            created_at: chrono::Utc::now(),
            author: "system".into(),
            changelog: format!("Registered prompt {}", entry.key),
        });
        self.entries.insert(key, entry);
    }

    /// 按 key 查询 prompt。
    pub fn get(&self, key: &str) -> Option<&PromptEntry> {
        self.entries.get(key)
    }

    /// 按 key 查询可变引用。
    pub fn get_mut(&mut self, key: &str) -> Option<&mut PromptEntry> {
        self.entries.get_mut(key)
    }

    /// 按标签过滤。
    pub fn by_tag(&self, tag: &str) -> Vec<&PromptEntry> {
        self.entries
            .values()
            .filter(|e| e.tags.contains(&tag.to_string()))
            .collect()
    }

    /// 按前缀匹配（如 `self_review:` 返回所有 SelfReview 相关 prompt）。
    pub fn by_prefix(&self, prefix: &str) -> Vec<&PromptEntry> {
        self.entries
            .values()
            .filter(|e| e.key.starts_with(prefix))
            .collect()
    }

    /// 列出所有 key。
    pub fn keys(&self) -> Vec<&String> {
        self.entries.keys().collect()
    }

    /// 批量注册（从 YAML/JSON 反序列化）。
    pub fn batch_register(&mut self, entries: Vec<PromptEntry>) {
        for e in entries {
            self.register(e);
        }
    }

    /// 激活指定 key 的 prompt，同时禁用同 key 的其他版本。
    pub fn activate(&mut self, key: &str) -> bool {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.active = true;
            // 找到同 domain:purpose 的其他版本并禁用
            let prefix = key.split(':').next().unwrap_or(key);
            for (k, e) in self.entries.iter_mut() {
                if k != key && k.starts_with(prefix) {
                    e.active = false;
                }
            }
            true
        } else {
            false
        }
    }

    /// 获取指定 prompt 的版本历史。
    pub fn version_history(&self, key: &str) -> Option<&VersionHistory> {
        self.version_histories.get(key)
    }

    /// 列出某 prompt 的所有版本号。
    pub fn list_versions(&self, key: &str) -> Vec<String> {
        self.version_histories
            .get(key)
            .map(|h| h.versions.iter().map(|v| v.version.clone()).collect())
            .unwrap_or_default()
    }

    /// 获取指定版本的 prompt 内容（如果该版本存在于历史中）。
    pub fn get_version(&self, key: &str, version: &str) -> Option<PromptVersion> {
        self.version_histories.get(key)?.get(version).cloned()
    }

    /// 对比两个版本的 diff。
    pub fn diff_versions(&self, key: &str, old: &str, new: &str) -> Option<String> {
        self.version_histories.get(key)?.diff(old, new)
    }

    /// 回滚到指定版本：将历史中的该版本内容复制为当前激活版本。
    pub fn rollback(&mut self, key: &str, version: &str) -> bool {
        let historical = match self.version_histories.get(key).and_then(|h| h.get(version)) {
            Some(v) => v.clone(),
            None => return false,
        };
        if let Some(entry) = self.entries.get_mut(key) {
            entry.content = historical.content;
            entry.version = historical.version.clone();
            // 也追加到版本历史，标记为回滚操作
            if let Some(history) = self.version_histories.get_mut(key) {
                history.add(PromptVersion {
                    version: format!(
                        "{}-rollback-{}",
                        historical.version,
                        chrono::Utc::now().timestamp()
                    ),
                    content: entry.content.clone(),
                    created_at: chrono::Utc::now(),
                    author: "system".into(),
                    changelog: format!("Rollback to version {}", version),
                });
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_crud() {
        let mut reg = PromptRegistry::new();
        reg.register(PromptEntry {
            key: "self_review:critique".into(),
            content: "Critique this.".into(),
            version: "1.0.0".into(),
            description: "Self-review critique prompt".into(),
            tags: vec!["agent".into()],
            source: PromptSource::FileSystem {
                path: "/dev/null".into(),
            },
            active: true,
            ab_group: None,
        });

        assert!(reg.get("self_review:critique").is_some());
        assert!(reg.get("missing").is_none());
    }
}
