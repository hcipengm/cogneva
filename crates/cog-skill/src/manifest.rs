//! Skill manifest parsing — SKILL.md frontmatter and `_meta.json`.

use cog_core::{SFResult, SkillFrontmatter};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// `_meta.json` content (publishing metadata).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetaJson {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    pub slug: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Parse `SKILL.md` frontmatter and body.
/// Returns `(frontmatter, body)`.
pub fn parse_skill_md(content: &str) -> SFResult<(SkillFrontmatter, String)> {
    let lines: Vec<&str> = content.lines().collect();

    if lines.is_empty() || lines[0].trim() != "---" {
        // No frontmatter — treat entire content as body.
        return Ok((SkillFrontmatter::default(), content.to_string()));
    }

    let mut end_idx = None;
    for (i, line) in lines.iter().enumerate().skip(1) {
        if line.trim() == "---" {
            end_idx = Some(i);
            break;
        }
    }

    let end_idx = end_idx.ok_or_else(|| {
        cog_core::SFError::Agent("SKILL.md frontmatter missing closing ---".into())
    })?;

    let frontmatter_text = lines[1..end_idx].join("\n");
    let body = lines[end_idx + 1..].join("\n");

    let frontmatter: SkillFrontmatter = serde_yaml::from_str(&frontmatter_text).map_err(|e| {
        cog_core::SFError::Agent(format!("SKILL.md frontmatter parse error: {}", e))
    })?;

    Ok((frontmatter, body))
}

/// Read `_meta.json` from a skill directory.
pub async fn read_meta_json(dir: &Path) -> SFResult<MetaJson> {
    let path = dir.join("_meta.json");
    if !path.exists() {
        return Ok(MetaJson::default());
    }
    let content = tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| cog_core::SFError::Agent(format!("read _meta.json failed: {}", e)))?;
    let meta: MetaJson = serde_json::from_str(&content)
        .map_err(|e| cog_core::SFError::Agent(format!("parse _meta.json failed: {}", e)))?;
    Ok(meta)
}
