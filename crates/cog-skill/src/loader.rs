//! Skill loader — parse SKILL.md, load metadata, and build SkillDef.

use cog_core::{SFResult, SkillDef, SkillMetadata};
use std::path::Path;

use crate::manifest::{parse_skill_md, read_meta_json};

/// Load a single skill from its directory.
pub async fn load_skill(dir: &Path) -> SFResult<SkillDef> {
    let skill_md_path = dir.join("SKILL.md");
    let skill_md_content = tokio::fs::read_to_string(&skill_md_path)
        .await
        .map_err(|e| {
            cog_core::SFError::Agent(format!("read SKILL.md failed for {}: {}", dir.display(), e))
        })?;

    let (frontmatter, _body) = parse_skill_md(&skill_md_content)?;
    let meta = read_meta_json(dir).await?;

    let skill_id = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();

    let metadata = SkillMetadata {
        id: skill_id.clone(),
        name: if frontmatter.name.is_empty() {
            skill_id.clone()
        } else {
            frontmatter.name.clone()
        },
        description: frontmatter.description.clone(),
        version: if meta.version.is_empty() {
            "0.1.0".into()
        } else {
            meta.version
        },
    };

    Ok(SkillDef {
        metadata,
        skill_md: skill_md_content,
        frontmatter,
    })
}
