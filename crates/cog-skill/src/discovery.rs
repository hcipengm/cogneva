//! Skill discovery — scan directories for `SKILL.md` files.

use cog_core::SFResult;
use std::path::{Path, PathBuf};

/// Scan `dir` for subdirectories containing `SKILL.md`.
pub async fn discover_skills(dir: &Path) -> SFResult<Vec<PathBuf>> {
    let mut skills = Vec::new();

    if !dir.exists() {
        tracing::warn!("Skills directory does not exist: {}", dir.display());
        return Ok(skills);
    }

    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|e| cog_core::SFError::Agent(format!("read skills dir failed: {}", e)))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| cog_core::SFError::Agent(format!("read dir entry failed: {}", e)))?
    {
        let path = entry.path();
        if path.is_dir() {
            let skill_md = path.join("SKILL.md");
            if skill_md.exists() {
                skills.push(path);
            }
        }
    }

    Ok(skills)
}

/// Discover skills from multiple directories (higher priority first).
pub async fn discover_all(dirs: &[PathBuf]) -> SFResult<Vec<(PathBuf, String)>> {
    let mut results = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for dir in dirs {
        let skills = discover_skills(dir).await?;
        for skill_path in skills {
            let skill_id = skill_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if seen.insert(skill_id.clone()) {
                results.push((skill_path, skill_id));
            }
        }
    }

    Ok(results)
}
