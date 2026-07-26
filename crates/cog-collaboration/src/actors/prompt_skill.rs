//! Prompt skill 解析（docs/20250605_squad_pge_architecture_refactor.md §4.2/4.3）。
//!
//! 把 PGE 角色的 prompt 模板与输出 schema 从代码硬编码迁移为 skill 配置：
//! - `resolve_prompt_skill` 从 `ExternalSkillRegistry` 解析 SKILL.md 正文作为
//!   prompt 模板，可选加载同目录 `output_schema.json` 作为输出 schema 指导；
//! - 解析失败（skill 不存在 / 资源缺失）一律降级为 None，调用方回退内置行为；
//! - schema 优先级：算子显式配置（pge.schemas）> skill 声明 > actor 内置。

use std::sync::Arc;

use cog_core::{ExternalSkillRegistry, PromptSkillDef};

/// skill 目录内可选的输出 schema 资源文件。
pub const SKILL_SCHEMA_RESOURCE: &str = "output_schema.json";

/// 从 skill registry 解析一个角色的 prompt skill。
///
/// 返回 None 的情形（均记 info 日志，不算错误）：
/// - skill_id 未配置；
/// - registry 不可用；
/// - skill 不存在或加载失败。
pub async fn resolve_prompt_skill(
    registry: Option<&Arc<dyn ExternalSkillRegistry>>,
    skill_id: Option<&str>,
    role: &str,
) -> Option<PromptSkillDef> {
    let skill_id = skill_id?;
    let registry = registry?;
    let def = match registry.resolve(skill_id).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(skill_id, role, error = %e, "PGE prompt skill 解析失败，回退内置 prompt");
            return None;
        }
    };
    let output_schema = match registry
        .load_resource(skill_id, SKILL_SCHEMA_RESOURCE)
        .await
    {
        Ok(text) => match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(skill_id, role, error = %e, "skill output_schema.json 非法，忽略");
                None
            }
        },
        Err(_) => None,
    };
    tracing::info!(
        skill_id,
        role,
        schema = output_schema.is_some(),
        "PGE prompt skill 已加载"
    );
    Some(PromptSkillDef {
        skill_id: skill_id.to_string(),
        role: role.to_string(),
        prompt_template: def.skill_md,
        output_schema,
        use_structured: None,
    })
}

/// 把 prompt skill 注入 actor 的 prompt ctx。
///
/// - `ctx["skill"]`：skill 身份信息；
/// - `ctx["skill_instructions"]`：SKILL.md 正文（prompt 模板）；
/// - skill 声明了 schema 且算子未显式覆盖时，写入 `output_schema` + `response_format`；
/// - `use_structured == Some(false)` 时显式标记自由文本输出（调用方宽松解析兜底）。
pub fn apply_prompt_skill(
    ctx: &mut serde_json::Value,
    skill: &PromptSkillDef,
    operator_schema: Option<&serde_json::Value>,
) {
    ctx["skill"] = serde_json::json!({
        "id": skill.skill_id,
        "role": skill.role,
    });
    if !skill.prompt_template.trim().is_empty() {
        ctx["skill_instructions"] = serde_json::json!(skill.prompt_template);
    }
    if operator_schema.is_none() {
        if let Some(ref schema) = skill.output_schema {
            ctx["output_schema"] = schema.clone();
            ctx["response_format"] = serde_json::json!("json");
        }
    }
    if skill.use_structured == Some(false) {
        ctx["response_format"] = serde_json::json!("text");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_injects_skill_context() {
        let mut ctx = serde_json::json!({"goal": "g"});
        let skill = PromptSkillDef {
            skill_id: "pge_planner".into(),
            role: "planner".into(),
            prompt_template: "You plan.".into(),
            output_schema: Some(serde_json::json!({"type": "object"})),
            use_structured: None,
        };
        apply_prompt_skill(&mut ctx, &skill, None);
        assert_eq!(ctx["skill"]["id"], "pge_planner");
        assert_eq!(ctx["skill_instructions"], "You plan.");
        assert_eq!(ctx["output_schema"]["type"], "object");
        assert_eq!(ctx["response_format"], "json");
    }

    #[test]
    fn operator_schema_wins_over_skill_schema() {
        let mut ctx = serde_json::json!({});
        let skill = PromptSkillDef {
            skill_id: "s".into(),
            role: "planner".into(),
            prompt_template: String::new(),
            output_schema: Some(serde_json::json!({"type": "object", "title": "skill"})),
            use_structured: None,
        };
        let operator = serde_json::json!({"type": "object", "title": "operator"});
        apply_prompt_skill(&mut ctx, &skill, Some(&operator));
        // 算子 schema 存在时 skill schema 不写入（由 actor 自己的 with_output_schema 路径写入算子版本）。
        assert!(ctx.get("output_schema").is_none());
    }

    #[test]
    fn free_text_output_marks_response_format() {
        let mut ctx = serde_json::json!({});
        let skill = PromptSkillDef {
            skill_id: "s".into(),
            role: "generator".into(),
            prompt_template: String::new(),
            output_schema: None,
            use_structured: Some(false),
        };
        apply_prompt_skill(&mut ctx, &skill, None);
        assert_eq!(ctx["response_format"], "text");
    }
}
