//! Prompt 模板引擎 — Jinja2 风格变量替换。
//! 支持 `{{ variable }}` 语法，可嵌套对象访问 `{{ obj.field }}`。

use serde::Serialize;
use std::collections::HashMap;

/// 模板变量集合。
#[derive(Debug, Clone, Default, Serialize)]
pub struct TemplateVars {
    #[serde(flatten)]
    inner: HashMap<String, serde_json::Value>,
}

impl TemplateVars {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with<K: Into<String>, V: Serialize>(mut self, key: K, value: V) -> Self {
        let key = key.into();
        match serde_json::to_value(value) {
            Ok(v) => {
                self.inner.insert(key, v);
            }
            Err(e) => {
                tracing::warn!("Failed to serialize template var '{}': {}", key, e);
            }
        }
        self
    }

    pub fn insert<K: Into<String>, V: Serialize>(&mut self, key: K, value: V) {
        let key = key.into();
        if let Ok(v) = serde_json::to_value(value) {
            self.inner.insert(key, v);
        }
    }

    pub fn extend(&mut self, other: TemplateVars) {
        self.inner.extend(other.inner);
    }
}

/// 轻量模板引擎。
pub struct TemplateEngine {
    #[allow(dead_code)]
    tera: tera::Tera,
}

impl Default for TemplateEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateEngine {
    pub fn new() -> Self {
        let mut tera = tera::Tera::default();
        tera.autoescape_on(vec![]);
        Self { tera }
    }

    /// Render a template string with variables.
    pub fn render(&self, template: &str, vars: &TemplateVars) -> anyhow::Result<String> {
        let ctx = tera::Context::from_serialize(vars)?;
        // tera::Tera::one_off requires a mutable reference, so we use a temporary Tera instance
        let mut tmp = tera::Tera::default();
        tmp.autoescape_on(vec![]);
        Ok(tmp.render_str(template, &ctx)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_replacement() {
        let engine = TemplateEngine::new();
        let vars = TemplateVars::new().with("name", "Alice");
        let result = engine.render("Hello {{ name }}!", &vars).unwrap();
        assert_eq!(result, "Hello Alice!");
    }

    #[test]
    fn test_nested_object() {
        let engine = TemplateEngine::new();
        let vars = TemplateVars::new().with("task", serde_json::json!({"type": "code_review"}));
        let result = engine.render("Task: {{ task.type }}", &vars).unwrap();
        assert_eq!(result, "Task: code_review");
    }
}
