//! Rhai embedded script engine runtime.

use async_trait::async_trait;
use cog_core::{SFResult, ScriptEngine, ScriptRequest, ScriptResult};
use std::sync::Mutex;

/// Rhai-backed script engine for trusted inline logic.
pub struct RhaiRuntime {
    engine: Mutex<rhai::Engine>,
}

impl RhaiRuntime {
    pub fn new() -> Self {
        let mut engine = rhai::Engine::new();
        // Restrict capabilities for safety even though Rhai is trusted inline.
        engine.set_max_expr_depths(64, 32);
        engine.set_max_string_size(1024 * 1024);
        engine.set_max_array_size(10_000);
        engine.set_max_map_size(10_000);
        Self {
            engine: Mutex::new(engine),
        }
    }

    /// Execute a closure with mutable access to the underlying Engine.
    pub fn with_engine<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut rhai::Engine) -> R,
    {
        let mut engine = self.engine.lock().unwrap();
        f(&mut engine)
    }
}

impl Default for RhaiRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ScriptEngine for RhaiRuntime {
    async fn eval(&self, req: &ScriptRequest) -> SFResult<ScriptResult> {
        let engine = self.engine.lock().unwrap();
        let mut scope = rhai::Scope::new();
        scope.push("input", req.input.clone().to_string());

        let start = std::time::Instant::now();
        let result = engine
            .eval_with_scope::<rhai::Dynamic>(&mut scope, &req.source)
            .map_err(|e| cog_core::SFError::Agent(format!("Rhai eval error: {}", e)))?;
        let duration_ms = start.elapsed().as_millis() as u64;

        let output = serde_json::json!(result.to_string());

        Ok(ScriptResult {
            output,
            duration_ms,
        })
    }

    async fn compile(&self, source: &str) -> SFResult<String> {
        let engine = self.engine.lock().unwrap();
        let ast = engine
            .compile(source)
            .map_err(|e| cog_core::SFError::Agent(format!("Rhai compile error: {}", e)))?;
        let id = uuid::Uuid::new_v4().to_string();
        // In a full implementation the AST would be cached by id.
        let _ = ast;
        Ok(id)
    }
}
