pub mod anthropic;
pub mod google;
pub mod ollama;
pub mod openai;

use cog_core::LlmClient as LLMProvider;

// 统一 LLM Provider trait；实际契约定义在 cog-core。
