//! 平台对每个智能体说的同一段话。
//!
//! 智能体是通用的：同一个内核、同一套工具、同一个部署里的任务与对话。它这一轮
//! 做什么，由它拿到的那件活或那句话决定；它是什么，不由一段写在代码里的自我
//! 介绍决定，也不由按角色分岔的预设决定。所以这里只有一段，不按 `role` 分岔。
//!
//! 这段指示是**声明**而不是常量：进程起来后把发布出来的提示词本子交进来，
//! 每次运行开头现取一次。改了声明，下一次运行就读到新的那句，不用发版。
//!
//! 没接上本子的进程（库的单元测试、独立跑的运行时）就是没有这段——空着，
//! 不在这里编一份兜底：兜底会让「改声明」看起来生效、实际没有。

use std::sync::{Arc, OnceLock};

use cog_core::PromptProvider;

static PROVIDER: OnceLock<Arc<dyn PromptProvider>> = OnceLock::new();

/// 把进程的提示词本子交给运行时；装配根在插件起来之后调一次。
pub fn install(provider: Arc<dyn PromptProvider>) {
    let _ = PROVIDER.set(provider);
}

/// 这一轮该给智能体的那段话。本子没接上、或没有这一条时是 `None`。
pub fn text() -> Option<String> {
    let pm = PROVIDER.get()?;
    pm.get("agent:default")
        .filter(|text| !text.trim().is_empty())
}
