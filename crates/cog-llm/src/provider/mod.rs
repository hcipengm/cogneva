pub mod anthropic;
pub mod google;
pub mod media;
pub mod ollama;
pub mod openai;
pub mod responses;

use cog_core::LlmClient as LLMProvider;

// 统一 LLM Provider trait；实际契约定义在 cog-core。

use std::time::Duration;

/// 首个事件到达之前，允许的静默：10 分钟。
///
/// 这一条量的是**没有进展**，不是干了多久：推理窗口长的上游在吐出第一个
/// 字节之前可以安静很久，那是它在干活；安静到十分钟还没有第一个事件，才按
/// 「这条流不可用」处理。曾经这里还有一条「整段流的硬上限」——它把一条仍在
/// 持续吐字的流按钟点掐断，长推理的产出连同已经收到的部分一起扔掉，那正是
/// 要拆的形状（总量不是进展的度量，间隔才是）。
// cap: bound — 首事件之前的静默间隔；量的是没有进展，不是总时长
pub const STREAM_FIRST_EVENT_GAP: Duration = Duration::from_secs(600);

/// 首个事件之后，两次事件之间允许的静默：60s。
///
/// 同样的道理：有事件到达就是在干活；安静超过这个数，才按「流卡住了」收尾。
/// 首事件之前用 [`STREAM_FIRST_EVENT_GAP`]——长推理窗口是正常的，之后收紧，
/// 是因为「上游在想」与「流卡住了」到这里必须分得开。
// cap: bound — 首事件之后的事件间隔；量的是没有进展，不是总时长
pub const STREAM_EVENT_TIMEOUT: Duration = Duration::from_secs(60);
