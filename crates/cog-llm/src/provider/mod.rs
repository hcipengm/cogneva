pub mod anthropic;
pub mod google;
pub mod media;
pub mod ollama;
pub mod openai;
pub mod responses;

use cog_core::LlmClient as LLMProvider;

// 统一 LLM Provider trait；实际契约定义在 cog-core。

use std::time::Duration;

/// 一条流式响应从头到尾的硬上限：10 分钟。
///
/// 它同时是 HTTP 请求的上限，也是「慢推理流永远占着整支小队」这道墙。两个
/// provider 曾各写一份 600；改一处就静默地让两条路各走各的，而读数面看不出
/// 差别。
pub const STREAM_TOTAL_TIMEOUT: Duration = Duration::from_secs(600);

/// 首个事件之后，两次事件之间允许的最大间隔：60s。
///
/// 首个事件之前用总上限——长推理窗口是正常的；之后收紧成这个数，是因为
/// 「上游在想」与「流卡住了」必须能分开，否则一次卡死要等满总上限。
pub const STREAM_EVENT_TIMEOUT: Duration = Duration::from_secs(60);

/// `chat()` 消费一条流的上限：3 分钟。
///
/// 有上游会开流之后既不结束也不发数据；不设这道墙，调用方要等满总上限才
/// 拿到错误。3 分钟以内没有任何事件结束的流，按不可用处理。
pub const CHAT_CONSUME_TIMEOUT: Duration = Duration::from_secs(180);
