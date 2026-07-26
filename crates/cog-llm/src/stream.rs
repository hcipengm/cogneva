use bytes::Bytes;
use cog_core::StreamEvent;
use eventsource_stream::Eventsource;
use futures::{
    io::{AsyncBufReadExt, BufReader},
    Stream, StreamExt, TryStreamExt,
};
use std::pin::Pin;

pub type LLMStream = Pin<Box<dyn Stream<Item = StreamEvent> + Send>>;

/// Parse SSE (Server-Sent Events) stream.
/// Uses eventsource_stream crate over a BufReader-backed line stream.
pub fn parse_sse_stream(
    bytes: impl Stream<Item = Result<Bytes, cog_core::SFError>> + Send + 'static,
) -> impl Stream<Item = StreamEvent> + Send {
    let reader = BufReader::new(bytes.map_err(std::io::Error::other).into_async_read());

    reader.lines().eventsource().map(|event| match event {
        Ok(ev) => parse_sse_event(&ev.data),
        Err(e) => StreamEvent::Error {
            error: format!("SSE error: {}", e),
            timestamp: chrono::Utc::now(),
        },
    })
}

fn parse_sse_event(data: &str) -> StreamEvent {
    if data == "[DONE]" {
        return StreamEvent::Done {
            timestamp: chrono::Utc::now(),
        };
    }

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(data) {
        if let Some(delta) = json
            .pointer("/choices/0/delta/content")
            .and_then(|v| v.as_str())
        {
            return StreamEvent::TextDelta {
                delta: delta.to_string(),
                timestamp: chrono::Utc::now(),
            };
        }
        if let Some(delta) = json
            .pointer("/choices/0/delta/reasoning_content")
            .and_then(|v| v.as_str())
        {
            return StreamEvent::ThinkingDelta {
                delta: delta.to_string(),
                timestamp: chrono::Utc::now(),
            };
        }
        if let Some(usage) = json.get("usage") {
            if let (Some(prompt), Some(completion), Some(total)) = (
                usage
                    .get("prompt_tokens")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32),
                usage
                    .get("completion_tokens")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32),
                usage
                    .get("total_tokens")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32),
            ) {
                return StreamEvent::Usage {
                    prompt_tokens: prompt,
                    completion_tokens: completion,
                    total_tokens: total,
                    timestamp: chrono::Utc::now(),
                };
            }
        }
    }

    StreamEvent::TextDelta {
        delta: data.to_string(),
        timestamp: chrono::Utc::now(),
    }
}
