//! `AnthropicProvider` 的流式解析：这是业务路径在 anthropic 面上唯一的取字与取工具调用
//! 的地方，而它此前没有任何夹具。
//!
//! 为什么现在补：平台的路由里 anthropic 面被排在 openai 面之后，openai 面在网关池里
//! 一个能接的成员都没有时，整条业务与进化链路会落到这一面来。落地之前这一面只有一次
//! 手工 `curl`（HTTP 200），没有任何东西证明把它回的字节喂进来的那个解析器是对的。
//!
//! 夹具喂的是 anthropic 自己的 SSE 形状（`event:` 行与 `data:` 行、块按 index 编号、
//! 工具参数以 `input_json_delta` 分批到达），走的是 `HttpClient` 契约，所以不联网、不要
//! 凭证。

use async_trait::async_trait;
use bytes::Bytes;
use cog_core::contract::net::{HttpClient, HttpRequest, HttpResponse, HttpStreamResponse};
use cog_core::{
    AssistantMessageEvent, ChatOptions, ContentBlock, LlmClient, Message, SFError, SFResult,
    StopReason,
};
use cog_llm::provider::anthropic::AnthropicProvider;
use cog_llm::{ApiType, Model, ModelCost, Provider};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// 把一段 SSE 正文按字节块送回去，并记下请求长什么样。
///
/// 分成多块而不是一整块：真实上游的一条事件常常跨两个 TCP 段，解析器若靠「一次读
/// 一整行」的工作方式就会在这里露出来。
#[derive(Debug)]
struct CannedClient {
    body: String,
    chunk: usize,
    seen: Mutex<Vec<HttpRequest>>,
}

#[async_trait]
impl HttpClient for CannedClient {
    async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
        self.seen.lock().unwrap().push(req);
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: self.body.clone().into_bytes(),
        })
    }

    async fn execute_stream(&self, req: HttpRequest) -> SFResult<HttpStreamResponse> {
        self.seen.lock().unwrap().push(req);
        let bytes = self.body.clone().into_bytes();
        let chunk = self.chunk.max(1);
        let chunks: Vec<Result<Bytes, SFError>> = bytes
            .chunks(chunk)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        Ok(HttpStreamResponse {
            status: 200,
            headers: HashMap::new(),
            stream: Box::pin(futures::stream::iter(chunks)),
        })
    }
}

fn model() -> Model {
    Model {
        id: "m".into(),
        name: "m".into(),
        api: ApiType::AnthropicMessages,
        provider: Provider::Anthropic,
        // 版本段属于 base（本 crate 只往后接 `/messages`），与网关里的
        // `base_url: http://.../v1` 同形。
        base_url: "http://gw.example/v1".into(),
        context_window: 200_000,
        max_tokens: 8192,
        supports_tools: true,
        supports_streaming: true,
        supports_vision: false,
        supports_reasoning: false,
        cost: ModelCost::default(),
        headers: HashMap::new(),
    }
}

fn provider(body: &str, chunk: usize) -> (AnthropicProvider, Arc<CannedClient>) {
    let client = Arc::new(CannedClient {
        body: body.into(),
        chunk,
        seen: Mutex::new(Vec::new()),
    });
    (
        AnthropicProvider::new(model(), "placeholder-key").with_client(client.clone()),
        client,
    )
}

async fn drain(
    mut stream: cog_llm::AssistantMessageEventStream,
) -> (Vec<AssistantMessageEvent>, cog_llm::ChatResponse) {
    let result = stream.result();
    let mut events = Vec::new();
    while let Some(ev) = stream.next().await {
        events.push(ev);
    }
    (events, result.await)
}

/// 一段完整文本答复，按 `chunk` 字节切开发送——切点故意落在 JSON 中间。
#[tokio::test]
async fn a_text_reply_is_assembled_from_split_sse_chunks() {
    let body = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":11,\"output_tokens\":0}}}\n",
        "\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n",
        "\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n",
        "\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n",
        "\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n",
        "\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":11,\"output_tokens\":3}}\n",
        "\n",
        "data: {\"type\":\"message_stop\"}\n",
    );
    let (p, client) = provider(body, 17);
    let stream = p
        .chat_stream(&[Message::user("hi")], &ChatOptions::default())
        .await
        .expect("连接这一层必须成功");
    let (events, response) = drain(stream).await;

    let deltas: String = events
        .iter()
        .filter_map(|e| match e {
            AssistantMessageEvent::TextDelta { delta, .. } => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(deltas, "Hello world", "增量文本要按到达顺序拼回整句");

    let text: String = response
        .content
        .iter()
        .filter_map(|b| b.as_text())
        .collect();
    assert_eq!(text, "Hello world", "收尾时的整块正文与增量必须一致");
    assert_eq!(response.stop_reason, StopReason::Stop);
    assert_eq!(response.response_id.as_deref(), Some("msg_1"));
    assert_eq!(response.usage.input, 11);
    assert_eq!(
        response.usage.output, 3,
        "message_delta 那一次的用量要覆盖 message_start 的 0"
    );
    assert_eq!(response.error_message, None);

    // 地址与凭证头：版本段在 base 里，本 crate 只往后接 `/messages`；anthropic 面认
    // `x-api-key`。拼错这两处的表现是上游 404/401，看起来都像「这家坏了」。
    let seen = client.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].url, "http://gw.example/v1/messages");
    assert_eq!(seen[0].method, "POST");
    assert_eq!(
        seen[0].headers.get("x-api-key").map(String::as_str),
        Some("placeholder-key")
    );
}

/// 工具调用：参数以 `input_json_delta` 分批到达，只有块结束时才拼成 JSON。
///
/// 业务与进化链路的每一件改代码的活儿都长这样，所以这一格坏了不是「答得不好」，
/// 而是整个自进化停摆——而且看起来像模型不肯用工具。
#[tokio::test]
async fn a_tool_call_is_reassembled_from_partial_json_deltas() {
    let body = concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_2\",\"usage\":{\"input_tokens\":40,\"output_tokens\":0}}}\n",
        "\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"read_file\",\"input\":{}}}\n",
        "\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\"}}\n",
        "\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"src/main.rs\\\"}\"}}\n",
        "\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n",
        "\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":12}}\n",
        "\n",
        "data: {\"type\":\"message_stop\"}\n",
    );
    let (p, _client) = provider(body, 23);
    let stream = p
        .chat_stream(&[Message::user("read it")], &ChatOptions::default())
        .await
        .expect("连接这一层必须成功");
    let (events, response) = drain(stream).await;

    let mut partial = String::new();
    for ev in &events {
        if let AssistantMessageEvent::ToolCallDelta { delta, .. } = ev {
            partial.push_str(delta);
        }
    }
    assert_eq!(partial, "{\"path\":\"src/main.rs\"}");

    let tool = response
        .content
        .iter()
        .find_map(|b| match b {
            ContentBlock::ToolCall {
                id,
                name,
                arguments,
                ..
            } => Some((id.clone(), name.clone(), arguments.clone())),
            _ => None,
        })
        .expect("工具调用必须出现在最终内容里");
    assert_eq!(tool.0, "toolu_1");
    assert_eq!(tool.1, "read_file");
    assert_eq!(
        tool.2,
        serde_json::json!({"path": "src/main.rs"}),
        "分片的 partial_json 必须在块结束时拼成一个对象，而不是整串塞进 arguments"
    );
    assert_eq!(response.stop_reason, StopReason::ToolUse);
    assert!(response.error_message.is_none());
}
