//! The MCP server face: what it answers, and what it must not answer.
//!
//! What these cases check is the part of a server that is not the tool call
//! itself: that a notification is left unanswered (a reply to a message with no
//! `id` is a reply nobody can match), that the reply carries back the `id` it
//! was asked with -- and carries it back as the same JSON type, since a client
//! pairing answers to requests has nothing else to go on -- and that the three
//! kinds of failure stay apart: a method nobody implements, a request whose
//! params do not name a tool, and a tool that ran and failed. The last of those
//! is a **result** with `isError`, not an error envelope; answering it with
//! "method not found" tells the caller its request was wrong when the request
//! was fine and the tool was not.
//!
//! The server is driven through a pair of in-memory pipes rather than a spawned
//! process, because a child would add nothing to read: everything judged here
//! is decided between reading a line and writing one, and a pipe makes the
//! ordering of those two observable without a process scheduler in between.
//! Whether the bytes also flow through a real child's standard input and output
//! is a different claim, covered where the transport that spawns one is.

use std::sync::Arc;

use tokio::io::{AsyncWriteExt, BufReader};

use cog_core::{SFResult, ToolDefinition, ToolExecutor};
use cog_protocol::McpServer;

/// A tool set to answer from: one tool that echoes, one that fails.
struct Stub;

#[async_trait::async_trait]
impl ToolExecutor for Stub {
    async fn execute(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        match name {
            "echo" => Ok(serde_json::json!({"echoed": arguments})),
            "boom" => Err(cog_core::SFError::Agent("the tool blew up".into())),
            other => Err(cog_core::SFError::Agent(format!("no such tool: {other}"))),
        }
    }
}

fn server() -> Arc<McpServer> {
    let definition = |name: &str| ToolDefinition {
        name: name.to_string(),
        description: format!("{name} does what it says"),
        parameters: serde_json::json!({"type": "object"}),
    };
    Arc::new(
        McpServer::new(vec![definition("echo"), definition("boom")]).with_executor(Arc::new(Stub)),
    )
}

/// Feed the given lines in, close this side, and answer everything the server
/// wrote back.
///
/// Closing before reading is deliberate and is what makes the ordering a claim
/// rather than a hope: the server only returns once its input ends, so every
/// reply is written by the time anything is read. Reading first would make this
/// helper race the thing under test.
async fn ask(server: &Arc<McpServer>, lines: &[&str]) -> Vec<serde_json::Value> {
    let (mut from_client, to_server) = tokio::io::duplex(64 * 1024);
    let (from_server, mut to_client) = tokio::io::duplex(64 * 1024);

    let server = server.clone();
    let serving = tokio::spawn(async move {
        cog_protocol::transport::serve(&server, BufReader::new(to_server), from_server).await
    });

    for line in lines {
        from_client
            .write_all(line.as_bytes())
            .await
            .expect("write a request line");
        from_client
            .write_all(b"\n")
            .await
            .expect("terminate the request line");
    }
    drop(from_client);

    tokio::time::timeout(std::time::Duration::from_secs(5), serving)
        .await
        .expect("the server has to stop when the client closes its side")
        .expect("the serving task does not panic")
        .expect("serving ends cleanly");

    let mut written = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut to_client, &mut written)
        .await
        .expect("read what the server wrote");
    written
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("the server wrote a line that is not JSON ({e}): {line:?}")
            })
        })
        .collect()
}

fn ids(replies: &[serde_json::Value]) -> Vec<serde_json::Value> {
    replies.iter().map(|r| r["id"].clone()).collect()
}

fn text_of(reply: &serde_json::Value) -> String {
    reply["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("expected a text content in {reply}"))
        .to_string()
}

/// The handshake, the tool list and a call, each answered on a line of its own,
/// and the notification in between answered not at all.
#[tokio::test]
async fn the_handshake_the_list_and_a_call_are_each_answered_once() {
    let replies = ask(
        &server(),
        &[
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"q":"hi"}}}"#,
        ],
    )
    .await;

    assert_eq!(
        ids(&replies),
        vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3)
        ],
        "one reply per request and none for the notification: a reply to a message \
         with no id is a reply with nothing to match it to"
    );

    assert_eq!(
        replies[0]["result"]["protocolVersion"],
        serde_json::json!(cog_protocol::MCP_PROTOCOL_VERSION)
    );
    assert_eq!(
        replies[0]["result"]["capabilities"]["tools"],
        serde_json::json!({}),
        "the server has to declare the one capability it answers for"
    );

    let tools = replies[1]["result"]["tools"]
        .as_array()
        .expect("tools is an array");
    assert_eq!(tools.len(), 2);
    assert!(
        tools[0].get("inputSchema").is_some(),
        "a tool's schema goes out under the protocol's name: {}",
        tools[0]
    );

    assert_eq!(replies[2]["result"]["isError"], serde_json::json!(false));
    assert_eq!(
        text_of(&replies[2]),
        serde_json::json!({"echoed": {"q": "hi"}}).to_string()
    );
}

/// An `id` comes back as what it was: strings stay strings.
#[tokio::test]
async fn an_id_comes_back_as_the_type_it_arrived_with() {
    let replies = ask(
        &server(),
        &[r#"{"jsonrpc":"2.0","id":"ask-7","method":"tools/list"}"#],
    )
    .await;

    assert_eq!(
        ids(&replies),
        vec![serde_json::json!("ask-7")],
        "a client matches answers by id, so an id that changes shape on the way back \
         is an answer that never arrives"
    );
}

/// A tool that ran and failed is a result with `isError`, not a protocol error.
#[tokio::test]
async fn a_tool_that_failed_is_reported_as_a_result_and_not_as_a_bad_request() {
    let replies = ask(
        &server(),
        &[r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"boom","arguments":{}}}"#],
    )
    .await;

    assert!(
        replies[0].get("error").is_none(),
        "the request was well formed: answering it with an error envelope says the \
         caller did something wrong and it did not: {}",
        replies[0]
    );
    assert_eq!(replies[0]["result"]["isError"], serde_json::json!(true));
    assert!(
        text_of(&replies[0]).contains("blew up"),
        "the failure has to say what failed: {}",
        replies[0]
    );
}

/// A request naming a tool that is not on the list is the caller's mistake, and
/// is answered as one.
#[tokio::test]
async fn a_tool_that_is_not_on_the_list_is_refused_as_a_bad_request() {
    let replies = ask(
        &server(),
        &[r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"nope","arguments":{}}}"#],
    )
    .await;

    assert_eq!(replies[0]["error"]["code"], serde_json::json!(-32602));
    assert!(replies[0].get("result").is_none(), "{}", replies[0]);
}

/// Arguments that are not an object are refused rather than read as no arguments.
#[tokio::test]
async fn arguments_that_are_not_an_object_are_refused() {
    let replies = ask(
        &server(),
        &[r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":"hi"}}"#],
    )
    .await;

    assert_eq!(replies[0]["error"]["code"], serde_json::json!(-32602));
}

/// A method this server does not implement is named in the refusal.
#[tokio::test]
async fn an_unknown_method_is_refused_by_name() {
    let replies = ask(
        &server(),
        &[r#"{"jsonrpc":"2.0","id":1,"method":"resources/list"}"#],
    )
    .await;

    assert_eq!(replies[0]["error"]["code"], serde_json::json!(-32601));
    assert!(
        replies[0]["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("resources/list"),
        "the refusal has to name what was asked for: {}",
        replies[0]
    );
}

/// A line that is not JSON is answered with a parse error carrying a null id,
/// and the session goes on.
#[tokio::test]
async fn a_line_that_is_not_json_is_answered_and_the_session_goes_on() {
    let replies = ask(
        &server(),
        &[
            "this is not a JSON-RPC message",
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
        ],
    )
    .await;

    assert_eq!(
        ids(&replies),
        vec![serde_json::Value::Null, serde_json::json!(1)],
        "the unparseable line gets an answer too: dropping it leaves the caller \
         waiting for a reply that will never come"
    );
    assert_eq!(replies[0]["error"]["code"], serde_json::json!(-32700));
    assert!(
        replies[1].get("result").is_some(),
        "and the session is still usable afterwards: {}",
        replies[1]
    );
}

/// An empty line is not a message: it is not answered, and it does not end the
/// session either.
#[tokio::test]
async fn a_blank_line_is_skipped() {
    let replies = ask(
        &server(),
        &["", r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#],
    )
    .await;

    assert_eq!(ids(&replies), vec![serde_json::json!(1)]);
}
