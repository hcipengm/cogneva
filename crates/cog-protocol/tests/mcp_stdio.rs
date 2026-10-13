//! The MCP stdio transport, against a stub server that speaks the wire.
//!
//! What these cases check is the part of this transport that comes from the child
//! being a **process** rather than from JSON-RPC itself: that a request leaves as
//! one line on its standard input and the answer comes back as one line on its
//! standard output, that the handshake happens before any tool work, that replies
//! are matched by their `id` rather than by the order they arrive, and the two
//! directions of "the child is not answering": output that stops (it exited) has
//! to fail the waiting call at once, while output that says something else (a
//! stray print, a lot of noise on the error stream) must not.
//!
//! Each of these is a way a plausible implementation reads as working while
//! needing nothing. A transport that waited for a child that had exited would
//! report a hang where the truth is "it is not coming". One that matched replies
//! by arrival order would hand a call the answer to another call whenever two are
//! in flight. One that read every line as JSON would kill a session over a print.
//! One that left the child's standard error unread would block a server that had
//! already answered everything it was asked -- the server stops there because the
//! pipe is full, and from this side that is indistinguishable from a slow one.
//!
//! The stub is a real child process on the other end of the pipes, so the
//! spawning, the framing, the flushing and the closing of the streams are under
//! test too -- none of them exist when the far side is called in-process.

use std::path::{Path, PathBuf};
use std::time::Duration;

use cog_protocol::mcp::{McpContent, McpToolCall};
use cog_protocol::{McpTransport, StdioTransport};

/// The stub server: line-delimited JSON-RPC in and out, and every line it was
/// sent appended to the file `COGNEVA_PROBE_RECEIPT` names.
///
/// It is a shell script rather than a Rust fixture binary so that what the
/// transport talks to is unambiguously a separate process on the far side of two
/// pipes -- a helper linked into this test binary would share its address space
/// and prove nothing about the pipes.
///
/// The receipt is how the test reads what the transport actually wrote: the
/// transport hands the child a frame and moves on, so "what did it send" can only
/// be answered by the process that read it, and the receipt is written before any
/// reply, which makes it ordered by construction.
///
/// `COGNEVA_PROBE_BEHAVIOUR` picks the one thing a case is about.
///
/// The replies below are spelled the way the protocol spells them (the wire is
/// camelCase: `inputSchema`, `isError`), not the way this crate's own structs
/// happen to name their fields. A stub written in the implementation's spelling
/// would agree with the implementation by construction and pass no matter which
/// of the two the far side actually speaks -- which is exactly what happened
/// here once, with the field names this file no longer uses.
const STUB: &str = r#"
set -u
receipt=${COGNEVA_PROBE_RECEIPT:?the receipt file has to be named}
behaviour=${COGNEVA_PROBE_BEHAVIOUR:-answers}

if [ "$behaviour" = exit_immediately ]; then
  exit 0
fi

reply() {
  printf '{"jsonrpc":"2.0","id":%s,"result":%s}\n' "$1" "$2"
}

while IFS= read -r line; do
  printf '%s\n' "$line" >> "$receipt"
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      reply "$id" '{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"probe","version":"0"}}'
      ;;
    *'"method":"tools/list"'*)
      if [ "$behaviour" = close_output ]; then
        # Nothing will be written to standard output again: from the transport's
        # side this is the child's output ending, an exit without the exit.
        exec 1>&-
        continue
      fi
      if [ "$behaviour" = logs_to_stdout ]; then
        printf 'this line is not JSON at all\n'
      fi
      if [ "$behaviour" = stderr_flood ]; then
        i=0
        while [ $i -lt 4000 ]; do
          printf 'noise noise noise noise noise noise noise noise noise noise\n' >&2
          i=$((i+1))
        done
      fi
      reply "$id" '{"tools":[{"name":"echo","description":"echoes its argument back","inputSchema":{"type":"object"}}]}'
      ;;
    *'"method":"tools/call"'*)
      q=$(printf '%s\n' "$line" | sed -n 's/.*"q":"\([^"]*\)".*/\1/p')
      if [ "$behaviour" = out_of_order ] && [ "$q" = first ]; then
        held=$id
        continue
      fi
      if [ "$behaviour" = out_of_order ] && [ "$q" = second ] && [ -n "${held:-}" ]; then
        reply "$held" '{"content":[{"type":"text","text":"echo:first"}],"isError":false}'
        held=
      fi
      reply "$id" "{\"content\":[{\"type\":\"text\",\"text\":\"echo:$q\"}],\"isError\":false}"
      ;;
  esac
done
"#;

struct Probe {
    transport: StdioTransport,
    receipt: PathBuf,
}

/// The transport under test, pointed at the stub.
fn probe(behaviour: &str) -> Probe {
    let receipt = std::env::temp_dir().join(format!(
        "cogneva-mcp-stdio-probe-{}-{}.recv",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let _ = std::fs::remove_file(&receipt);
    let transport = StdioTransport::new("sh", vec!["-c".to_string(), STUB.to_string()])
        .expect("a command with something in it")
        .with_env(vec![
            (
                "COGNEVA_PROBE_RECEIPT".to_string(),
                receipt.display().to_string(),
            ),
            ("COGNEVA_PROBE_BEHAVIOUR".to_string(), behaviour.to_string()),
        ]);
    Probe { transport, receipt }
}

/// Every line the stub read, in the order it read them.
fn received(receipt: &Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(receipt).unwrap_or_default();
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|e| {
                panic!(
                    "the transport wrote a line that is not one JSON-RPC message ({e}): {line:?}"
                )
            })
        })
        .collect()
}

fn methods(seen: &[serde_json::Value]) -> Vec<String> {
    seen.iter()
        .map(|v| v["method"].as_str().unwrap_or("").to_string())
        .collect()
}

fn call(q: &str) -> McpToolCall {
    McpToolCall {
        name: "echo".to_string(),
        arguments: [("q".to_string(), serde_json::json!(q))]
            .into_iter()
            .collect(),
    }
}

fn text_of(result: &cog_protocol::mcp::McpToolResult) -> String {
    match &result.content[0] {
        McpContent::Text { text } => text.clone(),
        other => panic!("expected text content, got {other:?}"),
    }
}

/// One request is one line, the handshake comes first and only it is answered,
/// and the call's answer comes back as a line.
#[tokio::test]
async fn a_call_is_a_line_on_the_childs_input_and_the_answer_a_line_back() {
    let probe = probe("answers");

    let tools = probe.transport.list_tools().await.expect("list the tools");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "echo");

    let result = probe
        .transport
        .call_tool(call("hi"))
        .await
        .expect("call the tool");
    assert!(!result.is_error);
    assert_eq!(text_of(&result), "echo:hi");

    let seen = received(&probe.receipt);
    assert_eq!(
        methods(&seen),
        vec![
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/call"
        ],
        "the handshake comes first, and only it is answered"
    );
    assert_eq!(
        seen.len(),
        4,
        "one message per line, and nothing merged into it"
    );
    assert_eq!(
        seen[0]["params"]["protocolVersion"],
        serde_json::json!(cog_protocol::MCP_PROTOCOL_VERSION)
    );
    assert!(
        seen[1].get("id").is_none(),
        "a notification carries no id: with one it would be a request, and this one has no \
         answer by the protocol's own definition"
    );
    assert_eq!(
        seen[3]["params"],
        serde_json::json!({"name": "echo", "arguments": {"q": "hi"}}),
        "a tool call carries the name and the arguments as MCP defines them"
    );
}

/// Two calls in flight, answered back to front: each gets its own reply.
#[tokio::test]
async fn replies_are_matched_by_id_and_not_by_arrival_order() {
    let probe = probe("out_of_order");
    probe
        .transport
        .list_tools()
        .await
        .expect("the session handshakes before the calls under test");

    let call = |q: &'static str| {
        let transport = &probe.transport;
        async move {
            transport
                .call_tool(McpToolCall {
                    name: "echo".to_string(),
                    arguments: [("q".to_string(), serde_json::json!(q))]
                        .into_iter()
                        .collect(),
                })
                .await
                .expect("call the tool")
        }
    };
    let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(call("first"), call("second"))
    })
    .await
    .expect("both calls have to come back on their own");

    assert_eq!(
        text_of(&first),
        "echo:first",
        "the reply that arrived last is the one this call asked for"
    );
    assert_eq!(text_of(&second), "echo:second");
}

/// A child whose output ends leaves nothing to answer the call: the call fails
/// rather than waiting for a reply that cannot come.
#[tokio::test]
async fn a_child_that_stops_answering_fails_the_waiting_call_instead_of_hanging() {
    let probe = probe("close_output");

    let outcome = tokio::time::timeout(Duration::from_secs(5), probe.transport.list_tools())
        .await
        .expect("the call has to come back on its own, not be cut off by this test");
    let error = outcome.expect_err("there is no reply, so this cannot be Ok");
    assert!(
        error.to_string().contains("closed its output"),
        "the failure has to name what happened: {error}"
    );
}

/// A child that is gone before it is asked anything fails the call too -- the
/// ordinary shape of a command that is not installed.
#[tokio::test]
async fn a_child_that_exits_before_it_is_asked_fails_the_call_instead_of_hanging() {
    let probe = probe("exit_immediately");

    let outcome = tokio::time::timeout(Duration::from_secs(5), probe.transport.list_tools())
        .await
        .expect("the call has to come back on its own, not be cut off by this test");
    let error = outcome.expect_err("there is no reply, so this cannot be Ok");
    let message = error.to_string();
    // Two shapes are honest here and which one a run gets is a race between this
    // side writing and the child exiting: the write can fail outright, or it can
    // land in a pipe nobody will read. Both have to name what was being talked to.
    assert!(
        message.contains("mcp ") && message.contains("stdio"),
        "the failure has to name the transport and the child: {message}"
    );
}

/// A stray line on the child's output is dropped, and the session goes on.
#[tokio::test]
async fn a_line_that_is_not_json_is_dropped_and_not_read_as_an_answer() {
    let probe = probe("logs_to_stdout");

    probe
        .transport
        .list_tools()
        .await
        .expect("a print on the child's output is not the session dying");

    let result = probe
        .transport
        .call_tool(call("hi"))
        .await
        .expect("and the session is still usable afterwards");
    assert_eq!(text_of(&result), "echo:hi");
}

/// The child's error stream is read, so a server with a lot to say on it can
/// still answer on its output.
///
/// Without a reader on that pipe the child blocks once it is full -- 64 KiB in,
/// long before the 220 KiB this stub writes -- and a server that has already
/// answered everything it was asked stops there. From this side that is a call
/// with no reply, and nothing says the reason is a pipe nobody drained.
#[tokio::test]
async fn a_child_with_a_lot_to_say_on_its_error_stream_is_still_answered() {
    let probe = probe("stderr_flood");

    let tools = tokio::time::timeout(Duration::from_secs(20), probe.transport.list_tools())
        .await
        .expect("the child has to be kept moving: it is blocked on its error stream if not")
        .expect("list the tools");
    assert_eq!(tools[0].name, "echo");
}
