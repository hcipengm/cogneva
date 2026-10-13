//! The MCP server face of the binary, over a real child process.
//!
//! Each case here starts `cogneva mcp-serve` and talks to it the way an MCP
//! client does. What is being judged cannot be judged in-process: that the
//! process a client launches *is* an MCP server -- that its standard output
//! carries nothing but JSON-RPC (a single diagnostic print there is a message
//! no client can read), that the tools it lists are backed by a sandbox backend
//! this process could actually build, and that a call reaches a real file on
//! this machine.
//!
//! The client is this workspace's own [`StdioTransport`], the same one used to
//! talk to external MCP servers. That is deliberate: the two ends were written
//! to the same protocol, and this is where the claim that they meet is made.
//!
//! What these cases assume about the machine they run on is exactly what the
//! default configuration says: no remote sandbox executor is configured, so the
//! tools run in this process. With one configured, the backend would be that
//! pod's and a file on this host is not a file it can read -- a real difference,
//! and one this suite would report rather than hide.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use cog_protocol::mcp::{McpContent, McpToolCall};
use cog_protocol::{McpTransport, StdioTransport};

/// The server this suite is about, launched the way a client launches it.
fn client() -> StdioTransport {
    StdioTransport::new(env!("CARGO_BIN_EXE_cogneva"), vec!["mcp-serve".to_string()])
        .expect("the path of this binary is a command with something in it")
}

fn text_of(result: &cog_protocol::mcp::McpToolResult) -> String {
    match &result.content[0] {
        McpContent::Text { text } => text.clone(),
        other => panic!("expected text content, got {other:?}"),
    }
}

/// Handshake, tool list, and a call that reads a file this test wrote.
#[tokio::test]
async fn the_binary_lists_its_tools_and_reads_a_file_on_request() {
    let transport = client();

    let tools = tokio::time::timeout(Duration::from_secs(60), transport.list_tools())
        .await
        .expect("the server has to answer the handshake and the list")
        .expect("list the tools");
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["read_file", "run_command", "write_file"],
        "the tool set is the environment one, in a stable order"
    );
    assert!(
        tools
            .iter()
            .all(|tool| tool.input_schema.get("type").is_some()),
        "every tool has to carry the schema a caller fills in: {tools:?}"
    );

    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("hello.txt");
    let content = "the file the server was asked to read\n";
    std::fs::write(&path, content).expect("write the file to read back");

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        transport.call_tool(McpToolCall {
            name: "read_file".to_string(),
            arguments: [(
                "path".to_string(),
                serde_json::json!(path.display().to_string()),
            )]
            .into_iter()
            .collect(),
        }),
    )
    .await
    .expect("the call has to come back")
    .expect("call read_file");

    assert!(
        !result.is_error,
        "reading a file that exists is not a tool failure: {}",
        text_of(&result)
    );
    assert!(
        text_of(&result).contains(content.trim()),
        "the answer has to be the file's content: {}",
        text_of(&result)
    );
}

/// A tool this server does not have comes back as a refusal, not as a result.
#[tokio::test]
async fn a_tool_that_is_not_on_the_list_is_refused() {
    let transport = client();

    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        transport.call_tool(McpToolCall {
            name: "there_is_no_such_tool".to_string(),
            arguments: Default::default(),
        }),
    )
    .await
    .expect("the call has to come back")
    .expect_err("a tool that is not on the list cannot be called");

    assert!(
        outcome.to_string().contains("there_is_no_such_tool"),
        "the refusal has to name the tool: {outcome}"
    );
}

/// Standard output carries the protocol and nothing else, while the diagnostics
/// go to standard error -- where a client is not reading the protocol.
///
/// This is the claim the whole shape rests on. A server that logs to standard
/// output still answers everything it is asked, so the tests above pass while
/// the thing a client sees is a stream with lines it cannot match to anything;
/// the only way to notice is to read the pipe raw instead of through a client
/// that drops what it does not recognize.
#[test]
fn standard_output_carries_the_protocol_and_the_diagnostics_go_elsewhere() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cogneva"))
        .arg("mcp-serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start the server");

    let mut stdin = child.stdin.take().expect("the server reads its input");
    let stdout = child.stdout.take().expect("the server writes its answers");
    let mut lines = BufReader::new(stdout).lines();

    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{}}}}"#
    )
    .expect("ask for the handshake");
    stdin.flush().expect("the request has to leave");

    // Read before closing the input: closing it first ends the session, and a
    // session that ended before it answered would look the same as one that
    // answered nothing.
    let first = lines
        .next()
        .expect("the server answers the handshake")
        .expect("reading the answer cannot fail");
    let message: serde_json::Value = serde_json::from_str(&first).unwrap_or_else(|e| {
        panic!("the first thing on standard output has to be a JSON-RPC message ({e}): {first:?}")
    });
    assert_eq!(message["id"], serde_json::json!(1));

    drop(stdin);
    let status = child
        .wait()
        .expect("the server exits when its input closes");
    assert!(status.success(), "the server exited with {status}");

    let mut stderr = String::new();
    std::io::Read::read_to_string(
        &mut child.stderr.take().expect("the server has an error stream"),
        &mut stderr,
    )
    .expect("read the server's error stream");
    assert!(
        stderr.contains("mcp-serve"),
        "the diagnostics have to land somewhere they can be read, and that is \
         standard error; this stream said: {stderr:?}"
    );
}
