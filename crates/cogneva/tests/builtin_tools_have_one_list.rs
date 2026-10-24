//! The built-in tool set is laid out once, and the process that serves it to
//! an outside caller serves the same one.
//!
//! Two things serve these tools: the agent loops inside the platform, and the
//! MCP server that hands them to a client over stdio. The module header of the
//! MCP server says the tool set is "the same batch as the main application, not
//! a weaker copy laid out again" -- and until this file existed it was a weaker
//! copy: three of six tools, with the other three simply absent. Nothing about
//! that failure is visible at the call site. A shorter list registers cleanly,
//! the server starts, the client lists what it got, and every one of those
//! readings is consistent with a platform that has three tools. The claim that
//! was false lived in a comment, and a comment is not a place a build can
//! disagree with.
//!
//! So the list moved into one constructor, `register_builtins`, and this pins
//! the two properties that make "one list" mean something:
//!
//! - **What the list is.** Registering with everything available produces
//!   exactly the whole set, named. The names are written out here rather than
//!   derived from the list itself, because a check that reads its expectation
//!   from the thing it checks agrees with any list, including one that lost a
//!   tool.
//! - **That the serving paths take it whole.** Neither the MCP server nor the
//!   agent plugin may construct a built-in tool itself. That is the shape the
//!   weaker copy had, and it is the only shape that can go stale quietly: a
//!   call to `register_builtins` cannot be a subset of `register_builtins`.
//!
//! One absence is deliberately *not* a removal. With no HTTP client there is no
//! `http_request`, and the memory tools stay: their own design is to be
//! registered and then fail with a message naming what is missing, because a
//! caller has to be able to tell "this layer is absent" from "this namespace is
//! empty". Dropping them from the list instead would merge those two readings
//! into the same answer, which is the failure this file is about.
//!
//! Its ceiling, stated once: the source half names the two serving paths, so a
//! *third* list written in a new file would not be caught here. The runtime
//! half says nothing about whether a registered tool is any good -- an empty
//! result from a tool that works is covered where that tool is tested.
//!
//! principle: feedback_a_documented_tool_set_is_a_promise_the_build_must_check

use std::sync::Arc;

use cog_agent::tools::builtins::{register_builtins, BuiltinInputs};
use cog_core::{AgentRegistry, HttpClient, HttpRequest, HttpResponse, MessageBackend, SFResult};
use cog_storage::MemoryAgentRegistry;
use cog_stream::MemoryMessageBackend;

/// The names the whole built-in set answers to. Written out, not derived.
const WHOLE_SET: [&str; 10] = [
    "http_request",
    "list_agents",
    "raw_fetch",
    "raw_list",
    "read_file",
    "run_command",
    "send_to_agent",
    "web_fetch",
    "web_search",
    "write_file",
];

/// An HTTP client that is never asked anything. It exists so the registry can
/// be built as a process with a client build one, and the check is about which
/// tools are in the list rather than what any of them does.
#[derive(Debug)]
struct UnusedClient;

#[async_trait::async_trait]
impl HttpClient for UnusedClient {
    async fn execute(&self, _req: HttpRequest) -> SFResult<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: Default::default(),
            body: Vec::new(),
        })
    }
}

fn names_with(client: Option<Arc<dyn HttpClient>>, messaging: bool) -> Vec<String> {
    let registry = cog_agent::tools::ToolRegistry::new();
    let (message_backend, agent_registry) = if messaging {
        (
            Some(Arc::new(MemoryMessageBackend::new()) as Arc<dyn MessageBackend>),
            Some(Arc::new(MemoryAgentRegistry::new()) as Arc<dyn AgentRegistry>),
        )
    } else {
        (None, None)
    };
    register_builtins(
        &registry,
        BuiltinInputs {
            http_client: client,
            metrics: None,
            memory_api_base: Some("http://memory.invalid".into()),
            external_archive_threshold_chars: 4096,
            message_backend,
            agent_registry,
            web_search_base: Some("http://searxng.invalid:8080".into()),
        },
    );
    let mut names = registry.names();
    names.sort();
    names
}

fn names_with_client(present: bool) -> Vec<String> {
    let client: Option<Arc<dyn HttpClient>> = present.then(|| Arc::new(UnusedClient) as Arc<_>);
    names_with(client, true)
}

/// With everything available, the list is the whole set and nothing else.
#[test]
fn the_whole_builtin_set_is_registered_by_one_constructor() {
    let names = names_with_client(true);
    let expected: Vec<String> = WHOLE_SET.iter().map(|n| (*n).to_string()).collect();
    assert_eq!(
        names, expected,
        "the built-in set is the ten named tools; a tool added or dropped here is a \
         change to what every caller of this process is offered"
    );
}

/// A missing HTTP client removes exactly the tool that needs one.
///
/// The memory tools take the client as an option and stay in the list when it
/// is absent, which is the difference between "this layer is absent" and "there
/// is no such tool". This asserts the subtraction is one name, not three: a
/// registry that quietly drops everything client-shaped is the weaker copy
/// again, moved from the MCP server into the shared list.
#[test]
fn a_missing_client_removes_one_tool_and_does_not_empty_the_list() {
    let with = names_with_client(true);
    let without = names_with_client(false);

    let removed: Vec<&String> = with.iter().filter(|n| !without.contains(n)).collect();
    assert_eq!(
        removed,
        vec!["http_request"],
        "only the tool that cannot work at all without a client may leave the list: \
         with={with:?} without={without:?}"
    );
    assert!(
        without.contains(&"raw_list".to_string()) && without.contains(&"raw_fetch".to_string()),
        "the memory tools are registered even with no client, so a caller can tell an \
         absent layer from an empty namespace: {without:?}"
    );
}

/// A process with no roster removes the two tools that are addressed through
/// it, and nothing else.
///
/// The pairing is the point for the send half: a message published without a
/// roster check lands in a stream no consumer group reads, which looks like a
/// successful delivery and leaves a key behind. The listing half has nothing to
/// list. Both are absent here together, and the names say which condition held
/// rather than leaving a runtime error that reads like a delivery failure.
#[test]
fn a_missing_roster_removes_the_two_addressing_tools() {
    let with = names_with_client(true);
    let without = names_with(Some(Arc::new(UnusedClient) as Arc<_>), false);

    let removed: Vec<&String> = with.iter().filter(|n| !without.contains(n)).collect();
    assert_eq!(
        removed,
        vec!["list_agents", "send_to_agent"],
        "only the tools addressed through the roster may leave the list: \
         with={with:?} without={without:?}"
    );
}

/// The two paths that serve these tools take the list whole.
///
/// Each must call the one constructor and must not build a tool itself. This is
/// the shape the MCP server was in when it served three of six: a hand-written
/// list at the serving site, which reads as complete because nothing beside it
/// says how long the real one is.
#[test]
fn neither_serving_path_lays_out_its_own_tool_list() {
    let factories = [
        "read_file",
        "write_file",
        "run_command",
        "raw_list",
        "raw_fetch",
        "http_request",
        "send_to_agent",
        "list_agents",
        "web_search",
        "web_fetch",
    ];

    // Relative to this crate's manifest, which is where cargo runs the test.
    let serving_paths = [
        ("the MCP server", "src/mcp_serve.rs"),
        ("the agent plugin", "../cog-agent/src/plugin.rs"),
    ];

    for (who, rel) in serving_paths {
        let source = std::fs::read_to_string(rel)
            .unwrap_or_else(|e| panic!("{rel} is readable from the crate root: {e}"));

        assert!(
            source.contains("register_builtins("),
            "{who} ({rel}) does not call register_builtins, so the tools it serves are \
             a list of its own and the two lists can drift"
        );

        for factory in factories {
            let call = format!("builtins::{factory}(");
            assert!(
                !source.contains(&call),
                "{who} ({rel}) constructs `{call}` itself. That is a second answer to \
                 which tools exist, and it is the shorter answer that looks finished: \
                 register the whole set through register_builtins instead"
            );
        }
    }
}
