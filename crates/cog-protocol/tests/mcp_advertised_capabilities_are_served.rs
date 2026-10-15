//! principle: feedback_an_advertised_capability_is_a_row_in_the_serving_table
//! principle: feedback_a_two_sided_equality_over_a_renaming_pins_where_the_sort_happens
//!
//! The MCP server's advertised capability set has to be computed from what it
//! actually answers, not written next to it.
//!
//! A client reads `initialize`'s `capabilities` and picks what to use from it.
//! While that object was a literal inside the reply body, two things could drift
//! apart silently: a new reply arm could be added without appearing in the
//! object, and the object could keep naming a namespace whose arms had been
//! removed. Both leave the caller acting on a capability list that does not
//! describe the server it is talking to.
//!
//! The fix is one construction point: the reply's object is built from a
//! declared table, and the dispatch matches on the same names that table names.
//! What these cases pin is that the two ends still meet, in both directions:
//!
//! - every method the table names is answered, and every method the dispatch
//!   answers is declared -- so an arm cannot be added undeclared;
//! - the advertised object names exactly the table's capabilities -- so a
//!   capability cannot be advertised without a row, nor a row without a key;
//! - a message naming a namespace this server does not implement is refused by
//!   name *and* that namespace is not advertised, which is the arm that tells
//!   "declared because it is served" apart from "declared because someone typed
//!   it".
//!
//! **Ceiling, so the next reader does not read this as more than it is.** It
//! covers one server's dispatch and one reply object. It reads the dispatch's
//! *shape* (which `METHOD_*` constants appear as match patterns), not the arms'
//! bodies -- an arm that answers with nothing still reads as served here. It
//! says nothing about any other server in the tree, and the convention it
//! depends on (method names live in `pub const METHOD_*` and are spelled
//! `METHOD_` + the method's own name upper-cased with `/` as `_`) is stated
//! rather than enforced globally -- a second server has to adopt it before a
//! scan over the tree would mean anything.

use std::collections::BTreeSet;

use cog_protocol::mcp::{
    capabilities_json, served_methods, McpServer, ADVERTISED_CAPABILITIES, NON_CAPABILITY_METHODS,
};

/// The one spelling rule this gate depends on: a method's constant is its name
/// upper-cased, with `/` becoming `_`, behind `METHOD_`.
fn ident_of(method: &str) -> String {
    format!("METHOD_{}", method.to_ascii_uppercase().replace('/', "_"))
}

/// The declared method set as identifiers.
///
/// A set, not an ordered list, and deliberately so: the spelling rule above is
/// not order-preserving (a `/` becomes `_`, which sorts after a letter), so two
/// sides that arrive at their order by different routes -- one sorted by method
/// name, the other by identifier -- can disagree on order while agreeing on
/// membership. Comparing sets keeps the ordering out of the criterion entirely,
/// rather than pinning which route sorted when.
fn ident_set_of_served_methods() -> BTreeSet<String> {
    served_methods().iter().map(|m| ident_of(m)).collect()
}

/// The `pub const METHOD_*: &str = "...";` declarations the server file makes.
fn declared_method_constants(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in src.lines() {
        let t = line.trim();
        let Some(rest) = t.strip_prefix("pub const METHOD_") else {
            continue;
        };
        let Some(rest) = rest.strip_suffix(';') else {
            continue;
        };
        let Some((name, value)) = rest.split_once(": &str = ") else {
            continue;
        };
        let value = value.trim().trim_matches('"');
        out.push((format!("METHOD_{name}"), value.to_string()));
    }
    out.sort();
    out
}

/// The `METHOD_*` constants the dispatch in `handle` matches on.
///
/// A match arm is read as "a *line* whose first token is a `METHOD_` constant
/// and whose remainder starts with `=>`". Bounding it to the `handle` function
/// keeps a `METHOD_` name used in a doc comment or an assertion out of the set.
fn dispatch_arms(src: &str) -> BTreeSet<String> {
    let Some(start) = src.find("pub async fn handle(") else {
        return BTreeSet::new();
    };
    let body = &src[start..];
    // The dispatch ends with its catch-all arm; take everything up to it so a
    // later function's arms cannot leak in.
    let end = body.find("other =>").unwrap_or(body.len());
    let mut out = BTreeSet::new();
    for line in body[..end].lines() {
        let t = line.trim();
        let Some((name, rest)) = t.split_once(char::is_whitespace) else {
            continue;
        };
        if name.starts_with("METHOD_") && rest.trim_start().starts_with("=>") {
            out.insert(name.to_string());
        }
    }
    out
}

fn server_source() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mcp.rs");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("the server source has to be readable at {}: {e}", path.display()))
}

fn server() -> McpServer {
    McpServer::new(Vec::new())
}

async fn reply_to(method: &str) -> serde_json::Value {
    let message = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": {}});
    server()
        .handle(&message)
        .await
        .unwrap_or_else(|| panic!("a request with an id gets an answer: {method}"))
}

fn is_method_not_found(reply: &serde_json::Value) -> bool {
    reply["error"]["code"] == serde_json::json!(-32601)
}

/// A declaration that nothing backs is advertising a capability the server does
/// not have; a dispatch arm nothing declared is a capability reachable through a
/// name no caller was told about. Both are the drift this file exists to catch.
#[tokio::test]
async fn the_dispatch_answers_exactly_the_declared_methods() {
    let src = server_source();

    let declared = declared_method_constants(&src);
    assert!(
        declared.len() >= 4,
        "the fixture has to find the method constants, or the comparison below is \
         between two empty sets: {declared:?}"
    );

    let arms = dispatch_arms(&src);
    assert!(
        !arms.is_empty(),
        "the dispatch has to be found in src/mcp.rs, not skipped"
    );

    let expected = ident_set_of_served_methods();
    assert_eq!(
        arms, expected,
        "the dispatch's arms and the declared method set have to be the same set: an \
         arm answered here but declared nowhere reaches callers under a name that was \
         never advertised, and a declared method with no arm is advertised and refused"
    );

    // The spelling rule, pinned so the comparison above cannot pass by both
    // sides breaking the same way: each constant has to hold its own name.
    for method in served_methods() {
        let ident = ident_of(method);
        let value = declared
            .iter()
            .find(|(name, _)| *name == ident)
            .unwrap_or_else(|| panic!("{ident} has to be declared in src/mcp.rs"));
        assert_eq!(
            value.1, method,
            "{ident} has to hold the method's own name"
        );
    }
}

/// Every method the table names is answered, and a namespace the server does not
/// implement is refused **and** not advertised.
#[tokio::test]
async fn every_declared_method_is_answered_and_undeclared_ones_are_not_advertised() {
    assert!(
        !served_methods().is_empty(),
        "an empty method set would make every assertion below vacuous"
    );

    for method in served_methods() {
        let reply = reply_to(method).await;
        assert!(
            !is_method_not_found(&reply),
            "`{method}` is declared as served, so answering it with \"method not \
             found\" is the declaration and the server disagreeing: {reply}"
        );
    }

    let advertised = capabilities_json();
    let keys: BTreeSet<String> = advertised
        .as_object()
        .expect("capabilities is an object")
        .keys()
        .cloned()
        .collect();

    for undeclared in ["resources/list", "resources/read", "prompts/list"] {
        let reply = reply_to(undeclared).await;
        assert!(
            is_method_not_found(&reply),
            "`{undeclared}` has no arm, so it is refused: {reply}"
        );
        let namespace = undeclared.split('/').next().unwrap_or_default();
        assert!(
            !keys.contains(namespace),
            "`{namespace}` is not implemented, so it must not be advertised -- a key \
             here is the server claiming a namespace it refuses: {advertised}"
        );
    }
}

/// The advertised object is the table, both directions: no key without a row and
/// no row without a key.
#[tokio::test]
async fn the_advertised_object_is_the_table() {
    assert!(
        !ADVERTISED_CAPABILITIES.is_empty(),
        "an empty table would make this comparison vacuous"
    );

    let reply = reply_to("initialize").await;
    let advertised = reply["result"]["capabilities"].clone();
    let keys: BTreeSet<String> = advertised
        .as_object()
        .expect("capabilities is an object")
        .keys()
        .cloned()
        .collect();
    let rows: BTreeSet<String> = ADVERTISED_CAPABILITIES
        .iter()
        .map(|capability| capability.capability.to_string())
        .collect();

    assert_eq!(
        keys, rows,
        "the reply's keys and the declared capabilities have to be the same set"
    );
    assert_eq!(
        advertised,
        capabilities_json(),
        "the reply has to carry the object built from the table, not a second copy \
         of it"
    );

    let served = served_methods();
    for capability in ADVERTISED_CAPABILITIES {
        assert!(
            !capability.methods.is_empty(),
            "`{}` is declared with no method behind it: a capability nobody \
             implements must be advertised by writing no row at all, not a row with \
             an empty list",
            capability.capability
        );
        for method in capability.methods {
            assert!(
                served.contains(method),
                "`{}` names `{method}`, which is not served",
                capability.capability
            );
        }
    }

    // The protocol-level methods are the other half of the served set, and they
    // are deliberately not capabilities: every server answers the handshake.
    for method in NON_CAPABILITY_METHODS {
        assert!(
            served.contains(method),
            "`{method}` is served, so it has to be in the served set"
        );
    }
}

/// The gate rejects: drop the `tools/call` arm from a copy of the real source
/// and the two sets stop agreeing.
///
/// Without this, the comparison could be passing because the extractor finds
/// nothing on either side. The anchor has to be found or the mutation proof is
/// itself vacuous, so a missing anchor fails rather than skips.
#[test]
fn dropping_an_arm_makes_the_dispatch_and_the_declaration_disagree() {
    let src = server_source();
    let before = dispatch_arms(&src);
    assert_eq!(
        before,
        ident_set_of_served_methods(),
        "the unmutated source is the control: it has to agree first"
    );

    let anchor = "            METHOD_TOOLS_CALL => Some(self.call_reply(id, message).await),\n";
    assert!(
        src.contains(anchor),
        "the mutation's anchor has to be present, or the control below proves nothing"
    );
    let mutated = src.replace(anchor, "");
    assert_eq!(mutated.len(), src.len() - anchor.len(), "the mutation applied");

    let after = dispatch_arms(&mutated);
    let expected = ident_set_of_served_methods();
    assert_ne!(
        after, expected,
        "with the arm gone, the dispatch no longer answers everything the table \
         declares -- and the assertion in the first case is what would have caught it"
    );
    assert!(
        !after.contains(&ident_of("tools/call")),
        "the mutation has to actually remove the constant from the extracted set"
    );
}
