//! Gate: a default body on a required capability must not answer with the shape
//! of success it did not do.
//!
//! `MessageBackend` is the one place where "this backend cannot do that" and
//! "this backend did it and there was nothing to do" both have to reach a
//! caller, and a default body is where the two get folded together. A default
//! answering `Ok(())` or `Ok(Vec::new())` reads to every caller as a completed
//! call, so a backend that never implemented the capability inherits a claim it
//! cannot support — and the caller cannot tell, because the shape it gets back
//! is the shape of success.
//!
//! The fix has a shape, and it is mechanical: a method that must not be
//! silently inherited is declared **required** (no body, `;`), and a method
//! that keeps a default must have that default **do something** — delegate to
//! another method of the same trait. `ack`, `pending_stats` and `claim_pending`
//! were made required for this reason; `publish_batch`, `dlq` and
//! `delay_publish` keep defaults that each end in a `self.` call. `dlq` and
//! `delay_publish` are the shape this gate exists for: their bodies once
//! answered `Ok(())` while the comment above them promised a delivery.
//!
//! So the rule this reads: **a defaulted body contains a call on `self`.**
//! Every method with a body is checked against it, and the methods without one
//! are counted so a scan that read no trait at all cannot pass.
//!
//! Its ceiling, stated once: this reads the *shape* of a body, not what the
//! body does. A default that delegates to another method which itself answers
//! `Ok(())` passes here, and the chain is what a reader has to follow. It is
//! scoped to the one trait whose contract names the distinction — widening it
//! to every trait in the tree would red on the dozens of genuinely optional
//! hooks that answer `Ok(())` because there is nothing to do, and that
//! counterexample census has not been taken.
//!
//! principle: feedback_a_trait_default_body_is_where_a_missing_capability_hides_as_success

use std::path::Path;

/// The trait, and the file that declares it, named relative to this crate's
/// manifest.
const TRAIT_FILE: &str = "../cog-core/src/contract/stream.rs";
const TRAIT_DECL: &str = "pub trait MessageBackend";

/// The capabilities the design names for this trait. A floor rather than a
/// copy: a capability that disappeared from the trait would otherwise narrow
/// this gate silently, and the gate would go on passing over a smaller contract.
const DECLARED_CAPABILITIES: &[&str] = &[
    "publish",
    "publish_batch",
    "payload_limit",
    "subscribe",
    "subscribe_from",
    "create_consumer_group",
    "ack",
    "claim_pending",
    "pending_stats",
    "dlq",
    "delay_publish",
    "delete_stream",
];

/// One method of the trait: its name, and its body when it has a default.
#[derive(Debug)]
struct Method {
    name: String,
    body: Option<String>,
}

fn source() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(TRAIT_FILE);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

/// The delimited text starting at `open`, with nesting, string and char
/// literals and comments skipped. `format!("{stream}:dlq")` inside a default
/// body is balanced either way, but a lone brace in a literal or a doc comment
/// would otherwise desynchronise the scan for the rest of the file.
fn balanced(text: &str, open: usize, open_b: u8, close_b: u8) -> &str {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    let mut in_line_comment = false;
    let mut in_block_comment = 0usize;
    let mut in_string = false;
    let mut in_char = false;
    while i < bytes.len() {
        let c = bytes[i];
        let prev = if i > 0 { bytes[i - 1] } else { b' ' };
        if in_line_comment {
            if c == b'\n' {
                in_line_comment = false;
            }
        } else if in_block_comment > 0 {
            if prev == b'*' && c == b'/' {
                in_block_comment -= 1;
            } else if prev == b'/' && c == b'*' {
                in_block_comment += 1;
            }
        } else if in_string {
            if prev != b'\\' && c == b'"' {
                in_string = false;
            }
        } else if in_char {
            if prev != b'\\' && c == b'\'' {
                in_char = false;
            }
        } else if prev == b'/' && c == b'/' {
            in_line_comment = true;
        } else if prev == b'/' && c == b'*' {
            in_block_comment = 1;
        } else if c == b'"' {
            in_string = true;
        } else if c == b'\'' {
            in_char = true;
        } else if c == open_b {
            depth += 1;
        } else if c == close_b {
            depth -= 1;
            if depth == 0 {
                return &text[open..=i];
            }
        }
        i += 1;
    }
    panic!("unbalanced {} at byte {open}", open_b as char);
}

/// The trait: its declaration through the matching close of its body.
fn trait_body(text: &str) -> &str {
    let at = text
        .find(TRAIT_DECL)
        .unwrap_or_else(|| panic!("{TRAIT_DECL} is not declared in {TRAIT_FILE}"));
    let open = text[at..]
        .find('{')
        .map(|i| at + i)
        .unwrap_or_else(|| panic!("{TRAIT_DECL} is declared but has no body in {TRAIT_FILE}"));
    let block = balanced(text, open, b'{', b'}');
    let end = open + block.len();
    let whole = &text[at..end];
    assert!(
        whole.starts_with(TRAIT_DECL),
        "the text read back starts at the declaration, so the gate judges this trait"
    );
    whole
}

/// Every `fn` the trait body declares, in source order, with its default body
/// when it has one.
fn methods(body: &str) -> Vec<Method> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        // Skip comments and literals so a `fn` in prose is not read as a method.
        if bytes[i] == b'/' && i + 1 < bytes.len() && (bytes[i + 1] == b'/' || bytes[i + 1] == b'*')
        {
            i += comment_len(body, i);
            continue;
        }
        if bytes[i] == b'"' {
            i = skip_literal(bytes, i);
            continue;
        }
        let needle: &[u8] = if bytes[i..].starts_with(b"async fn ") {
            b"async fn "
        } else if bytes[i..].starts_with(b"fn ") && (i == 0 || bytes[i - 1].is_ascii_whitespace()) {
            b"fn "
        } else {
            i += 1;
            continue;
        };
        let name_at = i + needle.len();
        let name_end = bytes[name_at..]
            .iter()
            .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
            .map(|rel| name_at + rel)
            .unwrap_or_else(|| panic!("fn at byte {i} has no name"));
        let name = body[name_at..name_end].to_string();
        let args_open = bytes[name_end..]
            .iter()
            .position(|b| *b == b'(')
            .map(|rel| name_end + rel)
            .unwrap_or_else(|| panic!("fn {name} has no argument list"));
        let args_len = balanced(body, args_open, b'(', b')').len();

        // After the argument list comes the return type, if any, and then the
        // terminator: `;` for a required method, `{` for a default body.
        let mut j = args_open + args_len;
        loop {
            assert!(j < bytes.len(), "fn {name} has neither `;` nor a body");
            match bytes[j] {
                b'{' => {
                    let body_text = balanced(body, j, b'{', b'}').to_string();
                    j += body_text.len();
                    out.push(Method {
                        name,
                        body: Some(body_text),
                    });
                    break;
                }
                b';' => {
                    out.push(Method { name, body: None });
                    j += 1;
                    break;
                }
                b'"' => j = skip_literal(bytes, j),
                _ => j += 1,
            }
        }
        i = j;
    }
    out
}

/// How many bytes the comment starting at `at` takes.
fn comment_len(text: &str, at: usize) -> usize {
    let rest = &text[at..];
    if let Some(body) = rest.strip_prefix("//") {
        return body.find('\n').map(|i| i + 3).unwrap_or(2 + body.len());
    }
    if let Some(body) = rest.strip_prefix("/*") {
        return body.find("*/").map(|i| i + 4).unwrap_or(rest.len());
    }
    1
}

/// The index just past the literal opening at `at`.
fn skip_literal(bytes: &[u8], at: usize) -> usize {
    let mut i = at + 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == b'"' {
            return i + 1;
        }
        i += 1;
    }
    i
}

#[test]
fn every_defaulted_body_calls_the_trait_it_defaults_for() {
    let text = source();
    let parsed = methods(trait_body(&text));

    let required = parsed.iter().filter(|m| m.body.is_none()).count();
    let defaulted: Vec<&Method> = parsed.iter().filter(|m| m.body.is_some()).collect();

    // Anti-vacuity: a scan that found no methods, or only methods with bodies,
    // would satisfy the rule below by having nothing to check.
    assert!(
        required >= 5,
        "a contract that distinguishes the two cases would be mostly required: found {required}"
    );
    assert!(
        !defaulted.is_empty(),
        "the trait is expected to keep defaults for the methods that can have one"
    );
    for capability in DECLARED_CAPABILITIES {
        assert!(
            parsed.iter().any(|m| m.name == *capability),
            "the trait still declares {capability}, which this gate is drawn around"
        );
    }

    for method in &defaulted {
        let body = method.body.as_deref().unwrap_or_default();
        assert!(
            default_body_is_honest(body),
            "`{}` has a default body that neither calls the trait nor refuses: {}. \
             A default answering a success shape on behalf of a backend that \
             never implemented the capability is a claim the backend cannot \
             support; either make the method required (as `ack`, `pending_stats` \
             and `claim_pending` are), give the default real work to do, or have \
             it return the error that says this backend cannot.",
            method.name,
            body.trim()
        );
    }
}

/// The two shapes a default body may have, and nothing else.
///
/// Either it does the work through the trait (`self.`), or it **refuses**: it
/// returns an error, which is the one answer that cannot be mistaken for
/// delivery. A body that returns a success shape — `Ok(())`, an empty vector, a
/// `None` — is what this gate exists to reject; the two accepted shapes are the
/// only ones that say something true about a backend nobody asked.
fn default_body_is_honest(body: &str) -> bool {
    body.contains("self.") || (body.contains("Err(") && !body.contains("Ok("))
}

/// The predicate above is the gate's instrument, so its own reading is pinned
/// against the four shapes that matter. Without this, widening the predicate
/// once would quietly widen what the gate accepts: a `self.`-free body that
/// returns `Ok(())` would pass as "contains Err(" only if nobody looked.
#[test]
fn the_default_body_predicate_splits_the_four_shapes() {
    let delegates = "for payload in payloads { self.publish(subject, payload).await?; } Ok(())";
    let refuses = "Err(SFError::Agent(format!(\"cannot delete {subject:?}\")))";
    let claims_success = "Ok(())";
    let empty = "";

    assert!(default_body_is_honest(delegates), "delegation is honest");
    assert!(
        default_body_is_honest(refuses),
        "an explicit refusal is honest"
    );
    assert!(
        !default_body_is_honest(claims_success),
        "a bare success shape is the one answer a default may not give"
    );
    assert!(!default_body_is_honest(empty), "an empty body says nothing");
    assert!(
        default_body_is_honest("Err(e)"),
        "a bare refusal is the other accepted shape"
    );
    assert!(
        !default_body_is_honest("if cached { return Ok(()); } Err(e)"),
        "a refusal that also answers success somewhere in the body is not a refusal"
    );
}

/// The parser is the gate's instrument, so its own reading is pinned: the three
/// methods that carry defaults today are found as defaulted, and the methods
/// the contract made required are found as required.
#[test]
fn the_parser_tells_a_required_method_from_a_defaulted_one() {
    let parsed = methods(trait_body(&source()));
    let by_name = |name: &str| {
        parsed
            .iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("{name} was not parsed"))
    };

    for name in ["publish_batch", "dlq", "delay_publish"] {
        assert!(
            by_name(name).body.is_some(),
            "{name} carries a default body"
        );
    }
    for name in [
        "publish",
        "payload_limit",
        "ack",
        "claim_pending",
        "pending_stats",
    ] {
        assert!(
            by_name(name).body.is_none(),
            "{name} is required, so it has no body to inherit"
        );
    }
}
