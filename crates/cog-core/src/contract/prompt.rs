//! How the document handed to an agent is layered, and where the runtime has to
//! render each layer.
//!
//! The input document (`{"task": …, "context": …}`) is serialized as **one user
//! message** and sent upstream. Serialization follows JSON key order — and the
//! fields that change on every attempt (`context.attempt`, `context.generation`,
//! `context.repair_feedback`) are exactly the ones that sort to the front. An
//! upstream prefix cache compares from the first byte: everything after the
//! first changed byte is bought again at full price. The actor's **verbatim**
//! instructions ("You are the Planner…", the change-format contract, the output
//! schema) sort after them, so they never entered the cacheable prefix at all:
//! two attempts share only `{"context":{"attempt":` worth of bytes.
//!
//! The contract: the [`PROMPT_CONTRACT_KEY`] entry of the document is **the half
//! that does not change from one attempt to the next**. The runtime must render
//! it where the model reads first (the leading system message) and drop it from
//! the user message. Stable first, varying last, is the only shape a prefix cache
//! can work with.
//!
//! Hoisting the contract fixes the head of the request but not the rest of it:
//! what is left still serializes in key order, and `context` sorts before
//! `task`. The user message therefore opens on `{"context":{"attempt":`, a byte
//! that moves every attempt, and the request underneath it — the largest
//! constant block in the whole request, identical for every attempt of one task
//! — is bought again at full price each time. [`render_varying_half`] is the
//! other half of that rule: the request first, what this attempt adds last.
//!
//! The key name lives here instead of being spelled out at each site: producer
//! (the actors) and consumer (the agent runtime) read one constant, whereas two
//! literals for one agreement drift.

/// The key that carries the stable half of an input document.
pub const PROMPT_CONTRACT_KEY: &str = "contract";

/// Document keys that state the request rather than this attempt at it.
///
/// A prefix cache compares from the first byte, so the order these render in
/// decides what it can hold: whatever stands before the first attempt-specific
/// byte is shared by every attempt. `task` is the work being done and does not
/// move between the attempts of one task — same id, same input — while
/// everything that does move (the attempt number, the previous feedback, this
/// round's generation) is assembled under `context` by the caller. In key order
/// the two arrive the other way round and the varying byte leads.
const REQUEST_KEYS: [&str; 1] = ["task"];

/// Take the stable half out of the document and return `(stable half, the rest)`.
///
/// A string is used verbatim (a caller that wants prose writes prose); anything
/// else is serialized as JSON — the two are the same thing at runtime, so no
/// producer has to contort a structure into a string to match a shape.
///
/// A document without this key (an input built by some other path) returns
/// `None` with the document untouched: this contract is **optional**, and a
/// producer that does not write it keeps its behavior byte for byte. An empty
/// object and `null` count as absent too — a system message that says only `{}`
/// says nothing and costs the model a paragraph to read.
pub fn split_contract(mut input: serde_json::Value) -> (Option<String>, serde_json::Value) {
    let Some(contract) = input
        .as_object_mut()
        .and_then(|doc| doc.remove(PROMPT_CONTRACT_KEY))
    else {
        return (None, input);
    };
    let text = match contract {
        serde_json::Value::Null => return (None, input),
        serde_json::Value::Object(fields) if fields.is_empty() => return (None, input),
        serde_json::Value::String(text) => text,
        other => other.to_string(),
    };
    (Some(text), input)
}

/// Render the varying half as one user message, with the request in front.
///
/// The content is [`split_contract`]'s payload, byte for byte — this decides
/// the order, not the contents, and the result parses back to the same value.
/// Everything under [`REQUEST_KEYS`] is written first, then the remaining keys
/// in their own order; a payload that is not an object is rendered as it is,
/// like [`split_contract`] passes non-object documents through.
///
/// What this buys is the prefix: on the second attempt at the same task the
/// request block is byte-identical to the first attempt's, so the upstream
/// cache holds it instead of being invalidated by the attempt counter that used
/// to stand in front of it.
pub fn render_varying_half(payload: &serde_json::Value) -> String {
    let Some(fields) = payload.as_object() else {
        return payload.to_string();
    };
    let key_text = |key: &str| serde_json::Value::String(key.to_string()).to_string();
    let mut members: Vec<String> = Vec::with_capacity(fields.len());
    for key in REQUEST_KEYS {
        if let Some(value) = fields.get(key) {
            members.push(format!("{}:{}", key_text(key), value));
        }
    }
    for (key, value) in fields {
        if REQUEST_KEYS.contains(&key.as_str()) {
            continue;
        }
        members.push(format!("{}:{}", key_text(key), value));
    }
    format!("{{{}}}", members.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stable_half_is_lifted_out_and_the_rest_is_untouched() {
        let doc = serde_json::json!({
            PROMPT_CONTRACT_KEY: {"instructions": "You are the Planner", "output_schema": {"a": "b"}},
            "task": {"id": "t1"},
            "context": {"attempt": 2},
        });
        let (contract, payload) = split_contract(doc);
        let contract = contract.expect("the stable half");
        assert!(contract.contains("You are the Planner"));
        assert!(contract.contains("output_schema"));
        // The varying half is untouched byte for byte, and the key itself is gone
        // from the user message — leaving it there sends the same text twice.
        assert!(payload.get(PROMPT_CONTRACT_KEY).is_none());
        assert_eq!(payload["task"]["id"], "t1");
        assert_eq!(payload["context"]["attempt"], 2);
    }

    #[test]
    fn a_document_without_the_key_is_returned_as_is() {
        let doc = serde_json::json!({"goal": "g", "instruction": "choose"});
        let (contract, payload) = split_contract(doc.clone());
        assert!(contract.is_none(), "no stable half was written");
        assert_eq!(payload, doc);
    }

    /// An empty value reads as "not written": `null` must not become an empty
    /// system message — upstream would see a message that says nothing, and
    /// nobody reading it later could tell where it came from.
    #[test]
    fn a_null_contract_is_no_contract() {
        let doc = serde_json::json!({PROMPT_CONTRACT_KEY: null, "task": 1});
        let (contract, payload) = split_contract(doc);
        assert!(contract.is_none());
        assert!(payload.get(PROMPT_CONTRACT_KEY).is_none());
    }

    /// Prose is used verbatim, without quoting: the producer wrote words for the
    /// model to read, not a JSON value.
    #[test]
    fn a_prose_contract_is_used_verbatim() {
        let doc = serde_json::json!({PROMPT_CONTRACT_KEY: "Emit JSON only"});
        let (contract, _) = split_contract(doc);
        assert_eq!(contract.as_deref(), Some("Emit JSON only"));
    }

    /// An empty object is not a contract: a system message that says only `{}`
    /// says nothing.
    #[test]
    fn an_empty_contract_object_says_nothing() {
        let doc = serde_json::json!({PROMPT_CONTRACT_KEY: {}, "task": 1});
        let (contract, payload) = split_contract(doc);
        assert!(contract.is_none());
        assert!(payload.get(PROMPT_CONTRACT_KEY).is_none());
    }

    /// A non-object document (a path outside this contract may pass an array or a
    /// string straight through) does not panic.
    #[test]
    fn a_non_object_document_is_not_a_crash() {
        let (contract, payload) = split_contract(serde_json::json!("just a string"));
        assert!(contract.is_none());
        assert_eq!(payload, serde_json::json!("just a string"));
    }

    /// The request is written before the attempt, so two attempts at one task
    /// share everything up to the first attempt-specific byte.
    ///
    /// This is the reading, not the arrangement: what matters is the length of
    /// the common prefix between two attempts, which is what the upstream cache
    /// gets to keep. Rendering in key order gave it `{"context":{"attempt":`.
    #[test]
    fn the_request_stands_before_the_attempt_that_moves() {
        let doc = |attempt: u32| {
            serde_json::json!({
                PROMPT_CONTRACT_KEY: {"instructions": "You are the Planner"},
                "task": {"id": "t1", "input": {"goal": "fix the parser"}},
                "context": {"attempt": attempt, "previous_feedback": null},
            })
        };
        let (_, first) = split_contract(doc(1));
        let (_, second) = split_contract(doc(2));
        let (first, second) = (render_varying_half(&first), render_varying_half(&second));

        let shared = first
            .bytes()
            .zip(second.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        assert!(first.starts_with("{\"task\":"), "{first}");
        assert!(
            shared > "{\"context\":{\"attempt\":".len(),
            "two attempts share only {shared} bytes: {first}"
        );

        // Same value, ordered: reordering decides where the cache starts, never
        // what the model reads.
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&first).unwrap(),
            split_contract(doc(1)).1
        );
    }

    /// Reordering must not depend on the payload happening to carry a task: a
    /// document built by some other path is still rendered as one JSON value.
    #[test]
    fn a_payload_without_a_request_renders_in_its_own_order() {
        let payload = serde_json::json!({"b": 2, "a": 1});
        let rendered = render_varying_half(&payload);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&rendered).unwrap(),
            payload
        );
        assert_eq!(
            render_varying_half(&serde_json::json!("prose")),
            "\"prose\""
        );
    }
}
