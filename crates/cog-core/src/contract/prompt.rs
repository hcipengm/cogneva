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
//! The key name lives here instead of being spelled out at each site: producer
//! (the actors) and consumer (the agent runtime) read one constant, whereas two
//! literals for one agreement drift.

/// The key that carries the stable half of an input document.
pub const PROMPT_CONTRACT_KEY: &str = "contract";

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
}
