//! Every namespace the backend searches is on the list the extractor reads.
//!
//! `READ_NAMESPACES` says it names every namespace this backend searches, and
//! the extractor's copy is pinned equal to it. Neither check can see an
//! omission, because an omission is present on both sides: on 2026-11-05
//! `retrieve_relevant` searched `knowledge` while the list named four other
//! namespaces, and both the copy and the comparison against it were green.
//! The list is what decides whether extraction is paid for, so an omission
//! there is a spend decision taken by accident.
//!
//! This test reads the searches out of the source and compares the namespaces
//! they name with the declared list, in both directions: a search missing from
//! the list (paid nothing, read anyway) and a listed namespace nobody searches
//! (paid for, read by no one) are both red.

use std::collections::BTreeSet;

const SOURCE: &str = include_str!("../src/unified_knowledge_backend.rs");

/// The file up to its test module.
///
/// The tests below the marker call the same search methods on fixtures, and a
/// namespace those fixtures use says nothing about what production reads.
fn production_source() -> &'static str {
    // The module marker, not any `#[cfg(test)]`: the file also carries
    // test-only methods further up, and cutting at the first one would drop the
    // constants and the searches this census is about.
    match SOURCE.find("#[cfg(test)]\nmod tests {") {
        Some(at) => &SOURCE[..at],
        None => SOURCE,
    }
}

/// `NS_EXECUTION` -> `task_execution`, for the constants the file declares.
fn declared_namespaces() -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    for line in production_source().lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("const NS_") else {
            continue;
        };
        let Some((name, tail)) = rest.split_once(':') else {
            continue;
        };
        let Some((_, value)) = tail.split_once("= \"") else {
            continue;
        };
        let Some((value, _)) = value.split_once('"') else {
            continue;
        };
        out.insert((format!("NS_{name}"), value.to_string()));
    }
    out
}

/// Every `NS_*` identifier the production code hands to a search method.
fn searched_constants() -> BTreeSet<String> {
    let source = production_source();
    let mut out = BTreeSet::new();
    for call in ["search_all(", "search_schema(", "search_summary("] {
        let mut from = 0;
        while let Some(at) = source[from..].find(call) {
            let start = from + at + call.len();
            // The namespace is the first identifier in the argument list, and
            // the call that spans lines puts it on the next one.
            let window = &source[start..(start + 160).min(source.len())];
            if let Some(name) = first_ns_identifier(window) {
                out.insert(name);
            }
            from = start;
        }
    }
    out
}

fn first_ns_identifier(text: &str) -> Option<String> {
    let at = text.find("NS_")?;
    let tail = &text[at..];
    let end = tail
        .find(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
        .unwrap_or(tail.len());
    Some(tail[..end].to_string())
}

#[test]
fn the_list_names_every_namespace_the_searches_ask_for() {
    let declared = declared_namespaces();
    assert!(
        declared.len() >= 5,
        "the constants could not be read out of the source; this test would \
         pass on an empty census: {declared:?}"
    );

    let searched = searched_constants();
    assert!(
        !searched.is_empty(),
        "no search calls were found in the source, so the census proves nothing"
    );

    let listed: BTreeSet<String> = cog_wiki::unified_knowledge_backend::READ_NAMESPACES
        .iter()
        .filter_map(|namespace| {
            declared
                .iter()
                .find(|(_, value)| value == namespace)
                .map(|(name, _)| name.clone())
        })
        .collect();
    assert_eq!(
        listed.len(),
        cog_wiki::unified_knowledge_backend::READ_NAMESPACES.len(),
        "a namespace on the read list is not declared in the file: {:?}",
        cog_wiki::unified_knowledge_backend::READ_NAMESPACES
    );

    let unlisted: Vec<_> = searched.difference(&listed).collect();
    assert!(
        unlisted.is_empty(),
        "these namespaces are searched but not on the read list, so extraction \
         is not allowed to fill what the searches read: {unlisted:?}"
    );

    let unsearched: Vec<_> = listed.difference(&searched).collect();
    assert!(
        unsearched.is_empty(),
        "these namespaces are on the read list but no production search asks \
         for them, so extraction would be paying for rows nobody reads: \
         {unsearched:?}"
    );
}
