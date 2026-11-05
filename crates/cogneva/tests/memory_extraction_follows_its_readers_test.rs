//! Extraction only runs where something searches.
//!
//! Memory extraction is the platform's most expensive standing loop: one model
//! call per archived conversation, around the clock, each carrying the whole
//! transcript. What it buys is rows in a namespace, and a row is worth its
//! price only when a reader queries that namespace. On 2026-11-05 the two
//! lists disagreed: extraction wrote to `default` (83,662 schema rows and
//! 3,969 summaries by that afternoon) and every production search asked one of
//! `task_decomposition`, `implementation`, `failure_pattern`, `task_execution`.
//! Not one of the 83,662 rows had ever been retrieved, and the summary search
//! method had never been called in production at all.
//!
//! The ingest gate reads the list the readers declare. This test pins the two
//! together: the copy the extractor holds and the list the searches use must be
//! the same list. A reader added without the copy following it would silently
//! re-open a spend that buys nothing; the copy growing without a reader would
//! stop paying for something in use. Both are one red line here rather than a
//! reading nobody takes.
//!
//! The gate itself is pinned where it runs, in `cog-memory`'s ingestor tests.

/// The list the production assembly gives the ingestor, in declaration order.
///
/// Built through the same `From` conversion the plugin uses, so this reads the
/// value the deployment actually runs with rather than a default that only
/// tests see.
fn extractor_copy() -> Vec<String> {
    cog_memory::MemoryIngestorConfig::from(&cog_memory::IngestConfig::default()).read_namespaces
}

#[test]
fn the_extractor_extracts_only_where_the_searches_look() {
    let extractor = extractor_copy();
    let readers: Vec<String> = cog_wiki::unified_knowledge_backend::READ_NAMESPACES
        .iter()
        .map(|n| n.to_string())
        .collect();
    assert_eq!(
        extractor, readers,
        "the namespaces extraction writes and the namespaces the knowledge \
         retrieval searches have drifted apart"
    );
}

#[test]
fn the_copy_names_every_one_of_them_and_nothing_else() {
    // Sorted, so the assertion is about the set and not about the order two
    // files happen to declare their constants in.
    let mut extractor = extractor_copy();
    extractor.sort();
    extractor.dedup();
    let mut readers: Vec<String> = cog_wiki::unified_knowledge_backend::READ_NAMESPACES
        .iter()
        .map(|n| n.to_string())
        .collect();
    readers.sort();
    readers.dedup();
    assert_eq!(
        readers.len(),
        cog_wiki::unified_knowledge_backend::READ_NAMESPACES.len(),
        "the declared read list repeats a namespace"
    );
    assert_eq!(extractor, readers);
    assert!(
        !extractor.contains(&"default".to_string()),
        "`default` is where extraction used to write and where nothing searches; \
         naming it here would re-open the spend this gate closed"
    );
}
