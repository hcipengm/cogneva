//! principle: feedback_a_call_site_count_counts_paths_not_copies_of_the_data
//! One model, one session: the sparse half is derived, not loaded again.
//!
//! BGE-M3 answers with two vectors from one forward pass -- a dense one pooled
//! out of the output and a sparse one weighting each token's hidden state. The
//! obvious way to get both out of a library is to build the library's dense
//! model and its sparse model side by side, and that is what this tree did.
//! It works. It also loads the weights twice: the two entries point at the same
//! repository and the same files, and each session reads them into buffers of
//! its own, so the pages are not shared and the process pays for a second copy.
//!
//! That failure has no wrong answer to give. Retrieval keeps working, the
//! vectors keep matching, and the only reading that moves is memory -- which is
//! why nothing here noticed until the boot marks split the startup into phases
//! and a delta the size of a whole set of weights appeared between two of them.
//! A gate is the only thing that can hold a shape whose only symptom is a
//! number in a graph nobody was looking at.
//!
//! So: no production line names fastembed's second-session constructors, and
//! the derived path is present rather than merely intended. Both directions are
//! needed. Without the first, a second session can be reintroduced and read as
//! an ordinary refactor; without the second, the sparse half can be dropped and
//! every test still passes, because a provider without it answers dense
//! requests and refuses sparse ones politely.
//!
//! Test modules are not scanned, and that is the point: the equivalence test
//! beside the weights builds a `SparseTextEmbedding` on purpose, to check the
//! derived vectors against the ones the second session produced.
//!
//! Ablation, against the revision this landed on rather than a specimen written
//! for the occasion: that revision's `embedding_provider.rs` built the sparse
//! options and the sparse session as production code, and the census reports
//! all three lines. Deleting the derivation instead leaves the second test red
//! and the first green -- the two directions really are independent.

use crate::common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, relative_name};

/// The types that each open a session over the model files.
///
/// Named individually rather than by the `Sparse` prefix: a prefix match would
/// also catch a type about sparse data structures that has nothing to do with
/// weights, and a gate that is red on legitimate code gets switched off before
/// it catches anything.
const SECOND_SESSION: &[&str] = &["SparseTextEmbedding", "SparseInitOptions", "SparseModel"];

/// The call that turns one forward pass into the sparse half.
///
/// The name is the one the derivation has; a caller reading this gate after a
/// rename should move the string, not delete the check.
const DERIVATION: &str = "sparse_from_hidden(";

/// A production line that names one of the types above.
#[derive(Debug)]
struct Found {
    location: String,
    line: String,
}

#[test]
fn no_production_line_builds_a_second_session_for_the_sparse_half() {
    let root = repo_root();
    let mut found = Vec::new();
    for source in production_sources(&root) {
        let rel = relative_name(&root, &source);
        let Ok(text) = std::fs::read_to_string(&source) else {
            continue;
        };
        for (number, line) in production_lines(&text).into_iter().enumerate() {
            if SECOND_SESSION.iter().any(|name| line.contains(name)) {
                found.push(Found {
                    location: format!("{rel}:{}", number + 1),
                    line: line.trim().to_string(),
                });
            }
        }
    }

    let offenders = found
        .iter()
        .map(|f| format!("{}  {}", f.location, f.line))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        found.is_empty(),
        "these lines ask fastembed for a second session over the model files, which buys a \
         second private copy of the weights:\n{offenders}\n\
         The sparse half is the token hidden states of the same forward pass through the sparse \
         head; see cog-memory's `sparse_head`. If a second session is genuinely needed, the \
         costing has to be measured at boot first -- the delta between the `embedding-dense` \
         and `embedding-sparse` marks is where it shows up."
    );
}

#[test]
fn the_sparse_half_is_derived_somewhere_in_production() {
    let root = repo_root();
    let mut callers = Vec::new();
    for source in production_sources(&root) {
        let rel = relative_name(&root, &source);
        let Ok(text) = std::fs::read_to_string(&source) else {
            continue;
        };
        for (number, line) in production_lines(&text).into_iter().enumerate() {
            if line.contains(DERIVATION) {
                callers.push(format!("{rel}:{}", number + 1));
            }
        }
    }

    assert!(
        !callers.is_empty(),
        "no production line calls `{DERIVATION}`, so the sparse half is not being derived from \
         the forward pass at all. A provider that drops it answers dense requests and refuses \
         sparse ones politely, so every other test stays green while hybrid retrieval is gone."
    );
}
