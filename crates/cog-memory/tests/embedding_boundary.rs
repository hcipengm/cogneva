//! Whether the embedding loader can be fed entirely from a directory, measured on the weights.
//!
//! The memory layers keep their summary vectors in a vector store, and the vectors are
//! produced by BGE-M3 running in this process: one session for the dense vector, another for
//! the sparse one, both reading the same repository. Nothing about that path can be built on
//! a network call at load time -- the cluster reaches no model hub -- so the weights have to
//! be a directory somebody put there first, and the loader has to find them in it. The
//! producer is `deploy/scripts/fetch-model-weights.sh --model bge-m3`; this is the measurement that says
//! the directory it writes is a directory the loader can actually load from.
//!
//! It matters because the layout is not one anybody checks by eye. The loader looks up the
//! commit in `refs/main` and opens `snapshots/<commit>/<file>`, so a missing newline there,
//! a symlink that resolves nowhere, or a file list that forgot the tensor the ONNX graph
//! reads beside the model file all look exactly like a directory full of weights -- until
//! the process tries to use them. The numbers this prints (all on the weights fetched on
//! 2026-10-10, repository `BAAI/bge-m3`, revision `e44369c5`):
//!
//! - **A dense vector is 1024 wide**, the width the vector store's collection was declared
//!   with, so a vector this loader produces fits a point already in it.
//! - **The dense control separates**: `如何申请年假？` against an annual-leave policy scores
//!   cosine **0.717**, the same question against an unrelated canteen menu **0.430**. A
//!   loader that returned whatever its bytes happened to mean would not keep that order.
//! - **The sparse session answers too**, and its vector is a map from token id to weight with
//!   the two sides the same length -- the shape the sparse column and the collection's named
//!   sparse vector both take.
//!
//! It needs about 4.3 GiB of weights resident (each session loads its own), so it is ignored
//! by default and is not in CI (a runner has neither the weights nor a mirror to fetch them
//! from):
//!
//! ```text
//! deploy/scripts/fetch-model-weights.sh --model bge-m3 --dest /srv/cogneva/models/fastembed
//! FASTEMBED_CACHE_DIR=/srv/cogneva/models/fastembed \
//!   cargo test -p cog-memory --test embedding_boundary -- --ignored --nocapture
//! ```
//!
//! Re-run it before treating the numbers as current: they are a statement about specific
//! weights, not about the idea of a local embedding session. What this file asserts is only
//! what has to hold for the directory to be usable at all: that a vector of the declared
//! width comes out, that the sparse side is non-empty, and that the dense vectors keep a
//! relevant pair apart from an irrelevant one.

use std::path::PathBuf;

use cog_core::EmbeddingProvider;
use cog_memory::FastEmbedProvider;

/// The width the summary collection is declared with; a vector of another width is a vector
/// the store will refuse.
const DIM: usize = 1024;

/// The relevant pair the control uses, and an unrelated one: a loader that read the wrong
/// bytes, or half a model, would not keep these in this order.
const QUERY: &str = "如何申请年假？";
const RELEVANT: &str = "员工年假申请流程：填写申请表，由直属主管审批后交人事备案。";
const IRRELEVANT: &str = "公司食堂本周菜单：周一红烧肉，周二宫保鸡丁。";

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    dot / (na * nb)
}

#[tokio::test]
#[ignore = "needs the BGE-M3 weights in FASTEMBED_CACHE_DIR; see the module comment"]
async fn the_embedding_loader_reads_both_sessions_from_a_directory() {
    let dir = std::env::var("FASTEMBED_CACHE_DIR").expect(
        "set FASTEMBED_CACHE_DIR to a directory the weights were fetched into \
         (deploy/scripts/fetch-model-weights.sh --model bge-m3)",
    );
    // fastembed reads HF_HOME first and only then its own variable, so a set HF_HOME would
    // send the load somewhere else and the failure would read as a bad weight directory.
    assert!(
        std::env::var("HF_HOME").is_err(),
        "HF_HOME is set, and fastembed prefers it over FASTEMBED_CACHE_DIR; unset it for this \
         run or point it at the same directory"
    );

    let provider = FastEmbedProvider::try_new_with_cache_dir(Some(PathBuf::from(&dir)))
        .expect("the weights did not load from FASTEMBED_CACHE_DIR");

    assert_eq!(
        provider.dimension(),
        DIM,
        "the provider reports a width the summary collection was not declared with"
    );

    // The dense side: two vectors out, of the declared width.
    let dense = provider
        .embed(vec![QUERY.to_string(), RELEVANT.to_string()])
        .await
        .expect("the dense session did not embed");
    assert_eq!(dense.len(), 2, "one vector per input, in order");
    for vector in &dense {
        assert_eq!(
            vector.len(),
            DIM,
            "a dense vector narrower or wider than the collection's width cannot be stored"
        );
        assert!(
            vector.iter().all(|v| v.is_finite()),
            "a dense vector with a non-finite component cannot be compared to anything"
        );
        assert!(
            vector.iter().any(|v| *v != 0.0),
            "an all-zero dense vector would rank every passage the same"
        );
    }

    // The sparse side: token ids with weights, the two sides the same length.
    let sparse = provider
        .embed_sparse(vec![QUERY.to_string()])
        .await
        .expect("the sparse session did not embed");
    assert_eq!(sparse.len(), 1, "one sparse vector per input");
    assert!(
        !sparse[0].indices.is_empty(),
        "an empty sparse vector carries no token and would match nothing"
    );
    assert_eq!(
        sparse[0].indices.len(),
        sparse[0].values.len(),
        "the sparse vector's indices and weights have to line up"
    );
    let ids: std::collections::BTreeSet<u32> = sparse[0].indices.iter().copied().collect();
    assert_eq!(
        ids.len(),
        sparse[0].indices.len(),
        "a repeated token id would make the sparse vector ambiguous to a dot product"
    );

    // The control, and the only thing asserted about quality here: a relevant pair has to
    // score above an irrelevant one, or the sweep below would be reading numbers from a
    // session that cannot tell the two apart.
    let query = provider
        .embed(vec![QUERY.to_string()])
        .await
        .expect("embedded")
        .remove(0);
    let relevant = provider
        .embed(vec![RELEVANT.to_string()])
        .await
        .expect("embedded")
        .remove(0);
    let irrelevant = provider
        .embed(vec![IRRELEVANT.to_string()])
        .await
        .expect("embedded")
        .remove(0);
    let (relevant_score, irrelevant_score) =
        (cosine(&query, &relevant), cosine(&query, &irrelevant));
    println!(
        "\n== control\na relevant passage {relevant_score:.3}, an irrelevant one \
         {irrelevant_score:.3}"
    );
    assert!(
        relevant_score > irrelevant_score + 0.1,
        "the weights do not separate a pair they were trained for ({relevant_score:.3} vs \
         {irrelevant_score:.3}); a vector this loader produced would rank the two the same"
    );
}
