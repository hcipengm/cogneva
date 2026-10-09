//! Importance is the one field the model and the extractor both rate, and the
//! field decay demotes over time. These tests pin the effect it has on
//! retrieval ranking, and carry the effect as a measured self-recall number so
//! a regression is a number rather than a claim.

use cog_core::{MemoryBackend, SchemaEntry, SchemaKind, SourceRef, SummaryEntry};
use cog_memory::*;

fn source_ref(raw_id: &str) -> SourceRef {
    SourceRef::new(format!("memory://{}", raw_id), "test/v1")
}

fn summary(id: &str, embedding: Vec<f32>, importance: f32) -> SummaryEntry {
    SummaryEntry::new(id, "default", id, embedding, "m", source_ref(id)).with_importance(importance)
}

fn axis(a: f32, b: f32, c: f32) -> Vec<f32> {
    vec![a, b, c]
}

#[tokio::test]
async fn higher_importance_ranks_first_at_equal_similarity() {
    let backend = MemoryMemoryBackend::new();
    // Identical vectors carry identical similarity to the query, so the only
    // thing left to order them is importance.
    backend
        .store_summary("default", &summary("low", axis(1.0, 0.0, 0.0), 0.3))
        .await
        .unwrap();
    backend
        .store_summary("default", &summary("high", axis(1.0, 0.0, 0.0), 1.0))
        .await
        .unwrap();

    let results = backend
        .search_summary("default", &axis(1.0, 0.0, 0.0), 2, None)
        .await
        .unwrap();
    assert_eq!(results[0].entry.id, "high");
    assert_eq!(results[1].entry.id, "low");
    assert!(results[0].score > results[1].score);
}

#[tokio::test]
async fn a_low_importance_near_match_sinks_below_a_more_important_farther_one() {
    let backend = MemoryMemoryBackend::new();
    // `near` is far more similar to the query than `far`, but carries a tenth
    // of the value: the weight has to sink it.
    backend
        .store_summary("default", &summary("near", axis(0.98, 0.2, 0.0), 0.1))
        .await
        .unwrap();
    backend
        .store_summary("default", &summary("far", axis(0.8, 0.6, 0.0), 1.0))
        .await
        .unwrap();

    let results = backend
        .search_summary("default", &axis(0.98, 0.2, 0.0), 2, None)
        .await
        .unwrap();
    assert_eq!(
        results[0].entry.id, "far",
        "importance must be able to outrank raw similarity"
    );
    assert_eq!(results[1].entry.id, "near");
}

#[tokio::test]
async fn an_anti_correlated_entry_is_never_promoted_by_importance() {
    let backend = MemoryMemoryBackend::new();
    // `opposite` is maximally important but points away from the query. Scaling
    // a negative cosine by importance must not lift it past a related entry.
    backend
        .store_summary("default", &summary("related", axis(0.1, 1.0, 0.0), 0.1))
        .await
        .unwrap();
    backend
        .store_summary("default", &summary("opposite", axis(-1.0, 0.0, 0.0), 1.0))
        .await
        .unwrap();

    let results = backend
        .search_summary("default", &axis(1.0, 0.0, 0.0), 2, None)
        .await
        .unwrap();
    assert_eq!(results[0].entry.id, "related");
    assert_eq!(results[1].score, 0.0, "an unrelated entry scores zero");
}

/// The clamp maps every non-positive similarity onto one score, so the score
/// alone does not order that half. The ranking key keeps the raw similarity as
/// its second key, which is what makes these three come back in similarity
/// order rather than in whatever order the store happened to hand them over in.
#[tokio::test]
async fn the_unrelated_half_comes_back_in_similarity_order() {
    let backend = MemoryMemoryBackend::new();
    // Cosine against the query (1, 0, 0), exactly: -0.9, -0.1, and 0.0.
    let unrelated = [
        ("most-opposite", axis(-0.9, 0.435_889_9, 0.0)),
        ("nearly-orthogonal", axis(-0.1, 0.994_987_5, 0.0)),
        ("orthogonal", axis(0.0, 1.0, 0.0)),
    ];
    for (id, embedding) in &unrelated {
        backend
            .store_summary("default", &summary(id, embedding.clone(), 0.5))
            .await
            .unwrap();
    }
    backend
        .store_summary(
            "default",
            &summary("related", axis(0.2, 0.979_795_9, 0.0), 0.5),
        )
        .await
        .unwrap();

    let query = axis(1.0, 0.0, 0.0);
    let order = |results: Vec<cog_core::SummarySearchResult>| {
        results.into_iter().map(|r| r.entry.id).collect::<Vec<_>>()
    };
    let first = order(
        backend
            .search_summary("default", &query, 10, None)
            .await
            .unwrap(),
    );
    assert_eq!(
        first,
        vec![
            "related",
            "orthogonal",
            "nearly-orthogonal",
            "most-opposite"
        ],
        "the related entry leads, and the flat zero half keeps its similarity order"
    );

    // Same query over the same corpus: the order must not depend on the order
    // the map happened to hand the entries over in.
    let second = order(
        backend
            .search_summary("default", &query, 10, None)
            .await
            .unwrap(),
    );
    assert_eq!(first, second, "a ranking must not vary between calls");
}

/// `importance` is a plain weight on a declared `0.0..=1.0` scale, so its floor
/// is the one value that makes an entry unrankable: everything it could score is
/// multiplied by zero. That is a consequence of the declared range rather than a
/// hidden filter — the producers that *rate* entries keep a floor of their own
/// (a rating of 1 out of 10), so reaching zero takes a caller passing it
/// explicitly, and this test is what says so.
#[tokio::test]
async fn an_explicit_zero_importance_ranks_last_rather_than_disappearing() {
    let backend = MemoryMemoryBackend::new();
    // The zero-importance entry is the *exact* match for the query; the other is
    // merely close. Importance still has to put it last.
    backend
        .store_summary("default", &summary("zero", axis(1.0, 0.0, 0.0), 0.0))
        .await
        .unwrap();
    backend
        .store_summary(
            "default",
            &summary("close", axis(0.9, 0.435_889_9, 0.0), 0.9),
        )
        .await
        .unwrap();

    let results = backend
        .search_summary("default", &axis(1.0, 0.0, 0.0), 10, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2, "a zero weight must not remove the entry");
    assert_eq!(results[0].entry.id, "close");
    assert_eq!(results[1].entry.id, "zero");
    assert_eq!(results[1].score, 0.0);
}

#[tokio::test]
async fn schema_search_ranks_by_importance() {
    let backend = MemoryMemoryBackend::new();
    for (id, importance) in [("s-low", 0.2_f32), ("s-high", 0.9_f32)] {
        let entry = SchemaEntry::new(
            id,
            "default",
            SchemaKind::Entity,
            "Postgres",
            "postgres",
            source_ref(id),
        )
        .with_importance(importance);
        backend.store_schema("default", &entry).await.unwrap();
    }

    let results = backend
        .search_schema("default", "postgres", 10)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].entry.id, "s-high");
    assert_eq!(results[1].entry.id, "s-low");
    assert!(results[0].score > results[1].score);
}

/// The retrieval-quality reading: for every entry, query the store with the
/// entry's *own* vector and ask whether it comes back first. Under pure
/// similarity each entry is its own exact match, so every one must self-hit.
/// Weighting by importance is allowed to displace exactly the entries whose
/// value is low enough to lose to a similar richer one — and nothing else. A
/// run where a high-importance entry stops finding itself is a regression, and
/// this test reports it as a number.
#[tokio::test]
async fn self_recall_measures_what_importance_weighting_does_to_findability() {
    // Similarity-space layout: e1/e2/e3 are orthogonal, e4 leans between e1 and
    // e2 so that a rich neighbour can outrank it.
    let corpus = [
        ("e1", axis(1.0, 0.0, 0.0), 0.95_f32),
        ("e2", axis(0.0, 1.0, 0.0), 0.9),
        ("e3", axis(0.0, 0.0, 1.0), 0.9),
        ("e4", axis(0.7, 0.7, 0.0), 0.1),
    ];

    let plain = MemoryMemoryBackend::new();
    let weighted = MemoryMemoryBackend::new();
    for (id, embedding, importance) in &corpus {
        plain
            .store_summary("default", &summary(id, embedding.clone(), 1.0))
            .await
            .unwrap();
        weighted
            .store_summary("default", &summary(id, embedding.clone(), *importance))
            .await
            .unwrap();
    }

    async fn top1(backend: &MemoryMemoryBackend, embedding: &[f32]) -> Option<String> {
        backend
            .search_summary("default", embedding, 1, None)
            .await
            .unwrap()
            .first()
            .map(|r| r.entry.id.clone())
    }

    let mut plain_misses = Vec::new();
    let mut weighted_misses = Vec::new();
    for (id, embedding, _) in corpus {
        if top1(&plain, &embedding).await.as_deref() != Some(id) {
            plain_misses.push(id);
        }
        if top1(&weighted, &embedding).await.as_deref() != Some(id) {
            weighted_misses.push(id);
        }
    }

    assert!(
        plain_misses.is_empty(),
        "pure similarity must self-recall every entry, missed {plain_misses:?}"
    );
    assert_eq!(
        weighted_misses,
        vec!["e4"],
        "weighting may only displace the low-value outlier"
    );
}
