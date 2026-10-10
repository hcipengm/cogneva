//! `cogneva_knowledge_retrieval_total` is written only when a retrieval
//! consults a layer, and the cells it would write are also the only thing that
//! tells a cell apart from a series that was never published. A process that
//! comes up and consults nothing -- no work to run, or the work held upstream --
//! therefore has to publish the closed set itself, and a process that reaches no
//! knowledge backend has to say so in the series' own cells rather than leaving
//! it to a warning in a log. These tests pin both, because either one going
//! quiet is invisible from every reader: the scrape and the alert rule both read
//! the series, and an unwritten cell and a zero cell look the same there.

use std::sync::Arc;

use cog_core::contract::knowledge::{
    publish_no_knowledge_backend, seed_retrieval_cells, RETRIEVAL_LAYERS, RETRIEVAL_OUTCOMES,
    RETRIEVAL_OUTCOME_ABSENT,
};
use cog_core::metric_names::KNOWLEDGE_RETRIEVAL_TOTAL;
use cog_core::MetricsBackend;

mod common;

use common::mocks::MockMetricsBackend;

fn series_calls(backend: &MockMetricsBackend) -> Vec<(String, String, f64)> {
    backend
        .recorded_calls()
        .into_iter()
        .filter(|r| r.name == KNOWLEDGE_RETRIEVAL_TOTAL.as_str())
        .map(|r| {
            (
                r.labels.get("layer").cloned().unwrap_or_default(),
                r.labels.get("outcome").cloned().unwrap_or_default(),
                r.value,
            )
        })
        .collect()
}

/// Every cell of the closed set is written as zero, so a boot that runs no
/// retrieval reads as a grid of zeros rather than as the absence a build with
/// no such reading would show.
#[tokio::test]
async fn seeding_writes_the_whole_cross_of_layers_and_outcomes_at_zero() {
    let recorder = Arc::new(MockMetricsBackend::new());
    let metrics: Arc<dyn MetricsBackend> = recorder.clone();

    seed_retrieval_cells(&metrics).await;

    let calls = series_calls(&recorder);
    assert_eq!(
        calls.len(),
        RETRIEVAL_LAYERS.len() * RETRIEVAL_OUTCOMES.len()
    );
    for layer in RETRIEVAL_LAYERS {
        for outcome in RETRIEVAL_OUTCOMES {
            assert!(
                calls
                    .iter()
                    .any(|(l, o, v)| l == layer && o == outcome && *v == 0.0),
                "no zero seed for {layer}/{outcome}: the cell would be missing \
                 from a scrape of a process that has not consulted anything"
            );
        }
    }
}

/// A process that obtained no backend consults no layer, and says so on every
/// layer's `absent` cell. The wiki cell is the one that names this rather than
/// ordinary traffic: a process holding the backend never writes `absent` for
/// wiki, since the wiki layer is what the backend is built from.
#[tokio::test]
async fn a_missing_backend_is_published_as_absent_on_every_layer() {
    let recorder = Arc::new(MockMetricsBackend::new());
    let metrics: Arc<dyn MetricsBackend> = recorder.clone();

    publish_no_knowledge_backend(&metrics).await;

    let calls = series_calls(&recorder);
    assert_eq!(calls.len(), RETRIEVAL_LAYERS.len());
    for layer in RETRIEVAL_LAYERS {
        assert!(
            calls
                .iter()
                .any(|(l, o, v)| l == layer && o == RETRIEVAL_OUTCOME_ABSENT && *v == 1.0),
            "a missing backend did not land on {layer}: the boot would look the \
             same as one that simply had nothing to consult"
        );
    }
}
