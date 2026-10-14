//! The pin-wiring census, as a scraped reading.
//!
//! The runner audits the pin graph once, at startup, and logs its verdict. For
//! the family this module reads, that log is the only surface there is: one
//! `info!` per pin, with the aggregate behind `debug!`, and `enforce` looking
//! only at required unsatisfied reads. So "published here, read by nobody"
//! reaches whoever tails the log and nothing else -- not the exposition, not a
//! rule, not a ledger. A family with no reading surface is a family whose size
//! nobody notices changing: it grew to thirty-one pins without a single reading
//! ever disagreeing.
//!
//! Two series, because either half alone answers the wrong question. The count
//! says how many pins are in this state, and it stays the same when the
//! population changes underneath it. The per-pin series carries the names. The
//! count is derivable from those with `count()`, and is still published on its
//! own: an absent family and a family of zero are different readings, and only
//! one of them means "nothing is left unread".
//!
//! Nothing reads a threshold off these. A nonzero count is not by itself a
//! fault -- a pin published for a reader in another process lands in this same
//! family, and the census cannot tell that apart from a pin with no reader
//! anywhere. What this reading is for is the question it can answer: which types,
//! and how many.

use cog_core::{
    DimensionSpec, Observable, PinAudit, PluginContext, RawMetric, SFResult, TraceFragment,
};

/// How many pins were published and never read, in this process.
pub const PIN_CENSUS_UNCONSUMED_COUNT_METRIC: &str = "cogneva_plugin_pin_unconsumed_count";

/// One series per such pin, value 1, labels `pin` (the compiler's name for the
/// type) and `publisher` (the plugin that published it, or the binary root).
pub const PIN_CENSUS_UNCONSUMED_METRIC: &str = "cogneva_plugin_pin_unconsumed";

/// The label carrying the pinned type's name.
pub const PIN_LABEL: &str = "pin";

/// The label carrying the publisher's plugin name.
pub const PUBLISHER_LABEL: &str = "publisher";

/// What the census says when no plugin published the pin. The pin audit uses
/// the same spelling for the same case, so a reader of both sees one token.
const BINARY_ROOT: &str = "<binary root>";

/// Census a context's wiring.
///
/// The composition root calls this on the same context the runner audited, and
/// with the same descriptor list the runner was built from
/// (`plugin_registry::all_descriptors()`), so the reading and the startup log
/// line describe one graph rather than two evaluations of it. Two differences
/// from the runner's copy are harmless and worth naming, since both look like
/// they could matter: the runner sorts its descriptors topologically, and
/// config-driven filtering removes some from it. Order cannot change the verdict
/// (`evaluate` reads the wiring as observed; the one traversal it does over
/// descriptors, `requires_closure`, builds a map and walks it). Filtering can,
/// but only for plugins that are not there to publish or read anything -- and a
/// live plugin whose requirement was filtered out fails startup in
/// `validate_after_filter` before the census is ever taken.
pub fn census(ctx: &PluginContext) -> PinAudit {
    PinAudit::evaluate(&ctx.pin_wiring(), crate::plugin_registry::all_descriptors())
}

/// Holds the census until a scrape asks for it.
///
/// Published before the plugins initialise, because the gateway takes its
/// snapshot of the observables during `init`; filled once the runner has
/// finished starting them, which is the point where the whole graph has been
/// observed. Until then it reports nothing: a census nobody has taken is not a
/// census of zero.
#[derive(Default)]
pub struct PinCensusObservable {
    audit: tokio::sync::Mutex<Option<PinAudit>>,
}

impl PinCensusObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn record(&self, audit: PinAudit) {
        *self.audit.lock().await = Some(audit);
    }
}

#[async_trait::async_trait]
impl Observable for PinCensusObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let guard = self.audit.lock().await;
        let Some(audit) = guard.as_ref() else {
            return Ok(Vec::new());
        };
        let mut out = vec![RawMetric::new(
            PIN_CENSUS_UNCONSUMED_COUNT_METRIC,
            audit.unconsumed.len() as f64,
        )];
        for (pin, publishers) in &audit.unconsumed {
            // The publisher list is a label, not a series per publisher: a pin
            // with two publishers is one unread pin, and the second name would
            // otherwise read as a second pin in the count.
            let named: Vec<&str> = publishers
                .iter()
                .map(|p| p.unwrap_or(BINARY_ROOT))
                .collect();
            out.push(
                RawMetric::new(PIN_CENSUS_UNCONSUMED_METRIC, 1.0)
                    .with_label(PIN_LABEL, pin.name())
                    .with_label(PUBLISHER_LABEL, named.join(",")),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The census is a whole-process fact with no per-dimension branch.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// A type nobody reads, for the tests: the census names types, not values.
#[cfg(test)]
#[derive(Debug)]
struct NothingReadsThis;

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Pin, PinWiring};
    use std::sync::Arc;

    /// An observed graph with one publisher and no reader.
    fn one_unread_pin(publisher: Option<&'static str>) -> PinAudit {
        PinAudit::evaluate(
            &[PinWiring {
                pin: Pin::of::<NothingReadsThis>(),
                publishers: vec![publisher],
                consumers: Vec::new(),
            }],
            &[],
        )
    }

    fn value_of(metrics: &[RawMetric], name: &str) -> Option<f64> {
        metrics.iter().find(|m| m.name == name).map(|m| m.value)
    }

    #[tokio::test]
    async fn a_census_nobody_took_publishes_nothing() {
        let observable = PinCensusObservable::new();
        assert!(
            observable.collect_metrics("").await.unwrap().is_empty(),
            "before the runner has started the plugins there is no census, which is not a census of zero"
        );
    }

    #[tokio::test]
    async fn the_reading_publishes_the_count_and_the_name() {
        let observable = PinCensusObservable::new();
        observable.record(one_unread_pin(Some("cog-thing"))).await;
        let metrics = observable.collect_metrics("").await.unwrap();

        assert_eq!(
            value_of(&metrics, PIN_CENSUS_UNCONSUMED_COUNT_METRIC),
            Some(1.0),
            "the count is the size of the family"
        );
        let named: Vec<&RawMetric> = metrics
            .iter()
            .filter(|m| m.name == PIN_CENSUS_UNCONSUMED_METRIC)
            .collect();
        assert_eq!(named.len(), 1, "one series per unread pin");
        assert_eq!(
            named[0].labels.get(PIN_LABEL).map(String::as_str),
            Some(std::any::type_name::<NothingReadsThis>()),
            "the label is the compiler's name for the pinned type"
        );
        assert_eq!(
            named[0].labels.get(PUBLISHER_LABEL).map(String::as_str),
            Some("cog-thing"),
            "and the other names who published it"
        );
    }

    #[tokio::test]
    async fn a_census_with_nothing_unread_still_answers_zero() {
        let observable = PinCensusObservable::new();
        // Published and read: the same pin the other cases leave unread, this
        // time with a reader, so the family is empty rather than unknown.
        let audit = PinAudit::evaluate(
            &[PinWiring {
                pin: Pin::of::<NothingReadsThis>(),
                publishers: vec![Some("cog-thing")],
                consumers: vec![cog_core::PinConsumer {
                    owner: Some("cog-other"),
                    required: false,
                    during_init: false,
                }],
            }],
            &[],
        );
        assert!(audit.unconsumed.is_empty(), "the fixture has a reader");
        observable.record(audit).await;
        let metrics = observable.collect_metrics("").await.unwrap();

        assert_eq!(
            value_of(&metrics, PIN_CENSUS_UNCONSUMED_COUNT_METRIC),
            Some(0.0),
            "an empty family reads as zero, not as a family with no series"
        );
        assert!(
            !metrics
                .iter()
                .any(|m| m.name == PIN_CENSUS_UNCONSUMED_METRIC),
            "and there is no name to report"
        );
    }

    /// The reading and the startup log line must describe one graph. They are
    /// two calls to the same pure function, so what could differ is the input:
    /// the runner evaluates against its own (sorted, config-filtered) descriptor
    /// list, this module against the static one. Assert they agree on the real
    /// descriptor set with a pin nothing reads.
    #[tokio::test]
    async fn the_reading_agrees_with_the_runner_about_the_same_wiring() {
        let ctx = PluginContext::new(cog_core::Config::default());
        ctx.publish(Arc::new(NothingReadsThis));

        let runner =
            cog_core::PluginRunner::from_descriptors(crate::plugin_registry::all_descriptors())
                .expect("the registered descriptors are a valid graph");
        let logged = runner
            .audit_pins(&ctx)
            .expect("nothing required is missing, so the audit does not refuse startup");

        let read = census(&ctx);
        let names = |a: &PinAudit| {
            let mut v: Vec<&str> = a.unconsumed.iter().map(|(p, _)| p.name()).collect();
            v.sort_unstable();
            v
        };
        assert_eq!(
            names(&read),
            names(&logged),
            "the family the reading carries is the family the startup line reports"
        );
        assert!(!read.unconsumed.is_empty(), "the fixture pin is unread");
    }
}
