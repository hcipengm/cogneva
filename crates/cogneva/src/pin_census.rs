//! principle: feedback_a_family_member_with_several_parties_is_one_series_not_one_per_party
//!
//! The pin-wiring census, as a scraped reading.
//!
//! The runner audits the pin graph once, at startup, and logs its verdict. For
//! the families this module reads, that log is the whole surface there is: a
//! per-pin line at `info!` or `warn!`, the aggregate behind `debug!`, and
//! `enforce` looking only at required unsatisfied reads. So "published here,
//! read by nobody" reaches whoever tails the log and nothing else -- not the
//! exposition, not a rule, not a ledger. A family with no reading surface is a
//! family whose size nobody notices changing: it grew to thirty-one pins
//! without a single reading ever disagreeing.
//!
//! Four anomaly families, each published the same way: a count that always
//! answers, and one series per member carrying the names. Both halves are
//! needed. The count alone stays the same when the population changes
//! underneath it; the names alone make "this family is empty" and "this reading
//! is not wired up" look alike on the exposition. The count is derivable from
//! the per-member series with `count()`, and is still published on its own: an
//! absent family and a family of zero are different readings, and only one of
//! them means "nothing is left in this state".
//!
//! A fifth pair is not an anomaly family and is shaped differently: one series
//! per plugin in the descriptor set, carrying how many pins it published, zeros
//! included. The four above can only name a plugin that appears in the wiring,
//! and a plugin that produced no pin appears nowhere in it -- so "this plugin
//! published nothing" had no reading at all, and the descriptor table plus the
//! startup log on their own cannot tell it apart from a plugin that is simply
//! wired to something. Its members are the whole population rather than a
//! subset, which is why its count is the size of that population and not the
//! size of a fault.
//!
//! A member is its label set, so the count is the number of distinct members: a
//! plugin that reads the same pin twice while initialising is one member, not
//! two. That diverges from the startup log, which prints one line per read
//! record and so can name one more line than the exposition carries. The extra
//! line is the duplication showing itself -- the two lines carry the same three
//! names, which is why the exposition cannot hold both.
//!
//! The parties of a member are a label rather than a series each, so a pin with
//! two publishers stays one pin in its family instead of reading as two.
//!
//! Nothing reads a threshold off these. A member is not by itself a fault: a
//! pin published for a reader in another process lands in the same family as a
//! pin with no reader anywhere, an optional read is expected to go unsatisfied,
//! and the audit cannot tell those apart. What these readings are for is the
//! question they can answer: which types, and how many.
//!
//! One list the audit carries is deliberately not published: the pairs of an
//! unsatisfied read and an unconsumed publish that share a trailing type name.
//! It is derived from two families that are published, and its own log line
//! says why it cannot be read as a verdict -- the two names it pairs may be
//! different types that happen to end the same way. A reader who wants it can
//! pair the two published families by the same rule the audit uses.

use std::collections::BTreeSet;

use cog_core::{
    DimensionSpec, Observable, PinAudit, PluginContext, RawMetric, SFResult, TraceFragment,
};

/// How many pins were read and never published, in this process.
pub const PIN_CENSUS_UNSATISFIED_COUNT_METRIC: &str = "cogneva_plugin_pin_unsatisfied_count";

/// One series per such pin, value 1, labels `pin` and `reader`.
pub const PIN_CENSUS_UNSATISFIED_METRIC: &str = "cogneva_plugin_pin_unsatisfied";

/// How many pins were published and never read, in this process.
pub const PIN_CENSUS_UNCONSUMED_COUNT_METRIC: &str = "cogneva_plugin_pin_unconsumed_count";

/// One series per such pin, value 1, labels `pin` and `publisher`.
pub const PIN_CENSUS_UNCONSUMED_METRIC: &str = "cogneva_plugin_pin_unconsumed";

/// How many pins have more than one publisher.
pub const PIN_CENSUS_MULTI_PUBLISHED_COUNT_METRIC: &str =
    "cogneva_plugin_pin_multi_published_count";

/// One series per such pin, value 1, labels `pin` and `publisher`.
pub const PIN_CENSUS_MULTI_PUBLISHED_METRIC: &str = "cogneva_plugin_pin_multi_published";

/// How many init-time reads a missing `requires` edge left unordered, counting
/// a read once however many times the same plugin registered it.
pub const PIN_CENSUS_UNORDERED_INIT_READ_COUNT_METRIC: &str =
    "cogneva_plugin_pin_unordered_init_read_count";

/// One series per such read, value 1, labels `pin`, `reader` and `publisher`.
pub const PIN_CENSUS_UNORDERED_INIT_READ_METRIC: &str = "cogneva_plugin_pin_unordered_init_read";

/// The label carrying the pinned type's name.
pub const PIN_LABEL: &str = "pin";

/// The label carrying whoever published the pin.
pub const PUBLISHER_LABEL: &str = "publisher";

/// The label carrying whoever read the pin.
pub const READER_LABEL: &str = "reader";

/// How many plugins the descriptor set holds.
///
/// Not the number of plugins that published something: this is the population
/// the next metric is taken over, and it answers even when nothing published.
pub const PIN_CENSUS_PUBLISHES_COUNT_METRIC: &str = "cogneva_plugin_publishes_count";

/// One series per plugin, value = how many pins it published, label `plugin`.
///
/// The only family here whose value is not 1, and the only one whose members
/// include plugins that produced nothing. That is the point: a plugin with no
/// pin is absent from every other family, so without this line the question
/// "did this plugin publish anything at all" has no reading anywhere -- the
/// descriptor table says it exists, the startup log says it was started, and
/// the pin graph says nothing, because a pin it never published is not a pin.
///
/// A value of 0 is a fact and not by itself a fault: a plugin that only reads
/// pins, or one disabled by config, publishes nothing for two different
/// reasons and reads the same here. The label says which plugin; it does not
/// claim to say why.
pub const PIN_CENSUS_PUBLISHES_METRIC: &str = "cogneva_plugin_publishes";

/// The label carrying a plugin's name.
pub const PLUGIN_LABEL: &str = "plugin";

/// What the census says when no plugin stood in the role. The pin audit uses
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

/// Emit one family: its size, then one series per member with the names in
/// labels.
///
/// The size is the number of *distinct* members, which is not always the number
/// of rows the audit hands over. A plugin that reads the same pin twice while
/// initialising is recorded twice -- the demand list is a list of read calls,
/// not of pins -- and both records carry the same three names. The exposition
/// identifies a series by its label set, so those two rows are one series
/// whatever is published here: Prometheus keeps one sample and drops the other.
/// A size that added them up would be a number no scrape can ever show, and a
/// count nothing can check is worse than a count one smaller, because the one
/// claim this reading makes is that its size and its members agree.
///
/// The labels are sorted before they are compared, so two members are the same
/// member when they carry the same names with the same values, in whatever
/// order the caller wrote them.
fn publish_family(
    out: &mut Vec<RawMetric>,
    count_metric: &'static str,
    member_metric: &'static str,
    members: impl Iterator<Item = Vec<(&'static str, String)>>,
) {
    let mut distinct: BTreeSet<Vec<(&'static str, String)>> = BTreeSet::new();
    for mut labels in members {
        labels.sort();
        distinct.insert(labels);
    }
    out.push(RawMetric::new(count_metric, distinct.len() as f64));
    for labels in distinct {
        let mut metric = RawMetric::new(member_metric, 1.0);
        for (label, value) in labels {
            metric = metric.with_label(label, value);
        }
        out.push(metric);
    }
}

/// Whoever stood in a role, as one label value. A pin can have several parties
/// and the label says so; `""` only ever means the list was empty, which the
/// audit does not produce for a member of one of these families.
fn parties(names: impl Iterator<Item = Option<&'static str>>) -> String {
    names
        .map(|name| name.unwrap_or(BINARY_ROOT))
        .collect::<Vec<_>>()
        .join(",")
}

#[async_trait::async_trait]
impl Observable for PinCensusObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let guard = self.audit.lock().await;
        let Some(audit) = guard.as_ref() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();

        publish_family(
            &mut out,
            PIN_CENSUS_UNSATISFIED_COUNT_METRIC,
            PIN_CENSUS_UNSATISFIED_METRIC,
            audit.unsatisfied.iter().map(|(pin, consumers)| {
                vec![
                    (PIN_LABEL, pin.name().to_string()),
                    (READER_LABEL, parties(consumers.iter().map(|c| c.owner))),
                ]
            }),
        );

        publish_family(
            &mut out,
            PIN_CENSUS_UNCONSUMED_COUNT_METRIC,
            PIN_CENSUS_UNCONSUMED_METRIC,
            audit.unconsumed.iter().map(|(pin, publishers)| {
                vec![
                    (PIN_LABEL, pin.name().to_string()),
                    (PUBLISHER_LABEL, parties(publishers.iter().copied())),
                ]
            }),
        );

        publish_family(
            &mut out,
            PIN_CENSUS_MULTI_PUBLISHED_COUNT_METRIC,
            PIN_CENSUS_MULTI_PUBLISHED_METRIC,
            audit.multi_published.iter().map(|(pin, publishers)| {
                vec![
                    (PIN_LABEL, pin.name().to_string()),
                    (PUBLISHER_LABEL, parties(publishers.iter().copied())),
                ]
            }),
        );

        publish_family(
            &mut out,
            PIN_CENSUS_UNORDERED_INIT_READ_COUNT_METRIC,
            PIN_CENSUS_UNORDERED_INIT_READ_METRIC,
            audit.unordered_init_reads.iter().map(|read| {
                vec![
                    (PIN_LABEL, read.pin.name().to_string()),
                    (READER_LABEL, read.reader.to_string()),
                    (PUBLISHER_LABEL, read.publisher.to_string()),
                ]
            }),
        );

        // Published in full, zeros included, and it is the one family whose
        // members are not anomalies.
        out.push(RawMetric::new(
            PIN_CENSUS_PUBLISHES_COUNT_METRIC,
            audit.publishes_by_plugin.len() as f64,
        ));
        for (plugin, published) in &audit.publishes_by_plugin {
            out.push(
                RawMetric::new(PIN_CENSUS_PUBLISHES_METRIC, *published as f64)
                    .with_label(PLUGIN_LABEL, plugin.to_string()),
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

/// Types nobody reads, for the tests: the census names types, not values.
#[cfg(test)]
#[derive(Debug)]
struct NothingReadsThis;

#[cfg(test)]
#[derive(Debug)]
struct TwoPublishers;

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Pin, PinConsumer, PinWiring};
    use std::sync::Arc;

    fn reader(owner: &'static str) -> PinConsumer {
        PinConsumer {
            owner: Some(owner),
            required: false,
            during_init: false,
        }
    }

    /// The same read, taken while the plugin is still initialising.
    fn init_reader(owner: &'static str) -> PinConsumer {
        PinConsumer {
            during_init: true,
            ..reader(owner)
        }
    }

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

    fn members<'a>(metrics: &'a [RawMetric], name: &str) -> Vec<&'a RawMetric> {
        metrics.iter().filter(|m| m.name == name).collect()
    }

    fn label_of<'a>(metric: &'a RawMetric, label: &str) -> &'a str {
        metric
            .labels
            .get(label)
            .map(String::as_str)
            .unwrap_or("<missing>")
    }

    /// Every family the audit carries, by name, sorted -- the shape both sides
    /// of the agreement test are compared in.
    fn families(audit: &PinAudit) -> Vec<(String, Vec<String>)> {
        let sorted = |mut names: Vec<String>| {
            names.sort_unstable();
            names
        };
        vec![
            (
                "unsatisfied".to_string(),
                sorted(
                    audit
                        .unsatisfied
                        .iter()
                        .map(|(pin, _)| pin.name().to_string())
                        .collect(),
                ),
            ),
            (
                "unconsumed".to_string(),
                sorted(
                    audit
                        .unconsumed
                        .iter()
                        .map(|(pin, _)| pin.name().to_string())
                        .collect(),
                ),
            ),
            (
                "multi_published".to_string(),
                sorted(
                    audit
                        .multi_published
                        .iter()
                        .map(|(pin, _)| pin.name().to_string())
                        .collect(),
                ),
            ),
            (
                "unordered_init_reads".to_string(),
                sorted(
                    audit
                        .unordered_init_reads
                        .iter()
                        .map(|read| format!("{} <-{}", read.pin.name(), read.reader))
                        .collect(),
                ),
            ),
        ]
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
        let named = members(&metrics, PIN_CENSUS_UNCONSUMED_METRIC);
        assert_eq!(named.len(), 1, "one series per unread pin");
        assert_eq!(
            label_of(named[0], PIN_LABEL),
            std::any::type_name::<NothingReadsThis>(),
            "the label is the compiler's name for the pinned type"
        );
        assert_eq!(
            label_of(named[0], PUBLISHER_LABEL),
            "cog-thing",
            "and the other names who published it"
        );
    }

    /// A descriptor the census only reads the name of. The factory is never
    /// called: what is counted here is who published, and that comes from the
    /// observed wiring and the names in the set.
    fn unused_factory() -> Box<dyn cog_core::SystemPlugin> {
        unreachable!("the census reads descriptor names, never factories")
    }

    fn descriptor(name: &'static str) -> cog_core::PluginDescriptor {
        cog_core::PluginDescriptor {
            name,
            requires: &[],
            optional_requires: &[],
            factory: unused_factory,
        }
    }

    /// The one reading that survives a plugin publishing nothing.
    ///
    /// Every other family is built out of the wiring, and a plugin that
    /// published no pin leaves no entry in it. So the plugin that is on the
    /// table, starts, and wires up nothing is the one case none of the other
    /// series can name -- and it reads exactly like a plugin that was never
    /// there, which is the mistake this reading exists to make impossible.
    #[tokio::test]
    async fn a_plugin_that_published_no_pin_appears_with_its_zero() {
        let descriptors = [
            descriptor("cog-publisher"),
            descriptor("cog-idle"),
            descriptor("cog-other"),
        ];
        let audit = PinAudit::evaluate(
            &[PinWiring {
                pin: Pin::of::<NothingReadsThis>(),
                publishers: vec![Some("cog-publisher")],
                consumers: vec![reader("cog-other")],
            }],
            &descriptors,
        );

        let observable = PinCensusObservable::new();
        observable.record(audit).await;
        let metrics = observable.collect_metrics("").await.unwrap();

        assert_eq!(
            value_of(&metrics, PIN_CENSUS_PUBLISHES_COUNT_METRIC),
            Some(3.0),
            "the count is the size of the descriptor set, not of the fault"
        );
        let published: Vec<(&str, f64)> = members(&metrics, PIN_CENSUS_PUBLISHES_METRIC)
            .into_iter()
            .map(|m| (label_of(m, PLUGIN_LABEL), m.value))
            .collect();
        assert_eq!(
            published,
            vec![
                ("cog-publisher", 1.0),
                ("cog-idle", 0.0),
                ("cog-other", 0.0),
            ],
            "every plugin is a series, and the ones that published nothing say zero"
        );
    }

    /// Every family is carried, and the parties of a member are a label rather
    /// than a series of their own: a pin published twice is one pin.
    #[tokio::test]
    async fn every_family_carries_its_count_and_its_names() {
        let mut audit = PinAudit::evaluate(
            &[
                // Read, never published.
                PinWiring {
                    pin: Pin::of::<NothingReadsThis>(),
                    publishers: Vec::new(),
                    consumers: vec![reader("cog-reader")],
                },
                // Two publishers, so which instance a reader gets depends on
                // the order they initialised in.
                PinWiring {
                    pin: Pin::of::<TwoPublishers>(),
                    publishers: vec![Some("cog-first"), Some("cog-second")],
                    consumers: vec![reader("cog-reader")],
                },
            ],
            &[],
        );
        // The unordered family is derived from a descriptor graph, which this
        // fixture has none of: what is under test here is the mapping from an
        // audit to series, and the agreement test below covers the derivation.
        audit
            .unordered_init_reads
            .push(cog_core::UnorderedInitRead {
                pin: Pin::of::<TwoPublishers>(),
                reader: "cog-reader",
                publisher: "cog-first",
            });

        let observable = PinCensusObservable::new();
        observable.record(audit).await;
        let metrics = observable.collect_metrics("").await.unwrap();

        assert_eq!(
            value_of(&metrics, PIN_CENSUS_UNSATISFIED_COUNT_METRIC),
            Some(1.0)
        );
        let unsatisfied = members(&metrics, PIN_CENSUS_UNSATISFIED_METRIC);
        assert_eq!(unsatisfied.len(), 1);
        assert_eq!(
            label_of(unsatisfied[0], READER_LABEL),
            "cog-reader",
            "the reader is named on the member, not in a series of its own"
        );

        assert_eq!(
            value_of(&metrics, PIN_CENSUS_MULTI_PUBLISHED_COUNT_METRIC),
            Some(1.0)
        );
        let multi = members(&metrics, PIN_CENSUS_MULTI_PUBLISHED_METRIC);
        assert_eq!(
            multi.len(),
            1,
            "two publishers are one pin, or the count and the list would disagree"
        );
        assert_eq!(label_of(multi[0], PUBLISHER_LABEL), "cog-first,cog-second");

        assert_eq!(
            value_of(&metrics, PIN_CENSUS_UNORDERED_INIT_READ_COUNT_METRIC),
            Some(1.0)
        );
        let unordered = members(&metrics, PIN_CENSUS_UNORDERED_INIT_READ_METRIC);
        assert_eq!(unordered.len(), 1);
        assert_eq!(label_of(unordered[0], READER_LABEL), "cog-reader");
        assert_eq!(label_of(unordered[0], PUBLISHER_LABEL), "cog-first");
    }

    /// A read the same plugin registered twice is one member, and the size is
    /// the number of members.
    ///
    /// The demand list holds read calls, not pins: a plugin that reads one pin
    /// twice while initialising is recorded twice, and both records carry the
    /// same pin, reader and publisher. The exposition identifies a series by
    /// its label set, so those two records are one series whatever is
    /// published -- which is the shape the first live reading of this family
    /// had, a size one larger than the number of series any query could count.
    /// A reading whose two halves cannot be reconciled is not a reading.
    #[tokio::test]
    async fn a_read_registered_twice_is_one_member_and_the_size_says_so() {
        let audit = PinAudit::evaluate(
            &[PinWiring {
                pin: Pin::of::<TwoPublishers>(),
                publishers: vec![Some("cog-publisher")],
                consumers: vec![init_reader("cog-reader"), init_reader("cog-reader")],
            }],
            &[],
        );
        assert_eq!(
            audit.unordered_init_reads.len(),
            2,
            "the audit keeps one entry per read call, and there were two"
        );

        let observable = PinCensusObservable::new();
        observable.record(audit).await;
        let metrics = observable.collect_metrics("").await.unwrap();

        let families = [
            (
                PIN_CENSUS_UNSATISFIED_COUNT_METRIC,
                PIN_CENSUS_UNSATISFIED_METRIC,
            ),
            (
                PIN_CENSUS_UNCONSUMED_COUNT_METRIC,
                PIN_CENSUS_UNCONSUMED_METRIC,
            ),
            (
                PIN_CENSUS_MULTI_PUBLISHED_COUNT_METRIC,
                PIN_CENSUS_MULTI_PUBLISHED_METRIC,
            ),
            (
                PIN_CENSUS_UNORDERED_INIT_READ_COUNT_METRIC,
                PIN_CENSUS_UNORDERED_INIT_READ_METRIC,
            ),
        ];
        for (count, member) in families {
            let named = members(&metrics, member);
            assert_eq!(
                value_of(&metrics, count),
                Some(named.len() as f64),
                "the size of {member} has to be the number of its series, or the two halves of the reading disagree"
            );
        }
        assert_eq!(
            value_of(&metrics, PIN_CENSUS_UNORDERED_INIT_READ_COUNT_METRIC),
            Some(1.0),
            "two records under one label set are one member"
        );
    }

    #[tokio::test]
    async fn a_census_with_no_anomalies_answers_zero_for_every_family() {
        let observable = PinCensusObservable::new();
        // Published and read: the same pin the other cases leave unread, this
        // time with a reader, so the family is empty rather than unknown.
        let audit = PinAudit::evaluate(
            &[PinWiring {
                pin: Pin::of::<NothingReadsThis>(),
                publishers: vec![Some("cog-thing")],
                consumers: vec![reader("cog-other")],
            }],
            &[],
        );
        assert!(
            audit.unconsumed.is_empty() && audit.unsatisfied.is_empty(),
            "the fixture is a healthy read"
        );
        observable.record(audit).await;
        let metrics = observable.collect_metrics("").await.unwrap();

        for count in [
            PIN_CENSUS_UNSATISFIED_COUNT_METRIC,
            PIN_CENSUS_UNCONSUMED_COUNT_METRIC,
            PIN_CENSUS_MULTI_PUBLISHED_COUNT_METRIC,
            PIN_CENSUS_UNORDERED_INIT_READ_COUNT_METRIC,
        ] {
            assert_eq!(
                value_of(&metrics, count),
                Some(0.0),
                "an empty family reads as zero, not as a family with no series: {}",
                count
            );
        }
        for member in [
            PIN_CENSUS_UNSATISFIED_METRIC,
            PIN_CENSUS_UNCONSUMED_METRIC,
            PIN_CENSUS_MULTI_PUBLISHED_METRIC,
            PIN_CENSUS_UNORDERED_INIT_READ_METRIC,
        ] {
            assert!(
                members(&metrics, member).is_empty(),
                "and there is no member to name: {}",
                member
            );
        }
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
        assert_eq!(
            families(&read),
            families(&logged),
            "every family the reading carries is the family the startup line reports"
        );
        assert!(
            !read.unconsumed.is_empty(),
            "the fixture pin is unread, so the comparison is not empty on both sides"
        );
    }
}
