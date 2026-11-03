//! The capability ledger: three codes plus "was this judged at all".
//!
//! Each case here pins a distinction that a reader downstream actually selects
//! on. They are written against the ledger directly — no database, no plugin,
//! no scrape — because the ledger is where the distinction is made; a test that
//! went through the scrape would pass on a ledger that got it wrong as long as
//! the exporter happened to drop the sample.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cog_core::loop_health::Cadence;
use cog_core::observability::Observable;
use cog_observability::capability::{
    Capability, CapabilityLedger, DeclareError, Verdict, CAPABILITY_AGE_METRIC,
    CAPABILITY_CADENCE_METRIC, CAPABILITY_DECLARED_METRIC, CAPABILITY_LABEL,
    CAPABILITY_MEASURED_METRIC, CAPABILITY_VERDICT_METRIC, VERDICT_OK, VERDICT_RED,
    VERDICT_UNMEASURED,
};

struct Fixed {
    id: &'static str,
    answer: Mutex<Verdict>,
}

impl Fixed {
    fn boxed(id: &'static str, answer: Verdict) -> Arc<dyn Capability> {
        Arc::new(Self {
            id,
            answer: Mutex::new(answer),
        })
    }
}

#[async_trait]
impl Capability for Fixed {
    fn id(&self) -> &'static str {
        self.id
    }

    fn cadence(&self) -> Cadence {
        // The ledger never waits on a cadence in these cases; the value only has
        // to be one the loop framework accepts.
        Cadence::Periodic(Duration::from_secs(60))
    }

    async fn judge(&self) -> Verdict {
        self.answer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

fn sample<'a>(
    metrics: &'a [cog_core::observability::RawMetric],
    name: &str,
    id: &str,
) -> Option<&'a cog_core::observability::RawMetric> {
    metrics
        .iter()
        .find(|m| m.name == name && m.labels.get(CAPABILITY_LABEL).map(String::as_str) == Some(id))
}

/// One id, one series. A second declaration under the same id would be judged
/// by nobody and reported under a series that says someone is looking after it.
#[tokio::test]
async fn a_second_declaration_of_one_id_is_refused() {
    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(Fixed::boxed("backup_restorable", Verdict::Ok))
        .expect("first declaration");

    let err = ledger
        .declare(Fixed::boxed("backup_restorable", Verdict::Red("no".into())))
        .expect_err("the second declaration must be refused");
    assert_eq!(err, DeclareError::DuplicateId("backup_restorable"));
    assert_eq!(ledger.len(), 1);
}

/// The id becomes a label value and a loop name. A shape no selector can be
/// written against is refused at declaration rather than discovered at query
/// time, when the series simply never matches.
#[tokio::test]
async fn an_id_no_selector_can_name_is_refused() {
    for bad in ["Backup", "1backup", "backup-drill", ""] {
        let mut ledger = CapabilityLedger::new();
        let err = ledger
            .declare(Fixed::boxed(bad, Verdict::Ok))
            .expect_err("an unselectable id must be refused");
        assert!(matches!(err, DeclareError::BadId(_)), "{bad}: {err:?}");
        assert!(ledger.is_empty());
    }

    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(Fixed::boxed("backup_drill_restorable", Verdict::Ok))
        .expect("a selectable id");
}

/// The state this module exists for: declared, loop not yet run. It must not
/// look like a pass. `measured=0` and no verdict sample at all — a sample of
/// `verdict=0` here is the false green in its purest form.
#[tokio::test]
async fn a_capability_nobody_judged_exports_measured_zero_and_no_verdict() {
    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(Fixed::boxed("llm_pool_healthy", Verdict::Ok))
        .expect("declare");

    let metrics = ledger.collect_metrics("").await.expect("collect");
    let measured = sample(&metrics, CAPABILITY_MEASURED_METRIC, "llm_pool_healthy")
        .expect("a measured sample is published even before the first judgement");
    assert_eq!(measured.value, 0.0);
    assert!(
        sample(&metrics, CAPABILITY_VERDICT_METRIC, "llm_pool_healthy").is_none(),
        "a never-judged capability must publish no verdict at all"
    );
    assert!(
        sample(&metrics, CAPABILITY_AGE_METRIC, "llm_pool_healthy").is_none(),
        "an age of zero is what a healthy fast loop looks like; never-judged must not print it"
    );
}

/// A judgement that happened is `measured=1` whatever it said.
#[tokio::test]
async fn a_judged_capability_exports_its_code_and_age() {
    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(Fixed::boxed("release_is_the_one_built", Verdict::Ok))
        .expect("declare");
    ledger.judge_all_once().await;

    let metrics = ledger.collect_metrics("").await.expect("collect");
    assert_eq!(
        sample(
            &metrics,
            CAPABILITY_MEASURED_METRIC,
            "release_is_the_one_built"
        )
        .expect("measured")
        .value,
        1.0
    );
    assert_eq!(
        sample(
            &metrics,
            CAPABILITY_VERDICT_METRIC,
            "release_is_the_one_built"
        )
        .expect("verdict")
        .value,
        VERDICT_OK as f64
    );
    assert!(
        sample(&metrics, CAPABILITY_AGE_METRIC, "release_is_the_one_built").is_some(),
        "a judged capability carries how long ago it was judged"
    );
    // The age has no meaning without the cadence it belongs to, so the two are
    // published together: a reader deciding "late" divides one by the other.
    assert_eq!(
        sample(
            &metrics,
            CAPABILITY_CADENCE_METRIC,
            "release_is_the_one_built"
        )
        .expect("cadence")
        .value,
        60.0,
        "the cadence published is the one the capability declared"
    );
}

/// "I could not look" is not a pass. It is also not the same as "never looked":
/// the loop ran, so the having-judged is still 1, and the code is 3.
#[tokio::test]
async fn an_unmeasurable_round_is_not_ok_and_still_counts_as_judged() {
    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(Fixed::boxed(
            "backup_drill_restorable",
            Verdict::Unmeasured("the database could not be read".into()),
        ))
        .expect("declare");
    ledger.judge_all_once().await;

    let metrics = ledger.collect_metrics("").await.expect("collect");
    assert_eq!(
        sample(
            &metrics,
            CAPABILITY_VERDICT_METRIC,
            "backup_drill_restorable"
        )
        .expect("verdict")
        .value,
        VERDICT_UNMEASURED as f64
    );
    assert_ne!(
        sample(
            &metrics,
            CAPABILITY_VERDICT_METRIC,
            "backup_drill_restorable"
        )
        .expect("verdict")
        .value,
        VERDICT_OK as f64
    );
    assert_eq!(
        sample(
            &metrics,
            CAPABILITY_MEASURED_METRIC,
            "backup_drill_restorable"
        )
        .expect("measured")
        .value,
        1.0
    );
}

/// The denominator. Every other series in the family exists only where a claim
/// was declared, so a ledger whose declarations were all removed would export
/// nothing — and nothing reads like a quiet, healthy face.
#[tokio::test]
async fn an_empty_ledger_still_says_it_claims_nothing() {
    let ledger = CapabilityLedger::new();
    let metrics = ledger.collect_metrics("").await.expect("collect");
    assert_eq!(metrics.len(), 1, "{metrics:?}");
    assert_eq!(metrics[0].name, CAPABILITY_DECLARED_METRIC);
    assert_eq!(metrics[0].value, 0.0);

    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(Fixed::boxed("backup_restorable", Verdict::Ok))
        .expect("declare");
    let metrics = ledger.collect_metrics("").await.expect("collect");
    let declared = metrics
        .iter()
        .find(|m| m.name == CAPABILITY_DECLARED_METRIC)
        .expect("a declared count");
    assert_eq!(declared.value, 1.0);
}

/// A red verdict carries the reason that made it red: two reds with different
/// repairs must not be indistinguishable to the operator.
#[tokio::test]
async fn a_red_verdict_keeps_its_reason() {
    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(Fixed::boxed(
            "install_verified",
            Verdict::Red("ownership reading unreadable".into()),
        ))
        .expect("declare");
    ledger.judge_all_once().await;

    let snaps = ledger.snapshot();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].id, "install_verified");
    assert_eq!(
        snaps[0].verdict.as_ref().map(Verdict::code),
        Some(VERDICT_RED)
    );
    assert_eq!(
        snaps[0].verdict.as_ref().and_then(Verdict::reason),
        Some("ownership reading unreadable")
    );
    assert_eq!(snaps[0].judged_total, 1);
}
