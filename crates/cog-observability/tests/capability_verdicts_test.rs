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

/// 一条能力环不是「判一次就结束」：判词是某一刻的读数，而能力是持续的状态。
///
/// 2026-11-03 的现场：`start_all` 起的循环判完第一轮就 return，而
/// `loop_health::spawn` 把一次 normal return 读成「这条环自己决定结束了」——它不重启，
/// 只记一次死亡。于是两个已声明的能力在同一分钟里各死一次
/// （`background_loop_died{loop=capability_spend_verdict_current}` 与
/// `..._usage_rollup_current}`），判词冻在启动那一刻，随后
/// `capability_judgement_overdue` 一直响：读的人看见的是「跑到一半不跑了」，
/// 而这一面没有任何别的读数能区分它和「一切都好」。
///
/// 判据落在**第二拍**上：一拍之内，写错的版本和写对的版本都判过一次，读数完全一样。
/// 只有等到节拍过去还不再判，才说明这条环死了。
#[tokio::test]
async fn a_capability_is_judged_again_on_its_own_cadence() {
    struct Counting {
        judged: std::sync::atomic::AtomicU64,
    }

    #[async_trait]
    impl Capability for Counting {
        fn id(&self) -> &'static str {
            "counts_its_rounds"
        }

        fn cadence(&self) -> Cadence {
            // 这个数只为测试存在：真的能力按分钟到小时判，而这条判据要的是
            // 「第二拍发生了没有」，不是任何一条能力的真实节拍。
            Cadence::Periodic(Duration::from_millis(20))
        }

        async fn judge(&self) -> Verdict {
            self.judged
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Verdict::Ok
        }
    }

    let capability = Arc::new(Counting {
        judged: std::sync::atomic::AtomicU64::new(0),
    });
    let mut ledger = CapabilityLedger::new();
    ledger
        .declare(capability.clone())
        .expect("declare the counting capability");

    let shutdown = cog_core::ShutdownSignal::new();
    ledger.start_all(shutdown.clone());
    tokio::time::sleep(Duration::from_millis(200)).await;

    let judged = capability.judged.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        judged >= 2,
        "一条能力环必须按自己的节拍一直判下去，{} 毫秒里只判了 {judged} 次：\
         判完第一拍就 return 的那版会让这一面停在启动那一刻的读数上",
        Duration::from_millis(200).as_millis()
    );
    let snaps = ledger.snapshot();
    assert_eq!(
        snaps[0].judged_total, judged,
        "账上记的次数要与真的判过的次数一致"
    );

    // 停机是所有者要的结束，不是这条环自己死掉：死亡计数两边都不许动。
    shutdown.trigger();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let metrics = cog_core::loop_health::registry()
        .collect_metrics("")
        .await
        .expect("collect loop health");
    let deaths = metrics
        .iter()
        .find(|m| {
            m.name == cog_core::loop_health::LOOP_DEATHS_TOTAL
                && m.labels
                    .get("loop")
                    .map(String::as_str)
                    .is_some_and(|v| v == "capability_counts_its_rounds")
        })
        .expect(
            "死亡计数这一格必须存在：读不到它，「没死」与「没人记」就是同一个空集，\
             而这条判据的后半句会变成空断言",
        )
        .value;
    assert_eq!(deaths, 0.0, "按停机信号结束的能力环不该被记成死亡");
}
