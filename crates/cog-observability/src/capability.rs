//! What the product promises about itself, judged on a clock and published as
//! a verdict.
//!
//! Every face of this platform makes promises that only the running system can
//! answer: the newest backup opens and restores, the LLM pool has an upstream
//! that answers, the release now deployed is the one that was built, the ledger
//! has a live writer. Those promises used to live outside the product — as a
//! hand-written line in a document plus a shell command someone remembered to
//! run. A document cannot go red, and a command nobody runs is the same as no
//! claim at all: it reads exactly like a claim that is true.
//!
//! A capability is that promise with the three things that make it a
//! measurement:
//!
//! - **it declares itself** where it is owned, via [`Capability::judge`]. There
//!   is no central list to keep in step with the code, because the list *is* the
//!   code: [`CapabilityLedger`] is filled by whoever implements the face, and a
//!   face that stops registering stops being claimed.
//! - **it runs on its own cadence** through [`crate::capability::CapabilityLedger::start_all`],
//!   which spawns one supervised loop per capability. The cadence belongs to the
//!   judgement, not to the scraper: judging at scrape time would make the
//!   measurement's period the scrape's period, so a face that got slower would
//!   look the same as a scrape that got faster.
//! - **it lands as a verdict with a denominator for "was this judged at all"**.
//!   Three codes, plus a separate `measured` series, because "never judged" and
//!   "judged and fine" must not be the same sample — a capability nobody has
//!   looked at is the exact state this whole module exists to make visible.
//!
//! The verdict codes are `0` ok, `1` red, `3` unmeasured. `3` is the same "this
//! run judged nothing" code the rest of the platform's readings use, and it is
//! deliberately not `0`: a face whose input could not be read (the database is
//! down, the file is missing) has not passed, and folding it into `ok` is how a
//! control plane reports green during an outage.
//!
//! A capability that is declared but never judged is **not** red either. It
//! exports `measured=0` and no verdict sample at all, so a reader can tell "the
//! loop has not run yet" from "the loop ran and said fine" without a fourth
//! code. The age series answers the next question — how long has this been
//! true — and is absent for the same reason.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric};
use cog_core::SFResult;

/// The verdict of one capability: 0 ok, 1 red, 3 unmeasured.
pub const CAPABILITY_VERDICT_METRIC: &str = "cogneva_capability_verdict";

/// Whether this capability has ever been judged: 1 judged, 0 never.
///
/// Separate from the verdict on purpose. A reader that only sees
/// `verdict=0` cannot tell a healthy face from one whose loop never started,
/// and "no rows" is what a broken producer looks like on every other series
/// too — so the having-judged is published rather than inferred.
pub const CAPABILITY_MEASURED_METRIC: &str = "cogneva_capability_measured";

/// Seconds since this capability was last judged.
///
/// Absent, not zero, before the first judgement: an age of zero is what a
/// healthy per-second loop looks like, and printing it for a loop that never
/// started is the same false green in a different shape.
pub const CAPABILITY_AGE_METRIC: &str = "cogneva_capability_verdict_age_seconds";

/// How often this capability is judged, as its own series.
///
/// Exported rather than written into the rule so the bound stays derived: a
/// reader comparing the age against a constant would be comparing it against
/// whatever cadence the author of the rule had in mind, and a capability that
/// slowed down would then read as a face that is late.
pub const CAPABILITY_CADENCE_METRIC: &str = "cogneva_capability_cadence_seconds";

/// How many claims this build declares.
///
/// Published always, even at zero: the rest of the family exists only where a
/// claim was declared, so without this a build that declares nothing is
/// indistinguishable from a quiet one.
pub const CAPABILITY_DECLARED_METRIC: &str = "cogneva_capability_declared";

/// The label that identifies which capability a sample belongs to.
///
/// Bounded by construction: one series per declared capability, and the set of
/// declarations is fixed at build time.
pub const CAPABILITY_LABEL: &str = "capability";

// cap: bound — 闭集的一格，不是可调的数：换一个数就是换一种语言，读的人先前的选择器全部失配
pub const VERDICT_OK: u8 = 0;
// cap: bound — 闭集的一格，不是可调的数
pub const VERDICT_RED: u8 = 1;
// cap: bound — 3 不是 2：平台里「这一轮什么都没量到」一直是 3，沿用那一格，读者不必再学一套
pub const VERDICT_UNMEASURED: u8 = 3;

/// What a capability answered this round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The claim holds this round.
    Ok,
    /// The claim does not hold, and this is what went wrong.
    ///
    /// The reason is carried rather than folded into the code because the
    /// repair differs per reason — "the last drill restored 69 tables" and
    /// "the database could not be read" are both red or unmeasured, but only
    /// one of them is a defect in the backup path.
    Red(String),
    /// The capability could not be judged this round.
    ///
    /// Not a pass: the input it needs was not readable. This is a real verdict
    /// (the loop ran), which is why it exports `measured=1`.
    Unmeasured(String),
}

impl Verdict {
    pub fn code(&self) -> u8 {
        match self {
            Verdict::Ok => VERDICT_OK,
            Verdict::Red(_) => VERDICT_RED,
            Verdict::Unmeasured(_) => VERDICT_UNMEASURED,
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Verdict::Ok => None,
            Verdict::Red(r) | Verdict::Unmeasured(r) => Some(r),
        }
    }
}

/// One promise the product makes about itself.
///
/// The id is the identity a reader selects by, so it is held to the same shape
/// as every other label value in this platform: lowercase, digits, underscore.
/// It is also the loop name (prefixed, see [`loop_name`]), which is how a
/// capability that stops being judged shows up in the loop-liveness family
/// instead of simply vanishing.
#[async_trait]
pub trait Capability: Send + Sync {
    /// Stable identity. Appears as the `capability` label.
    fn id(&self) -> &'static str;

    /// How often the judgement is worth repeating.
    ///
    /// A backup drill is a daily fact and a pool health check is a minute-scale
    /// one; running both at the faster cadence spends the expensive one's cost
    /// on no new information.
    fn cadence(&self) -> cog_core::loop_health::Cadence;

    /// Judge the claim now.
    ///
    /// Returning is a verdict; panicking is a bug in the capability and the
    /// loop's restart budget applies to it.
    async fn judge(&self) -> Verdict;
}

/// Why a declaration was refused.
///
/// Both variants are refusals rather than silent repairs: a duplicate id makes
/// one of the two capabilities invisible to every reader of the series, and a
/// malformed id produces a label value that no selector can be written against.
/// A declaration that cannot be selected by is not a claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclareError {
    DuplicateId(&'static str),
    BadId(&'static str),
}

impl std::fmt::Display for DeclareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeclareError::DuplicateId(id) => write!(
                f,
                "capability '{id}' is declared twice; the second declaration would never be read"
            ),
            DeclareError::BadId(id) => write!(
                f,
                "capability id '{id}' is not [a-z][a-z0-9_]*; it is a label value and a selector"
            ),
        }
    }
}

fn id_is_selectable(id: &str) -> bool {
    let mut chars = id.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// The loop name a capability's cadence is reported under.
///
/// Prefixed so a capability id can never collide with the name of a loop that
/// does something else: the loop-liveness family is selected by name, and two
/// meanings under one name would make one of them unreadable.
pub fn loop_name(id: &str) -> String {
    format!("capability_{id}")
}

/// What the ledger remembers about one capability.
struct EntryState {
    /// The last verdict and when it was taken.
    last: Option<(Verdict, Instant)>,
    /// How many times this capability has been judged since start.
    judged: u64,
}

struct Entry {
    capability: Arc<dyn Capability>,
    state: Mutex<EntryState>,
    judged_total: AtomicU64,
}

impl Entry {
    async fn judge_once(&self) -> Verdict {
        let verdict = self.capability.judge().await;
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.last = Some((verdict.clone(), Instant::now()));
            state.judged += 1;
        }
        self.judged_total.fetch_add(1, Ordering::Relaxed);
        verdict
    }

    fn record(&self) {
        // Kept for the read side: the count is per process, so a restart resets
        // it, and the age series is what answers "is this still being judged".
        let _ = self.judged_total.load(Ordering::Relaxed);
    }
}

/// One capability as a scraper sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilitySnapshot {
    pub id: &'static str,
    /// `None` until the first judgement.
    pub verdict: Option<Verdict>,
    /// Seconds since the judgement, when there is one.
    pub age: Option<Duration>,
    pub judged_total: u64,
}

/// Every claim the build makes about itself.
#[derive(Default)]
pub struct CapabilityLedger {
    entries: BTreeMap<&'static str, Arc<Entry>>,
}

impl CapabilityLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take a declaration.
    ///
    /// The first declaration of an id stands; a second one is refused rather
    /// than overwritten, because the caller that loses is a face whose promise
    /// would then be judged by someone else's body and reported under a series
    /// that says it is fine.
    pub fn declare(&mut self, capability: Arc<dyn Capability>) -> Result<(), DeclareError> {
        let id = capability.id();
        if !id_is_selectable(id) {
            return Err(DeclareError::BadId(id));
        }
        if self.entries.contains_key(id) {
            return Err(DeclareError::DuplicateId(id));
        }
        self.entries.insert(
            id,
            Arc::new(Entry {
                capability,
                state: Mutex::new(EntryState {
                    last: None,
                    judged: 0,
                }),
                judged_total: AtomicU64::new(0),
            }),
        );
        Ok(())
    }

    pub fn ids(&self) -> Vec<&'static str> {
        self.entries.keys().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The current state of every capability, ordered by id.
    ///
    /// Ordered rather than hash-ordered so a diff of two rallies reads as a
    /// change in the system and not as a change in iteration order.
    pub fn snapshot(&self) -> Vec<CapabilitySnapshot> {
        self.entries
            .iter()
            .map(|(id, entry)| {
                let state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
                let (verdict, age) = match &state.last {
                    Some((v, at)) => (Some(v.clone()), Some(at.elapsed())),
                    None => (None, None),
                };
                CapabilitySnapshot {
                    id,
                    verdict,
                    age,
                    judged_total: state.judged,
                }
            })
            .collect()
    }

    /// Judge every capability once, in id order.
    ///
    /// The `start_all` path uses one loop per capability so each keeps its own
    /// cadence; this exists for the caller that wants one round now — a test,
    /// or a process that is about to exit and would rather report a fresh
    /// verdict than a stale one.
    pub async fn judge_all_once(&self) {
        for entry in self.entries.values() {
            entry.judge_once().await;
        }
    }

    /// Spawn one supervised loop per declared capability.
    ///
    /// The loops are named [`loop_name`]`(id)` and registered with the
    /// platform's loop-liveness family, so a capability whose loop dies reads as
    /// a dead loop rather than as a verdict that stopped moving.
    pub fn start_all(&self, shutdown: cog_core::ShutdownSignal) {
        for (id, entry) in &self.entries {
            let entry = Arc::clone(entry);
            let cadence = entry.capability.cadence();
            // The name is bound here rather than built inside the call because
            // a loop name is a declaration, not an expression: the liveness
            // family selects by it, so it has to be one identifier a reader can
            // find. Uniqueness comes from `declare`, which refuses a second
            // capability under one id — that is the same check, one layer up,
            // where the duplicate can still be reported instead of silently
            // sharing a series.
            let name = loop_name(id);
            cog_core::loop_health::spawn(name, cadence, shutdown.clone(), move |_beat| {
                let entry = Arc::clone(&entry);
                async move {
                    let verdict = entry.judge_once().await;
                    match &verdict {
                        Verdict::Ok => {}
                        Verdict::Red(reason) => tracing::warn!(
                            capability = entry.capability.id(),
                            reason = %reason,
                            "capability does not hold"
                        ),
                        Verdict::Unmeasured(reason) => tracing::warn!(
                            capability = entry.capability.id(),
                            reason = %reason,
                            "capability could not be judged"
                        ),
                    }
                    entry.record();
                }
            });
        }
    }
}

#[async_trait]
impl Observable for CapabilityLedger {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        // How many claims this build makes, published first and always.
        //
        // Every other series in the family is published only when a capability
        // declares itself, so a build in which every declaration was removed
        // exports nothing — and nothing reads exactly like a quiet, healthy
        // face. This is the denominator that makes the emptiness a value: zero
        // means "this process claims nothing about itself", which is a defect
        // in the ledger's wiring, not a clean face.
        let mut out = vec![RawMetric::new(
            CAPABILITY_DECLARED_METRIC,
            self.entries.len() as f64,
        )];
        for snap in self.snapshot() {
            let label = CAPABILITY_LABEL;
            let mut measured = RawMetric::new(CAPABILITY_MEASURED_METRIC, 0.0)
                .with_label(label, snap.id.to_string());
            if let Some(verdict) = &snap.verdict {
                measured.value = 1.0;
                out.push(measured);
                out.push(
                    RawMetric::new(CAPABILITY_VERDICT_METRIC, verdict.code() as f64)
                        .with_label(label, snap.id.to_string()),
                );
                if let Some(age) = snap.age {
                    out.push(
                        RawMetric::new(CAPABILITY_AGE_METRIC, age.as_secs_f64())
                            .with_label(label, snap.id.to_string()),
                    );
                    // The age is only readable as "late" against the cadence it
                    // belongs to, and that number is the capability's, not the
                    // rule's. A capability with no period exports no cadence,
                    // and the age rule then has nothing to compare it with
                    // rather than comparing it with a constant that goes stale.
                    let period = self
                        .entries
                        .get(snap.id)
                        .map(|e| e.capability.cadence().period_secs())
                        .unwrap_or(0);
                    if period > 0 {
                        out.push(
                            RawMetric::new(CAPABILITY_CADENCE_METRIC, period as f64)
                                .with_label(label, snap.id.to_string()),
                        );
                    }
                }
            } else {
                out.push(measured);
            }
        }
        Ok(out)
    }

    async fn collect_trace(
        &self,
        _task_id: &str,
    ) -> SFResult<Vec<cog_core::observability::TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded(CAPABILITY_LABEL)]
    }
}
