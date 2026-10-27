//! principle: feedback_a_summary_is_prose_so_promql_written_into_it_is_read_as_placeholders
//! principle: feedback_a_config_fields_contract_lives_in_the_crate_that_parses_it
//! principle: feedback_a_brace_in_prose_is_read_as_a_placeholder_of_the_document_it_sits_in
//! principle: feedback_removing_a_rule_does_not_close_the_rows_it_left
//! Infrastructure alert watcher — the producer side of persisted infra alerts.
//!
//! SupervisorEvents only cover what the supervisor itself observes (agents,
//! quota, tasks). Infrastructure faults — node disk filling up, pods crash
//! looping, memory pressure — happen underneath the application and were
//! invisible to self-discovery: the 2026-09 node disk exhaustion only
//! surfaced as cascading pod evictions, with no alert row anything could
//! react to.
//!
//! This watcher polls a Prometheus-compatible endpoint with configured
//! PromQL rules and drives each series' condition into the persistent alert
//! state machine (`alerts` table) plus the notification outlet. Rules,
//! thresholds, and the endpoint itself are configuration: deployment policy
//! must not require a rebuild.
//!
//! Resolution semantics: a series whose condition stopped being true is
//! resolved; a series that disappeared entirely (target gone, pod deleted)
//! is resolved too — a vanished signal source must not leave a row firing
//! forever. Rows from before a process restart are adopted on the first
//! tick so restarts neither duplicate nor strand alerts. A rule taken out of
//! the configuration is the one case adoption cannot cover, because nothing
//! adopts what it no longer runs: `retired_rules` names those, and the watcher
//! closes their rows on startup.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use tracing::{info, warn};

use cog_core::{AlertEvent, AlertInstance, AlertState};

use crate::alert_store::{AlertRecord, AlertTransition, NewAlert, PostgresAlertStore};
use crate::alerts::AlertManager;
use crate::config::InfraWatchConfig;

/// Labels that never identify an alert instance — they describe the scrape,
/// not the thing being watched. Keeping them in the dedup key would fork a
/// new alert row every time the monitoring stack reschedules its own pods.
///
/// They are excluded from the *stored* labels too, not just the key: for a
/// series every deployment's endpoint serves, they name whichever pod answered
/// that tick, so a row carrying them reports a subject that did not produce the
/// reading — and those labels are what the remediation goal text is built from.
///
/// `namespace` and `container` are deliberately absent. They read as scrape
/// labels only while every rule happens to be scoped to one namespace with one
/// container per pod; the moment a rule spans namespaces, leaving them out
/// makes two different victims share one row, and the pair flaps firing and
/// resolved against each other. A pod name identifies neither once more than
/// one namespace is in play.
const NON_IDENTITY_LABELS: &[&str] = &["__name__", "job", "endpoint", "service", "prometheus"];

/// Rule name for the watcher's self-alert: a rule whose query keeps failing
/// is itself an incident (dead Prometheus, blocked network path), raised
/// through the same persistent state machine as infrastructure alerts so
/// self-discovery sees the observation gap. Kept distinct from configured
/// rule names so the series-resolution pass never touches these rows.
pub const EVAL_FAILURE_RULE: &str = "infra_watch_eval_failure";

/// This loop's name in the liveness census.
pub const INFRA_WATCH_LOOP: &str = "infra_watch";

/// Outlets the watcher drives. Both are optional: with no store the watcher
/// still notifies, with no notifier it still persists, with neither it does
/// not run at all (the plugin decides).
#[derive(Clone)]
pub struct InfraWatchOutlets {
    pub store: Option<Arc<PostgresAlertStore>>,
    pub notifier: Option<Arc<AlertManager>>,
    /// Where a batch that has no notifier is counted as undelivered. The
    /// watcher persists such a batch and returns; without this the persisted
    /// row is the only trace and it reads as though a person was told.
    pub metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
}

/// Background loop; same shutdown pattern as the alert bridge.
pub async fn run_infra_watch_loop(
    config: InfraWatchConfig,
    outlets: InfraWatchOutlets,
    http: Arc<dyn cog_core::HttpClient>,
    shutdown: cog_core::ShutdownSignal,
) {
    let interval = Duration::from_secs(
        config
            .poll_interval_secs
            .max(InfraWatchConfig::MIN_POLL_INTERVAL_SECS),
    );
    // Everything this loop reports is a judgement about other people's series, so
    // its death is the one failure it cannot report: the rules it evaluates go
    // quiet, and a rule that is quiet because nothing is wrong looks exactly like
    // a rule that is quiet because nobody is evaluating it. It is therefore run
    // under the supervisor rather than watched after the fact -- see
    // `cog_core::loop_health` on what a restart does and what it does not.
    // The join error is deliberately not reported: the ordinary way this fn's
    // task ends is being aborted at shutdown, which is an error from the handle's
    // point of view and not a fact about the loop.
    let _ = cog_core::loop_health::spawn(
        INFRA_WATCH_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // Rebuilt per attempt, so every handle the body consumes is cloned here.
        move |beat| {
            let config = config.clone();
            let outlets = outlets.clone();
            let http = Arc::clone(&http);
            let shutdown = shutdown.clone();
            async move {
                info!(
                    rules = config.rules.len(),
                    interval_secs = interval.as_secs(),
                    url = %config.prometheus_url,
                    "infra alert watcher started"
                );

                // dedup keys currently believed firing; adopted from the store on
                // the first tick so a restart resolves rows it can no longer
                // observe instead of stranding them. Eval-failure self-alerts live
                // in their own set: they are keyed by rule name, not series
                // identity, and must survive the series-resolution pass untouched.
                let mut known_firing: HashSet<String> = HashSet::new();
                let mut eval_failure_firing: HashSet<String> = HashSet::new();
                let mut failure_streaks: HashMap<String, u32> = HashMap::new();
                // dedup keys whose condition is holding but whose rule asks for a
                // `for_seconds` window first: value is the first tick it held.
                let mut holding: HashMap<String, Instant> = HashMap::new();
                let mut adopted = false;
                // What the alert mirror last saw Prometheus firing. Empty on a
                // restart is correct: the first successful read lists it all.
                let mut mirror_state = MirrorState::default();
                // 没有停用规则时这一趟本来就没事可做，标成已跑完，免得每拍都去
                // 问一次 store——空名单不是"还没跑"，是"跑完了，没有要结算的"。
                let mut retired = config.retired_rules.is_empty();

                // The rule set this process holds, read once at startup. The
                // document on disk can be replaced at any time and this loop
                // is the only place that can compare the two.
                let loaded_rule_names: Vec<String> =
                    config.rules.iter().map(|rule| rule.name.clone()).collect();
                let mut rule_set_watch = crate::config_delivery::LoadedRuleSetWatch::default();

                let mut ticker = tokio::time::interval(interval);
                loop {
                    // Stamped once per cycle, on every cycle: a rule set that found
                    // nothing to fire is the ordinary state, and it must not read as
                    // a watcher that is not running.
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => break,
                        _ = ticker.tick() => {
                            if !adopted {
                                adopted = true;
                                if let Some(store) = &outlets.store {
                                    adopt_active_alerts(store, &config, &mut known_firing, &mut eval_failure_firing).await;
                                }
                            }
                            // 在求值之前结算停用的规则：这些行不在任何一条规则的
                            // 身份里，晚一拍只是让一条已经没人求值的告警多亮一拍。
                            if !retired {
                                retired = retire_rules(&config, &outlets).await;
                            }
                            tick(&config, &outlets, &http, &mut known_firing, &mut eval_failure_firing, &mut failure_streaks, &mut holding).await;
                            // After the rules, so a read that fails is one
                            // more broken reading in the same tick's budget
                            // rather than a reason to skip them.
                            mirror_prometheus_alerts(
                                &config,
                                &outlets,
                                &http,
                                &mut mirror_state,
                                &mut eval_failure_firing,
                            )
                            .await;
                            crate::config_delivery::check_loaded_rule_set(
                                &mut rule_set_watch,
                                &loaded_rule_names,
                                &outlets.store,
                                &outlets.notifier,
                                outlets.metrics.as_ref(),
                            )
                            .await;
                        }
                    }
                }
            }
        },
    )
    .await;
}

/// Seed `known_firing` with rows this watcher's rules raised before a
/// restart. Rows raised by other producers (different rule names) are left
/// alone. Eval-failure self-alert rows are adopted separately: they stay
/// firing until the affected rule queries successfully again.
async fn adopt_active_alerts(
    store: &PostgresAlertStore,
    config: &InfraWatchConfig,
    known_firing: &mut HashSet<String>,
    eval_failure_firing: &mut HashSet<String>,
) {
    // Ask for this watcher's own rules rather than for "the active alerts", and
    // ask without a limit: with one, a busy table turns the answer into a
    // sample, and a row past the cut is not adopted -- from this process it is
    // indistinguishable from a row that does not exist, so after a restart
    // nothing will ever close it.
    let mut wanted: Vec<String> = config.rules.iter().map(|r| r.name.clone()).collect();
    wanted.push(EVAL_FAILURE_RULE.to_string());
    match store.firing_rows_for_rules(&wanted).await {
        Ok(records) => {
            for record in records {
                if record.rule == EVAL_FAILURE_RULE {
                    eval_failure_firing.insert(record.dedup_key);
                } else {
                    known_firing.insert(record.dedup_key);
                }
            }
        }
        Err(e) => warn!(error = %e, "infra watch: adopting active alerts failed"),
    }
}

/// 结算本部署已经停用的规则留下的行。
///
/// 一条从配置里被删掉的规则，它的行从此没人认领：watcher 只接手它**现在**有的
/// 规则的行，而这条规则已经不在其中——于是那行停在 `state='firing'`、`resolved_at`
/// 永远为空、`last_seen_at` 不再走动。而运营问"现在有什么在响"读的正是
/// `state='firing'` 那一面，于是它读起来跟一条**正在持续**的告警没有区别，
/// 只有把三个时钟放在一起看才分得出来。这里按名单把它关掉，走的是和普通解析
/// 同一条路（含通知），不是从表里悄悄删掉：一次停用是一次**结算**，不是抹掉记录。
///
/// 返回这趟有没有够到 store。够不到时调用侧下一拍重试——停用是持久的，第一拍
/// store 还没起来就把它当"结算过了"，那些行就再也没人关了。
async fn retire_rules(config: &InfraWatchConfig, outlets: &InfraWatchOutlets) -> bool {
    if config.retired_rules.is_empty() {
        return true;
    }
    let Some(store) = &outlets.store else {
        return true;
    };
    // Same reason as adoption: a limited read would settle only the retired
    // rules that happen to be in the sample, and the rest stay firing with no
    // one left to close them.
    match store.firing_rows_for_rules(&config.retired_rules).await {
        Ok(records) => {
            for key in retired_keys(&config.retired_rules, &records) {
                resolve(key, outlets).await;
            }
            true
        }
        Err(e) => {
            warn!(error = %e, "infra watch: 开着的行读不回来，停用规则的结算下一拍重试");
            false
        }
    }
}

/// 开着的行里，属于已停用规则的那些的 `dedup_key`。
///
/// 按规则名而不是按身份挑：停用说的是"这条规则不在了"，它留下几行就是几行——
/// 一条每台上游各一行的规则停用时会有多行，只取第一行等于只结算一部分。
fn retired_keys<'a>(retired: &[String], records: &'a [AlertRecord]) -> Vec<&'a str> {
    records
        .iter()
        .filter(|record| retired.iter().any(|name| name == &record.rule))
        .map(|record| record.dedup_key.as_str())
        .collect()
}

/// What one rule's successful evaluation does to the rows it owns.
///
/// `report` holds indexes into the series this tick returned, not dedup keys:
/// the sample is still needed to render the summary, and recomputing which
/// series a key came from would be a second reading of the same labels.
#[derive(Debug, PartialEq, Eq)]
struct RulePlan {
    report: Vec<usize>,
    close: Vec<String>,
}

/// The plan for one rule that queried successfully.
///
/// Every sample whose condition holds is reported, every tick, whether or not
/// the watcher already believes it is firing. The store keeps a clock of when
/// a condition was last *seen* true, and it can only move when it is told:
/// skipping the samples already believed firing freezes that clock at the
/// firing edge for exactly the rules that stay broken, and a frozen clock is
/// indistinguishable from a watcher that stopped looking. Whether a report is a
/// transition worth notifying, or only a sighting that moves the clock, is the
/// store's decision from the row it holds — not the watcher's from its memory.
///
/// Closing is the part the watcher's own memory does decide, and it is scoped
/// to the keys this rule raised: a key belonging to another rule is not this
/// rule's to close, and a rule that failed to query has no plan at all.
fn plan_rule(
    rule: &crate::config::InfraRule,
    series: &[SeriesSample],
    believed: &HashSet<String>,
) -> RulePlan {
    let report: Vec<usize> = series
        .iter()
        .enumerate()
        .filter(|(_, sample)| rule.condition.evaluate(sample.value))
        .map(|(i, _)| i)
        .collect();
    let reported: HashSet<String> = report
        .iter()
        .map(|&i| dedup_key(&rule.name, &series[i].labels))
        .collect();
    let own = format!("{}:", rule.name);
    let close = believed
        .iter()
        .filter(|key| key.starts_with(own.as_str()) && !reported.contains(*key))
        .cloned()
        .collect();
    RulePlan { report, close }
}

/// What one failing tick owes the two audiences of a self-alert.
struct EvalFailurePlan {
    /// Tell the store, which advances the row's `last_seen_at`.
    sight: bool,
    /// Tell a person.
    notify: bool,
}

/// The same decision `plan_rule` makes for infrastructure alerts, for the
/// watcher's self-alert -- and it has to be made separately here, because this
/// path is throttled.
///
/// The throttle exists so a rule that stays broken does not re-notify every
/// poll interval, and the latch that implements it (`eval_failure_firing`, also
/// refilled from the store on restart) is what decides whether a person is told.
/// Letting that same latch decide whether the store is told is the mistake: the
/// row then advances `last_seen_at` once, at its first tick, and never again --
/// for exactly the rules that keep failing, which is when the clock matters. A
/// row frozen at its firing edge is what a watcher that stopped querying looks
/// like, so the one field that could tell them apart says the wrong thing.
fn eval_failure_plan(threshold: u32, streak: u32, latched: bool) -> Option<EvalFailurePlan> {
    if threshold == 0 || streak < threshold {
        return None;
    }
    Some(EvalFailurePlan {
        sight: true,
        notify: !latched,
    })
}

/// One evaluation pass over every configured rule.
async fn tick(
    config: &InfraWatchConfig,
    outlets: &InfraWatchOutlets,
    http: &Arc<dyn cog_core::HttpClient>,
    known_firing: &mut HashSet<String>,
    eval_failure_firing: &mut HashSet<String>,
    failure_streaks: &mut HashMap<String, u32>,
    holding: &mut HashMap<String, Instant>,
) {
    for rule in &config.rules {
        let series = match query_prometheus(http, &config.prometheus_url, &rule.promql).await {
            Ok(series) => series,
            Err(e) => {
                // A failed query must NOT resolve anything: absence of
                // evidence is not evidence of health. No plan is built for
                // this rule, so its previously-firing rows survive this tick
                // untouched.
                //
                // It clears the rule's hold clocks for the same reason: a tick
                // that could not read the condition is not a tick where it ceased
                // to hold, but it is also not evidence that it went on holding.
                // Leaving the clock running would let a rule fire on the first
                // successful tick after an outage, crediting the gap as time the
                // condition was observed.
                forget_held(rule, holding);
                warn!(rule = %rule.name, error = %e, "infra watch: rule query failed");
                let streak = failure_streaks.entry(rule.name.clone()).or_insert(0);
                *streak = streak.saturating_add(1);
                let key = format!("{EVAL_FAILURE_RULE}:{}", rule.name);
                let latched = eval_failure_firing.contains(&key);
                if let Some(plan) =
                    eval_failure_plan(config.eval_failure_alert_after, *streak, latched)
                {
                    if plan.sight {
                        sight_eval_failure(
                            &rule.name,
                            Some(rule.face.as_str()),
                            *streak,
                            &e,
                            &key,
                            outlets,
                        )
                        .await;
                    }
                    if plan.notify {
                        eval_failure_firing.insert(key.clone());
                        notify_eval_failure(
                            &rule.name,
                            Some(rule.face.as_str()),
                            *streak,
                            &e,
                            outlets,
                        )
                        .await;
                    }
                }
                continue;
            }
        };
        // The rule queried successfully: any eval-failure self-alert for it
        // resolves (including rows adopted after a restart, for which no
        // in-process streak exists), and the streak resets.
        failure_streaks.remove(&rule.name);
        let eval_key = format!("{EVAL_FAILURE_RULE}:{}", rule.name);
        if eval_failure_firing.contains(&eval_key) && resolve(&eval_key, outlets).await {
            eval_failure_firing.remove(&eval_key);
        }

        let plan = plan_rule(rule, &series, known_firing);
        let now = Instant::now();
        for &i in &plan.report {
            let key = dedup_key(&rule.name, &series[i].labels);
            if !known_firing.contains(&key) && !hold_elapsed(rule, &key, now, holding) {
                // The condition holds, but the rule asks for it to keep
                // holding. Nothing is written this tick: a row is a claim about
                // what happened, and one sample of a reading that disappears on
                // its own producer's restart does not support that claim.
                continue;
            }
            known_firing.insert(key.clone());
            // The row now carries the condition; the clock that got it there has
            // no further say and would otherwise sit in the map for as long as
            // the alert is open.
            holding.remove(&key);
            report_sighting(rule, &series[i], &key, outlets).await;
        }
        // A condition that stopped holding restarts its clock: the window is
        // about one uninterrupted stretch, not about a total across ticks.
        forget_held_except(rule, &series, holding);
        for key in &plan.close {
            // Keep believing it until the row is actually closed: the close
            // retry lives in this set, and dropping the key here is what turns
            // one failed write into a row that stays `firing` for good.
            let closed = resolve(key, outlets).await;
            settle_close(known_firing, key, closed);
        }
    }
}

/// The pseudo rule name a failed mirror read is booked under.
///
/// A read of `/api/v1/alerts` that fails must not resolve anything — absence of
/// evidence is not evidence of health, exactly as for a rule's own query — and
/// it must not vanish either: without a row, a mirror that has been broken for
/// a day looks the same as one with nothing to copy. Booked through the same
/// self-alert path as a broken rule so the two read alike.
pub const MIRROR_SOURCE: &str = "prometheus_alerts";

/// The face that owns a failure of the mirror itself.
///
/// It is spelled here rather than left empty because the mirror is the alerting
/// pipeline: when it cannot read Prometheus, the reading that would have said
/// "this rule is firing" stops existing, and the face that keeps the alerting
/// pipeline working is the one that has to act. An empty face would file that
/// under nobody -- which is the exact failure this whole ownership field exists
/// to end.
const MIRROR_SOURCE_FACE: &str = "可观测与告警";

/// What the mirror remembers between ticks.
#[derive(Default)]
struct MirrorState {
    /// Consecutive failed reads, for the self-alert's threshold. The keys the
    /// mirror believes firing are deliberately *not* remembered here: the
    /// store holds them, and a memory that resets with the process is what
    /// stranded the rows opened before the restart.
    failure_streak: u32,
}

/// Mirror-owned rows Prometheus is no longer firing.
///
/// `open` comes from the store, `seen` from this pass's read. The subtraction
/// is the whole rule, and it is a rule about rows rather than about memory:
/// a row opened by a previous process, a row whose close write failed, and a
/// row whose identity moved because the rule's expression gained or lost a
/// label are each unobserved without being closed, and only the store still
/// lists all three.
fn mirror_orphans(open: &[String], seen: &HashSet<String>) -> Vec<String> {
    open.iter()
        .filter(|key| !seen.contains(*key))
        .cloned()
        .collect()
}

/// One alert Prometheus evaluated and is currently firing, reduced to what a
/// row needs.
#[derive(Debug, Clone, PartialEq)]
struct MirroredAlert {
    rule: String,
    severity: cog_core::AlertSeverity,
    /// Identity labels only. The rule's name, its severity and Prometheus's own
    /// alert-state label describe the alert rather than the thing that is wrong,
    /// and a key carrying them would fork a row when either changes.
    labels: BTreeMap<String, String>,
    /// The face the rule declared, kept out of [`Self::labels`] and out of the
    /// key for the same reason severity is: it says who is called, not what
    /// happened. Held separately so the row can still carry it -- a reader that
    /// has to answer "who acts on this" reads it from the stored row.
    face: Option<String>,
    message: String,
}

impl MirroredAlert {
    fn key(&self) -> String {
        dedup_key(&self.rule, &self.labels)
    }
}

/// Labels that are part of Prometheus's alert envelope, not of the subject.
///
/// `cogneva_io_face` belongs here rather than with the identity labels: it is
/// the rule's own declaration of who acts, so handing the same rule to another
/// face would otherwise fork a second row for an incident that never changed.
const MIRROR_ENVELOPE_LABELS: &[&str] = &["alertname", "alertstate", "severity", ALERT_FACE_LABEL];

/// Read one tick's worth of Prometheus-evaluated alerting state.
///
/// The pure part is split out because the decisions it makes — which alerts
/// belong in this table, and what identifies one — are the whole feature, and a
/// fixture can pin them without a server.
fn parse_alerts_response(
    body: &serde_json::Value,
    mirror: &crate::config::AlertMirrorConfig,
    own_rules: &HashSet<&str>,
) -> Result<Vec<MirroredAlert>, String> {
    if body.get("status").and_then(|s| s.as_str()) != Some("success") {
        let detail = body
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("no error message");
        return Err(format!("prometheus alerts query failed: {detail}"));
    }
    let alerts = body
        .get("data")
        .and_then(|d| d.get("alerts"))
        .and_then(|a| a.as_array())
        .ok_or_else(|| "prometheus alerts response has no data.alerts array".to_string())?;

    let mut out = Vec::new();
    for alert in alerts {
        if alert.get("state").and_then(|s| s.as_str()) != Some("firing") {
            continue;
        }
        let Some(raw) = alert.get("labels").and_then(|l| l.as_object()) else {
            continue;
        };
        let labels: BTreeMap<String, String> = raw
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
            .collect();
        let marked = labels.get(&mirror.label).map(String::as_str) == Some(mirror.value.as_str());
        if !marked && !mirror.include_unmarked {
            continue;
        }
        let Some(rule) = labels.get(crate::LABEL_ALERTNAME).cloned() else {
            continue;
        };
        // Exclusions beat both selections: a name listed here is never an
        // incident, marked or not. It also keeps the reader honest -- the gap
        // reading subtracts this same list from what it expects in the table.
        if mirror.exclude.iter().any(|name| name == &rule) {
            continue;
        }
        // Already booked by the in-process evaluation of the same rule: two
        // writers keyed differently would open two rows for one incident.
        if own_rules.contains(rule.as_str()) || rule == EVAL_FAILURE_RULE || rule == MIRROR_SOURCE {
            continue;
        }
        let severity = match labels.get(crate::LABEL_SEVERITY).map(String::as_str) {
            Some("critical") => cog_core::AlertSeverity::Critical,
            Some("info") => cog_core::AlertSeverity::Info,
            _ => cog_core::AlertSeverity::Warning,
        };
        let summary = alert
            .get("annotations")
            .and_then(|a| a.get("summary"))
            .and_then(|s| s.as_str())
            .or_else(|| {
                alert
                    .get("annotations")
                    .and_then(|a| a.get("description"))
                    .and_then(|s| s.as_str())
            })
            .map(str::to_string)
            .unwrap_or_else(|| rule.clone());
        let face = labels
            .get(ALERT_FACE_LABEL)
            .filter(|f| !f.is_empty())
            .cloned();
        let labels = labels
            .into_iter()
            .filter(|(k, _)| {
                !MIRROR_ENVELOPE_LABELS.contains(&k.as_str())
                    && !NON_IDENTITY_LABELS.contains(&k.as_str())
                    // The marking label says where the alert is delivered, not
                    // what it is about; it is on every mirrored alert by
                    // construction, so it identifies nothing.
                    && k != &mirror.label
            })
            .collect();
        out.push(MirroredAlert {
            rule,
            severity,
            labels,
            face,
            message: summary,
        });
    }
    Ok(out)
}

async fn query_prometheus_alerts(
    http: &Arc<dyn cog_core::HttpClient>,
    base_url: &str,
) -> Result<serde_json::Value, String> {
    let url = format!("{}/api/v1/alerts", base_url.trim_end_matches('/'));
    let mut req = cog_core::HttpRequest::get(url);
    req.timeout_secs = Some(10);
    let resp = http
        .execute(req)
        .await
        .map_err(|e| format!("request failed: {e}"))?;
    if !resp.is_success() {
        return Err(match describe_api_error(&resp.body) {
            Some(detail) => format!("prometheus returned {}: {detail}", resp.status),
            None => format!("prometheus returned {}", resp.status),
        });
    }
    serde_json::from_slice(&resp.body).map_err(|e| format!("invalid JSON: {e}"))
}

/// Copy the alerts Prometheus is firing under the mirror label into the alert
/// table, and resolve the ones it stopped firing.
///
/// Resolution is driven by Prometheus's own `for:` windows and conditions: the
/// mirror never re-evaluates them, because two evaluations of one rule is the
/// pair that drifts.
async fn mirror_prometheus_alerts(
    config: &InfraWatchConfig,
    outlets: &InfraWatchOutlets,
    http: &Arc<dyn cog_core::HttpClient>,
    state: &mut MirrorState,
    eval_failure_firing: &mut HashSet<String>,
) {
    let Some(mirror) = &config.mirror_alerts else {
        return;
    };
    let own_rules: HashSet<&str> = config.rules.iter().map(|r| r.name.as_str()).collect();
    let body = match query_prometheus_alerts(http, &config.prometheus_url).await {
        Ok(body) => body,
        Err(e) => {
            // Nothing is resolved here, for the same reason a rule's failed
            // query resolves nothing: a read that did not happen is not a read
            // that came back healthy.
            state.failure_streak = state.failure_streak.saturating_add(1);
            warn!(error = %e, "infra watch: alert mirror read failed");
            let key = format!("{EVAL_FAILURE_RULE}:{MIRROR_SOURCE}");
            let latched = eval_failure_firing.contains(&key);
            if let Some(plan) = eval_failure_plan(
                config.eval_failure_alert_after,
                state.failure_streak,
                latched,
            ) {
                if plan.sight {
                    sight_eval_failure(
                        MIRROR_SOURCE,
                        Some(MIRROR_SOURCE_FACE),
                        state.failure_streak,
                        &e,
                        &key,
                        outlets,
                    )
                    .await;
                }
                if plan.notify {
                    eval_failure_firing.insert(key.clone());
                    notify_eval_failure(
                        MIRROR_SOURCE,
                        Some(MIRROR_SOURCE_FACE),
                        state.failure_streak,
                        &e,
                        outlets,
                    )
                    .await;
                }
            }
            return;
        }
    };
    let alerts = match parse_alerts_response(&body, mirror, &own_rules) {
        Ok(alerts) => alerts,
        Err(e) => {
            state.failure_streak = state.failure_streak.saturating_add(1);
            warn!(error = %e, "infra watch: alert mirror response unreadable");
            return;
        }
    };
    state.failure_streak = 0;
    let eval_key = format!("{EVAL_FAILURE_RULE}:{MIRROR_SOURCE}");
    if eval_failure_firing.contains(&eval_key) && resolve(&eval_key, outlets).await {
        eval_failure_firing.remove(&eval_key);
    }

    let mut seen: HashSet<String> = HashSet::new();
    for alert in &alerts {
        let key = alert.key();
        seen.insert(key.clone());
        mirror_report(alert, &key, outlets).await;
    }
    // What to close is asked of the store, not of this process's memory: the
    // row outlives the writer that opened it. An in-memory set is empty after
    // a restart, and it is also blind to a failed close write and to a rule
    // whose identity labels moved -- in all three the store holds an open row
    // that memory has no entry for, so a memory-driven diff closes nothing and
    // the row keeps counting as an alert that is still happening.
    let open = match &outlets.store {
        Some(store) => match store.firing_keys_from_source(MIRROR_SOURCE).await {
            Ok(keys) => keys,
            Err(e) => {
                // A store that did not answer is not a store that said
                // "nothing is open": resolving on this tick would turn one
                // read error into every mirrored alert reporting recovery.
                warn!(error = %e, "infra watch: alert mirror could not read its open rows");
                return;
            }
        },
        None => Vec::new(),
    };
    for key in mirror_orphans(&open, &seen) {
        resolve_from(&key, outlets, MIRROR_SOURCE).await;
    }
}

/// Report one mirrored sighting: the row advances its liveness clock every
/// tick, and a transition — not a repeat — is what notifies.
async fn mirror_report(alert: &MirroredAlert, key: &str, outlets: &InfraWatchOutlets) {
    let Some(store) = &outlets.store else {
        mirror_notify(alert, &HashMap::new(), outlets).await;
        return;
    };
    let mut labels: HashMap<String, String> = alert
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    // The key leaves the face out; the row must not. A reader answering "who
    // acts on this" only has the stored row to read.
    if let Some(face) = &alert.face {
        labels.insert(ALERT_FACE_LABEL.into(), face.clone());
    }
    labels.insert(crate::LABEL_MESSAGE.into(), alert.message.clone());
    labels.insert(crate::alerts::LABEL_SOURCE.into(), MIRROR_SOURCE.into());
    let row = NewAlert {
        rule: alert.rule.clone(),
        dedup_key: key.to_string(),
        severity: alert.severity.as_str().to_string(),
        message: alert.message.clone(),
        labels: serde_json::to_value(&labels).unwrap_or_else(|_| serde_json::json!({})),
    };
    match store.set_alert(true, &row).await {
        Ok(AlertTransition::Fired) => {
            warn!(rule = %alert.rule, key = %key, message = %alert.message, "mirrored alert firing");
            mirror_notify(alert, &labels, outlets).await;
        }
        Ok(_) => {}
        Err(e) => warn!(rule = %alert.rule, error = %e, "mirrored alert persist failed"),
    }
}

async fn mirror_notify(
    alert: &MirroredAlert,
    labels: &HashMap<String, String>,
    outlets: &InfraWatchOutlets,
) {
    let now = Utc::now();
    let inst = AlertInstance {
        rule_name: alert.rule.clone(),
        labels: labels.clone(),
        state: AlertState::Firing,
        severity: alert.severity,
        value: 1.0,
        starts_at: now,
        ends_at: None,
        updated_at: now,
    };
    deliver(
        outlets,
        vec![AlertEvent::Firing(inst)],
        outlets.store.is_some(),
    )
    .await;
}

/// Whether a rule's hold window has elapsed for a series that is not firing yet.
///
/// Returns true immediately for a rule without `for_seconds`, so the ordinary
/// rule path is unchanged. The first sighting starts the clock and answers
/// false; later ticks answer false until the window is over, then true -- and
/// the caller drops the entry by inserting the key into `known_firing`.
fn hold_elapsed(
    rule: &crate::config::InfraRule,
    key: &str,
    now: Instant,
    holding: &mut HashMap<String, Instant>,
) -> bool {
    let Some(window) = rule.for_seconds.filter(|secs| *secs > 0) else {
        return true;
    };
    let first = *holding.entry(key.to_string()).or_insert(now);
    now.saturating_duration_since(first) >= Duration::from_secs(window)
}

/// Drop one rule's hold clocks, all of them.
fn forget_held(rule: &crate::config::InfraRule, holding: &mut HashMap<String, Instant>) {
    let prefix = format!("{}:", rule.name);
    holding.retain(|key, _| !key.starts_with(prefix.as_str()));
}

/// Drop this rule's hold clocks for every series that is not still satisfying
/// its condition.
fn forget_held_except(
    rule: &crate::config::InfraRule,
    series: &[SeriesSample],
    holding: &mut HashMap<String, Instant>,
) {
    if holding.is_empty() {
        return;
    }
    let prefix = format!("{}:", rule.name);
    let mut still_holding: HashSet<String> = HashSet::new();
    for sample in series {
        if rule.condition.evaluate(sample.value) {
            still_holding.insert(dedup_key(&rule.name, &sample.labels));
        }
    }
    holding.retain(|key, _| !key.starts_with(prefix.as_str()) || still_holding.contains(key));
}

/// One evaluated series sample from a Prometheus vector result.
#[derive(Debug, Clone)]
pub struct SeriesSample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Query the instant HTTP API and parse the vector result. Pure parsing is
/// split out for tests.
pub async fn query_prometheus(
    http: &Arc<dyn cog_core::HttpClient>,
    base_url: &str,
    promql: &str,
) -> Result<Vec<SeriesSample>, String> {
    let url = format!(
        "{}/api/v1/query?query={}",
        base_url.trim_end_matches('/'),
        urlencoding(promql)
    );
    let mut req = cog_core::HttpRequest::get(url);
    req.timeout_secs = Some(10);
    let resp = http
        .execute(req)
        .await
        .map_err(|e| format!("request failed: {e}"))?;
    if !resp.is_success() {
        return Err(match describe_api_error(&resp.body) {
            Some(detail) => format!("prometheus returned {}: {detail}", resp.status),
            None => format!("prometheus returned {}", resp.status),
        });
    }
    let body: serde_json::Value =
        serde_json::from_slice(&resp.body).map_err(|e| format!("invalid JSON: {e}"))?;
    parse_vector_response(&body)
}

/// The reason Prometheus rejected a query, read from the response body.
///
/// The status code alone cannot carry the decision: Prometheus answers 422 for
/// every execution failure, and those are not interchangeable — a duplicate
/// series in the match group clears up on its own, while a rule that can never
/// evaluate is broken configuration. `errorType` plus `error` says which. None
/// when the body is not the API's error shape (a proxy in the path, a partial
/// read), so the caller still reports the bare status instead of nothing.
fn describe_api_error(body: &[u8]) -> Option<String> {
    /// Prometheus messages embed the whole match group; enough to recognise
    /// the cause, not enough to flood the alert row.
    const MAX_CHARS: usize = 300;
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    let kind = v.get("errorType").and_then(|e| e.as_str())?;
    let msg = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
    let full = format!("{kind}: {msg}");
    if full.chars().count() <= MAX_CHARS {
        return Some(full);
    }
    Some(
        full.chars()
            .take(MAX_CHARS)
            .chain(std::iter::once('…'))
            .collect(),
    )
}

/// Fill a rule summary from the sample that fired it.
///
/// `{value}` becomes the sample's value; every other `{name}` is taken from the
/// sample's labels, because a summary like "pending state for {stream} ..." is
/// telling the reader which series fired and the series carries that in a
/// label, not in a separate field. A placeholder that names neither the value
/// nor a label of this sample is left exactly as written: a summary that reads
/// `{asset}` is visibly wrong, while dropping the name would silently produce a
/// sentence about nobody — and the reader cannot tell that from a rule that
/// legitimately has no third party to name.
pub fn render_summary(template: &str, value: f64, labels: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            out.push_str(&rest[open..]);
            return out;
        };
        let name = &after[..close];
        match name {
            "value" => out.push_str(&format!("{value:.2}")),
            other => match labels.get(other) {
                Some(v) => out.push_str(v),
                None => out.push_str(&rest[open..open + close + 2]),
            },
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out
}

/// Parse a Prometheus instant-vector response body into samples. Anything
/// that is not a successful vector result is an error — silently treating it
/// as "no series" would resolve firing alerts on a Prometheus hiccup.
pub fn parse_vector_response(body: &serde_json::Value) -> Result<Vec<SeriesSample>, String> {
    if body.get("status").and_then(|s| s.as_str()) != Some("success") {
        return Err(format!(
            "non-success status: {}",
            body.get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown")
        ));
    }
    let result = body
        .pointer("/data/result")
        .and_then(|r| r.as_array())
        .ok_or_else(|| "missing data.result".to_string())?;
    let mut out = Vec::with_capacity(result.len());
    for entry in result {
        let labels: BTreeMap<String, String> = entry
            .get("metric")
            .and_then(|m| m.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let value = entry
            .get("value")
            .and_then(|v| v.as_array())
            .and_then(|pair| pair.get(1))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| "series without a numeric value".to_string())?;
        out.push(SeriesSample { labels, value });
    }
    Ok(out)
}

/// Minimal percent-encoding for query strings (PromQL only needs a small
/// reserved set escaped; the HTTP client takes the URL verbatim).
fn urlencoding(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The labels that identify the instance: everything but the scrape's own.
/// Used both for the dedup key and for what a row stores, so the two can never
/// disagree about which labels are the subject.
fn identity_labels(labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    labels
        .iter()
        .filter(|(k, _)| !NON_IDENTITY_LABELS.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Stable alert-instance identity: rule name + sorted identity labels.
fn dedup_key(rule_name: &str, labels: &BTreeMap<String, String>) -> String {
    let identity = identity_labels(labels)
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{rule_name}:{identity}")
}

/// Labels carried by a stored/notified alert: the instance's identity labels
/// plus the rendered message and the source. Built from [`identity_labels`] so
/// the row's subject is the same one the key was computed for — a scrape label
/// here would name a pod that merely served the metric.
///
/// The face is added here, after the key was computed, and never enters it: the
/// face says who is called, not what happened. Reassigning a face must not fork
/// a row, and a rule that names no face must not make its rows indistinguishable
/// from owned ones, so an empty face writes no label at all rather than an empty
/// one -- readers count rows whose label is absent or blank as unowned, and an
/// empty string would make the absence look like a value.
fn alert_labels(sample: &SeriesSample, message: &str, face: &str) -> HashMap<String, String> {
    let mut labels: HashMap<String, String> = identity_labels(&sample.labels).into_iter().collect();
    if !face.is_empty() {
        labels.insert(ALERT_FACE_LABEL.into(), face.to_string());
    }
    labels.insert(crate::LABEL_MESSAGE.into(), message.to_string());
    labels.insert(crate::alerts::LABEL_SOURCE.into(), "infra_watch".into());
    labels
}

/// The label a stored alert row uses to name the face that acts on it.
///
/// The same key the PrometheusRule manifests put on their rules, because every
/// writer fills one table and the reader there must not have to know which
/// writer wrote the row. Re-exported from the alerts contract, where it is
/// spelled once for writers that do not share a crate.
pub use cog_core::alerts::ALERT_FACE_LABEL;

/// Report one sighting to the store and notify on the edge.
///
/// Called for every sample whose condition holds, on every tick, not only when
/// the condition first holds: the store needs each sighting to move its
/// liveness clock, and it is the store that decides from the row it holds
/// whether this is a transition (notify) or a repeat (do not).
async fn report_sighting(
    rule: &crate::config::InfraRule,
    sample: &SeriesSample,
    key: &str,
    outlets: &InfraWatchOutlets,
) {
    let message = render_summary(&rule.summary, sample.value, &sample.labels);
    let labels = alert_labels(sample, &message, &rule.face);

    if let Some(store) = &outlets.store {
        let alert = NewAlert {
            rule: rule.name.clone(),
            dedup_key: key.to_string(),
            severity: rule.severity.as_str().to_string(),
            message: message.clone(),
            labels: serde_json::to_value(&labels).unwrap_or_else(|_| serde_json::json!({})),
        };
        match store.set_alert(true, &alert).await {
            Ok(AlertTransition::Fired) => {
                warn!(rule = %rule.name, key = %key, %message, "infra alert firing");
                notify(rule, sample, &labels, outlets).await;
            }
            Ok(_) => {}
            Err(e) => warn!(rule = %rule.name, error = %e, "infra alert persist failed"),
        }
    } else {
        notify(rule, sample, &labels, outlets).await;
    }
}

/// The text both audiences of a self-alert get.
fn eval_failure_text(rule_name: &str, streak: u32, error: &str) -> String {
    format!("infra watch rule \"{rule_name}\" query failed {streak} times in a row: {error}")
}

/// The labels an eval-failure row and its notification are both built from.
///
/// One helper because the two must be the same row: the sighting writes the
/// alert instance a reader selects by `watched_rule`, and the notification is
/// what a person receives about that same instance. Built separately, a rename
/// on one side leaves the notification describing a rule no stored row carries
/// -- and a selector matching nothing reads the same as "this rule never
/// failed".
fn eval_failure_labels(
    rule_name: &str,
    message: &str,
    face: Option<&str>,
) -> HashMap<String, String> {
    let mut labels = HashMap::from([
        ("watched_rule".to_string(), rule_name.to_string()),
        (crate::LABEL_MESSAGE.to_string(), message.to_string()),
        (
            crate::alerts::LABEL_SOURCE.to_string(),
            "infra_watch".to_string(),
        ),
    ]);
    // The rule that could not be evaluated is watched by the same face that
    // would act on it firing: to that face the two are one thing -- a reading it
    // owns is not being produced -- and a self-alert nobody owns is the very
    // failure this row reports.
    if let Some(face) = face.filter(|f| !f.is_empty()) {
        labels.insert(ALERT_FACE_LABEL.to_string(), face.to_string());
    }
    labels
}

/// Raise the watcher's self-alert for a rule whose query keeps failing. Goes
/// through the same persistent state machine as infrastructure alerts so the
/// observation gap becomes a signal self-discovery can consume.
///
/// Told on every failing tick, not once per streak: `set_alert` is what advances
/// the row's `last_seen_at`, and that field is the only one that separates a
/// rule still being watched from a watcher that died. The throttle belongs to
/// the notification, not to the sighting -- see `notify_eval_failure`.
async fn sight_eval_failure(
    rule_name: &str,
    face: Option<&str>,
    streak: u32,
    error: &str,
    key: &str,
    outlets: &InfraWatchOutlets,
) {
    let Some(store) = &outlets.store else { return };
    let message = eval_failure_text(rule_name, streak, error);
    let labels = eval_failure_labels(rule_name, &message, face);
    let alert = NewAlert {
        rule: EVAL_FAILURE_RULE.to_string(),
        dedup_key: key.to_string(),
        severity: cog_core::AlertSeverity::Warning.as_str().to_string(),
        message,
        labels: serde_json::to_value(&labels).unwrap_or_else(|_| serde_json::json!({})),
    };
    match store.set_alert(true, &alert).await {
        Ok(AlertTransition::Fired) => {
            warn!(rule = %rule_name, streak, "infra watch eval-failure alert firing");
        }
        Ok(_) => {}
        Err(e) => warn!(rule = %rule_name, error = %e, "eval-failure alert persist failed"),
    }
}

/// Tell a person, once per streak: a rule that stays broken for a day is one
/// incident, and re-notifying it every poll interval is the noise the streak
/// threshold exists to prevent.
async fn notify_eval_failure(
    rule_name: &str,
    face: Option<&str>,
    streak: u32,
    error: &str,
    outlets: &InfraWatchOutlets,
) {
    let message = eval_failure_text(rule_name, streak, error);
    let labels = eval_failure_labels(rule_name, &message, face);
    let now = Utc::now();
    let inst = AlertInstance {
        rule_name: EVAL_FAILURE_RULE.to_string(),
        labels,
        state: AlertState::Firing,
        severity: cog_core::AlertSeverity::Warning,
        value: streak as f64,
        starts_at: now,
        ends_at: None,
        updated_at: now,
    };
    deliver(
        outlets,
        vec![AlertEvent::Firing(inst)],
        outlets.store.is_some(),
    )
    .await;
}

/// Hand a batch to the notifier, or record where it actually went.
///
/// Three states used to share one reading: an alert an external outlet took, an
/// alert that reached only the `alerts` table (which the WebUI reads, so a
/// person *can* see it), and an alert that reached nobody. `persisted` is the
/// caller's answer for the middle state -- every call site writes the row before
/// it calls here.
async fn deliver(outlets: &InfraWatchOutlets, events: Vec<AlertEvent>, persisted: bool) {
    match &outlets.notifier {
        Some(notifier) => notifier.notify(&events).await,
        None if persisted => {
            crate::alerts::record_delivered_in_app(outlets.metrics.as_ref(), events.len()).await
        }
        None => crate::alerts::record_undelivered(outlets.metrics.as_ref(), events.len()).await,
    }
}

/// Close one alert row and notify the resolution.
///
/// Returns whether the row is closed now. A caller that forgets the key without
/// asking shuts the door behind itself: the key leaves the set that would retry
/// the close, so a write that failed once leaves the row `firing` for good.
/// Measured 2026-10-23: two rows were closed at 22:59:48 while the database was
/// answering in two-second statements; both writes failed, both keys were
/// dropped anyway, and both rows are still `firing` -- one of them naming a Pod
/// that no longer exists, and both counting against every reader of the table.
async fn resolve(key: &str, outlets: &InfraWatchOutlets) -> bool {
    resolve_from(key, outlets, "infra_watch").await
}

/// Forget a key only when its row is known to be closed.
///
/// Separated out because "keep believing it" is the safe direction and
/// "forget it" is the one that costs: the next tick retries whatever is still
/// believed, and nothing retries what was forgotten.
fn settle_close(believed: &mut HashSet<String>, key: &str, closed: bool) {
    if closed {
        believed.remove(key);
    }
}

/// The same, saying which producer the row belonged to: a mirror row resolved
/// under the in-process label would name the wrong writer in the very
/// notification that says the incident is over.
async fn resolve_from(key: &str, outlets: &InfraWatchOutlets, source: &str) -> bool {
    if let Some(store) = &outlets.store {
        let alert = NewAlert {
            rule: key.split(':').next().unwrap_or(key).to_string(),
            dedup_key: key.to_string(),
            severity: "info".into(),
            message: String::new(),
            labels: serde_json::json!({}),
        };
        match store.set_alert(false, &alert).await {
            Ok(AlertTransition::Resolved) => {
                info!(key = %key, "infra alert resolved");
                let labels =
                    HashMap::from([(crate::alerts::LABEL_SOURCE.to_string(), source.to_string())]);
                let inst = AlertInstance {
                    rule_name: alert.rule.clone(),
                    labels,
                    state: AlertState::Resolved,
                    severity: cog_core::AlertSeverity::Info,
                    value: 0.0,
                    starts_at: Utc::now(),
                    ends_at: Some(Utc::now()),
                    updated_at: Utc::now(),
                };
                deliver(outlets, vec![AlertEvent::Resolved(inst)], true).await;
                true
            }
            // Nothing was firing under this key: there is nothing left to close,
            // so the key may be forgotten.
            Ok(_) => true,
            Err(e) => {
                warn!(key = %key, error = %e, "infra alert resolve failed");
                false
            }
        }
    } else {
        // No store, no row: nothing is owed and nothing can be left behind.
        true
    }
}

async fn notify(
    rule: &crate::config::InfraRule,
    sample: &SeriesSample,
    labels: &HashMap<String, String>,
    outlets: &InfraWatchOutlets,
) {
    let now = Utc::now();
    let inst = AlertInstance {
        rule_name: rule.name.clone(),
        labels: labels.clone(),
        state: AlertState::Firing,
        severity: rule.severity,
        value: sample.value,
        starts_at: now,
        ends_at: None,
        updated_at: now,
    };
    deliver(
        outlets,
        vec![AlertEvent::Firing(inst)],
        outlets.store.is_some(),
    )
    .await;
}

/// What a comparison's side carries into the join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// One value with no labels: pairs with anything, sample by sample.
    Scalar,
    /// A single series with an empty label set: pairs with nothing labelled.
    LabelLess,
    /// Series that carry labels, from the scrape or from a grouping clause.
    Labelled,
    /// Not decidable from the text.
    Unknown,
}

/// Report a comparison whose two sides can never meet.
///
/// A PromQL comparison keeps a sample only where both sides carry the *same*
/// label set. A side that arrives without labels — a bare aggregate with no
/// grouping clause, or arithmetic built from one — has exactly one, empty label
/// set, so against a labelled series it pairs on nothing and the comparison
/// yields no series at all. The rule still loads, is still evaluated on every
/// tick, and can never fire: the failure is silence, and silence is exactly what
/// a healthy rule looks like from the outside.
///
/// `scalar()` is the fix rather than a reformatting: it turns the side into a
/// scalar, and a comparison against a scalar compares each sample of the series.
/// Returns `None` when the shape cannot be judged from the text alone — a
/// modifier such as `on()` or `ignoring()` rewrites the join, and an expression
/// this scanner does not recognise is not evidence of a defect. The reading is
/// therefore a lower bound: no hit means "no defect of this shape", not "all
/// rules verified".
pub fn unpaired_comparison(promql: &str) -> Option<String> {
    let (at, len) = top_level_comparison(promql)?;
    let lhs = promql[..at].trim();
    let mut rhs = promql[at + len..].trim();
    // Modifiers sit between the operator and the right operand. `bool` only
    // changes the result value; the others rewrite which labels must agree, and
    // then the text alone no longer says whether the sides can meet.
    while let Some((word, rest)) = leading_ident(rhs) {
        match word {
            "bool" => rhs = rest.trim_start(),
            "on" | "ignoring" | "group_left" | "group_right" => return None,
            _ => break,
        }
    }
    let (left, right) = (side_of(lhs), side_of(rhs));
    let mismatched = matches!(
        (left, right),
        (Side::LabelLess, Side::Labelled) | (Side::Labelled, Side::LabelLess)
    );
    if !mismatched {
        return None;
    }
    Some(format!(
        "`{lhs}` {op} `{rhs}`: one side is a single series with no labels and the other carries labels, \
         so the comparison keeps no sample. Wrap the label-less side in `scalar()` if it is a bound, \
         or give it a `by` clause if it is a per-label aggregate.",
        op = &promql[at..at + len],
    ))
}

/// Byte offset and length of the first comparison at the top level.
fn top_level_comparison(text: &str) -> Option<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' | b'`' => i = skip_string(bytes, i),
            b'(' | b'{' | b'[' => depth += 1,
            b')' | b'}' | b']' => depth -= 1,
            b'>' | b'<' | b'!' | b'=' if depth == 0 => {
                // `>=`, `<=`, `==`, `!=` are two bytes; a lone `>`, `<` or `=` is one.
                let len = 1 + usize::from(bytes.get(i + 1) == Some(&b'='));
                return Some((i, len));
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Index of the byte that closes a quoted run, so the caller's own advance
/// steps over it exactly once. Tolerates an unterminated run by returning the
/// last byte, which is also where the caller stops.
fn skip_string(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            return i;
        }
        i += 1;
    }
    bytes.len().saturating_sub(1)
}

/// The identifier a slice starts with, and the remainder.
fn leading_ident(text: &str) -> Option<(&str, &str)> {
    let t = text.trim_start();
    let end = t
        .char_indices()
        .find(|(_, c)| !(c.is_alphanumeric() || *c == '_' || *c == ':'))
        .map(|(i, _)| i)
        .unwrap_or(t.len());
    if end == 0 {
        return None;
    }
    Some((&t[..end], &t[end..]))
}

/// The text inside a pair of parentheses that spans the whole slice.
fn balanced_inner(text: &str) -> Option<&str> {
    let t = text.trim();
    if !t.starts_with('(') || !t.ends_with(')') {
        return None;
    }
    let mut depth = 0i32;
    let bytes = t.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' | b'`' => i = skip_string(bytes, i),
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 && i != bytes.len() - 1 {
                    return None; // the closing paren is not the last character
                }
            }
            _ => {}
        }
        i += 1;
    }
    t.get(1..t.len() - 1)
}

/// Split on arithmetic operators at the top level, ignoring a leading sign.
fn split_arithmetic(text: &str) -> Option<Vec<&str>> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut cuts = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' | b'`' => i = skip_string(bytes, i),
            b'(' | b'{' | b'[' => depth += 1,
            b')' | b'}' | b']' => depth -= 1,
            b'+' | b'*' | b'/' | b'%' | b'^' if depth == 0 => cuts.push((i, 1)),
            b'-' if depth == 0 && i > 0 => cuts.push((i, 1)),
            _ => {}
        }
        i += 1;
    }
    if cuts.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    let mut start = 0usize;
    for (at, len) in cuts {
        parts.push(&text[start..at]);
        start = at + len;
    }
    parts.push(&text[start..]);
    Some(parts)
}

/// A call whose argument list is the whole expression: `name(args)`.
fn whole_call(text: &str) -> Option<(&str, &str)> {
    let (name, rest) = leading_ident(text)?;
    let rest = rest.trim_start();
    if !rest.starts_with('(') {
        return None;
    }
    Some((name, balanced_inner(rest)?))
}

/// An aggregator, which drops the labels of its input unless a grouping clause
/// names the ones to keep.
fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "sum"
            | "min"
            | "max"
            | "avg"
            | "group"
            | "stddev"
            | "stdvar"
            | "count"
            | "quantile"
            | "topk"
            | "bottomk"
            | "count_values"
    )
}

/// Whether the tail opens with a `by`/`without` grouping keyword as a whole word.
fn starts_with_modifier(tail: &str) -> bool {
    ["by", "without"].iter().any(|word| {
        tail.strip_prefix(word)
            .is_some_and(|rest| rest.starts_with(|c: char| c.is_whitespace() || c == '('))
    })
}

fn is_number(text: &str) -> bool {
    let t = text.trim();
    t.parse::<f64>().is_ok() || matches!(t, "Inf" | "+Inf" | "-Inf" | "NaN")
}

/// Classify one side of a comparison.
fn side_of(expr: &str) -> Side {
    let mut text = expr.trim();
    while let Some(inner) = balanced_inner(text) {
        text = inner.trim();
    }
    if text.is_empty() {
        return Side::Unknown;
    }
    if is_number(text) {
        return Side::Scalar;
    }
    if let Some((name, _args)) = whole_call(text) {
        return match name {
            "scalar" | "time" | "pi" | "rand" => Side::Scalar,
            "vector" => Side::LabelLess,
            // An aggregator with no grouping clause collapses everything into
            // one series carrying no labels. `topk`, `bottomk` and
            // `count_values` are deliberately not in this list: they hand back
            // the source series, labels and all.
            "sum" | "min" | "max" | "avg" | "group" | "stddev" | "stdvar" | "count"
            | "quantile" => Side::LabelLess,
            // Every other function — the `*_over_time` family, `rate`, `abs`,
            // `clamp_*`, `label_replace`, and so on — returns an instant vector
            // that carries its input's labels. `scalar()` is the only one that
            // yields a scalar, and it is handled above.
            _ => Side::Labelled,
        };
    }
    if let Some((name, rest)) = leading_ident(text) {
        let tail = rest.trim_start();
        // `<aggregate> by (...) (...)` and `<aggregate> without (...) (...)`
        // keep whatever labels the clause does not name.
        if is_aggregate(name) && starts_with_modifier(tail) {
            return Side::Labelled;
        }
        // A selector: a metric name, with or without matchers. Series arrive
        // from a scrape, so they carry whatever labels the exporter attached.
        if tail.is_empty() || tail.starts_with('{') {
            return Side::Labelled;
        }
    }
    if let Some(parts) = split_arithmetic(text) {
        let sides: Vec<Side> = parts.iter().map(|p| side_of(p)).collect();
        if sides.iter().all(|s| *s == Side::Scalar) {
            return Side::Scalar;
        }
        if sides.contains(&Side::Unknown) {
            return Side::Unknown;
        }
        if sides.contains(&Side::LabelLess) {
            return Side::LabelLess;
        }
        return Side::Labelled;
    }
    Side::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// A close that did not go through must leave the key believed, because the
    /// belief set is the retry set: nothing else remembers that a row still has
    /// to be closed. Measured 2026-10-23 — two rows were closed while the
    /// database answered in two-second statements; the writes failed, the keys
    /// were dropped anyway, and both rows stayed `firing` for good, one of them
    /// naming a Pod that no longer exists.
    #[test]
    fn a_close_that_failed_keeps_the_key_so_the_next_tick_retries() {
        let key = "pod_restarting_repeatedly:pod=gone";
        let mut believed: HashSet<String> = HashSet::from([key.to_string()]);
        settle_close(&mut believed, key, false);
        assert!(
            believed.contains(key),
            "关不掉的行必须留在重试集里，否则再没有东西会去关它"
        );
        settle_close(&mut believed, key, true);
        assert!(
            !believed.contains(key),
            "关掉之后不许再留着，那是重试集，不是历史"
        );
    }

    #[test]
    fn parse_vector_extracts_labels_and_values() {
        let body = serde_json::json!({
            "status": "success",
            "data": {"resultType": "vector", "result": [
                {"metric": {"node": "vm-1", "mountpoint": "/", "__name__": "x"},
                 "value": [1700000000, "85.5"]},
                {"metric": {"node": "vm-1", "mountpoint": "/data"},
                 "value": [1700000000, "12"]}
            ]}
        });
        let samples = parse_vector_response(&body).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].value, 85.5);
        assert_eq!(samples[0].labels.get("node").unwrap(), "vm-1");
    }

    #[test]
    fn parse_vector_rejects_error_responses() {
        let body = serde_json::json!({"status": "error", "error": "parse error"});
        assert!(parse_vector_response(&body).is_err());
        // Missing data.result must error too, not read as zero series —
        // zero series resolves firing alerts.
        assert!(parse_vector_response(&serde_json::json!({"status": "success"})).is_err());
    }

    /// A `/api/v1/alerts` body with four alerts that differ in exactly the
    /// dimensions the mirror decides on: state, the marking label, and whether
    /// this process already evaluates the rule.
    fn alerts_fixture() -> serde_json::Value {
        serde_json::json!({
            "status": "success",
            "data": {"alerts": [
                {"state": "firing", "value": "1",
                 "labels": {"alertname": "CognevaE2eRingFailed", "severity": "warning",
                            "alertstate": "firing", "cogneva_io_managed": "true",
                            "mountpoint": "/"},
                 "annotations": {"summary": "环 3 停在失败态"}},
                {"state": "pending", "value": "1",
                 "labels": {"alertname": "StillBurning", "severity": "warning",
                            "alertstate": "pending", "cogneva_io_managed": "true"}},
                {"state": "firing", "value": "1",
                 "labels": {"alertname": "Watchdog", "severity": "none",
                            "alertstate": "firing"}},
                {"state": "firing", "value": "1",
                 "labels": {"alertname": "node_disk_usage_high", "severity": "warning",
                            "alertstate": "firing", "cogneva_io_managed": "true"},
                 "annotations": {"description": "整窗拟合说盘要到触发线了"}},
                {"state": "firing", "value": "1",
                 "labels": {"alertname": "NoSummary", "severity": "critical",
                            "alertstate": "firing", "cogneva_io_managed": "true"}}
            ]}
        })
    }

    #[test]
    fn mirror_takes_only_firing_marked_alerts_this_process_does_not_evaluate() {
        let mirror = crate::config::AlertMirrorConfig {
            label: "cogneva_io_managed".into(),
            value: "true".into(),
            include_unmarked: false,
            exclude: Vec::new(),
        };
        // `node_disk_usage_high` is one of this process's own rules: the
        // in-process evaluation books it, and a second writer would open a
        // second row for one incident.
        let own: HashSet<&str> = ["node_disk_usage_high"].into_iter().collect();
        let alerts = parse_alerts_response(&alerts_fixture(), &mirror, &own).unwrap();
        let names: Vec<&str> = alerts.iter().map(|a| a.rule.as_str()).collect();
        assert_eq!(names, vec!["CognevaE2eRingFailed", "NoSummary"]);
        // Summary first, then description, then the rule name: a row with no
        // text says nothing about what is wrong.
        assert_eq!(alerts[0].message, "环 3 停在失败态");
        assert_eq!(alerts[1].message, "NoSummary");
        assert_eq!(alerts[1].severity, cog_core::AlertSeverity::Critical);
    }

    fn upstream_fixture() -> serde_json::Value {
        serde_json::json!({
            "status": "success",
            "data": {"alerts": [
                {"state": "firing",
                 "labels": {"alertname": "KubeJobFailed", "severity": "warning",
                            "namespace": "cogneva", "pod": "cogneva-build-7f9c"}},
                {"state": "firing",
                 "labels": {"alertname": "Watchdog", "severity": "none"}},
                {"state": "firing",
                 "labels": {"alertname": "CognevaAlertUnattended", "severity": "warning",
                            "cogneva_io_managed": "true"}},
                {"state": "resolved",
                 "labels": {"alertname": "KubePodCrashLooping", "severity": "warning"}}
            ]}
        })
    }

    fn upstream_mirror(include_unmarked: bool) -> crate::config::AlertMirrorConfig {
        crate::config::AlertMirrorConfig {
            label: "cogneva_io_managed".into(),
            value: "true".into(),
            include_unmarked,
            exclude: vec!["Watchdog".into(), "InfoInhibitor".into()],
        }
    }

    #[test]
    fn unmarked_alerts_stay_out_until_the_mirror_is_asked_for_them() {
        // 默认只抄带标记的：现网每一次升级都走这条路径，行为必须一模一样。
        let alerts = parse_alerts_response(
            &upstream_fixture(),
            &upstream_mirror(false),
            &HashSet::new(),
        )
        .unwrap();
        let names: Vec<&str> = alerts.iter().map(|a| a.rule.as_str()).collect();
        assert_eq!(names, vec!["CognevaAlertUnattended"]);
    }

    #[test]
    fn asking_for_unmarked_alerts_puts_the_upstream_ones_in_the_ledger() {
        let alerts =
            parse_alerts_response(&upstream_fixture(), &upstream_mirror(true), &HashSet::new())
                .unwrap();
        let names: Vec<&str> = alerts.iter().map(|a| a.rule.as_str()).collect();
        // `Watchdog` fires by construction, so it is in the fixture and must not
        // come back; `KubePodCrashLooping` is resolved, not firing.
        assert_eq!(
            names,
            vec!["KubeJobFailed", "CognevaAlertUnattended"],
            "exclusions and the firing filter both still apply"
        );
        // 上游规则的身份带 pod：同一个 Job 失败一次就是一个实例，这一行说得清是谁。
        assert_eq!(
            alerts[0].key(),
            "KubeJobFailed:namespace=cogneva,pod=cogneva-build-7f9c"
        );
    }

    #[test]
    fn an_excluded_name_is_skipped_even_when_it_carries_the_marker() {
        // 排除表压过两种选择：列进去的名字永远不是事件。读侧减的是同一份表，
        // 所以它也不会被报成缺口。
        let mut mirror = upstream_mirror(true);
        mirror.exclude.push("CognevaAlertUnattended".into());
        let alerts = parse_alerts_response(&upstream_fixture(), &mirror, &HashSet::new()).unwrap();
        let names: Vec<&str> = alerts.iter().map(|a| a.rule.as_str()).collect();
        assert_eq!(names, vec!["KubeJobFailed"]);
    }

    #[test]
    fn mirror_key_names_the_subject_not_the_alert_envelope() {
        let mirror = crate::config::AlertMirrorConfig {
            label: "cogneva_io_managed".into(),
            value: "true".into(),
            include_unmarked: false,
            exclude: Vec::new(),
        };
        let alerts = parse_alerts_response(&alerts_fixture(), &mirror, &HashSet::new()).unwrap();
        let e2e = &alerts[0];
        assert_eq!(e2e.key(), "CognevaE2eRingFailed:mountpoint=/");
        // The envelope labels must not reach the row either: they describe the
        // alert, and a stored label is what the remediation text is built from.
        assert!(!e2e.labels.contains_key("alertname"));
        assert!(!e2e.labels.contains_key("severity"));
        assert!(!e2e.labels.contains_key("alertstate"));
        assert_eq!(e2e.labels.get("mountpoint").map(String::as_str), Some("/"));
    }

    /// The face tells a reader who to call, so it has to survive into the row —
    /// and it must stay out of the key, or handing a rule to another face forks
    /// a second row for an incident that never changed.
    #[test]
    fn mirror_keeps_the_declared_face_out_of_the_key_and_in_the_row() {
        let mirror = crate::config::AlertMirrorConfig {
            label: "cogneva_io_managed".into(),
            value: "true".into(),
            include_unmarked: false,
            exclude: Vec::new(),
        };
        let with_face = serde_json::json!({
            "status": "success",
            "data": {"alerts": [
                {"state": "firing", "value": "1",
                 "labels": {"alertname": "CognevaE2eRingFailed", "severity": "warning",
                            "alertstate": "firing", "cogneva_io_managed": "true",
                            "cogneva_io_face": "端到端演练", "mountpoint": "/"}}
            ]}
        });
        let alerts = parse_alerts_response(&with_face, &mirror, &HashSet::new()).unwrap();
        let alert = &alerts[0];
        assert_eq!(alert.key(), "CognevaE2eRingFailed:mountpoint=/");
        assert_eq!(alert.face.as_deref(), Some("端到端演练"));
        assert!(!alert.labels.contains_key(ALERT_FACE_LABEL));

        // A rule that declares no face keeps the same key and an empty face:
        // the reader counts that row unowned rather than lending it the face of
        // whatever fired next to it.
        let unfaced = parse_alerts_response(&alerts_fixture(), &mirror, &HashSet::new()).unwrap();
        assert_eq!(unfaced[0].key(), alert.key());
        assert_eq!(unfaced[0].face, None);
    }

    /// The in-process writer hangs the face on the row the same way: present
    /// when the rule declared one, absent (not empty) when it did not, and never
    /// part of the identity the key was computed from.
    #[test]
    fn alert_labels_carry_the_face_without_changing_identity() {
        let s = sample(&[("node", "vm-1"), ("job", "node-exporter")], 90.0);
        let key_before = dedup_key("node_disk_usage_high", &s.labels);

        let owned = alert_labels(&s, "root filesystem 90%", "资源管理");
        assert_eq!(
            owned.get(ALERT_FACE_LABEL).map(String::as_str),
            Some("资源管理")
        );
        assert_eq!(dedup_key("node_disk_usage_high", &s.labels), key_before);
        // The face must not become an identity label: it is not derived from
        // the series, and identity labels are copied from the sample.
        assert!(!identity_labels(&s.labels).contains_key(ALERT_FACE_LABEL));

        let unowned = alert_labels(&s, "root filesystem 90%", "");
        assert!(
            !unowned.contains_key(ALERT_FACE_LABEL),
            "an undeclared face must write no label at all, not an empty one"
        );
    }

    #[test]
    fn mirror_rejects_error_and_malformed_responses() {
        let mirror = crate::config::AlertMirrorConfig {
            label: "cogneva_io_managed".into(),
            value: "true".into(),
            include_unmarked: false,
            exclude: Vec::new(),
        };
        let err = serde_json::json!({"status": "error", "error": "boom"});
        assert!(parse_alerts_response(&err, &mirror, &HashSet::new()).is_err());
        // `success` with no alerts array must error rather than read as "nothing
        // is firing": that answer resolves every mirrored row.
        let empty = serde_json::json!({"status": "success", "data": {}});
        assert!(parse_alerts_response(&empty, &mirror, &HashSet::new()).is_err());
    }

    #[test]
    fn dedup_key_ignores_scrape_labels() {
        let a = BTreeMap::from([
            ("node".to_string(), "vm-1".to_string()),
            ("job".to_string(), "node-exporter".to_string()),
            ("__name__".to_string(), "m".to_string()),
        ]);
        let b = BTreeMap::from([
            ("node".to_string(), "vm-1".to_string()),
            ("job".to_string(), "other-scrape".to_string()),
            ("namespace".to_string(), "monitoring".to_string()),
            ("pod".to_string(), "exporter-abc".to_string()),
            ("container".to_string(), "exporter".to_string()),
        ]);
        assert_eq!(dedup_key("disk", &a), "disk:node=vm-1");
        // pod is an identity label (which pod is crashlooping matters), and so
        // are its namespace and container: the same pod name in another
        // namespace is another victim, and one pod can run several containers.
        assert_eq!(
            dedup_key("crash", &b),
            "crash:container=exporter,namespace=monitoring,node=vm-1,pod=exporter-abc"
        );
        // The failure this guards: two crashlooping pods that differ only in
        // namespace used to land on one key, so the row flapped between them.
        let mut elsewhere = b.clone();
        elsewhere.insert("namespace".to_string(), "cogneva".to_string());
        assert_ne!(dedup_key("crash", &b), dedup_key("crash", &elsewhere));
    }

    /// `deployment` is the one identity label that has to survive even though
    /// it looks like a scrape label: it is written by the producing process
    /// (which deployment it runs as), not by whoever scraped the endpoint, so
    /// it says who is being reported and not who answered.
    ///
    /// The live case it comes from: two deployments serve the same reading from
    /// the same binary under different service accounts, and a ClusterRoleBinding
    /// named one of them and not the other. Both rows carry the same `resource`
    /// and the same namespace, so without this label the alert says "nodes is
    /// unreadable" and cannot say which account to look at -- and when both fail
    /// at once, their two incidents are one row.
    #[test]
    fn the_deployment_that_wrote_a_reading_is_part_of_its_identity() {
        let gateway = BTreeMap::from([
            ("resource".to_string(), "nodes".to_string()),
            ("deployment".to_string(), "cogneva".to_string()),
            ("job".to_string(), "cogneva".to_string()),
        ]);
        let evolution = BTreeMap::from([
            ("resource".to_string(), "nodes".to_string()),
            ("deployment".to_string(), "cogneva-evolution".to_string()),
            ("job".to_string(), "cogneva-evolution".to_string()),
        ]);
        assert_ne!(
            dedup_key("cluster_read_failed", &gateway),
            dedup_key("cluster_read_failed", &evolution),
            "两个进程的同一格读数落在同一个 dedup key 上：告警点不出是哪个 ServiceAccount"
        );
        assert!(
            identity_labels(&gateway).contains_key("deployment"),
            "deployment 被当成抓取标签丢掉了：生产者写的进程名到不了告警行"
        );
    }

    /// The live reading this comes from: a store-held series is rendered by
    /// every deployment that scrapes the endpoint, so the sample that reports a
    /// sighting carries the *answering pod's* job/service. Stored as written,
    /// `memory_raw_backlog_aged_out` was persisted naming `cogneva-evolution`
    /// while only the `cogneva` deployment produces the reading — and those
    /// labels are what the remediation goal text is built from. The subject a
    /// row names must be the one the dedup key was computed for.
    #[test]
    fn a_stored_alert_names_the_instance_and_not_the_scrape() {
        let s = sample(
            &[
                ("node", "vm-1"),
                ("namespace", "cogneva"),
                ("pod", "cogneva-evolution-6c5cb5669d-grvst"),
                ("job", "cogneva-evolution"),
                ("service", "cogneva-evolution"),
                ("endpoint", "http"),
                ("prometheus", "monitoring"),
                ("__name__", "m"),
            ],
            1.0,
        );
        let labels = alert_labels(&s, "backlog aged out", "");

        assert_eq!(labels.get("node").map(String::as_str), Some("vm-1"));
        assert_eq!(
            labels.get("namespace").map(String::as_str),
            Some("cogneva"),
            "namespace distinguishes victims and must survive"
        );
        assert_eq!(
            labels.get("pod").map(String::as_str),
            Some("cogneva-evolution-6c5cb5669d-grvst"),
            "which pod is the victim must survive"
        );
        assert_eq!(
            labels.get(crate::LABEL_MESSAGE).map(String::as_str),
            Some("backlog aged out")
        );
        assert_eq!(
            labels.get(crate::alerts::LABEL_SOURCE).map(String::as_str),
            Some("infra_watch")
        );
        for gone in ["job", "service", "endpoint", "prometheus", "__name__"] {
            assert!(
                !labels.contains_key(gone),
                "{gone} names the scrape, not the subject: storing it re-attaches \
                 a pod that merely served the metric"
            );
        }
        // The stored labels and the key must agree on the subject: a caller
        // cannot see both and believe two different things caused the row.
        assert_eq!(
            dedup_key("aged_out", &s.labels),
            "aged_out:namespace=cogneva,node=vm-1,pod=cogneva-evolution-6c5cb5669d-grvst"
        );
    }

    fn sample(pairs: &[(&str, &str)], value: f64) -> SeriesSample {
        SeriesSample {
            labels: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value,
        }
    }

    /// The live reading this comes from: `aof_repair_reading_absent` has been
    /// firing since 09-25 with `last_seen_at` frozen at its firing edge, while
    /// the condition evaluated true on every tick in between. A row nobody
    /// advances reads exactly like a watcher that stopped looking, which is the
    /// one thing the sighting clock exists to tell apart.
    #[test]
    fn a_series_already_believed_firing_is_reported_again_every_tick() {
        let rule = test_rule("disk");
        let series = vec![sample(&[("node", "vm-1")], 1.0)];
        let believed = HashSet::from([dedup_key("disk", &series[0].labels)]);

        let plan = plan_rule(&rule, &series, &believed);
        assert_eq!(
            plan.report,
            vec![0],
            "a series whose condition still holds must be reported again: the \
             store's sighting clock only moves when it is told"
        );
        assert!(plan.close.is_empty(), "nothing stopped holding");
    }

    #[test]
    fn a_series_that_stopped_holding_is_closed_and_one_that_vanished_is_too() {
        let rule = test_rule("disk");
        let stopped = sample(&[("node", "vm-1")], 0.0);
        let gone = sample(&[("node", "vm-2")], 1.0);
        let believed = HashSet::from([
            dedup_key("disk", &stopped.labels),
            dedup_key("disk", &gone.labels),
        ]);

        // The first came back below its threshold, the second did not come back
        // at all; both are rows this rule can no longer see holding.
        let plan = plan_rule(&rule, std::slice::from_ref(&stopped), &believed);
        assert!(plan.report.is_empty());
        let mut closed = plan.close.clone();
        closed.sort();
        let mut expected = vec![
            dedup_key("disk", &stopped.labels),
            dedup_key("disk", &gone.labels),
        ];
        expected.sort();
        assert_eq!(closed, expected);

        let plan = plan_rule(&rule, &[], &believed);
        assert!(plan.report.is_empty());
        assert_eq!(
            plan.close.len(),
            2,
            "an empty result closes everything it held"
        );
    }

    /// Closing is scoped to the rows this rule raised. A key belonging to
    /// another rule is not this rule's to close: the two would close each
    /// other's rows and the alert would flap.
    #[test]
    fn one_rule_does_not_close_another_rules_rows() {
        let rule = test_rule("disk");
        let elsewhere = dedup_key(
            "cpu",
            &BTreeMap::from([("node".to_string(), "vm-1".to_string())]),
        );
        let believed = HashSet::from([elsewhere]);
        let plan = plan_rule(&rule, &[], &believed);
        assert!(plan.report.is_empty());
        assert!(
            plan.close.is_empty(),
            "closed a row belonging to another rule: {plan:?}"
        );
    }

    /// The live reading this comes from: an alert that fired with the summary
    /// "Pending state for {stream} has not been measured for 372.37s" — the
    /// stream was in the labels the whole time, the renderer just never looked.
    #[test]
    fn summary_placeholders_are_filled_from_the_firing_sample() {
        let labels = BTreeMap::from([
            ("stream".to_string(), "changes".to_string()),
            ("pod".to_string(), "cogneva-evolution-0".to_string()),
        ]);
        assert_eq!(
            render_summary(
                "Pending state for {stream} has not been measured for {value}s",
                372.375,
                &labels
            ),
            "Pending state for changes has not been measured for 372.38s"
        );
        // A placeholder naming neither the value nor a label of this sample
        // stays visible: the reader must be able to see the rule is not
        // producing the sentence it was written to produce.
        assert_eq!(
            render_summary("tier {tier} demoted for {value}s", 12.0, &labels),
            "tier {tier} demoted for 12.00s"
        );
        // Not a placeholder at all: an unclosed brace is literal text.
        assert_eq!(render_summary("cost {usd", 1.0, &labels), "cost {usd");
        assert_eq!(
            render_summary("no placeholders", 1.0, &labels),
            "no placeholders"
        );
    }

    /// Filling a summary means asserting the series carries that label, so the
    /// allowed vocabulary is read from the two places labels come from — the
    /// label names the metrics declare in code, and the label names the shipped
    /// rules' own PromQL selects — never from a list written here. A name
    /// spelled wrong, or one whose producer was renamed, fails this instead of
    /// reaching the reader as `{stram}`.
    #[test]
    fn shipped_rule_summaries_only_name_value_or_a_label_that_exists() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");

        // Every label any metric declares. Scanned from source so a produced
        // label needs no second edit here.
        //
        // A crate writes a label's name in one of two shapes, and the scan reads
        // both. It used to read only the first, and that made it blind exactly
        // when the tree got better: on 2026-10-27 the crates moved their label
        // names into constants one commit at a time, and the scan went on
        // finding two names out of more than a hundred and forty -- so the check
        // still passed on a workspace where it could not see the vocabulary the
        // shipped rules are written against, until the assertion below caught it
        // in CI, where it held the whole deployment.
        //
        // Which of the two shapes a call site uses is a style the tree is
        // allowed to change; what cannot change is that a summary may only name
        // a label some producer writes.
        let mut declared: BTreeSet<String> = BTreeSet::new();
        let mut files = 0;
        for entry in walk_rs_files(&root.join("crates")) {
            let text = std::fs::read_to_string(&entry).expect("read crate source");
            files += 1;
            // Written at the call site: `.with_label("tier", ...)`.
            for rest in text.split(".with_label(\"").skip(1) {
                if let Some(name) = rest.split('"').next() {
                    declared.insert(name.to_string());
                }
            }
            // Bound once and referred to by name: `const LABEL_TIER: &str =
            // "tier";`. Matched line by line because that is the shape a
            // one-literal constant keeps under rustfmt; a declaration it wraps
            // is not read, and the name it binds is then missing from the
            // vocabulary, which the assertion below reports rather than a
            // summary quietly escaping the check.
            for line in text.lines() {
                let Some(declaration) = line.find("const LABEL") else {
                    continue;
                };
                let Some(binding) = line[declaration..].find("&str = \"") else {
                    continue;
                };
                let literal = &line[declaration + binding + "&str = \"".len()..];
                if let Some(name) = literal.split('"').next() {
                    if !name.is_empty() {
                        declared.insert(name.to_string());
                    }
                }
            }
        }
        assert!(
            files > 0,
            "no crate sources scanned; the workspace path moved"
        );
        assert!(
            declared.contains("stream") && declared.contains("tier") && declared.contains("asset"),
            "the scan missed labels it must see: {declared:?}"
        );

        let path = root.join("deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&path).expect("read the chart's cogneva.json");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("chart JSON parses");
        let rules = parsed["observability"]["infra_watch"]["rules"]
            .as_array()
            .expect("rules array");
        let mut checked = 0;
        for rule in rules {
            let promql = rule["promql"].as_str().unwrap_or_default();
            // Labels this rule's own PromQL names: `label="..."` matchers and
            // `by (...)` groupings both keep that label on the sample.
            let mut selected: BTreeSet<String> = BTreeSet::new();
            for kv in promql
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '='))
                .filter(|s| s.contains('='))
            {
                if let Some((name, _)) = kv.split_once('=') {
                    if !name.is_empty() {
                        selected.insert(name.to_string());
                    }
                }
            }
            for group in promql
                .split("by (")
                .skip(1)
                .chain(promql.split("without (").skip(1))
            {
                if let Some(list) = group.split(')').next() {
                    for name in list.split(',') {
                        let name = name.trim();
                        if !name.is_empty() {
                            selected.insert(name.to_string());
                        }
                    }
                }
            }

            let summary = rule["summary"].as_str().unwrap_or_default();
            let mut rest = summary;
            while let Some(open) = rest.find('{') {
                let after = &rest[open + 1..];
                let close = after.find('}').expect("summary has an unclosed brace");
                let name = &after[..close];
                assert!(
                    name == "value" || declared.contains(name) || selected.contains(name),
                    "rule {} summary names {{{name}}}, which is neither the value, a label the \
                     metrics declare, nor a label this rule selects: {summary}",
                    rule["name"].as_str().unwrap_or_default()
                );
                rest = &after[close + 1..];
            }
            checked += 1;
        }
        assert_eq!(checked, rules.len(), "every shipped rule was inspected");
        assert!(checked > 0, "no rules read; the config path moved");
    }

    /// 遍历随包发布的每一条规则：这种缺陷的读者必须落在会出事的那条路上——规则是
    /// 随 chart 发出去的，改坏了在这里红，而不是等它装载、每拍求值、然后静默一整天。
    #[test]
    fn no_shipped_rule_compares_a_labelled_side_with_an_unlabelled_one() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let path = root.join("deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("chart JSON parses");
        let rules = parsed
            .pointer(cog_core::config_sections::ALERT_RULES_POINTER)
            .and_then(|v| v.as_array())
            .expect("rules array");
        assert!(!rules.is_empty(), "no rules read; the config path moved");
        let mut flagged: Vec<String> = Vec::new();
        for rule in rules {
            let Some(promql) = rule["promql"].as_str() else {
                continue;
            };
            if let Some(why) = unpaired_comparison(promql) {
                flagged.push(format!(
                    "{}: {why}",
                    rule["name"].as_str().unwrap_or("<unnamed>")
                ));
            }
        }
        assert!(
            flagged.is_empty(),
            "规则的比较两侧配不上：它会装载、每拍求值，却一个样本也留不下:\n{}",
            flagged.join("\n")
        );
    }

    /// 反例取自那条规则**修前**的原文。判据抓不住它，就等于没接上——一条只会给
    /// 已经修好的形状报绿的检查，和被它检查的东西一样静默。
    #[test]
    fn a_comparison_against_a_bare_aggregate_is_unpairable() {
        let before =
            "cogneva_stream_read_silent_seconds > 12 * max(cogneva_stream_read_block_seconds)";
        let why = unpaired_comparison(before).expect("修前那条必须被抓住");
        assert!(why.contains("scalar()"), "判词要指出出路: {why}");
    }

    #[test]
    fn a_scalar_wrapped_bound_pairs() {
        let after = "cogneva_stream_read_silent_seconds > \
                     scalar(12 * max(cogneva_stream_read_block_seconds))";
        assert_eq!(unpaired_comparison(after), None);
    }

    /// 两侧都无标签时它们确实配得上：空标签集等于空标签集。把这些报成缺陷就是
    /// 假阳性，会把本来正确的规则逼着改坏。
    #[test]
    fn two_unlabelled_sides_do_pair() {
        assert_eq!(unpaired_comparison("time() - max(a) > 6 * max(b)"), None);
        assert_eq!(unpaired_comparison("max(a) > min(b)"), None);
    }

    #[test]
    fn a_number_bound_needs_no_scalar() {
        assert_eq!(
            unpaired_comparison("sum by (namespace) (increase(failures_total[6h])) > 6"),
            None
        );
    }

    /// `on()` / `ignoring()` / `group_left` 改写配对规则，两侧能不能遇上不再由
    /// 文本本身说明——判不了就不判，宁漏不误报。
    #[test]
    fn an_explicit_join_modifier_is_left_alone() {
        assert_eq!(
            unpaired_comparison("a > on(instance) group_left max(b)"),
            None
        );
    }

    /// 比较号出现在标签匹配器的字符串里时它不是比较运算符。
    #[test]
    fn a_comparison_inside_a_matcher_string_is_not_a_comparison() {
        assert_eq!(unpaired_comparison(r#"a{kind=">"} > 3"#), None);
    }

    /// Test-only source walk: reads filenames, never follows symlinks out of
    /// the tree, so a stray link cannot make the scan read something unrelated.
    fn walk_rs_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_rs_files(&path));
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
        out
    }

    #[test]
    fn urlencoding_escapes_promql() {
        assert_eq!(
            urlencoding("up{job=\"x\"} > 0"),
            "up%7Bjob%3D%22x%22%7D%20%3E%200"
        );
    }

    /// The status code is not the diagnosis: Prometheus puts the cause in the
    /// body, and two rules can both answer 422 for unrelated reasons.
    #[test]
    fn describe_api_error_reads_the_reason_from_the_body() {
        let body = br#"{"status":"error","errorType":"execution","error":"found duplicate series for the match group {namespace=\"cogneva\"} on the right hand-side"}"#;
        let detail = describe_api_error(body).unwrap();
        assert!(detail.starts_with("execution: "), "{detail}");
        assert!(detail.contains("duplicate series"), "{detail}");
    }

    /// A body that is not the API's error shape must still yield a reportable
    /// status rather than a bogus detail.
    #[test]
    fn describe_api_error_gives_nothing_for_a_foreign_body() {
        assert!(describe_api_error(b"<html>502 Bad Gateway</html>").is_none());
        assert!(describe_api_error(br#"{"status":"error"}"#).is_none());
    }

    /// `error` carries the whole match group; the alert row does not need it.
    #[test]
    fn describe_api_error_bounds_what_it_carries() {
        let long = "x".repeat(5000);
        let body = format!(r#"{{"status":"error","errorType":"bad_data","error":"{long}"}}"#);
        let detail = describe_api_error(body.as_bytes()).unwrap();
        assert_eq!(detail.chars().count(), 301, "capped plus an ellipsis");
        assert!(detail.ends_with('…'));
    }

    /// HTTP client stub: fails every request until flipped to succeed.
    #[derive(Debug)]
    struct FlakyHttp {
        fail: std::sync::atomic::AtomicBool,
    }

    impl Default for FlakyHttp {
        fn default() -> Self {
            Self {
                fail: std::sync::atomic::AtomicBool::new(true),
            }
        }
    }

    impl FlakyHttp {
        fn succeeding() -> Self {
            Self {
                fail: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for FlakyHttp {
        async fn execute(
            &self,
            _req: cog_core::HttpRequest,
        ) -> cog_core::SFResult<cog_core::HttpResponse> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::IO("connection refused".into()));
            }
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: HashMap::new(),
                body: serde_json::to_vec(&serde_json::json!({
                    "status": "success",
                    "data": {"resultType": "vector", "result": []}
                }))
                .unwrap(),
            })
        }
    }

    fn test_rule(name: &str) -> crate::config::InfraRule {
        crate::config::InfraRule {
            name: name.to_string(),
            promql: "up".to_string(),
            condition: cog_core::AlertCondition::GreaterThan(0.0),
            severity: cog_core::AlertSeverity::Warning,
            summary: "s".to_string(),
            face: String::new(),
            for_seconds: None,
        }
    }

    fn test_config(threshold: u32) -> InfraWatchConfig {
        InfraWatchConfig {
            enabled: true,
            prometheus_url: "http://prom:9090".to_string(),
            poll_interval_secs: 60,
            eval_failure_alert_after: threshold,
            rules: vec![test_rule("rule_a")],
            retired_rules: Vec::new(),
            mirror_alerts: None,
        }
    }

    fn firing_record(rule: &str, key: &str) -> AlertRecord {
        AlertRecord {
            id: "id".into(),
            rule: rule.into(),
            dedup_key: key.into(),
            severity: "warning".into(),
            state: "firing".into(),
            message: String::new(),
            labels: serde_json::json!({}),
            fired_at: Utc::now(),
            resolved_at: None,
            updated_at: Utc::now(),
            last_seen_at: Some(Utc::now()),
            acknowledged_at: None,
            acknowledged_by: None,
        }
    }

    /// 镜像要关掉的行由**表**决定，不由本进程记得什么决定。
    ///
    /// 2026-10-23 实测：表里六行镜像告警开着，`last_seen_at` 冻在同一秒，而
    /// Prometheus 早已不再报它们——它们各自的规则表达式换了聚合方式，标签集变了，
    /// 于是新身份写出一行新的，旧身份那行留在 `state='firing'`。镜像当时拿
    /// `state.firing`（内存里的 HashSet，重启即空、失败一次即丢）去减这一拍看见的键，
    /// 减出来的是空集：**它只关得掉自己还记得的那些行，而需要关的恰恰是它不记得的**。
    ///
    /// 所以判据的输入换成 store：`open` 是表里还开着的、带本生产者 source 标签的键。
    /// 下面钉住三种「记得就关不掉」的形状：另一进程开的行、这一拍没看见的行、
    /// 以及这一拍看见了别的行时不该被误关的行。
    #[test]
    fn mirror_closes_rows_the_store_holds_that_this_pass_did_not_see() {
        let seen: HashSet<String> = ["CognevaAlertUnattended:".to_string()]
            .into_iter()
            .collect();
        let open = vec![
            // 本进程这一拍确实看见了它——不许关。
            "CognevaAlertUnattended:".to_string(),
            // 上一进程开的、身份带面标签的旧行：本进程从没见过这个键。
            "CognevaAlertUnattended:cogneva_io_face=可观测与告警".to_string(),
            "CognevaAcceptanceSpecStale:verdict=stale,instance=cogneva".to_string(),
        ];
        let mut orphans = mirror_orphans(&open, &seen);
        orphans.sort_unstable();
        assert_eq!(
            orphans,
            vec![
                "CognevaAcceptanceSpecStale:verdict=stale,instance=cogneva",
                "CognevaAlertUnattended:cogneva_io_face=可观测与告警",
            ]
        );

        // 表里一行都没开着：无事可做，不是错误，也不该凭空发出解析通知。
        assert!(mirror_orphans(&[], &seen).is_empty());
    }

    /// 停用一条规则要把它**所有**开着的行都结算掉，而不只是第一条。
    ///
    /// 一条按序列开多行的规则（每台上游一行）停用时，只关掉其中一行会让剩下的
    /// 行继续以"正在持续"的样子留在 `state='firing'` 那一面上，而它们的规则
    /// 已经不在了——那正是这份名单要消掉的东西。
    #[test]
    fn retirement_selects_every_open_row_of_a_retired_rule() {
        let records = vec![
            firing_record("llm_pool_down", "llm_pool_down"),
            firing_record("llm_calls_all_failing", "llm_calls_all_failing:a"),
            firing_record("llm_calls_all_failing", "llm_calls_all_failing:b"),
            firing_record("llm_upstream_pool_down", "llm_upstream_pool_down@1"),
        ];
        let retired = vec!["llm_pool_down".to_string()];
        assert_eq!(retired_keys(&retired, &records), vec!["llm_pool_down"]);

        // 另一条生产者（代码侧那条按事件身份开的行）不在名单里，就不该被动到：
        // 停用的语义是"这条规则不在了"，不是"凡是不在名单上的都关掉"。
        let retired = vec![
            "llm_pool_down".to_string(),
            "llm_calls_all_failing".to_string(),
        ];
        let mut keys = retired_keys(&retired, &records);
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "llm_calls_all_failing:a",
                "llm_calls_all_failing:b",
                "llm_pool_down"
            ]
        );

        // 名单里的名字没有任何行时无事发生，不是错误。
        let keys = retired_keys(&["never_existed".to_string()], &records);
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn eval_failures_raise_self_alert_after_threshold_and_resolve_on_success() {
        let config = test_config(2);
        let outlets = InfraWatchOutlets {
            store: None,
            notifier: None,
            metrics: None,
        };
        let http: Arc<dyn cog_core::HttpClient> = Arc::new(FlakyHttp::default());
        let mut known_firing = HashSet::new();
        let mut eval_firing = HashSet::new();
        let mut streaks = HashMap::new();
        let mut holding = HashMap::new();
        let key = format!("{EVAL_FAILURE_RULE}:rule_a");

        // First failure: below threshold, no self-alert.
        tick(
            &config,
            &outlets,
            &http,
            &mut known_firing,
            &mut eval_firing,
            &mut streaks,
            &mut holding,
        )
        .await;
        assert!(eval_firing.is_empty());

        // Second failure: threshold reached, self-alert key latches.
        tick(
            &config,
            &outlets,
            &http,
            &mut known_firing,
            &mut eval_firing,
            &mut streaks,
            &mut holding,
        )
        .await;
        assert!(eval_firing.contains(&key));

        // Recovery: successful query clears the self-alert and the streak.
        let http = Arc::new(FlakyHttp::succeeding());
        let http: Arc<dyn cog_core::HttpClient> = http;
        tick(
            &config,
            &outlets,
            &http,
            &mut known_firing,
            &mut eval_firing,
            &mut streaks,
            &mut holding,
        )
        .await;
        assert!(eval_firing.is_empty());
        assert!(streaks.is_empty());
    }

    /// The second carrier of what `a_series_already_believed_firing_is_reported_again_every_tick`
    /// pins: throttling the notification must not throttle the sighting.
    ///
    /// The live reading this comes from is a self-alert row whose `last_seen_at`
    /// sits frozen at the tick its streak reached the threshold while the rule
    /// went on failing every minute for hours. That row reads exactly like the
    /// watcher having stopped querying, which is the one thing the sighting
    /// clock exists to tell apart -- and it can never catch up, because the
    /// latch is also refilled from the store on restart.
    ///
    /// What this pins is the decision, like its sibling above; the call sites
    /// that act on it are not covered from here, because the store these tests
    /// have is `None`. Rerouting a sighting behind the notification latch would
    /// therefore pass this test -- the row's clock is only observable against a
    /// real store.
    #[test]
    fn a_failing_rule_re_sights_its_self_alert_every_tick_but_notifies_once() {
        // Below the threshold there is nothing to sight or to tell.
        assert!(eval_failure_plan(3, 2, false).is_none());

        // The tick that reaches the threshold does both.
        let first = eval_failure_plan(3, 3, false).expect("threshold reached");
        assert!(first.sight, "the row's clock has to start moving");
        assert!(first.notify, "a person is told once");

        // Every later tick keeps sighting and stops telling.
        let later = eval_failure_plan(3, 9, true).expect("still failing");
        assert!(
            later.sight,
            "a row that stops moving reads as a dead watcher"
        );
        assert!(
            !later.notify,
            "re-notifying every tick is the noise the streak prevents"
        );

        // Restarted process: the streak map starts empty, so it climbs back to
        // the threshold, but the latch came back with the row -- so the tick
        // that reaches the threshold sights and does not tell.
        assert!(
            eval_failure_plan(3, 1, true).is_none(),
            "streak restarts empty"
        );
        let adopted = eval_failure_plan(3, 3, true).expect("threshold reached again");
        assert!(
            adopted.sight,
            "the clock resumes even though this process never notified"
        );
        assert!(!adopted.notify);

        // Zero disables the self-alert entirely.
        assert!(eval_failure_plan(0, 9, false).is_none());
    }

    #[tokio::test]
    async fn eval_failure_threshold_zero_disables_self_alert() {
        let config = test_config(0);
        let outlets = InfraWatchOutlets {
            store: None,
            notifier: None,
            metrics: None,
        };
        let http: Arc<dyn cog_core::HttpClient> = Arc::new(FlakyHttp::default());
        let mut known_firing = HashSet::new();
        let mut eval_firing = HashSet::new();
        let mut streaks = HashMap::new();
        let mut holding = HashMap::new();
        for _ in 0..5 {
            tick(
                &config,
                &outlets,
                &http,
                &mut known_firing,
                &mut eval_firing,
                &mut streaks,
                &mut holding,
            )
            .await;
        }
        assert!(eval_firing.is_empty());
        assert_eq!(streaks.get("rule_a"), Some(&5));
    }

    /// 一条带 `for_seconds` 的规则，第一条样本还不算证据：它要的是「一直成立」。
    ///
    /// 现场：claim-store 这个 DaemonSet 每次滚动都被换掉，它一走 `cogneva_data_volume_used_bytes`
    /// 就没有了，而「这个 claim 没有用量读数」正是缺这条序列的形状——于是每次滚动都写一行
    /// 「12 个 claim 无人测量」，两分钟后新 Pod 扫完又自己结算。读者分不出这行和它名字所指的
    /// 条件。窗口把「产出者刚重启」挡在行外，而产出者真停了超过窗口，行照旧落下。
    #[test]
    fn a_hold_window_keeps_one_sample_of_a_disappearing_reading_out_of_the_table() {
        let mut rule = test_rule("claim_usage_unmeasured");
        rule.for_seconds = Some(120);
        let mut holding: HashMap<String, Instant> = HashMap::new();
        let t0 = Instant::now();

        // 第一拍：条件成立，但只成立了零秒。
        assert!(!hold_elapsed(
            &rule,
            "claim_usage_unmeasured:",
            t0,
            &mut holding
        ));
        // 窗口内再看一拍，仍然只是「一直成立着」。
        assert!(!hold_elapsed(
            &rule,
            "claim_usage_unmeasured:",
            t0 + Duration::from_secs(60),
            &mut holding
        ));
        // 满窗口之后才是证据。
        assert!(hold_elapsed(
            &rule,
            "claim_usage_unmeasured:",
            t0 + Duration::from_secs(121),
            &mut holding
        ));
    }

    /// 没有写窗口的规则行为不变：第一拍就落行（大多数读数不会自己消失）。
    #[test]
    fn a_rule_without_a_hold_window_raises_on_its_first_tick() {
        let rule = test_rule("pod_restarting_fast");
        let mut holding: HashMap<String, Instant> = HashMap::new();
        assert!(hold_elapsed(
            &rule,
            "pod_restarting_fast:pod=redis-0",
            Instant::now(),
            &mut holding
        ));
        assert!(holding.is_empty());
    }

    /// 条件中断，窗口从头再数；查询失败也清零——读不到不等于一直成立。
    #[test]
    fn a_hold_clock_restarts_when_the_condition_drops_or_the_query_fails() {
        let mut rule = test_rule("claim_store_node_unreported");
        rule.for_seconds = Some(600);
        let mut holding: HashMap<String, Instant> = HashMap::new();
        let t0 = Instant::now();
        let held = "claim_store_node_unreported:node=cogneva";
        assert!(!hold_elapsed(&rule, held, t0, &mut holding));

        // 这一拍里这条序列掉到阈值以下（或者干脆没了）：时钟作废。
        let below = SeriesSample {
            labels: BTreeMap::from([("node".to_string(), "cogneva".to_string())]),
            value: 0.0,
        };
        forget_held_except(&rule, std::slice::from_ref(&below), &mut holding);
        assert!(
            !holding.contains_key(held),
            "a condition that stopped holding keeps no clock"
        );

        // 再成立时从零开始：刚过原来的窗口也不算数。
        assert!(!hold_elapsed(
            &rule,
            held,
            t0 + Duration::from_secs(600),
            &mut holding
        ));
        assert!(!hold_elapsed(
            &rule,
            held,
            t0 + Duration::from_secs(1199),
            &mut holding
        ));

        // 查询失败把这一条规则的所有时钟一起清掉。
        forget_held(&rule, &mut holding);
        assert!(holding.is_empty());
    }

    /// Adopting and retiring must see every firing row of the rules they name.
    ///
    /// Both read through `list_active(1000)`, which orders by `fired_at DESC`
    /// and stops. Past a thousand open rows that is a sample, and a sampled row
    /// is indistinguishable from a row that does not exist: it is not adopted,
    /// so after the restart nothing remembers it, and it is not settled, so it
    /// stays `firing` with `resolved_at` empty and `last_seen_at` frozen --
    /// exactly the reading a continuing incident produces. Nothing else in the
    /// process knows the key, so nothing else will ever close it.
    ///
    /// The rows that fall out are the oldest ones, which is also what a real
    /// table looks like: after a restart everything that was already firing is
    /// older than whatever is firing now.
    #[tokio::test]
    #[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
    async fn adoption_and_retirement_reach_rows_past_the_old_read_limit() {
        use std::str::FromStr;

        use sqlx::postgres::PgConnectOptions;
        use sqlx::{Connection, PgConnection, PgPool};

        let url = std::env::var("COGNEVA_TEST_DATABASE_URL")
            .expect("set COGNEVA_TEST_DATABASE_URL to a throwaway database");
        let schema = format!("alert_adopt_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&url)
            .await
            .expect("the throwaway database is reachable");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&mut admin)
            .await
            .expect("the probe schema is created");
        let options = PgConnectOptions::from_str(&url)
            .expect("the test database URL parses")
            .options([("search_path", schema.as_str())]);
        let pool = PgPool::connect_with(options)
            .await
            .expect("the probe schema is reachable");
        let store = PostgresAlertStore::new(pool.clone());
        store
            .init_schema()
            .await
            .expect("the alert DDL runs in one piece");

        // 1001 rows newer than the three that matter: any reader that takes the
        // newest N of the whole table loses exactly these.
        sqlx::query(
            "INSERT INTO alerts (id, rule, dedup_key, severity, state, message, labels, \
             fired_at, updated_at, last_seen_at) \
             SELECT 'noise-' || g, 'noise_rule', 'noise_rule:' || g, 'warning', 'firing', \
                    'noise', '{}'::jsonb, now() - (g || ' seconds')::interval, now(), now() \
             FROM generate_series(1, 1001) g",
        )
        .execute(&pool)
        .await
        .expect("the competing rows are written");
        sqlx::query(
            "INSERT INTO alerts (id, rule, dedup_key, severity, state, message, labels, \
             fired_at, updated_at, last_seen_at) VALUES \
             ('w-1', 'watched_rule', 'watched_rule:pod=a', 'warning', 'firing', 'm', \
              '{}'::jsonb, now() - interval '3 days', now(), now()), \
             ('r-1', 'retired_rule', 'retired_rule:pod=b', 'warning', 'firing', 'm', \
              '{}'::jsonb, now() - interval '3 days', now(), now()), \
             ('e-1', 'infra_watch_eval_failure', 'infra_watch_eval_failure:rule_a', \
              'warning', 'firing', 'm', '{}'::jsonb, now() - interval '3 days', now(), now())",
        )
        .execute(&pool)
        .await
        .expect("the watched rows are written");

        let mut config = test_config(3);
        config.rules = vec![test_rule("watched_rule")];
        config.retired_rules = vec!["retired_rule".to_string()];

        let mut known_firing = HashSet::new();
        let mut eval_failure_firing = HashSet::new();
        adopt_active_alerts(&store, &config, &mut known_firing, &mut eval_failure_firing).await;
        assert!(
            known_firing.contains("watched_rule:pod=a"),
            "a row older than a thousand newer ones is still this watcher's \
             to close; skipping it leaves it firing for good"
        );
        assert!(
            eval_failure_firing.contains("infra_watch_eval_failure:rule_a"),
            "the eval-failure latch has to survive a restart the same way"
        );

        let outlets = InfraWatchOutlets {
            store: Some(Arc::new(PostgresAlertStore::new(pool.clone()))),
            notifier: None,
            metrics: None,
        };
        assert!(
            retire_rules(&config, &outlets).await,
            "the retirement pass has to reach the store"
        );
        let resolved: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM alerts \
             WHERE dedup_key = 'retired_rule:pod=b' AND state = 'resolved'",
        )
        .fetch_one(&pool)
        .await
        .expect("the retired row is read back");
        assert_eq!(
            resolved, 1,
            "a retired rule's row older than the read limit would otherwise \
             never be settled"
        );

        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&mut admin)
            .await
            .expect("the probe schema is dropped");
    }
}
