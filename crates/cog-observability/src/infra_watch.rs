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
//! tick so restarts neither duplicate nor strand alerts.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn};

use cog_core::{AlertEvent, AlertInstance, AlertState};

use crate::alert_store::{AlertTransition, NewAlert, PostgresAlertStore};
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
                let mut adopted = false;

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
                            tick(&config, &outlets, &http, &mut known_firing, &mut eval_failure_firing, &mut failure_streaks).await;
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
    match store.list_active(1000).await {
        Ok(records) => {
            let rule_names: HashSet<&str> = config.rules.iter().map(|r| r.name.as_str()).collect();
            for record in records {
                if record.rule == EVAL_FAILURE_RULE {
                    eval_failure_firing.insert(record.dedup_key);
                } else if rule_names.contains(record.rule.as_str()) {
                    known_firing.insert(record.dedup_key);
                }
            }
        }
        Err(e) => warn!(error = %e, "infra watch: adopting active alerts failed"),
    }
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
) {
    for rule in &config.rules {
        let series = match query_prometheus(http, &config.prometheus_url, &rule.promql).await {
            Ok(series) => series,
            Err(e) => {
                // A failed query must NOT resolve anything: absence of
                // evidence is not evidence of health. No plan is built for
                // this rule, so its previously-firing rows survive this tick
                // untouched.
                warn!(rule = %rule.name, error = %e, "infra watch: rule query failed");
                let streak = failure_streaks.entry(rule.name.clone()).or_insert(0);
                *streak = streak.saturating_add(1);
                let key = format!("{EVAL_FAILURE_RULE}:{}", rule.name);
                let latched = eval_failure_firing.contains(&key);
                if let Some(plan) =
                    eval_failure_plan(config.eval_failure_alert_after, *streak, latched)
                {
                    if plan.sight {
                        sight_eval_failure(&rule.name, *streak, &e, &key, outlets).await;
                    }
                    if plan.notify {
                        eval_failure_firing.insert(key.clone());
                        notify_eval_failure(&rule.name, *streak, &e, outlets).await;
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
        if eval_failure_firing.remove(&eval_key) {
            resolve(&eval_key, outlets).await;
        }

        let plan = plan_rule(rule, &series, known_firing);
        for &i in &plan.report {
            let key = dedup_key(&rule.name, &series[i].labels);
            known_firing.insert(key.clone());
            report_sighting(rule, &series[i], &key, outlets).await;
        }
        for key in &plan.close {
            resolve(key, outlets).await;
            known_firing.remove(key);
        }
    }
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
fn alert_labels(sample: &SeriesSample, message: &str) -> HashMap<String, String> {
    let mut labels: HashMap<String, String> = identity_labels(&sample.labels).into_iter().collect();
    labels.insert("message".into(), message.to_string());
    labels.insert("source".into(), "infra_watch".into());
    labels
}

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
    let labels = alert_labels(sample, &message);

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
                notify(rule, sample, &labels, true, outlets).await;
            }
            Ok(_) => {}
            Err(e) => warn!(rule = %rule.name, error = %e, "infra alert persist failed"),
        }
    } else {
        notify(rule, sample, &labels, true, outlets).await;
    }
}

/// The text both audiences of a self-alert get.
fn eval_failure_text(rule_name: &str, streak: u32, error: &str) -> String {
    format!("infra watch rule \"{rule_name}\" query failed {streak} times in a row: {error}")
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
    streak: u32,
    error: &str,
    key: &str,
    outlets: &InfraWatchOutlets,
) {
    let Some(store) = &outlets.store else { return };
    let message = eval_failure_text(rule_name, streak, error);
    let labels = HashMap::from([
        ("watched_rule".to_string(), rule_name.to_string()),
        ("message".to_string(), message.clone()),
        ("source".to_string(), "infra_watch".to_string()),
    ]);
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
    streak: u32,
    error: &str,
    outlets: &InfraWatchOutlets,
) {
    let Some(notifier) = &outlets.notifier else {
        return;
    };
    let message = eval_failure_text(rule_name, streak, error);
    let labels = HashMap::from([
        ("watched_rule".to_string(), rule_name.to_string()),
        ("message".to_string(), message),
        ("source".to_string(), "infra_watch".to_string()),
    ]);
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
    notifier.notify(&[AlertEvent::Firing(inst)]).await;
}

/// Close one alert row and notify the resolution.
async fn resolve(key: &str, outlets: &InfraWatchOutlets) {
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
                let labels = HashMap::from([("source".to_string(), "infra_watch".to_string())]);
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
                if let Some(notifier) = &outlets.notifier {
                    notifier.notify(&[AlertEvent::Resolved(inst)]).await;
                }
            }
            Ok(_) => {}
            Err(e) => warn!(key = %key, error = %e, "infra alert resolve failed"),
        }
    }
}

async fn notify(
    rule: &crate::config::InfraRule,
    sample: &SeriesSample,
    labels: &HashMap<String, String>,
    firing: bool,
    outlets: &InfraWatchOutlets,
) {
    let Some(notifier) = &outlets.notifier else {
        return;
    };
    let now = Utc::now();
    let inst = AlertInstance {
        rule_name: rule.name.clone(),
        labels: labels.clone(),
        state: if firing {
            AlertState::Firing
        } else {
            AlertState::Resolved
        },
        severity: rule.severity,
        value: sample.value,
        starts_at: now,
        ends_at: None,
        updated_at: now,
    };
    notifier.notify(&[AlertEvent::Firing(inst)]).await;
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
        let labels = alert_labels(&s, "backlog aged out");

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
            labels.get("message").map(String::as_str),
            Some("backlog aged out")
        );
        assert_eq!(
            labels.get("source").map(String::as_str),
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
        let mut declared: BTreeSet<String> = BTreeSet::new();
        let mut files = 0;
        for entry in walk_rs_files(&root.join("crates")) {
            let text = std::fs::read_to_string(&entry).expect("read crate source");
            files += 1;
            for rest in text.split(".with_label(\"").skip(1) {
                if let Some(name) = rest.split('"').next() {
                    declared.insert(name.to_string());
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
        }
    }

    fn test_config(threshold: u32) -> InfraWatchConfig {
        InfraWatchConfig {
            enabled: true,
            prometheus_url: "http://prom:9090".to_string(),
            poll_interval_secs: 60,
            eval_failure_alert_after: threshold,
            rules: vec![test_rule("rule_a")],
        }
    }

    #[tokio::test]
    async fn eval_failures_raise_self_alert_after_threshold_and_resolve_on_success() {
        let config = test_config(2);
        let outlets = InfraWatchOutlets {
            store: None,
            notifier: None,
        };
        let http: Arc<dyn cog_core::HttpClient> = Arc::new(FlakyHttp::default());
        let mut known_firing = HashSet::new();
        let mut eval_firing = HashSet::new();
        let mut streaks = HashMap::new();
        let key = format!("{EVAL_FAILURE_RULE}:rule_a");

        // First failure: below threshold, no self-alert.
        tick(
            &config,
            &outlets,
            &http,
            &mut known_firing,
            &mut eval_firing,
            &mut streaks,
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
        };
        let http: Arc<dyn cog_core::HttpClient> = Arc::new(FlakyHttp::default());
        let mut known_firing = HashSet::new();
        let mut eval_firing = HashSet::new();
        let mut streaks = HashMap::new();
        for _ in 0..5 {
            tick(
                &config,
                &outlets,
                &http,
                &mut known_firing,
                &mut eval_firing,
                &mut streaks,
            )
            .await;
        }
        assert!(eval_firing.is_empty());
        assert_eq!(streaks.get("rule_a"), Some(&5));
    }
}
