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
const NON_IDENTITY_LABELS: &[&str] = &[
    "__name__",
    "job",
    "endpoint",
    "service",
    "container",
    "namespace",
    "prometheus",
];

/// Rule name for the watcher's self-alert: a rule whose query keeps failing
/// is itself an incident (dead Prometheus, blocked network path), raised
/// through the same persistent state machine as infrastructure alerts so
/// self-discovery sees the observation gap. Kept distinct from configured
/// rule names so the series-resolution pass never touches these rows.
pub const EVAL_FAILURE_RULE: &str = "infra_watch_eval_failure";

/// Outlets the watcher drives. Both are optional: with no store the watcher
/// still notifies, with no notifier it still persists, with neither it does
/// not run at all (the plugin decides).
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
    info!(
        rules = config.rules.len(),
        interval_secs = interval.as_secs(),
        url = %config.prometheus_url,
        "infra alert watcher started"
    );

    // dedup keys currently believed firing; adopted from the store on the
    // first tick so a restart resolves rows it can no longer observe instead
    // of stranding them. Eval-failure self-alerts live in their own set:
    // they are keyed by rule name, not series identity, and must survive the
    // series-resolution pass untouched.
    let mut known_firing: HashSet<String> = HashSet::new();
    let mut eval_failure_firing: HashSet<String> = HashSet::new();
    let mut failure_streaks: HashMap<String, u32> = HashMap::new();
    let mut adopted = false;

    let mut ticker = tokio::time::interval(interval);
    loop {
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

/// One evaluation pass over every configured rule.
async fn tick(
    config: &InfraWatchConfig,
    outlets: &InfraWatchOutlets,
    http: &Arc<dyn cog_core::HttpClient>,
    known_firing: &mut HashSet<String>,
    eval_failure_firing: &mut HashSet<String>,
    failure_streaks: &mut HashMap<String, u32>,
) {
    let mut true_keys: HashSet<String> = HashSet::new();
    let mut queried_prefixes: HashSet<String> = HashSet::new();
    for rule in &config.rules {
        let series = match query_prometheus(http, &config.prometheus_url, &rule.promql).await {
            Ok(series) => series,
            Err(e) => {
                // A failed query must NOT resolve anything: absence of
                // evidence is not evidence of health. The rule contributes
                // no prefix to `queried_prefixes`, so its previously-firing
                // rows survive this tick untouched.
                warn!(rule = %rule.name, error = %e, "infra watch: rule query failed");
                let streak = failure_streaks.entry(rule.name.clone()).or_insert(0);
                *streak = streak.saturating_add(1);
                if config.eval_failure_alert_after > 0 && *streak >= config.eval_failure_alert_after
                {
                    let key = format!("{EVAL_FAILURE_RULE}:{}", rule.name);
                    if eval_failure_firing.insert(key.clone()) {
                        fire_eval_failure(&rule.name, *streak, &e, &key, outlets).await;
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
        queried_prefixes.insert(format!("{}:", rule.name));
        for sample in &series {
            if !rule.condition.evaluate(sample.value) {
                continue;
            }
            let key = dedup_key(&rule.name, &sample.labels);
            true_keys.insert(key.clone());
            if known_firing.contains(&key) {
                continue;
            }
            known_firing.insert(key.clone());
            fire(rule, sample, &key, outlets).await;
        }
    }

    // Resolve previously-firing keys whose condition no longer holds or
    // whose series vanished — but only for rules that actually queried
    // successfully this tick.
    let stale: Vec<String> = known_firing
        .iter()
        .filter(|key| {
            !true_keys.contains(*key)
                && queried_prefixes.iter().any(|p| key.starts_with(p.as_str()))
        })
        .cloned()
        .collect();
    for key in stale {
        resolve(&key, outlets).await;
        known_firing.remove(&key);
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
        return Err(format!("prometheus returned {}", resp.status));
    }
    let body: serde_json::Value =
        serde_json::from_slice(&resp.body).map_err(|e| format!("invalid JSON: {e}"))?;
    parse_vector_response(&body)
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

/// Stable alert-instance identity: rule name + sorted identity labels.
fn dedup_key(rule_name: &str, labels: &BTreeMap<String, String>) -> String {
    let identity = labels
        .iter()
        .filter(|(k, _)| !NON_IDENTITY_LABELS.contains(&k.as_str()))
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{rule_name}:{identity}")
}

/// Raise one alert: persist the state transition, notify on the edge.
async fn fire(
    rule: &crate::config::InfraRule,
    sample: &SeriesSample,
    key: &str,
    outlets: &InfraWatchOutlets,
) {
    let message = rule
        .summary
        .replace("{value}", &format!("{:.2}", sample.value));
    let mut labels: HashMap<String, String> = sample
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    labels.insert("message".into(), message.clone());
    labels.insert("source".into(), "infra_watch".into());

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

/// Raise the watcher's self-alert for a rule whose query keeps failing.
/// Goes through the same persistent state machine as infrastructure alerts
/// so the observation gap becomes a signal self-discovery can consume.
async fn fire_eval_failure(
    rule_name: &str,
    streak: u32,
    error: &str,
    key: &str,
    outlets: &InfraWatchOutlets,
) {
    let message =
        format!("infra watch rule \"{rule_name}\" query failed {streak} times in a row: {error}");
    let labels = HashMap::from([
        ("watched_rule".to_string(), rule_name.to_string()),
        ("message".to_string(), message.clone()),
        ("source".to_string(), "infra_watch".to_string()),
    ]);
    if let Some(store) = &outlets.store {
        let alert = NewAlert {
            rule: EVAL_FAILURE_RULE.to_string(),
            dedup_key: key.to_string(),
            severity: cog_core::AlertSeverity::Warning.as_str().to_string(),
            message: message.clone(),
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
    if let Some(notifier) = &outlets.notifier {
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

#[cfg(test)]
mod tests {
    use super::*;

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
            ("pod".to_string(), "exporter-abc".to_string()),
        ]);
        assert_eq!(dedup_key("disk", &a), "disk:node=vm-1");
        // pod is an identity label (which pod is crashlooping matters)
        assert_eq!(dedup_key("crash", &b), "crash:node=vm-1,pod=exporter-abc");
    }

    #[test]
    fn urlencoding_escapes_promql() {
        assert_eq!(
            urlencoding("up{job=\"x\"} > 0"),
            "up%7Bjob%3D%22x%22%7D%20%3E%200"
        );
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
