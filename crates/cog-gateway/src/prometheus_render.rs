use std::collections::HashMap;

use cog_core::MetricSample;

/// Quantiles published for every summary. A summary's quantile set is part of
/// its definition rather than a tunable: consumers read specific members of
/// this set, so the set itself is the contract. p99 is the latency-regression
/// signal the canary gate watches.
const SUMMARY_QUANTILES: [f64; 3] = [0.5, 0.9, 0.99];

/// Render a set of counter samples in Prometheus text format.
/// Samples are aggregated by their label set and summed.
pub fn render_counters(name: &str, help: &str, samples: &[MetricSample]) -> String {
    if samples.is_empty() {
        return String::new();
    }

    let mut out = format!("# HELP {name} {help}\n# TYPE {name} counter\n");

    // Aggregate by label set
    let mut aggregated: HashMap<String, f64> = HashMap::new();
    for s in samples {
        let key = format_labels(&s.labels);
        *aggregated.entry(key).or_insert(0.0) += s.value;
    }

    let mut keys: Vec<String> = aggregated.keys().cloned().collect();
    keys.sort_unstable();
    for labels in keys {
        let value = aggregated[&labels];
        out.push_str(&format!("{name}{{{labels}}} {value}\n"));
    }

    out
}

/// Render a set of histogram samples in Prometheus text format as a summary.
/// Produces one series per quantile plus `_count` and `_sum` per label set.
/// A summary without its quantile series is unreadable as a latency signal:
/// consumers can only derive a mean from `_sum`/`_count`, so a tail-latency
/// regression disappears into it.
pub fn render_histograms(name: &str, help: &str, samples: &[MetricSample]) -> String {
    if samples.is_empty() {
        return String::new();
    }

    let mut out = format!("# HELP {name} {help}\n# TYPE {name} summary\n");

    // Aggregate by label set. Observations are kept so quantiles can be
    // computed; the caller only ever passes one scrape window, so this is
    // bounded by the window rather than by the metric's lifetime.
    let mut groups: HashMap<String, (HashMap<String, String>, Vec<f64>)> = HashMap::new();
    for s in samples {
        let key = format_labels(&s.labels);
        groups
            .entry(key)
            .or_insert_with(|| (s.labels.clone(), Vec::new()))
            .1
            .push(s.value);
    }

    let mut keys: Vec<String> = groups.keys().cloned().collect();
    keys.sort_unstable();

    for key in keys {
        let (labels, mut observations) = groups.remove(&key).expect("key came from groups");
        observations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        for q in SUMMARY_QUANTILES {
            let mut with_quantile = labels.clone();
            with_quantile.insert("quantile".into(), format!("{q}"));
            out.push_str(&format!(
                "{name}{{{}}} {}\n",
                format_labels(&with_quantile),
                quantile(&observations, q)
            ));
        }

        let count = observations.len();
        let sum: f64 = observations.iter().sum();
        out.push_str(&format!("{name}_count{{{key}}} {count}\n"));
        out.push_str(&format!("{name}_sum{{{key}}} {sum}\n"));
    }

    out
}

/// Render a set of gauge samples in Prometheus text format.
///
/// A gauge is a point-in-time value, so unlike a counter or a summary there is
/// nothing to aggregate: the newest sample per label set wins. Each value is
/// accompanied by the timestamp of the sample it came from, because the pass
/// that produces a gauge runs on a cadence of its own — minutes, for the ones
/// that come from a periodic scan — while this exposition is scraped far more
/// often. Without the timestamp a value left over from an earlier pass is
/// indistinguishable from one just refreshed, and "the producer stalled" reads
/// the same as "nothing to report".
pub fn render_gauges(name: &str, help: &str, samples: &[MetricSample]) -> String {
    if samples.is_empty() {
        return String::new();
    }

    let mut latest: HashMap<String, &MetricSample> = HashMap::new();
    for s in samples {
        let key = format_labels(&s.labels);
        match latest.get(&key) {
            Some(prev) if prev.timestamp >= s.timestamp => {}
            _ => {
                latest.insert(key, s);
            }
        }
    }

    let mut keys: Vec<String> = latest.keys().cloned().collect();
    keys.sort_unstable();

    let mut out = format!("# HELP {name} {help}\n# TYPE {name} gauge\n");
    for key in &keys {
        let s = latest[key];
        if key.is_empty() {
            out.push_str(&format!("{name} {}\n", s.value));
        } else {
            out.push_str(&format!("{name}{{{key}}} {}\n", s.value));
        }
    }

    out.push_str(&format!(
        "# HELP {name}_observed_timestamp_seconds Unix seconds the {name} value above was produced at\n\
         # TYPE {name}_observed_timestamp_seconds gauge\n"
    ));
    for key in &keys {
        let ts = latest[key].timestamp.timestamp();
        if key.is_empty() {
            out.push_str(&format!("{name}_observed_timestamp_seconds {ts}\n"));
        } else {
            out.push_str(&format!(
                "{name}_observed_timestamp_seconds{{{key}}} {ts}\n"
            ));
        }
    }

    out
}

/// Linear-interpolated quantile over ascending observations, matching the
/// interpolation PostgreSQL's `percentile_cont` uses so in-process and
/// in-database answers agree.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    match sorted.len() {
        0 => 0.0,
        1 => sorted[0],
        n => {
            let position = q * (n as f64 - 1.0);
            let lower = position.floor() as usize;
            let upper = position.ceil() as usize;
            if lower == upper {
                sorted[lower]
            } else {
                let weight = position - lower as f64;
                sorted[lower] * (1.0 - weight) + sorted[upper] * weight
            }
        }
    }
}

/// Render raw observable metrics (D5/D8 pulls) in Prometheus text format.
/// Metrics are grouped by name; names ending in `_total` render as counters,
/// everything else as gauges. One sample per distinct label set.
pub fn render_raw_metrics(metrics: &[cog_core::RawMetric]) -> String {
    if metrics.is_empty() {
        return String::new();
    }

    let mut by_name: HashMap<&str, Vec<&cog_core::RawMetric>> = HashMap::new();
    for m in metrics {
        by_name.entry(m.name.as_str()).or_default().push(m);
    }

    let mut names: Vec<&str> = by_name.keys().copied().collect();
    names.sort_unstable();

    let mut out = String::new();
    for name in names {
        let kind = if name.ends_with("_total") {
            "counter"
        } else {
            "gauge"
        };
        out.push_str(&format!("# TYPE {name} {kind}\n"));
        for m in &by_name[name] {
            let labels = format_labels(&m.labels);
            if labels.is_empty() {
                out.push_str(&format!("{name} {}\n", m.value));
            } else {
                out.push_str(&format!("{name}{{{labels}}} {}\n", m.value));
            }
        }
    }

    out
}

/// Render a label set in a canonical form. The order must be derived from the
/// label names, not from the map's iteration order: the same logical series
/// arrives as a fresh `HashMap` per sample, so an iteration-ordered rendering
/// splits one series into as many series as there are label permutations, each
/// carrying a fraction of the total. Downstream, those fragments look like
/// distinct series and every per-series aggregate is wrong.
fn format_labels(labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut names: Vec<&String> = labels.keys().collect();
    names.sort_unstable();
    let pairs: Vec<String> = names
        .into_iter()
        .map(|k| format!("{k}=\"{}\"", escape_label_value(&labels[k])))
        .collect();
    pairs.join(",")
}

fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(value: f64, labels: &[(&str, &str)]) -> MetricSample {
        MetricSample {
            timestamp: chrono::Utc::now(),
            value,
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// 同一个逻辑序列必须只渲染出一条：标签顺序若取自 map 的迭代顺序，
    /// 一个序列会碎成标签排列数那么多条，每条各拿一部分总量，
    /// 下游任何按序列聚合的结果都是错的。
    #[test]
    fn one_logical_series_is_not_fragmented_by_label_order() {
        let labels = [
            ("endpoint", "/api/v1/tasks"),
            ("method", "POST"),
            ("status", "201"),
            ("pad_a", "a"),
            ("pad_b", "b"),
            ("pad_c", "c"),
        ];
        let samples: Vec<MetricSample> = (0..16).map(|_| sample(1.0, &labels)).collect();

        let out = render_counters("http_requests_total", "help", &samples);
        let series: Vec<&str> = out.lines().filter(|l| !l.starts_with('#')).collect();
        assert_eq!(series.len(), 1, "one series rendered as {series:?}");
        assert!(series[0].ends_with(" 16"), "values must sum: {}", series[0]);
    }

    #[test]
    fn labels_render_in_canonical_name_order() {
        let labels: HashMap<String, String> = [
            ("status", "200"),
            ("endpoint", "/metrics"),
            ("method", "GET"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert_eq!(
            format_labels(&labels),
            "endpoint=\"/metrics\",method=\"GET\",status=\"200\""
        );
    }

    /// summary 必须发布分位线：只有 `_count`/`_sum` 的 summary 读不出尾部延迟，
    /// 金丝雀的 p99 回归判据会永远读到 0 而静默失效。
    #[test]
    fn summary_publishes_p99_quantile() {
        let samples: Vec<MetricSample> = (1..=100)
            .map(|i| sample(i as f64, &[("endpoint", "/api/v1/tasks")]))
            .collect();

        let out = render_histograms("http_request_duration_ms", "help", &samples);
        let p99_line = out
            .lines()
            .find(|l| l.starts_with("http_request_duration_ms{") && l.contains("quantile=\"0.99\""))
            .expect("p99 quantile series");
        let value: f64 = p99_line
            .split_whitespace()
            .last()
            .and_then(|v| v.parse().ok())
            .expect("series carries a value");
        assert!((value - 99.01).abs() < 1e-9, "{p99_line}");
        assert!(out.contains("http_request_duration_ms_count{endpoint=\"/api/v1/tasks\"} 100"));
        assert!(out.contains("# TYPE http_request_duration_ms summary\n"));
    }

    /// 一个 gauge 是一个时点值，不是一段窗口的和：同一序列在回看窗里有多个样本
    /// 时只渲染最新的那个，求和或求平均都会把「当前积压」变成另一个数。
    #[test]
    fn gauge_renders_the_newest_sample_not_a_sum() {
        let mut old = sample(7.0, &[]);
        old.timestamp = chrono::Utc::now() - chrono::Duration::minutes(30);
        let mut older = sample(5.0, &[]);
        older.timestamp = chrono::Utc::now() - chrono::Duration::minutes(50);
        let mut newest = sample(3.0, &[]);
        newest.timestamp = chrono::Utc::now() - chrono::Duration::minutes(1);

        let out = render_gauges("memory_unextracted_raw", "help", &[old, older, newest]);
        assert!(
            out.contains("\nmemory_unextracted_raw 3\n"),
            "newest sample must win: {out}"
        );
        assert!(!out.contains("memory_unextracted_raw 15"), "{out}");
        assert!(out.contains("# TYPE memory_unextracted_raw gauge\n"));
    }

    /// gauge 的产出节拍与抓取节拍不同，所以要连样本时间一起发出去：
    /// 少了它，一个停了很久的生产者与一个刚跑过的生产者读起来一模一样。
    #[test]
    fn gauge_carries_the_sample_timestamp() {
        let mut s = sample(4.0, &[]);
        let ts = chrono::Utc::now() - chrono::Duration::minutes(12);
        s.timestamp = ts;

        let out = render_gauges("memory_unextracted_raw", "help", &[s]);
        assert!(
            out.contains(&format!(
                "memory_unextracted_raw_observed_timestamp_seconds {}\n",
                ts.timestamp()
            )),
            "{out}"
        );
    }

    /// 没有样本就不渲染：一条只有 HELP 的 gauge 会让下游把「读不到」当成
    /// 「值为空」，而正文里出现空序列比序列缺席更难分辨。
    #[test]
    fn empty_gauge_renders_nothing() {
        assert!(render_gauges("memory_unextracted_raw", "help", &[]).is_empty());
    }

    #[test]
    fn render_raw_metrics_groups_by_name_and_labels() {
        let metrics = vec![
            cog_core::RawMetric::new("ralph_terminations_total", 2.0)
                .with_label("reason", "stagnated"),
            cog_core::RawMetric::new("collaboration_success_rate", 0.5),
        ];
        let out = render_raw_metrics(&metrics);
        assert!(out.contains("# TYPE ralph_terminations_total counter\n"));
        assert!(out.contains("ralph_terminations_total{reason=\"stagnated\"} 2\n"));
        assert!(out.contains("# TYPE collaboration_success_rate gauge\n"));
        assert!(out.contains("collaboration_success_rate 0.5\n"));
    }

    #[test]
    fn render_raw_metrics_empty_is_empty() {
        assert!(render_raw_metrics(&[]).is_empty());
    }
}
