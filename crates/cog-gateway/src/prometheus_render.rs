use std::collections::HashMap;

use cog_core::{HistogramTotals, MetricSample};

/// Render a set of counter samples in Prometheus text format.
/// Samples are aggregated by their label set and summed.
///
/// Each series is accompanied by the time of the newest observation behind it,
/// for the same reason a gauge carries its sample time: a cumulative counter
/// that has stopped growing looks identical whether its producer is running and
/// finding nothing to count, or has gone away. Recorded at whatever cadence the
/// producer's call sites happen to fire, read at whatever cadence the scrape
/// runs — the timestamp is what lets a reader tell a quiet counter from an
/// abandoned one instead of inferring it from a flat line.
pub fn render_counters(name: &str, help: &str, samples: &[MetricSample]) -> String {
    if samples.is_empty() {
        return String::new();
    }

    let mut out = format!("# HELP {name} {help}\n# TYPE {name} counter\n");

    // Aggregate by label set, keeping the newest observation time seen for it:
    // a total is a sum, but "when was this last fed" is a maximum, and summing
    // timestamps would answer a question nobody asked.
    let mut aggregated: HashMap<String, (f64, i64)> = HashMap::new();
    for s in samples {
        let key = format_labels(&s.labels);
        let entry = aggregated
            .entry(key)
            .or_insert((0.0, s.timestamp.timestamp()));
        entry.0 += s.value;
        entry.1 = entry.1.max(s.timestamp.timestamp());
    }

    let mut keys: Vec<String> = aggregated.keys().cloned().collect();
    keys.sort_unstable();
    for labels in &keys {
        let (value, _) = aggregated[labels];
        out.push_str(&format!("{name}{{{labels}}} {value}\n"));
    }

    let observed: Vec<(String, i64)> = keys
        .iter()
        .map(|labels| (labels.clone(), aggregated[labels].1))
        .collect();
    out.push_str(&render_observed_timestamps(name, &observed));

    out
}

/// Render cumulative histogram buckets in Prometheus text format.
///
/// Emits `_bucket{le=...}`, `_sum` and `_count` per label set, all cumulative
/// since recording began. Cumulative is what makes the series readable at all:
/// quantiles and rates come from differencing two scrapes, so a bucket count
/// that can fall — which is what a window over aging observations produces —
/// reads downstream as negative traffic, and `histogram_quantile` has no
/// monotonic series to interpolate.
///
/// The boundaries come from the backend that accumulated the counts, so it is
/// the accumulation that decides what a bucket means, not this renderer. Two
/// backends could in principle report different boundaries for one name; what
/// keeps them in step is that the scheme is a pure function of the name, not a
/// per-backend choice.
///
/// Each series carries the time of the last observation it accumulated, taken
/// from the same row the counts come from. An accumulation can say that it is
/// cumulative but never that it is current: a histogram whose producer went
/// away and one whose producer is merely quiet render as the same plausible
/// buckets, and only the timestamp separates them.
pub fn render_histograms(name: &str, help: &str, series: &[HistogramTotals]) -> String {
    if series.is_empty() {
        return String::new();
    }

    let mut out = format!("# HELP {name} {help}\n# TYPE {name} histogram\n");

    let mut ordered: Vec<&HistogramTotals> = series.iter().collect();
    ordered.sort_by_key(|s| format_labels(&s.labels));

    let mut observed: Vec<(String, i64)> = Vec::with_capacity(ordered.len());
    for totals in ordered {
        let key = format_labels(&totals.labels);
        for (bound, count) in totals.cumulative_buckets() {
            let mut with_bound = totals.labels.clone();
            with_bound.insert("le".into(), format_bound(bound));
            out.push_str(&format!(
                "{name}_bucket{{{}}} {count}\n",
                format_labels(&with_bound)
            ));
        }
        out.push_str(&format!("{name}_sum{{{key}}} {}\n", totals.sum));
        out.push_str(&format!("{name}_count{{{key}}} {}\n", totals.count));
        observed.push((key, totals.updated_at.timestamp()));
    }

    out.push_str(&render_observed_timestamps(name, &observed));

    out
}

/// The `le` label's value, in the form Prometheus parses.
///
/// `+Inf` has to be spelled exactly that way — it is the label every histogram
/// carries and the one `histogram_quantile` reads to know where the
/// observations end. Rust would render the bound as `inf`, which Prometheus
/// accepts as a float but which no query written against a standard histogram
/// matches.
fn format_bound(bound: f64) -> String {
    if bound.is_infinite() {
        "+Inf".to_string()
    } else {
        format!("{bound}")
    }
}

/// Render a set of gauge samples in Prometheus text format.
///
/// A gauge is a point-in-time value, so unlike a counter there is nothing to
/// aggregate: the newest sample per label set wins. Each value is accompanied by
/// the timestamp of the sample it came from, because the pass that produces a
/// gauge runs on a cadence of its own — minutes, for the ones that come from a
/// periodic scan — while this exposition is scraped far more often. Without the
/// timestamp a value left over from an earlier pass is indistinguishable from
/// one just refreshed, and "the producer stalled" reads the same as "nothing to
/// report".
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

    let observed: Vec<(String, i64)> = keys
        .iter()
        .map(|key| (key.clone(), latest[key].timestamp.timestamp()))
        .collect();
    out.push_str(&render_observed_timestamps(name, &observed));

    out
}

/// The `_observed_timestamp_seconds` family for a rendered series set.
///
/// One shape for all three kinds, because it answers one question: when did the
/// newest thing behind this series happen. A scrape cannot tell an abandoned
/// series from a quiet one without it — nothing in the value itself moves — and
/// a reader that has to guess between the two will read a dead metric as a live
/// one with nothing to report. The pairs arrive in the same order as the values
/// they belong to, so a series is never paired with another series' time.
fn render_observed_timestamps(name: &str, series: &[(String, i64)]) -> String {
    if series.is_empty() {
        return String::new();
    }

    let mut out = format!(
        "# HELP {name}_observed_timestamp_seconds Unix seconds the newest observation behind {name} was recorded at\n\
         # TYPE {name}_observed_timestamp_seconds gauge\n"
    );
    for (key, ts) in series {
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
        let series: Vec<&str> = out
            .lines()
            .filter(|l| !l.starts_with('#') && !l.contains("_observed_timestamp_seconds"))
            .collect();
        assert_eq!(series.len(), 1, "one series rendered as {series:?}");
        assert!(series[0].ends_with(" 16"), "values must sum: {}", series[0]);
        // 值只有一条，时间戳也只能有一条：两边一起数才不会出现「值合并了、
        // 时间戳还碎着」这种一半对一半错的形态。
        assert_eq!(
            out.lines()
                .filter(|l| l.contains("_observed_timestamp_seconds") && !l.starts_with('#'))
                .count(),
            1,
            "{out}"
        );
    }

    /// 一个计数器累加停了，可能是生产者还在跑但没什么可数的，也可能是生产者
    /// 没了；两者的曲线一模一样，只有最近一次观测的时间能分开它们。
    #[test]
    fn counter_carries_the_newest_observation_time() {
        let mut old = sample(3.0, &[("job", "a")]);
        old.timestamp = chrono::Utc::now() - chrono::Duration::minutes(40);
        let mut newer = sample(2.0, &[("job", "a")]);
        newer.timestamp = chrono::Utc::now() - chrono::Duration::minutes(5);
        let newest_ts = newer.timestamp.timestamp();

        let out = render_counters("http_requests_total", "help", &[old, newer]);
        assert!(out.contains("http_requests_total{job=\"a\"} 5\n"), "{out}");
        assert!(
            out.contains(&format!(
                "http_requests_total_observed_timestamp_seconds{{job=\"a\"}} {newest_ts}\n"
            )),
            "累加是求和，时间是取最新，不能把时间也加起来: {out}"
        );
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

    /// 延迟必须以**累积直方图**发布：分位数由下游用 `histogram_quantile` 从
    /// `le` 桶插值而来，只有 `_count`/`_sum` 的序列读不出尾部延迟。
    ///
    /// 桶是累积的，`+Inf` 桶要写成 Prometheus 认的那个字面量：它既是
    /// `histogram_quantile` 用来定位观测总数的桶，写错名字就等于没有它。
    #[test]
    fn histogram_publishes_cumulative_buckets_with_an_inf_bound() {
        let observed_at = chrono::Utc::now() - chrono::Duration::minutes(7);
        let series = vec![HistogramTotals {
            labels: [("endpoint".to_string(), "/api/v1/tasks".to_string())]
                .into_iter()
                .collect(),
            // 每条桶自带的是**桶内**观测数，不是累积数：10 + 20 + 15 + 溢出 5 = 50。
            buckets: vec![(1.0, 10), (2.0, 20), (4.0, 15)],
            overflow: 5,
            count: 50,
            sum: 123.0,
            updated_at: observed_at,
        }];

        let out = render_histograms("http_request_duration_ms", "help", &series);
        assert!(
            out.contains("# TYPE http_request_duration_ms histogram\n"),
            "{out}"
        );
        // 桶累积：le=2 的计数含 le=1 的那 10 个。
        assert!(
            out.contains("_bucket{endpoint=\"/api/v1/tasks\",le=\"2\"} 30\n"),
            "{out}"
        );
        assert!(
            out.contains("_bucket{endpoint=\"/api/v1/tasks\",le=\"+Inf\"} 50\n"),
            "{out}"
        );
        assert!(
            out.contains("_sum{endpoint=\"/api/v1/tasks\"} 123\n"),
            "{out}"
        );
        assert!(
            out.contains("_count{endpoint=\"/api/v1/tasks\"} 50\n"),
            "{out}"
        );
        // 累积量能说自己是累积的，却说不出自己是不是还活着：时间戳必须来自
        // 同一行累积数据，才能把「生产者没了」和「生产者只是安静」分开。
        assert!(
            out.contains(&format!(
                "http_request_duration_ms_observed_timestamp_seconds{{endpoint=\"/api/v1/tasks\"}} {}\n",
                observed_at.timestamp()
            )),
            "{out}"
        );
        // 只有 HELP/TYPE 头没有正文，读起来像「系统空闲」而不是「读不到」。
        assert!(render_histograms("http_request_duration_ms", "help", &[]).is_empty());
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
