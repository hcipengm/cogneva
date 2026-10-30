//! The six readings that answer "is anything about to break".
//!
//! Each one is a decision that has to be made now — a disk that will not fit the
//! next build, a database whose oldest transaction has stopped moving, an
//! ingress returning 5xx, a vector store that got slower, a dependency that is
//! no longer reachable. The WebUI reads them here rather than from Grafana
//! because the value is what an operator acts on; Grafana is where they go to
//! ask the follow-up question, and every reading carries the panel it belongs to.
//!
//! Every reading names the series it reads and the file that produces it. A
//! reading whose series nobody produces is not "zero": it is unanswerable, and
//! the shape below keeps that case distinct from a real zero.

use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadingSpec {
    pub id: &'static str,
    pub title: &'static str,
    /// PromQL. One scalar per reading: a reading that needs two numbers is two
    /// readings, because a card can only show one verdict.
    pub query: &'static str,
    pub unit: &'static str,
    /// Thresholds, read according to `higher_is_worse`. `None` is "this reading
    /// has no threshold" — a count that is only meaningful as a trend must not
    /// borrow a number somebody invented, or it will start firing on its own.
    pub warn: Option<f64>,
    pub crit: Option<f64>,
    pub higher_is_worse: bool,
    /// The series this reads, spelled the way it appears in Prometheus. Written
    /// out so a reader can paste it into the Grafana explore view.
    pub series: &'static str,
    /// The file in this repository that produces it. Foreign series name their
    /// owner instead ("postgres-exporter", "traefik"), because that is who has
    /// to be looked at when the series disappears.
    pub produced_by: &'static str,
    /// The Grafana dashboard uid this reading deep-links to. `None` means the
    /// reading is real but has no panel: the WebUI says so rather than showing
    /// a link that goes nowhere.
    pub dashboard_uid: Option<&'static str>,
}

/// Disk headroom, as a fraction of the filesystem the root lives on.
pub const DISK_FREE: ReadingSpec = ReadingSpec {
    id: "disk_free_ratio",
    title: "根盘剩余占比",
    query: r#"min(1 - (node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay|squashfs"} / node_filesystem_size_bytes{mountpoint="/",fstype!~"tmpfs|overlay|squashfs"}))"#,
    unit: "%",
    warn: Some(0.15),
    crit: Some(0.07),
    higher_is_worse: false,
    series: "node_filesystem_avail_bytes / node_filesystem_size_bytes",
    produced_by: "node-exporter",
    dashboard_uid: Some("cogneva-storage"),
};

/// How much of postgres' connection ceiling is in use.
pub const PG_CONNECTIONS: ReadingSpec = ReadingSpec {
    id: "pg_connections",
    title: "数据库连接数",
    query: r#"sum(pg_stat_database_numbackends{datname="cogneva"})"#,
    unit: "条",
    warn: Some(80.0),
    crit: Some(110.0),
    higher_is_worse: true,
    series: "pg_stat_database_numbackends",
    produced_by: "postgres-exporter（数据库 Pod 内的边车）",
    dashboard_uid: Some("cogneva-postgres"),
};

/// The oldest transaction still open. A number that only grows means something
/// is holding a snapshot and vacuum cannot reclaim behind it.
pub const PG_OLDEST_TX: ReadingSpec = ReadingSpec {
    id: "pg_oldest_tx_seconds",
    title: "最老事务年龄",
    query: r#"max(pg_stat_activity_max_tx_duration{datname="cogneva"})"#,
    unit: "秒",
    warn: Some(300.0),
    crit: Some(1800.0),
    higher_is_worse: true,
    series: "pg_stat_activity_max_tx_duration",
    produced_by: "postgres-exporter（数据库 Pod 内的边车）",
    dashboard_uid: Some("cogneva-postgres"),
};

/// Vectors held by the store. Read as a trend, not a limit: a jump means an
/// ingest nobody announced.
pub const QDRANT_VECTORS: ReadingSpec = ReadingSpec {
    id: "qdrant_vectors",
    title: "向量总数",
    query: "sum(collections_vector_total)",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "collections_vector_total",
    produced_by: "qdrant",
    dashboard_uid: Some("cogneva-storage"),
};

/// Ingress 5xx per second. The ratio would be nicer, but a rate of zero when
/// there is no traffic and a rate of zero when everything is fine are the same
/// number here; the panel beside it carries the request rate for that reason.
pub const INGRESS_5XX: ReadingSpec = ReadingSpec {
    id: "ingress_5xx_rate",
    title: "入口 5xx 速率",
    query: r#"sum(rate(traefik_service_requests_total{code=~"5.."}[5m])) or sum(rate(nginx_ingress_controller_requests{status=~"5.."}[5m]))"#,
    unit: "/s",
    warn: Some(0.1),
    crit: Some(1.0),
    higher_is_worse: true,
    series: "traefik_service_requests_total{code=~\"5..\"}",
    produced_by: "traefik（k3s 形态）/ ingress-nginx（标准 k8s 形态）",
    dashboard_uid: Some("cogneva-reachability"),
};

/// Probe targets that are currently failing, including the certificate check.
pub const UNREACHABLE: ReadingSpec = ReadingSpec {
    id: "unreachable_targets",
    title: "不通的目标数",
    query: "sum(probe_success == 0)",
    unit: "个",
    warn: Some(1.0),
    crit: Some(2.0),
    higher_is_worse: true,
    series: "probe_success",
    produced_by: "blackbox-exporter（`19-blackbox-exporter.yaml` + 可达性值域文件）",
    dashboard_uid: Some("cogneva-reachability"),
};

pub const READINGS: [&ReadingSpec; 6] = [
    &DISK_FREE,
    &PG_CONNECTIONS,
    &PG_OLDEST_TX,
    &QDRANT_VECTORS,
    &INGRESS_5XX,
    &UNREACHABLE,
];

/// The word a reading carries about its own value.
///
/// The board colours a row by this word and prints a Chinese name beside it
/// through a map keyed by the same word. Both of those fall back when the word
/// is not one they know — the neutral colour, the word itself — so a verdict
/// spelled somewhere else, or added here without telling the board, would show
/// up as a value nobody acts on rather than as a disagreement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadingVerdict {
    /// Within the warning threshold, or below every threshold this reading has.
    Ok,
    /// Past `warn`, not yet past `crit`.
    Warning,
    /// Past `crit`.
    Critical,
    /// No value: no series, no answer, no threshold to judge by. Kept apart
    /// from `Ok` on purpose — one is a quiet system, the other is a blind spot.
    Unknown,
}

impl ReadingVerdict {
    /// Every word this vocabulary has, for readers that must cover all of them
    /// rather than listing the variants a second time.
    pub const ALL: [ReadingVerdict; 4] = [
        ReadingVerdict::Ok,
        ReadingVerdict::Warning,
        ReadingVerdict::Critical,
        ReadingVerdict::Unknown,
    ];

    /// The word this verdict travels as, and the key the board's maps use.
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadingVerdict::Ok => "ok",
            ReadingVerdict::Warning => "warning",
            ReadingVerdict::Critical => "critical",
            ReadingVerdict::Unknown => "unknown",
        }
    }

    /// Read a word back. The wire is the only place these words are not typed.
    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|verdict| verdict.as_str().eq_ignore_ascii_case(word))
    }
}

/// Serialised through [`ReadingVerdict::as_str`] rather than a per-variant
/// `#[serde(rename)]`, which would be a second place the word is written.
impl serde::Serialize for ReadingVerdict {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Reading {
    #[serde(flatten)]
    pub spec: ReadingSpec,
    /// `None` is "no answer", which is not the same as zero. A missing series
    /// and a series reading zero lead to different actions — one is a broken
    /// exporter, the other is a quiet system.
    pub value: Option<f64>,
    pub verdict: ReadingVerdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_because: Option<String>,
}

/// Judge one value against its spec.
pub fn verdict_of(spec: &ReadingSpec, value: Option<f64>) -> ReadingVerdict {
    let Some(value) = value else {
        return ReadingVerdict::Unknown;
    };
    let at_or_past = |threshold: Option<f64>| match threshold {
        Some(threshold) if spec.higher_is_worse => value >= threshold,
        Some(threshold) => value <= threshold,
        // 没有阈值的读数只能报值，不能报警：编一个数出来，它就会在自己选的时刻响。
        None => false,
    };
    if at_or_past(spec.crit) {
        ReadingVerdict::Critical
    } else if at_or_past(spec.warn) {
        ReadingVerdict::Warning
    } else {
        ReadingVerdict::Ok
    }
}

/// Pull the single scalar out of a Prometheus `/api/v1/query` answer.
///
/// An empty result and a malformed body both mean "no answer"; neither means
/// zero. Prometheus answers `200` with an empty `result` when nothing matches,
/// so status code is not a verdict either.
pub fn parse_scalar(body: &serde_json::Value) -> Option<f64> {
    let result = body.get("data")?.get("result")?.as_array()?;
    let first = result.first()?;
    let value = first.get("value")?.as_array()?;
    let raw = value.get(1)?.as_str()?;
    raw.parse::<f64>().ok()
}

/// Where Prometheus is, as the deployment declares it.
///
/// The chart fills this from the same values leaf that fills the config's
/// `infra_watch.prometheus_url`, because both ask the same Prometheus: the rule
/// evaluator and this board must not be the two readers of one fact that can
/// disagree.
///
/// Nothing declared is an error, not a default. A guessed in-cluster address is
/// the one kind of wrong answer this surface cannot show: a query against a
/// closed port and a query that matched no series both come back as "no
/// reading", so a guess that moved with the release reads exactly like a quiet
/// week.
pub fn prometheus_base() -> Result<String, String> {
    let declared = std::env::var("COGNEVA_PROMETHEUS_URL").ok();
    prometheus_base_from(declared.as_deref())
}

/// [`prometheus_base`] with the declaration handed in, so the rule about what
/// counts as declared can be read without an environment to set.
fn prometheus_base_from(declared: Option<&str>) -> Result<String, String> {
    declared
        .map(str::trim)
        .map(|s| s.trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "没有声明 Prometheus 地址（COGNEVA_PROMETHEUS_URL）：读数面不猜集群内的地址，\
             猜错与「这条序列没有样本」在读数面上同形"
                .to_string()
        })
}

/// The client these queries go through.
///
/// The process-wide one the assembly layer injected, not one built here: this
/// crate does not own the implementation — timeouts, pool and proxy have one
/// definition and it lives where the client is constructed. A second client
/// built inside a handler would carry its own numbers and be discovered only by
/// a query that hangs in one place and not another. When nothing was injected
/// the answer says so instead of quietly falling back, because a self-built
/// client reads exactly like the configured one from here.
fn query_client() -> Result<std::sync::Arc<dyn cog_core::HttpClient>, String> {
    crate::contribution_admin::http_client()
}

/// The deadline for one query. A reading that cannot answer in five seconds is
/// already the answer; a failed query is reported as "no reading", never as 0.
// cap: bound — 一次查询的等待上限；超时报「没有读数」，绝不报 0
const QUERY_TIMEOUT_SECS: u64 = 5;

/// Ask Prometheus for one query.
async fn ask(
    client: &std::sync::Arc<dyn cog_core::HttpClient>,
    base: &str,
    query: &str,
) -> Result<Option<f64>, String> {
    let url = format!("{base}/api/v1/query?query={}", percent_encode(query));
    let request = cog_core::HttpRequest::get(url).timeout(QUERY_TIMEOUT_SECS);
    let response = client
        .execute(request)
        .await
        .map_err(|e| format!("prometheus unreachable: {e}"))?;
    if !response.is_success() {
        return Err(format!("prometheus answered {}", response.status));
    }
    let body: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|e| format!("prometheus response is not JSON: {e}"))?;
    Ok(parse_scalar(&body))
}

/// Percent-encode a PromQL expression for a query string.
///
/// PromQL is mostly punctuation — braces, quotes, `=~`, brackets — so a query
/// pasted into a URL unencoded arrives as something else entirely. Only the
/// unreserved set passes through; everything else becomes `%XX`.
fn percent_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() * 3);
    for byte in raw.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Every reading, with its verdict. Queries run concurrently: six serial round
/// trips at a 5s timeout each would let one slow series blank the whole board.
pub async fn observability_readings_handler() -> axum::response::Response {
    use axum::response::IntoResponse;
    use axum::Json;

    let base = match prometheus_base() {
        Ok(base) => base,
        Err(e) => {
            // 地址没声明与 Prometheus 不可达是两件事：一个要改清单，一个要查
            // 那台 Prometheus。混成一句「取不到读数」，运维会去查一台根本没被
            // 指到的机器。
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "prometheus_not_configured", "message": e})),
            )
                .into_response();
        }
    };
    let client = match query_client() {
        Ok(c) => c,
        Err(e) => {
            // 装配层没交进客户端：说出来，而不是临时造一个让「漏了装配」看起来像
            // 「装配好了」。
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "http_client_not_wired", "message": e})),
            )
                .into_response();
        }
    };

    let mut handles = Vec::with_capacity(READINGS.len());
    for spec in READINGS {
        let client = client.clone();
        let base = base.clone();
        handles.push(tokio::spawn(async move {
            ask(&client, &base, spec.query).await
        }));
    }
    let mut by_id: HashMap<&'static str, Result<Option<f64>, String>> = HashMap::new();
    for (spec, handle) in READINGS.iter().zip(handles) {
        let outcome = handle
            .await
            .unwrap_or_else(|e| Err(format!("query task failed: {e}")));
        by_id.insert(spec.id, outcome);
    }

    let readings: Vec<Reading> = READINGS
        .iter()
        .map(|spec| {
            let (value, why) = match by_id.remove(spec.id) {
                Some(Ok(Some(v))) => (Some(v), None),
                Some(Ok(None)) => (
                    None,
                    Some(format!(
                        "没有序列：`{}` 当前一条样本都没有（产出方：{}）",
                        spec.series, spec.produced_by
                    )),
                ),
                Some(Err(e)) => (None, Some(e)),
                None => (None, Some("查询没有回来".to_string())),
            };
            Reading {
                spec: (*spec).clone(),
                value,
                verdict: verdict_of(spec, value),
                unavailable_because: why,
            }
        })
        .collect();

    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({
            "prometheus": base,
            "readings": readings,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_undeclared_prometheus_is_an_error_not_an_address() {
        // 猜一个集群内地址的坏处不是「猜错」本身，是猜错后的症状与「这条序列
        // 没有样本」同形：面板上两格都写着没有读数，一种要改清单、一种要查
        // Prometheus。
        assert!(prometheus_base_from(None).is_err());
        assert!(prometheus_base_from(Some("")).is_err());
        assert!(prometheus_base_from(Some("   ")).is_err());
        // 声明了就用它，只做去空白的收尾（末尾斜杠会拼出 `//api/v1/query`）。
        assert_eq!(
            prometheus_base_from(Some(" http://prom.monitoring:9090/ ")).unwrap(),
            "http://prom.monitoring:9090"
        );
    }

    #[test]
    fn an_empty_prometheus_answer_is_not_zero() {
        // Prometheus 的 200 + 空 result 是"没有这条序列"，读成 0 会把一个坏掉的
        // exporter 显示成一切安静。
        assert_eq!(
            parse_scalar(&serde_json::json!({"data": {"result": []}})),
            None
        );
        assert_eq!(parse_scalar(&serde_json::json!({"data": {}})), None);
        assert_eq!(
            parse_scalar(&serde_json::json!({
                "data": {"result": [{"value": [1792936073, "0.930000"]}]}
            })),
            Some(0.93)
        );
    }

    #[test]
    fn a_reading_with_no_value_is_unknown_rather_than_ok() {
        assert_eq!(verdict_of(&DISK_FREE, None), ReadingVerdict::Unknown);
        // 0 是一个真实的读数：剩余 0% 是 critical，不是 unknown。
        assert_eq!(verdict_of(&DISK_FREE, Some(0.0)), ReadingVerdict::Critical);
    }

    #[test]
    fn thresholds_point_the_way_the_reading_gets_worse() {
        // 剩余量越低越坏。
        assert_eq!(verdict_of(&DISK_FREE, Some(0.5)), ReadingVerdict::Ok);
        assert_eq!(verdict_of(&DISK_FREE, Some(0.10)), ReadingVerdict::Warning);
        assert_eq!(verdict_of(&DISK_FREE, Some(0.05)), ReadingVerdict::Critical);
        // 连接数越高越坏——同一组阈值、相反的方向。
        assert_eq!(verdict_of(&PG_CONNECTIONS, Some(10.0)), ReadingVerdict::Ok);
        assert_eq!(
            verdict_of(&PG_CONNECTIONS, Some(90.0)),
            ReadingVerdict::Warning
        );
        assert_eq!(
            verdict_of(&PG_CONNECTIONS, Some(120.0)),
            ReadingVerdict::Critical
        );
    }

    #[test]
    fn a_reading_without_a_threshold_reports_a_value_but_never_fires() {
        // 向量总数只有趋势意义：给它编一个上限，它就会在没人约定的时刻响。
        assert_eq!(
            verdict_of(&QDRANT_VECTORS, Some(9.9e12)),
            ReadingVerdict::Ok
        );
        assert_eq!(verdict_of(&QDRANT_VECTORS, None), ReadingVerdict::Unknown);
    }

    /// Every declared verdict travels as its own word, and no two share one.
    ///
    /// The board looks each word up in a colour map and a Chinese-name map; a
    /// duplicate spelling would make one of the four unreachable on screen
    /// while this side still counts it as covered.
    #[test]
    fn every_declared_verdict_reads_back_as_itself() {
        let mut words = std::collections::BTreeSet::new();
        for verdict in ReadingVerdict::ALL {
            assert!(
                words.insert(verdict.as_str()),
                "two variants share the word {:?}; the board's maps would only ever reach one",
                verdict.as_str()
            );
            assert_eq!(ReadingVerdict::parse(verdict.as_str()), Some(verdict));
            assert_eq!(
                serde_json::to_string(&verdict).unwrap(),
                format!("\"{}\"", verdict.as_str())
            );
        }
        assert_eq!(ReadingVerdict::parse("OK"), Some(ReadingVerdict::Ok));
        assert_eq!(ReadingVerdict::parse("fine"), None);
    }

    #[test]
    fn a_threshold_is_serialised_as_null_not_as_a_made_up_number() {
        // serde_json 把非有限的 f64 写成 null；接口里绝不能出现一个读的人以为
        // 有意义的阈值。
        let wire = serde_json::to_value(Reading {
            spec: QDRANT_VECTORS.clone(),
            value: None,
            verdict: ReadingVerdict::Unknown,
            unavailable_because: None,
        })
        .unwrap();
        assert!(wire["warn"].is_null());
        assert!(wire["crit"].is_null());
    }

    #[test]
    fn a_promql_expression_survives_being_put_in_a_url() {
        // 大括号、引号、`=~` 原样塞进查询串会被读成别的表达式；未保留字符以外的
        // 一律转义，运算符本身必须原样到达 Prometheus。
        assert_eq!(percent_encode("up"), "up");
        assert_eq!(
            percent_encode(r#"sum(x{a="b"})"#),
            "sum%28x%7Ba%3D%22b%22%7D%29"
        );
        assert_eq!(percent_encode(r#"a=~"5..""#), "a%3D~%225..%22");
        // 空格是 PromQL 的分隔符，绝不能变成 `+`。
        assert_eq!(percent_encode("a b"), "a%20b");
    }

    #[test]
    fn every_reading_names_a_producer_and_a_series() {
        // 一条读数说不出谁产出它，就等于没有人对它的消失负责。
        for spec in READINGS {
            assert!(!spec.series.is_empty(), "{} has no series", spec.id);
            assert!(!spec.produced_by.is_empty(), "{} has no producer", spec.id);
            assert!(!spec.query.is_empty(), "{} has no query", spec.id);
        }
    }
}
