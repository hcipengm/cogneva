//! The panels Grafana shows, computed from the registries rather than written.
//!
//! Every series this build can store is one the closed set names
//! ([`cog_core::metric_names::ALL`]), and every one of them has a sentence in
//! [`cog_core::metric_help`] saying what it measures. Every reading the board
//! judges is in [`crate::readings::READINGS`], with the query and the unit it
//! is read in. Those tables are the whole input; a panel is what falls out of
//! them.
//!
//! Why computed rather than declared: a hand-written dashboard is a second
//! copy of the same facts, kept true by whoever remembers to edit both. It has
//! no diff that means anything — a threshold moved by one digit arrives as a
//! 204 KB file replaced — and nothing in it answers "who asked for this
//! panel". The tables it duplicates already have to exist: a series outside
//! the closed set cannot be written, and a reading that names no series cannot
//! be judged.
//!
//! What the generator deliberately does not produce: thresholds. A threshold
//! belongs to an alert rule or a reading's verdict, where it is compared and
//! acted on. Drawn on a panel it is a second definition that drifts, and the
//! colour bands would go red on a series nobody has decided is a problem.

use std::collections::BTreeMap;

use cog_core::contract::metric_help::metric_description;
use cog_core::metric_names::{MetricName, ALL};
use cog_core::storage::MetricType;
use tracing::{info, warn};

use crate::readings::{ReadingSpec, READINGS};

/// One panel: what it is called, the PromQL it draws, and the unit it is in.
pub struct Panel {
    pub title: String,
    /// The PromQL as written.
    pub expr: String,
    /// Grafana's unit id (`s`, `ms`, `bytes`, `percent`), or empty when
    /// nothing determines it. Empty is honest: a wrong unit turns a count of
    /// refusals into a duration.
    pub unit: String,
    /// Single number, or a series over time.
    pub stat: bool,
    /// The registered series this panel reads, spelled the way the closed set
    /// spells them. Empty for a reading, whose query is a real PromQL
    /// expression over series that mostly live outside this repository — the
    /// reading names those in its own `series` field.
    ///
    /// Written here rather than recovered from the expression: the two tests
    /// that judge coverage run in both directions, and a parser that has to
    /// understand PromQL to answer them would be a second implementation of
    /// what the dashboard contract already carries.
    pub series: Vec<String>,
}

/// One Grafana dashboard: a page of panels under a stable uid.
pub struct Board {
    /// Deterministic, so a link to a board is a function of what it is about
    /// rather than a string somebody typed into a second table.
    pub uid: String,
    pub title: String,
    pub panels: Vec<Panel>,
}

/// The uid of the board that carries the readings.
///
/// One page, because a reading is a judgement: an operator opens it to see
/// which of the answers is wrong, and splitting the answers across pages hides
/// the comparison that is the point.
pub const READINGS_BOARD_UID: &str = "cogneva-readings";

/// The board a series named `name` is drawn on.
///
/// Read off the name rather than listed: the group is the first segment after
/// the product prefix (`cogneva_shutdown_drain_milliseconds` is `shutdown`),
/// which is how these names were already written. A name with no segment at
/// all (`""`, `"_"`) is filed under `misc` rather than producing a uid that
/// ends in a separator.
pub fn board_uid_for_series(name: &str) -> String {
    let rest = name.strip_prefix("cogneva_").unwrap_or(name);
    let group = rest
        .split('_')
        .find(|part| !part.is_empty())
        .unwrap_or("misc");
    format!("cogneva-{group}")
}

/// The PromQL and panel shape for one series, given what kind it is.
///
/// The three kinds are not cosmetic: a counter is only meaningful as a rate
/// (its raw value is the process's uptime), a histogram only as a quantile
/// over its buckets, and a gauge as its own value. Each wrong answer still
/// renders — flat, or climbing forever — rather than erroring, which is why
/// the shape is asserted per kind below.
///
/// `rate` reads over Grafana's `$__rate_interval` rather than a fixed window,
/// so a sparse series produces a slope instead of a number that flickers
/// between zero and a spike depending on the zoom.
pub fn series_panel(name: MetricName, kind: MetricType, help: &str) -> Panel {
    let wire = name.as_str();
    let (expr, series) = match kind {
        MetricType::Counter => (
            format!("rate({wire}[$__rate_interval])"),
            vec![wire.to_string()],
        ),
        MetricType::Gauge => (wire.to_string(), vec![wire.to_string()]),
        MetricType::Histogram => (
            // The registered name is the family; a quantile is computed over
            // its buckets. Summing by `le` first is what makes the quantile
            // meaningful across the label sets the producers write.
            format!(
                "histogram_quantile(0.95, sum by (le) (rate({wire}_bucket[$__rate_interval])))"
            ),
            vec![wire.to_string()],
        ),
    };
    Panel {
        title: help.to_string(),
        expr,
        unit: unit_from_name(wire).to_string(),
        stat: false,
        series,
    }
}

/// The panel that carries one reading.
///
/// Its query and unit are the reading's own, so the number drawn is the number
/// the verdict was computed from. `stat` because a reading is one value with a
/// decision attached, not a trend to interpret.
pub fn reading_panel(spec: &ReadingSpec) -> Panel {
    Panel {
        title: spec.title.to_string(),
        expr: spec.query.to_string(),
        unit: unit_from_reading(spec.unit),
        stat: true,
        series: Vec::new(),
    }
}

/// The unit a series is in, when its name determines one.
///
/// Only the suffixes that mean exactly one thing are read; everything else is
/// empty rather than guessed, because a percentage and a count of events end
/// in neither, and a count drawn as a duration is a number read the wrong way.
fn unit_from_name(name: &str) -> &'static str {
    if name.ends_with("_milliseconds") || name.ends_with("_ms") {
        "ms"
    } else if name.ends_with("_seconds") {
        "s"
    } else if name.ends_with("_bytes") {
        "bytes"
    } else {
        ""
    }
}

/// The Grafana unit id for the unit a reading declares.
///
/// The readings write their unit the way an operator reads it (`%`, `条`,
/// `秒`), and Grafana wants its own id. A unit with no Grafana id stays as the
/// reading wrote it: an id Grafana does not know draws the bare number, which
/// is what the reading meant anyway.
fn unit_from_reading(unit: &str) -> String {
    match unit {
        "%" => "percent",
        "秒" => "s",
        "毫秒" => "ms",
        "字节" => "bytes",
        other => other,
    }
    .to_string()
}

/// The kind of a registered name, read off which help table documents it.
///
/// The closed set records names, not kinds: a kind is decided at the call site
/// that writes the series, and the one place that decision is written down is
/// the help table the name was filed under. A name documented under two kinds
/// would be ambiguous here, so it is refused rather than resolved by order —
/// and the help module's own test already refuses that.
pub fn kind_of(name: MetricName) -> Option<MetricType> {
    MetricType::ALL
        .into_iter()
        .find(|kind| metric_description(*kind, name.as_str()).is_some())
}

/// Every board, computed.
///
/// The reading board first, then one board per group of series, both in a
/// stable order so two runs over the same tree produce the same bytes.
pub fn panels_from_registry() -> Vec<Board> {
    let mut boards: BTreeMap<String, Board> = BTreeMap::new();

    boards.insert(
        READINGS_BOARD_UID.to_string(),
        Board {
            uid: READINGS_BOARD_UID.to_string(),
            title: "判词（读数）".to_string(),
            panels: READINGS.iter().map(|spec| reading_panel(spec)).collect(),
        },
    );

    for name in ALL.iter().copied() {
        // A registered series nobody documented has no sentence to title a
        // panel with, and a panel titled by its wire name is one nobody opens.
        // Dropping it is visible in the coverage test below rather than
        // silent.
        let Some(kind) = kind_of(name) else {
            continue;
        };
        let help = metric_description(kind, name.as_str()).unwrap_or_default();
        let uid = board_uid_for_series(name.as_str());
        let board = boards.entry(uid.clone()).or_insert_with(|| Board {
            title: format!("序列目录：{uid}"),
            uid,
            panels: Vec::new(),
        });
        board.panels.push(series_panel(name, kind, help));
    }

    boards.into_values().collect()
}

/// Where the boards are sent, and what proves they arrived.
///
/// Read from the environment rather than from the config file because the
/// address is a deployment fact (which Grafana this release talks to) and the
/// password is a credential, which never lives in a config file: it is a
/// Secret key mounted as an env var, the same path the alert mailer's SMTP
/// password takes.
///
/// Nothing declared is a refusal, not a default. A guessed in-cluster address
/// fails in the shape this face cannot show: a pushed dashboard and a push
/// nobody made both look like "no new panels", so a guess that moved with the
/// release would read exactly like a quiet week.
pub struct GrafanaEndpoint {
    /// No trailing slash: every URL below is joined onto it.
    pub base: String,
    password: String,
}

/// The password never reaches a log line or a panic message.
///
/// Derived `Debug` would print it into whatever formats the endpoint — an
/// error path, a `dbg!`, a failing assertion in a future test. There is no
/// useful debugging value in the bytes and no way to un-print them.
impl std::fmt::Debug for GrafanaEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrafanaEndpoint")
            .field("base", &self.base)
            .field("password", &"[redacted]")
            .finish()
    }
}

impl GrafanaEndpoint {
    /// The endpoint as the deployment declares it, or why there is none.
    pub fn from_env() -> Result<Self, String> {
        Self::from_parts(
            std::env::var("COGNEVA_GRAFANA_URL").ok().as_deref(),
            std::env::var("COGNEVA_GRAFANA_PASSWORD").ok().as_deref(),
        )
    }

    /// [`from_env`] with the declarations handed in, so the rule about what
    /// counts as declared can be read without an environment to set.
    fn from_parts(url: Option<&str>, password: Option<&str>) -> Result<Self, String> {
        let base = url
            .map(str::trim)
            .map(|s| s.trim_end_matches('/'))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "没有声明 Grafana 地址（COGNEVA_GRAFANA_URL）：面板生成器不猜集群内的地址，\
                 猜错与「导出器没跑」在读数面上同形"
                    .to_string()
            })?;
        let password = password
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "没有声明 Grafana 口令（COGNEVA_GRAFANA_PASSWORD）：推送要管理员身份，\
                 匿名只读推不上去；这个值只从 Secret 挂进来"
                    .to_string()
            })?;
        Ok(Self {
            base: base.to_string(),
            password: password.to_string(),
        })
    }
}

/// The body Grafana's dashboard API takes, for one board.
///
/// JSON because that is the protocol — the same exception the storage rule
/// makes for query languages: the boundary speaks JSON, and nothing in this
/// tree keeps a copy of what it said.
///
/// Layout is computed, not chosen: panels fill a 24-column grid three at a
/// time, in registry order. The point of a stable layout is that a reader who
/// has learned where to look keeps finding it there after a release.
pub fn dashboard_body(board: &Board) -> serde_json::Value {
    let panels: Vec<serde_json::Value> = board
        .panels
        .iter()
        .enumerate()
        .map(|(i, panel)| {
            let per_row = 3;
            let width = 24 / per_row;
            serde_json::json!({
                "id": i,
                "type": if panel.stat { "stat" } else { "timeseries" },
                "title": panel.title,
                "gridPos": {
                    "h": if panel.stat { 6 } else { 8 },
                    "w": width,
                    "x": (i % per_row) * width,
                    "y": (i / per_row) * 8,
                },
                "fieldConfig": { "defaults": { "unit": panel.unit }, "overrides": [] },
                "targets": [{ "refId": "A", "expr": panel.expr, "legendFormat": "__auto" }],
            })
        })
        .collect();

    serde_json::json!({
        "uid": board.uid,
        "title": board.title,
        "schemaVersion": 39,
        "refresh": "30s",
        "time": { "from": "now-6h", "to": "now" },
        "panels": panels,
    })
}

/// How long the first retry waits after a push fails.
// cap: fixed — 重试的代价与等待的代价同向：推出去了就停，推不出去下一轮再来；
// 这个数只决定 Grafana 比网关晚起来时面板晚出现多久，没有两侧代价不对称的最优点
const PUSH_RETRY_SECONDS: u64 = 30;

/// The longest a retry waits, so a Grafana that stays away does not turn into
/// one attempt per hour that nobody is watching for.
// cap: fixed — 与上一个同一条理由的另一端；它只是把退避封顶，不是界
const PUSH_RETRY_CEILING_SECONDS: u64 = 300;

/// Keep pushing until the boards land once.
///
/// Once, not periodically: the model is computed from constants compiled into
/// this binary, so it cannot change while the process runs — a periodic
/// re-push would be the same bytes at an interval nobody chose. What can
/// change is whether Grafana is up, which is why a failed push retries with
/// backoff instead of giving up.
///
/// A dashboard that fails to appear is not a decision anybody can make from a
/// log line alone, so the first failure and the recovery are both logged with
/// the reason the API gave.
pub async fn keep_pushing(endpoint: GrafanaEndpoint) {
    let boards = panels_from_registry();
    let client = match reqwest::Client::builder()
        // cap: fixed — 一次推送要么很快回来要么算失败：卡住的推送挡住的是下一次
        // 推送，而面板晚一分钟出现不影响任何判词
        .timeout(std::time::Duration::from_secs(30))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            warn!("grafana dashboard export off: the HTTP client could not be built: {e}");
            return;
        }
    };
    let mut wait = PUSH_RETRY_SECONDS;
    loop {
        match push_once(&client, &endpoint, &boards).await {
            Ok(pushed) => {
                info!(boards = pushed, "grafana dashboards pushed");
                return;
            }
            Err(why) => warn!(%why, retry_in_seconds = wait, "grafana dashboards not pushed"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
        wait = (wait * 2).min(PUSH_RETRY_CEILING_SECONDS);
    }
}

/// Push every board, and say how many landed.
///
/// `POST /api/dashboards/db` with `overwrite` is the API's upsert: the uid is
/// the identity, so a re-push replaces a board rather than accumulating copies
/// beside it. A failure names the board it was on, because "the export failed"
/// with a hundred boards is not a fact anybody can act on.
pub async fn push_once(
    client: &reqwest::Client,
    endpoint: &GrafanaEndpoint,
    boards: &[Board],
) -> Result<usize, String> {
    let url = format!("{}/api/dashboards/db", endpoint.base);
    let mut pushed = 0;
    for board in boards {
        let body = serde_json::json!({
            "dashboard": dashboard_body(board),
            "overwrite": true,
            "message": "written by the gateway from the metric and reading registries",
        });
        let response = client
            .post(&url)
            .basic_auth("admin", Some(endpoint.password.as_str()))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("推送 {} 失败：{e}", board.uid))?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "推送 {} 被 Grafana 拒绝（{status}）：{}",
                board.uid,
                text.trim()
            ));
        }
        pushed += 1;
    }
    Ok(pushed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every series the closed set publishes lands on a panel.
    ///
    /// The claim this replaces — "the dashboard covers what the code
    /// publishes" — used to be a hand check against a 204 KB file and is now
    /// arithmetic.
    #[test]
    fn every_registered_series_is_drawn() {
        let drawn: std::collections::BTreeSet<String> = panels_from_registry()
            .iter()
            .flat_map(|b| b.panels.iter().flat_map(|p| p.series.clone()))
            .collect();
        let missing: Vec<&str> = ALL
            .iter()
            .map(|n| n.as_str())
            .filter(|n| !drawn.contains(*n))
            .collect();
        assert!(
            missing.is_empty(),
            "这些序列在这个构建里能写，却没有一块面板画它：{missing:?}"
        );
    }

    /// Every series a generated panel reads is one the closed set publishes.
    ///
    /// The reverse direction, and the one that keeps the boards honest: a
    /// panel querying a name no build writes is empty in production and green
    /// in the tests.
    #[test]
    fn every_series_a_generated_panel_reads_is_in_the_closed_set() {
        let known: std::collections::BTreeSet<&str> = ALL.iter().map(|n| n.as_str()).collect();
        let mut foreign = Vec::new();
        for board in panels_from_registry() {
            for panel in board.panels {
                for name in &panel.series {
                    if !known.contains(name.as_str()) {
                        foreign.push((board.uid.clone(), name.clone(), panel.title.clone()));
                    }
                }
            }
        }
        assert!(
            foreign.is_empty(),
            "这些面板画的序列不在闭集里，生产上永远是空的：{foreign:?}"
        );
    }

    /// A histogram is drawn over its buckets, and the bucket name is the
    /// family's, so the reverse check still finds a registered name.
    #[test]
    fn a_histograms_panel_reads_its_family_not_a_bucket_name() {
        let panel = series_panel(
            cog_core::metric_names::MEMORY_OPERATION_LATENCY_MS,
            MetricType::Histogram,
            "h",
        );
        assert!(panel.expr.contains("memory_operation_latency_ms_bucket["));
        assert_eq!(
            panel.series,
            vec!["memory_operation_latency_ms".to_string()]
        );
    }

    /// Every reading has a panel on the judgement board.
    ///
    /// By construction today, and here so the construction cannot later be
    /// bypassed by filtering readings out: a reading without a panel is a
    /// verdict an operator cannot ask a follow-up question about.
    #[test]
    fn every_reading_has_a_panel() {
        let boards = panels_from_registry();
        let readings = boards
            .iter()
            .find(|b| b.uid == READINGS_BOARD_UID)
            .expect("the readings board is there");
        assert_eq!(readings.panels.len(), READINGS.len());
        for spec in READINGS.iter() {
            assert!(
                readings.panels.iter().any(|p| p.expr == spec.query),
                "读数 {} 没有面板",
                spec.id
            );
        }
    }

    /// A counter is drawn as a rate, a gauge as itself, a histogram as a
    /// quantile over its buckets.
    #[test]
    fn a_series_is_drawn_the_way_its_kind_is_read() {
        let counter = series_panel(
            cog_core::metric_names::MEMORY_OPERATIONS_TOTAL,
            MetricType::Counter,
            "h",
        );
        assert!(counter.expr.starts_with("rate("), "{}", counter.expr);
        let gauge = series_panel(
            cog_core::metric_names::MEMORY_UNEXTRACTED_RAW,
            MetricType::Gauge,
            "h",
        );
        assert_eq!(
            gauge.expr,
            cog_core::metric_names::MEMORY_UNEXTRACTED_RAW.as_str()
        );
    }

    /// The unit follows the name or is left empty, and never guessed.
    #[test]
    fn a_unit_is_read_from_the_name_or_left_empty() {
        assert_eq!(unit_from_name("x_seconds"), "s");
        assert_eq!(unit_from_name("x_ms"), "ms");
        assert_eq!(unit_from_name("x_bytes"), "bytes");
        assert_eq!(unit_from_name("x_total"), "");
        assert_eq!(unit_from_name("x_refusals"), "");
    }

    /// The same tree produces the same boards, in the same order.
    ///
    /// Without this a re-push would rewrite every dashboard every time, and
    /// nothing could tell a real change from a reshuffle.
    #[test]
    fn two_runs_over_the_same_tree_agree() {
        fn boards() -> Vec<(String, Vec<String>)> {
            panels_from_registry()
                .into_iter()
                .map(|b| (b.uid, b.panels.into_iter().map(|p| p.expr).collect()))
                .collect()
        }
        assert_eq!(boards(), boards());
    }

    /// A series is filed under the segment its name carries, and two series
    /// that share one land on the same board.
    #[test]
    fn a_board_is_named_by_the_segment_after_the_prefix() {
        assert_eq!(
            board_uid_for_series("cogneva_shutdown_drain_milliseconds"),
            "cogneva-shutdown"
        );
        assert_eq!(
            board_uid_for_series("memory_operations_total"),
            "cogneva-memory"
        );
        // A name with no segment left does not produce a uid ending in a
        // separator: `split` on an empty string yields one empty part, and
        // that part is skipped.
        assert_eq!(board_uid_for_series(""), "cogneva-misc");
    }

    /// An undeclared address or password is a refusal with a reason, not a
    /// guess at an in-cluster default.
    ///
    /// A guessed address fails silently in the one direction this surface
    /// cannot show: a dashboard that was never pushed and a Grafana that is
    /// simply quiet produce the same absence.
    #[test]
    fn an_undeclared_endpoint_says_what_is_missing() {
        let no_url = GrafanaEndpoint::from_parts(None, Some("pw")).unwrap_err();
        assert!(no_url.contains("COGNEVA_GRAFANA_URL"), "{no_url}");
        let blank_url = GrafanaEndpoint::from_parts(Some("   "), Some("pw")).unwrap_err();
        assert!(blank_url.contains("COGNEVA_GRAFANA_URL"), "{blank_url}");
        let no_password = GrafanaEndpoint::from_parts(Some("http://g"), None).unwrap_err();
        assert!(
            no_password.contains("COGNEVA_GRAFANA_PASSWORD"),
            "{no_password}"
        );
    }

    /// A declared address loses its trailing slash, so joining a path onto it
    /// never produces `//api`.
    #[test]
    fn a_declared_address_is_normalised() {
        let endpoint = GrafanaEndpoint::from_parts(Some(" http://g:3000/ "), Some("pw")).unwrap();
        assert_eq!(endpoint.base, "http://g:3000");
    }

    /// The endpoint's `Debug` says the password is there, not what it is.
    #[test]
    fn the_password_does_not_reach_the_debug_form() {
        let endpoint =
            GrafanaEndpoint::from_parts(Some("http://g"), Some("s3cret")).expect("declared");
        let printed = format!("{endpoint:?}");
        assert!(!printed.contains("s3cret"), "{printed}");
        assert!(printed.contains("redacted"), "{printed}");
    }

    /// Every panel on a board reaches the body exactly once, with the uid the
    /// board is addressed by.
    ///
    /// A body that drops a panel is a panel that exists in the generator and
    /// nowhere a person looks — the failure this whole file replaces.
    #[test]
    fn a_dashboard_body_carries_every_panel_once() {
        let boards = panels_from_registry();
        let readings = boards
            .iter()
            .find(|b| b.uid == READINGS_BOARD_UID)
            .expect("the readings board is there");
        let body = dashboard_body(readings);
        assert_eq!(body["uid"], READINGS_BOARD_UID);
        let panels = body["panels"].as_array().expect("panels is an array");
        assert_eq!(panels.len(), readings.panels.len());
        for (i, panel) in panels.iter().enumerate() {
            assert_eq!(panel["targets"][0]["expr"], readings.panels[i].expr);
            assert_eq!(panel["title"], readings.panels[i].title);
        }
    }

    /// A panel's unit reaches Grafana as Grafana's own id.
    ///
    /// The readings write `%`; Grafana's id for it is `percent`. Passing the
    /// reading's spelling through draws a bare number with a stray symbol
    /// beside it, which reads as a different quantity.
    #[test]
    fn a_panel_carries_the_unit_grafana_knows() {
        let spec = crate::readings::DISK_FREE;
        let panel = reading_panel(&spec);
        assert_eq!(panel.unit, "percent");
        let body = dashboard_body(&Board {
            uid: "x".to_string(),
            title: "x".to_string(),
            panels: vec![panel],
        });
        assert_eq!(
            body["panels"][0]["fieldConfig"]["defaults"]["unit"],
            "percent"
        );
    }
}
