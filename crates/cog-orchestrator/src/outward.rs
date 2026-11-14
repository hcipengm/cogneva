//! The outward loop: read what is happening outside this tree, and hand what
//! matters to the goal path.
//!
//! Everything else in this process looks at itself — the tree, its own accounts,
//! its own workloads. That is the inner loop, and a system that only looks
//! inward can only refine what it already is: it cannot learn that a dependency
//! shipped a release, that an interface it codes against changed, or that
//! something it assumed about the world stopped being true. Those facts arrive
//! from outside, and nothing here was going out to meet them. This loop is the
//! outward half of the same cycle: it asks a search backend the deployment
//! already runs what is new on chosen subjects, and hands each result to the
//! one path that does work — the goal graph.
//!
//! What it deliberately does not do is decide anything. A result becomes a goal
//! whose text says what was found and what to do with it; whether it matters is
//! answered by whoever takes that goal, through the same handover every other
//! piece of work arrives by. Nothing is written into the change account from
//! here, and nothing is marked done: an item this loop hands over is an item
//! *somebody was asked about*, which is the most it can honestly claim.
//!
//! Deduplication is the graph's job, not memory's. The id of the task that
//! carries an item is a function of the item's URL, so an item the graph
//! already holds is recognised across restarts, replica changes and a lost
//! process — a `HashSet` in this loop would forget everything it knew the first
//! time the pod restarted and re-ask the same questions every day. The loop
//! still asks the graph first (`get_task`) so a repeat costs no planner call;
//! the identity is what makes that check meaningful, not decoration.
//!
//! Everything it reads is configuration: with no queries configured it makes no
//! outbound call at all, and says at start-up which of "not configured",
//! "configured and unreadable" and "configured and running" it is in. A loop
//! that quietly did nothing would leave a reader unable to tell an idle
//! capability from a broken one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric};
use cog_core::{
    HttpClient, HttpRequest, OrchestratorControl, OwnerLeaseBroker, SFResult, ShutdownSignal, Task,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Loop name reported through the background-loop liveness family.
///
/// The role this loop contends for through the owner lease is the same word on
/// purpose: a liveness reading and a lease row that name the same loop by two
/// spellings are two things a reader has to know are the same. Declared once,
/// so a rename cannot move one and leave the other behind.
pub const OUTWARD_LOOP: &str = "orchestrator_outward";
/// The role this loop contends for through the owner lease.
pub const OUTWARD_ROLE: &str = OUTWARD_LOOP;

/// The environment variable that names what to look for outside. Published so
/// the deployment's own wiring can be held to it rather than described.
pub const QUERIES_ENV: &str = "COGNEVA_OUTWARD_QUERIES";
/// The search backend. The deployment already points this at its own metasearch
/// instance for the agent tools; the outward loop reads the same value rather
/// than taking a second endpoint to keep in step. The name itself is declared
/// in the contract crate, where both readers can see one spelling.
const SEARCH_BASE_ENV: &str = cog_core::WEB_SEARCH_BASE_ENV;

/// Asked once per source unless the source says otherwise. Hours, not minutes:
/// the world this reads changes on the scale of releases and articles, and a
/// tighter cadence would spend the same money to find the same items and have
/// them deduplicated.
// cap: bound — 被读的那件事按发布与文章变化，不按秒；更密的节奏买的是同一批条目
const DEFAULT_CADENCE_SECS: u64 = 6 * 60 * 60;
/// How many results one source may hand over per pass. The backend returns its
/// whole ranked list; an unbounded handover would turn one query into a day of
/// work and bury everything else in the graph.
// cap: bound — 一条查询变成一天的工作，图里其余的东西就都被埋了
const DEFAULT_MAX_ITEMS: usize = 3;
/// The backend runs inside this cluster, so a slow answer is a broken one: a
/// request that needs longer than this is not going to be answered usefully,
/// and holding the tick open costs every other source its turn.
// cap: bound — SearXNG 就在集群里，三十秒还没答就是没在答
const FETCH_TIMEOUT_SECS: u64 = 30;
/// How much of a result's excerpt travels in the goal text. Enough to tell the
/// taker whether it is worth opening; the URL is what they read when it is.
// cap: bound — 够判断值不值得打开原文；要读细节的人去读 URL 那一页
const EXCERPT_CHARS: usize = 600;
/// The longest the loop sleeps between wakes, whatever its sources ask for.
///
/// A source's cadence says how often that source may be asked *again*; it does
/// not say how long a fresh process must stay silent. The loop keeps no record
/// of the previous process's asks -- the census is in memory and starts empty --
/// so a process whose first wake is one whole cadence away never asks at all in
/// a deployment that rolls more often than the cadence. This platform rolls
/// several times a day against daily sources, which is exactly that deployment:
/// the loop reads as configured and permanently unexercised, and no reading ever
/// says the outside world went unread. Waking sooner costs one timer, not one
/// query -- each source is still only asked when its own cadence is due.
// cap: bound — 一觉睡满一个节奏等于滚动比节奏快的部署里一次都不问
const WAKE_CEILING_SECS: u64 = 5 * 60;

/// One subject to look for, and how often.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OutwardSource {
    pub name: String,
    pub query: String,
    pub cadence_secs: u64,
    pub max_items: usize,
}

impl Default for OutwardSource {
    fn default() -> Self {
        Self {
            name: String::new(),
            query: String::new(),
            cadence_secs: DEFAULT_CADENCE_SECS,
            max_items: DEFAULT_MAX_ITEMS,
        }
    }
}

/// What the outward loop reads from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutwardConfig {
    /// Base of the search backend; `/search` is appended by the caller.
    pub search_base: String,
    pub sources: Vec<OutwardSource>,
}

/// Why there is no configuration to run with. Each variant is a different fact
/// and each is reported as itself, because "nobody asked for this" and "someone
/// asked and the ask does not parse" look identical from a stopped loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFault {
    /// Nothing was configured.
    Unset,
    /// Configured, but not a list of sources this loop can read.
    Unparsable,
    /// Configured and readable, and the list is empty.
    Empty,
    /// A source is missing its name or its query.
    Incomplete,
    /// Sources are configured but no search backend is named.
    NoEndpoint,
}

impl ConfigFault {
    /// The value recorded under the `result` label of the configuration series.
    pub fn cell(self) -> &'static str {
        match self {
            ConfigFault::Unset => "unset",
            ConfigFault::Unparsable => "unparsable",
            ConfigFault::Empty => "empty",
            ConfigFault::Incomplete => "incomplete",
            ConfigFault::NoEndpoint => "no_endpoint",
        }
    }

    /// What a reader is meant to do about it.
    pub fn why(self) -> &'static str {
        match self {
            ConfigFault::Unset => {
                "no outward queries are configured, so nothing is looked for outside this tree"
            }
            ConfigFault::Unparsable => {
                "outward queries are configured but do not parse as a list of \
                 {name, query, cadence_secs?, max_items?}"
            }
            ConfigFault::Empty => "outward queries are configured but the list is empty",
            ConfigFault::Incomplete => {
                "an outward source is missing its name or its query, so the list is not usable \
                 as written"
            }
            ConfigFault::NoEndpoint => {
                "outward queries are configured but no search backend is named, so there is \
                 nowhere to ask"
            }
        }
    }
}

impl OutwardConfig {
    /// Read the configuration from the process environment.
    ///
    /// Fail-closed, like the other configuration-gated loops here: anything
    /// short of a complete list is not started, and the reason is returned
    /// rather than swallowed, so the caller can record which of the five it was.
    pub fn from_env() -> Result<Self, ConfigFault> {
        let raw = std::env::var(QUERIES_ENV).ok();
        let base = std::env::var(SEARCH_BASE_ENV).ok();
        Self::resolve(raw.as_deref(), base.as_deref())
    }

    /// The pure half of [`Self::from_env`], so every refusal is testable
    /// without touching a process-wide variable.
    pub fn resolve(raw: Option<&str>, search_base: Option<&str>) -> Result<Self, ConfigFault> {
        let raw = raw
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .ok_or(ConfigFault::Unset)?;
        let mut sources: Vec<OutwardSource> =
            serde_json::from_str(raw).map_err(|_| ConfigFault::Unparsable)?;
        if sources.is_empty() {
            return Err(ConfigFault::Empty);
        }
        // A blank field is not a source anybody meant: a name is what the
        // readings are labelled by, and a query nothing asks is a source that
        // would report "found nothing" on every pass forever.
        if sources
            .iter()
            .any(|s| s.name.trim().is_empty() || s.query.trim().is_empty())
        {
            return Err(ConfigFault::Incomplete);
        }
        for source in &mut sources {
            source.cadence_secs = source.cadence_secs.max(60);
            source.max_items = source.max_items.clamp(1, 20);
        }
        let search_base = search_base
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .ok_or(ConfigFault::NoEndpoint)?;
        Ok(Self {
            search_base: search_base.trim_end_matches('/').to_string(),
            sources,
        })
    }

    /// The period the loop's own ticker runs at: the shortest cadence among the
    /// sources. Each source is still only asked on its own schedule.
    pub fn period(&self) -> Duration {
        let least = self
            .sources
            .iter()
            .map(|s| s.cadence_secs)
            .min()
            .unwrap_or(DEFAULT_CADENCE_SECS);
        Duration::from_secs(least.max(60))
    }

    /// How long the loop may sleep between wakes: its shortest source cadence,
    /// bounded by [`WAKE_CEILING_SECS`].
    ///
    /// The two are different questions. The cadence says when a source may be
    /// asked again; the wake says how long a loop may go without looking at
    /// whether something is due. Tying the wake to a daily cadence makes a fresh
    /// process wait a day before its first look, and a process that is replaced
    /// before then never looks at all -- so the bound is what keeps the loop
    /// reachable in a deployment whose restarts are faster than its cadence.
    pub fn wake_interval(&self) -> Duration {
        self.period().min(Duration::from_secs(WAKE_CEILING_SECS))
    }
}

/// One result as the search backend hands it back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutwardItem {
    pub url: String,
    pub title: String,
    pub excerpt: String,
}

/// What reading one source ended in, for the `result` reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchFault {
    /// The backend answered with a status that is not a success.
    HttpError,
    /// The request never got an answer.
    Transport,
    /// The backend answered, and the body is not a search answer.
    Unparsable,
}

impl FetchFault {
    pub fn cell(self) -> &'static str {
        match self {
            FetchFault::HttpError => "http_error",
            FetchFault::Transport => "transport_error",
            FetchFault::Unparsable => "unparsable",
        }
    }
}

/// What one source's turn produced: the items worth handing over, how many
/// entries in the answer could not be read as an item, and what the fetch ended
/// in when it failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTick {
    pub items: Vec<OutwardItem>,
    pub unusable: usize,
    pub fault: Option<FetchFault>,
}

/// A source's tick that produced nothing but a reason.
fn faulted(fault: FetchFault) -> SourceTick {
    SourceTick {
        items: Vec::new(),
        unusable: 0,
        fault: Some(fault),
    }
}

/// Percent-encode one query component. `ALPHA / DIGIT / "-" / "." / "_" / "~"`
/// travel as themselves, everything else as `%XX` — the conservative rule, so a
/// query carrying `&`, `#` or a space asks what it says instead of truncating
/// into a different one.
fn encode_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The URL this loop asks for one source.
pub fn search_url(base: &str, query: &str) -> String {
    format!(
        "{}/search?q={}&format=json&safesearch=1",
        base.trim_end_matches('/'),
        encode_component(query)
    )
}

/// The identity of the task that carries one item.
///
/// A function of the URL and of nothing else. That is what lets the graph be
/// the deduplication memory: the same page found by two different queries, or
/// by the same query a week later, is one row. The 16 hex characters are the
/// first half of a BLAKE3 digest — wide enough that the collision a wrong match
/// would need is not a thing that happens, and short enough that the id stays
/// legible in a task list.
pub fn task_id_for(url: &str) -> String {
    let digest = blake3::hash(url.as_bytes()).to_hex().to_string();
    format!("outward-{}", &digest[..16])
}

/// What the taker is asked. The item is quoted, not summarised: a summary here
/// would be this loop's opinion of the result, and the loop has none.
pub fn goal_for(source: &OutwardSource, item: &OutwardItem) -> String {
    let title = if item.title.trim().is_empty() {
        item.url.as_str()
    } else {
        item.title.trim()
    };
    let mut goal = format!(
        "外部信息评估（来源：{}）\n{}\n{}\n",
        source.name, title, item.url
    );
    let excerpt = item.excerpt.trim();
    if !excerpt.is_empty() {
        let short: String = excerpt.chars().take(EXCERPT_CHARS).collect();
        goal.push_str(&short);
        if short.chars().count() < excerpt.chars().count() {
            goal.push('…');
        }
        goal.push('\n');
    }
    // The tool is named rather than assumed: an assembly that publishes a change
    // account registers it on the shared tool table, and a taker in one that does
    // not still has to produce the three things a change application consists of.
    // Asking for the answer rather than for a particular call keeps the item
    // workable wherever it lands.
    goal.push_str(
        "这是本系统之外的一条动态。判断它是否影响这个平台：有影响就说清影响到哪里\
         （哪个文件、哪条配置、哪条行为），并把它变成一条变更申请——要改的路径、\
         落地后该出现的字面量、为什么。这个进程里配有 propose_self_change 就用它提交；\
         没有就把这三项原样写进结果。没有影响就说明为什么不需要改，不要提交。\
         拿不准的先去读原文再判断。",
    );
    goal
}

/// Read a search backend's answer into items.
///
/// Entries that carry no usable URL are counted rather than dropped silently: a
/// backend that answers a hundred rows of which ninety are cards and engine
/// suggestions is a different situation from one that answered ten results, and
/// the two read the same once the unusable ones are gone.
///
/// Repeated URLs within one answer are folded: a metasearch backend asks several
/// engines and overlaps them, and one page ranked twice is one page.
pub fn items_from(body: &str, max_items: usize) -> Result<(Vec<OutwardItem>, usize), FetchFault> {
    #[derive(Deserialize)]
    struct Answer {
        #[serde(default)]
        results: Vec<Hit>,
    }
    #[derive(Deserialize)]
    struct Hit {
        #[serde(default)]
        url: String,
        #[serde(default)]
        title: String,
        #[serde(default)]
        content: String,
    }

    let answer: Answer = serde_json::from_str(body).map_err(|_| FetchFault::Unparsable)?;
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    let mut unusable = 0;
    for hit in answer.results {
        let url = hit.url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            unusable += 1;
            continue;
        }
        if !seen.insert(url.to_string()) {
            continue;
        }
        if items.len() < max_items {
            items.push(OutwardItem {
                url: url.to_string(),
                title: hit.title,
                excerpt: hit.content,
            });
        }
    }
    Ok((items, unusable))
}

/// Ask one source and read the answer.
async fn fetch_source(
    http: &Arc<dyn HttpClient>,
    base: &str,
    source: &OutwardSource,
) -> SourceTick {
    let req = HttpRequest::get(search_url(base, &source.query))
        .header("Accept", cog_core::CONTENT_TYPE_JSON)
        .timeout(FETCH_TIMEOUT_SECS);
    let resp = match http.execute(req).await {
        Ok(resp) => resp,
        Err(e) => {
            warn!(source = %source.name, error = %e, "outward: the search backend is unreachable");
            return faulted(FetchFault::Transport);
        }
    };
    if !resp.is_success() {
        warn!(
            source = %source.name,
            status = resp.status,
            "outward: the search backend refused the query"
        );
        return faulted(FetchFault::HttpError);
    }
    let body = match resp.text() {
        Ok(body) => body,
        Err(e) => {
            warn!(source = %source.name, error = %e, "outward: the answer was not readable text");
            return faulted(FetchFault::Unparsable);
        }
    };
    match items_from(&body, source.max_items) {
        Ok((items, unusable)) => SourceTick {
            items,
            unusable,
            fault: None,
        },
        Err(fault) => {
            warn!(
                source = %source.name,
                "outward: the search backend answered with something that is not a search answer"
            );
            faulted(fault)
        }
    }
}

/// The readings this loop leaves behind.
///
/// Published as an `Observable` rather than written through the metrics backend
/// because these series are counters this process alone knows: they have no rows
/// to leave behind, so they cost nothing to hold here and need no name in the
/// store's closed registry.
#[derive(Default)]
pub struct OutwardCensus {
    state: Mutex<CensusState>,
}

#[derive(Default)]
struct CensusState {
    /// What start-up decided: the cell, and how many sources it found.
    config: Option<(&'static str, usize)>,
    /// Per source, counts keyed by outcome cell.
    passes: BTreeMap<String, BTreeMap<String, u64>>,
    items: BTreeMap<String, BTreeMap<String, u64>>,
}

/// Series names. `passes` counts ticks, `items` counts what a tick did with
/// each result — the two are separate because a tick that answers cleanly and
/// hands nothing over and one that hands ten over are the same `ok` pass.
pub const PASSES_METRIC: &str = "cogneva_outward_passes_total";
pub const ITEMS_METRIC: &str = "cogneva_outward_items_total";
/// The configuration as start-up found it, and how many sources it named. The
/// two are separate series because "this deployment asked for nothing" and "it
/// asked for three sources" have the same absence otherwise: a deployment that
/// never turned the loop on and one whose queries parsed but matched no source
/// would both publish no `passes` at all.
pub const CONFIG_METRIC: &str = "cogneva_outward_config";
pub const SOURCES_METRIC: &str = "cogneva_outward_sources_configured";

pub(crate) const LABEL_SOURCE: &str = "source";

fn bump(map: &mut BTreeMap<String, BTreeMap<String, u64>>, source: &str, cell: &str, by: u64) {
    *map.entry(source.to_string())
        .or_default()
        .entry(cell.to_string())
        .or_insert(0) += by;
}

impl OutwardCensus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record what start-up decided. Called exactly once, before any pass, so
    /// the configuration is a reading even when no loop was started.
    pub async fn record_config(&self, cell: &'static str, sources: usize) {
        self.state.lock().await.config = Some((cell, sources));
    }

    /// 这条源的三格结局从见到它那一刻起就存在，值可以是 0。
    ///
    /// 计数只在事情发生时加一，于是「一条也没交出去」与「这一格没接上」在读数面
    /// 上长得一样：两条都是序列缺席。外循环最要紧的那个问题恰好是前一种——它跑
    /// 了、抓到了、却一条也没交出去——所以三格先落到 0，之后只往上加。
    pub async fn declare_source(&self, source: &str) {
        let mut state = self.state.lock().await;
        for cell in [
            Handover::Handed.cell(),
            Handover::AlreadyHeld.cell(),
            Handover::Failed.cell(),
        ] {
            bump(&mut state.items, source, cell, 0);
        }
    }

    pub async fn record_pass(&self, source: &str, cell: &str) {
        bump(&mut self.state.lock().await.passes, source, cell, 1);
    }

    pub async fn record_items(&self, source: &str, cell: &str, count: u64) {
        if count > 0 {
            bump(&mut self.state.lock().await.items, source, cell, count);
        }
    }
}

#[async_trait]
impl Observable for OutwardCensus {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let state = self.state.lock().await;
        let mut out = Vec::new();
        if let Some((cell, sources)) = state.config {
            out.push(RawMetric::new(CONFIG_METRIC, 1.0).with_label(crate::LABEL_RESULT, cell));
            out.push(RawMetric::new(SOURCES_METRIC, sources as f64));
        }
        for (source, cells) in &state.passes {
            for (cell, count) in cells {
                out.push(
                    RawMetric::new(PASSES_METRIC, *count as f64)
                        .with_label(LABEL_SOURCE, source.as_str())
                        .with_label(crate::LABEL_RESULT, cell.as_str()),
                );
            }
        }
        for (source, cells) in &state.items {
            for (cell, count) in cells {
                out.push(
                    RawMetric::new(ITEMS_METRIC, *count as f64)
                        .with_label(LABEL_SOURCE, source.as_str())
                        .with_label(crate::LABEL_RESULT, cell.as_str()),
                );
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

    /// None of this varies by dimension, so declaring none is what gets it
    /// pulled once per scrape.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// What one item's handover ended in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Handover {
    Handed,
    AlreadyHeld,
    Failed,
}

impl Handover {
    fn cell(self) -> &'static str {
        match self {
            Handover::Handed => "handed",
            Handover::AlreadyHeld => "already_held",
            Handover::Failed => "failed",
        }
    }
}

/// Hand one item to the goal path.
///
/// The graph is asked first. `submit_goal_auto` would answer an already-held id
/// by not returning it, but by then the planner has already been asked to
/// decompose the goal — one model call per repeated item per pass, forever,
/// which is exactly the price a deduplication that lives in memory would have
/// looked like it avoided. `get_task` is the cheap question; the id is what
/// makes it answerable.
async fn hand_over(
    orchestrator: &Arc<dyn OrchestratorControl>,
    source: &OutwardSource,
    item: &OutwardItem,
) -> Handover {
    let id = task_id_for(&item.url);
    if orchestrator.get_task(&id).await.is_some() {
        return Handover::AlreadyHeld;
    }
    let goal = goal_for(source, item);
    let row = Task::goal_row(id.clone(), &goal);
    match orchestrator.submit_goal_auto(&goal, vec![row]).await {
        Ok(added) if added.is_empty() => {
            // The graph took nothing: another replica handed the same item over
            // between the read and the submission. The intent is in hand either
            // way, which is what the empty answer means.
            Handover::AlreadyHeld
        }
        Ok(_) => Handover::Handed,
        Err(e) => {
            warn!(
                source = %source.name,
                url = %item.url,
                error = %e,
                "outward: an item could not be handed to the goal path"
            );
            Handover::Failed
        }
    }
}

/// How many wakes one item's hand-over is tried across before it is given up on.
///
/// The pass that found an item is the first attempt; the next wakes carry the
/// rest. Three is enough for a refusal that is a collision or a momentary
/// outage to clear, and bounded so that an item the goal path will never take
/// stops costing a submission every five minutes for the rest of the day.
// cap: bound — 一次交接被拒多是撞车或片刻不可用，下一两个唤醒就过去了；界设在「今天剩下的每一拍都再交一次」之前
const HANDOVER_ATTEMPTS: u32 = 3;

/// An item whose hand-over failed, waiting for the next wake.
struct Retry {
    /// Which source found it: the census is per source, and so is the text the
    /// goal carries.
    source: usize,
    item: OutwardItem,
    /// How many attempts have already been made.
    attempts: u32,
}

/// Ask again for the items whose hand-over failed on an earlier wake.
///
/// A source asked once a day would otherwise lose a finding for the whole day
/// to a single refusal — and the graph can refuse for reasons that pass: a
/// submission landing between another replica's read and its insert, a store
/// that is briefly unavailable. The census records an item once, when its fate
/// is decided: counting the failed attempts would turn `items_total` from "what
/// this pass found and what became of it" into a count of tries, and the pass
/// reading beside it already says how often the source was asked.
async fn drain_retries(
    orchestrator: &Arc<dyn OrchestratorControl>,
    sources: &[OutwardSource],
    pending: &mut Vec<Retry>,
    census: &Arc<OutwardCensus>,
) {
    if pending.is_empty() {
        return;
    }
    let mut still = Vec::new();
    for mut retry in pending.drain(..) {
        let Some(source) = sources.get(retry.source) else {
            // The source list of this process is the one it was started with;
            // an index that names nothing is a bookkeeping error, not an item to
            // count against a source that does not exist.
            warn!(
                index = retry.source,
                url = %retry.item.url,
                "outward: a retry names a source this loop does not have"
            );
            continue;
        };
        let outcome = hand_over(orchestrator, source, &retry.item).await;
        if outcome == Handover::Failed && retry.attempts + 1 < HANDOVER_ATTEMPTS {
            retry.attempts += 1;
            still.push(retry);
            continue;
        }
        census.record_items(&source.name, outcome.cell(), 1).await;
    }
    *pending = still;
}

/// Start the resident outward loop.
///
/// The loop asks each source on its own cadence, hands what it found to the goal
/// path, and reports what happened. A failed fetch is a reading, not a stall:
/// the other sources still get their turn and the next tick asks again.
pub fn spawn_outward_loop(
    orchestrator: Arc<dyn OrchestratorControl>,
    http: Arc<dyn HttpClient>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    shutdown: ShutdownSignal,
    config: OutwardConfig,
    census: Arc<OutwardCensus>,
) {
    let period = config.period();
    let wake = config.wake_interval();
    let sources = config.sources.clone();
    let base = config.search_base.clone();
    info!(
        sources = sources.len(),
        period_secs = period.as_secs(),
        wake_secs = wake.as_secs(),
        arbitrated = role.is_some(),
        "outward loop enabled"
    );

    drop(cog_core::loop_health::spawn(
        OUTWARD_LOOP,
        cog_core::loop_health::Cadence::Periodic(period),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let orchestrator = orchestrator.clone();
            let http = http.clone();
            let role = role.clone();
            let shutdown = shutdown.clone();
            let sources = sources.clone();
            let base = base.clone();
            let census = census.clone();
            async move {
                // Only one replica asks and hands over at a time. The ids would
                // make a second replica's work harmless, but the reads and the
                // submissions would also be paid for twice.
                let hold = cog_core::RoleHold::start(role, OUTWARD_ROLE, beat.clone(), &shutdown);
                // The wake is bounded separately from the source cadence, so a
                // process whose sources are asked daily still looks at them
                // within minutes of starting. The cadence itself is enforced by
                // `next_due` below, not by how long this sleeps.
                let mut ticker = tokio::time::interval(wake);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // First tick fires immediately; consume it so the first pass
                // lands one wake in rather than racing start-up.
                ticker.tick().await;
                let now = Instant::now();
                let mut next_due: Vec<Instant> = sources.iter().map(|_| now).collect();
                // Items a pass found but could not hand over. They are asked
                // again on the next wake, not on the source's next cadence.
                let mut pending: Vec<Retry> = Vec::new();
                // 每一格先落到 0：这条源跑起来了、一条也没交出去，读数是 0，不是
                // 一条缺席的序列。
                for source in &sources {
                    census.declare_source(&source.name).await;
                }
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    if !hold.may_act().await {
                        continue;
                    }
                    drain_retries(&orchestrator, &sources, &mut pending, &census).await;
                    let now = Instant::now();
                    for (index, (source, due)) in
                        sources.iter().zip(next_due.iter_mut()).enumerate()
                    {
                        if now < *due {
                            continue;
                        }
                        *due = now + Duration::from_secs(source.cadence_secs);
                        let tick = fetch_source(&http, &base, source).await;
                        census
                            .record_pass(
                                &source.name,
                                tick.fault.map(FetchFault::cell).unwrap_or("ok"),
                            )
                            .await;
                        for item in &tick.items {
                            let outcome = hand_over(&orchestrator, source, item).await;
                            if outcome == Handover::Failed && HANDOVER_ATTEMPTS > 1 {
                                pending.push(Retry {
                                    source: index,
                                    item: item.clone(),
                                    attempts: 1,
                                });
                            } else {
                                census.record_items(&source.name, outcome.cell(), 1).await;
                            }
                        }
                    }
                }
            }
        },
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{HttpRequest, HttpResponse, SFError};
    use std::sync::Mutex as StdMutex;

    #[test]
    fn a_query_with_reserved_characters_asks_what_it_says() {
        assert_eq!(encode_component("a b"), "a%20b");
        assert_eq!(encode_component("x&y=z#1"), "x%26y%3Dz%231");
        assert_eq!(encode_component("rust/1.85~beta"), "rust%2F1.85~beta");
        assert_eq!(
            search_url("http://searxng:8080/", "rust & tokio"),
            "http://searxng:8080/search?q=rust%20%26%20tokio&format=json&safesearch=1"
        );
    }

    #[test]
    fn the_same_page_found_twice_is_one_task_and_a_new_page_is_another() {
        let a = task_id_for("https://example.com/release");
        assert_eq!(a, task_id_for("https://example.com/release"));
        assert_ne!(a, task_id_for("https://example.com/release-notes"));
        // Stable across processes: a literal, not a hash of this run.
        assert_eq!(a, task_id_for("https://example.com/release"));
        assert!(a.starts_with("outward-"));
        assert_eq!(a.len(), "outward-".len() + 16);
    }

    #[test]
    fn reading_an_answer_keeps_what_can_be_opened_and_counts_what_cannot() {
        let body = r#"{
            "results": [
                {"url": "https://a.example/1", "title": "One", "content": "first"},
                {"url": "javascript:void(0)", "title": "card", "content": ""},
                {"url": "https://b.example/2", "title": "Two", "content": ""},
                {"url": "https://a.example/1", "title": "One again", "content": "dup"}
            ]
        }"#;
        let (items, unusable) = items_from(body, 10).expect("a search answer");
        assert_eq!(
            unusable, 1,
            "the non-http entry should be counted, not silently dropped"
        );
        let urls: Vec<&str> = items.iter().map(|i| i.url.as_str()).collect();
        assert_eq!(urls, vec!["https://a.example/1", "https://b.example/2"]);
    }

    #[test]
    fn an_answer_is_capped_at_what_the_source_asked_for() {
        let body = r#"{"results":[
            {"url":"https://a/1"},{"url":"https://a/2"},{"url":"https://a/3"}
        ]}"#;
        let (items, _) = items_from(body, 2).expect("a search answer");
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn something_that_is_not_a_search_answer_is_not_an_empty_answer() {
        assert_eq!(
            items_from("<html>blocked</html>", 3),
            Err(FetchFault::Unparsable)
        );
        // A well-formed answer with nothing in it is a reading of the web and
        // must not be reported as unreadable.
        let (items, unusable) =
            items_from(r#"{"results":[]}"#, 3).expect("an empty answer is an answer");
        assert!(items.is_empty());
        assert_eq!(unusable, 0);
    }

    #[test]
    fn configuration_says_which_of_the_five_it_is_in() {
        assert_eq!(
            OutwardConfig::resolve(None, Some("http://s")),
            Err(ConfigFault::Unset)
        );
        assert_eq!(
            OutwardConfig::resolve(Some("   "), Some("http://s")),
            Err(ConfigFault::Unset)
        );
        assert_eq!(
            OutwardConfig::resolve(Some("not json"), Some("http://s")),
            Err(ConfigFault::Unparsable)
        );
        assert_eq!(
            OutwardConfig::resolve(Some("[]"), Some("http://s")),
            Err(ConfigFault::Empty)
        );
        assert_eq!(
            OutwardConfig::resolve(Some(r#"[{"name":"a","query":"  "}]"#), Some("http://s")),
            Err(ConfigFault::Incomplete)
        );
        assert_eq!(
            OutwardConfig::resolve(Some(r#"[{"name":"a","query":"b"}]"#), None),
            Err(ConfigFault::NoEndpoint)
        );

        let ok = OutwardConfig::resolve(
            Some(r#"[{"name":"rust","query":"rust release"}]"#),
            Some("http://searxng:8080/"),
        )
        .expect("a complete configuration");
        assert_eq!(ok.search_base, "http://searxng:8080");
        assert_eq!(ok.sources[0].cadence_secs, DEFAULT_CADENCE_SECS);
        assert_eq!(ok.sources[0].max_items, DEFAULT_MAX_ITEMS);
    }

    #[test]
    fn every_refusal_has_its_own_cell_and_a_reason() {
        let cells: Vec<&str> = [
            ConfigFault::Unset,
            ConfigFault::Unparsable,
            ConfigFault::Empty,
            ConfigFault::Incomplete,
            ConfigFault::NoEndpoint,
        ]
        .iter()
        .map(|f| f.cell())
        .collect();
        let unique: std::collections::HashSet<&&str> = cells.iter().collect();
        assert_eq!(
            unique.len(),
            cells.len(),
            "two refusals share a cell: {cells:?}"
        );
        assert!(ConfigFault::Unset.why().contains("nothing is looked for"));
    }

    #[test]
    fn the_goal_carries_what_was_found_and_what_to_do_with_it() {
        let source = OutwardSource {
            name: "rust".into(),
            query: "rust".into(),
            ..OutwardSource::default()
        };
        let item = OutwardItem {
            url: "https://blog.rust-lang.org/1.85".into(),
            title: "Announcing Rust 1.85".into(),
            excerpt: "edition 2024".into(),
        };
        let goal = goal_for(&source, &item);
        for needle in [
            "Announcing Rust 1.85",
            "https://blog.rust-lang.org/1.85",
            "propose_self_change",
            "rust",
        ] {
            assert!(goal.contains(needle), "goal is missing {needle}: {goal}");
        }
    }

    /// A client that answers one canned response and records every request.
    #[derive(Debug)]
    struct CannedClient {
        status: u16,
        body: Vec<u8>,
        calls: StdMutex<Vec<String>>,
    }

    impl CannedClient {
        fn new(status: u16, body: impl Into<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self {
                status,
                body: body.into(),
                calls: StdMutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl HttpClient for CannedClient {
        async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
            self.calls.lock().unwrap().push(req.url.clone());
            Ok(HttpResponse {
                status: self.status,
                headers: std::collections::HashMap::new(),
                body: self.body.clone(),
            })
        }
    }

    /// A fresh process looks at its sources within its first wake, not one
    /// cadence in.
    ///
    /// The cadence bounds how often a source may be asked again; it is not a
    /// promise that a process lasts that long. This platform rolls several
    /// times a day against sources configured daily, so a first pass one
    /// cadence away is a first pass that never happens: the census says
    /// "configured" and no pass is ever counted, which reads exactly like a
    /// deployment that did not turn the loop on.
    ///
    /// Paused time makes the two behaviours distinguishable without waiting:
    /// the runtime advances the clock to the next timer, so the assertion is
    /// about *which* timer fired first, not about how long the test took.
    #[tokio::test(start_paused = true)]
    async fn a_source_is_asked_within_its_first_wake_not_one_cadence_later() {
        let client = CannedClient::new(
            200,
            r#"{"results":[{"url":"https://x/1","title":"t","content":"c"}]}"#,
        );
        let config = OutwardConfig {
            search_base: "http://s".into(),
            sources: vec![OutwardSource {
                name: "x".into(),
                query: "q".into(),
                cadence_secs: 86_400,
                max_items: 2,
            }],
        };
        assert!(
            config.wake_interval() < config.period(),
            "a daily source whose loop only wakes daily is a loop no rollout shorter \
             than a day ever sees run"
        );
        spawn_outward_loop(
            Arc::new(RememberingOrchestrator::default()),
            client.clone() as Arc<dyn HttpClient>,
            None,
            ShutdownSignal::new(),
            config,
            Arc::new(OutwardCensus::new()),
        );
        // Let the loop reach its first wait before moving the clock: a ticker
        // created after the clock moved would be one full wake later.
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        // Past the wake ceiling and nowhere near the cadence: a loop that tied
        // its wake to the cadence is still asleep here and asks nothing.
        tokio::time::advance(Duration::from_secs(WAKE_CEILING_SECS + 60)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            client.calls.lock().unwrap().len(),
            1,
            "the loop never asked its only source: its first wake is as far away as \
             the cadence, so a process replaced sooner asks nothing at all"
        );
        // Asked once, not on every wake: the cadence still governs the repeats.
        tokio::time::advance(Duration::from_secs(WAKE_CEILING_SECS + 60)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            client.calls.lock().unwrap().len(),
            1,
            "the loop asked again before the source's cadence was due: the wake bound \
             turned a daily source into a pass every few minutes"
        );
    }

    #[tokio::test]
    async fn a_source_that_answers_is_asked_its_own_question() {
        let client = CannedClient::new(
            200,
            r#"{"results":[{"url":"https://x/1","title":"t","content":"c"}]}"#,
        );
        let source = OutwardSource {
            name: "x".into(),
            query: "what is new".into(),
            ..OutwardSource::default()
        };
        let tick = fetch_source(
            &(client.clone() as Arc<dyn HttpClient>),
            "http://s",
            &source,
        )
        .await;
        assert_eq!(tick.fault, None);
        assert_eq!(tick.items.len(), 1);
        assert_eq!(
            client.calls.lock().unwrap().as_slice(),
            ["http://s/search?q=what%20is%20new&format=json&safesearch=1"]
        );
    }

    #[tokio::test]
    async fn a_backend_that_refuses_reads_as_a_refusal_and_not_as_an_empty_web() {
        let client = CannedClient::new(503, "");
        let source = OutwardSource::default();
        let tick = fetch_source(
            &(client.clone() as Arc<dyn HttpClient>),
            "http://s",
            &source,
        )
        .await;
        assert_eq!(tick.fault, Some(FetchFault::HttpError));
        assert!(tick.items.is_empty());
    }

    /// An orchestrator that remembers the ids it was given, exactly like the
    /// graph does, so the dedup path is exercised rather than assumed.
    #[derive(Debug, Default)]
    struct RememberingOrchestrator {
        held: StdMutex<std::collections::HashSet<String>>,
        submissions: StdMutex<Vec<String>>,
        /// How many more submissions to refuse before the graph starts taking
        /// them: a refusal that passes, which is what the retry is for.
        refusals_left: StdMutex<usize>,
    }

    impl RememberingOrchestrator {
        fn refusing(times: usize) -> Self {
            let this = Self::default();
            *this.refusals_left.lock().unwrap() = times;
            this
        }
    }

    #[async_trait]
    impl OrchestratorControl for RememberingOrchestrator {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<()> {
            Ok(())
        }
        async fn submit_goal_auto(&self, _goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
            {
                let mut left = self.refusals_left.lock().unwrap();
                if *left > 0 {
                    *left -= 1;
                    return Err(SFError::Internal(
                        "the graph refused this submission".into(),
                    ));
                }
            }
            let mut held = self.held.lock().unwrap();
            let mut added = Vec::new();
            for task in tasks {
                self.submissions.lock().unwrap().push(task.id.clone());
                if held.insert(task.id.clone()) {
                    added.push(task.id.clone());
                }
            }
            Ok(added)
        }
        async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn add_task(&self, _task: Task) -> SFResult<()> {
            Ok(())
        }
        async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
            false
        }
        async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
            0
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            Vec::new()
        }
        async fn get_all_tasks(&self) -> Vec<Task> {
            Vec::new()
        }
        async fn push_to_dlq(&self, _task_id: &str, _error: String) -> SFResult<bool> {
            Ok(false)
        }
        async fn retry_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn dlq_len(&self) -> SFResult<usize> {
            Ok(0)
        }
        async fn start_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn complete_task(
            &self,
            _task_id: &str,
            _result: serde_json::Value,
        ) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn fail_task(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::contract::llm::UpstreamFailure>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            Ok((false, Vec::new(), false))
        }
        async fn fail_task_after(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::contract::llm::UpstreamFailure>,
            _retry_after_secs: Option<u64>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            Ok((false, Vec::new(), false))
        }
        async fn cancel_task(&self, _task_id: &str) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn get_task(&self, task_id: &str) -> Option<Task> {
            self.held
                .lock()
                .unwrap()
                .contains(task_id)
                .then(|| Task::goal_row(task_id.to_string(), "held"))
        }
        async fn schedule_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            Vec::new()
        }
        async fn get_dependents(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            (Vec::new(), Vec::new())
        }
        async fn delete_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn all_completed(&self) -> bool {
            true
        }
        async fn replay_dlq(&self, _task_id: &str) -> SFResult<bool> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn an_item_is_handed_over_once_and_a_second_pass_asks_the_graph_not_the_planner() {
        let orchestrator = Arc::new(RememberingOrchestrator::default());
        let control: Arc<dyn OrchestratorControl> = orchestrator.clone();
        let source = OutwardSource {
            name: "rust".into(),
            query: "rust".into(),
            ..OutwardSource::default()
        };
        let item = OutwardItem {
            url: "https://x/1".into(),
            title: "t".into(),
            excerpt: "c".into(),
        };

        assert_eq!(hand_over(&control, &source, &item).await, Handover::Handed);
        assert_eq!(
            hand_over(&control, &source, &item).await,
            Handover::AlreadyHeld
        );
        // The second pass never reached the submission path: the graph answered
        // first, so nothing was decomposed twice.
        assert_eq!(orchestrator.submissions.lock().unwrap().len(), 1);
    }

    async fn census_items(census: &OutwardCensus) -> Vec<(String, String, f64)> {
        let metrics = census.collect_metrics("").await.expect("a reading");
        metrics
            .into_iter()
            .filter(|m| m.name == ITEMS_METRIC)
            .map(|m| {
                (
                    m.labels.get(LABEL_SOURCE).cloned().unwrap_or_default(),
                    m.labels
                        .get(crate::LABEL_RESULT)
                        .cloned()
                        .unwrap_or_default(),
                    m.value,
                )
            })
            .collect()
    }

    /// 一次拒绝不该让这条发现丢一整天。
    ///
    /// 源是每天问一次的：一轮里交不出去、就此算掉，这条发现在明天之前都不会再
    /// 被问。2026-11-05 的第一版外循环就是这样——四条发现全部交不出去，读数上
    /// 只有 `failed`，而下一次提问在二十四小时之后。
    #[tokio::test]
    async fn an_item_whose_hand_over_failed_is_asked_again_on_the_next_wake() {
        let orchestrator = Arc::new(RememberingOrchestrator::refusing(1));
        let sources = vec![OutwardSource {
            name: "rust".into(),
            query: "rust".into(),
            ..OutwardSource::default()
        }];
        let control: Arc<dyn OrchestratorControl> = orchestrator.clone();
        let census = Arc::new(OutwardCensus::new());
        let mut pending = vec![Retry {
            source: 0,
            item: OutwardItem {
                url: "https://x/1".into(),
                title: "t".into(),
                excerpt: "c".into(),
            },
            attempts: 1,
        }];

        drain_retries(&control, &sources, &mut pending, &census).await;
        assert_eq!(pending.len(), 1, "被拒的那一条留在队里等下一拍");
        assert!(
            census_items(&census).await.is_empty(),
            "还没定局的东西不记进 items：这一格数的是发现的下场，不是尝试次数"
        );

        drain_retries(&control, &sources, &mut pending, &census).await;
        assert!(pending.is_empty());
        assert_eq!(
            census_items(&census).await,
            vec![("rust".to_string(), "handed".to_string(), 1.0)]
        );
    }

    /// 一条也没交出去，读数要说 0，而不是整条缺席。
    ///
    /// 缺席的序列与「跑了一整天、一条都没交出去」在面板上是同一片空白，而后者
    /// 正是外循环要回答的问题。三格在见到源的那一刻就落到 0。
    #[tokio::test]
    async fn a_source_that_handed_nothing_reads_zero_rather_than_nothing() {
        let census = OutwardCensus::new();
        census.declare_source("rust").await;

        let mut cells = census_items(&census).await;
        cells.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            cells,
            vec![
                ("rust".to_string(), "already_held".to_string(), 0.0),
                ("rust".to_string(), "failed".to_string(), 0.0),
                ("rust".to_string(), "handed".to_string(), 0.0),
            ],
            "三格先落到 0：读者要能分清「一条都没交出去」与「这一格没接上」"
        );

        census.record_items("rust", "handed", 1).await;
        let handed = census_items(&census)
            .await
            .into_iter()
            .find(|(_, cell, _)| cell == "handed")
            .expect("handed 还在");
        assert_eq!(handed.2, 1.0, "落到 0 之后照常往上加");
    }

    /// 一直交不出去的那一条最终要算一次 `failed`，而不是无声消失。
    #[tokio::test]
    async fn an_item_that_never_lands_is_counted_once_and_given_up_on() {
        let orchestrator = Arc::new(RememberingOrchestrator::refusing(usize::MAX));
        let sources = vec![OutwardSource {
            name: "rust".into(),
            query: "rust".into(),
            ..OutwardSource::default()
        }];
        let control: Arc<dyn OrchestratorControl> = orchestrator.clone();
        let census = Arc::new(OutwardCensus::new());
        let mut pending = vec![Retry {
            source: 0,
            item: OutwardItem {
                url: "https://x/1".into(),
                title: "t".into(),
                excerpt: "c".into(),
            },
            attempts: 1,
        }];

        for _ in 0..HANDOVER_ATTEMPTS {
            drain_retries(&control, &sources, &mut pending, &census).await;
        }
        assert!(pending.is_empty(), "给过的尝试次数用完就不再占着队");
        assert_eq!(
            census_items(&census).await,
            vec![("rust".to_string(), "failed".to_string(), 1.0)],
            "一条发现只记一次下场"
        );
    }

    #[tokio::test]
    async fn the_census_keeps_refusals_and_results_apart() {
        let census = OutwardCensus::new();
        census.record_config("ok", 2).await;
        census.record_pass("rust", "ok").await;
        census.record_pass("rust", "http_error").await;
        census.record_items("rust", "handed", 2).await;
        census.record_items("rust", "failed", 1).await;

        let metrics = census.collect_metrics("").await.expect("a reading");
        assert_eq!(
            metric_value(&metrics, PASSES_METRIC, Some("rust"), Some("ok")),
            Some(1.0)
        );
        assert_eq!(
            metric_value(&metrics, PASSES_METRIC, Some("rust"), Some("http_error")),
            Some(1.0)
        );
        assert_eq!(
            metric_value(&metrics, ITEMS_METRIC, Some("rust"), Some("handed")),
            Some(2.0)
        );
        assert_eq!(
            metric_value(&metrics, ITEMS_METRIC, Some("rust"), Some("already_held")),
            None
        );
        assert_eq!(
            metric_value(&metrics, CONFIG_METRIC, None, Some("ok")),
            Some(1.0)
        );
        assert_eq!(
            metric_value(&metrics, SOURCES_METRIC, None, None),
            Some(2.0)
        );
    }

    /// The value of the one series a reader is asking about.
    ///
    /// `source` and `result` are optional because two of the four readings are
    /// published without them. `None` there means "do not constrain on this
    /// label", not "match an empty value": a call that omits one must not be
    /// satisfied by a series that happens to carry an empty label.
    fn metric_value(
        metrics: &[RawMetric],
        name: &str,
        source: Option<&str>,
        result: Option<&str>,
    ) -> Option<f64> {
        metrics
            .iter()
            .find(|m| {
                m.name == name
                    && source
                        .is_none_or(|s| m.labels.get(LABEL_SOURCE).map(String::as_str) == Some(s))
                    && result.is_none_or(|r| {
                        m.labels.get(crate::LABEL_RESULT).map(String::as_str) == Some(r)
                    })
            })
            .map(|m| m.value)
    }

    #[test]
    fn a_fetch_failure_has_its_own_cell() {
        assert_ne!(FetchFault::HttpError.cell(), FetchFault::Transport.cell());
        assert_ne!(FetchFault::Unparsable.cell(), FetchFault::HttpError.cell());
        // Unreachable client: the transport cell, not an empty result set.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let client = Arc::new(FailingClient) as Arc<dyn HttpClient>;
        let tick = rt.block_on(fetch_source(&client, "http://s", &OutwardSource::default()));
        assert_eq!(tick.fault, Some(FetchFault::Transport));
    }

    #[derive(Debug)]
    struct FailingClient;

    #[async_trait]
    impl HttpClient for FailingClient {
        async fn execute(&self, _req: HttpRequest) -> SFResult<HttpResponse> {
            Err(SFError::IO("no route to host".into()))
        }
    }
}
