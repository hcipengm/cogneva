//! The resident loop that keeps configured faces' plans in the DAG.
//!
//! One supervised loop reads every configured face's SOP on its own cadence,
//! compiles it, and hands the verified tasks to the planner — which injects them
//! straight into the DAG. The loop is elected through the owner lease, so a
//! rolling deployment whose replicas briefly overlap still has one writer, and it
//! is registered with the background-loop liveness family so a stopped loop is a
//! reading rather than a silence.
//!
//! Everything face-specific is configuration: the loop starts only when
//! `COGNEVA_TASK_FACE_SOURCES` names at least one face, so a deployment that
//! configures none carries the capability dormant and makes no outbound call.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cog_core::{
    metric_names, ActionPlanner, HttpClient, HttpRequest, MetricName, MetricsBackend,
    OwnerLeaseBroker, ShutdownSignal, SkillRegistry,
};
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::compile::{compile, TaskFaceSource};

/// Loop name reported through the background-loop liveness family.
pub const SOP_DISPATCH_LOOP: &str = "orchestrator_sop_dispatch";

/// The role the dispatch loop contends for through the owner lease.
pub const SOP_DISPATCH_ROLE: &str = "orchestrator_sop_dispatch";

/// The environment variable that names the faces to dispatch.
const TASK_FACE_SOURCES_ENV: &str = "COGNEVA_TASK_FACE_SOURCES";

/// Where the face SOPs live and where to read them from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SopDispatchConfig {
    /// The platform endpoint the face documents are read through. It is the
    /// gateway's prefix, so the process holds no platform credential of its own.
    pub api_base: String,
    pub sources: Vec<TaskFaceSource>,
}

impl SopDispatchConfig {
    /// Read the dispatch configuration out of the environment.
    ///
    /// Returning `None` for anything wrong — unset, blank, unparsable, or an
    /// empty face list, or a missing endpoint — is the fail-closed contract: a
    /// deployment that has not been told which faces to run must not run any, and
    /// a half-read configuration is one that has not been told. Each `None` is
    /// logged with its cause so the silence is at least attributable.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(TASK_FACE_SOURCES_ENV).ok()?;
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        let sources: Vec<TaskFaceSource> = match serde_json::from_str(raw) {
            Ok(sources) => sources,
            Err(e) => {
                warn!(
                    env = TASK_FACE_SOURCES_ENV,
                    error = %e,
                    "task face sources configured but not parseable; SOP dispatch stays off"
                );
                return None;
            }
        };
        if sources.is_empty() {
            warn!(
                env = TASK_FACE_SOURCES_ENV,
                "task face sources configured but empty; SOP dispatch stays off"
            );
            return None;
        }
        let api_base = match std::env::var(cog_core::contract::ci::GITEE_API_BASE_ENV) {
            Ok(base) if !base.trim().is_empty() => base,
            _ => {
                warn!(
                    env = cog_core::contract::ci::GITEE_API_BASE_ENV,
                    "task face sources configured but no platform endpoint; SOP dispatch stays off"
                );
                return None;
            }
        };
        Some(Self { api_base, sources })
    }
}

/// Per-face state carried across ticks of the one loop.
struct FaceState {
    last_hash: Option<String>,
    next_due: Instant,
}

/// What one face's tick ended in, for the `result` reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchOutcome {
    Compiled,
    SkippedUnchanged,
    FetchError,
    CompileError,
}

impl DispatchOutcome {
    fn cell(self) -> &'static str {
        match self {
            DispatchOutcome::Compiled => "compiled",
            DispatchOutcome::SkippedUnchanged => "skipped_unchanged",
            DispatchOutcome::FetchError => "fetch_error",
            DispatchOutcome::CompileError => "compile_error",
        }
    }
}

/// Start the resident SOP dispatch loop.
///
/// The loop starts only when at least one face has a non-zero cadence; a
/// configuration that names no usable face is a decision not to dispatch, and
/// starting a loop that would never do anything would make "disabled" and
/// "idle" read the same.
pub fn spawn_sop_dispatch_loop(
    planner: Arc<dyn ActionPlanner>,
    skills: Arc<RwLock<SkillRegistry>>,
    http: Arc<dyn HttpClient>,
    metrics: Option<Arc<dyn MetricsBackend>>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    shutdown: ShutdownSignal,
    config: SopDispatchConfig,
) {
    let sources: Vec<TaskFaceSource> = config
        .sources
        .iter()
        .filter(|s| s.cadence_secs > 0)
        .cloned()
        .collect();
    let Some(period) = sources.iter().map(|s| s.cadence_secs).min() else {
        info!("SOP dispatch disabled: no task face has a non-zero cadence");
        return;
    };
    let period = Duration::from_secs(period);
    let api_base = config.api_base.clone();
    info!(
        faces = sources.len(),
        period_secs = period.as_secs(),
        arbitrated = role.is_some(),
        "SOP dispatch enabled"
    );

    drop(cog_core::loop_health::spawn(
        SOP_DISPATCH_LOOP,
        cog_core::loop_health::Cadence::Periodic(period),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let planner = planner.clone();
            let skills = skills.clone();
            let http = http.clone();
            let metrics = metrics.clone();
            let role = role.clone();
            let shutdown = shutdown.clone();
            let sources = sources.clone();
            let api_base = api_base.clone();
            async move {
                // 角色按租约自己的节拍续；循环体里仍每轮问一次，丢了角色要在丢的
                // 那一轮就停下来。问不到也不动手——不知道是不是只有自己，和知道
                // 只有自己不是一回事。
                let hold =
                    cog_core::RoleHold::start(role, SOP_DISPATCH_ROLE, beat.clone(), &shutdown);
                let mut ticker = tokio::time::interval(period);
                // 第一次 tick 立即返回；先吃掉它，让第一轮读取落在起点之后一个
                // 周期，而不是插件刚 start 就和启动期抢同一把锁。
                ticker.tick().await;
                let now = Instant::now();
                let mut states: Vec<FaceState> = sources
                    .iter()
                    .map(|_| FaceState {
                        last_hash: None,
                        next_due: now,
                    })
                    .collect();
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    if !hold.may_act().await {
                        continue;
                    }
                    let now = Instant::now();
                    for (source, state) in sources.iter().zip(states.iter_mut()) {
                        if now < state.next_due {
                            continue;
                        }
                        state.next_due = now + Duration::from_secs(source.cadence_secs);
                        let tick = dispatch_face(
                            source,
                            &api_base,
                            &http,
                            &planner,
                            &skills,
                            &mut state.last_hash,
                        )
                        .await;
                        record(
                            &metrics,
                            metric_names::SOP_DISPATCH_TOTAL,
                            &source.id,
                            Some(tick.outcome.cell()),
                            1.0,
                        )
                        .await;
                        if tick.injected > 0 {
                            record(
                                &metrics,
                                metric_names::SOP_TASKS_INJECTED_TOTAL,
                                &source.id,
                                None,
                                tick.injected as f64,
                            )
                            .await;
                        }
                        if tick.unmapped_roles > 0 {
                            record(
                                &metrics,
                                metric_names::SOP_UNMAPPED_ROLE_TOTAL,
                                &source.id,
                                None,
                                tick.unmapped_roles as f64,
                            )
                            .await;
                        }
                    }
                }
            }
        },
    ));
}

/// What one face's turn produced: the outcome, how many tasks it put in the DAG,
/// and how many of the compiled tasks named a role no registered hand answers to.
struct FaceTick {
    outcome: DispatchOutcome,
    injected: usize,
    unmapped_roles: usize,
}

impl FaceTick {
    fn outcome_only(outcome: DispatchOutcome) -> Self {
        Self {
            outcome,
            injected: 0,
            unmapped_roles: 0,
        }
    }
}

/// Read one face's document and inject its tasks, returning what the tick ended
/// in. A face that fails does not abort the tick: the others still get their
/// turn, and the failure is a reading rather than a stalled loop.
async fn dispatch_face(
    source: &TaskFaceSource,
    api_base: &str,
    http: &Arc<dyn HttpClient>,
    planner: &Arc<dyn ActionPlanner>,
    skills: &Arc<RwLock<SkillRegistry>>,
    last_hash: &mut Option<String>,
) -> FaceTick {
    let sop_md = match fetch_sop(http, api_base, source).await {
        Ok(text) => text,
        Err(e) => {
            warn!(face = %source.id, error = %e, "SOP fetch failed");
            return FaceTick::outcome_only(DispatchOutcome::FetchError);
        }
    };

    let hash = blake3::hash(sop_md.as_bytes()).to_hex().to_string();
    if last_hash.as_deref() == Some(hash.as_str()) {
        return FaceTick::outcome_only(DispatchOutcome::SkippedUnchanged);
    }

    let tasks = match compile(source, &sop_md) {
        Ok(tasks) => tasks,
        Err(e) => {
            warn!(face = %source.id, error = %e, "SOP did not compile");
            // 记哈希只针对「编译成功」的文本：下一轮若文档没变，这里仍会重试，
            // 因为一次编译失败是文档的缺陷，它需要在下一次被重新读到并重新报出来，
            // 而不是被上次的哈希挡住。
            return FaceTick::outcome_only(DispatchOutcome::CompileError);
        }
    };

    let registry = skills.read().await;
    // 任务名下的 role 是 SOP 自己的说法，未必有手认领它。谁来接由执行器表说了算，
    // 不由 SOP 一侧的用词说了算，所以判据问的是注册表自己的角色→手映射。
    // 认领不上这件事必须可读：不数它，一个没人接的任务和一件正常派下去的任务
    // 在指标面上长得一模一样。
    let unmapped_roles = tasks
        .iter()
        .filter(|task| {
            let role = task
                .input
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or_default();
            registry.skill_for_role(role).is_none()
        })
        .count();

    let goal = format!("face:{}", source.id);
    match planner.process_goal(&goal, tasks, &registry).await {
        Ok(queued) => {
            // 注入成功才记住哈希：注入失败时下一轮要重试同一份文本。
            *last_hash = Some(hash);
            FaceTick {
                outcome: DispatchOutcome::Compiled,
                injected: queued.len(),
                unmapped_roles,
            }
        }
        Err(e) => {
            warn!(face = %source.id, error = %e, "SOP task injection failed");
            FaceTick::outcome_only(DispatchOutcome::FetchError)
        }
    }
}

/// Fetch a face's SOP text through the platform endpoint.
///
/// The endpoint answers the contents of one repository path. It may hand back
/// the raw document or the platform's contents envelope, so the body is read
/// once and both shapes are accepted — the caller wants the document either way,
/// and a second request to disambiguate would be a request spent on the client's
/// own uncertainty.
async fn fetch_sop(
    http: &Arc<dyn HttpClient>,
    api_base: &str,
    source: &TaskFaceSource,
) -> Result<String, String> {
    let url = format!(
        "{}/repos/{}/contents/{}",
        api_base.trim_end_matches('/'),
        source.repo,
        source.sop_path
    );
    let req = HttpRequest::get(url)
        .header("Accept", cog_core::CONTENT_TYPE_JSON)
        .timeout(30);
    let resp = http.execute(req).await.map_err(|e| e.to_string())?;
    if !resp.is_success() {
        return Err(format!("platform returned HTTP {}", resp.status));
    }
    let body = resp.text().map_err(|e| e.to_string())?;
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
        // Not JSON: the endpoint handed back the document itself.
        return Ok(body);
    };
    let Some(content) = value.get("content").and_then(|c| c.as_str()) else {
        return Err("the contents response carried no document".into());
    };
    // The envelope wraps the document in base64, and the platform wraps the
    // lines; every non-base64 byte has to come out before decoding.
    let compact: String = content.chars().filter(|c| !c.is_whitespace()).collect();
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(compact)
        .map_err(|e| format!("the document was not valid base64: {e}"))?;
    String::from_utf8(bytes).map_err(|e| format!("the document was not UTF-8: {e}"))
}

/// Write a per-face counter. A failed write only warns: a reading that cannot be
/// recorded must not stop the work it describes.
async fn record(
    metrics: &Option<Arc<dyn MetricsBackend>>,
    name: MetricName,
    face: &str,
    result: Option<&str>,
    value: f64,
) {
    let Some(metrics) = metrics else {
        return;
    };
    let mut labels = HashMap::new();
    labels.insert("face".to_string(), face.to_string());
    if let Some(result) = result {
        labels.insert("result".to_string(), result.to_string());
    }
    if let Err(e) = metrics.record_counter(name, value, labels).await {
        warn!(error = %e, series = name.as_str(), "SOP dispatch: could not record");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{HttpClient, HttpRequest, HttpResponse, SFResult};

    /// A client that answers one canned response and records the requests.
    #[derive(Debug)]
    struct CannedClient {
        status: u16,
        body: Vec<u8>,
        calls: std::sync::Mutex<Vec<String>>,
    }

    impl CannedClient {
        fn new(status: u16, body: impl Into<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self {
                status,
                body: body.into(),
                calls: std::sync::Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for CannedClient {
        async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
            self.calls.lock().unwrap().push(req.url.clone());
            Ok(HttpResponse {
                status: self.status,
                headers: HashMap::new(),
                body: self.body.clone(),
            })
        }
    }

    fn source() -> TaskFaceSource {
        TaskFaceSource {
            id: "example-face".into(),
            repo: "example/sop-repo".into(),
            sop_path: "SOP.md".into(),
            cadence_secs: 60,
        }
    }

    const SOP: &str = r#"
```yaml
face: example-face
tasks:
  - id: A1
    name: fact.verify
    face: facts
    role: fact-checker
    blocked_by: []
    product: facts/facts.yaml
```
"#;

    /// The contents endpoint is composed from the configured `owner/repo` and
    /// path, and its JSON envelope is unwrapped to the document text.
    #[tokio::test]
    async fn the_envelope_is_unwrapped_to_the_document() {
        use base64::Engine as _;
        let envelope = serde_json::json!({
            "encoding": "base64",
            "content": base64::engine::general_purpose::STANDARD.encode(SOP),
        })
        .to_string();
        let client = CannedClient::new(200, envelope);
        let http: Arc<dyn HttpClient> = client.clone();
        let text = fetch_sop(&http, "http://gw:8081/gitee", &source())
            .await
            .expect("fetch ok");
        assert_eq!(text, SOP);
        assert_eq!(
            client.calls.lock().unwrap().as_slice(),
            ["http://gw:8081/gitee/repos/example/sop-repo/contents/SOP.md"]
        );
    }

    /// A non-success status is a fetch error rather than a parse of the body.
    #[tokio::test]
    async fn a_non_success_status_is_an_error() {
        let client = CannedClient::new(503, "nope");
        let http: Arc<dyn HttpClient> = client;
        assert!(fetch_sop(&http, "http://gw:8081/gitee", &source())
            .await
            .is_err());
    }

    /// Nothing configured is nothing dispatched. The loop makes no outbound call
    /// on a deployment that has not named a face, so the configuration reader
    /// has to answer "none" rather than fall back to a default endpoint or an
    /// empty-but-present list. A half-read configuration counts as not
    /// configured for the same reason: being told to dispatch without being told
    /// where is not being told to dispatch.
    ///
    /// The variables are process-wide, so this test owns both of them by naming
    /// them here; nothing else in the crate reads either.
    #[test]
    fn a_deployment_that_names_no_face_dispatches_nothing() {
        let saved_sources = std::env::var(TASK_FACE_SOURCES_ENV).ok();
        let saved_base = std::env::var(cog_core::contract::ci::GITEE_API_BASE_ENV).ok();

        std::env::remove_var(TASK_FACE_SOURCES_ENV);
        std::env::set_var(cog_core::contract::ci::GITEE_API_BASE_ENV, "http://gw");
        assert!(SopDispatchConfig::from_env().is_none(), "unset");

        std::env::set_var(TASK_FACE_SOURCES_ENV, "   ");
        assert!(SopDispatchConfig::from_env().is_none(), "blank");

        std::env::set_var(TASK_FACE_SOURCES_ENV, "not json");
        assert!(SopDispatchConfig::from_env().is_none(), "unparsable");

        std::env::set_var(TASK_FACE_SOURCES_ENV, "[]");
        assert!(SopDispatchConfig::from_env().is_none(), "empty list");

        std::env::set_var(
            TASK_FACE_SOURCES_ENV,
            r#"[{"id":"f","repo":"o/r","sop_path":"S.md","cadence_secs":60}]"#,
        );
        std::env::remove_var(cog_core::contract::ci::GITEE_API_BASE_ENV);
        assert!(SopDispatchConfig::from_env().is_none(), "no endpoint");

        std::env::set_var(cog_core::contract::ci::GITEE_API_BASE_ENV, "http://gw");
        assert!(
            SopDispatchConfig::from_env().is_some(),
            "a named face and an endpoint is a complete configuration"
        );

        match saved_sources {
            Some(v) => std::env::set_var(TASK_FACE_SOURCES_ENV, v),
            None => std::env::remove_var(TASK_FACE_SOURCES_ENV),
        }
        match saved_base {
            Some(v) => std::env::set_var(cog_core::contract::ci::GITEE_API_BASE_ENV, v),
            None => std::env::remove_var(cog_core::contract::ci::GITEE_API_BASE_ENV),
        }
    }

    /// An unchanged document is skipped without being re-injected. The fake
    /// planner records every goal it is handed so the count is readable.
    #[tokio::test]
    async fn an_unchanged_document_is_not_injected_twice() {
        use base64::Engine as _;
        #[derive(Debug, Default)]
        struct CountingPlanner {
            goals: std::sync::Mutex<Vec<String>>,
        }
        #[async_trait::async_trait]
        impl ActionPlanner for CountingPlanner {
            async fn process_goal(
                &self,
                goal: &str,
                _tasks: Vec<cog_core::Task>,
                _skills: &SkillRegistry,
            ) -> SFResult<Vec<String>> {
                self.goals.lock().unwrap().push(goal.to_string());
                Ok(Vec::new())
            }
        }

        let envelope = serde_json::json!({
            "content": base64::engine::general_purpose::STANDARD.encode(SOP),
        })
        .to_string();
        let http: Arc<dyn HttpClient> = CannedClient::new(200, envelope);
        let planner: Arc<dyn ActionPlanner> = Arc::new(CountingPlanner::default());
        let skills = Arc::new(RwLock::new(SkillRegistry::from_skills(vec![])));

        let mut last_hash = None;
        let src = source();
        let first =
            dispatch_face(&src, "http://gw", &http, &planner, &skills, &mut last_hash).await;
        assert_eq!(first.outcome, DispatchOutcome::Compiled);
        assert_eq!(first.injected, 0, "the fake planner queues nothing");
        // 名下的 role 是 `fact-checker`，注册表里没有这只手——这正是要看得见的缺口。
        assert_eq!(first.unmapped_roles, 1);
        let second =
            dispatch_face(&src, "http://gw", &http, &planner, &skills, &mut last_hash).await;
        assert_eq!(second.outcome, DispatchOutcome::SkippedUnchanged);
        assert_eq!(second.unmapped_roles, 0);
    }
}
