//! Agent 自有配置段（agent_loop / agent_pool）——core config.rs 不聚合
//! 单 crate 配置。自读 cogneva.json 对应段并叠加
//! `COGNEVA_AGENT_LOOP_*` / `COGNEVA_AGENT_POOL_*` env 覆盖。

use serde::{Deserialize, Serialize};
use std::path::Path;

use cog_core::{SFError, SFResult};

fn load_section<T: serde::de::DeserializeOwned + Default>(
    pointer: &str,
    env_map: &[(&str, &str)],
) -> SFResult<T> {
    let path = cog_core::paths::config_file();
    load_section_from(Path::new(&path), pointer, env_map)
}

fn load_section_from<T: serde::de::DeserializeOwned + Default>(
    path: &Path,
    pointer: &str,
    env_map: &[(&str, &str)],
) -> SFResult<T> {
    let mut section = match std::fs::read_to_string(path) {
        Ok(text) => {
            let root: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
            root.pointer(pointer)
                .cloned()
                .unwrap_or(serde_json::json!({}))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
        Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
    };
    cog_core::config::apply_env_paths(&mut section, env_map);
    serde_json::from_value(section)
        .map_err(|e| SFError::Config(format!("{} {pointer}: {e}", path.display())))
}

/// The env-var names this section answers to. Published so the deploy-config
/// gate can tell "honored, but not by the core schema" from "honored by
/// nobody".
pub const AGENT_LOOP_ENV: &[(&str, &str)] = &[
    ("COGNEVA_AGENT_LOOP_MAX_ITERATIONS", "max_iterations"),
    (
        "COGNEVA_AGENT_LOOP_EVAL_MAX_ITERATIONS",
        "eval_max_iterations",
    ),
    ("COGNEVA_AGENT_LOOP_ROLE", "role"),
    (
        "COGNEVA_AGENT_LOOP_SKILL_CACHE_TTL_SECS",
        "skill_cache_ttl_secs",
    ),
    (
        "COGNEVA_AGENT_LOOP_THINK_STALL_TIMEOUT_SECS",
        "think_stall_timeout_secs",
    ),
    // The platform's own HTTP surface, borrowed the way the business pods
    // borrow the gateway (see `COGNEVA_GITHUB_API_BASE`): the loop archives an
    // oversized tool output by POSTing to it, carrying no token. Named without
    // the `AGENT_LOOP` prefix because it points at a shared endpoint rather
    // than a knob of this section.
    (cog_core::MEMORY_API_BASE_ENV, "memory_api_base"),
    (
        "COGNEVA_AGENT_LOOP_EXTERNAL_ARCHIVE_THRESHOLD_CHARS",
        "external_archive_threshold_chars",
    ),
    // 网页检索的地址：集群内自建的 SearXNG。与内存 API 同一条理由——它指向一个
    // 共享端点而不是本节的一个旋钮，所以名字不带 `AGENT_LOOP` 前缀。空表示这个
    // 部署没有接网页检索，工具在调用时报出这个名字，而不是从列表里消失。
    (crate::web::WEB_SEARCH_BASE_ENV, "web_search_base"),
    // 检索的后备端点：只在自建的 SearXNG 一个引擎都没应答时才用。不设就是不
    // 兜底——那不是一个缺省值，而是这个部署选择了让查询不出自己这棵树。
    (
        crate::web::WEB_SEARCH_FALLBACK_BASE_ENV,
        "web_search_fallback_base",
    ),
    // 网页渲染后端：设了就表示这个部署用真浏览器读页面（客户端渲染页与 PDF 才有
    // 正文），不设就是在进程内抽取。两者不是同一个能力，所以不互相兜底。
    (crate::web::WEB_FETCH_BASE_ENV, "web_fetch_base"),
];

const AGENT_POOL_ENV: &[(&str, &str)] = &[
    ("COGNEVA_AGENT_POOL_WORKER_COUNT", "worker_count"),
    ("COGNEVA_AGENT_POOL_WORKER_ROLE", "worker_role"),
];

/// Agent registration and heartbeat configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentLoopConfig {
    /// 缺省不是一个人。这个配置节给的是池子的**缺省参数**，池子里每一位
    /// 员工的名字由它自己被造出来时决定（管理器逐位覆盖）；从前这里写着
    /// `agent-001`，一个谁也没雇过、名册上没有的名字，读配置的人会以为
    /// 平台上真有这么一位。
    pub agent_id: String,
    pub role: String,
    /// 这一份部署给智能体写下的轮数天花板；`None` 表示**不设天花板**。
    ///
    /// 不设的时候，一轮什么时候结束由模型自己不再要工具来定——这正是交互面板
    /// 里那个员工跑的同一套循环。它在这里曾经是一个写死的数（10），而那个数对
    /// 每一件活都是同一个断言：比它长的活不存在。事实并非如此，撞上它的那些轮
    /// 只在读数里留下一句「预算用完」，读的人看不出这是界在裁剪工作还是工作在
    /// 迷路。写下一个数仍然有效——那是一个具体的部署决定，不是平台的缺省。
    ///
    /// 止损不靠轮数：单步停滞超时管「不吭声」，上下文窗口管「装不下」，成本记
    /// 在使用账上，任务由心跳回收。轮数是这三样都缺时的替代品。
    pub max_iterations: Option<u32>,
    /// 评测运行那一侧的天花板，语义同上。它比生产那一侧窄曾经是有理由的
    /// （判一份交上来的结果用不了那么多轮），但同样是一个断言而不是一条度量，
    /// 所以同样交给部署去写。
    pub eval_max_iterations: Option<u32>,
    /// TTL for the available_skills cache in AgentRuntime (seconds).
    pub skill_cache_ttl_secs: u64,
    /// Abort a streaming think turn only after no stream event arrives for
    /// this many consecutive seconds (stall detection, not a wall-clock cap).
    ///
    /// Zero is refused at load: a zero-second window is not "abort at once" as
    /// a setting — it kills every stream at its first quiet moment, and the
    /// value that would actually run would be some other number than the one
    /// written here.
    pub think_stall_timeout_secs: u64,
    /// Base URL of the platform memory API where an oversized tool output is
    /// archived. Absent when the deployment names none; the loop then still
    /// truncates, and the failure reading carries the cause.
    pub memory_api_base: Option<String>,
    /// How large an `http_request` response may be before it is stored as an
    /// external document and the tool hands back a reference instead of the
    /// body, in characters.
    ///
    /// Measured in characters, not bytes, because it is compared against the
    /// text the tool would otherwise return and the tool-return budget is a
    /// character count too. A response at or below this stays in the
    /// conversation as it always has; one above it becomes its own raw source,
    /// which is what gives externally fetched material a face of its own
    /// instead of leaving it as one more turn of a transcript.
    ///
    /// Keep it under the tool-return budget so the preview the tool returns in
    /// place of the body still fits one turn -- above that the preview would be
    /// cut again on the way into the context, and the second cut is the one
    /// that drops the reference.
    #[serde(default = "default_external_archive_threshold_chars")]
    pub external_archive_threshold_chars: usize,
    /// Base URL of the SearXNG instance behind the `web_search` tool. Absent
    /// when the deployment runs none; the tool then stays in the list and says
    /// which setting is missing, so "no web access here" is not read as "no
    /// such tool".
    pub web_search_base: Option<String>,
    /// Base URL of a second search backend, used only when the first one did
    /// not answer at all -- every engine silent, twice. Absent means the
    /// deployment has chosen that a query never leaves this tree; present
    /// means an unanswered query is put to somewhere else, and the answer it
    /// returns says which backend produced it.
    ///
    /// It is a fallback rather than a peer on purpose. While the self-hosted
    /// backend answers, the property "the query stays in this deployment" is
    /// untouched; the only query that leaves is one that was never asked of
    /// anything here, so nothing is given up that was being held.
    pub web_search_fallback_base: Option<String>,
    /// Base URL of the page-rendering backend behind `web_fetch`. Absent means
    /// pages are read in process, which cannot run scripts; present means the
    /// fetch is rendered in a real browser, and a backend that does not answer
    /// is an error rather than a downgrade.
    pub web_fetch_base: Option<String>,
}

/// The size at which a fetched response stops being a conversational answer and
/// starts being a document: comfortably above what an API or a page summary
/// returns, comfortably below the tool-return budget of the smallest window the
/// loop is configured with.
// cap: bound — 一头是 API/网页摘要的典型长度，另一头是最小窗口的工具返回预算
const EXTERNAL_ARCHIVE_THRESHOLD_CHARS: usize = 8192;

fn default_external_archive_threshold_chars() -> usize {
    EXTERNAL_ARCHIVE_THRESHOLD_CHARS
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            agent_id: String::new(),
            role: cog_core::roles::PLANNER.into(),
            max_iterations: None,
            eval_max_iterations: None,
            skill_cache_ttl_secs: cog_core::config::SKILL_HOT_RELOAD_INTERVAL_SECS,
            // 停滞窗的声明在运行时契约里（`cog_core::RuntimeConfig`），那边也
            // 正是读它的地方；这里读那一份，不再抄一个 240。
            think_stall_timeout_secs: cog_core::RuntimeConfig::default().think_stall_timeout_secs,
            memory_api_base: None,
            external_archive_threshold_chars: default_external_archive_threshold_chars(),
            web_search_base: None,
            web_search_fallback_base: None,
            web_fetch_base: None,
        }
    }
}

impl AgentLoopConfig {
    pub fn load() -> SFResult<Self> {
        let config: Self = load_section("/agent_loop", AGENT_LOOP_ENV)?;
        config.refuse_a_window_that_cannot_be_a_window()?;
        Ok(config)
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        let config: Self = load_section_from(path, "/agent_loop", AGENT_LOOP_ENV)?;
        config.refuse_a_window_that_cannot_be_a_window()?;
        Ok(config)
    }

    /// 停滞窗是「多久没有收到流事件才算死」：0 秒不是一个窗口，是「每一条流在
    /// 第一个静默瞬间就被判死」。读它的那一层从前把这个 0 悄悄换成 1 秒——声明的
    /// 值与生效的值于是有两个，而写配置的人读不到后一个。声明在哪里拒在哪里。
    fn refuse_a_window_that_cannot_be_a_window(&self) -> SFResult<()> {
        if self.think_stall_timeout_secs == 0 {
            return Err(SFError::Config(
                "agent_loop.think_stall_timeout_secs 声明为 0 秒：停滞窗不是「立刻算死」的另一种写法。给它一个周期。"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl From<AgentLoopConfig> for cog_core::RuntimeConfig {
    fn from(c: AgentLoopConfig) -> Self {
        Self {
            agent_id: c.agent_id,
            role: c.role,
            // 没有写下的天花板就是没有天花板：运行面只有一个数表示「不设界」，
            // 不让「没配置」在别处变成一个新的缺省数。
            max_iterations: c.max_iterations.unwrap_or(u32::MAX),
            // 这一格没有配置来源：窗口由运行时从型号公布的规格与实测里定，
            // 构造时写下的只是「还没量到」。
            context_window_size: None,
            skill_cache_ttl_secs: c.skill_cache_ttl_secs,
            think_stall_timeout_secs: c.think_stall_timeout_secs,
            skill_config: None,
            crew_id: None,
            squad_id: None,
            owner_task_id: None,
            // 配置文件不知道这次要跑哪件活；声明由拿到任务的那一层补。
            deliverable: cog_core::Deliverable::Unstated,
            deliverable_state: cog_core::DeclarationState::Unstated,
            stated_directions: Vec::new(),
        }
    }
}

/// Agent 池配置（worker 数量与角色）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentManagerConfig {
    /// 操作者自己写下的并发上限；0 表示不写，此时只有资源读数约束并发。
    ///
    /// 它曾经是并发本身：写 3 就同时跑 3 个任务，谁也不问容器装不装得下。
    /// 现在并发由 `pool_sizing` 按容器内存与单任务实测开销算出来，这里写下的
    /// 数只在它比实测更紧时生效。写 0 就是不设这个上限。
    pub worker_count: usize,
    pub worker_role: String,
}

impl Default for AgentManagerConfig {
    fn default() -> Self {
        Self {
            worker_count: 0,
            worker_role: cog_core::roles::PLANNER.into(),
        }
    }
}

impl AgentManagerConfig {
    pub fn load() -> SFResult<Self> {
        load_section("/agent_pool", AGENT_POOL_ENV)
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        load_section_from(path, "/agent_pool", AGENT_POOL_ENV)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 声明的停滞窗原样生效；0 秒被点名拒绝，而不是在读到它的那一层被换成 1。
    #[test]
    fn a_declared_stall_window_survives_and_zero_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cogneva.json");
        std::fs::write(
            &path,
            serde_json::json!({"agent_loop": {"think_stall_timeout_secs": 90}}).to_string(),
        )
        .unwrap();
        let declared = AgentLoopConfig::load_from(&path).unwrap();
        assert_eq!(declared.think_stall_timeout_secs, 90);

        std::fs::write(
            &path,
            serde_json::json!({"agent_loop": {"think_stall_timeout_secs": 0}}).to_string(),
        )
        .unwrap();
        let refused = AgentLoopConfig::load_from(&path).unwrap_err();
        let message = refused.to_string();
        assert!(
            message.contains("think_stall_timeout_secs"),
            "the refusal must name the field: {message}"
        );
    }

    #[test]
    fn defaults_match_legacy_core_values() {
        let l = AgentLoopConfig::default();
        // 池子的缺省参数不替任何一位员工起名：名字由管理器逐位覆盖，这里空着
        // 才是实情。写一个名字进来，读配置的人会以为平台上真有那一位。
        assert!(l.agent_id.is_empty());
        assert_eq!(l.role, "planner");
        // No number written means no ceiling: a default here would be an
        // assertion about every job, made before any of them has run.
        assert_eq!(l.max_iterations, None);
        assert_eq!(l.eval_max_iterations, None);
        let p = AgentManagerConfig::default();
        // 0 = 不声明上限：默认并发由资源读数决定，不再是一个写死的数。
        assert_eq!(p.worker_count, 0);
    }

    /// 没写下的天花板，转成运行配置后就是不设天花板。
    ///
    /// 这一跳是整条链上唯一把「部署没有表态」翻译成运行面数字的地方。它要是自己
    /// 补一个数，前面那些「不写就是不设界」的注释就全是假话，而读配置的人会以为
    /// 这个部署已经把轮数交回给模型了。
    #[test]
    fn a_ceiling_nobody_wrote_does_not_appear_in_the_runtime_config() {
        let runtime: cog_core::RuntimeConfig = AgentLoopConfig::default().into();
        assert_eq!(runtime.max_iterations, u32::MAX);

        let written: cog_core::RuntimeConfig = AgentLoopConfig {
            max_iterations: Some(12),
            ..AgentLoopConfig::default()
        }
        .into();
        assert_eq!(written.max_iterations, 12);
    }

    /// 随这份仓发布的配置不给智能体写轮数。
    ///
    /// 写一个数就是对每一件活下的同一个断言：比它长的活不存在。撞上它的那些轮只在
    /// 读数里留一句「预算用完」，读的人分不出这是界在裁剪工作还是工作在迷路。部署
    /// 真要设界，写在它自己的 values / 配置里——那是这个部署的决定，不是随产品发出去
    /// 的缺省。
    #[test]
    fn the_shipped_config_writes_no_iteration_ceiling() {
        let root = crate::checkout::repo_root();
        for rel in ["deploy/helm/cogneva/files/cogneva.json"] {
            let path = root.join(rel);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
            let root_json: serde_json::Value = serde_json::from_str(&text).expect("config is JSON");
            let section = root_json
                .pointer("/agent_loop")
                .unwrap_or_else(|| panic!("{rel}: agent_loop section present"));
            for key in ["max_iterations", "eval_max_iterations"] {
                assert!(
                    section.get(key).is_none(),
                    "{rel}: agent_loop.{key} 又写了一个数——这是给所有活下的同一个断言"
                );
            }
        }
    }

    /// 文档里没有窗口这一格，写成什么都不认。
    ///
    /// 窗口只能来自读数：设置页里那只型号公布的规格，加上这棵树实测到底收下过
    /// 多大、被拒时上游自述多大的界。文档里写一个数就是替上游拍一个它没公布过的
    /// 窗口——写小了裁掉装得下的历史，写大了第一次调用就被拒。所以这一格连同
    /// 它的环境变量映射一起删掉了，不是改成「写 null」。
    #[test]
    fn the_document_has_no_window_slot() {
        let rel = "deploy/helm/cogneva/files/cogneva.json";
        let path = crate::checkout::repo_root().join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
        let root_json: serde_json::Value = serde_json::from_str(&text).expect("config is JSON");
        let section = root_json
            .pointer("/agent_loop")
            .unwrap_or_else(|| panic!("{rel}: agent_loop section present"));
        assert!(
            section.get("context_window_size").is_none(),
            "{rel}: agent_loop 里又出现了 context_window_size——窗口只由型号规格与实测给出"
        );
        assert!(
            !text.contains("COGNEVA_AGENT_LOOP_CONTEXT_WINDOW_SIZE"),
            "{rel}: 环境变量映射里还留着窗口——没有这一格就没有要映射的东西"
        );
        // 环境变量那一头也只认 `AGENT_LOOP_ENV` 里的名字：表上没有了，就没有
        // 一条 env 能把窗口递进来。
        assert!(
            !AGENT_LOOP_ENV
                .iter()
                .any(|(_, field)| *field == "context_window_size"),
            "环境变量映射表里还留着窗口：没有这一格，就没有能递进一个数的名字"
        );
    }

    #[test]
    fn the_default_external_archive_threshold_stays_under_the_tool_return_budget() {
        // A response over the threshold is stored and comes back as a preview
        // of at most that many characters. That preview is the turn, and the
        // loop archives any turn that does not fit -- so a threshold at or
        // above the budget would make the loop store the preview too, a second
        // copy of a document already stored, and count a cut for it. The two
        // stores stay disjoint only while the threshold is below the budget of
        // the smallest window the loop runs with.
        let threshold = default_external_archive_threshold_chars();
        assert_eq!(threshold, EXTERNAL_ARCHIVE_THRESHOLD_CHARS);
        assert!(threshold < crate::context::tool_result_budget_chars(Some(32000)));
    }

    #[test]
    fn the_memory_api_base_env_is_honored_by_this_section() {
        // The archive call borrows the platform's own HTTP surface by env, the
        // way the business pods borrow the gateway. If the name drops out of
        // this table the deploy-config gate stops being able to tell "honored"
        // from "honored by nobody", and the loop silently falls back to
        // unconfigured.
        assert!(AGENT_LOOP_ENV.contains(&(cog_core::MEMORY_API_BASE_ENV, "memory_api_base")));
    }

    #[test]
    fn reads_section_and_missing_file_defaults() {
        let p = Path::new("/nonexistent/cogneva.json");
        assert_eq!(AgentLoopConfig::load_from(p).unwrap().role, "planner");
        // 文件里没写上限就当作没声明：并发交给资源读数定。
        assert_eq!(AgentManagerConfig::load_from(p).unwrap().worker_count, 0);

        let dir = std::env::temp_dir().join(format!("cog-agent-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"agent_loop": {"role": "evaluator", "max_iterations": 3,
                    "eval_max_iterations": 7, "memory_api_base": "http://cogneva:8080"},
                "agent_pool": {"worker_count": 8}}"#,
        )
        .unwrap();
        let l = AgentLoopConfig::load_from(&path).unwrap();
        assert_eq!(l.role, "evaluator");
        assert_eq!(l.max_iterations, Some(3));
        assert_eq!(l.eval_max_iterations, Some(7));
        assert_eq!(l.memory_api_base.as_deref(), Some("http://cogneva:8080"));
        assert_eq!(
            AgentManagerConfig::load_from(&path).unwrap().worker_count,
            8
        );
        let rt: cog_core::RuntimeConfig = l.into();
        assert_eq!(rt.role, "evaluator");
        std::fs::remove_dir_all(&dir).ok();
    }
}
