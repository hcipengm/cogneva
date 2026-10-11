//! Toolathlon 的四件适配器：数据、环境、工具包、判分。
//!
//! 这一列与另两列的分野：环境是**一套 Docker Compose 工程**（postgres ＋ 这道题声明的
//! MCP server），工具面就是那些 MCP server，判分是**确定性脚本**（每题自己的
//! `evaluation/main.py`）。数据不是一个大文件，而是一棵**每题一个目录**的树：目录里有
//! 任务陈述、`task_config.json`、`preprocess/main.py`、`evaluation/main.py`、groundtruth。
//!
//! 所以这一列的两个外部依赖都从外面注入：起 compose、跑官方判分脚本是平台侧的事
//! （[`ToolathlonBackend`]），MCP server 的实现也是（[`ToolathlonToolkit`] 只认名字）。
//! 缺任何一件都必须**报错**，不是给一个空环境或空工具面让分数悄悄掉下去。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::Tool;
use serde::Deserialize;

use crate::bench::{CaseJudge, CaseSource, EnvProvider, Toolkit, Verdict};
use crate::dataset::EvalCase;
use crate::scaffold::{AgentOutput, CaseEnv, ToolSet};

/// 每题一个目录的那棵树的根在数据根里的相对路径。
pub const TOOLATHLON_TASKS_DIR: &str = "toolathlon-gym/tasks/finalpool";

/// 数据适配器：一棵每题一个目录的树 → 一批 case。
///
/// 目录里没有 `task_config.json` 的**不是一道题**（树里还带着 `.utils` 这样的辅助目录），
/// 跳过；这件事有真实数据上的行数断言守着，静默少读几题会被逮住。
pub struct ToolathlonCaseSource;

#[derive(Debug, Default, Deserialize)]
struct TaskConfig {
    #[serde(default)]
    needed_mcp_servers: Vec<String>,
    #[serde(default)]
    needed_local_tools: Vec<String>,
}

impl ToolathlonCaseSource {
    fn dir_to_case(dir: &Path) -> anyhow::Result<EvalCase> {
        let name = dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        let cfg_path = dir.join("task_config.json");
        let cfg: TaskConfig = serde_json::from_str(
            &std::fs::read_to_string(&cfg_path)
                .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", cfg_path.display()))?,
        )
        .map_err(|e| anyhow::anyhow!("{}: {e}", cfg_path.display()))?;

        let task_path = dir.join("docs/task.md");
        let task = std::fs::read_to_string(&task_path)
            .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", task_path.display()))?;

        let mut metadata = HashMap::new();
        metadata.insert("task_name".into(), name.clone());
        // 判分与环境都要回到这个目录（跑 preprocess / evaluation），所以把它**加载时**的
        // 绝对路径带上——离开这棵树这一个 case 就没法复现，这一点要在元数据里看得见。
        metadata.insert("task_dir".into(), dir.display().to_string());
        metadata.insert(
            "needed_mcp_servers".into(),
            serde_json::to_string(&cfg.needed_mcp_servers)?,
        );
        metadata.insert(
            "needed_local_tools".into(),
            serde_json::to_string(&cfg.needed_local_tools)?,
        );
        let prompt_path = dir.join("docs/agent_system_prompt.md");
        if let Ok(prompt) = std::fs::read_to_string(&prompt_path) {
            metadata.insert("agent_system_prompt".into(), prompt);
        }
        metadata.insert(
            "has_groundtruth".into(),
            dir.join("groundtruth_workspace").is_dir().to_string(),
        );

        let mut tags = vec!["toolathlon".to_string()];
        tags.extend(cfg.needed_mcp_servers.iter().map(|s| format!("server:{s}")));

        Ok(EvalCase {
            id: format!("toolathlon-{name}"),
            name: name.clone(),
            // 外壳看到的只是任务陈述；判分脚本、groundtruth、要哪些 server 是判分方与环境方的。
            input: serde_json::json!({ "task": task }),
            // 判分是「官方脚本退出码 0」的 0/1，不是答案相等，这里不挂一个会误导的指标名。
            expected_output: None,
            expected_tools: None,
            tags,
            metrics: vec![],
            metadata,
        })
    }
}

#[async_trait]
impl CaseSource for ToolathlonCaseSource {
    fn cases(&self, root: &Path) -> anyhow::Result<Vec<EvalCase>> {
        let tasks = root.join(TOOLATHLON_TASKS_DIR);
        let entries = std::fs::read_dir(&tasks).map_err(|e| {
            anyhow::anyhow!(
                "cannot read {}: {e} (Toolathlon ships as a tarball of one directory per task; \
                 run the fetch script to extract it under the same --dest)",
                tasks.display()
            )
        })?;

        // 按名字排序：目录的枚举顺序是文件系统的，题序若随它变，两次跑的种子就对不上同一题。
        let mut dirs: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir() && p.join("task_config.json").is_file())
            .collect();
        dirs.sort();

        let mut cases = Vec::with_capacity(dirs.len());
        for dir in &dirs {
            cases.push(Self::dir_to_case(dir)?);
        }
        Ok(cases)
    }
}

/// 一道题里的一台 MCP server 的通过与否。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolathlonCheck {
    pub name: String,
    pub passed: bool,
}

/// 一次官方判分跑的回报。
#[derive(Debug, Clone, Default)]
pub struct ToolathlonReport {
    /// 官方 `evaluation/main.py` 的进程退出码：`0` = 全过，`1` = 有检查没过。
    /// 其它值是判分脚本**自己没跑出判词**（崩了 / 被杀），那不是一个判词。
    pub exit_code: i32,
    /// 脚本自己打印的逐条检查（给归因用；判词只看退出码）。
    pub checks: Vec<ToolathlonCheck>,
    /// 判分脚本的原始输出。
    pub raw: String,
}

/// Toolathlon 的 Compose 后端。评测台**声明它、不实现它**：起 postgres 与题目声明的
/// MCP server、跑 preprocess 与 evaluation，都归平台侧。
#[async_trait]
pub trait ToolathlonBackend: Send + Sync {
    /// 起这道题的 Compose 环境、跑 `preprocess/main.py` 铺好工作区，返回环境句柄。
    async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>>;

    /// 跑官方 `evaluation/main.py`，回报退出码与逐条检查。
    async fn evaluate(
        &self,
        env: &Arc<dyn CaseEnv>,
        case: &EvalCase,
    ) -> anyhow::Result<ToolathlonReport>;
}

/// 环境适配器：把起 Compose 交给后端，自己只保证「没有后端时报错、不是判错」。
pub struct ToolathlonEnvProvider {
    backend: Option<Arc<dyn ToolathlonBackend>>,
}

impl ToolathlonEnvProvider {
    pub fn new(backend: Option<Arc<dyn ToolathlonBackend>>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl EnvProvider for ToolathlonEnvProvider {
    async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
        let Some(backend) = &self.backend else {
            anyhow::bail!(
                "Toolathlon runs each task against a Docker Compose project and no compose backend \
                 is wired (case {}); a case scored without one would read as the model failing the \
                 task",
                case.id
            );
        };
        backend.acquire(case).await
    }
}

/// Toolathlon 的工具包：这道题声明的那些 MCP server。
///
/// 工具集由**基准**定、不由方法定，但这里与 HLE / SWE-bench Pro 不同：要哪几件由**每题**
/// 的 `task_config.json` 定，不是一个固定清单。所以这一层持的是「名字 → 实现」的登记处，
/// 判分器按每题的声明挑选；**声明里有、登记处里没有的 server 直接报错**——少给一台
/// server 会让这一题的工具面比原题小，分数就不可归因了。
pub struct ToolathlonToolkit {
    registry: Option<BTreeMap<String, Tool>>,
}

impl ToolathlonToolkit {
    /// 带工具的那一套：登记处按名字建，重复的名字是错误（谁是那台 server 就说不清了）。
    pub fn with_tools(tools: Vec<Tool>) -> anyhow::Result<Self> {
        let mut registry = BTreeMap::new();
        for tool in tools {
            let name = tool.name.clone();
            if registry.insert(name.clone(), tool).is_some() {
                anyhow::bail!("Toolathlon tools are registered by name; `{name}` appears twice");
            }
        }
        Ok(Self {
            registry: Some(registry),
        })
    }

    /// 关工具的那一套（同一 harness 只关工具的那一次跑）。
    pub fn without_tools() -> Self {
        Self { registry: None }
    }
}

impl Toolkit for ToolathlonToolkit {
    fn toolset(&self, case: &EvalCase, _env: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet> {
        let Some(registry) = &self.registry else {
            return Ok(ToolSet::empty());
        };
        let wanted: Vec<String> = serde_json::from_str(
            case.metadata
                .get("needed_mcp_servers")
                .map(String::as_str)
                .unwrap_or("[]"),
        )?;
        let mut picked = Vec::with_capacity(wanted.len());
        let mut missing = Vec::new();
        for name in &wanted {
            match registry.get(name) {
                Some(tool) => picked.push(tool.clone()),
                None => missing.push(name.clone()),
            }
        }
        if !missing.is_empty() {
            anyhow::bail!(
                "case {} declares MCP servers that are not registered: {missing:?}; a smaller tool \
                 face would score the task for the wrong reason",
                case.id
            );
        }
        ToolSet::new(picked)
    }
}

/// 判分适配器：跑官方的 `evaluation/main.py`，不叫任何模型。
pub struct ToolathlonJudge {
    backend: Option<Arc<dyn ToolathlonBackend>>,
}

impl ToolathlonJudge {
    pub fn new(backend: Option<Arc<dyn ToolathlonBackend>>) -> Self {
        Self { backend }
    }
}

/// 判分规则本身（纯函数，可单独验）：官方脚本的退出码 `0` = 全过、`1` = 有检查没过；
/// **别的退出码不是判词**——脚本自己崩了/被杀，那时这一格该记 `errored`，不是判错。
fn grade(exit_code: i32, checks: &[ToolathlonCheck], raw: &str) -> anyhow::Result<Verdict> {
    let failed: Vec<&str> = checks
        .iter()
        .filter(|c| !c.passed)
        .map(|c| c.name.as_str())
        .collect();
    match exit_code {
        0 => Ok(Verdict::pass(format!(
            "official evaluation passed ({}/{} checks)",
            checks.len(),
            checks.len()
        ))),
        1 => Ok(Verdict::fail(if failed.is_empty() {
            // 脚本判了「没过」但没给逐条检查：不能因此翻成通过，只能如实说没有归因。
            format!(
                "official evaluation failed (exit 1) but named no check: {}",
                tail(raw)
            )
        } else {
            format!("official evaluation failed: {}", failed.join(" | "))
        })),
        other => anyhow::bail!(
            "the official evaluation did not run to a verdict (exit {other}); a crashed grader is \
             not a wrong answer, so this case must be reported as errored"
        ),
    }
}

fn tail(raw: &str) -> String {
    let s: String = raw.chars().rev().take(500).collect();
    s.chars().rev().collect()
}

#[async_trait]
impl CaseJudge for ToolathlonJudge {
    async fn judge(
        &self,
        case: &EvalCase,
        env: &Arc<dyn CaseEnv>,
        _output: &AgentOutput,
    ) -> anyhow::Result<Verdict> {
        let Some(backend) = &self.backend else {
            anyhow::bail!(
                "Toolathlon judges by running the task's own evaluation/main.py and no compose \
                 backend is wired (case {}); a case judged without one would be scored as wrong \
                 when it was never run",
                case.id
            );
        };
        let report = backend.evaluate(env, case).await?;

        let mut verdict = grade(report.exit_code, &report.checks, &report.raw)?;
        verdict
            .readings
            .insert("exit_code".into(), report.exit_code.to_string());
        verdict
            .readings
            .insert("checks_total".into(), report.checks.len().to_string());
        verdict.readings.insert(
            "checks_passed".into(),
            report
                .checks
                .iter()
                .filter(|c| c.passed)
                .count()
                .to_string(),
        );
        verdict
            .readings
            .insert("report".into(), report.raw.chars().take(2000).collect());
        Ok(verdict)
    }
}

/// 把四件拼成一个 Toolathlon 基准，交给运行器。
///
/// `backend` 是 Compose 后端：可达时给一个实现，缺席时给 `None`——那时起环境与判分都
/// 报错，这一列读起来是「没跑成」而不是「分数低」。`tools` 决定带工具还是关工具的那一次。
pub fn toolathlon_benchmark(
    backend: Option<Arc<dyn ToolathlonBackend>>,
    tools: ToolathlonToolkit,
) -> crate::bench::Benchmark {
    crate::bench::Benchmark {
        name: "toolathlon".into(),
        metric: "pass@1".into(),
        cases: Arc::new(ToolathlonCaseSource),
        env: Arc::new(ToolathlonEnvProvider::new(backend.clone())),
        tools: Arc::new(tools),
        judge: Arc::new(ToolathlonJudge::new(backend)),
        budget: crate::scaffold::Budget::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{Budget, FinishReason, NoEnv};
    use cog_core::{ToolImplementation, Usage};

    fn tempdir() -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "toolathlon-test-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(d.join(TOOLATHLON_TASKS_DIR)).unwrap();
        d
    }

    /// 在树里放一道题：task_config.json + docs/task.md（+ 可选其它）。
    fn write_task(root: &Path, name: &str, servers: &[&str]) -> PathBuf {
        let dir = root.join(TOOLATHLON_TASKS_DIR).join(name);
        std::fs::create_dir_all(dir.join("docs")).unwrap();
        std::fs::write(
            dir.join("task_config.json"),
            serde_json::json!({
                "needed_mcp_servers": servers,
                "needed_local_tools": ["claim_done", "python_execute"],
                "meta": {},
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(dir.join("docs/task.md"), format!("do the {name} task")).unwrap();
        dir
    }

    #[test]
    fn each_task_directory_becomes_a_case_in_a_stable_order() {
        let root = tempdir();
        // 故意乱序建目录，读回来必须按名字有序。
        write_task(&root, "zeta", &["notion"]);
        write_task(&root, "alpha", &["excel", "filesystem"]);
        // 不是一道题的目录（树里真带着 `.utils` 这种）要被跳过，而不是把整棵树读崩。
        std::fs::create_dir_all(root.join(TOOLATHLON_TASKS_DIR).join(".utils")).unwrap();

        let cases = ToolathlonCaseSource.cases(&root).unwrap();
        assert_eq!(cases.len(), 2);
        assert_eq!(cases[0].id, "toolathlon-alpha");
        assert_eq!(cases[1].id, "toolathlon-zeta");
        assert_eq!(cases[0].input["task"], "do the alpha task");
        assert_eq!(
            cases[0].metadata["needed_mcp_servers"],
            r#"["excel","filesystem"]"#
        );
        assert_eq!(cases[0].metadata["has_groundtruth"], "false");
        assert!(cases[0].tags.contains(&"server:excel".to_string()));
        // 判分脚本与要哪些 server 不进外壳的输入。
        assert!(cases[0].input.get("needed_mcp_servers").is_none());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_missing_tree_names_what_to_run() {
        let root = tempdir();
        std::fs::remove_dir_all(root.join(TOOLATHLON_TASKS_DIR)).ok();
        let err = ToolathlonCaseSource.cases(&root).unwrap_err().to_string();
        assert!(err.contains("tarball"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    fn tool(name: &str) -> Tool {
        Tool {
            name: name.into(),
            description: name.into(),
            parameters: serde_json::json!({"type": "object"}),
            implementation: ToolImplementation::Native(Arc::new(|_| {
                Box::pin(async move { Ok(serde_json::json!(null)) })
            })),
        }
    }

    fn case(id: &str, servers: &[&str]) -> EvalCase {
        let mut metadata = HashMap::new();
        metadata.insert(
            "needed_mcp_servers".into(),
            serde_json::to_string(servers).unwrap(),
        );
        EvalCase {
            id: id.into(),
            name: id.into(),
            input: serde_json::json!({"task": "x"}),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata,
        }
    }

    #[test]
    fn the_tool_face_is_exactly_what_the_task_declares() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let kit =
            ToolathlonToolkit::with_tools(vec![tool("notion"), tool("excel"), tool("filesystem")])
                .unwrap();
        // 只给这道题声明的那几件。
        let set = kit
            .toolset(&case("t1", &["excel", "filesystem"]), &env)
            .unwrap();
        assert_eq!(set.definitions().len(), 2);
        // 声明里有、登记处里没有 ⇒ 报错，不给一个更小的工具面。
        let err = kit
            .toolset(&case("t2", &["excel", "woocommerce"]), &env)
            .unwrap_err()
            .to_string();
        assert!(err.contains("woocommerce"), "{err}");
        // 重复注册是错误。
        assert!(ToolathlonToolkit::with_tools(vec![tool("excel"), tool("excel")]).is_err());
        // 关工具的那一套。
        assert!(ToolathlonToolkit::without_tools()
            .toolset(&case("t3", &["excel"]), &env)
            .unwrap()
            .is_empty());
    }

    fn output() -> AgentOutput {
        AgentOutput {
            final_answer: String::new(),
            trace: vec![],
            tokens: Usage::default(),
            finish: FinishReason::Answered,
        }
    }

    fn check(name: &str, passed: bool) -> ToolathlonCheck {
        ToolathlonCheck {
            name: name.into(),
            passed,
        }
    }

    /// 一个按预置回报判分结果的后端，不真起 compose。
    struct ScriptedBackend {
        report: ToolathlonReport,
    }

    #[async_trait]
    impl ToolathlonBackend for ScriptedBackend {
        async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
            Ok(Arc::new(NoEnv))
        }
        async fn evaluate(
            &self,
            _env: &Arc<dyn CaseEnv>,
            _case: &EvalCase,
        ) -> anyhow::Result<ToolathlonReport> {
            Ok(self.report.clone())
        }
    }

    fn backend(report: ToolathlonReport) -> Arc<dyn ToolathlonBackend> {
        Arc::new(ScriptedBackend { report })
    }

    #[tokio::test]
    async fn the_judge_reads_the_official_scripts_verdict() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);

        // 退出码 0 ⇒ 通过。
        let judge = ToolathlonJudge::new(Some(backend(ToolathlonReport {
            exit_code: 0,
            checks: vec![check("Excel", true), check("Notion", true)],
            raw: "SUMMARY Overall: PASS".into(),
        })));
        let v = judge
            .judge(&case("c1", &["excel"]), &env, &output())
            .await
            .unwrap();
        assert!(v.resolved, "{}", v.detail);
        assert_eq!(v.readings["exit_code"], "0");
        assert_eq!(v.readings["checks_passed"], "2");

        // 退出码 1 ⇒ 失败，且点名没过的那条检查。
        let judge = ToolathlonJudge::new(Some(backend(ToolathlonReport {
            exit_code: 1,
            checks: vec![check("Excel", true), check("Notion", false)],
            raw: "SUMMARY Overall: FAIL".into(),
        })));
        let v = judge
            .judge(&case("c2", &["excel"]), &env, &output())
            .await
            .unwrap();
        assert!(!v.resolved);
        assert!(v.detail.contains("Notion"), "{}", v.detail);

        // 退出码 1 但没给出逐条检查：仍是失败（不能翻成通过），判词如实说没有归因。
        let judge = ToolathlonJudge::new(Some(backend(ToolathlonReport {
            exit_code: 1,
            checks: vec![],
            raw: "boom".into(),
        })));
        let v = judge
            .judge(&case("c3", &["excel"]), &env, &output())
            .await
            .unwrap();
        assert!(!v.resolved);
        assert!(v.detail.contains("named no check"), "{}", v.detail);
    }

    #[tokio::test]
    async fn a_grader_that_did_not_run_to_a_verdict_is_an_error_not_a_fail() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let judge = ToolathlonJudge::new(Some(backend(ToolathlonReport {
            exit_code: 137,
            checks: vec![],
            raw: "killed".into(),
        })));
        let err = judge
            .judge(&case("c", &["excel"]), &env, &output())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("errored"), "{err}");

        // 没有后端 ⇒ 判不了，不是判错。
        let judge = ToolathlonJudge::new(None);
        let err = judge
            .judge(&case("c2", &["excel"]), &env, &output())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("never run"), "{err}");
    }

    #[tokio::test]
    async fn the_four_adapters_compose_into_a_benchmark() {
        let root = tempdir();
        write_task(&root, "solo", &["excel"]);
        let bench = toolathlon_benchmark(None, ToolathlonToolkit::without_tools());
        assert_eq!(bench.name, "toolathlon");
        assert_eq!(bench.metric, "pass@1");
        assert_eq!(bench.budget, Budget::default());
        let cases = bench.cases.cases(&root).unwrap();
        assert_eq!(cases.len(), 1);
        // 没有 compose 后端 ⇒ 起环境就报错。
        assert!(bench.env.acquire(&cases[0]).await.is_err());
        std::fs::remove_dir_all(&root).ok();
    }
}
