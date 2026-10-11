//! 运行器与报表：把「基准 × 外壳 × 种子」跑成主表的一格。
//!
//! 一格 ＝ 一个基准 × 一个外壳 × 3 个独立种子，报**均值 ± 标准差**。三个种子是
//! 三个独立运行，不是一个数的三次采样——同一格外壳得真跑三遍。
//!
//! 这个文件是评测台。它认识 [`AgentScaffold`] 这一个接口，不认识任何一个具体外壳：
//! 外壳是注册进来的 `Arc<dyn AgentScaffold>`，加一行只加一个注册，这里一行不改。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cog_core::LlmClient;
use tokio::sync::Semaphore;

use crate::bench::Benchmark;
use crate::dataset::EvalCase;
use crate::scaffold::{AgentScaffold, SolveContext};
use crate::subset::CaseSubset;

/// 数据根目录从哪个环境变量取。
///
/// 名字只写在这一个地方：取数脚本写进哪个目录、跑表的人把哪个目录指过来，靠的是
/// 同一个变量。各写一份拼写，就会有两份能互相不一致的说法。
pub const DATA_ROOT_ENV: &str = "COG_EVAL_DATA_ROOT";

/// 一次实验的配置。
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// 数据集根目录，取数脚本的 `--dest` 落点。
    ///
    /// **没有默认值**，也不从环境变量偷偷取：给了默认值，「复现」就会变成「在作者
    /// 那台机器上复现」，而失败时读起来像基准本身有问题。
    pub data_root: PathBuf,
    /// 同一格的独立运行数。主表规格是 3。
    pub seeds: Vec<u64>,
    /// 同时在跑的 case 数。基准的环境是有重量的（容器、compose），不是纯 IO。
    pub max_concurrency: usize,
    /// 只跑存档点名的那几道题；`None` ＝全量。
    ///
    /// 子集是**读进来的**，不是跑的时候现抽的：现抽的话，题面数据换一版、抽取实现改一行，
    /// 同一个开关在不同时候跑的就不是同一批题，两次迭代的分差随之失去意义。
    pub subset: Option<Arc<CaseSubset>>,
}

impl RunConfig {
    /// 主表规格：三个种子。
    pub fn new(data_root: impl Into<PathBuf>) -> Self {
        Self {
            data_root: data_root.into(),
            seeds: vec![0, 1, 2],
            max_concurrency: 4,
            subset: None,
        }
    }

    /// 从 [`DATA_ROOT_ENV`] 取数据根目录。
    ///
    /// 变量没设、或设成空串都是错误，不回落：一条默认路径会把「数据根指错了」这条
    /// 症状藏起来，而它读起来正好像是「基准读不出题」。
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var(DATA_ROOT_ENV).map_err(|_| {
            anyhow::anyhow!(
                "{DATA_ROOT_ENV} is not set: the rig does not guess where the benchmark data \
                 lives -- point it at the directory the fetch script wrote to"
            )
        })?;
        if raw.trim().is_empty() {
            anyhow::bail!(
                "{DATA_ROOT_ENV} is empty: an empty data root reads as a benchmark with no cases"
            );
        }
        Ok(Self::new(raw))
    }
}

/// 一个种子跑完整个基准之后的一行读数。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeedScore {
    pub seed: u64,
    pub resolved: usize,
    /// 参与计分的题数（＝这个基准的全部题）。跑不动的题按**没过**计，不从这里扣掉。
    pub total: usize,
    /// 外壳或环境出错、没能给出答案的题数。
    ///
    /// 它按没过计分，但必须单独看得见：「上游挂了」与「答错了」算出来都是 0 分，
    /// 而前者说明这一格不是这一行的数，是上游的。少了这一列，两者在表里同形。
    pub errored: usize,
    /// 没过的题的判词（case id → 为什么）。低分要能归因到某一题。
    pub failures: Vec<(String, String)>,
    pub tokens: u64,
    pub steps: u64,
    pub duration_ms: u64,
}

impl SeedScore {
    /// 这个种子上的得分。分母是全部题，出错的也算在里面。
    pub fn rate(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        self.resolved as f64 / self.total as f64
    }
}

/// 均值 ± 标准差。
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct CellStat {
    pub mean: f64,
    /// 样本标准差（除以 n-1）。
    ///
    /// `n == 1` 时没有标准差可言，这里记 0 并同时记 `n = 1`——把 0 读成「很稳」
    /// 就是把「只跑了一次」读成了「三次都一样」。
    pub std: f64,
    pub n: usize,
}

impl CellStat {
    pub fn of(rates: &[f64]) -> Self {
        let n = rates.len();
        if n == 0 {
            return Self {
                mean: 0.0,
                std: 0.0,
                n: 0,
            };
        }
        let mean = rates.iter().sum::<f64>() / n as f64;
        let std = if n < 2 {
            0.0
        } else {
            let var = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
            var.sqrt()
        };
        Self { mean, std, n }
    }

    /// 表里那一格的写法。`n` 不足主表规格时标出来，不做四舍五入以外的事。
    pub fn cell_text(&self, wanted_n: usize) -> String {
        let body = format!("{:.1}±{:.1}", self.mean * 100.0, self.std * 100.0);
        if self.n == 0 {
            return "—".into();
        }
        if self.n < wanted_n {
            return format!("{body} (n={})", self.n);
        }
        body
    }
}

/// 一格：一个基准 × 一个外壳。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Cell {
    pub benchmark: String,
    pub metric: String,
    pub scaffold: String,
    pub per_seed: Vec<SeedScore>,
}

impl Cell {
    pub fn stat(&self) -> CellStat {
        let rates: Vec<f64> = self.per_seed.iter().map(|s| s.rate()).collect();
        CellStat::of(&rates)
    }

    pub fn errored(&self) -> usize {
        self.per_seed.iter().map(|s| s.errored).sum()
    }
}

/// 主表。行＝外壳，列＝基准（＋均值）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Table {
    pub rows: Vec<String>,
    pub cols: Vec<String>,
    pub cells: Vec<Cell>,
    /// 这次要求的种子数。写进表里，免得读的人以为每格都是三个数。
    pub wanted_seeds: usize,
    /// 哪个基准的题没能拿到，以及为什么（基准名，原因）。
    ///
    /// 它不参与计分，但报表里那句「缺了什么」要能说出**因**：读不出数据与子集点名的题
    /// 不在这一版数据里，都表现为一整列题数为 0，而追查的方向完全不同。
    #[serde(default)]
    pub unreadable: Vec<(String, String)>,
}

impl Table {
    pub fn cell(&self, scaffold: &str, benchmark: &str) -> Option<&Cell> {
        self.cells
            .iter()
            .find(|c| c.scaffold == scaffold && c.benchmark == benchmark)
    }

    /// 一行在所有列上的对等平均。
    ///
    /// 「对等」是字面的：三个基准各算一份，不按题数加权——按题数加权的话，题多的
    /// 那个基准事实上就是这一列的分数了。缺的列不参与平均，但会在表里留一个「—」。
    pub fn average(&self, scaffold: &str) -> Option<CellStat> {
        let rates: Vec<f64> = self
            .cols
            .iter()
            .filter_map(|b| self.cell(scaffold, b))
            .map(|c| c.stat().mean)
            .collect();
        if rates.is_empty() {
            return None;
        }
        Some(CellStat {
            mean: rates.iter().sum::<f64>() / rates.len() as f64,
            std: CellStat::of(&rates).std,
            n: rates.len(),
        })
    }

    /// 主表本体。每格是「均值±标准差」，格里的 n 不足规格时标出来。
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str("| Scaffold |");
        for col in &self.cols {
            md.push_str(&format!(" {col} |"));
        }
        md.push_str(" Avg |\n");
        md.push_str("|---|");
        for _ in &self.cols {
            md.push_str("---|");
        }
        md.push_str("---|\n");
        for row in &self.rows {
            md.push_str(&format!("| **{row}** |"));
            for col in &self.cols {
                match self.cell(row, col) {
                    Some(cell) => {
                        md.push_str(&format!(" {} |", cell.stat().cell_text(self.wanted_seeds)))
                    }
                    None => md.push_str(" — |"),
                }
            }
            match self.average(row) {
                Some(avg) => md.push_str(&format!(" {:.1} |\n", avg.mean * 100.0)),
                None => md.push_str(" — |\n"),
            }
        }
        md.push_str(&format!(
            "\n_每格 = 均值±标准差(%)，取 {} 个独立种子；n 不足者随格标出。_\n",
            self.wanted_seeds
        ));
        md
    }
}

/// 运行器。
pub struct Rig {
    llm: Arc<dyn LlmClient>,
    scaffolds: Vec<Arc<dyn AgentScaffold>>,
    config: RunConfig,
}

impl Rig {
    pub fn new(llm: Arc<dyn LlmClient>, config: RunConfig) -> Self {
        Self {
            llm,
            scaffolds: Vec::new(),
            config,
        }
    }

    /// 注册一个外壳。同名重复注册是错误——两行同名会让表里出现两个一样的行，
    /// 读的人分不出哪个是哪个。
    pub fn with_scaffold(mut self, scaffold: Arc<dyn AgentScaffold>) -> Self {
        let name = scaffold.name().to_string();
        if self.scaffolds.iter().any(|s| s.name() == name) {
            panic!("two scaffolds registered under the name `{name}`");
        }
        self.scaffolds.push(scaffold);
        self
    }

    pub fn data_root(&self) -> &std::path::Path {
        &self.config.data_root
    }

    /// 跑整张表。
    pub async fn run(&self, benchmarks: &[Benchmark]) -> Table {
        let mut cells = Vec::new();
        let mut unreadable: Vec<(String, String)> = Vec::new();
        for bench in benchmarks {
            let cases = match bench.cases.cases(&self.config.data_root) {
                Ok(cases) => match &self.config.subset {
                    Some(subset) => match subset.select(&bench.name, cases) {
                        Ok(cases) => cases,
                        // 子集选不出题与数据读不出来，在表里都表现为一整列题数为 0，
                        // 所以这里把原因也带出去给报表用。
                        Err(e) => {
                            tracing::error!(benchmark = %bench.name, error = %e, "cannot apply the case subset");
                            unreadable.push((bench.name.clone(), e.to_string()));
                            Vec::new()
                        }
                    },
                    None => cases,
                },
                Err(e) => {
                    // 读不出数据不是「分数低」：是这一次实验没跑成。留一格带判词的
                    // 空读数，比静默少一列好——静默少一列会被读成「这个基准没过」。
                    tracing::error!(benchmark = %bench.name, error = %e, "cannot read the benchmark's cases");
                    unreadable.push((
                        bench.name.clone(),
                        format!("the case source could not read its cases: {e}"),
                    ));
                    Vec::new()
                }
            };
            for scaffold in &self.scaffolds {
                let mut per_seed = Vec::new();
                for seed in &self.config.seeds {
                    per_seed.push(self.run_seed(bench, &cases, scaffold.clone(), *seed).await);
                }
                cells.push(Cell {
                    benchmark: bench.name.clone(),
                    metric: bench.metric.clone(),
                    scaffold: scaffold.name().to_string(),
                    per_seed,
                });
            }
        }
        let mut rows: Vec<String> = self
            .scaffolds
            .iter()
            .map(|s| s.name().to_string())
            .collect();
        rows.sort();
        Table {
            rows,
            cols: benchmarks.iter().map(|b| b.name.clone()).collect(),
            cells,
            wanted_seeds: self.config.seeds.len(),
            unreadable,
        }
    }

    /// 一个种子跑完一个基准。
    async fn run_seed(
        &self,
        bench: &Benchmark,
        cases: &[EvalCase],
        scaffold: Arc<dyn AgentScaffold>,
        seed: u64,
    ) -> SeedScore {
        let started = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrency.max(1)));
        let mut handles = Vec::new();

        for case in cases {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let case = case.clone();
            let llm = self.llm.clone();
            let env_provider = bench.env.clone();
            let toolkit = bench.tools.clone();
            let judge = bench.judge.clone();
            let budget = bench.budget;
            let scaffold = scaffold.clone();
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                let env = match env_provider.acquire(&case).await {
                    Ok(env) => env,
                    Err(e) => {
                        return CaseOutcome::errored(&case, format!("cannot acquire env: {e}"));
                    }
                };
                let base = match toolkit.toolset(&case, &env) {
                    Ok(tools) => tools,
                    Err(e) => {
                        env.teardown().await;
                        return CaseOutcome::errored(&case, format!("cannot build toolset: {e}"));
                    }
                };
                // 有效工具面 = (基准工具面 ∩ 臂) ∪ 外壳动作空间。臂由 toolkit 定（无工具臂
                // 交回空面）；动作空间那半在这里并进来——它不参与消融，两臂都拿得到，所以
                // 外壳拿到的这个面里读不出自己在哪一臂。
                let tools = match toolkit.action_tools(&case, &env, scaffold.action_space()) {
                    Ok(action) => base.union(action),
                    Err(e) => {
                        env.teardown().await;
                        return CaseOutcome::errored(
                            &case,
                            format!("cannot build action tools: {e}"),
                        );
                    }
                };
                let ctx = SolveContext {
                    llm,
                    tools: Arc::new(tools),
                    env: env.clone(),
                    budget,
                    seed,
                };
                // 交到外壳手上的只有题面：答案键与评测台的旋钮在 `shell_view` 里就没了，
                // 判分读的仍是**原来那一份**。
                let solved = scaffold.solve(&case.shell_view(), &ctx).await;
                let outcome = match solved {
                    Err(e) => CaseOutcome::errored(&case, format!("scaffold failed: {e}")),
                    Ok(output) => match judge.judge(&case, &env, &output).await {
                        Ok(verdict) => CaseOutcome {
                            case_id: case.id.clone(),
                            verdict,
                            errored: false,
                            tokens: output.tokens.total_tokens as u64,
                            steps: output.trace.len() as u64,
                        },
                        Err(e) => CaseOutcome::errored(&case, format!("judge failed: {e}")),
                    },
                };
                // 判分读完之后才回收：回收早了判分就没有现场了。
                env.teardown().await;
                outcome
            }));
        }

        let mut score = SeedScore {
            seed,
            resolved: 0,
            total: cases.len(),
            errored: 0,
            failures: Vec::new(),
            tokens: 0,
            steps: 0,
            duration_ms: 0,
        };
        for handle in handles {
            let outcome = match handle.await {
                Ok(outcome) => outcome,
                Err(e) => {
                    // 任务 panic：这一题没结果，但**不能丢**——丢一道题的静默失败
                    // 会让分母也跟着小，分数于是偏高。
                    score.errored += 1;
                    score.failures.push(("<panicked>".into(), e.to_string()));
                    continue;
                }
            };
            score.tokens += outcome.tokens;
            score.steps += outcome.steps;
            if outcome.errored {
                score.errored += 1;
            }
            if outcome.verdict.resolved {
                score.resolved += 1;
            } else {
                score
                    .failures
                    .push((outcome.case_id.clone(), outcome.verdict.detail.clone()));
            }
        }
        score.duration_ms = started.elapsed().as_millis() as u64;
        score
    }
}

struct CaseOutcome {
    case_id: String,
    verdict: crate::bench::Verdict,
    errored: bool,
    tokens: u64,
    steps: u64,
}

impl CaseOutcome {
    /// 跑不动的一题：判词记**没过**（分母不缩水），并且标成出错。
    fn errored(case: &EvalCase, detail: String) -> Self {
        Self {
            case_id: case.id.clone(),
            verdict: crate::bench::Verdict::fail(detail),
            errored: true,
            tokens: 0,
            steps: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::{CaseJudge, CaseSource, EnvProvider, Toolkit, Verdict};
    use crate::dataset::EvalCase;
    use crate::scaffold::{AgentOutput, Budget, CaseEnv, FinishReason, NoEnv, ToolSet};
    use async_trait::async_trait;
    use cog_core::{ChatOptions, ChatResponse, Message, SFResult};

    /// 只用来占位：这几条路径都不该碰到上游。
    struct NeverCalled;

    #[async_trait]
    impl LlmClient for NeverCalled {
        async fn chat_stream(
            &self,
            _m: &[Message],
            _o: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!()
        }
        async fn complete_stream(
            &self,
            _p: &str,
            _o: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!()
        }
        async fn chat(&self, _m: &[Message], _o: &ChatOptions) -> SFResult<ChatResponse> {
            unreachable!()
        }
        async fn health_check(&self) -> bool {
            false
        }
    }

    fn cases() -> Vec<EvalCase> {
        ["a", "b"]
            .iter()
            .map(|id| EvalCase {
                id: (*id).into(),
                name: (*id).into(),
                input: serde_json::json!(id),
                expected_output: Some(serde_json::json!("ok")),
                expected_tools: None,
                tags: vec![],
                metrics: vec![],
                metadata: Default::default(),
            })
            .collect()
    }

    struct MemorySource(Vec<EvalCase>);
    #[async_trait]
    impl CaseSource for MemorySource {
        fn cases(&self, _root: &std::path::Path) -> anyhow::Result<Vec<EvalCase>> {
            Ok(self.0.clone())
        }
    }

    struct OkEnv;
    #[async_trait]
    impl EnvProvider for OkEnv {
        async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
            Ok(Arc::new(NoEnv))
        }
    }

    /// 环境起不起来是另一条路径：这里的基准有一半起不来。
    struct HalfBrokenEnv;
    #[async_trait]
    impl EnvProvider for HalfBrokenEnv {
        async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
            if case.id == "b" {
                anyhow::bail!("container image missing")
            }
            Ok(Arc::new(NoEnv))
        }
    }

    struct NoTools;
    impl Toolkit for NoTools {
        fn toolset(&self, _c: &EvalCase, _e: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet> {
            Ok(ToolSet::empty())
        }
    }

    /// 判分器：答案等于 "ok" 才算过。它不认识外壳，只看交回来的东西。
    struct OkJudge;
    #[async_trait]
    impl CaseJudge for OkJudge {
        async fn judge(
            &self,
            _case: &EvalCase,
            _env: &Arc<dyn CaseEnv>,
            output: &AgentOutput,
        ) -> anyhow::Result<Verdict> {
            Ok(if output.final_answer == "ok" {
                Verdict::pass("matches")
            } else {
                Verdict::fail(format!("got `{}`", output.final_answer))
            })
        }
    }

    fn bench(name: &str, env: Arc<dyn EnvProvider>) -> Benchmark {
        bench_named(name, cases(), env)
    }

    fn bench_named(name: &str, cases: Vec<EvalCase>, env: Arc<dyn EnvProvider>) -> Benchmark {
        Benchmark {
            name: name.into(),
            metric: "pass@1".into(),
            cases: Arc::new(MemorySource(cases)),
            env,
            tools: Arc::new(NoTools),
            judge: Arc::new(OkJudge),
            budget: Budget::default(),
        }
    }

    /// 全对的壳。
    struct AlwaysRight;
    #[async_trait]
    impl AgentScaffold for AlwaysRight {
        fn name(&self) -> &str {
            "always-right"
        }
        async fn solve(&self, _c: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
            Ok(AgentOutput {
                final_answer: "ok".into(),
                trace: vec![],
                tokens: Default::default(),
                finish: FinishReason::Answered,
            })
        }
    }

    /// 一半对的壳：这就是主表里 0.5 那种行。
    struct HalfRight;
    #[async_trait]
    impl AgentScaffold for HalfRight {
        fn name(&self) -> &str {
            "half-right"
        }
        async fn solve(&self, case: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
            Ok(AgentOutput {
                final_answer: if case.id == "a" { "ok" } else { "no" }.into(),
                trace: vec![],
                tokens: Default::default(),
                finish: FinishReason::Answered,
            })
        }
    }

    /// 报错的壳：它交不出答案，但**不许**被当成答错。
    struct Broken;
    #[async_trait]
    impl AgentScaffold for Broken {
        fn name(&self) -> &str {
            "broken"
        }
        async fn solve(&self, _c: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
            anyhow::bail!("upstream refused")
        }
    }

    /// 记下它收到的那份 case，供「外壳看不到答案」这条判据读。
    type Seen = Arc<std::sync::Mutex<Option<EvalCase>>>;

    struct RecordingScaffold(Seen);
    #[async_trait]
    impl AgentScaffold for RecordingScaffold {
        fn name(&self) -> &str {
            "recording"
        }
        async fn solve(&self, case: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
            *self.0.lock().unwrap() = Some(case.clone());
            Ok(AgentOutput {
                final_answer: "ok".into(),
                trace: vec![],
                tokens: Default::default(),
                finish: FinishReason::Answered,
            })
        }
    }

    /// 判分器看到的那一份，用它证明收窄只发生在外壳那一侧。
    struct RecordingJudge(Seen);
    #[async_trait]
    impl CaseJudge for RecordingJudge {
        async fn judge(
            &self,
            case: &EvalCase,
            _env: &Arc<dyn CaseEnv>,
            _output: &AgentOutput,
        ) -> anyhow::Result<Verdict> {
            *self.0.lock().unwrap() = Some(case.clone());
            Ok(Verdict::pass("recorded"))
        }
    }

    fn rig() -> Rig {
        Rig::new(
            Arc::new(NeverCalled),
            RunConfig {
                data_root: "/nonexistent".into(),
                seeds: vec![7, 8, 9],
                max_concurrency: 4,
                subset: None,
            },
        )
    }

    /// 把一份存档写到临时文件里再读回来——跑表走的就是这条路。
    fn archived(text: &str) -> Arc<CaseSubset> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subset.tsv");
        std::fs::write(&path, text).unwrap();
        Arc::new(CaseSubset::load(&path).unwrap())
    }

    fn rig_on(subset: Arc<CaseSubset>) -> Rig {
        Rig::new(
            Arc::new(NeverCalled),
            RunConfig {
                data_root: "/nonexistent".into(),
                seeds: vec![7],
                max_concurrency: 4,
                subset: Some(subset),
            },
        )
    }

    /// 子集跑的是子集：分母跟着子集走，不是基准的题数。
    #[tokio::test]
    async fn a_subset_run_scores_the_archived_cases_and_not_the_whole_benchmark() {
        let rig = rig_on(archived("hle\ta\n")).with_scaffold(Arc::new(HalfRight));
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await;
        let cell = table.cell("half-right", "hle").unwrap();
        assert_eq!(cell.per_seed[0].total, 1, "分母＝存档里的题数");
        assert_eq!(cell.per_seed[0].resolved, 1, "只跑存档点名的 a");
        assert!(table.unreadable.is_empty(), "{:?}", table.unreadable);
    }

    /// 存档点名的题这一版数据里没有：那一列题数为 0，而**原因**要写在表里，
    /// 不能让读的人去追数据根与追存档之间猜。
    #[tokio::test]
    async fn a_subset_that_names_a_missing_case_says_so_instead_of_scoring_zero() {
        let rig = rig_on(archived("hle\tghost\n")).with_scaffold(Arc::new(AlwaysRight));
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await;
        assert_eq!(
            table.cell("always-right", "hle").unwrap().per_seed[0].total,
            0
        );
        let (benchmark, why) = table.unreadable.first().expect("缺席要说出来");
        assert_eq!(benchmark, "hle");
        assert!(why.contains("ghost"), "{why}");
    }

    /// 存档没覆盖这个基准，是报错，不是「那就跑全量」。
    #[tokio::test]
    async fn a_benchmark_the_archive_does_not_cover_is_reported_not_run_in_full() {
        let rig = rig_on(archived("hle\ta\n")).with_scaffold(Arc::new(AlwaysRight));
        let table = rig
            .run(&[
                bench("hle", Arc::new(OkEnv)),
                bench("toolathlon", Arc::new(OkEnv)),
            ])
            .await;
        let (benchmark, why) = table.unreadable.first().expect("缺席要说出来");
        assert_eq!(benchmark, "toolathlon");
        assert!(why.contains("hle"), "要点出存档里有什么：{why}");
        // 覆盖到的那个基准照常跑，不受影响。
        assert_eq!(
            table.cell("always-right", "hle").unwrap().per_seed[0].total,
            1
        );
    }

    #[tokio::test]
    async fn a_cell_is_three_seeds_and_reports_mean_plus_minus_std() {
        let rig = rig()
            .with_scaffold(Arc::new(AlwaysRight))
            .with_scaffold(Arc::new(HalfRight));
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await;

        let right = table.cell("always-right", "hle").unwrap();
        assert_eq!(right.per_seed.len(), 3, "三个种子＝三次独立运行");
        assert_eq!(right.per_seed[0].seed, 7);
        assert_eq!(right.stat().mean, 1.0);
        assert_eq!(right.stat().std, 0.0);

        let half = table.cell("half-right", "hle").unwrap();
        assert_eq!(half.stat().mean, 0.5);
        // 三个种子都一样，标准差才是 0——不是因为只跑了一次。
        assert_eq!(half.stat().n, 3);
        assert_eq!(half.per_seed[0].failures.len(), 1);
        assert_eq!(half.per_seed[0].failures[0].0, "b");
        assert!(half.per_seed[0].failures[0].1.contains("got `no`"));

        let md = table.to_markdown();
        assert!(md.contains("| **always-right** | 100.0±0.0 |"), "{md}");
        assert!(md.contains("| **half-right** | 50.0±0.0 |"), "{md}");
        assert!(md.contains("100.0 |"), "平均列应当是 100.0：{md}");
    }

    #[tokio::test]
    async fn a_case_that_cannot_run_is_counted_as_missing_the_bar_and_stays_visible() {
        let rig = rig().with_scaffold(Arc::new(AlwaysRight));
        let table = rig.run(&[bench("swe", Arc::new(HalfBrokenEnv))]).await;
        let cell = table.cell("always-right", "swe").unwrap();
        // 分母不缩水：起不来的那道题按没过算。
        assert_eq!(cell.per_seed[0].total, 2);
        assert_eq!(cell.per_seed[0].resolved, 1);
        assert_eq!(cell.per_seed[0].errored, 1);
        assert_eq!(cell.stat().mean, 0.5);
        assert!(cell.per_seed[0]
            .failures
            .iter()
            .any(|(_, d)| d.contains("container image missing")));
    }

    #[tokio::test]
    async fn a_scaffold_that_errors_is_errored_not_silently_wrong() {
        let rig = rig().with_scaffold(Arc::new(Broken));
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await;
        let cell = table.cell("broken", "hle").unwrap();
        assert_eq!(cell.stat().mean, 0.0);
        assert_eq!(cell.errored(), 6, "两道题 × 三个种子都该记成出错");
        assert!(cell.per_seed[0].failures[0].1.contains("upstream refused"));
    }

    /// 一个环境永远起不来的基准：整列都是 0。
    struct DeadEnv;
    #[async_trait]
    impl EnvProvider for DeadEnv {
        async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
            anyhow::bail!("no runtime available")
        }
    }

    #[tokio::test]
    async fn the_average_is_over_columns_not_over_cases() {
        let rig = rig().with_scaffold(Arc::new(AlwaysRight));
        // 两列的题数**不一样**（2 与 1）：按题数加权算出来是 2/3，对等平均是 1/2。
        // 这两个数不同，所以这条断言能分辨表里那一列到底是哪一种。
        let table = rig
            .run(&[
                bench("hle", Arc::new(OkEnv)),
                bench_named("swe", cases()[..1].to_vec(), Arc::new(DeadEnv)),
            ])
            .await;
        assert_eq!(table.cols, vec!["hle", "swe"]);
        assert_eq!(table.cell("always-right", "hle").unwrap().stat().mean, 1.0);
        assert_eq!(table.cell("always-right", "swe").unwrap().stat().mean, 0.0);
        let avg = table.average("always-right").unwrap();
        assert_eq!(avg.mean, 0.5, "对等平均：两列各算一份，不按题数加权");
        assert_eq!(avg.n, 2);
    }

    #[test]
    fn a_single_seed_is_not_reported_as_a_steady_cell() {
        let stat = CellStat::of(&[0.5]);
        assert_eq!(stat.n, 1);
        assert_eq!(stat.cell_text(3), "50.0±0.0 (n=1)");
        assert_eq!(CellStat::of(&[0.5, 0.6, 0.7]).cell_text(3), "60.0±10.0");
        assert_eq!(CellStat::of(&[]).cell_text(3), "—");
    }

    fn python_tool() -> cog_core::Tool {
        cog_core::Tool {
            name: "python".into(),
            description: "an interpreter".into(),
            parameters: serde_json::json!({"type": "object"}),
            implementation: cog_core::ToolImplementation::Native(Arc::new(|args| {
                Box::pin(async move { Ok(args) })
            })),
        }
    }

    /// 工具面为空（无工具臂）但仍供得出外壳的动作空间：记下它被问到要哪几件。
    struct ActionToolkit(std::sync::Mutex<Vec<String>>);
    impl Toolkit for ActionToolkit {
        fn toolset(&self, _c: &EvalCase, _e: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet> {
            // 无工具臂：基准外部工具面是空的。
            Ok(ToolSet::empty())
        }
        fn action_tools(
            &self,
            _c: &EvalCase,
            _e: &Arc<dyn CaseEnv>,
            wanted: &[&str],
        ) -> anyhow::Result<ToolSet> {
            *self.0.lock().unwrap() = wanted.iter().map(|s| (*s).to_string()).collect();
            ToolSet::new(vec![python_tool()])
        }
    }

    /// 记下它这一步看到的工具名，并声明一个动作空间。
    struct SeesTools(std::sync::Mutex<Vec<String>>);
    #[async_trait]
    impl AgentScaffold for SeesTools {
        fn name(&self) -> &str {
            "sees-tools"
        }
        fn action_space(&self) -> &'static [&'static str] {
            &["python", "bash"]
        }
        async fn solve(&self, _c: &EvalCase, ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
            *self.0.lock().unwrap() = ctx
                .tools
                .definitions()
                .iter()
                .map(|d| d.name.clone())
                .collect();
            Ok(AgentOutput {
                final_answer: "ok".into(),
                trace: vec![],
                tokens: Default::default(),
                finish: FinishReason::Answered,
            })
        }
    }

    /// 动作空间由外壳声明、按名向 toolkit 要，且**不随工具面被消融**：基准工具面为空
    /// （无工具臂）时，外壳仍拿得到自己的动作空间。
    #[tokio::test]
    async fn the_action_space_is_unioned_in_and_survives_an_empty_tool_face() {
        let toolkit = Arc::new(ActionToolkit(std::sync::Mutex::new(Vec::new())));
        let seen = Arc::new(SeesTools(std::sync::Mutex::new(Vec::new())));
        let mut b = bench("hle", Arc::new(OkEnv));
        b.tools = toolkit.clone();

        let table = rig().with_scaffold(seen.clone()).run(&[b]).await;

        // 面空了也要并进动作空间：外壳这一步看得到它的解释器。
        assert_eq!(*seen.0.lock().unwrap(), vec!["python".to_string()]);
        // 问的是**外壳声明的名字**，不是判分器或工具自己猜的。
        assert_eq!(
            *toolkit.0.lock().unwrap(),
            vec!["python".to_string(), "bash".to_string()]
        );
        assert_eq!(table.cell("sees-tools", "hle").unwrap().stat().mean, 1.0);
    }

    /// 交到外壳手上的那份 case 里没有答案键：`expected_output` 与评测台起环境/判分用的
    /// 旋钮都不在，题面还在。判分器读的仍是**原来那一份**——收窄的是外壳的输入，
    /// 不是判分的依据。
    #[tokio::test]
    async fn the_shell_is_handed_the_case_without_the_answer_key() {
        let shell_saw: Seen = Arc::new(std::sync::Mutex::new(None));
        let judge_saw: Seen = Arc::new(std::sync::Mutex::new(None));

        let mut case = cases().remove(0);
        case.metadata
            .insert("fail_to_pass".into(), "tests::the_answer".into());

        let b = Benchmark {
            name: "hle".into(),
            metric: "pass@1".into(),
            cases: Arc::new(MemorySource(vec![case])),
            env: Arc::new(OkEnv),
            tools: Arc::new(NoTools),
            judge: Arc::new(RecordingJudge(judge_saw.clone())),
            budget: Budget::default(),
        };

        rig()
            .with_scaffold(Arc::new(RecordingScaffold(shell_saw.clone())))
            .run(&[b])
            .await;

        let handed = shell_saw.lock().unwrap().clone().expect("外壳被叫过");
        assert!(handed.expected_output.is_none(), "答案键不许到外壳手上");
        assert!(handed.expected_tools.is_none(), "期望工具也不许");
        assert!(handed.metadata.is_empty(), "评测台的旋钮不许到外壳手上");
        assert_eq!(handed.input, serde_json::json!("a"), "题面还是题面");
        assert_eq!(handed.id, "a", "身份还在：外壳要按它记 trace");

        let judged = judge_saw.lock().unwrap().clone().expect("判分被叫过");
        assert!(judged.expected_output.is_some(), "判分仍读得到答案键");
        assert!(
            judged.metadata.contains_key("fail_to_pass"),
            "判分仍读得到评测台的旋钮"
        );
    }
}
