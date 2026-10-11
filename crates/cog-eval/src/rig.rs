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
    /// 逐例检查点：跑过的例落在这里，重跑时从这里接。`None` ＝不落、不接。
    ///
    /// 它是「暂停」能成立的前提，不是它的后续：不能从断点接的话，停下来的那次实验只能整份
    /// 丢掉，那就不叫暂停。续的**粒度是例**，不是例内——一道题跑到一半的现场在它自己的
    /// 容器里，没有落盘面，被打断的例按没跑过重跑。
    pub checkpoint: Option<Checkpoint>,
}

/// 检查点的落点与它的身份。
///
/// 身份与记录写在一起：接的时候先比它，不一致就**拒绝**（不是从头再来）——两次实验的例
/// 混进同一张表，比丢掉一次重跑贵得多，而且从表上根本看不出来。
///
/// **记哪些例，只此一条判据**：判分器给出过判词的才记。起不来环境、上游拒了这些**不记**，
/// 续跑时重跑它们——它们是这一格之外的原因，记下来就等于把一次上游抖动钉成这一格的分数。
/// 反过来说，记录里没有的例在格子里按**没过**计（分母不缩水），只是不该被当成「已经有
/// 结论」而跳过：一句话，记的是结论，不是「跑过」。
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub path: PathBuf,
    /// 这次实验的身份。由调用方从它的 pins 导出（数据 rev 与 sha、骨干与裁判的模型名与
    /// 版本、种子表、子集文件、外壳清单）——评测台不认识 pins，只负责把这一串原样带着走。
    pub run_id: String,
}

/// 检查点里的一行。头一行记这次实验的身份，其余每行是**一个例**的坐标与结果。
///
/// 坐标是四个：基准、外壳、种子、例 id。三个种子是三个独立运行，少一个坐标就会把
/// 「第 2 个种子的第 7 题」读成「第 1 个种子的第 7 题」——那会让一张表看起来是三次
/// 运行，其实是同一个数当了三份。
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CheckpointLine {
    Header {
        run_id: String,
    },
    Case {
        benchmark: String,
        scaffold: String,
        seed: u64,
        case_id: String,
        verdict: crate::bench::Verdict,
        errored: bool,
        tokens: u64,
        steps: u64,
    },
}

/// 已经在跑的那份检查点：接的时候读它，跑完一例追加一行。
struct CheckpointLog {
    file: std::sync::Mutex<std::fs::File>,
    /// 已经跑完的例：坐标 → 它当时的结果。
    done: std::collections::HashMap<(String, String, u64, String), CaseOutcome>,
}

impl CheckpointLog {
    /// 打开（必要时新建）检查点，读回已经跑完的那些例，并**占住这一份记录**。
    ///
    /// 三种情形分开处理，因为它们在文件系统上长得一样、处理却完全不同：
    /// - 还不存在：新建，写头一行。
    /// - 头一行是**另一次实验**：拒绝，不是从头再来。把两次实验的例装进同一份记录，
    ///   表上完全看不出来，而它的读数会被当成一次完整实验读（这正是「id 已被占用」
    ///   与「工作还在手上」是两件事的那一族）。
    /// - 末行残缺：一次崩在追加中途留下的。截到最后一个换行，那一例按没跑过重跑——
    ///   半行 JSON 留在文件里的话，下次接会在中间那一行炸掉，把整份记录变成不可读。
    ///
    /// 同时把这一份记录**独占**下来：两个驱动往同一个检查点里追加，两边的行会交错，
    /// 而表上依然像是「跑完了」。
    fn open(cp: &Checkpoint) -> anyhow::Result<Self> {
        use std::io::{Read, Seek, SeekFrom, Write};

        let mut bytes = Vec::new();
        if cp.path.exists() {
            std::fs::File::open(&cp.path)
                .and_then(|mut f| f.read_to_end(&mut bytes))
                .map_err(|e| {
                    anyhow::anyhow!("cannot read the checkpoint {}: {e}", cp.path.display())
                })?;
        }
        let usable = bytes
            .iter()
            .rposition(|b| *b == b'\n')
            .map(|at| at + 1)
            .unwrap_or(0);

        let mut done = std::collections::HashMap::new();
        let text = String::from_utf8_lossy(&bytes[..usable]);
        let mut lines = text.lines();
        if let Some(first) = lines.next() {
            match serde_json::from_str::<CheckpointLine>(first) {
                Ok(CheckpointLine::Header { run_id }) => {
                    if run_id != cp.run_id {
                        anyhow::bail!(
                            "the checkpoint {} belongs to a different run ({} vs {}): resuming it \
                             would put two experiments' cases in one table, so this run refuses \
                             to continue -- move the file aside to start the new experiment",
                            cp.path.display(),
                            run_id,
                            cp.run_id
                        );
                    }
                }
                _ => anyhow::bail!(
                    "the checkpoint {} does not start with a run header: it is either not a \
                     checkpoint or it was written by something else",
                    cp.path.display()
                ),
            }
            for (n, line) in lines.enumerate() {
                let parsed: CheckpointLine = serde_json::from_str(line).map_err(|e| {
                    anyhow::anyhow!(
                        "the checkpoint {}: line {} is unreadable ({e}); only the last line of a \
                         checkpoint may be incomplete",
                        cp.path.display(),
                        n + 2
                    )
                })?;
                match parsed {
                    CheckpointLine::Case {
                        benchmark,
                        scaffold,
                        seed,
                        case_id,
                        verdict,
                        errored,
                        tokens,
                        steps,
                    } => {
                        done.insert(
                            (benchmark, scaffold, seed, case_id.clone()),
                            CaseOutcome {
                                case_id,
                                verdict,
                                errored,
                                tokens,
                                steps,
                            },
                        );
                    }
                    CheckpointLine::Header { .. } => anyhow::bail!(
                        "the checkpoint {}: line {} is a second header",
                        cp.path.display(),
                        n + 2
                    ),
                }
            }
        }

        // 不截断：长度由 `usable` 那一处说了算，这里只把写指针放到那儿。
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&cp.path)
            .map_err(|e| {
                anyhow::anyhow!("cannot open the checkpoint {}: {e}", cp.path.display())
            })?;
        // 一份记录只能有一个写者。两个驱动同时往里添，两次实验的例会交错着落进同一份
        // 记录，而表上仍然读作一张跑完的表——所以拿不到独占权就不跑，不是排队等。
        // 进程崩掉时由内核放开，正是续跑要能接上的场合。
        let mut file = exclusive(file).map_err(|e| {
            anyhow::anyhow!(
                "another driver holds the checkpoint {}: two of them appending to one record \
                 interleave their cases and the table still reads as finished ({e})",
                cp.path.display()
            )
        })?;
        if file.metadata()?.len() != usable as u64 {
            file.set_len(usable as u64)?;
        }
        file.seek(SeekFrom::Start(usable as u64))?;
        if usable == 0 {
            writeln!(
                file,
                "{}",
                serde_json::to_string(&CheckpointLine::Header {
                    run_id: cp.run_id.clone()
                })?
            )?;
            file.flush()?;
        }
        Ok(Self {
            file: std::sync::Mutex::new(file),
            done,
        })
    }

    fn get(
        &self,
        benchmark: &str,
        scaffold: &str,
        seed: u64,
        case_id: &str,
    ) -> Option<CaseOutcome> {
        self.done
            .get(&(
                benchmark.to_string(),
                scaffold.to_string(),
                seed,
                case_id.to_string(),
            ))
            .cloned()
    }

    /// 记下跑完的一例。写在判分之后——判词是这一行的一半。
    fn record(&self, line: &CheckpointLine) -> anyhow::Result<()> {
        use std::io::Write;
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{}", serde_json::to_string(line)?)?;
        file.flush()?;
        Ok(())
    }
}

/// 拿一份记录的独占权。第二个驱动拿不到就当场失败，不排队等：它等的是另一份可能
/// 要跑几个钟头的跑次，而两个驱动本来就不该同时驱动同一份记录。
#[cfg(unix)]
fn exclusive(file: std::fs::File) -> std::io::Result<std::fs::File> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: flock 只碰本进程这张描述符表里、由本作用域持有的这一个描述符，
    // EWOULDBLOCK 是预期答案，没有内存上的义务。
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(file)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// 没有 flock 的平台没办法把「只有一个写者」做实，那就不假装有：这次跑宁可不带
/// 检查点，也不留一份可能把两次实验摞在一起的记录。
#[cfg(not(unix))]
fn exclusive(_file: std::fs::File) -> std::io::Result<std::fs::File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "this platform has no exclusive-lock primitive",
    ))
}

impl RunConfig {
    /// 主表规格：三个种子。
    pub fn new(data_root: impl Into<PathBuf>) -> Self {
        Self {
            data_root: data_root.into(),
            seeds: vec![0, 1, 2],
            max_concurrency: 4,
            subset: None,
            checkpoint: None,
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
    /// 这一格里有多少题是**从检查点接回来的**（没经过这一轮的外壳与判分器）。
    ///
    /// 它不影响分数，但必须看得见：接回来的题用的是当时那份外壳、那次上游，与这一轮
    /// 跑出来的那些不是同一个条件。少了这一列，「这一格是这一轮的读数」与「这一格是
    /// 两轮拼起来的」在表上同形。
    #[serde(default)]
    pub resumed: usize,
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

    /// 这一格里有几题是从检查点接回来的。
    pub fn resumed(&self) -> usize {
        self.per_seed.iter().map(|s| s.resumed).sum()
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
    ///
    /// 落错检查点是**拒绝**而不是继续：`Err` 只在配置了检查点、而它接不上时出现
    /// （身份不同、或另一个驱动正占着它）。不配检查点这条路不会因为文件而失败。
    pub async fn run(&self, benchmarks: &[Benchmark]) -> anyhow::Result<Table> {
        let log = match &self.config.checkpoint {
            Some(cp) => Some(Arc::new(CheckpointLog::open(cp)?)),
            None => None,
        };
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
                    per_seed.push(
                        self.run_seed(bench, &cases, scaffold.clone(), *seed, log.clone())
                            .await?,
                    );
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
        Ok(Table {
            rows,
            cols: benchmarks.iter().map(|b| b.name.clone()).collect(),
            cells,
            wanted_seeds: self.config.seeds.len(),
            unreadable,
        })
    }

    /// 一个种子跑完一个基准。
    async fn run_seed(
        &self,
        bench: &Benchmark,
        cases: &[EvalCase],
        scaffold: Arc<dyn AgentScaffold>,
        seed: u64,
        log: Option<Arc<CheckpointLog>>,
    ) -> anyhow::Result<SeedScore> {
        let started = Instant::now();
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrency.max(1)));
        let mut handles = Vec::new();
        // 上一次跑过的例：不建环境、不调上游、不判分——它那一格已经定了，重跑只会把
        // 一个非确定的读数换成另一个。
        let mut resumed: Vec<CaseOutcome> = Vec::new();

        for case in cases {
            if let Some(log) = &log {
                if let Some(prior) = log.get(&bench.name, scaffold.name(), seed, &case.id) {
                    resumed.push(prior);
                    continue;
                }
            }
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let case = case.clone();
            let llm = self.llm.clone();
            let env_provider = bench.env.clone();
            let toolkit = bench.tools.clone();
            let judge = bench.judge.clone();
            let budget = bench.budget;
            let scaffold = scaffold.clone();
            let log = log.clone();
            let bench_name = bench.name.clone();
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
                    benchmark: bench_name.clone(),
                    scaffold: scaffold.name().to_string(),
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
                // 落的只有**判分器给出判词**的那些。「起不来环境」「上游拒了」不进检查点：
                // 它们是这一格之外的原因，写进去了，续跑就会把一次上游抖动当成这一行的
                // 分数，而 `errored` 这一列的全部用意就是让这两种事分得开。
                if !outcome.errored {
                    if let Some(log) = &log {
                        let line = CheckpointLine::Case {
                            benchmark: bench_name,
                            scaffold: scaffold.name().to_string(),
                            seed,
                            case_id: outcome.case_id.clone(),
                            verdict: outcome.verdict.clone(),
                            errored: false,
                            tokens: outcome.tokens,
                            steps: outcome.steps,
                        };
                        if let Err(e) = log.record(&line) {
                            tracing::warn!(case = %outcome.case_id, error = %e, "cannot append to the checkpoint: this case will be re-run next time");
                        }
                    }
                }
                outcome
            }));
        }

        let mut score = SeedScore {
            seed,
            resolved: 0,
            total: cases.len(),
            errored: 0,
            failures: Vec::new(),
            resumed: resumed.len(),
            tokens: 0,
            steps: 0,
            duration_ms: 0,
        };
        // 接回来的那些先记：它们和刚跑完的那些走同一套计数，所以「这格的分数是三次
        // 运行」在这条路上也成立——不因为它们是从盘上读回来的就少算一题。
        for outcome in &resumed {
            fold(&mut score, outcome);
        }
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
            fold(&mut score, &outcome);
        }
        score.duration_ms = started.elapsed().as_millis() as u64;
        Ok(score)
    }
}

#[derive(Clone)]
struct CaseOutcome {
    case_id: String,
    verdict: crate::bench::Verdict,
    errored: bool,
    tokens: u64,
    steps: u64,
}

/// 把一题的结果记进它那一格的计数。接回来的与刚跑完的走同一条路：两处的规则只此一份，
/// 免得「从盘上读回来的少算一题」这类偏差在两条路上各长一遍。
fn fold(score: &mut SeedScore, outcome: &CaseOutcome) {
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
                checkpoint: None,
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
                checkpoint: None,
            },
        )
    }

    /// 子集跑的是子集：分母跟着子集走，不是基准的题数。
    #[tokio::test]
    async fn a_subset_run_scores_the_archived_cases_and_not_the_whole_benchmark() {
        let rig = rig_on(archived("hle\ta\n")).with_scaffold(Arc::new(HalfRight));
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await.unwrap();
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
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await.unwrap();
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
            .await
            .unwrap();
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
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await.unwrap();

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
        let table = rig
            .run(&[bench("swe", Arc::new(HalfBrokenEnv))])
            .await
            .unwrap();
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
        let table = rig.run(&[bench("hle", Arc::new(OkEnv))]).await.unwrap();
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
            .await
            .unwrap();
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

        let table = rig().with_scaffold(seen.clone()).run(&[b]).await.unwrap();

        // 面空了也要并进动作空间：外壳这一步看得到它的解释器。
        assert_eq!(*seen.0.lock().unwrap(), vec!["python".to_string()]);
        // 问的是**外壳声明的名字**，不是判分器或工具自己猜的。
        assert_eq!(
            *toolkit.0.lock().unwrap(),
            vec!["python".to_string(), "bash".to_string()]
        );
        assert_eq!(table.cell("sees-tools", "hle").unwrap().stat().mean, 1.0);
    }

    /// 同名、但一被叫到就报错的壳。它顶上「同一个外壳」的名字，用来证明第二遍**没有**
    /// 再叫它——叫了的话那一题就成了一笔出错，而不是接回来的那一笔。
    struct SameNameButBroken;
    #[async_trait]
    impl AgentScaffold for SameNameButBroken {
        fn name(&self) -> &str {
            "always-right"
        }
        async fn solve(&self, _c: &EvalCase, _ctx: &SolveContext) -> anyhow::Result<AgentOutput> {
            anyhow::bail!("this case should have come back from the checkpoint, not been re-run")
        }
    }

    fn rig_checkpointed(cp: Checkpoint) -> Rig {
        Rig::new(
            Arc::new(NeverCalled),
            RunConfig {
                data_root: "/nonexistent".into(),
                seeds: vec![7],
                max_concurrency: 4,
                subset: None,
                checkpoint: Some(cp),
            },
        )
    }

    fn checkpoint_at(path: &std::path::Path, run_id: &str) -> Checkpoint {
        Checkpoint {
            path: path.to_path_buf(),
            run_id: run_id.into(),
        }
    }

    /// 接回来的那半不重跑：同一份检查点跑第二遍，交给外壳的题数必须是零。
    #[tokio::test]
    async fn a_resumed_run_does_not_re_run_the_cases_it_already_has() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.cases.jsonl");

        let first = rig_checkpointed(checkpoint_at(&path, "run-1"))
            .with_scaffold(Arc::new(AlwaysRight))
            .run(&[bench("hle", Arc::new(OkEnv))])
            .await
            .unwrap();
        assert_eq!(first.cell("always-right", "hle").unwrap().stat().mean, 1.0);
        assert_eq!(first.cell("always-right", "hle").unwrap().errored(), 0);

        // 第二遍换个壳：名字一样，行为是「被叫到就炸」。这一遍的读数只能来自检查点。
        let second = rig_checkpointed(checkpoint_at(&path, "run-1"))
            .with_scaffold(Arc::new(SameNameButBroken))
            .run(&[bench("hle", Arc::new(OkEnv))])
            .await
            .unwrap();
        let cell = second.cell("always-right", "hle").unwrap();
        assert_eq!(cell.errored(), 0, "出错就说明它被重跑了，不是接回来的");
        assert_eq!(cell.stat().mean, 1.0, "接回来的判词还是原来那条");
        assert_eq!(cell.per_seed[0].total, 2, "分母仍是这一列的全部题");
        assert_eq!(
            cell.resumed(),
            2,
            "这一格两题都是从盘上接回来的，表上要看得见"
        );
    }

    /// 另一份实验的检查点**拒绝**接，不是从头再来：两次实验的例装进同一份记录，
    /// 表上完全看不出来，而它的读数会被当成一次完整实验读。
    #[tokio::test]
    async fn a_checkpoint_from_another_run_is_refused_rather_than_merged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.cases.jsonl");

        rig_checkpointed(checkpoint_at(&path, "run-1"))
            .with_scaffold(Arc::new(AlwaysRight))
            .run(&[bench("hle", Arc::new(OkEnv))])
            .await
            .unwrap();

        let err = rig_checkpointed(checkpoint_at(&path, "run-2"))
            .with_scaffold(Arc::new(AlwaysRight))
            .run(&[bench("hle", Arc::new(OkEnv))])
            .await
            .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("different run"), "{text}");
        assert!(text.contains("run-1") && text.contains("run-2"), "{text}");
    }

    /// 落进检查点的只有**判分器给出判词**的那些：「起不来环境」是这一格之外的原因，
    /// 写进去了，续跑就会把一次上游抖动当成这一行的分数。
    #[tokio::test]
    async fn a_case_that_errored_is_left_out_of_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.cases.jsonl");

        let table = rig_checkpointed(checkpoint_at(&path, "run-1"))
            .with_scaffold(Arc::new(AlwaysRight))
            .run(&[bench("hle", Arc::new(HalfBrokenEnv))])
            .await
            .unwrap();
        assert_eq!(table.cell("always-right", "hle").unwrap().errored(), 1);

        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "头一行 ＋ 只有跑成的那一题：{text}");
        assert!(lines[0].contains("header"), "{text}");
        assert!(lines[1].contains("\"case_id\":\"a\""), "{text}");
    }

    /// 崩在追加中途留下的半行：丢掉它、接着用，不是把整份记录作废。
    #[tokio::test]
    async fn a_checkpoint_cut_off_mid_line_loses_only_that_case() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.cases.jsonl");
        let mut text = serde_json::to_string(&CheckpointLine::Header {
            run_id: "run-1".into(),
        })
        .unwrap();
        text.push('\n');
        text.push_str(
            &serde_json::to_string(&CheckpointLine::Case {
                benchmark: "hle".into(),
                scaffold: "always-right".into(),
                seed: 7,
                case_id: "a".into(),
                verdict: Verdict::pass("ok"),
                errored: false,
                tokens: 0,
                steps: 0,
            })
            .unwrap(),
        );
        text.push('\n');
        // 崩在给 b 追加这一行的中途：写到 case_id 就断了。
        text.push_str(
            "{\"kind\":\"case\",\"benchmark\":\"hle\",\"scaffold\":\"always-right\",\
             \"seed\":7,\"case_id\":\"b\",\"verdict\":{\"resolved\":true,\"det",
        );
        std::fs::write(&path, text).unwrap();

        // 一题从检查点接回来，另一题真跑；被截断的那半行不再留在文件里。
        let table = rig_checkpointed(checkpoint_at(&path, "run-1"))
            .with_scaffold(Arc::new(AlwaysRight))
            .run(&[bench("hle", Arc::new(OkEnv))])
            .await
            .unwrap();
        assert_eq!(table.cell("always-right", "hle").unwrap().stat().mean, 1.0);

        let after = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = after.lines().collect();
        assert_eq!(lines.len(), 3, "头一行 ＋ 两题，半行不留：{after}");
        for line in &lines {
            serde_json::from_str::<CheckpointLine>(line).expect("每一行都该读得回来");
        }
        assert!(lines[1].contains("\"case_id\":\"a\""), "{after}");
        assert!(
            lines[2].contains("\"case_id\":\"b\""),
            "被截断的那题要重跑，不是当成接回来了：{after}"
        );
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
            .await
            .unwrap();

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
