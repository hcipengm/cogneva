//! 主表的运行入口：一条命令把「数据根 ＋ 基准 ＋ 外壳 ＋ 种子」跑成一张带 pins 的表。
//!
//! 它落在组合根而不是评测台里，因为一次真正的跑要构造两样评测台构造不出来的东西：
//! 骨干上游（`LlmClient`，只有这里看得见 `llm_routing` 那段配置）和平台（`PlatformRunner`，
//! 只有这里看得见平台类型）。评测台那边保留的是一条纯函数式的入口，谁把依赖注入进去就
//! 用谁的——注入的是真上游还是测试替身，决定这次跑出的是数字还是结构。
//!
//! **没有 pins 的表不比任何东西**：每个基准的 rev 与 sha、骨干与裁判的模型名与版本、
//! 种子表，都随表一起出去。pins 不是注释，是这张表能被别人复跑的前提。
//!
//! 同一个数据根跑两次，输出逐字节相同——所以这里不写墙上时钟、不遍历无序容器、不打印
//! 输入之外的路径。可 diff 说的是**管道**在拿到同样答案之后确定，不是「上游端到端可复现」：
//! 温度不为零的采样本来就不会两次给出同样的 token。

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use cog_core::{HttpClient, LlmClient};
use cog_eval::adapters::{
    hle_benchmark, swe_pro_benchmark, toolathlon_benchmark, HleToolkit, SweProBackend,
    SweProToolkit, ToolathlonBackend, ToolathlonToolkit,
};
use cog_eval::scaffolds::{table_scaffolds, PlatformRunner};
use cog_eval::{Benchmark, CaseSubset, Rig, RunConfig, Table};

use crate::eval_platform::{BridgeSettings, NoBackbone, NoPlatform, PlatformApiRunner};

/// 基准的规范顺序，也就是表里列的顺序。
///
/// 表要能两次跑逐字节相等，列序就不能由命令行给的先后决定——那会让两次「同一组基准」
/// 因为参数顺序不同而产出两张排不上版的表。
pub const BENCHMARKS: [&str; 3] = ["hle", "swe-bench-pro", "toolathlon"];

/// 表里的列名 → 取数脚本目录里的名字。
///
/// 两者只有 Toolathlon 不同：数据来自 `eigent-ai/toolathlon_gym`，取数脚本按仓库叫它
/// `toolathlon-gym`，而这一列在表里是 Toolathlon。名字只在这个表里对应一次，基准名的
/// 两处拼写就不会各改各的。
fn pins_key(benchmark: &str) -> &str {
    match benchmark {
        "toolathlon" => "toolathlon-gym",
        other => other,
    }
}

/// 外壳的规范顺序，与表里的行序一致。
const SCAFFOLDS: [&str; 4] = ["codeact", "gepa", "agentflow", "nql"];

/// 每个格子打印几条失败明细。
const FAILURE_SAMPLE: usize = 3;

pub const USAGE: &str = "\
usage: cogneva eval-table --pins <file> --backbone <model>@<version> --judge <model>@<version>
                          [--tools <none|platform>] [--data-root <dir>]
                          [--benchmarks hle,swe-bench-pro,toolathlon]
                          [--scaffolds codeact,gepa,agentflow,nql] [--seeds 0,1,2]
                          [--subset <file>] [--out <file>]

  --pins       the pins file: what the data is pinned by, as printed by
               deploy/scripts/fetch-benchmark-data.sh --print-pins
  --tools      which arm of the tool axis this run is. Defaults to `platform`, the arm the
               main table is run on. `none` is a diagnostic arm only: its numbers print but
               are not table rows, because a run on a different tool face is not the same
               experiment as the rows it would sit beside.
  --backbone   the model every scaffold drives, as name@version. The name has to be one the
               configured routing serves -- a pin that names a model nobody configured is a
               label, not a pin.
  --judge      the model that grades the columns whose judge is generative, name@version
  --data-root  the fetch script's --dest; falls back to COG_EVAL_DATA_ROOT
  --benchmarks comma-separated; default all three, in the canonical order
  --scaffolds  comma-separated; default all four, in the table's row order
  --seeds      comma-separated independent runs per cell; default 0,1,2 (the main-table spec)
  --subset     run the cases an archived subset names, instead of every case. The archive is a
               file of `benchmark<TAB>case id` lines drawn once by `cogneva eval-case-subset`;
               it is read, never redrawn here. A benchmark the archive does not cover, or an id
               this data root does not have, stops the run instead of silently running in full
               or dropping a case. The archive's path and content hash go into the table header,
               because which cases ran is part of what the table says.
  --out        write the output here instead of standard output

  the nql row reaches the platform from the environment: COG_EVAL_PLATFORM_BASE is the platform
  endpoint (COG_EVAL_PLATFORM_TOKEN, COG_EVAL_PLATFORM_DEADLINE_SECS optional). With no base the
  row errors rather than scoring; the endpoint it did use is printed under `## wiring`.";

/// 一个模型的 pin：名字必须在配置里存在，版本是这次跑被记录成的那一个。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPin {
    pub role: &'static str,
    pub model: String,
    pub version: String,
}

impl ModelPin {
    /// `name@version`。名字里允许有 `@`，所以从**最后一个** `@` 处切。
    fn parse(role: &'static str, raw: &str) -> Result<Self, String> {
        let bad = || format!("--{role} wants <model>@<version>, got `{raw}`");
        let (model, version) = raw.rsplit_once('@').ok_or_else(bad)?;
        if model.trim().is_empty() || version.trim().is_empty() {
            return Err(bad());
        }
        Ok(Self {
            role,
            model: model.trim().to_string(),
            version: version.trim().to_string(),
        })
    }

    fn render(&self) -> String {
        format!("{}@{}", self.model, self.version)
    }
}

/// 工具轴上的哪一臂。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolArm {
    /// 无工具臂：关掉工具，别的都一样。今天它**只作诊断**——工具面不同的跑次与其它行
    /// 比的不是同一件事，所以它的数字不进表。
    None,
    /// 有工具臂：工具由平台提供。这是主表要跑的那一臂，所以它是默认。
    Platform,
}

impl ToolArm {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "none" => Ok(Self::None),
            "platform" => Ok(Self::Platform),
            other => Err(format!("--tools wants `none` or `platform`, got `{other}`")),
        }
    }

    fn render(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Platform => "platform",
        }
    }

    /// 这一臂的数字能不能进主表。只有工具面与主表一致的跑次才作数；关掉工具的跑次是
    /// 诊断，读数照样打印，但不作表行。
    fn is_table_arm(self) -> bool {
        matches!(self, Self::Platform)
    }
}

/// 解析好的一次运行的参数。
#[derive(Debug, Clone)]
pub struct EvalTableArgs {
    /// `None` 表示没给，用 `COG_EVAL_DATA_ROOT`（再没有就报错）。
    pub data_root: Option<PathBuf>,
    /// 已经排成规范顺序。
    pub benchmarks: Vec<&'static str>,
    pub scaffolds: Vec<&'static str>,
    pub seeds: Vec<u64>,
    /// 只跑这份存档点名的题；`None` ＝全量。
    pub subset: Option<PathBuf>,
    pub pins: PathBuf,
    pub tools: ToolArm,
    pub backbone: ModelPin,
    pub judge: ModelPin,
    pub out: Option<PathBuf>,
}

fn next_arg<'a>(args: &'a [String], at: usize, name: &str) -> Result<&'a str, String> {
    args.get(at + 1)
        .map(String::as_str)
        .ok_or_else(|| format!("{name} needs a value"))
}

fn parse_list(raw: &str) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for item in raw.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(format!("empty item in list `{raw}`"));
        }
        if out.iter().any(|seen| seen == item) {
            return Err(format!("`{item}` is listed twice"));
        }
        out.push(item.to_string());
    }
    if out.is_empty() {
        return Err("an empty list is not a selection".into());
    }
    Ok(out)
}

/// 把命令行给的选择收进规范顺序里：给的是**选哪几个**，不是排哪几列。
fn canonicalize<'a>(raw: &[String], known: &[&'a str], what: &str) -> Result<Vec<&'a str>, String> {
    for name in raw {
        if !known.contains(&name.as_str()) {
            return Err(format!(
                "unknown {what} `{name}` (have: {})",
                known.join(", ")
            ));
        }
    }
    Ok(known
        .iter()
        .copied()
        .filter(|k| raw.iter().any(|n| n == k))
        .collect())
}

impl EvalTableArgs {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let mut data_root = None;
        let mut tool_arm = None;
        let mut pins = None;
        let mut backbone = None;
        let mut judge = None;
        let mut out = None;
        let mut benchmarks: Option<Vec<String>> = None;
        let mut scaffolds: Option<Vec<String>> = None;
        let mut seeds: Option<Vec<u64>> = None;
        let mut subset: Option<PathBuf> = None;

        let mut i = 0;
        while i < args.len() {
            let flag = args[i].as_str();
            match flag {
                "--data-root" => {
                    data_root = Some(PathBuf::from(next_arg(args, i, flag)?));
                    i += 2;
                }
                "--pins" => {
                    pins = Some(PathBuf::from(next_arg(args, i, flag)?));
                    i += 2;
                }
                "--out" => {
                    out = Some(PathBuf::from(next_arg(args, i, flag)?));
                    i += 2;
                }
                "--tools" => {
                    tool_arm = Some(ToolArm::parse(next_arg(args, i, flag)?)?);
                    i += 2;
                }
                "--backbone" => {
                    backbone = Some(ModelPin::parse("backbone", next_arg(args, i, flag)?)?);
                    i += 2;
                }
                "--judge" => {
                    judge = Some(ModelPin::parse("judge", next_arg(args, i, flag)?)?);
                    i += 2;
                }
                "--benchmarks" => {
                    benchmarks = Some(parse_list(next_arg(args, i, flag)?)?);
                    i += 2;
                }
                "--scaffolds" => {
                    scaffolds = Some(parse_list(next_arg(args, i, flag)?)?);
                    i += 2;
                }
                "--subset" => {
                    subset = Some(PathBuf::from(next_arg(args, i, flag)?));
                    i += 2;
                }
                "--seeds" => {
                    let mut parsed = Vec::new();
                    for item in parse_list(next_arg(args, i, flag)?)? {
                        parsed.push(
                            item.parse::<u64>()
                                .map_err(|e| format!("--seeds: `{item}` is not a seed: {e}"))?,
                        );
                    }
                    seeds = Some(parsed);
                    i += 2;
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        let selected = match benchmarks {
            Some(raw) => canonicalize(&raw, &BENCHMARKS, "benchmark")?,
            None => BENCHMARKS.to_vec(),
        };
        let scaffold_names = match scaffolds {
            Some(raw) => canonicalize(&raw, &SCAFFOLDS, "scaffold")?,
            None => SCAFFOLDS.to_vec(),
        };

        Ok(Self {
            data_root,
            benchmarks: selected,
            scaffolds: scaffold_names,
            // 种子表的默认值取自评测台的主表规格，不在这里再抄一份数字。
            seeds: seeds.unwrap_or_else(|| RunConfig::new(".").seeds),
            subset,
            pins: pins.ok_or("--pins is required: a table without pins is not comparable")?,
            // 工具臂默认 `platform`：那是主表要跑的那一臂。`none` 要显式写出，好让
            // 「这次跑的是一臂诊断」在命令行上就被点明，而不是默认落进去。
            tools: tool_arm.unwrap_or(ToolArm::Platform),
            backbone: backbone.ok_or("--backbone <model>@<version> is required")?,
            judge: judge.ok_or("--judge <model>@<version> is required")?,
            out,
        })
    }
}

/// 一个基准被什么钉住。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinRead {
    /// 一份被逐字节钉住的文件。这是评测台真正打开的那一份。
    File {
        path: String,
        sha256: String,
        size: u64,
    },
    /// 一棵被遍历的目录树。目录没有内容哈希，所以它的字节靠来源归档钉住。
    Dir { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinSource {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkPin {
    pub revision: String,
    pub read: PinRead,
    pub source: Option<PinSource>,
}

/// 取数脚本 `--print-pins` 的产物。
#[derive(Debug, Clone, Default)]
pub struct Pins {
    pub by_benchmark: BTreeMap<String, BenchmarkPin>,
}

impl Pins {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read the pins file {}", path.display()))?;
        let mut sources: BTreeMap<String, PinSource> = BTreeMap::new();
        let mut reads: BTreeMap<String, PinRead> = BTreeMap::new();
        let mut revisions: BTreeMap<String, String> = BTreeMap::new();

        for (n, line) in text.lines().enumerate() {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() != 7 {
                bail!(
                    "{}:{}: a pin record has seven tab-separated fields, this line has {}",
                    path.display(),
                    n + 1,
                    fields.len()
                );
            }
            let (kind, benchmark, revision, rel, sha256, size) = (
                fields[0], fields[1], fields[2], fields[3], fields[4], fields[5],
            );
            let at = || format!("{}:{}", path.display(), n + 1);
            let parse_size = || -> Result<u64> {
                size.parse::<u64>()
                    .with_context(|| format!("{}: `{size}` is not a size", at()))
            };
            match kind {
                "source" => {
                    let parsed = PinSource {
                        path: rel.to_string(),
                        sha256: sha256.to_string(),
                        size: parse_size()?,
                    };
                    if sources.insert(benchmark.to_string(), parsed).is_some() {
                        bail!("{}: two `source` records for `{benchmark}`", at());
                    }
                }
                "read-file" => {
                    let parsed = PinRead::File {
                        path: rel.to_string(),
                        sha256: sha256.to_string(),
                        size: parse_size()?,
                    };
                    if reads.insert(benchmark.to_string(), parsed).is_some() {
                        bail!("{}: two read records for `{benchmark}`", at());
                    }
                }
                "read-dir" => {
                    let parsed = PinRead::Dir {
                        path: rel.to_string(),
                    };
                    if reads.insert(benchmark.to_string(), parsed).is_some() {
                        bail!("{}: two read records for `{benchmark}`", at());
                    }
                }
                other => bail!(
                    "{}: unknown pin kind `{other}` (have: source, read-file, read-dir)",
                    at()
                ),
            }
            revisions.insert(benchmark.to_string(), revision.to_string());
        }

        let mut by_benchmark = BTreeMap::new();
        for (benchmark, read) in reads {
            let revision = revisions
                .get(&benchmark)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("`{benchmark}` has a read path but no revision"))?;
            let source = sources.get(&benchmark).cloned();
            by_benchmark.insert(
                benchmark,
                BenchmarkPin {
                    revision,
                    read,
                    source,
                },
            );
        }
        Ok(Self { by_benchmark })
    }

    /// 把 pins 与盘上的字节对一次，并把每一列是怎么核过的写成可打印的行。
    ///
    /// 不核的话 pins 只是一张贴纸：镜像换了字节而表上仍写着旧哈希，读的人无从知道。
    /// 核的是**评测台真正打开的那一个**路径——同一个基准，镜像上的归档与转换后的文本
    /// 是两份不同的字节，来源对不保证读的那份对。
    fn verify(&self, root: &Path, selected: &[&str]) -> Result<BTreeMap<String, String>> {
        let mut verified = BTreeMap::new();
        for benchmark in selected {
            let key = pins_key(benchmark);
            let pin = self.by_benchmark.get(key).ok_or_else(|| {
                anyhow::anyhow!(
                    "the pins file has no read path for `{benchmark}` (its records are keyed \
                     `{key}`); a column without a pin cannot be compared with anything"
                )
            })?;
            let mut how = match &pin.read {
                PinRead::File { path, sha256, size } => {
                    let full = root.join(path);
                    let got_size = std::fs::metadata(&full)
                        .with_context(|| {
                            format!("the pinned file {} is not under {}", path, root.display())
                        })?
                        .len();
                    if got_size != *size {
                        bail!(
                            "{path} is {got_size} bytes, the pin says {size}: these are not the \
                             bytes the pin names"
                        );
                    }
                    let got = sha256_file(&full)?;
                    if &got != sha256 {
                        bail!("{path} hashes to {got}, the pin says {sha256}");
                    }
                    format!("read-file {path} sha256({got})")
                }
                PinRead::Dir { path } => {
                    let full = root.join(path);
                    if !full.is_dir() {
                        bail!(
                            "the pinned tree {} is not a directory under {}",
                            path,
                            root.display()
                        );
                    }
                    // 目录没有内容哈希，能核的是它从哪来：来源归档的 sha 由下面这段核。
                    format!("read-dir {path} (a directory has no hash of its own)")
                }
            };
            match &pin.source {
                Some(source) => {
                    let full = root.join(&source.path);
                    if full.is_file() {
                        let got = sha256_file(&full)?;
                        if got != source.sha256 {
                            bail!(
                                "{} hashes to {}, the pin says {}",
                                source.path,
                                got,
                                source.sha256
                            );
                        }
                        how.push_str(&format!("; source {} sha256({got})", source.path));
                    } else {
                        // 镜像上的原件不在盘上，就说出来：沉默地少核一份，读的人会以为
                        // 这条 verified 覆盖了它。
                        how.push_str(&format!(
                            "; source {} is not on disk, only the read path was verified",
                            source.path
                        ));
                    }
                }
                None => how.push_str("; the pins file names no source artifact for it"),
            }
            verified.insert((*benchmark).to_string(), how);
        }
        Ok(verified)
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("cannot open {} to hash it", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// 这次跑用到的外部依赖。`None` 不是「随便来个默认」——每一处缺席都在报表里说出来。
#[derive(Default)]
pub struct Wiring {
    pub backbone: Option<Arc<dyn LlmClient>>,
    pub judge: Option<Arc<dyn LlmClient>>,
    pub swe_pro: Option<Arc<dyn SweProBackend>>,
    pub toolathlon: Option<Arc<dyn ToolathlonBackend>>,
    pub platform: Option<Arc<dyn PlatformRunner>>,
    /// 平台端点的身份，跟平台本身分开记：`PlatformRunner` 只说要跑，说不出跑在哪。
    /// NQL 那一行的读数出于哪个平台，是这张表能不能被别人复跑的一部分，所以它跟骨干、
    /// 裁判的模型名一样要随表出去——不能只有「接上了」这一个字。
    pub platform_base: Option<String>,
}

/// 一次跑的结果：要写出去的那段文本，以及这次跑是不是一次完整的实验。
pub struct Outcome {
    pub text: String,
    /// 缺了什么（人为可读）。非空即这次跑不是一次完整的实验。
    pub missing: Vec<String>,
    /// 报出来跑不动的题数（不是答错，是没能给出答案）。
    pub errored: usize,
}

/// 跑一次表并渲染。依赖由调用方注入：命令行注入真上游与真平台，测试注入替身。
pub async fn run_table(args: &EvalTableArgs, wiring: &Wiring, pins: &Pins) -> Result<Outcome> {
    let data_root = match &args.data_root {
        Some(root) => root.clone(),
        None => RunConfig::from_env()?.data_root,
    };
    if !data_root.is_dir() {
        bail!("the data root {} is not a directory", data_root.display());
    }

    // 子集先读、先查覆盖面，在核 pins 之前：存档是本机一个小文件，核 pins 要按哈希逐份
    // 读数据。先在便宜的那一步把「这次跑的是哪一批题」定下来，指错了当场就知道，不必
    // 先扫一遍几百 MB 的题面；覆盖不到某个基准也要在开跑之前说，而不是跑到那一列才发现。
    let subset = match &args.subset {
        Some(path) => {
            let subset = Arc::new(CaseSubset::load(path)?);
            let uncovered: Vec<&str> = args
                .benchmarks
                .iter()
                .copied()
                .filter(|name| subset.count(name).is_none())
                .collect();
            if !uncovered.is_empty() {
                bail!(
                    "the subset archive {} names no cases for {}; it covers: {}. A benchmark the \
                     archive does not cover cannot run beside the others: it would run in full \
                     while its neighbours run a subset, and the two columns' numbers would not \
                     be the same experiment",
                    path.display(),
                    uncovered.join(", "),
                    subset.benchmarks().collect::<Vec<_>>().join(", ")
                );
            }
            Some(subset)
        }
        None => None,
    };

    let verified = pins.verify(&data_root, &args.benchmarks)?;

    // 每一处缺席都记名字：报表里缺什么、怎么补，比一个非零退出码有用。
    let mut missing = Vec::new();
    // 诊断臂不是缺席的依赖，但它同样意味着这次跑不是一次完整的实验：没有主表那一臂的
    // 工具面，格子就不能作表行。记进来，退出码于是不会把一次诊断跑当成一次成表跑。
    if !args.tools.is_table_arm() {
        missing.push(
            "tools: this run used the tools-off diagnostic arm, so it is not a main-table row"
                .to_string(),
        );
    }
    if wiring.backbone.is_none() {
        missing.push("backbone: no LLM upstream is configured".to_string());
    }
    if wiring.judge.is_none() {
        missing.push("judge: no generative judge upstream is configured".to_string());
    }
    if args.benchmarks.contains(&"swe-bench-pro") && wiring.swe_pro.is_none() {
        missing.push("swe-bench-pro: no container backend is wired".to_string());
    }
    if args.benchmarks.contains(&"toolathlon") && wiring.toolathlon.is_none() {
        missing.push("toolathlon: no compose backend is wired".to_string());
    }
    if args.scaffolds.contains(&"nql") && wiring.platform.is_none() {
        missing.push("nql: no platform is wired".to_string());
    }

    let backbone: Arc<dyn LlmClient> = wiring
        .backbone
        .clone()
        .unwrap_or_else(|| Arc::new(NoBackbone::new("no llm_routing backend is configured")));
    let platform: Arc<dyn PlatformRunner> = wiring
        .platform
        .clone()
        .unwrap_or_else(|| Arc::new(NoPlatform::new("the composition root wires no platform")));

    let mut rig = Rig::new(
        backbone,
        RunConfig {
            data_root: data_root.clone(),
            seeds: args.seeds.clone(),
            max_concurrency: 4,
            subset: subset.clone(),
        },
    );
    for scaffold in table_scaffolds(platform) {
        if args.scaffolds.contains(&scaffold.name()) {
            rig = rig.with_scaffold(scaffold);
        }
    }

    let mut benchmarks = Vec::new();
    for name in &args.benchmarks {
        benchmarks.push(build_benchmark(
            name,
            wiring.judge.clone(),
            wiring,
            args.tools,
        )?);
    }

    let table = rig.run(&benchmarks).await;
    let errored: usize = table.cells.iter().map(|c| c.errored()).sum();

    // 读不出题的基准会留下一整列「题数为 0」的格子。那不是一个读数为零的格子，是这一列
    // 没有读数——两者在表里都写 0，必须在这里分开。运行器把原因带了出来（数据读不出来／
    // 子集点名的题不在这一版数据里），照它的原话记：这两条追查方向完全不同，合成一句
    // 「题数为 0」就等于把因说成了命。
    for (benchmark, why) in &table.unreadable {
        missing.push(format!("{benchmark}: {why}"));
    }
    for cell in &table.cells {
        let named = table
            .unreadable
            .iter()
            .any(|(benchmark, _)| benchmark == &cell.benchmark);
        if !named && cell.per_seed.iter().all(|s| s.total == 0) {
            let why = format!(
                "{}: the case source read no cases (the column would read as a score of zero)",
                cell.benchmark
            );
            if !missing.contains(&why) {
                missing.push(why);
            }
        }
    }

    let text = render(
        args,
        WiringReport {
            missing: &missing,
            platform_base: wiring.platform_base.as_deref(),
        },
        Readings {
            data_root: &data_root,
            pins,
            verified: &verified,
            subset: subset.as_deref(),
            table: &table,
            errored,
        },
    );
    Ok(Outcome {
        text,
        missing,
        errored,
    })
}

/// 按名字造一个基准。名字与取数适配器只有这一份权威：画子集那条命令也走这里，
/// 不再抄一份「哪个基准叫什么、题从哪儿读」。
pub(crate) fn build_benchmark(
    name: &str,
    judge: Option<Arc<dyn LlmClient>>,
    wiring: &Wiring,
    tools: ToolArm,
) -> Result<Benchmark> {
    // 工具臂在这一处收口：`None` 是诊断臂；有工具臂（主表那一臂）要一整套工具实现，而
    // 今天组合根没有。少给几个工具会让这一格因为「工具面不同」得分——那是在量别的东西，
    // 所以宁可不跑。要现在就跑，只能显式选诊断臂。
    if tools == ToolArm::Platform {
        bail!(
            "--tools platform (the default): the with-tools arm needs implementations for the \
             tools each benchmark pins (web_search/python, bash/file_edit, the MCP servers), and \
             the composition root has none. Running the tools-off arm instead would score every \
             case on a different tool face; pass `--tools none` explicitly if all you want is a \
             diagnostic run."
        );
    }
    Ok(match name {
        "hle" => hle_benchmark(judge, HleToolkit::without_tools()),
        "swe-bench-pro" => {
            swe_pro_benchmark(wiring.swe_pro.clone(), SweProToolkit::without_tools())
        }
        "toolathlon" => toolathlon_benchmark(
            wiring.toolathlon.clone(),
            ToolathlonToolkit::without_tools(),
        ),
        other => bail!("unknown benchmark `{other}`"),
    })
}

/// 报表里 `## wiring` 那一段要的两样东西：说不出话的依赖（缺席），以及接上了的那个平台
/// 是谁。两者都是「这次跑的外部上游是什么」，所以一起递。
struct WiringReport<'a> {
    missing: &'a [String],
    platform_base: Option<&'a str>,
}

/// 要渲染的那次跑读了什么、跑出了什么。
///
/// 数据是哪一份（`data_root`、`pins`、`verified`、`subset`）与结果是哪一份（`table`、
/// `errored`）是同一件事的两半——「这张表说的是哪次跑」缺了任何一半都答不出来，所以
/// 一起递，而不是在参数表上排成一串。
struct Readings<'a> {
    data_root: &'a Path,
    pins: &'a Pins,
    verified: &'a BTreeMap<String, String>,
    subset: Option<&'a CaseSubset>,
    table: &'a Table,
    errored: usize,
}

fn render(args: &EvalTableArgs, wiring: WiringReport<'_>, readings: Readings<'_>) -> String {
    let Readings {
        data_root,
        pins,
        verified,
        subset,
        table,
        errored,
    } = readings;
    let mut out = String::new();
    out.push_str("# eval-table\n\n## pins\n");
    out.push_str(&format!("backbone\t{}\n", args.backbone.render()));
    out.push_str(&format!("judge\t{}\n", args.judge.render()));
    out.push_str(&format!("tools\t{}\n", args.tools.render()));
    // 诊断臂要把自己的身份写在 pins 段里，紧挨着它标注的那一行：读的人从这一行就能
    // 看出下面的数字为什么不是表行，不用去猜 `tools none` 意味着什么。
    if !args.tools.is_table_arm() {
        out.push_str(
            "diagnostic\tthis run is the tools-off diagnostic arm; its numbers are not a table row\n",
        );
    }
    out.push_str(&format!(
        "seeds\t{}\n",
        args.seeds
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    ));
    out.push_str(&format!("data-root\t{}\n", data_root.display()));
    // 跑的是哪一批题，跟基准数据是哪个版本一样，是这张表说的话的一部分：两次跑用了不同的
    // 子集（或一次全量一次子集），表头看不出区别的话，两个分就不该并排读。
    if let Some(subset) = subset {
        out.push_str(&format!(
            "subset\t{}\tsha256:{}\n",
            subset.source().path.display(),
            subset.source().sha256
        ));
        for name in &args.benchmarks {
            if let Some(count) = subset.count(name) {
                out.push_str(&format!("subset-cases\t{name}\t{count}\n"));
            }
        }
    }
    for name in &args.benchmarks {
        if let Some(pin) = pins.by_benchmark.get(pins_key(name)) {
            let (kind, read) = match &pin.read {
                PinRead::File { path, .. } => ("read-file", path.as_str()),
                PinRead::Dir { path } => ("read-dir", path.as_str()),
            };
            out.push_str(&format!(
                "benchmark\t{name}\trev={}\t{kind}={read}\tverified[{}]\n",
                pin.revision,
                verified.get(*name).map(String::as_str).unwrap_or("no")
            ));
        }
    }

    out.push_str("\n## wiring\n");
    // 平台接上了就要说出接在哪：NQL 那一行的读数长在哪个平台上，跟骨干的模型名一样
    // 是这张表能被复跑的前提，不能只剩「接上了」三个字。
    if let Some(base) = wiring.platform_base {
        out.push_str(&format!("platform\t{base}\n"));
    }
    if wiring.missing.is_empty() {
        out.push_str("complete\n");
    } else {
        for line in wiring.missing {
            out.push_str(&format!("missing\t{line}\n"));
        }
    }

    out.push_str("\n## table\n");
    if args.tools.is_table_arm() {
        out.push_str(&table.to_markdown());
    } else {
        // 拒收为表行：工具面与主表不同的跑次，格子与它并排放的就不是同一件事。逐格读数
        // 仍在下面的 `## per seed` 里，够诊断用，但它不构成一行表。
        out.push_str(
            "(withheld: this run is the tools-off diagnostic arm; its cells are not a table row. \
             The per-seed readings below are kept for diagnosis.)\n",
        );
    }

    out.push_str("\n## per seed\n");
    for col in &table.cols {
        for row in &table.rows {
            let Some(cell) = table.cell(row, col) else {
                continue;
            };
            for seed in &cell.per_seed {
                out.push_str(&format!(
                    "cell\t{col}\t{row}\tseed={}\tresolved={}\ttotal={}\terrored={}\ttokens={}\tsteps={}\n",
                    seed.seed, seed.resolved, seed.total, seed.errored, seed.tokens, seed.steps
                ));
            }
        }
    }

    // 判词按题目 id 排，不按完成先后：并发跑的时候完成序是调度给的，两次跑不一样，
    // 而这张表要能两次 diff 相等。
    out.push_str("\n## failures\n");
    for col in &table.cols {
        for row in &table.rows {
            let Some(cell) = table.cell(row, col) else {
                continue;
            };
            for seed in &cell.per_seed {
                let mut failures = seed.failures.clone();
                failures.sort();
                let shown = failures.len().min(FAILURE_SAMPLE);
                for (case, detail) in &failures[..shown] {
                    out.push_str(&format!(
                        "failure\t{col}\t{row}\tseed={}\t{case}\t{detail}\n",
                        seed.seed
                    ));
                }
                if failures.len() > shown {
                    out.push_str(&format!(
                        "failure\t{col}\t{row}\tseed={}\t…and {} more\n",
                        seed.seed,
                        failures.len() - shown
                    ));
                }
            }
        }
    }

    out.push_str(&format!("\nerrored\t{errored}\n"));
    out
}

/// 子命令入口的参数：命令行给的是 `argv[0]=cogneva`，第二个词是子命令名。
pub fn operands(argv: &[String]) -> Vec<String> {
    let mut rest: Vec<String> = argv.to_vec();
    if rest.first().map(String::is_empty) == Some(false) {
        rest.remove(0);
    }
    // 子命令名是这里第一个不以 `-` 开头的词；每个子命令的开关都是 `--x`，所以这个判据
    // 不会把某个参数当成命令名吃掉。照名字写死一个，加一条子命令就要回来改这里一次。
    if rest
        .first()
        .map(|word| !word.starts_with('-'))
        .unwrap_or(false)
    {
        rest.remove(0);
    }
    rest
}

/// 组合根把平台接进 `PlatformRunner` 端口：端点、令牌、时限都从环境取。
///
/// 端点缺席时交回 `None` 而不是一个会立刻失败的实现——驱动那边留着 `NoPlatform`，
/// NQL 那一行于是报「跑不起来」，而不是拿一个假答复去计分。
///
/// 平台与骨干是两条独立的上游：`llm_routing` 读不出来不该顺手把平台也丢掉，所以这个
/// 函数在 `wiring_from_config` 里先于任何骨干的早退被调用。
fn platform_from_env() -> Option<(Arc<dyn PlatformRunner>, String)> {
    let settings = BridgeSettings::from_env()?;
    let (runner, base) = platform_from_settings(settings);
    Some((Arc::new(runner), base))
}

/// 把一份端点配置变成平台端口，并把它的身份一并交出。端点从环境读还是从别处读与这里无关，
/// 所以这两段分开：接线本身能被单测钉住，而不用去动进程的环境变量。
fn platform_from_settings(settings: BridgeSettings) -> (PlatformApiRunner, String) {
    let base = settings.base.clone();
    let http: Arc<dyn HttpClient> = Arc::new(cog_net::factory::ReqwestHttpClient::from_config(
        &cog_net::HttpClientConfig::load().unwrap_or_else(|e| {
            tracing::warn!("http client config is unreadable ({e}); platform bridge uses defaults");
            Default::default()
        }),
    ));
    (PlatformApiRunner::new(http, settings), base)
}

/// 组装这一次跑的依赖：骨干与裁判来自 `llm_routing`，缺了就缺席，不猜。
///
/// pin 里写的模型名如果没有任何已启用的上游在供，不算「有了这个依赖」——那只是一张贴纸。
/// 这里把它记成缺席而不是直接失败：表仍然出得来，读的人从 wiring 段看见真相。
async fn wiring_from_config(args: &EvalTableArgs) -> Wiring {
    let mut wiring = Wiring::default();
    if let Some((platform, base)) = platform_from_env() {
        wiring.platform = Some(platform);
        wiring.platform_base = Some(base);
    }
    let routing = match cog_llm::LLMRoutingConfig::load() {
        Ok(routing) => routing,
        Err(e) => {
            tracing::warn!("llm routing is unavailable: {e}");
            return wiring;
        }
    };
    let served: Vec<String> = routing
        .backends
        .iter()
        .filter(|b| b.enabled)
        .map(|b| b.model.clone())
        .collect();
    let mut withheld: Vec<String> = Vec::new();
    if served.is_empty() {
        withheld.push("no enabled llm_routing backend serves any model".to_string());
    } else {
        for pin in [&args.backbone, &args.judge] {
            if !served.iter().any(|m| m == &pin.model) {
                withheld.push(format!(
                    "{}: the pin names `{}`, which no enabled upstream serves (have: {})",
                    pin.role,
                    pin.model,
                    served.join(", ")
                ));
            }
        }
    }
    if !withheld.is_empty() {
        for line in &withheld {
            tracing::warn!("eval-table pin not served: {line}");
        }
        return wiring;
    }

    let capacity = cog_llm::TuningConfig::load()
        .map(|t| t.stream_capacity)
        .unwrap_or_default();
    let max_tokens = crate::config_loader::load()
        .core
        .system
        .anthropic_default_max_tokens;
    match cog_llm::plugin::build_llm_provider(capacity, max_tokens, &routing, None) {
        Ok(llm) => {
            // 骨干与裁判今天是同一个上游：判分那一路的模型选择属配置面（llm_routing
            // 单入口），不在这里再开一个。
            wiring.backbone = Some(llm.clone());
            wiring.judge = Some(llm);
        }
        Err(e) => tracing::warn!("no backbone provider could be built: {e}"),
    }
    wiring
}

/// 子命令主入口。
pub async fn run_from_args() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().collect();
    let args = match EvalTableArgs::parse(&operands(&argv)) {
        Ok(args) => args,
        // 用法跟着错误走：缺一个必填项与写错一个选项名，读的人都要能当场看见这个
        // 子命令收什么。
        Err(e) => return Err(format!("{e}\n\n{USAGE}").into()),
    };
    let pins = Pins::load(&args.pins)?;
    let wiring = wiring_from_config(&args).await;
    let outcome = run_table(&args, &wiring, &pins).await?;

    match &args.out {
        Some(path) => {
            let mut file = std::fs::File::create(path)
                .with_context(|| format!("cannot write {}", path.display()))?;
            file.write_all(outcome.text.as_bytes())?;
        }
        None => print!("{}", outcome.text),
    }

    // 退出码说的是「这次跑是不是一次完整的实验」：缺了东西、或有题跑不动，都不是。
    if !outcome.missing.is_empty() {
        return Err(format!(
            "this run is not a complete experiment: {}",
            outcome.missing.join("; ")
        )
        .into());
    }
    if outcome.errored > 0 {
        return Err(format!(
            "{} case run(s) failed to run: the table is not a clean reading",
            outcome.errored
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(raw: &[&str]) -> Result<EvalTableArgs, String> {
        EvalTableArgs::parse(&raw.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    fn minimal() -> Vec<String> {
        [
            "--pins",
            "/tmp/pins.tsv",
            "--tools",
            "none",
            "--backbone",
            "m@1",
            "--judge",
            "j@1",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn benchmarks_and_scaffolds_come_out_in_the_canonical_order() {
        let mut raw = minimal();
        raw.extend(
            ["--benchmarks", "toolathlon,hle"]
                .iter()
                .map(|s| s.to_string()),
        );
        raw.extend(["--scaffolds", "nql,codeact"].iter().map(|s| s.to_string()));
        let parsed = EvalTableArgs::parse(&raw).unwrap();
        assert_eq!(parsed.benchmarks, vec!["hle", "toolathlon"]);
        assert_eq!(parsed.scaffolds, vec!["codeact", "nql"]);

        // 命令行给的先后不同，选出来的顺序必须一样：列序不能由参数顺序决定，
        // 否则「同一组基准」会因为参数写的先后不同而产出两张对不上的表。
        let mut other = minimal();
        other.extend(
            ["--benchmarks", "hle,toolathlon"]
                .iter()
                .map(|s| s.to_string()),
        );
        other.extend(["--scaffolds", "codeact,nql"].iter().map(|s| s.to_string()));
        let other = EvalTableArgs::parse(&other).unwrap();
        assert_eq!(parsed.benchmarks, other.benchmarks);
        assert_eq!(parsed.scaffolds, other.scaffolds);
    }

    #[test]
    fn the_defaults_are_the_main_table_shape() {
        let parsed = EvalTableArgs::parse(&minimal()).unwrap();
        assert_eq!(parsed.benchmarks, BENCHMARKS.to_vec());
        assert_eq!(parsed.scaffolds, SCAFFOLDS.to_vec());
        assert_eq!(parsed.seeds, vec![0, 1, 2], "主表规格是三个种子");
    }

    #[test]
    fn the_pins_have_no_default_and_the_tool_arm_defaults_to_platform() {
        // pins 缺了这张表不比任何东西；工具臂则有个正确的默认——主表跑的那一臂。诊断臂
        // 要显式选，好让「这次不是表行」在命令行上就被写下来。
        assert_eq!(
            EvalTableArgs::parse(&minimal()).unwrap().tools,
            ToolArm::None,
            "显式 --tools none 就照它办"
        );

        let without_arm: Vec<String> = minimal()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 2 && *i != 3)
            .map(|(_, s)| s.clone())
            .collect();
        assert_eq!(
            EvalTableArgs::parse(&without_arm).unwrap().tools,
            ToolArm::Platform,
            "不给就是主表那一臂"
        );
        assert!(args(&["--backbone", "m@1", "--judge", "j@1"])
            .unwrap_err()
            .contains("--pins"));
    }

    fn one_cell_table() -> Table {
        Table {
            rows: vec!["nql".into()],
            cols: vec!["hle".into()],
            cells: vec![],
            wanted_seeds: 3,
            unreadable: vec![],
        }
    }

    #[test]
    fn the_tools_off_arm_marks_itself_diagnostic_and_withholds_the_table() {
        let args = EvalTableArgs::parse(&minimal()).unwrap();
        assert!(!args.tools.is_table_arm(), "none 不是表行那一臂");
        let verified = BTreeMap::new();
        let text = render(
            &args,
            WiringReport {
                missing: &[],
                platform_base: None,
            },
            Readings {
                data_root: Path::new("/data"),
                pins: &Pins::default(),
                verified: &verified,
                subset: None,
                table: &one_cell_table(),
                errored: 0,
            },
        );
        assert!(text.contains("tools\tnone\n"), "{text}");
        assert!(text.contains("diagnostic\t"), "{text}");
        assert!(
            text.contains("## table\n(withheld:"),
            "表体必须被拒收：{text}"
        );
        assert!(!text.contains("| Scaffold |"), "{text}");
    }

    #[test]
    fn the_platform_arm_is_the_one_that_carries_the_table() {
        let mut raw = minimal();
        raw.extend(["--tools", "platform"].iter().map(|s| s.to_string()));
        let args = EvalTableArgs::parse(&raw).unwrap();
        assert!(args.tools.is_table_arm());
        let verified = BTreeMap::new();
        let text = render(
            &args,
            WiringReport {
                missing: &[],
                platform_base: None,
            },
            Readings {
                data_root: Path::new("/data"),
                pins: &Pins::default(),
                verified: &verified,
                subset: None,
                table: &one_cell_table(),
                errored: 0,
            },
        );
        assert!(!text.contains("diagnostic\t"), "{text}");
        assert!(text.contains("| Scaffold |"), "表体要在：{text}");
    }

    #[test]
    fn unknown_names_and_shapes_are_errors_not_silent_drops() {
        let mut raw = minimal();
        raw.extend(["--benchmarks", "hle,gaia"].iter().map(|s| s.to_string()));
        assert!(EvalTableArgs::parse(&raw).unwrap_err().contains("gaia"));

        let mut raw = minimal();
        raw.extend(["--scaffolds", "planner"].iter().map(|s| s.to_string()));
        assert!(EvalTableArgs::parse(&raw).unwrap_err().contains("planner"));

        let mut raw = minimal();
        raw.extend(["--seeds", "0,x"].iter().map(|s| s.to_string()));
        assert!(EvalTableArgs::parse(&raw)
            .unwrap_err()
            .contains("not a seed"));

        let mut raw = minimal();
        raw.extend(["--tools", "maybe"].iter().map(|s| s.to_string()));
        assert!(EvalTableArgs::parse(&raw).unwrap_err().contains("maybe"));

        let mut raw = minimal();
        raw.extend(["--backbone", "noversion"].iter().map(|s| s.to_string()));
        assert!(EvalTableArgs::parse(&raw)
            .unwrap_err()
            .contains("<model>@<version>"));

        let mut raw = minimal();
        raw.extend(["--seeds", "1,1"].iter().map(|s| s.to_string()));
        assert!(EvalTableArgs::parse(&raw)
            .unwrap_err()
            .contains("listed twice"));
    }

    #[test]
    fn a_model_pin_splits_at_the_last_at_sign() {
        let pin = ModelPin::parse("backbone", "vendor/model@2026-10-01").unwrap();
        assert_eq!(pin.model, "vendor/model");
        assert_eq!(pin.version, "2026-10-01");
        assert_eq!(pin.render(), "vendor/model@2026-10-01");
    }

    #[test]
    fn a_pins_file_is_read_by_kind_and_an_unknown_kind_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pins.tsv");
        std::fs::write(
            &path,
            "source\thle\trev1\thle/p.parquet\tabc\t10\tfetched from the mirror\n\
             read-file\thle\trev1\thle/x.jsonl\tdef\t20\tderived by --convert\n\
             read-dir\ttoolathlon\trev2\ttoolathlon/tasks/finalpool\t-\t-\tdir\n",
        )
        .unwrap();
        let pins = Pins::load(&path).unwrap();
        assert_eq!(pins.by_benchmark.len(), 2);
        assert_eq!(pins.by_benchmark["hle"].revision, "rev1");
        assert_eq!(
            pins.by_benchmark["hle"].read,
            PinRead::File {
                path: "hle/x.jsonl".into(),
                sha256: "def".into(),
                size: 20
            }
        );
        assert_eq!(
            pins.by_benchmark["hle"].source.as_ref().unwrap().sha256,
            "abc"
        );
        assert_eq!(
            pins.by_benchmark["toolathlon"].read,
            PinRead::Dir {
                path: "toolathlon/tasks/finalpool".into()
            }
        );

        std::fs::write(&path, "wat\thle\trev1\tx\ty\t1\tz\n").unwrap();
        assert!(Pins::load(&path).unwrap_err().to_string().contains("wat"));

        std::fs::write(&path, "read-file\thle\trev1\tx\ty\n").unwrap();
        assert!(Pins::load(&path).unwrap_err().to_string().contains("seven"));

        // 一份被钉住的读数只有一份：两行都说自己钉着同一列，读的人不知道信哪个。
        std::fs::write(
            &path,
            "read-file\thle\trev1\tx\ty\t1\tz\nread-file\thle\trev1\tx\tw\t2\tz\n",
        )
        .unwrap();
        assert!(Pins::load(&path)
            .unwrap_err()
            .to_string()
            .contains("two read records"));
    }

    #[test]
    fn a_column_without_a_pin_is_refused_before_it_runs() {
        let dir = tempfile::tempdir().unwrap();
        let err = Pins::default().verify(dir.path(), &["hle"]).unwrap_err();
        assert!(err.to_string().contains("no read path"), "{err}");
    }

    #[test]
    fn a_pin_that_does_not_match_the_bytes_on_disk_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("x.jsonl"), b"hello").unwrap();
        let mut pins = Pins::default();
        pins.by_benchmark.insert(
            "hle".into(),
            BenchmarkPin {
                revision: "rev1".into(),
                read: PinRead::File {
                    path: "x.jsonl".into(),
                    sha256: "0".repeat(64),
                    size: 5,
                },
                source: None,
            },
        );
        let err = pins.verify(dir.path(), &["hle"]).unwrap_err();
        assert!(err.to_string().contains("the pin says"), "{err}");

        // 长度对不上时先报长度：一个被截断的下载不该走到哈希那一步才知道不对。
        pins.by_benchmark.get_mut("hle").unwrap().read = PinRead::File {
            path: "x.jsonl".into(),
            sha256: "0".repeat(64),
            size: 6,
        };
        let err = pins.verify(dir.path(), &["hle"]).unwrap_err();
        assert!(err.to_string().contains("bytes"), "{err}");
    }

    #[test]
    fn a_directory_pin_is_verified_as_a_tree_not_silently_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mut pins = Pins::default();
        pins.by_benchmark.insert(
            "toolathlon-gym".into(),
            BenchmarkPin {
                revision: "rev1".into(),
                read: PinRead::Dir {
                    path: "toolathlon-gym/tasks/finalpool".into(),
                },
                source: None,
            },
        );
        let err = pins.verify(dir.path(), &["toolathlon"]).unwrap_err();
        assert!(err.to_string().contains("not a directory"), "{err}");
        std::fs::create_dir_all(dir.path().join("toolathlon-gym/tasks/finalpool")).unwrap();
        let how = pins.verify(dir.path(), &["toolathlon"]).unwrap();
        assert!(how["toolathlon"].contains("no hash of its own"));
    }

    #[test]
    fn the_toolathlon_column_reads_the_pins_record_keyed_by_its_repository_name() {
        // 列名与取数脚本目录名在 Toolathlon 上不同名：表里这一列是 Toolathlon，数据来自
        // eigent-ai/toolathlon_gym，取数脚本按仓库叫它 toolathlon-gym。两个名字的对应写在
        // 驱动器里一处，这条测试把那一处钉住——找不到记录时要报的是「列名 vs 记录名」。
        assert_eq!(pins_key("toolathlon"), "toolathlon-gym");
        assert_eq!(pins_key("hle"), "hle");
        assert_eq!(pins_key("swe-bench-pro"), "swe-bench-pro");

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("toolathlon-gym/tasks/finalpool")).unwrap();
        std::fs::write(dir.path().join("toolathlon-gym.tar.gz"), b"x").unwrap();
        use sha2::{Digest, Sha256};
        let sha = hex::encode(Sha256::digest(b"x"));
        let mut pins = Pins::default();
        pins.by_benchmark.insert(
            "toolathlon-gym".into(),
            BenchmarkPin {
                revision: "rev".into(),
                read: PinRead::Dir {
                    path: "toolathlon-gym/tasks/finalpool".into(),
                },
                source: Some(PinSource {
                    path: "toolathlon-gym.tar.gz".into(),
                    sha256: sha,
                    size: 1,
                }),
            },
        );
        let how = pins.verify(dir.path(), &["toolathlon"]).unwrap();
        assert!(
            how["toolathlon"].contains("read-dir toolathlon-gym/tasks/finalpool"),
            "{}",
            how["toolathlon"]
        );
        assert!(
            how["toolathlon"].contains("source toolathlon-gym.tar.gz sha256("),
            "{}",
            how["toolathlon"]
        );
    }

    #[test]
    fn a_source_pin_that_is_not_on_disk_is_said_out_loud() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("x.jsonl"), b"hello").unwrap();
        let sha = sha256_file(&dir.path().join("x.jsonl")).unwrap();
        let mut pins = Pins::default();
        pins.by_benchmark.insert(
            "hle".into(),
            BenchmarkPin {
                revision: "rev1".into(),
                read: PinRead::File {
                    path: "x.jsonl".into(),
                    sha256: sha,
                    size: 5,
                },
                source: Some(PinSource {
                    path: "hle/p.parquet".into(),
                    sha256: "f".repeat(64),
                    size: 999,
                }),
            },
        );
        let how = pins.verify(dir.path(), &["hle"]).unwrap();
        assert!(how["hle"].contains("not on disk"), "{}", how["hle"]);
    }

    #[test]
    fn the_operand_list_drops_the_program_and_the_subcommand_name() {
        let argv: Vec<String> = ["cogneva", "eval-table", "--pins", "p"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(operands(&argv), vec!["--pins", "p"]);
    }

    /// 组合根这条接线自己的判词：端点一给就接上、且把接在哪一起交出；端点不给就不接。
    ///
    /// 「不给就不接」是这里的重点——驱动那边留着会报错的 `NoPlatform`，NQL 那一行走的是
    /// 「跑不起来」而不是一个低分，靠的正是这里交回 `None`。
    #[test]
    fn a_configured_endpoint_is_wired_and_a_missing_one_is_not() {
        let settings = BridgeSettings::from_parts(Some("http://platform.test/"), None, None)
            .expect("a base is enough to configure the bridge");
        let (runner, base) = platform_from_settings(settings);
        // 报出去的身份与真正用上的端点是同一个：两处走散的端点比不报还坏。
        assert_eq!(base, "http://platform.test");
        assert_eq!(runner.base(), "http://platform.test");

        assert!(
            BridgeSettings::from_parts(None, Some("t"), None).is_none(),
            "没有端点就不算接上了平台"
        );
    }
}
