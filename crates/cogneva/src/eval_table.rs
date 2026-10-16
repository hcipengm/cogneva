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
use cog_eval::scaffolds::{table_scaffolds, ForeignFlow, PlatformRunner};
use cog_eval::{Benchmark, CaseSubset, Checkpoint, Rig, RunConfig, Table};
use sha2::Digest;

use crate::eval_platform::{
    BridgeSettings, NoBackbone, NoPlatform, PlatformApiRunner, PLATFORM_BASE_ENV,
};

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
                          [--scaffolds codeact,gepa,agentflow,nql[,<declared flow>]] [--seeds 0,1,2]
                          [--subset <file>] [--out <file>]
                          [--concurrency-slot <n>] [--admission <dir>]
                          [--admission-floor-unmeasured <n>]
                          [--admission-ceiling-unmeasured <n>]

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
  --scaffolds  comma-separated; default every row of this run's row set -- the four in-tree
               scaffolds, plus whatever external flows the declaration in the environment names
               (COGNEVA_EVAL_FOREIGN_FLOWS). An external flow is selected by the name it is
               declared under; a name nobody declared is not a scaffold.

  --seeds      comma-separated independent runs per cell; default 0,1,2 (the main-table spec)
  --subset     run the cases an archived subset names, instead of every case. The archive is a
               file of `benchmark<TAB>case id` lines drawn once by `cogneva eval-case-subset`;
               it is read, never redrawn here. A benchmark the archive does not cover, or an id
               this data root does not have, stops the run instead of silently running in full
               or dropping a case. The archive's path and content hash go into the table header,
               because which cases ran is part of what the table says.
  --out        write the output here instead of standard output
  --concurrency-slot
               how many cases this run may have in flight at once. It is one of three factors and
               the effective concurrency is the smallest of them: this slot, the admission gate's
               bound at that moment, and each column's own isolation ceiling (toolathlon's cases
               share one postgres, so its ceiling is one). A slot and a gate are not alternatives:
               give both and both apply. At least one of the two has to be given -- a run with
               neither is refused before it starts, because nothing would bound how much of a host
               that also runs the cluster this run takes.
  --admission  an existing directory that turns the admission gate on. Point it at the same
               directory the build gate uses (self_evolution.build_gate.lock_dir): the point of
               it is that builds and benchmark cases contend for one machine, and two gates on
               two directories cap each other's absence. The directory has to exist -- a new
               empty one would gate nothing while looking like it does. Without this flag the
               gate is not in force and the run says so, and then the static slot and each column's
               own ceiling are the only bounds left. The gate's bound is not a constant: a
               controller moves it between the
               two bounds below as the host's pressure moves, and only judged cases are ever held
               back.
  --admission-floor-unmeasured
  --admission-ceiling-unmeasured
               the gate's lower and upper bounds. NEITHER HAS BEEN MEASURED on this host: both are
               the driver's own form defaults (a floor that is never zero, a placeholder ceiling),
               not readings. The real ceiling has to be derived per case from its peak memory and
               cpu seconds, which waits for the benchmarks' execution side. The numbers in force
               are printed in the report's gate section, where they can be read back. They only
               mean anything with --admission: passing one without it is an error rather than a
               silently ignored number.

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
    /// 已经排成规范顺序：仓里那四行在前，声明进来的外部流派按声明顺序跟在后面。
    ///
    /// 名字是 `String` 而不是 `&'static str`：外面的流派**从环境里声明进来**，它的名字
    /// 不在编译期的那张表里。
    pub scaffolds: Vec<String>,
    pub seeds: Vec<u64>,
    /// 只跑这份存档点名的题；`None` ＝全量。
    pub subset: Option<PathBuf>,
    /// 这一格同时在跑的 case 数上限。**没有默认值**：给了默认值，这次跑的界就从命令行上
    /// 消失了，而它决定了这台与集群同机的宿主被压到什么程度。
    ///
    /// 它与闸门**不是二选一**：有效并发 ＝ min(这一列的隔离上界, 闸门此刻的界, 这个名额)。
    /// 两个都不给是解析期的错误，不是「那就用一个隐含的数」。
    pub concurrency_slot: Option<usize>,
    /// 准入闸门的目录；`None` ＝不开闸门，这时只有静态名额与每一列自己的隔离上界在管。
    ///
    /// 目录**由参数直接给出**，不给默认值：默认值会把「指到构建方那个目录」变成一句话里
    /// 的一个约定，而指错目录的闸门与没开闸门在报表上是两件不同的事，前者更糟——它看起来
    /// 在保护宿主。
    pub admission_dir: Option<PathBuf>,
    /// 闸门的下界。**未实测**。
    pub admission_floor_unmeasured: Option<usize>,
    /// 闸门的上界。**未实测**。
    pub admission_ceiling_unmeasured: Option<usize>,
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

fn count_arg(args: &[String], at: usize, name: &str) -> Result<usize, String> {
    let raw = next_arg(args, at, name)?;
    let n: usize = raw
        .parse()
        .map_err(|e| format!("{name}: `{raw}` is not a count: {e}"))?;
    if n == 0 {
        return Err(format!(
            "{name}: 0 is not a bound -- a gate that admits nothing is a stopped run wearing the \
             gate's name"
        ));
    }
    Ok(n)
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

/// 与 [`canonicalize`] 同一件事，只是认识的名字分成两处来源：编译期那四个，加上环境里
/// **声明**进来的外部流派。所以要 owned 的名字与 owned 的返回。
fn canonicalize_owned(raw: &[String], known: &[String], what: &str) -> Result<Vec<String>, String> {
    for name in raw {
        if !known.contains(name) {
            return Err(format!(
                "unknown {what} `{name}` (have: {})",
                known.join(", ")
            ));
        }
    }
    Ok(known.iter().filter(|k| raw.contains(k)).cloned().collect())
}

impl EvalTableArgs {
    /// 只看仓里那四行是不是合法。认识**外部流派**名字的调用方走
    /// [`EvalTableArgs::parse_with`]。
    pub fn parse(args: &[String]) -> Result<Self, String> {
        Self::parse_with(args, &[])
    }

    /// `declared_scaffolds` ＝ 环境里声明进来的外部流派名字。
    ///
    /// 为什么要传进来而不是在这里读环境：`--scaffolds` 的合法取值是「这次实验的行集的
    /// 那一份声明」，而那份声明来自环境——读在解析里，解析就不再是一个纯函数，命令行校验收
    /// 到的就不再是参数本身。
    pub fn parse_with(args: &[String], declared_scaffolds: &[String]) -> Result<Self, String> {
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
        let mut concurrency_slot: Option<usize> = None;
        let mut admission_dir: Option<PathBuf> = None;
        let mut admission_floor: Option<usize> = None;
        let mut admission_ceiling: Option<usize> = None;

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
                "--concurrency-slot" => {
                    concurrency_slot = Some(count_arg(args, i, flag)?);
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
                "--admission" => {
                    admission_dir = Some(PathBuf::from(next_arg(args, i, flag)?));
                    i += 2;
                }
                "--admission-floor-unmeasured" => {
                    admission_floor = Some(count_arg(args, i, flag)?);
                    i += 2;
                }
                "--admission-ceiling-unmeasured" => {
                    admission_ceiling = Some(count_arg(args, i, flag)?);
                    i += 2;
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        let selected = match benchmarks {
            Some(raw) => canonicalize(&raw, &BENCHMARKS, "benchmark")?,
            None => BENCHMARKS.to_vec(),
        };
        // 行集的合法取值：仓里那四行 + 这次声明进来的外部流派。不去重也不排序声明进来的
        // 那几个——它们按**声明的顺序**排在四行之后，表里行的次序于是与声明的次序一致。
        let mut known_scaffolds: Vec<String> =
            SCAFFOLDS.iter().map(|name| name.to_string()).collect();
        for name in declared_scaffolds {
            if !known_scaffolds.contains(name) {
                known_scaffolds.push(name.clone());
            }
        }
        let scaffold_names = match scaffolds {
            Some(raw) => canonicalize_owned(&raw, &known_scaffolds, "scaffold")?,
            None => known_scaffolds.clone(),
        };

        // 两个界限只在闸门开着时才有意义。给了数却不开闸门，是「以为自己在设一个界」而
        // 实际上没有—— silently 吞掉它，这次跑就会以静态名额跑完，而命令行上写着上界 3。
        for (flag, set) in [
            ("--admission-floor-unmeasured", admission_floor),
            ("--admission-ceiling-unmeasured", admission_ceiling),
        ] {
            if set.is_some() && admission_dir.is_none() {
                return Err(format!(
                    "{flag} needs --admission <dir>: a bound on a gate that is not in force is \
                     not a bound"
                ));
            }
        }
        if let (Some(floor), Some(ceiling)) = (admission_floor, admission_ceiling) {
            if floor > ceiling {
                return Err(format!(
                    "--admission-floor-unmeasured {floor} is above --admission-ceiling-unmeasured \
                     {ceiling}: the bound lives between them, and an empty interval has none"
                ));
            }
        }

        // 两个界至少要给一个，而且不是在这里补一个默认值：这次跑放行几个，是命令行上必须
        // 显式说出来的东西，缺了就停在解析这一步——跑起来才发现没有界，读的人只会以为
        // 「界是宿主给的」。
        if concurrency_slot.is_none() && admission_dir.is_none() {
            return Err(
                "--concurrency-slot or --admission is required: effective concurrency is the \
                 smallest of each column's own isolation ceiling, the gate's bound, and this \
                 slot, and a run that gives neither of the last two has no bound at all"
                    .into(),
            );
        }

        Ok(Self {
            data_root,
            benchmarks: selected,
            scaffolds: scaffold_names,
            // 种子表的默认值取自评测台的主表规格，不在这里再抄一份数字。
            seeds: seeds.unwrap_or_else(|| RunConfig::new(".").seeds),
            subset,
            concurrency_slot,
            pins: pins.ok_or("--pins is required: a table without pins is not comparable")?,
            // 工具臂默认 `platform`：那是主表要跑的那一臂。`none` 要显式写出，好让
            // 「这次跑的是一臂诊断」在命令行上就被点明，而不是默认落进去。
            tools: tool_arm.unwrap_or(ToolArm::Platform),
            backbone: backbone.ok_or("--backbone <model>@<version> is required")?,
            judge: judge.ok_or("--judge <model>@<version> is required")?,
            admission_dir,
            admission_floor_unmeasured: admission_floor,
            admission_ceiling_unmeasured: admission_ceiling,
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
    /// 传输建不出来时的那句原因。有了它，`backbone` 为 `None` 才说得分明：是**没有上游**，
    /// 还是**上游配了、传输没建出来**。两件事要读的人做的事不同（配 llm_routing 还是修
    /// http_client），一句「没有上游」把指引抹平，还会把它读成一次配置缺失而不是一次故障。
    pub no_transport: Option<String>,
    /// `swe_pro` 为 `None` 时的那句原因。
    ///
    /// 这一列缺的不是代码了，缺的可能是三样互不相同的东西：这台宿主上没有可用的容器机器、
    /// 数据根里没有官方那两件脚本、或者这次被显式关了。三样要做的下一步分别是装机器、取脚本、
    /// 改开关——压成一句「没接上」，读的人会去翻一个翻不出东西的地方。
    pub swe_pro_reason: Option<String>,
    /// 这次声明进来的外部流派（按声明顺序）。空 ＝ 这次跑只有仓里那四行。
    pub foreign_flows: Vec<crate::eval_container::ForeignFlowSpec>,
    /// 声明读不出来时的那句原因。它是**整块**不成立的原因：一块声明里有一行写坏，
    /// 剩下的也不接——半块声明读起来像一次完整的实验，而它少了几行。
    pub foreign_reason: Option<String>,
}

/// 一次跑的结果：要写出去的那段文本，以及这次跑是不是一次完整的实验。
pub struct Outcome {
    pub text: String,
    /// 缺了什么（人为可读）。非空即这次跑不是一次完整的实验。
    pub missing: Vec<String>,
    /// 报出来跑不动的题数，**只数没人提前声明过的那部分**：这些格子本来该跑得动，跑不动是缺陷。
    ///
    /// 与 `blocked` 分开是这条读数的要点：同一个 `errored` 底下混着「上游挂了／代码断了」与
    /// 「这一格本来就没有那个后端」，而两者的处置正相反——前者要修，后者要去做那个后端，
    /// 或者换一条臂。**总数不是人口**：一个把 1080 例「没有容器后端」与 240 例「少配一段」
    /// 加在一起的总数，读不出该去修哪一半。
    pub errored: usize,
    /// 被**开跑前就声明过的缺席**挡住的题数。
    ///
    /// 它按未解决计分（分母不缩），但它不是「跑错了」，是「没轮到这个格子跑」。
    pub blocked: usize,
}

/// 跑一次表并渲染。依赖由调用方注入：命令行注入真上游与真平台，测试注入替身。
/// 这次跑的检查点：落在哪、以及它是谁。
///
/// 身份把**这次跑要用的每一件东西**都收进来：数据的 pin、骨干与裁判的模型、工具臂、
/// 基准与外壳清单、种子表、子集存档，以及**这份代码本身**。少了最后一项就够了不着的
/// 那一种错——「同一个外壳改了代码重跑」会接上一次那条老记录，把旧代码的结果当成新代码
/// 的读数交回来，而表上完全看不出来。
///
/// 它与随表出去的 pins 是同一批东西（表里的 pins 让**别人**能复跑这张表，这里的身份让
/// **这次**跑接得上自己上一次的半成品），所以两处都从这一份材料出，不各写一份。
fn run_checkpoint(
    args: &EvalTableArgs,
    verified: &BTreeMap<String, String>,
    subset: Option<&CaseSubset>,
) -> Checkpoint {
    let mut material = String::new();
    for (benchmark, how) in verified {
        material.push_str(&format!("pin\t{benchmark}\t{how}\n"));
    }
    material.push_str(&format!(
        "backbone\t{}@{}\n",
        args.backbone.model, args.backbone.version
    ));
    material.push_str(&format!(
        "judge\t{}@{}\n",
        args.judge.model, args.judge.version
    ));
    material.push_str(&format!("tools\t{}\n", args.tools.render()));
    material.push_str(&format!("benchmarks\t{}\n", args.benchmarks.join(",")));
    material.push_str(&format!("scaffolds\t{}\n", args.scaffolds.join(",")));
    material.push_str(&format!("seeds\t{:?}\n", args.seeds));
    material.push_str(&format!("software\t{}\n", env!("COGNEVA_VERSION_ID")));
    if let Some(subset) = subset {
        material.push_str(&format!("subset\t{}\n", subset.source().sha256));
    }
    let run_id = hex::encode(sha2::Sha256::digest(material.as_bytes()));
    let path = match &args.out {
        // 检查点跟着报表走：一次跑的两份产物放在一起，谁也不会拿另一份去接。
        Some(out) => {
            let mut name = out.file_name().unwrap_or_default().to_os_string();
            name.push(".cases.jsonl");
            out.with_file_name(name)
        }
        // 没有 `--out` 时落在当前目录，名字带上身份的前一截：两套参数在同一目录里
        // 跑不会互相顶掉，而身份仍然是文件里逐行核过的那一个。
        None => PathBuf::from(format!("eval-table-{}.cases.jsonl", &run_id[..12])),
    };
    Checkpoint { path, run_id }
}

/// 一条缺席声明**管到哪些格子**。
///
/// 这一格补的是报表里两段各说各话的那道缝：`## wiring` 说「缺 X」，而 `## per seed` 里那些
/// 格子照样印 `errored=40`。没有「管到哪」，读的人只能猜那 40 是不是就是 X 害的——而猜错的
/// 方向很贵：把它读成「这一格有缺陷要修」，就会去修一个本来就没打算跑的格子；反过来把
/// 「少配一段」读成「这一格烂了」，就会去查一个查不出东西的格子。
///
/// 所以每一条声明自己说是哪一类，由这一类决定它盖住哪些格子。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissingScope {
    /// 整次跑。**故意一个格子都不盖**：这一类的判词是「这次跑不构成一行表」，而格子照跑、
    /// 读数照有（诊断臂就是这一种）。盖住它会把「这一格本来跑得动」这件事一起抹掉，而那正是
    /// 拒收为表行时要保住的信息。
    Run,
    /// 每一个格子。上游骨干、裁判这种缺了没有一格出得来读数。
    EveryCell,
    /// 某一列（基准）：缺的是这个基准要的后端。
    Column(String),
    /// 某一行（外壳）：缺的是这个外壳要的东西。
    Row(String),
}

impl MissingScope {
    /// 报表里那一格。列名行名带上，因为它就是这条声明管到哪的全部信息。
    fn render(&self) -> String {
        match self {
            MissingScope::Run => "run".to_string(),
            MissingScope::EveryCell => "every-cell".to_string(),
            MissingScope::Column(name) => format!("column:{name}"),
            MissingScope::Row(name) => format!("row:{name}"),
        }
    }

    /// 这条声明盖不盖「这一列 × 这一行」的格子。
    fn covers(&self, benchmark: &str, scaffold: &str) -> bool {
        match self {
            MissingScope::Run => false,
            MissingScope::EveryCell => true,
            MissingScope::Column(name) => name == benchmark,
            MissingScope::Row(name) => name == scaffold,
        }
    }
}

/// 这条缺席是**哪一类**。四类的处置互不相同，所以缺了它这句「缺 X」还不够读：
/// 没实现要写代码，没接上要接线（可能只是少配一段），这条臂按定义不供要换臂或换读法，
/// 而读不出题既不是代码也不是配置的事。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Absence {
    /// 树里没有这个东西的实现，所以没有可接的东西——不是一个配置旋钮。
    NotImplemented,
    /// 实现了，这次跑没把它接进来（上游没配、传输建不出来、端点没指）。
    NotWired,
    /// 这条臂按定义就不提供它。不是缺陷，是这次实验的定义。
    OutOfArm,
    /// 缺的不是代码也不是配置，是读数本身：这个基准的题一份也没读出来。
    NoReading,
}

impl Absence {
    fn render(&self) -> String {
        match self {
            Absence::NotImplemented => "not-implemented".to_string(),
            Absence::NotWired => "not-wired".to_string(),
            Absence::OutOfArm => "out-of-arm".to_string(),
            Absence::NoReading => "no-reading".to_string(),
        }
    }
}

/// 一条缺席：谁缺、管到哪些格子、是哪一类缺、为什么。
///
/// 四样缺一不可，因为它们回答的是四个不同的问题：读的人要照着做的那一步（怎么补）、
/// 这一步有多大（影响多少格子）、这一步属于哪一类活（写代码还是改配置）、以及为什么会这样。
pub struct Missing {
    subject: String,
    scope: MissingScope,
    kind: Absence,
    why: String,
}

impl Missing {
    fn new(subject: &str, scope: MissingScope, kind: Absence, why: impl Into<String>) -> Self {
        Self {
            subject: subject.to_string(),
            scope,
            kind,
            why: why.into(),
        }
    }

    /// `## wiring` 那一行：五格，前四格是结构化的判据，最后一格是给人读的因。
    fn wiring_line(&self) -> String {
        format!(
            "missing\t{}\t{}\t{}\t{}",
            self.subject,
            self.scope.render(),
            self.kind.render(),
            self.why
        )
    }

    /// 人读的那一句话。与加 scope 之前**逐字节相同**：退出码那一路上念的就是这些句子，
    /// 而它今天已经是一份能 diff 的契约，不因为这里多记了两样东西就换一个写法。
    fn as_line(&self) -> String {
        format!("{}: {}", self.subject, self.why)
    }

    fn covers(&self, benchmark: &str, scaffold: &str) -> bool {
        self.scope.covers(benchmark, scaffold)
    }
}

/// 把每一格跑出来的 `errored` 按「这一格有没有被**提前声明过**的缺席盖住」分成两半。
///
/// 这不是换一个说法，是分开两件处置相反的事：盖住的那半本来就没轮到跑（去做那个后端、
/// 或者换一条臂），没盖住的那半才是这一列的读数要指的东西（上游挂了、环境建不出来，要修）。
/// 合成一个数时，一个把 1080 例「这一列没有后端」与 360 例「少配一段」加在一起的总数，
/// 读不出该去修哪一半——**总数不是人口**。
///
/// 报表与退出码都从这一个函数取这两个数，所以两处不可能对不上。
fn error_split(table: &Table, declared: &[Missing]) -> (usize, usize) {
    let mut errored = 0;
    let mut blocked = 0;
    for cell in &table.cells {
        let covered = declared
            .iter()
            .any(|m| m.covers(&cell.benchmark, &cell.scaffold));
        if covered {
            blocked += cell.errored();
        } else {
            errored += cell.errored();
        }
    }
    (errored, blocked)
}

/// 平台缺席的那一句原因。**两处渲染都念这一份**：`## wiring` 的 `missing` 行，以及格子里
/// 那句 `scaffold failed`（驱动那一侧拿它当兜底平台的报错）。两边各写一句，读的人就会照着
/// 其中一份去找错地方。
///
/// 两种情况要分开，与骨干、容器后端那两条同理：
/// - 传输建不出来 ⇒ 与骨干共用同一个客户端，缺的是客户端，不是端点；
/// - 传输在、端点没配 ⇒ 缺的是**这一次跑的环境里一个值**，点名那个变量，否则读的人会去翻
///   组合根——那里三处都在（读环境、建桥、兜底），翻不出东西来。
fn platform_absence(wiring: &Wiring) -> String {
    match &wiring.no_transport {
        Some(why) => format!("the platform endpoint cannot be reached: {why}"),
        None => format!("{PLATFORM_BASE_ENV} is unset in this run's environment"),
    }
}

/// 开跑**之前**就看得出来缺什么。收窄到这几样而不是整份参数：判词只跟「哪一列、哪个外壳、
/// 哪条臂」有关，跟数据根、闸门那些无关，所以它是一条纯函数，能被单测直接钉住。
///
/// 一条依赖缺席的原因要说得准：`backbone` 与平台都可能「没配」，也可能「配了、传输建不出来」。
/// 这两种情况留在同一句话里，读的人会去配一个本来已经配好的东西。
///
/// 每条还要说清**管到哪些格子**与**是哪一类**：这两样决定 `## per seed` 里那些 `errored`
/// 该算缺陷还是算没轮到跑，见 `MissingScope` 与 `Absence`。
fn missing_dependencies(
    tools: ToolArm,
    benchmarks: &[&str],
    scaffolds: &[String],
    wiring: &Wiring,
) -> Vec<Missing> {
    let mut missing = Vec::new();
    // 诊断臂不是缺席的依赖，但它同样意味着这次跑不是一次完整的实验：没有主表那一臂的
    // 工具面，格子就不能作表行。记进来，退出码于是不会把一次诊断跑当成一次成表跑。
    //
    // `Run` 而不是 `EveryCell`：这条说的是「这次跑不构成一行表」，格子本身照跑照有读数
    // （下面 `## per seed` 里那些数字就是诊断的全部用处），把它算成「这些格子被挡住了」
    // 会连「这一格本来跑得动」一起抹掉。
    if !tools.is_table_arm() {
        missing.push(Missing::new(
            "tools",
            MissingScope::Run,
            Absence::OutOfArm,
            "this run used the tools-off diagnostic arm, so it is not a main-table row",
        ));
    }
    if wiring.backbone.is_none() {
        missing.push(Missing::new(
            "backbone",
            MissingScope::EveryCell,
            Absence::NotWired,
            match &wiring.no_transport {
                Some(why) => why.clone(),
                None => "no LLM upstream is configured".to_string(),
            },
        ));
    }
    if wiring.judge.is_none() {
        missing.push(Missing::new(
            "judge",
            MissingScope::EveryCell,
            Absence::NotWired,
            "no generative judge upstream is configured",
        ));
    }
    // 这一条是 `NotWired` 了：树里有实现（`eval_container` 里那个 buildah 后端），缺的只可能
    // 是「这次没把它接进来」——宿主上没有容器机器、数据根里没有官方脚本、或者被开关关掉了。
    // 原因由接线那一步写下来，照着念；没有再写上「没接上」这个不含指引的兜底。
    if benchmarks.contains(&"swe-bench-pro") && wiring.swe_pro.is_none() {
        missing.push(Missing::new(
            "swe-bench-pro",
            MissingScope::Column("swe-bench-pro".to_string()),
            Absence::NotWired,
            wiring
                .swe_pro_reason
                .clone()
                .unwrap_or_else(|| "the container backend is not wired into this run".to_string()),
        ));
    }
    // 这一条仍然是 `NotImplemented`：compose 后端树里没有实现（只有测试替身），所以「接上」
    // 这一步今天无物可接。写成「没接上」会把读的人送去翻配置，翻不出东西。
    if benchmarks.contains(&"toolathlon") && wiring.toolathlon.is_none() {
        missing.push(Missing::new(
            "toolathlon",
            MissingScope::Column("toolathlon".to_string()),
            Absence::NotImplemented,
            "no compose backend is implemented in this build, so there is nothing to wire; this \
             host has no compose runtime and the pack image has never been built",
        ));
    }
    if scaffolds.iter().any(|name| name == "nql") && wiring.platform.is_none() {
        missing.push(Missing::new(
            "nql",
            MissingScope::Row("nql".to_string()),
            Absence::NotWired,
            platform_absence(wiring),
        ));
    }
    // 外部流派的声明写坏了：**整块不成立**，所以这次跑的行集里没有它们。`Run` 而不是
    // 某一行：一行都没有建出来，没有格子可盖——它说的是「这一次跑不是你以为的那次实验」，
    // 而这正是要有人去看的那件事（一个写错的声明与「今天不接外部流派」不能长得一样）。
    if let Some(why) = &wiring.foreign_reason {
        missing.push(Missing::new(
            "foreign-flows",
            MissingScope::Run,
            Absence::NotWired,
            why.clone(),
        ));
    }
    // 声明了、也选了，但托管它们的机器没接上。盖**整行**：这一行的每一列都要那台机器。
    // 理由照念容器后端那句——今天能托管外部程序的机器只有它那一台。
    for flow in &wiring.foreign_flows {
        if scaffolds.iter().any(|name| name == &flow.name) && wiring.swe_pro.is_none() {
            missing.push(Missing::new(
                &flow.name,
                MissingScope::Row(flow.name.clone()),
                Absence::NotWired,
                wiring.swe_pro_reason.clone().unwrap_or_else(|| {
                    "the container backend that would host this flow is not wired into this run"
                        .to_string()
                }),
            ));
        }
    }
    missing
}

pub async fn run_table(args: &EvalTableArgs, wiring: &Wiring, pins: &Pins) -> Result<Outcome> {
    let data_root = resolve_data_root(args)?;
    if !data_root.is_dir() {
        bail!("the data root {} is not a directory", data_root.display());
    }

    // 闸门开在跑之前，而且开不起来就是**失败**，不是「照旧跑」：这个闸门守的是这台宿主，
    // 它和集群同一台机器。命令行的意思是「按一个会动的界放行」，不是「随便跑，回头在报表
    // 里记一句没开成」——后者会在宿主已经被压住的时候才发现。
    let admission = match &args.admission_dir {
        Some(dir) => {
            let mut cfg = cog_core::admission::AdmissionConfig::new(dir);
            if let Some(floor) = args.admission_floor_unmeasured {
                cfg.floor_unmeasured = floor;
            }
            if let Some(ceiling) = args.admission_ceiling_unmeasured {
                cfg.ceiling_unmeasured = ceiling;
            }
            Some(cog_core::admission::open(&cfg).map_err(|e| anyhow::anyhow!("{e}"))?)
        }
        None => None,
    };

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

    // 每一处缺席都记名字：报表里缺什么、怎么补，比一个非零退出码有用。跑起来之后才发现的
    // 缺席（读不出题的列）在下面追加，两批各写各的。
    let mut missing = missing_dependencies(args.tools, &args.benchmarks, &args.scaffolds, wiring);

    let backbone: Arc<dyn LlmClient> = wiring
        .backbone
        .clone()
        .unwrap_or_else(|| Arc::new(NoBackbone::new("no llm_routing backend is configured")));
    let platform: Arc<dyn PlatformRunner> = wiring
        .platform
        .clone()
        .unwrap_or_else(|| Arc::new(NoPlatform::new(platform_absence(&wiring))));

    let checkpoint = run_checkpoint(args, &verified, subset.as_deref());
    let mut rig = Rig::new(
        backbone,
        RunConfig {
            data_root: data_root.clone(),
            seeds: args.seeds.clone(),
            max_concurrency: args.concurrency_slot,
            subset: subset.clone(),
            checkpoint: Some(checkpoint.clone()),
            admission: admission.clone(),
        },
    );
    for scaffold in table_scaffolds(platform) {
        if args.scaffolds.iter().any(|name| name == scaffold.name()) {
            rig = rig.with_scaffold(scaffold);
        }
    }
    // 外部流派那一维：谁、落在哪几列上、怎么起它、上限多少都由环境里的声明给定，所以这一侧
    // 不加任何一支具体流派的名字——接一支新的就是改一次声明。
    for flow in &wiring.foreign_flows {
        if args.scaffolds.iter().any(|name| name == &flow.name) {
            rig = rig.with_scaffold(Arc::new(ForeignFlow::new(
                flow.name.clone(),
                flow.entrypoint.clone(),
                flow.limits,
                flow.columns.clone(),
            )));
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

    let table = rig.run(&benchmarks).await?;

    // 读不出题的基准会留下一整列「题数为 0」的格子。那不是一个读数为零的格子，是这一列
    // 没有读数——两者在表里都写 0，必须在这里分开。运行器把原因带了出来（数据读不出来／
    // 子集点名的题不在这一版数据里），照它的原话记：这两条追查方向完全不同，合成一句
    // 「题数为 0」就等于把因说成了命。
    //
    // 这两条与上面那批同一次跑里才发现，所以管到哪要按**列**说：一列读不出来就是整列
    // 没有读数，不是某一格的事。
    for (benchmark, why) in &table.unreadable {
        missing.push(Missing::new(
            benchmark,
            MissingScope::Column(benchmark.clone()),
            Absence::NoReading,
            why.clone(),
        ));
    }
    for cell in &table.cells {
        let named = table
            .unreadable
            .iter()
            .any(|(benchmark, _)| benchmark == &cell.benchmark);
        if !named && cell.per_seed.iter().all(|s| s.total == 0) {
            let line = Missing::new(
                &cell.benchmark,
                MissingScope::Column(cell.benchmark.clone()),
                Absence::NoReading,
                "the case source read no cases (the column would read as a score of zero)",
            );
            // 同一列的每一行都会走到这里，所以按「同一件事」去重，而不是按那句话重去重。
            if !missing
                .iter()
                .any(|m| m.subject == line.subject && m.why == line.why)
            {
                missing.push(line);
            }
        }
    }

    // 报表与退出码读的都是这两个数，而且都从这个函数取：一处算、两处念，才不会出现
    // 「表上印着 0，退出码却说有题跑不动」这种自相矛盾。
    let (errored, blocked) = error_split(&table, &missing);

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
            checkpoint: &checkpoint,
            table: &table,
        },
    );
    Ok(Outcome {
        text,
        missing: missing.iter().map(Missing::as_line).collect(),
        errored,
        blocked,
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
    // 工具臂在这一处收口：`None` 是诊断臂。有工具臂（主表那一臂）要一整套工具实现，而组合根
    // 今天只接得上 swe-bench-pro 那一束（bash/file_edit 落在这道题自己的容器里）；HLE 点名的
    // web_search/python 与 toolathlon 点名的那些 MCP server 都还没有实现。
    //
    // 少给几个工具会让这一格因为「工具面不同」得分——那是在量别的东西，所以宁可不跑：一个
    // 基准只要**有一件**工具没有实现，就整个不收工具臂，而不是拿半束工具顶上去。
    let with_tools = tools == ToolArm::Platform;
    if with_tools && !has_tool_implementations(name) {
        bail!(
            "--tools platform (the default): `{name}` pins tools the composition root has no \
             implementation for, and running it with a smaller tool face would score a different \
             thing. Today only swe-bench-pro's face (bash/file_edit, bound to the instance's own \
             container) is implemented; pass `--tools none` explicitly if all you want is a \
             diagnostic run."
        );
    }
    Ok(match name {
        "hle" => hle_benchmark(judge, HleToolkit::without_tools()),
        "swe-bench-pro" => swe_pro_benchmark(
            wiring.swe_pro.clone(),
            if with_tools {
                SweProToolkit::with_factory(crate::eval_container::swe_pro_container_tools())
            } else {
                SweProToolkit::without_tools()
            },
        ),
        "toolathlon" => toolathlon_benchmark(
            wiring.toolathlon.clone(),
            ToolathlonToolkit::without_tools(),
        ),
        other => bail!("unknown benchmark `{other}`"),
    })
}

/// 这个基准点名的工具，组合根今天有没有实现。
///
/// 与 [`build_benchmark`] 分开，是为了让「哪一列收工具臂」是一条**读得出来的**判据，而不是
/// 埋在 match 分支里的一个 if：加一列的工具实现时，改的是这里一处。
fn has_tool_implementations(name: &str) -> bool {
    matches!(name, "swe-bench-pro")
}

/// 报表里 `## wiring` 那一段要的两样东西：说不出话的依赖（缺席），以及接上了的那个平台
/// 是谁。两者都是「这次跑的外部上游是什么」，所以一起递。
///
/// 缺席这条也喂给 `## per seed`：那一段要按它把 `errored` 分成「没轮到跑」与「跑不动」。
struct WiringReport<'a> {
    missing: &'a [Missing],
    platform_base: Option<&'a str>,
}

/// 要渲染的那次跑读了什么、跑出了什么。
///
/// 数据是哪一份（`data_root`、`pins`、`verified`、`subset`）与结果是哪一份（`table`）
/// 是同一件事的两半——「这张表说的是哪次跑」缺了任何一半都答不出来，所以一起递，而不是
/// 在参数表上排成一串。
///
/// `errored`／`blocked` 这两个数**不在这里递**：它们是由 `table` 与 `wiring.missing` 算出来的，
/// 递进来就等于给了第二个来源，两处可以对不上——而「报表说 0、退出码说有题跑不动」正是
/// 这次改动要消灭的那种自相矛盾。所以 `render` 自己调那个共用的 `error_split`。
struct Readings<'a> {
    data_root: &'a Path,
    pins: &'a Pins,
    verified: &'a BTreeMap<String, String>,
    subset: Option<&'a CaseSubset>,
    checkpoint: &'a Checkpoint,
    table: &'a Table,
}

fn render(args: &EvalTableArgs, wiring: WiringReport<'_>, readings: Readings<'_>) -> String {
    let Readings {
        data_root,
        pins,
        verified,
        subset,
        checkpoint,
        table,
    } = readings;
    let (errored, blocked) = error_split(table, wiring.missing);
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
    // 续跑的口径跟着表走：读的人看到 `## per seed` 里那一列 `resumed` 时，得同时知道
    // **记下来的是哪些例**。判据只有一条——判分器给出过判词。跑不动 / 上游拒了不进记录，
    // 续跑时**重跑**它们，而不是把它们当成「跑过且没过」记住；反过来说，记录里没有的例
    // 在格子里按没过计，它只是不该被记成「已经有结论」。
    out.push_str(&format!(
        "checkpoint\t{}\trun-id={}\n",
        checkpoint.path.display(),
        &checkpoint.run_id[..12]
    ));
    out.push_str(
        "checkpoint-rule\tonly judged cases are recorded; a case that got no verdict (env or \
         upstream failure) is run again on resume rather than remembered as failed, and a case \
         with no record counts as not resolved in its cell\n",
    );
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

    // 这份报告是**哪一段代码**跑出来的：验收要求「跑前登记的 rev 与报告内的记录一致」，
    // 而在此之前这句话只有**操作者给 `--out` 起的文件名**里那个短 sha 撑着——那是手写
    // 约定，不是产物自带的。少了这一行，两 rev 的对照表读起来就是「两份同名格式的报告」，
    // 谁也没法从报告本身说出它跑的是哪一版；`run-id` 里其实已经把这个值卷进了摘要
    // （`run_checkpoint` 的 `software` 那一行），但卷进去与读得出来是两件事。
    //
    // 两个戳都印：`COGNEVA_VERSION_ID` 是检出标签（与 run-id 用的同一个），
    // `COGNEVA_GIT_REVISION` 是那笔提交本身。判据是「哪一版」时按后者比——标签是派生的，
    // 同一条提交在两次派生里可能得到不同的距离数。
    out.push_str(&format!(
        "software\t{} (rev {})\n",
        env!("COGNEVA_VERSION_ID"),
        env!("COGNEVA_GIT_REVISION")
    ));

    out.push_str("\n## wiring\n");
    // 平台接上了就要说出接在哪：NQL 那一行的读数长在哪个平台上，跟骨干的模型名一样
    // 是这张表能被复跑的前提，不能只剩「接上了」三个字。
    if let Some(base) = wiring.platform_base {
        out.push_str(&format!("platform\t{base}\n"));
    }
    if wiring.missing.is_empty() {
        out.push_str("complete\n");
    } else {
        // 每一行五格：谁缺、管到哪些格子、哪一类、为什么。后两格不是修饰——它们决定
        // 下面那些格子里的 `errored` 该读成缺陷还是读成没轮到跑，也决定照着做的那一步
        // 是写代码还是改配置。
        for line in wiring.missing {
            out.push_str(&line.wiring_line());
            out.push('\n');
        }
    }

    out.push_str("\n## gate\n");
    // 一次跑放行几个，由三个数一起定：命令行给的静态名额、闸门当下的界、以及每一列自己的
    // 隔离上界。三个里至少有一个是命令行给的（解析时就卡住了），但生效值只有一个——只报
    // 生效值的话，读的人没法判断这次跑是被谁挡住的，也没法判断换一个就能不能多放。
    out.push_str(&format!(
        "concurrency-slot\t{}\n",
        match args.concurrency_slot {
            Some(n) => n.to_string(),
            None => "none: this run is bounded by the gate and each column's own ceiling only"
                .to_string(),
        }
    ));
    out.push_str(
        "concurrency-rule\teffective = min(column ceiling, gate bound, static slot): the three \
         are not alternatives and the smallest one wins\n",
    );
    for (name, ceiling) in &table.column_ceilings {
        out.push_str(&format!(
            "concurrency-column\t{name}\t{}\n",
            match ceiling {
                Some(n) => format!("ceiling={n}"),
                None => "ceiling=none: the cases in this column do not share state, so the \
                         column itself adds no ceiling"
                    .to_string(),
            }
        ));
    }
    match &table.gate {
        Some(gate) => {
            // 目录身份跟着界一起出：两台宿主、两个目录，报出来的是两个界，而它们各自都
            // 看起来「生效了」。`dev:ino` 是这两份读数能不能并排读的判据。
            out.push_str(&format!("gate-dir\t{}\t{}\n", gate.dir, gate.identity));
            out.push_str(&format!(
                "gate\tbound={}\tfloor={}\tceiling={}\taction={}\tcontroller={}\tin-flight={}\tblocked={}\n",
                gate.bound,
                gate.floor,
                gate.ceiling,
                gate.action,
                if gate.controller { "yes" } else { "no" },
                gate.in_flight,
                gate.blocked
            ));
            // 界的来历：两个界限是拍的，压力是量的。分开写，免得读的人以为上界也是量出来的。
            out.push_str(&format!(
                "gate-bounds\tfloor={}\tceiling={}\tunmeasured: both come from \
                 --admission-floor-unmeasured/--admission-ceiling-unmeasured and are a form floor \
                 and a placeholder, not a reading on this host\n",
                gate.floor, gate.ceiling
            ));
            match gate.pressure {
                Some(p) => out.push_str(&format!(
                    "gate-pressure\tcpu={:.2}/{:.2}\tio={:.2}/{:.2}\tmemory={:.2}/{:.2}\t\
                     avg10/avg60\tMemAvailable={:.1}GiB\tbuild-slots-held={}\n",
                    p.cpu_avg10,
                    p.cpu_avg60,
                    p.io_avg10,
                    p.io_avg60,
                    p.mem_avg10,
                    p.mem_avg60,
                    p.mem_available_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                    p.builds_in_flight
                )),
                // 「读不出压力」与「压力是零」是两件事：前者让界不动，后者让界往上走。
                None => out.push_str(
                    "gate-pressure\tunreadable: no /proc pressure or memory reading on this host, \
                     so the bound does not move at all\n",
                ),
            }
            match &gate.red {
                Some(why) => out.push_str(&format!("gate-red\t{why}\n")),
                None => out.push_str("gate-red\tnone of the four lights is on\n"),
            }
            // 界贴着下界不是结论，是一个状态。它停下来多久是「这件事持续了多久」，而原因在
            // 上面那两行里——写成「界一直很低」就等于把因说成了命。
            if gate.windows_at_floor > 0 {
                out.push_str(&format!(
                    "gate-at-floor\t{} window(s) in a row at the floor; the pressure and the red \
                     line above are what held it there\n",
                    gate.windows_at_floor
                ));
            }
        }
        None => out.push_str(
            "gate\tnot in force: no --admission directory was given, so the bound on this run is \
             the static slot and each column's own ceiling, and nothing looked at the host\n",
        ),
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
            // 这一格被哪些**提前声明过**的缺席盖住，把名字点出来。名字是可点回去的：
            // 从 `blocked-by=` 里的每一个名字都能回到 `## wiring` 那一行，看是哪一类、怎么补。
            let covering: Vec<&str> = wiring
                .missing
                .iter()
                .filter(|m| m.covers(col, row))
                .map(|m| m.subject.as_str())
                .collect();
            let blocked_by = if covering.is_empty() {
                "-".to_string()
            } else {
                covering.join(",")
            };
            for seed in &cell.per_seed {
                // `errored` 与 `blocked` 互斥，和两格都由「这一格有没有被声明盖住」定：
                // 盖住的那些题**不是跑错了**，是这一格本来就没轮到跑（后端没实现、平台没接），
                // 所以它们进 `blocked`；没盖住的才是 `errored`——那些题本来该跑得动。
                // 两个数写在一行上、且总是相加等于原先那一个：并排放着才读得出「这一格里
                // 有多少是缺陷、多少是没轮到」，而不会把两者当成同一个数。
                let (errored, blocked) = if covering.is_empty() {
                    (seed.errored, 0)
                } else {
                    (0, seed.errored)
                };
                // `resumed` 单列出来：接回来的那些题用的是**当时**那份外壳与那次上游，
                // 与这一轮跑出来的不是同一个条件。归零（没有检查点或全新一次跑）就是
                // 这一格全是这一轮的读数。
                out.push_str(&format!(
                    "cell\t{col}\t{row}\tseed={}\tresolved={}\ttotal={}\terrored={}\tblocked={}\tblocked-by={}\tresumed={}\ttokens={}\tsteps={}\n",
                    seed.seed,
                    seed.resolved,
                    seed.total,
                    errored,
                    blocked,
                    blocked_by,
                    seed.resumed,
                    seed.tokens,
                    seed.steps
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

    // 两个数一起出。少一个，读的人就只能把 `blocked` 混进 `errored` 读——那正是这一版要
    // 拆开的东西。
    out.push_str(&format!("\nerrored\t{errored}\tblocked\t{blocked}\n"));
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

/// 这次跑要用的传输：平台与骨干**共用同一个**客户端，所以这条组合根上只建这一次。
///
/// 各建一个的代价不是多一个连接池，是配好的超时与代理只对其中一条上游生效，而两边的
/// 读数看起来一模一样。
///
/// 建不出来时交回原因而不是拿默认客户端顶上：配好的 `http_client` 段就是调用方要的
/// 那一份，换一个没超时没代理的顶上，读起来与配过的一模一样。
fn http_client() -> Result<Arc<dyn HttpClient>, String> {
    let config = cog_net::HttpClientConfig::load().unwrap_or_else(|e| {
        tracing::warn!("http client config is unreadable ({e}); the configured defaults are used");
        Default::default()
    });
    match cog_net::factory::ReqwestHttpClient::from_config(&config) {
        Ok(client) => Ok(Arc::new(client)),
        Err(e) => Err(format!(
            "cannot build an http client from http_client ({e})"
        )),
    }
}

/// 把一份端点配置变成平台端口，并把它的身份一并交出。端点从环境读还是从别处读与这里无关，
/// 所以这段能被单测钉住，而不用去动进程的环境变量。
///
/// 传输由调用方给——它就是 `http_client()` 建出来的那一个。所以这里不再有「建不出来」这一支：
/// 那一支已经在上游决定了，它同时决定骨干也缺席。
fn platform_from_settings(
    settings: BridgeSettings,
    http: Arc<dyn HttpClient>,
) -> (PlatformApiRunner, String) {
    let base = settings.base.clone();
    (PlatformApiRunner::new(http, settings), base)
}

/// 组装这一次跑的依赖：骨干与裁判来自 `llm_routing`，缺了就缺席，不猜。
///
/// pin 里写的模型名如果没有任何已启用的上游在供，不算「有了这个依赖」——那只是一张贴纸。
/// 这里把它记成缺席而不是直接失败：表仍然出得来，读的人从 wiring 段看见真相。
async fn wiring_from_config(args: &EvalTableArgs) -> Wiring {
    let mut wiring = Wiring::default();
    // 外部流派的声明与下面的骨干、容器后端都无关，所以先读它：一块写坏的声明读不出来时
    // 也要留下那句原因，而不是被后面某一条早退盖掉。
    match crate::eval_container::foreign_flows() {
        Ok(flows) => wiring.foreign_flows = flows,
        Err(why) => wiring.foreign_reason = Some(why),
    }
    // 传输先建：平台与骨干都要它，而且它建不出来时**两条上游一起缺席**。
    //
    // 这里不拿默认客户端顶上，因为骨干的每一家上游都是「没有客户端就报 no HttpClient
    // configured」——那种 provider 建得出来、一次调用也发不出去，把它记成「在」，报表就
    // 读成一次完整的实验，而它连一句答复都没收到过。所以传输这一层失败时骨干是缺席的，
    // 且 `missing` 那一行要说的是传输，不是「没有上游」。
    let http = match http_client() {
        Ok(http) => Some(http),
        Err(why) => {
            tracing::warn!(
                "no http client could be built ({why}); the platform bridge and the backbone \
                 are absent rather than defaulted"
            );
            wiring.no_transport = Some(why);
            None
        }
    };
    // 平台与骨干是两条独立的上游：`llm_routing` 读不出来不该顺手把平台也丢掉，所以这一段
    // 先于任何骨干的早退。端点没配、或传输没建出来，平台都交回 `None` 而不是一个会立刻
    // 失败的实现——驱动那边留着 `NoPlatform`，NQL 那一行于是报「跑不起来」，而不是拿一个
    // 假答复去计分。
    if let (Some(settings), Some(http)) = (BridgeSettings::from_env(), http.clone()) {
        let (runner, base) = platform_from_settings(settings, http);
        wiring.platform = Some(Arc::new(runner));
        wiring.platform_base = Some(base);
    }
    // 容器后端与上游是**两条互不相干**的依赖：诊断臂（关掉工具的那一次跑）一样要起容器、
    // 一样要跑判分，所以这一段必须排在下面任何一条骨干的早退**之前**——排到后面去，一次没有
    // 上游的跑就会连「这一列的容器接没接上」都读不出来。
    match resolve_data_root(args) {
        Ok(root) => match crate::eval_container::swe_pro_container_backend(&root).await {
            Ok(backend) => wiring.swe_pro = Some(backend),
            Err(why) => wiring.swe_pro_reason = Some(why),
        },
        Err(e) => {
            wiring.swe_pro_reason = Some(format!(
                "the data root could not be resolved, so this instance's run scripts cannot be \
                 located: {e}"
            ))
        }
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
    let http = match http {
        Some(http) => http,
        // 传输这一条已经记在 `no_transport` 里，`missing` 那一行照着它写。这里不再往下走：
        // 下面每一步都是为了建一个能发请求的骨干，而它已经建不出来了。
        None => return wiring,
    };
    match cog_llm::plugin::build_llm_provider(capacity, max_tokens, &routing, Some(http)) {
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

/// 这一次跑的数据根在哪。
///
/// 接线那一步也要它（每题的官方判分脚本按这个根去磁盘上找），所以只留这一处口径：
/// 命令行给了就用命令行那个，没给才回落到环境里那个。两处各写一遍，会出现「表读的是这儿、
/// 脚本找的是那儿」这种只有跑完才发现的错位。
fn resolve_data_root(args: &EvalTableArgs) -> Result<std::path::PathBuf> {
    match &args.data_root {
        Some(root) => Ok(root.clone()),
        None => Ok(RunConfig::from_env()?.data_root),
    }
}

/// 子命令主入口。
pub async fn run_from_args() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().collect();
    // 声明里的名字要先读出来：`--scaffolds` 的合法取值是「这次实验的行集」，而外部流派是
    // 环境声明进来的。读不出来时这里是空的——那时命令行上点它们会报「不认识的 scaffold」，
    // 而报表里另有一行说着声明为什么读不出来。
    let declared: Vec<String> = crate::eval_container::foreign_flows()
        .map(|flows| flows.into_iter().map(|flow| flow.name).collect())
        .unwrap_or_default();
    let args = match EvalTableArgs::parse_with(&operands(&argv), &declared) {
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
    // 逐格读数要能在夹具里摆出来，才有东西可测 `## per seed` 那一段。
    use cog_eval::{Cell, SeedScore};

    fn args(raw: &[&str]) -> Result<EvalTableArgs, String> {
        EvalTableArgs::parse(&raw.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    /// 检查点只是报表里的一行身份，这几条渲染的用例不关心它在哪。
    fn checkpoint_at(out: &str) -> Checkpoint {
        Checkpoint {
            path: PathBuf::from(format!("{out}.cases.jsonl")),
            run_id: "0123456789abcdef".into(),
        }
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
            // 两个界至少要给一个：这几条用例不带闸门，所以给静态名额。
            "--concurrency-slot",
            "2",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    /// 外部流派那几行的夹具：一支落在 swe-bench-pro 上的外部流派。
    fn one_flow(name: &str) -> crate::eval_container::ForeignFlowSpec {
        crate::eval_container::ForeignFlowSpec {
            name: name.to_string(),
            columns: vec!["swe-bench-pro".to_string()],
            entrypoint: "sweagent solve".to_string(),
            limits: cog_eval::ExecLimits {
                wall_clock: std::time::Duration::from_secs(900),
                cpus: 2.0,
                memory_bytes: 4 * 1024 * 1024 * 1024,
            },
        }
    }

    #[test]
    fn a_declared_flow_is_a_scaffold_and_a_name_nobody_declared_is_not() {
        let mut raw = minimal();
        raw.extend(
            ["--scaffolds", "swe-agent,nql"]
                .iter()
                .map(|s| s.to_string()),
        );
        let declared = vec!["swe-agent".to_string()];
        let parsed = EvalTableArgs::parse_with(&raw, &declared).unwrap();
        // 声明的行排在仓里那四行之后：行序由行集定，不由命令行的先后定。
        assert_eq!(parsed.scaffolds, vec!["nql", "swe-agent"]);

        // 没人声明过的名字仍然不是 scaffold，而且报错要把**认识的那几个**列出来，
        // 否则读的人不知道是自己拼错了还是这次没声明。
        let err = EvalTableArgs::parse_with(&raw, &[]).unwrap_err();
        assert!(err.contains("swe-agent"), "{err}");
        assert!(err.contains("codeact"), "{err}");
    }

    #[test]
    fn a_declared_flow_with_no_machine_behind_it_is_blocked_not_errored() {
        let wiring = Wiring {
            foreign_flows: vec![one_flow("swe-agent")],
            swe_pro_reason: Some("no container machine is usable on this host".to_string()),
            ..Default::default()
        };
        let missing = missing_dependencies(
            ToolArm::Platform,
            &["swe-bench-pro"],
            &["swe-agent".to_string()],
            &wiring,
        );
        let flow = missing
            .iter()
            .find(|m| m.subject == "swe-agent")
            .expect("this row has no machine, and that is a declaration, not a defect");
        assert_eq!(flow.scope, MissingScope::Row("swe-agent".to_string()));
        assert_eq!(flow.kind, Absence::NotWired);
        // 理由照念容器后端那句：读的人据此知道该去装机器，而不是去改这一行。
        assert!(flow.why.contains("no container machine"), "{}", flow.why);
    }

    #[test]
    fn a_broken_flow_declaration_rejects_the_whole_run_as_an_experiment() {
        let wiring = Wiring {
            foreign_reason: Some("`COGNEVA_EVAL_FOREIGN_FLOWS` has a line that is not ...".into()),
            ..Default::default()
        };
        let missing = missing_dependencies(ToolArm::Platform, &["hle"], &[], &wiring);
        let declaration = missing
            .iter()
            .find(|m| m.subject == "foreign-flows")
            .expect("a row set that is not what was asked for is not a complete experiment");
        // `Run` 而不是某一行：一块写坏的声明一行都没建出来，没有格子可盖。
        assert_eq!(declaration.scope, MissingScope::Run);
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

    /// 检查点的身份就是这张表的身份：换了外壳集合、换了骨干版本，就是另一次实验，
    /// 不能在同一份 `--out` 上接着上一次的半成品跑——那样交回来的读数一半是旧条件的。
    #[test]
    fn the_checkpoint_identity_rides_the_pins_of_the_run() {
        let verified: BTreeMap<String, String> = BTreeMap::new();
        let base = EvalTableArgs::parse(&minimal()).unwrap();
        let one = run_checkpoint(&base, &verified, None);

        let mut other = EvalTableArgs::parse(&minimal()).unwrap();
        other.backbone.version = "2".into();
        assert_ne!(
            one.run_id,
            run_checkpoint(&other, &verified, None).run_id,
            "换了骨干版本还是同一个身份"
        );

        let mut other = EvalTableArgs::parse(&minimal()).unwrap();
        other.scaffolds = vec!["nql".to_string()];
        assert_ne!(
            one.run_id,
            run_checkpoint(&other, &verified, None).run_id,
            "换了外壳集合还是同一个身份"
        );

        // 同一套参数重复算，身份相同：它要认得出自己上一次留下的半成品。
        assert_eq!(one.run_id, run_checkpoint(&base, &verified, None).run_id);

        // 没有 `--out` 时文件名带身份前截，两套参数在同一目录里跑不会互相顶掉。
        assert!(
            one.path.to_string_lossy().contains(&one.run_id[..12]),
            "{}",
            one.path.display()
        );
        assert_ne!(one.path, run_checkpoint(&other, &verified, None).path);

        // 给了 `--out`：检查点跟着报表走，一次跑的两份产物放在一起。
        let mut with_out = EvalTableArgs::parse(&minimal()).unwrap();
        with_out.out = Some(PathBuf::from("/tmp/table.md"));
        assert_eq!(
            run_checkpoint(&with_out, &verified, None).path,
            PathBuf::from("/tmp/table.md.cases.jsonl")
        );
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
        assert!(args(&[
            "--backbone",
            "m@1",
            "--judge",
            "j@1",
            // 放行几个也必给，这里一并给上，好让报出来的错就是缺 pins 那一条。
            "--concurrency-slot",
            "2",
        ])
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
            gate: None,
            // 这一列自己的隔离上界：hle 各例不共享状态，所以它不加界。
            column_ceilings: vec![("hle".into(), None)],
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &one_cell_table(),
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

    /// 检查点记的是**结论**不是「跑过」：这条判据要印在表里。少了它，「没跑过的例」
    /// 会被下一个人读成「跑过且没过」——而续跑正是照这条判据决定要不要重跑。
    #[test]
    fn the_report_states_what_the_checkpoint_remembers() {
        let args = EvalTableArgs::parse(&minimal()).unwrap();
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &one_cell_table(),
            },
        );
        assert!(
            text.contains("checkpoint\t/tmp/table.md.cases.jsonl\trun-id=0123456789ab"),
            "{text}"
        );
        assert!(text.contains("checkpoint-rule\t"), "{text}");
        assert!(
            text.contains("run again on resume rather than remembered as failed"),
            "{text}"
        );
    }

    /// 报告要说出**自己是哪一段代码**跑出来的。「跑前登记的 rev 与报告内的记录一致」这半条
    /// 验收，在此之前只有操作者给 `--out` 起的文件名撑着——那是手写约定，不是产物自带的身份。
    ///
    /// 两个戳都印、且印在 pins 段里：标签与提交分开读（登记比的是提交），位置在「这次跑用了
    /// 什么」那一段，读的人才不用去别处找。
    #[test]
    fn the_report_names_the_code_that_produced_it() {
        let args = EvalTableArgs::parse(&minimal()).unwrap();
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &one_cell_table(),
            },
        );
        let line = text
            .lines()
            .find(|line| line.starts_with("software\t"))
            .expect("a report that does not name its code cannot be paired with a registration");
        assert!(line.contains(env!("COGNEVA_VERSION_ID")), "{line}");
        assert!(line.contains(env!("COGNEVA_GIT_REVISION")), "{line}");
        assert!(line.contains("(rev "), "{line}");
        let pins = text
            .split("\n## wiring")
            .next()
            .expect("the report has a pins section");
        assert!(
            pins.contains("software\t"),
            "the code stamp belongs in the pins section, where what-ran is read: {pins}"
        );
    }

    fn one_cell_table_with_gate(gate: cog_core::admission::GateReading) -> Table {
        let mut table = one_cell_table();
        table.gate = Some(gate);
        table
    }

    fn gate_reading() -> cog_core::admission::GateReading {
        cog_core::admission::GateReading {
            bound: 2,
            floor: 1,
            ceiling: 4,
            action: "decreased".into(),
            pressure: Some(cog_core::admission::Pressure {
                cpu_avg10: 0.61,
                cpu_avg60: 2.10,
                io_avg10: 24.12,
                io_avg60: 29.60,
                mem_avg10: 0.99,
                mem_avg60: 1.30,
                mem_available_bytes: 41 * 1024 * 1024 * 1024,
                builds_in_flight: 1,
            }),
            red: Some("1 build slot(s) are held on this host".into()),
            blocked: 17,
            windows_at_floor: 3,
            in_flight: 1,
            controller: true,
            dir: "/opt/cogneva/sandbox/build-gate".into(),
            identity: "64770:1234".into(),
        }
    }

    #[test]
    fn without_a_directory_the_gate_is_off_and_the_report_says_so() {
        // 「没开闸门」与「开着闸门但一直没减」在表里必须分得开：读的人要能知道这些格子的
        // 并行度是固定名额给的，还是当时那台机器给的。
        let args = EvalTableArgs::parse(&minimal()).unwrap();
        assert!(args.admission_dir.is_none());
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &one_cell_table(),
            },
        );
        assert!(text.contains("## gate\n"), "{text}");
        assert!(text.contains("gate\tnot in force"), "{text}");
        assert!(!text.contains("gate-dir"), "{text}");
    }

    /// 一次跑放行几个，是用三个数取小来的；报告只印生效值的话，读的人看不出是被谁挡住的
    /// 那一次，也就没法判断换一个参数能不能多放。
    #[test]
    fn the_gate_section_names_the_slot_the_min_rule_and_every_columns_ceiling() {
        let args = EvalTableArgs::parse(&minimal()).unwrap();
        let verified = BTreeMap::new();
        let mut table = one_cell_table();
        // 一列自带隔离上界（各例共享一个 postgres），一列没有。
        table.column_ceilings = vec![("hle".into(), None), ("toolathlon".into(), Some(1))];
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &table,
            },
        );
        assert!(text.contains("concurrency-slot\t2\n"), "{text}");
        assert!(
            text.contains(
                "concurrency-rule\teffective = min(column ceiling, gate bound, static slot)"
            ),
            "取小这条规则要写在读数旁边，否则三个数只能读出「界是多少」：{text}"
        );
        assert!(
            text.contains("concurrency-column\thle\tceiling=none"),
            "{text}"
        );
        assert!(
            text.contains("concurrency-column\ttoolathlon\tceiling=1\n"),
            "列自己的上界要跟着列出，它不是命令行给的：{text}"
        );
    }

    /// 两个界一个都不给等于没有界。跑起来才发现的话，环境已经在起了——所以卡在解析这一步。
    #[test]
    fn a_run_that_gives_neither_a_slot_nor_a_gate_has_no_bound_and_is_refused() {
        let mut raw = minimal();
        let at = raw
            .windows(2)
            .position(|w| w[0] == "--concurrency-slot")
            .expect("minimal() supplies the slot");
        raw.drain(at..at + 2);
        let err = EvalTableArgs::parse(&raw).unwrap_err();
        assert!(err.contains("--concurrency-slot or --admission"), "{err}");
        assert!(err.contains("no bound at all"), "{err}");

        // 只给闸门就够了：要的是「至少一个」，不是「两个都要」。
        raw.extend(["--admission", "/tmp/gate"].iter().map(|s| s.to_string()));
        let args = EvalTableArgs::parse(&raw).expect("a gate alone is a bound");
        assert_eq!(args.concurrency_slot, None);
        assert!(args.admission_dir.is_some());
    }

    #[test]
    fn a_bound_on_a_gate_that_is_not_in_force_is_an_error() {
        // 命令行上写着上界 3 而实际上以固定名额跑完——这一次跑的读数就与它自己声明的条件
        // 对不上，而表上看不出来。
        let mut raw = minimal();
        raw.extend(
            ["--admission-ceiling-unmeasured", "3"]
                .iter()
                .map(|s| s.to_string()),
        );
        let err = EvalTableArgs::parse(&raw).unwrap_err();
        assert!(err.contains("--admission <dir>"), "{err}");
    }

    #[test]
    fn the_two_bounds_have_to_be_ordered_and_neither_may_be_zero() {
        let dir = ["--admission", "/tmp/gate"];
        let mut raw = minimal();
        raw.extend(dir.iter().map(|s| s.to_string()));
        raw.extend(
            [
                "--admission-floor-unmeasured",
                "4",
                "--admission-ceiling-unmeasured",
                "2",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        assert!(EvalTableArgs::parse(&raw).unwrap_err().contains("above"));

        let mut raw = minimal();
        raw.extend(dir.iter().map(|s| s.to_string()));
        raw.extend(
            ["--admission-ceiling-unmeasured", "0"]
                .iter()
                .map(|s| s.to_string()),
        );
        let err = EvalTableArgs::parse(&raw).unwrap_err();
        assert!(err.contains("not a bound"), "{err}");
    }

    /// 闸门那一段要能单独回答四件事：界是多少、上一刻的压力读数、界为什么动、以及挡住了
    /// 多少例。少任何一件，读的人就得去别处猜——这正是「界低」被读成「宿主闲」的原因。
    #[test]
    fn the_gate_section_prints_the_bound_its_pressure_its_cause_and_what_it_held_back() {
        let mut raw = minimal();
        raw.extend(
            ["--admission", "/opt/cogneva/sandbox/build-gate"]
                .iter()
                .map(|s| s.to_string()),
        );
        let args = EvalTableArgs::parse(&raw).unwrap();
        assert_eq!(
            args.admission_dir.as_deref(),
            Some(Path::new("/opt/cogneva/sandbox/build-gate"))
        );
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &one_cell_table_with_gate(gate_reading()),
            },
        );
        assert!(
            text.contains("gate-dir\t/opt/cogneva/sandbox/build-gate\t64770:1234"),
            "目录身份是两份读数能不能并排读的判据：{text}"
        );
        assert!(
            text.contains("bound=2\tfloor=1\tceiling=4\taction=decreased\tcontroller=yes\tin-flight=1\tblocked=17"),
            "{text}"
        );
        assert!(text.contains("io=24.12/29.60"), "{text}");
        assert!(text.contains("build-slots-held=1"), "{text}");
        assert!(
            text.contains("gate-red\t1 build slot(s) are held"),
            "{text}"
        );
        assert!(text.contains("gate-at-floor\t3 window(s)"), "{text}");
        assert!(
            text.contains("unmeasured: both come from"),
            "拍的界限必须与量出来的压力分开写：{text}"
        );
    }

    #[test]
    fn a_gate_that_could_not_read_the_host_says_so_instead_of_showing_calm() {
        let mut reading = gate_reading();
        reading.pressure = None;
        reading.action = "none".into();
        reading.red = None;
        reading.windows_at_floor = 0;
        let args = EvalTableArgs::parse(&minimal()).unwrap();
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &one_cell_table_with_gate(reading),
            },
        );
        assert!(text.contains("gate-pressure\tunreadable"), "{text}");
        assert!(text.contains("gate-red\tnone of the four lights"), "{text}");
        assert!(!text.contains("gate-at-floor"), "{text}");
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
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &one_cell_table(),
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
        let http = http_client().expect("the default http client config is a buildable one");
        let (runner, base) = platform_from_settings(settings, http);
        // 报出去的身份与真正用上的端点是同一个：两处走散的端点比不报还坏。
        assert_eq!(base, "http://platform.test");
        assert_eq!(runner.base(), "http://platform.test");

        assert!(
            BridgeSettings::from_parts(None, Some("t"), None).is_none(),
            "没有端点就不算接上了平台"
        );
    }

    /// 一条骨干**建得出来但发不出请求**时，`missing` 那一行说的必须是这件事。
    ///
    /// 每一家上游都是「没有 http 客户端就报 `no HttpClient configured`」，所以不带传输的
    /// provider 记成「在」，报表就读成一次完整的实验，而它一次调用都没发出去过。这一条
    /// 同时钉住反方向：因传输而缺席**不许**写成「没有上游配置」——那句话把读的人送去配
    /// 一个已经配好的东西。
    #[test]
    fn a_backbone_with_no_transport_is_missing_for_that_reason() {
        let wiring = Wiring {
            no_transport: Some("cannot build an http client from http_client (boom)".into()),
            ..Default::default()
        };
        let missing =
            missing_dependencies(ToolArm::Platform, &["hle"], &["nql".to_string()], &wiring);
        let backbone = missing
            .iter()
            .find(|m| m.subject == "backbone")
            .expect("a backbone that cannot send a request is not present")
            .as_line();
        assert!(backbone.contains("http_client"), "{backbone}");
        assert!(
            !backbone.contains("no LLM upstream is configured"),
            "absent for want of transport is not absent for want of an upstream: {backbone}"
        );
        // 平台这一侧同理：它跟骨干共用同一个客户端，所以传输没了它也没了。
        let nql = missing
            .iter()
            .find(|m| m.subject == "nql")
            .expect("the platform column cannot run without the shared transport")
            .as_line();
        assert!(nql.contains("http_client"), "{nql}");
    }

    /// 端点没配时，两处说这件事的判词必须点名**同一个变量**，而且必须是那个变量。
    ///
    /// 这一格钉的是 2026-10-16 那一跑读出来的形状：1440 例里 120 例（hle × nql）的失败判词
    /// 是 `no platform endpoint is wired: the composition root wires no platform`。那句话把读的
    /// 人送到组合根去找一段没写的代码，而组合根三处都在（读环境、建桥、兜底）——缺的是**这一次
    /// 跑的环境里一个值**：实测那次跑的 Pod（`eval-table-3b100eeb`）`env: []`、全树设这个变量的
    /// 地方一处也没有（只有它的常量、命令行帮助、README 三处**读**）。
    ///
    /// 反向也要钉：因传输而缺席时**不许**念这句话——那句话把读的人送去配一个已经配好的东西。
    #[test]
    fn an_unconfigured_platform_names_the_variable_both_faces_read() {
        let wiring = Wiring::default();
        let missing =
            missing_dependencies(ToolArm::Platform, &["hle"], &["nql".to_string()], &wiring);
        let nql = missing
            .iter()
            .find(|m| m.subject == "nql")
            .expect("没有平台时 nql 这一行是缺席的")
            .as_line();
        assert!(nql.contains(PLATFORM_BASE_ENV), "{nql}");
        // 驱动那一侧拿同一句当兜底平台的报错，两处必须同源。
        assert!(
            platform_absence(&wiring).contains(PLATFORM_BASE_ENV),
            "两个渲染面点名的不是同一个量"
        );
        // 反向：传输没建出来时缺的是客户端。
        let no_transport = Wiring {
            no_transport: Some("cannot build an http client from http_client (boom)".into()),
            ..Default::default()
        };
        assert!(
            !platform_absence(&no_transport).contains(PLATFORM_BASE_ENV),
            "传输缺失不是端点没配"
        );
    }

    /// 一张有真读数的表：两行（外壳）× 两列（基准），每格一个种子、每题 40 个例、
    /// 四格全都跑不动。
    ///
    /// 夹具必须带 `per_seed` 才走得到 `## per seed` 那一段。原来那些渲染用例的表都是空的
    /// （`cells: vec![]`），所以那一段**一条都没被测过**——这也正是这次那条缺陷能藏住的原因。
    fn table_with_cells() -> Table {
        let cell = |benchmark: &str, scaffold: &str| Cell {
            benchmark: benchmark.to_string(),
            metric: "accuracy".to_string(),
            scaffold: scaffold.to_string(),
            per_seed: vec![SeedScore {
                seed: 0,
                resolved: 0,
                total: 40,
                errored: 40,
                failures: vec![],
                resumed: 0,
                tokens: 0,
                steps: 0,
                duration_ms: 0,
            }],
        };
        Table {
            rows: vec!["gepa".into(), "nql".into()],
            cols: vec!["hle".into(), "swe-bench-pro".into()],
            cells: vec![
                cell("hle", "gepa"),
                cell("hle", "nql"),
                cell("swe-bench-pro", "gepa"),
                cell("swe-bench-pro", "nql"),
            ],
            wanted_seeds: 1,
            unreadable: vec![],
            gate: None,
            column_ceilings: vec![("hle".into(), None), ("swe-bench-pro".into(), None)],
        }
    }

    /// 把一张表和一批声明渲出来，只看两段：`## wiring` 与 `## per seed`。
    fn render_with(table: &Table, declared: &[Missing]) -> String {
        let args = EvalTableArgs::parse(&minimal()).unwrap();
        let verified = BTreeMap::new();
        render(
            &args,
            WiringReport {
                missing: declared,
                platform_base: None,
            },
            Readings {
                data_root: Path::new("/data"),
                pins: &Pins::default(),
                verified: &verified,
                subset: None,
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table,
            },
        )
    }

    /// 声明过的缺席**不许**再在它盖住的那一格上印 `errored>0`；没被点名的格子**必须**照印。
    ///
    /// 两向都钉在这一条上：正向少了它，一个「凡是跑不动就算没轮到跑」的实现会过掉；
    /// 反向少了它，一个「凡有声明就整表洗干净」的实现会过掉——而后者会把真缺陷一起抹掉。
    #[test]
    fn a_declared_absence_moves_its_own_cells_to_blocked_and_no_others() {
        let declared = [
            Missing::new(
                "swe-bench-pro",
                MissingScope::Column("swe-bench-pro".into()),
                Absence::NotWired,
                "no container machine is usable on this host",
            ),
            Missing::new(
                "nql",
                MissingScope::Row("nql".into()),
                Absence::NotWired,
                "no platform is wired",
            ),
        ];
        let text = render_with(&table_with_cells(), &declared);

        // 正向：被点名的三格。
        assert!(
            text.contains(
                "cell\thle\tnql\tseed=0\tresolved=0\ttotal=40\terrored=0\tblocked=40\tblocked-by=nql\t"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "cell\tswe-bench-pro\tgepa\tseed=0\tresolved=0\ttotal=40\terrored=0\tblocked=40\tblocked-by=swe-bench-pro\t"
            ),
            "{text}"
        );
        // 两处声明都盖到这一格：两个名字都要点出来，少一个就读不出是谁挡的。
        assert!(text.contains("blocked-by=swe-bench-pro,nql\t"), "{text}");
        // 反向：没人点名的那一格照印 `errored`。
        assert!(
            text.contains(
                "cell\thle\tgepa\tseed=0\tresolved=0\ttotal=40\terrored=40\tblocked=0\tblocked-by=-\t"
            ),
            "{text}"
        );
        // 尾数两个一起出，且与逐格的和对得上。
        assert!(text.contains("\nerrored\t40\tblocked\t120\n"), "{text}");
    }

    /// 整次跑那一类的声明**一个格子都不盖**。
    ///
    /// 它说的是「这次跑不构成一行表」（诊断臂），而格子是真跑过的读数，被洗成 `blocked`
    /// 就把「这一格本来跑得动」一起抹掉了。
    #[test]
    fn a_run_wide_absence_covers_no_cell() {
        let declared = [Missing::new(
            "tools",
            MissingScope::Run,
            Absence::OutOfArm,
            "this run used the tools-off diagnostic arm, so it is not a main-table row",
        )];
        let text = render_with(&table_with_cells(), &declared);

        assert!(
            text.contains(
                "cell\thle\tgepa\tseed=0\tresolved=0\ttotal=40\terrored=40\tblocked=0\tblocked-by=-\t"
            ),
            "{text}"
        );
        // 四格全没被盖住：160 条跑不动，一条都不进 `blocked`。
        assert!(text.contains("\nerrored\t160\tblocked\t0\n"), "{text}");
    }

    /// 另一向的整次跑缺席：骨干与裁判缺了，**每一格**都没读数，所以它们算没轮到跑。
    #[test]
    fn a_whole_run_absence_that_every_cell_needs_covers_every_cell() {
        let declared = [Missing::new(
            "backbone",
            MissingScope::EveryCell,
            Absence::NotWired,
            "no LLM upstream is configured",
        )];
        let text = render_with(&table_with_cells(), &declared);
        assert!(text.contains("\nerrored\t0\tblocked\t160\n"), "{text}");
        assert!(
            text.contains(
                "cell\thle\tgepa\tseed=0\tresolved=0\ttotal=40\terrored=0\tblocked=40\tblocked-by=backbone\t"
            ),
            "{text}"
        );
    }

    /// `## wiring` 那一行要说清**管到哪**与**哪一类**。
    ///
    /// 只有「缺 X」时，读的人看不出照着做的那一步是写代码还是改配置，也看不出 `## per seed`
    /// 里那些 `errored` 算不算缺陷。
    ///
    /// 两类缺席各钉一次，而且钉的是**各自那个类**：`toolathlon` 树里没有 compose 后端，所以
    /// 是 `not-implemented`；`swe-bench-pro` 树里有容器后端，缺席只可能是没接进来，所以是
    /// `not-wired`。把后者的类别写成前者，读的人会去做一件已经做完的事。
    #[test]
    fn a_wiring_line_says_what_it_covers_and_what_kind_of_absence_it_is() {
        let mut raw = minimal();
        // 这一段测的是几条声明本身；诊断臂那一条会混进来，所以换成主表那一臂。
        let at = raw.iter().position(|s| s == "none").expect("--tools none");
        raw[at] = "platform".to_string();
        let args = EvalTableArgs::parse(&raw).unwrap();
        let declared = missing_dependencies(
            ToolArm::Platform,
            &["hle", "swe-bench-pro", "toolathlon"],
            &["nql".to_string()],
            &Wiring::default(),
        );
        let verified = BTreeMap::new();
        let text = render(
            &args,
            WiringReport {
                missing: &declared,
                platform_base: None,
            },
            Readings {
                data_root: Path::new("/data"),
                pins: &Pins::default(),
                verified: &verified,
                subset: None,
                checkpoint: &checkpoint_at("/tmp/table.md"),
                table: &table_with_cells(),
            },
        );
        for expected in [
            "missing\tswe-bench-pro\tcolumn:swe-bench-pro\tnot-wired\t",
            "missing\ttoolathlon\tcolumn:toolathlon\tnot-implemented\t",
            "missing\tnql\trow:nql\tnot-wired\t",
            "missing\tbackbone\tevery-cell\tnot-wired\t",
            "missing\tjudge\tevery-cell\tnot-wired\t",
        ] {
            assert!(text.contains(expected), "{expected} 不在：{text}");
        }
        // 人读那句与原样一致。它**不在这段文本里**：`## wiring` 印的是上面那种五格的结构，
        // 人读那句随 `Outcome.missing` 出去（退出码那一路上念的就是它），所以两处各钉一次。
        let nql = declared
            .iter()
            .find(|m| m.subject == "nql")
            .expect("平台这一行在这里是缺席的")
            .as_line();
        assert_eq!(
            nql,
            format!("nql: {PLATFORM_BASE_ENV} is unset in this run's environment")
        );
    }

    /// 容器后端没接上时，那条声明要把**接线的原因**念出来。
    ///
    /// 这一列缺的可能是三样互不相同的东西（这台宿主上没有容器机器 / 数据根里没有官方脚本 /
    /// 这次被开关关了），下一步分别是装机器、取脚本、改开关。原因没传下来时印的那句兜底必须
    /// 仍然说得出「是接线这一步的事」——写成「树里没有实现」，读的人会去做一件已经做完的事。
    #[test]
    fn an_unwired_container_backend_reports_why_it_was_not_wired() {
        let why = "no container machine is usable on this host: …";
        let with_reason = Wiring {
            swe_pro_reason: Some(why.into()),
            ..Default::default()
        };
        let line = |wiring: &Wiring| {
            missing_dependencies(ToolArm::Platform, &["swe-bench-pro"], &[], wiring)
                .iter()
                .find(|m| m.subject == "swe-bench-pro")
                .expect("这一列在这里是缺席的")
                .wiring_line()
        };
        assert_eq!(
            line(&with_reason),
            format!("missing\tswe-bench-pro\tcolumn:swe-bench-pro\tnot-wired\t{why}")
        );
        let fallback = line(&Wiring::default());
        assert!(
            fallback.ends_with("not-wired\tthe container backend is not wired into this run"),
            "{fallback}"
        );
        assert!(
            !fallback.contains("no container backend is implemented"),
            "缺席的类别已经变了，这句会把人送去写一个已经写好的后端：{fallback}"
        );
    }
}
