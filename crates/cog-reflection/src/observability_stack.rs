//! 可观测性栈清单的周期收敛：仓库声明什么，现场就该是什么。
//!
//! 这个栈的清单只有安装脚本一条交付路径。装完一次之后仓库与现场各走各的：
//! 改探针预算、改资源上限、加一块面板都只是改 git，要等下一次有人想起来跑安装
//! 脚本。于是出现两种都没人看得见的状态——集群上是旧清单，或者集群上是一个
//! 已经不存在的版本；而"清单没生效"这件事本身没有判据，因为判据（告警规则）
//! 也住在同一套清单描述的栈里。
//!
//! 这里按仓库**当前 rev** 的内容周期 apply，把现场修回声明态，并把修不回的
//! 部分报出去。三样不做，都刻意为：装 helm chart（要网络与 helm，属安装期）、
//! 应用 `Role`/`RoleBinding`（一份清单能长出权限就等于权限可以自我扩张，见
//! [`DocFate::Rbac`]）、创建 Namespace（集群级，属安装期）。这三样缺失时本轮
//! 会给出结论说清楚是哪一样，而不是沉默。
//!
//! 漂移与否**先比后写**：逐文件把这一轮要交出去的字节交给 `kubectl diff`
//! （只读）与现场比一遍，只见差异才 apply。判据不能用 `apply` 自己的逐资源
//! 动词——它报的是「算出了一个补丁」，而「现场一致」这件事在某些对象上根本
//! 读不出来，于是告警永远解除不了，理由也变成了工具内部的补丁算法。见
//! [`DiffOutcome`]。
//!
//! **逐文件交付，不是整批**：一份清单被拒只封住它自己，同一轮里其余该修的
//! 照修，结论两边都说出来（见 [`obstructed_after_partial`]）。整批一起交的
//! 代价是实测过的：`01-namespace.yaml` 里一条 `resourcequotas` 的授权缺口让
//! 排在它后面的每一份都没被比过——外观上读作「这个循环什么都没做」。
//!
//! 交付对象由 `git ls-tree` **枚举**，跳过与否由清单目录里那张处置表
//! （`delivery-dispositions.txt`）说了算——安装脚本读同一张表。两处各留一份
//! 名单必然分叉，分叉的样子是"装的时候跳过、收敛的时候照做"。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use cog_core::{PersistentAlertDraft, PersistentAlertSink, SFError, SFResult};
use tracing::{info, warn};

use crate::config::ObservabilityStackConfig;
use crate::governance_drift::{CheckFailure, DriftClass, GovernanceDrift};
use crate::mainline_deployer::{classify_doc, split_docs, DocFate, MainlineDeployer};

/// 收敛面自己拥有的规则名。与配置里的规则集无关：这条判据不能在配置里，
/// 因为配置没到的时候它正是要说这件事的那一条。
pub const STACK_NOT_CONVERGED_RULE: &str = "observability_stack_not_converged";

/// 这一行归哪个面：清单已经在集群里、可观测栈却没收敛到它声明的形状，要动的人是
/// 把交付真正变成生效的那一环。
const STACK_NOT_CONVERGED_FACE: &str = "落地≠生效";

/// 处置表在清单目录里的文件名。刻意不是 `.yaml`：清单目录按 `*.yaml` 遍历，
/// 表自己被当成清单交付一次就成了一个误会。
pub const DISPOSITIONS_FILE: &str = "delivery-dispositions.txt";

/// 处置表里一行的处置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// 正常交付。写下来是为了让这张表成为整套清单的盘点：加一个文件必须写下
    /// 它算什么，而不是让它默认滑进交付面。
    Deliver,
    /// 永不交付，理由必填。
    Exempt,
    /// 日志/时序明细后端，由后端开关决定。
    Backends,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispositionEntry {
    pub file: String,
    pub disposition: Disposition,
    pub reason: String,
}

/// 解析处置表。理由缺失即错：一条没有理由的处置就是下一个没人知道的洞。
pub fn parse_dispositions(text: &str) -> Result<Vec<DispositionEntry>, String> {
    let mut out: Vec<DispositionEntry> = Vec::new();
    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(3, char::is_whitespace);
        let file = parts.next().unwrap_or("").trim();
        let disp = parts.next().unwrap_or("").trim();
        let reason = parts
            .next()
            .unwrap_or("")
            .trim()
            .trim_start_matches('#')
            .trim();
        if file.is_empty() {
            return Err(format!("第 {} 行没有文件名", idx + 1));
        }
        let disposition = match disp {
            "deliver" => Disposition::Deliver,
            "exempt" => Disposition::Exempt,
            "backends" => Disposition::Backends,
            "" => return Err(format!("第 {} 行（{file}）没有处置值", idx + 1)),
            other => return Err(format!("第 {} 行（{file}）的处置值不认: {other}", idx + 1)),
        };
        if reason.is_empty() {
            return Err(format!("第 {} 行（{file}）没有理由", idx + 1));
        }
        if out.iter().any(|e| e.file == file) {
            return Err(format!("{file} 登记了两次"));
        }
        out.push(DispositionEntry {
            file: file.to_string(),
            disposition,
            reason: reason.to_string(),
        });
    }
    Ok(out)
}

/// 一轮要交付什么。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeliveryPlan {
    pub deliver: Vec<String>,
    /// 跳过的文件与理由，理由要能直接读给人看。
    pub skipped: Vec<(String, String)>,
}

/// 按处置表算出这一轮交付哪些文件。
///
/// 没登记的文件按**交付**处理。这是刻意的失效方向：漏登记的结果是它被应用
/// （看得见），反方向是静默不交付（看不见）——而"静默不交付"正是这条路要修
/// 的那个缺陷。覆盖方向由门禁兜（目录里每个 yaml 都必须登记）。
pub fn plan_delivery(
    files: &[String],
    entries: &[DispositionEntry],
    backends: bool,
) -> DeliveryPlan {
    let mut plan = DeliveryPlan::default();
    for file in files.iter().filter(|f| f.ends_with(".yaml")) {
        match entries.iter().find(|e| &e.file == file) {
            None => plan.deliver.push(file.clone()),
            Some(e) => match e.disposition {
                Disposition::Deliver => plan.deliver.push(file.clone()),
                Disposition::Exempt => plan
                    .skipped
                    .push((file.clone(), format!("登记为永不交付：{}", e.reason))),
                Disposition::Backends => {
                    if backends {
                        plan.deliver.push(file.clone());
                    } else {
                        plan.skipped
                            .push((file.clone(), format!("日志/明细后端已关闭：{}", e.reason)));
                    }
                }
            },
        }
    }
    plan
}

/// 目录里有、表里没有的清单（门禁用；运行时按交付处理）。
pub fn unregistered_files(files: &[String], entries: &[DispositionEntry]) -> Vec<String> {
    files
        .iter()
        .filter(|f| f.ends_with(".yaml") && !entries.iter().any(|e| &e.file == *f))
        .cloned()
        .collect()
}

/// 表里有、目录里没有的清单（门禁用：改名后留下的陈旧豁免会让新文件按交付
/// 处理，方向是安全的，但那条豁免已经不是它说的那件事了）。
pub fn dangling_entries(entries: &[DispositionEntry], files: &[String]) -> Vec<String> {
    entries
        .iter()
        .filter(|e| !files.iter().any(|f| f == &e.file))
        .map(|e| e.file.clone())
        .collect()
}

/// 一次 `kubectl diff` 的读数。
///
/// 漂移的判据是「现场与这一份字节有没有差异」，这条由 `kubectl diff` 回答。
/// 曾经用的是 `kubectl apply` 自己的逐资源动词（`configured` = 漂移），**已
/// 被现场否掉**：`configured` 报的是「算出了一个补丁」，不是「现场与声明不
/// 一致」。StatefulSet 的 `volumeClaimTemplates` 在服务端被物化出清单里写不
/// 出来的字段（`apiVersion`/`kind`/`status`），于是 `clickhouse` 与 `loki` 这两
/// 个 StatefulSet 每一轮都被报成 `configured`——同一时刻 `kubectl diff` 读作
/// 没有差异，而这两个对象的内容确实与声明逐字段相同。一个不可能读成「一致」
/// 的读数不是判据：它让告警永远解除不了，并且把「有人在改集群」这个结论挂在
/// 一个工具内部的补丁算法上。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffOutcome {
    /// 现场就是这份字节。
    Clean,
    /// 现场与这份字节有差异。
    Differs,
    /// 读不出来（跑不起来 / API 报错 / kind 解不出）。
    Failed(String),
}

/// 退出码 + stderr → 一次 diff 的读数。
///
/// 退出码语义由 kubectl 定义（0 无差异、1 有差异、>1 出错），但**退出码 1 要
/// 再看一眼 stderr**：连不上 API、kind 解不出这类失败也带着 1 回来，把它们读
/// 成漂移会凭空报一个现场根本没有的不一致。警告行（`W…`）不算错误——对象在
/// 比较期间被改过，kubectl 会留一行这样的警告，而那一轮照样比出了结果。
pub fn classify_diff(exit_code: i32, stderr: &str) -> DiffOutcome {
    let error = stderr
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("Error"));
    match exit_code {
        0 => DiffOutcome::Clean,
        1 => match error {
            Some(line) => DiffOutcome::Failed(line.to_string()),
            None => DiffOutcome::Differs,
        },
        other => DiffOutcome::Failed(match error {
            Some(line) => format!("退出码 {other}：{line}"),
            None => format!("退出码 {other}：{}", first_error_line(stderr)),
        }),
    }
}

/// 逐文件读数 → 漂移清单。
///
/// 任何一个文件读不出来就让整轮没有结论，而不是只报读出来的那些：一个「与声明
/// 不一致」的结论要么覆盖这一轮要交付的全部文件，要么不说——少报一个文件与多
/// 报一个文件的代价不对称，而这里连「它是不是一致」都没读到。
pub fn drifted_files(readings: &[(String, DiffOutcome)]) -> Result<Vec<String>, String> {
    let mut drifted = Vec::new();
    for (file, outcome) in readings {
        match outcome {
            DiffOutcome::Clean => {}
            DiffOutcome::Differs => drifted.push(file.clone()),
            DiffOutcome::Failed(detail) => {
                return Err(format!("{file} 的 diff 读不出来：{detail}"));
            }
        }
    }
    Ok(drifted)
}

/// 一份 `kubectl diff` 输出里有多少行不一样。
///
/// 这是这个面上最接近"差了几个字段"的读数：清单是 YAML，一对一的差异就落在
/// 增删行上。`+++`／`---` 是统一差分自己的文件头，不是内容。
///
/// 数不清的时候它给 0，而**调用方不能把 0 当成"没有差异"**——`Differs` 是
/// 退出码说的，比这行文字更硬。见调用处怎么处理这个 0。
pub fn differing_lines(diff: &str) -> usize {
    diff.lines()
        .filter(|l| {
            (l.starts_with('+') || l.starts_with('-'))
                && !l.starts_with("+++")
                && !l.starts_with("---")
        })
        .count()
}

/// `Differs` 的下界：退出码说两边不一样，差分文字至少也要读成一处不同。
///
/// 这不是一个调出来的数，而是 `DiffOutcome::Differs` 的含义本身——"至少一处"，
/// 所以它不需要从哪次实测导出，名字就是它的出处。
// cap: bound — 不是调出来的数，是 DiffOutcome::Differs 的含义本身（至少一处），名字就是出处
const AT_LEAST_ONE_DIFFERENCE: usize = 1;

/// 一个 `Differs` 文件的读数：`(值, 这次读数是不是没解出来)`。
///
/// 值**下界是 [`AT_LEAST_ONE_DIFFERENCE`]**——`DiffOutcome` 是退出码给的结论，
/// 比差分那行文字硬。文字一行都解不出来时，值报 1、同时把"读不出来"报出去：把 0
/// 写进这张表就等于替一次没读成的比较声明"两边一致"。
pub fn differs_reading(lines: usize) -> (usize, bool) {
    (lines.max(AT_LEAST_ONE_DIFFERENCE), lines == 0)
}

/// apply 没成的原因，按**能不能自己修**分组。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Obstacle {
    /// 命名空间不存在：这个栈根本没装（或装过被删了）。集群级资源，收敛面
    /// 不创建它——这一步属安装期。
    NamespaceAbsent(String),
    /// CRD 不存在：指标栈（helm）没装成，或者装了但没到 CRD 那一步。
    CrdAbsent(String),
    /// 权限被拒：授权面没到位（Role 只能由安装期交付）。
    Forbidden(String),
    /// 清单里出现了 Secret：零带外凭证红线，密钥永不进清单链路。
    SecretRefused(String),
    /// 清单内容本身读不了（YAML 坏了）。
    Unusable(String),
}

impl Obstacle {
    /// 同一个成因、换一句更长的正文（带上文件名与这一轮的份数）。
    ///
    /// 成因的种类决定 severity 与 advice，这两样不该因为「顺便说清是哪份文件」
    /// 而变；所以只换正文，不换种类。
    pub fn with_detail(&self, detail: String) -> Obstacle {
        match self {
            Obstacle::NamespaceAbsent(_) => Obstacle::NamespaceAbsent(detail),
            Obstacle::CrdAbsent(_) => Obstacle::CrdAbsent(detail),
            Obstacle::Forbidden(_) => Obstacle::Forbidden(detail),
            Obstacle::SecretRefused(_) => Obstacle::SecretRefused(detail),
            Obstacle::Unusable(_) => Obstacle::Unusable(detail),
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Obstacle::NamespaceAbsent(d)
            | Obstacle::CrdAbsent(d)
            | Obstacle::Forbidden(d)
            | Obstacle::SecretRefused(d)
            | Obstacle::Unusable(d) => d,
        }
    }

    /// 给读这条告警的人一句能动手的话。
    pub fn advice(&self) -> &'static str {
        match self {
            Obstacle::NamespaceAbsent(_) => {
                "这个栈还没装：安装路径是 deploy/k3s/observability/scripts/install.sh（要 helm 与网络，属安装期步骤）。它不在的期间，自发现规则没有可查的数据源"
            }
            Obstacle::CrdAbsent(_) => {
                "指标栈的 CRD 不存在：install.sh 里的 helm 那一步没走完（ServiceMonitor/PodMonitor 无处可落）"
            }
            Obstacle::Forbidden(_) => {
                "授权面没到位：Role/RoleBinding 只能由安装期交付，收敛面按构造不应用它们"
            }
            Obstacle::SecretRefused(_) => "清单里不该有 Secret，密钥不进清单链路",
            Obstacle::Unusable(_) => "清单本身读不了，先修仓库里那份文件",
        }
    }

    /// 严重度：栈整个不在，是"自发现整条没有证据面"，按 critical；其余 warning。
    pub fn severity(&self) -> &'static str {
        match self {
            Obstacle::NamespaceAbsent(_) => "critical",
            _ => "warning",
        }
    }
}

/// 从 apply 的 stderr 判成因。按优先级取第一条命中的：命名空间不存在时每个
/// 资源都会报一次，先认它才不会把一件事说成二十件。
pub fn classify_obstacle(stderr: &str, namespace: &str) -> Option<Obstacle> {
    let text = stderr.trim();
    if text.is_empty() {
        return None;
    }
    let needle = format!("namespaces \"{namespace}\" not found");
    if text.contains(&needle) {
        return Some(Obstacle::NamespaceAbsent(format!(
            "命名空间 {namespace} 不存在（{}）",
            first_error_line(text)
        )));
    }
    if text.contains("no matches for kind") {
        return Some(Obstacle::CrdAbsent(format!(
            "清单里有集群不认识的自定义资源（{}）",
            first_error_line(text)
        )));
    }
    if text.contains("is forbidden") || text.contains("Forbidden") {
        return Some(Obstacle::Forbidden(first_error_line(text)));
    }
    Some(Obstacle::Unusable(first_error_line(text)))
}

/// 一轮里既有交付成功的、也有被拒的时候，结论仍是「没收敛」，但要把两边都说出来。
///
/// 只报第一条成因会让人以为整轮白跑；只报修好的份数会把一次没收敛说成收敛。
/// 严重度取最重的那条（命名空间整个不在时，其余每份都会陪报一次，不该让一条
/// 授权缺口把它盖过去）；成因正文带上份数与文件名，这也是 [`Obstacle::Unusable`]
/// 之外唯一能承载「一份文件」这个粒度的位置。
pub fn obstructed_after_partial(
    obstacles: &[(String, Obstacle)],
    repaired: usize,
) -> ConvergenceVerdict {
    let worst = obstacles
        .iter()
        .map(|(_, o)| o)
        .find(|o| o.severity() == "critical")
        .or_else(|| obstacles.first().map(|(_, o)| o));
    let Some(worst) = worst else {
        return ConvergenceVerdict::Converged { resources: 0 };
    };
    let names: Vec<&str> = obstacles.iter().map(|(f, _)| f.as_str()).collect();
    let mut detail = if names.len() == 1 {
        format!("{}（{}）", worst.detail(), names[0])
    } else {
        format!(
            "{}（本轮共 {} 份没能交付：{}）",
            worst.detail(),
            names.len(),
            names.join(", ")
        )
    };
    if repaired > 0 {
        detail.push_str(&format!("；另有 {repaired} 份已按声明修回，这一轮不是白跑"));
    }
    ConvergenceVerdict::Obstructed(worst.with_detail(detail))
}

fn first_error_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string()
}

/// 一轮收敛的结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvergenceVerdict {
    /// 现场本来就是声明态（这一轮没有任何文件与现场不一致，因此也没有写）。
    Converged { resources: usize },
    /// 现场不是声明态，这一轮修回去了。点名的单位是**清单文件**：判据是逐文件
    /// 比出来的，而处置动作也是按文件读的。要报：反复发生的修复说明有个东西在
    /// 改集群，或者上一次交付根本没跑到，而这两种都需要人看一眼。
    DriftRepaired { repaired: Vec<String> },
    /// 修不回去，原因是这个。
    Obstructed(Obstacle),
    /// 没有结论：读不到仓库里这一 rev 的清单。
    NoEvidence(String),
}

impl ConvergenceVerdict {
    pub fn is_firing(&self) -> bool {
        !matches!(self, ConvergenceVerdict::Converged { .. })
    }

    pub fn severity(&self) -> &'static str {
        match self {
            ConvergenceVerdict::Obstructed(o) => o.severity(),
            _ => "warning",
        }
    }

    /// 给人读的结论，有界（点名的资源数由配置上限截断，其余折成计数）。
    pub fn message(&self, rev: &str, max_named: usize) -> String {
        match self {
            ConvergenceVerdict::Converged { resources } => {
                format!("可观测性栈与仓库声明一致（{resources} 个资源，rev {}）", rev12(rev))
            }
            ConvergenceVerdict::DriftRepaired { repaired } => format!(
                "可观测性栈的现场与仓库声明不一致，已按 rev {} 修回（与现场不一致的清单文件）：{}（不是第一次出现的修复说明有个东西在改集群，或者上一次交付没跑到）",
                rev12(rev),
                join_named(repaired, max_named)
            ),
            ConvergenceVerdict::Obstructed(o) => format!(
                "可观测性栈没能在 rev {} 与仓库声明收敛：{}。{}",
                rev12(rev),
                o.detail(),
                o.advice()
            ),
            ConvergenceVerdict::NoEvidence(detail) => format!(
                "这一轮没有结论：{detail}（rev {} 的清单读不到）",
                rev12(rev)
            ),
        }
    }
}

fn rev12(rev: &str) -> String {
    rev.chars().take(12).collect()
}

/// 日志里一段文本的字符上限。界挂在**字符数**上而不是行数上：一行可以很长，
/// 按行截断等于没有上界（`kubectl diff` 的对象正文就是单行 JSON 拼出来的）。
// cap: bound — 界挂在字符数上而不是行数上：一行可以很长，按行截断等于没有上界
const DIFF_LOG_CHARS: usize = 2000;

/// 截断到 `max` 个字符，截断时留一个尾巴让读的人知道后面还有。
fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push_str("…[截断]");
    out
}

fn join_named(names: &[String], max_named: usize) -> String {
    if names.is_empty() {
        return "（无）".into();
    }
    if names.len() <= max_named {
        return names.join(", ");
    }
    format!(
        "{}, +{} more",
        names[..max_named].join(", "),
        names.len() - max_named
    )
}

/// 收敛面。
pub struct StackConvergence {
    deployer: Arc<MainlineDeployer>,
    cfg: ObservabilityStackConfig,
    sink: Option<Arc<dyn PersistentAlertSink>>,
    /// 这个面的漂移读数落在哪。与治理面共用同一张表结构、同一个失败计数器
    /// （按因分格），因为两边要回答的是同一句话：声明的与现场的不一样，而
    /// 这句话的读数只有一个地方收，才有人能把它当序列读。
    drift: Option<Arc<GovernanceDrift>>,
}

impl StackConvergence {
    /// `sink` 是持久化告警口。没有它结论只进日志——一条只在日志里的事实，
    /// 和"没人报"长得一样。
    ///
    /// `drift` 是漂移读数。没有它这一轮照样比、照样修，只是没人量得到——与
    /// `sink` 缺席同一个形状，构造处因此也要说一声。
    pub fn new(
        deployer: Arc<MainlineDeployer>,
        cfg: ObservabilityStackConfig,
        sink: Option<Arc<dyn PersistentAlertSink>>,
        drift: Option<Arc<GovernanceDrift>>,
    ) -> Arc<Self> {
        if drift.is_none() {
            warn!(
                "observability stack convergence running with no drift handle; the per-file \
                 comparison will reach no scrape"
            );
        }
        Arc::new(Self {
            deployer,
            cfg,
            sink,
            drift,
        })
    }

    /// 记一次做不成的比较。没有句柄时什么也不记：那是"这个进程没有读数面"，
    /// 与"比较失败"不是一回事，构造处已经说过。
    fn note_failure(&self, reason: CheckFailure) {
        if let Some(drift) = &self.drift {
            drift.record_check_failure(reason);
        }
    }

    /// 后台循环：先跑一轮（把"装完就没再管"这件事立刻显形），之后按间隔跑。
    pub async fn run(self: Arc<Self>, shutdown: cog_core::ShutdownSignal) {
        let interval = Duration::from_secs(
            self.cfg
                .interval_secs
                .max(ObservabilityStackConfig::MIN_INTERVAL_SECS),
        );
        loop {
            let rev = match self.deployer.bare_main_rev().await {
                Ok(rev) => rev,
                Err(e) => {
                    warn!(error = %e, "observability stack: cannot read the bare repo revision");
                    String::new()
                }
            };
            let verdict = self.converge_once(&rev).await;
            self.publish(&verdict, &rev).await;
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// 一轮：枚举 → 计划 → 应用 → 结论。
    async fn converge_once(&self, rev: &str) -> ConvergenceVerdict {
        if rev.is_empty() {
            return ConvergenceVerdict::NoEvidence("读不到 bare 仓库的分支 rev".into());
        }
        let dir = self.cfg.manifest_dir.trim_end_matches('/').to_string();
        let files = match self.deployer.git_ls_dir(rev, &dir).await {
            Ok(files) => files,
            Err(e) => {
                self.note_failure(CheckFailure::ObservabilityDeclarationUnreadable);
                return ConvergenceVerdict::NoEvidence(format!("{dir} 列不出文件: {e}"));
            }
        };
        let dispositions = match self
            .deployer
            .git_show(rev, &format!("{dir}/{DISPOSITIONS_FILE}"))
            .await
        {
            Ok(text) => match parse_dispositions(&text) {
                Ok(entries) => entries,
                Err(e) => {
                    self.note_failure(CheckFailure::ObservabilityDeclarationUnreadable);
                    return ConvergenceVerdict::NoEvidence(format!(
                        "{DISPOSITIONS_FILE} 读不了: {e}"
                    ));
                }
            },
            Err(e) => {
                self.note_failure(CheckFailure::ObservabilityDeclarationUnreadable);
                return ConvergenceVerdict::NoEvidence(format!("{DISPOSITIONS_FILE} 读不到: {e}"));
            }
        };
        // 漏登记的清单按交付处理，但要说出来：门禁拦得住提交，拦不住一个别人
        // 手工推到 bare 的 rev。
        let unregistered = unregistered_files(&files, &dispositions);
        if !unregistered.is_empty() {
            warn!(
                files = %unregistered.join(", "),
                "observability stack: manifests without a registered disposition; delivering them"
            );
        }
        let plan = plan_delivery(&files, &dispositions, self.cfg.backends);
        for (file, reason) in &plan.skipped {
            info!(file = %file, reason = %reason, "observability stack: not delivered this round");
        }

        // 逐个文件取内容、拆文档、按 kind 归置。逐文件留一份**这一轮真要交出去
        // 的字节**：漂移判据与 apply 必须读同一份，从这份字节里再算一遍是另一
        // 个读数，两者不同步时没人能说是谁错。
        let mut per_file: Vec<(String, String)> = Vec::new();
        let mut deliverable = String::new();
        let mut deliverable_docs = 0usize;
        let mut skipped_classes: Vec<String> = Vec::new();
        for file in &plan.deliver {
            let path = format!("{dir}/{file}");
            let text = match self.deployer.git_show(rev, &path).await {
                Ok(text) => text,
                Err(e) => {
                    self.note_failure(CheckFailure::ObservabilityDeclarationUnreadable);
                    return ConvergenceVerdict::NoEvidence(format!("{path} 读不到: {e}"));
                }
            };
            let docs = match split_docs(&text, &path) {
                Ok(docs) => docs,
                Err(e) => {
                    self.note_failure(CheckFailure::ObservabilityDeclarationUnreadable);
                    return ConvergenceVerdict::Obstructed(Obstacle::Unusable(format!("{e}")));
                }
            };
            let mut file_body = String::new();
            for doc in docs {
                let kind = doc.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                let name = doc
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                let ident = format!("{kind}/{name}");
                match classify_doc(kind) {
                    DocFate::Deliver => {
                        file_body.push_str("---\n");
                        file_body.push_str(
                            &serde_yaml::to_string(&doc).unwrap_or_else(|_| String::new()),
                        );
                        deliverable_docs += 1;
                    }
                    DocFate::ForbiddenSecret => {
                        return ConvergenceVerdict::Obstructed(Obstacle::SecretRefused(format!(
                            "{file} 里有 Secret（{ident}）"
                        )));
                    }
                    other => {
                        let class = match other {
                            DocFate::ClusterScoped => "集群级",
                            DocFate::Rbac => "授权面",
                            DocFate::StorageClaim => "卷声明",
                            DocFate::Deliver | DocFate::ForbiddenSecret => unreachable!(),
                        };
                        skipped_classes.push(format!("{file}:{ident}={class}"));
                    }
                }
            }
            if !file_body.is_empty() {
                deliverable.push_str(&file_body);
                per_file.push((file.clone(), file_body));
            }
        }
        if !skipped_classes.is_empty() {
            info!(
                withheld = %skipped_classes.join(" "),
                "observability stack: documents the loop must not deliver (install-time surfaces)"
            );
        }

        if deliverable.is_empty() {
            return ConvergenceVerdict::NoEvidence(format!(
                "{dir} 这一 rev 没有一个可交付的文档（跳过：{}）",
                plan.skipped
                    .iter()
                    .map(|(f, _)| f.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        // 判据先跑，而且**不改集群**：逐文件与现场比一遍，只见差异才写。
        //
        // 逐文件比要有总预算：每次调用各自的超时叠起来是「文件数 × 超时」，慢的
        // API 会让一轮退化成几十分钟。预算取配置面已有的那一个（与 apply 共用
        // 声明），用完就这一轮没有结论——不新造一个数。
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.cfg.apply_timeout_secs);
        let mut readings: Vec<(String, DiffOutcome)> = Vec::with_capacity(per_file.len());
        let mut line_counts: BTreeMap<String, usize> = BTreeMap::new();
        // 一份文件被拒**不再作废整轮**。代价是实测过的：`01-namespace.yaml` 里那条
        // `ResourceQuota` 因为 Role 少一条 `resourcequotas` 而被拒，而它按文件名排
        // 最前，于是每一轮都在第一份上返回，后面十几份根本没被比过、也没被交付——
        // 外观上读作「收敛循环什么都没做」。一份清单的授权缺口只该封住它自己。
        let mut obstacles: Vec<(String, Obstacle)> = Vec::new();
        for (file, body) in &per_file {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return ConvergenceVerdict::NoEvidence(format!(
                    "{file} 的 diff 没时间跑了：这一轮与集群打交道的预算（{} 秒）在一次比一遍里用完了",
                    self.cfg.apply_timeout_secs
                ));
            }
            let (code, diff, stderr) = match self
                .deployer
                .diff_capture(&self.cfg.namespace, body.as_bytes(), left.as_secs().max(1))
                .await
            {
                Ok(out) => out,
                Err(e) => return ConvergenceVerdict::NoEvidence(format!("{file} 的 diff: {e}")),
            };
            let outcome = classify_diff(code, &stderr);
            if let DiffOutcome::Failed(_) = &outcome {
                if let Some(obstacle) = classify_obstacle(&stderr, &self.cfg.namespace) {
                    warn!(
                        file = %file,
                        reason = %obstacle.detail(),
                        "observability stack: this file cannot be compared this round; the rest are still compared and delivered"
                    );
                    obstacles.push((file.clone(), obstacle));
                }
            }
            if !diff.trim().is_empty() {
                info!(file = %file, diff = %clip(&diff, DIFF_LOG_CHARS), "observability stack: live state differs from the declared manifest");
            }
            line_counts.insert(file.clone(), differing_lines(&diff));
            readings.push((file.clone(), outcome));
        }

        // 读数**先发、再修**。倒过来的顺序（先 apply 再发）会让一次成功的愈合
        // 把刚量到的漂移抹掉：下一轮 diff 必然 `Clean`，读数面上于是只剩"从来不
        // 漂"——而漂移只在被修掉之前的这一瞬间存在，正好是最该有人看见的那一瞬。
        //
        // 一个文件读不出来，这一轮就**不是**一次比较：按因计一格，表留在上一轮
        // 的样子。让比较失败的轮次去写表，等于替没做过的那次比较声明"两边一致"。
        //
        // 代价要说清：表是整张写的，所以冻住是**整张**冻住——连这一轮读得好好的
        // 那些文件的漂移读数也一起停在上一轮的数上。于是"冻住"与"真在漂"在
        // `cogneva_observability_drift_lines` 上长得一样，分辨它们的地方只有一个：
        // 同一次故障里 `cogneva_governance_check_failures_total` 按因分格的那一格
        // 同时在涨。这是**故意的两信号读数**，不是一处疏漏；只读 gauge 那一侧会把
        // 一次读不出来的比较读成一场修不好的漂移。
        let mut compared: BTreeMap<String, usize> = BTreeMap::new();
        let mut uncomparable = false;
        for (file, outcome) in &readings {
            match outcome {
                DiffOutcome::Clean => {
                    compared.insert(file.clone(), 0);
                }
                DiffOutcome::Differs => {
                    let (value, unparsed) =
                        differs_reading(line_counts.get(file).copied().unwrap_or(0));
                    if unparsed {
                        // 退出码说两边不一样，而这份差异里一行 `+`/`-` 都没解出
                        // 来：那是**这次比较的读数**读不出来，不是"没有差异"。
                        self.note_failure(CheckFailure::ObservabilityLiveUnparseable);
                    }
                    compared.insert(file.clone(), value);
                }
                DiffOutcome::Failed(detail) => {
                    warn!(
                        file = %file,
                        error = %detail,
                        "observability stack: this file's comparison could not be made"
                    );
                    self.note_failure(CheckFailure::ObservabilityLiveUnreadable);
                    uncomparable = true;
                }
            }
        }
        if !uncomparable {
            if let Some(drift) = &self.drift {
                drift.record_comparison(DriftClass::Observability, &compared);
            }
        }

        // 比不出来的那份不进 `drifted_files`：它既不是"一致"也不是"不一致"，
        // 是这一次没问到。它已经记在 `obstacles` 里，结论会带着它出去。
        let comparable: Vec<(String, DiffOutcome)> = readings
            .iter()
            .filter(|(_, outcome)| !matches!(outcome, DiffOutcome::Failed(_)))
            .cloned()
            .collect();
        let drifted = match drifted_files(&comparable) {
            Ok(drifted) => drifted,
            Err(detail) => return ConvergenceVerdict::NoEvidence(detail),
        };
        if drifted.is_empty() {
            if obstacles.is_empty() {
                return ConvergenceVerdict::Converged {
                    resources: deliverable_docs,
                };
            }
            return obstructed_after_partial(&obstacles, 0);
        }

        // 逐文件交付，而不是把整批拼成一份 blob：一份被拒时把整批一起拖下来，
        // 是这条路上实测过的形状（一条配额授权缺口封掉了排在它后面的所有清单）。
        let mut repaired: Vec<String> = Vec::new();
        for file in &drifted {
            let Some((_, body)) = per_file.iter().find(|(f, _)| f == file) else {
                continue;
            };
            let (ok, stdout, stderr) = match self
                .deployer
                .apply_capture(
                    &self.cfg.namespace,
                    body.as_bytes(),
                    self.cfg.apply_timeout_secs,
                )
                .await
            {
                Ok(out) => out,
                Err(e) => return ConvergenceVerdict::NoEvidence(format!("{file} 的 apply: {e}")),
            };
            if !ok {
                let obstacle = classify_obstacle(&stderr, &self.cfg.namespace)
                    .unwrap_or_else(|| Obstacle::Unusable(first_error_line(&stderr)));
                warn!(
                    file = %file,
                    reason = %obstacle.detail(),
                    "observability stack: this file was not delivered this round"
                );
                obstacles.push((file.clone(), obstacle));
                continue;
            }
            // apply 自己的输出只当交付细节记一行：`configured` 不是漂移读数（见
            // [`DiffOutcome`]），写在这里是为了出事时能看见它到底写了哪些对象。
            info!(
                file = %file,
                applied = %clip(&stdout, DIFF_LOG_CHARS),
                "observability stack: drifted manifest re-applied"
            );
            repaired.push(file.clone());
        }
        if !obstacles.is_empty() {
            return obstructed_after_partial(&obstacles, repaired.len());
        }
        ConvergenceVerdict::DriftRepaired { repaired }
    }

    /// 把结论送进持久化告警面（恢复时同一调用解除）。
    async fn publish(&self, verdict: &ConvergenceVerdict, rev: &str) {
        let message = verdict.message(rev, self.cfg.max_named_resources);
        let firing = verdict.is_firing();
        if firing {
            warn!(rev = %rev, "{}", message);
        } else {
            info!(rev = %rev, "{}", message);
        }
        let Some(ref sink) = self.sink else {
            return;
        };
        let draft = PersistentAlertDraft {
            rule: STACK_NOT_CONVERGED_RULE.into(),
            dedup_key: STACK_NOT_CONVERGED_RULE.into(),
            severity: verdict.severity().into(),
            message,
            labels: convergence_alert_labels(&self.cfg.namespace, &self.cfg.manifest_dir, rev),
        };
        if let Err(e) = sink.set_persistent_alert(firing, &draft).await {
            warn!(error = %e, "observability stack verdict not persisted");
        }
    }
}

/// 收敛告警那一行存下来时的标签。这条判据不能在规则库里：配置还没到的时候，正是它要说
/// 这件事。所以「该谁动」也只能在这里写，读表的人除了这一格外没有别的依据。
fn convergence_alert_labels(namespace: &str, manifest_dir: &str, rev: &str) -> serde_json::Value {
    serde_json::json!({
        "namespace": namespace,
        "manifest_dir": manifest_dir,
        "rev": rev12(rev),
        cog_core::alerts::ALERT_FACE_LABEL: STACK_NOT_CONVERGED_FACE,
    })
}

/// 供门禁使用：把清单目录里的文件名收敛成可比较的形状。
pub fn yaml_manifests(files: &[String]) -> Vec<String> {
    files
        .iter()
        .filter(|f| f.ends_with(".yaml"))
        .cloned()
        .collect()
}

/// 读仓库工作树里的处置表（门禁用；运行时不走文件系统，走 bare 仓库的 rev）。
pub fn read_dispositions_from(dir: &std::path::Path) -> Result<Vec<DispositionEntry>, String> {
    let path = dir.join(DISPOSITIONS_FILE);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    parse_dispositions(&text)
}

/// 与 `install.sh` 共用同一张表的自检：两边读的是同一个文件。
pub fn dispositions_path(manifest_dir: &std::path::Path) -> std::path::PathBuf {
    manifest_dir.join(DISPOSITIONS_FILE)
}

impl StackConvergence {
    /// 供测试与诊断：本轮要交付的文件名。
    pub fn plan_for(&self, files: &[String], entries: &[DispositionEntry]) -> DeliveryPlan {
        plan_delivery(files, entries, self.cfg.backends)
    }
}

impl std::fmt::Debug for StackConvergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StackConvergence")
            .field("namespace", &self.cfg.namespace)
            .field("manifest_dir", &self.cfg.manifest_dir)
            .field("interval_secs", &self.cfg.interval_secs)
            .finish()
    }
}

/// 配置面的自检：间隔与上限的取值不合法时直接失败，而不是悄悄用另一个数。
pub fn validate_config(cfg: &ObservabilityStackConfig) -> SFResult<()> {
    if cfg.manifest_dir.trim().is_empty() {
        return Err(SFError::Config(
            "mainline_deployer.observability_stack.manifest_dir is empty".into(),
        ));
    }
    if cfg.namespace.trim().is_empty() {
        return Err(SFError::Config(
            "mainline_deployer.observability_stack.namespace is empty".into(),
        ));
    }
    if cfg.interval_secs < ObservabilityStackConfig::MIN_INTERVAL_SECS {
        return Err(SFError::Config(format!(
            "mainline_deployer.observability_stack.interval_secs must be >= {}",
            ObservabilityStackConfig::MIN_INTERVAL_SECS
        )));
    }
    if cfg.max_named_resources == 0 {
        return Err(SFError::Config(
            "mainline_deployer.observability_stack.max_named_resources must be > 0".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一份文件被拒，不该把整轮的结论说成「什么都没发生」。
    ///
    /// 实测过的形状：`01-namespace.yaml` 里那条 `ResourceQuota` 因为 Role 少一条
    /// `resourcequotas` 被拒，而它按文件名排最前——整轮中止，排在它后面的十几份
    /// 连比都没比过，读起来像「收敛循环什么都没做」。这一格钉的是：那份被拒的
    /// 要点名，同一轮里修好的那些也要说出来，两者都不能被吞掉。
    #[test]
    fn a_refused_file_does_not_erase_what_the_same_round_did_deliver() {
        let obstacles = vec![(
            "01-namespace.yaml".to_string(),
            Obstacle::Forbidden("resourcequotas \"monitoring-quota\" is forbidden".into()),
        )];
        let verdict = obstructed_after_partial(&obstacles, 3);
        assert!(verdict.is_firing(), "有文件没交付，结论必须是没收敛");
        let message = verdict.message("0123456789abcdef", 8);
        assert!(
            message.contains("01-namespace.yaml"),
            "结论没说清是哪一份被拒：{message}"
        );
        assert!(
            message.contains("3 份"),
            "同一轮修好的那几份被吞掉了——读起来会像整轮白跑：{message}"
        );
    }

    /// 多份被拒时点名到份数，而不是只报第一条成因。
    #[test]
    fn several_refusals_are_counted_not_collapsed_into_the_first() {
        let obstacles = vec![
            (
                "01-namespace.yaml".to_string(),
                Obstacle::Forbidden("a".into()),
            ),
            (
                "04-servicemonitor-cogneva.yaml".to_string(),
                Obstacle::Forbidden("b".into()),
            ),
        ];
        let message = obstructed_after_partial(&obstacles, 0).message("0123456789abcdef", 8);
        assert!(
            message.contains("2 份"),
            "只报了第一条，份数丢了：{message}"
        );
        assert!(
            message.contains("04-servicemonitor-cogneva.yaml"),
            "点名只点到第一份：{message}"
        );
    }

    /// 严重度取最重的那条：命名空间整个不在时其余每份都会陪报一次，
    /// 一条授权缺口不该把「栈根本没装」压成 warning。
    #[test]
    fn the_round_takes_the_heaviest_obstacle_not_the_first_one() {
        let obstacles = vec![
            ("a.yaml".to_string(), Obstacle::Forbidden("x".into())),
            (
                "b.yaml".to_string(),
                Obstacle::NamespaceAbsent("monitoring 不存在".into()),
            ),
        ];
        assert_eq!(
            obstructed_after_partial(&obstacles, 1).severity(),
            "critical"
        );
    }

    /// 可观测栈没收敛那一行要说得出该谁动。这条规则按设计不在规则库里（配置没到的
    /// 时候正是它要说话），所以面没有配置可申报，只有这一格写着——删掉它，行照 firing，
    /// 只是没人被叫，而这在读数上和「有人接了但没动」同形。
    #[test]
    fn the_stack_convergence_row_names_the_face_that_acts_on_it() {
        let labels = convergence_alert_labels("cogneva", "/manifests", "0123456789abcdef");
        assert_eq!(
            labels
                .get(cog_core::alerts::ALERT_FACE_LABEL)
                .and_then(|v| v.as_str()),
            Some("落地≠生效"),
        );
        // 面是追加的一格，不是整份标签：丢掉 manifest_dir 或 rev，读这一行的人就不知道
        // 是哪一份清单没收敛。
        assert_eq!(
            labels.get("manifest_dir").and_then(|v| v.as_str()),
            Some("/manifests")
        );
        assert_eq!(
            labels.get(crate::LABEL_REV).and_then(|v| v.as_str()),
            Some("0123456789ab")
        );
    }

    fn entry(file: &str, disposition: Disposition, reason: &str) -> DispositionEntry {
        DispositionEntry {
            file: file.into(),
            disposition,
            reason: reason.into(),
        }
    }

    #[test]
    fn a_line_without_a_reason_is_rejected() {
        let err = parse_dispositions("02-networkpolicy.yaml exempt\n").unwrap_err();
        assert!(err.contains("没有理由"), "{err}");
    }

    #[test]
    fn an_unknown_disposition_is_rejected_rather_than_guessed() {
        let err = parse_dispositions("a.yaml sometime # because\n").unwrap_err();
        assert!(err.contains("处置值不认"), "{err}");
    }

    #[test]
    fn the_same_file_twice_is_rejected() {
        let err = parse_dispositions("a.yaml exempt # x\na.yaml backends # y\n").unwrap_err();
        assert!(err.contains("两次"), "{err}");
    }

    #[test]
    fn comments_and_blanks_are_not_entries() {
        let entries =
            parse_dispositions("# 头注释\n\n03-grafana-secrets.yaml exempt # 凭证只在安装期交付\n")
                .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file, "03-grafana-secrets.yaml");
        assert_eq!(entries[0].disposition, Disposition::Exempt);
        assert!(entries[0].reason.starts_with("凭证只在安装期交付"));
    }

    /// 没登记的清单要交付：漏登记的结果是它被应用（看得见），不是被静默跳过。
    #[test]
    fn an_unregistered_manifest_is_delivered_not_skipped() {
        let files = vec!["a.yaml".to_string(), "b.yaml".to_string()];
        let entries = vec![entry("a.yaml", Disposition::Exempt, "因为")];
        let plan = plan_delivery(&files, &entries, true);
        assert_eq!(plan.deliver, vec!["b.yaml"]);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(unregistered_files(&files, &entries), vec!["b.yaml"]);
        assert!(dangling_entries(&entries, &files).is_empty());
    }

    /// 「交付」也要写下来：这张表是整套清单的盘点，加文件必须写它算什么。
    #[test]
    fn a_deliver_row_still_needs_a_reason() {
        let err = parse_dispositions("a.yaml deliver\n").unwrap_err();
        assert!(err.contains("没有理由"), "{err}");
    }

    #[test]
    fn a_deliver_row_delivers_like_an_absent_row_does() {
        let files = vec!["a.yaml".to_string(), "b.yaml".to_string()];
        let entries = vec![entry("a.yaml", Disposition::Deliver, "指标抓取面")];
        let plan = plan_delivery(&files, &entries, false);
        assert_eq!(plan.deliver, vec!["a.yaml", "b.yaml"]);
        assert!(plan.skipped.is_empty());
        // 交付面一样，但盘点面不一样：登记过的那份不再算未登记。
        assert_eq!(unregistered_files(&files, &entries), vec!["b.yaml"]);
    }

    #[test]
    fn the_backends_knob_is_the_same_switch_as_the_installs() {
        let files = vec!["09-clickhouse.yaml".to_string(), "10-loki.yaml".to_string()];
        let entries = vec![
            entry("09-clickhouse.yaml", Disposition::Backends, "明细后端"),
            entry("10-loki.yaml", Disposition::Backends, "日志后端"),
        ];
        assert_eq!(plan_delivery(&files, &entries, true).deliver.len(), 2);
        let off = plan_delivery(&files, &entries, false);
        assert!(off.deliver.is_empty());
        assert_eq!(off.skipped.len(), 2);
        assert!(off.skipped[0].1.contains("关闭"), "{:?}", off.skipped);
    }

    /// 处置表指向一个不存在的文件：豁免是一条陈旧的话，不再是它说的那件事。
    #[test]
    fn an_exemption_for_a_file_that_is_gone_is_reported() {
        let files = vec!["a.yaml".to_string()];
        let entries = vec![entry("renamed.yaml", Disposition::Exempt, "因为")];
        assert_eq!(dangling_entries(&entries, &files), vec!["renamed.yaml"]);
    }

    /// 漂移读数读的是 kubectl 定义的退出码：0 一致、1 有差异。
    #[test]
    fn the_diff_exit_code_is_the_drift_reading() {
        assert_eq!(classify_diff(0, ""), DiffOutcome::Clean);
        assert_eq!(classify_diff(1, ""), DiffOutcome::Differs);
    }

    /// 退出码 1 也带着「跑不起来」回来，那种要读成读不出来而不是漂移——
    /// 否则一次连不上 API 会被报成现场有一处不存在的不一致。
    #[test]
    fn a_failed_diff_is_not_a_drift_reading() {
        let stderr =
            "Error from server (NotFound): the server could not find the requested resource\n";
        assert!(matches!(classify_diff(1, stderr), DiffOutcome::Failed(_)));
        assert!(matches!(
            classify_diff(2, "exit status 2"),
            DiffOutcome::Failed(_)
        ));
    }

    /// 警告不是错误：对象在比较期间被别人改过时 kubectl 会留一行 `W…`，
    /// 而那一轮照样给出了「有差异」这个结果。
    #[test]
    fn a_warning_does_not_turn_a_diff_into_a_failure() {
        let stderr = "W0927 04:58:42.717875 1662054 diff.go:723] Object (apps/v1, Kind=StatefulSet: loki) keeps changing, diffing without lock\n";
        assert_eq!(classify_diff(1, stderr), DiffOutcome::Differs);
    }

    /// 差异行数只数内容行：统一差分自己的 `+++`／`---` 是文件头，不是"改了这
    /// 一行"，算进去会让每个对象每次至少多算两行。
    #[test]
    fn only_content_lines_count_as_a_difference() {
        let diff = concat!(
            "diff -u -N /tmp/LIVE-1/v1.ConfigMap.a /tmp/MERGED-1/v1.ConfigMap.a\n",
            "--- /tmp/LIVE-1/v1.ConfigMap.a\n",
            "+++ /tmp/MERGED-1/v1.ConfigMap.a\n",
            "@@ -1,4 +1,5 @@\n",
            " data:\n",
            "-  old: 1\n",
            "+  new: 1\n",
            "+  more: 2\n",
        );
        assert_eq!(differing_lines(diff), 3);
        assert_eq!(differing_lines(""), 0);
    }

    /// `Differs` 是退出码给的结论，比差分那行文字硬：文字解不出行数时值取 1
    /// 并把"读不出来"报出去。把它写成 0 就是替一次没读成的比较说"两边一致"，
    /// 而那一格正是告警规则靠 `> 0` 判漂移的地方。
    #[test]
    fn a_diff_that_parses_to_nothing_is_not_published_as_agreement() {
        assert_eq!(differs_reading(0), (1, true));
        assert_eq!(differs_reading(3), (3, false));
        assert_eq!(differs_reading(1), (1, false));
    }

    /// 逐文件读数折成漂移清单：干净的一轮必须是空清单——告警靠它解除。
    #[test]
    fn a_clean_round_names_nothing() {
        let readings = vec![
            ("09-clickhouse.yaml".to_string(), DiffOutcome::Clean),
            ("10-loki.yaml".to_string(), DiffOutcome::Clean),
        ];
        assert!(drifted_files(&readings).unwrap().is_empty());
    }

    #[test]
    fn a_drifted_file_is_named_by_file() {
        let readings = vec![
            ("09-clickhouse.yaml".to_string(), DiffOutcome::Differs),
            ("10-loki.yaml".to_string(), DiffOutcome::Clean),
        ];
        assert_eq!(
            drifted_files(&readings).unwrap(),
            vec!["09-clickhouse.yaml"]
        );
    }

    /// 有文件读不出来就整轮没有结论：部分清单不构成「现场与声明是否一致」这个
    /// 结论，而少报一个文件的代价与多报一个不对称。
    #[test]
    fn one_unreadable_file_voids_the_round() {
        let readings = vec![
            ("09-clickhouse.yaml".to_string(), DiffOutcome::Clean),
            (
                "11-podmonitor-redis.yaml".to_string(),
                DiffOutcome::Failed("退出码 2：exit status 2".into()),
            ),
        ];
        let err = drifted_files(&readings).unwrap_err();
        assert!(err.contains("11-podmonitor-redis.yaml"), "{err}");
        assert!(err.contains("退出码 2"), "{err}");
    }

    #[test]
    fn a_missing_namespace_is_named_before_the_wall_of_errors() {
        let stderr = "Error from server (NotFound): error when creating \"STDIN\": namespaces \"monitoring\" not found\n\
                      Error from server (NotFound): error when creating \"STDIN\": namespaces \"monitoring\" not found\n";
        let obstacle = classify_obstacle(stderr, "monitoring").unwrap();
        assert!(matches!(obstacle, Obstacle::NamespaceAbsent(_)));
        assert_eq!(obstacle.severity(), "critical");
        assert!(obstacle.advice().contains("install.sh"));
    }

    #[test]
    fn a_missing_crd_is_its_own_cause() {
        let stderr = "error: resource mapping not found for name: \"cogneva\" namespace: \"monitoring\" from \"STDIN\": no matches for kind \"ServiceMonitor\" in version \"monitoring.coreos.com/v1\"\n";
        let obstacle = classify_obstacle(stderr, "monitoring").unwrap();
        assert!(matches!(obstacle, Obstacle::CrdAbsent(_)));
        assert_ne!(obstacle.severity(), "critical");
    }

    #[test]
    fn a_denied_apply_points_at_the_authority_surface() {
        let stderr = "Error from server (Forbidden): error when creating \"STDIN\": configmaps is forbidden: User \"system:serviceaccount:cogneva:cogneva-evolution\" cannot create resource \"configmaps\" in API namespace \"monitoring\"\n";
        let obstacle = classify_obstacle(stderr, "monitoring").unwrap();
        assert!(matches!(obstacle, Obstacle::Forbidden(_)));
        assert!(obstacle.advice().contains("安装期"));
    }

    /// 修不回去与修回去了是两件事：前者是缺陷，后者是证据。
    #[test]
    fn repaired_drift_and_unfixable_state_are_different_verdicts() {
        let repaired = ConvergenceVerdict::DriftRepaired {
            repaired: vec!["06-grafana-dashboard-configmap.yaml".into()],
        };
        assert!(repaired.is_firing());
        assert!(repaired
            .message("abcdef1234567890", 6)
            .contains("已按 rev abcdef123456 修回"));
        let converged = ConvergenceVerdict::Converged { resources: 7 };
        assert!(!converged.is_firing());
        assert!(converged.message("abcdef1234567890", 6).contains("一致"));
    }

    /// 点名的文件数必须有界：目录里可以有二十个文件都漂移，消息不能长到没人读。
    #[test]
    fn the_message_names_a_bounded_number_of_files() {
        let many: Vec<String> = (0..20).map(|i| format!("{i:02}-manifest.yaml")).collect();
        let msg = ConvergenceVerdict::DriftRepaired { repaired: many }.message("rev", 6);
        assert!(msg.contains("+14 more"), "{msg}");
        assert!(msg.matches("-manifest.yaml").count() == 6, "{msg}");
    }

    /// 没有结论这件事本身要说出来，且要说清是"没读到"而不是"没问题"。
    #[test]
    fn no_evidence_says_so_rather_than_reading_as_healthy() {
        let verdict = ConvergenceVerdict::NoEvidence("bare rev 读不到".into());
        assert!(verdict.is_firing());
        let msg = verdict.message("rev", 6);
        assert!(msg.contains("没有结论"), "{msg}");
        assert!(msg.contains("读不到"), "{msg}");
    }

    #[test]
    fn the_config_bounds_are_checked_not_clamped() {
        let mut cfg = ObservabilityStackConfig::default();
        assert!(validate_config(&cfg).is_ok());
        cfg.interval_secs = 5;
        assert!(validate_config(&cfg).is_err());
        cfg.interval_secs = 300;
        cfg.namespace = "  ".into();
        assert!(validate_config(&cfg).is_err());
    }
}
