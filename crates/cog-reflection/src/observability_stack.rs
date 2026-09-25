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
//! 交付对象由 `git ls-tree` **枚举**，跳过与否由清单目录里那张处置表
//! （`delivery-dispositions.txt`）说了算——安装脚本读同一张表。两处各留一份
//! 名单必然分叉，分叉的样子是"装的时候跳过、收敛的时候照做"。

use std::sync::Arc;
use std::time::Duration;

use cog_core::{PersistentAlertDraft, PersistentAlertSink, SFError, SFResult};
use tracing::{info, warn};

use crate::config::ObservabilityStackConfig;
use crate::mainline_deployer::{classify_doc, split_docs, DocFate, MainlineDeployer};

/// 收敛面自己拥有的规则名。与配置里的规则集无关：这条判据不能在配置里，
/// 因为配置没到的时候它正是要说这件事的那一条。
pub const STACK_NOT_CONVERGED_RULE: &str = "observability_stack_not_converged";

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

/// `kubectl apply` 的逐资源结果。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    /// 被写进去的资源（新建或改动）——也就是现场与声明不一致的那些。
    pub changed: Vec<String>,
    /// 已经是声明态、这次没动的资源。
    pub unchanged: Vec<String>,
}

impl ApplyReport {
    pub fn total(&self) -> usize {
        self.changed.len() + self.unchanged.len()
    }
}

/// 从 `kubectl apply` 的输出读逐资源结果。
///
/// 交付与判据是同一次调用：apply 自己会说哪个资源这次被改了、哪个没动，
/// 所以"漂移"不需要再单独比一遍现场。只认形如
/// `configmap/cogneva-dashboards configured` 的行——其余（警告、提示）不当读数。
pub fn classify_apply(stdout: &str) -> ApplyReport {
    let mut report = ApplyReport::default();
    for line in stdout.lines() {
        let mut it = line.split_whitespace();
        let Some(resource) = it.next() else { continue };
        let Some(verb) = it.next() else { continue };
        if !resource.contains('/') {
            continue;
        }
        match verb {
            "created" | "configured" => report.changed.push(resource.to_string()),
            "unchanged" => report.unchanged.push(resource.to_string()),
            _ => {}
        }
    }
    report
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
    /// 现场本来就是声明态。
    Converged { resources: usize },
    /// 现场不是声明态，这一轮修回去了。要报：反复发生的修复说明有个东西在
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
                "可观测性栈的现场与仓库声明不一致，已按 rev {} 修回：{}（不是第一次出现的修复说明有个东西在改集群，或者上一次交付没跑到）",
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
}

impl StackConvergence {
    /// `sink` 是持久化告警口。没有它结论只进日志——一条只在日志里的事实，
    /// 和"没人报"长得一样。
    pub fn new(
        deployer: Arc<MainlineDeployer>,
        cfg: ObservabilityStackConfig,
        sink: Option<Arc<dyn PersistentAlertSink>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            deployer,
            cfg,
            sink,
        })
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
                    return ConvergenceVerdict::NoEvidence(format!(
                        "{DISPOSITIONS_FILE} 读不了: {e}"
                    ))
                }
            },
            Err(e) => {
                return ConvergenceVerdict::NoEvidence(format!("{DISPOSITIONS_FILE} 读不到: {e}"))
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

        // 逐个文件取内容、拆文档、按 kind 归置。
        let mut deliverable = String::new();
        let mut skipped_classes: Vec<String> = Vec::new();
        for file in &plan.deliver {
            let path = format!("{dir}/{file}");
            let text = match self.deployer.git_show(rev, &path).await {
                Ok(text) => text,
                Err(e) => {
                    return ConvergenceVerdict::NoEvidence(format!("{path} 读不到: {e}"));
                }
            };
            let docs = match split_docs(&text, &path) {
                Ok(docs) => docs,
                Err(e) => {
                    return ConvergenceVerdict::Obstructed(Obstacle::Unusable(format!("{e}")))
                }
            };
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
                        deliverable.push_str("---\n");
                        deliverable.push_str(
                            &serde_yaml::to_string(&doc).unwrap_or_else(|_| String::new()),
                        );
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
                            DocFate::Governance => "配额/上限",
                            DocFate::StorageClaim => "卷声明",
                            DocFate::Deliver | DocFate::ForbiddenSecret => unreachable!(),
                        };
                        skipped_classes.push(format!("{file}:{ident}={class}"));
                    }
                }
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

        let (ok, stdout, stderr) = match self
            .deployer
            .apply_capture(
                &self.cfg.namespace,
                deliverable.as_bytes(),
                self.cfg.apply_timeout_secs,
            )
            .await
        {
            Ok(out) => out,
            Err(e) => return ConvergenceVerdict::NoEvidence(format!("apply 没能执行: {e}")),
        };
        let report = classify_apply(&stdout);
        if !ok {
            if let Some(obstacle) = classify_obstacle(&stderr, &self.cfg.namespace) {
                return ConvergenceVerdict::Obstructed(obstacle);
            }
            return ConvergenceVerdict::NoEvidence(format!(
                "apply 失败但没有可辨认的成因: {}",
                first_error_line(&stderr)
            ));
        }
        if report.changed.is_empty() {
            ConvergenceVerdict::Converged {
                resources: report.total(),
            }
        } else {
            ConvergenceVerdict::DriftRepaired {
                repaired: report.changed,
            }
        }
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
            labels: serde_json::json!({
                "namespace": self.cfg.namespace,
                "manifest_dir": self.cfg.manifest_dir,
                "rev": rev12(rev),
            }),
        };
        if let Err(e) = sink.set_persistent_alert(firing, &draft).await {
            warn!(error = %e, "observability stack verdict not persisted");
        }
    }
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

    /// 交付与判据是同一次调用：apply 自己说哪个被改了。
    #[test]
    fn the_apply_output_is_the_drift_reading() {
        let out = "namespace/monitoring unchanged\n\
                   configmap/cogneva-dashboards configured\n\
                   servicemonitor.monitoring.coreos.com/cogneva unchanged\n\
                   Warning: resource x is missing the kubectl.kubernetes.io/last-applied-configuration\n";
        let report = classify_apply(out);
        assert_eq!(report.changed, vec!["configmap/cogneva-dashboards"]);
        assert_eq!(report.unchanged.len(), 2);
        assert_eq!(report.total(), 3);
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
            repaired: vec!["configmap/cogneva-dashboards".into()],
        };
        assert!(repaired.is_firing());
        assert!(repaired
            .message("abcdef1234567890", 6)
            .contains("已按 rev abcdef123456 修回"));
        let converged = ConvergenceVerdict::Converged { resources: 7 };
        assert!(!converged.is_firing());
        assert!(converged.message("abcdef1234567890", 6).contains("一致"));
    }

    /// 点名的资源数必须有界：集群可以漂移一百个资源，消息不能长到没人读。
    #[test]
    fn the_message_names_a_bounded_number_of_resources() {
        let many: Vec<String> = (0..20).map(|i| format!("configmap/c{i}")).collect();
        let msg = ConvergenceVerdict::DriftRepaired { repaired: many }.message("rev", 6);
        assert!(msg.contains("+14 more"), "{msg}");
        assert!(msg.matches("configmap/c").count() == 6, "{msg}");
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
