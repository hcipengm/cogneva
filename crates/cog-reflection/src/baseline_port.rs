//! 基线移植控制器（规则3：旧变更跨基线自治移植）。
//!
//! 公版发布新 release tag（`v<x.y.z>`）后，实例历代晋级变更（`promote/*`
//! tag）不能随 `reset --hard` 丢成干净上游树——它们必须移植到新基线。
//! 本模块负责移植前的只读探测与计划：
//!
//! ```text
//! 发现新上游 release tag（比当前运行版本新）
//!   → 盘点全部 promote/<change_id> tag（= 该实例历代晋级变更）
//!   → 吸收检测：git cherry 按 patch-id 判定变更是否已被新基线包含
//!       （别的实例回流或官方自行修复）→ 已吸收跳过，不重复移植
//!   → 产出移植计划：Absorbed 跳过 / Pending 待移植
//! ```
//!
//! 移植执行（cherry-pick/apply + LLM 冲突解决 + 质量门）在后续阶段接入；
//! 本阶段只做只读探测，绝不改动工作区或引用。

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use cog_core::{SFError, SFResult};
use tracing::{debug, info, warn};

use crate::gitops_puller::parse_tag_message;

/// 一条历代晋级变更（从 `promote/<change_id>` annotated tag 解析）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotedChange {
    pub change_id: String,
    pub tag: String,
    /// tag 解引用到的 commit（annotated tag 的 *objectname）。
    pub commit: String,
    pub level: Option<String>,
    pub eval_summary: Option<String>,
}

/// 吸收检测结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsorptionStatus {
    /// 新基线已包含等价变更（patch-id 命中，或该提交已是新基线祖先）：
    /// 移植时跳过，不重复制造冲突。
    Absorbed,
    /// 新基线未包含：需要移植。
    Pending,
}

/// 移植计划中的单条：一条晋级变更 + 其吸收结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortPlanItem {
    pub change: PromotedChange,
    pub status: AbsorptionStatus,
}

/// 一次基线移植的完整计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortPlan {
    /// 新上游 release tag，如 `v0.5.8`。
    pub new_tag: String,
    /// 当前运行版本对应的 tag，如 `v0.5.7`。
    pub current_tag: String,
    pub items: Vec<PortPlanItem>,
}

impl PortPlan {
    pub fn pending(&self) -> impl Iterator<Item = &PortPlanItem> {
        self.items
            .iter()
            .filter(|i| i.status == AbsorptionStatus::Pending)
    }

    pub fn absorbed(&self) -> impl Iterator<Item = &PortPlanItem> {
        self.items
            .iter()
            .filter(|i| i.status == AbsorptionStatus::Absorbed)
    }
}

pub struct BaselinePorter {
    /// 沙盒源码工作仓库（bare 的克隆，同时持有上游 v* tag 与 promote/* tag）。
    repo_dir: PathBuf,
}

impl BaselinePorter {
    pub fn new(repo_dir: impl Into<PathBuf>) -> Self {
        Self {
            repo_dir: repo_dir.into(),
        }
    }

    async fn git(&self, args: &[&str]) -> SFResult<String> {
        let cmdline = format!("git {}", args.join(" "));
        let fut = tokio::process::Command::new("git")
            .args(args)
            .current_dir(&self.repo_dir)
            .kill_on_drop(true)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(120), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after 120s")))?
            .map_err(|e| SFError::IO(format!("failed to run git: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("{cmdline} failed: {stderr}")));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// 版本比较失败（ref 不存在等）按"无"处理，不刷成错误日志。
    async fn git_opt(&self, args: &[&str]) -> SFResult<Option<String>> {
        match self.git(args).await {
            Ok(s) if s.is_empty() => Ok(None),
            Ok(s) => Ok(Some(s)),
            Err(e) => {
                debug!(?args, error = %e, "git probe failed; treating as absent");
                Ok(None)
            }
        }
    }

    /// 仓库里最新的上游 release tag（`v<x.y.z>`，semver 取最大）；无 tag 返回 None。
    pub async fn latest_release_tag(&self) -> SFResult<Option<String>> {
        let out = self.git(&["tag", "--list", "v*"]).await?;
        let mut best: Option<((u64, u64, u64), String)> = None;
        for tag in out.lines().map(str::trim).filter(|t| !t.is_empty()) {
            if let Some(v) = parse_release_version(tag) {
                if best.as_ref().is_none_or(|(bv, _)| v > *bv) {
                    best = Some((v, tag.to_string()));
                }
            }
        }
        Ok(best.map(|(_, tag)| tag))
    }

    /// 盘点全部晋级变更：`promote/*` tag → commit + tag message 审计元数据。
    pub async fn list_promoted(&self) -> SFResult<Vec<PromotedChange>> {
        // 单行输出：`<tag> <tag-object> <peeled-commit>`。annotated tag 的
        // 解引用 commit 在 %(*objectname)；轻量 tag 该字段为空，回退 objectname。
        let out = self
            .git(&[
                "for-each-ref",
                "--format=%(refname:short) %(objectname) %(*objectname)",
                "refs/tags/promote/",
            ])
            .await?;
        let mut changes = Vec::new();
        for line in out.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }
            let tag = parts[0].to_string();
            let commit = match parts.get(2).copied() {
                Some(peeled) if !peeled.is_empty() => peeled.to_string(),
                _ => parts.get(1).copied().unwrap_or_default().to_string(),
            };
            if commit.is_empty() {
                warn!(%tag, "promote tag has no resolvable commit; skipping");
                continue;
            }
            let message = self
                .git(&["tag", "-l", "--format=%(contents)", &tag])
                .await?;
            let (change_id, level, eval_summary) = parse_tag_message(&message);
            let change_id = change_id
                .unwrap_or_else(|| tag.trim_start_matches("promote/").to_string());
            changes.push(PromotedChange {
                change_id,
                tag,
                commit,
                level,
                eval_summary,
            });
        }
        // for-each-ref 默认字典序，晋级历史线性，字典序即演进顺序。
        changes.sort_by(|a, b| a.tag.cmp(&b.tag));
        Ok(changes)
    }

    /// 含全部晋级提交的 tip：优先 evolution-release 分支远端引用，
    /// 退化为最后一个晋级 tag（晋级历史线性，新 tag 是旧 tag 的后代）。
    async fn evolution_tip(&self, changes: &[PromotedChange]) -> SFResult<Option<String>> {
        if let Some(tip) = self
            .git_opt(&["rev-parse", "--verify", "-q", "refs/remotes/local/evolution-release"])
            .await?
        {
            return Ok(Some(tip));
        }
        Ok(changes.last().map(|c| c.commit.clone()))
    }

    /// `git cherry <new_tag> <head>`：输出新基线..tip 范围每个提交一行，
    /// `- <sha>` = patch-id 命中（新基线已有等价 patch，已吸收），
    /// `+ <sha>` = 缺失（待移植）。返回 (absorbed, pending) 两个 sha 集合。
    async fn cherry_sets(&self, new_tag: &str, head: &str) -> SFResult<(HashSet<String>, HashSet<String>)> {
        let out = self.git(&["cherry", new_tag, head]).await?;
        let mut absorbed = HashSet::new();
        let mut pending = HashSet::new();
        for line in out.lines() {
            let mut it = line.split_whitespace();
            match (it.next(), it.next()) {
                (Some("-"), Some(sha)) => {
                    absorbed.insert(sha.to_string());
                }
                (Some("+"), Some(sha)) => {
                    pending.insert(sha.to_string());
                }
                _ => {}
            }
        }
        Ok((absorbed, pending))
    }

    /// 判定单条变更相对新基线的吸收状态。
    async fn classify(
        &self,
        change: &PromotedChange,
        new_tag: &str,
        absorbed: &HashSet<String>,
        pending: &HashSet<String>,
    ) -> SFResult<AbsorptionStatus> {
        if absorbed.contains(&change.commit) {
            return Ok(AbsorptionStatus::Absorbed);
        }
        if pending.contains(&change.commit) {
            return Ok(AbsorptionStatus::Pending);
        }
        // 不在 cherry 输出里：不在 new_tag..tip 范围内。正常情况是该提交
        // 已是新基线祖先（新基线直接包含了它）→ 已吸收。
        match self
            .git(&[
                "merge-base",
                "--is-ancestor",
                &change.commit,
                new_tag,
            ])
            .await
        {
            Ok(_) => Ok(AbsorptionStatus::Absorbed),
            Err(_) => {
                // 既不在范围也不是祖先：tip 没盖住它（非线性历史）。
                // 以该提交自身为 head 单独跑一次 cherry 判定。
                let (abs, pen) = self.cherry_sets(new_tag, &change.commit).await?;
                if abs.contains(&change.commit) {
                    Ok(AbsorptionStatus::Absorbed)
                } else if pen.contains(&change.commit) {
                    Ok(AbsorptionStatus::Pending)
                } else {
                    // 仍无结论：保守按待移植处理，交给移植阶段的质量门兜底，
                    // 绝不静默丢弃一条晋级变更。
                    warn!(
                        change_id = %change.change_id,
                        commit = %change.commit,
                        "absorption detection inconclusive; treating as pending"
                    );
                    Ok(AbsorptionStatus::Pending)
                }
            }
        }
    }

    /// 产出移植计划。当前版本（如 `0.5.7`，不带 v 前缀）不低于最新 release
    /// tag、或仓库里没有 release tag 时返回 None（无新基线可移植）。
    pub async fn plan(&self, current_version: &str) -> SFResult<Option<PortPlan>> {
        let Some(new_tag) = self.latest_release_tag().await? else {
            return Ok(None);
        };
        let current = parse_release_version(&format!("v{current_version}"));
        let latest = parse_release_version(&new_tag).expect("tag came from latest_release_tag");
        let newer = match current {
            Some(c) => latest > c,
            None => true,
        };
        if !newer {
            return Ok(None);
        }

        let changes = self.list_promoted().await?;
        let current_tag = format!("v{current_version}");
        if changes.is_empty() {
            info!(%new_tag, "new upstream release found; instance has zero promoted changes");
            return Ok(Some(PortPlan {
                new_tag,
                current_tag,
                items: Vec::new(),
            }));
        }

        let Some(tip) = self.evolution_tip(&changes).await? else {
            return Ok(Some(PortPlan {
                new_tag,
                current_tag,
                items: Vec::new(),
            }));
        };
        let (absorbed, pending) = self.cherry_sets(&new_tag, &tip).await?;

        let mut items = Vec::with_capacity(changes.len());
        for change in changes {
            let status = self.classify(&change, &new_tag, &absorbed, &pending).await?;
            info!(
                change_id = %change.change_id,
                tag = %change.tag,
                ?status,
                "baseline port absorption check"
            );
            items.push(PortPlanItem { change, status });
        }

        let n_absorbed = items
            .iter()
            .filter(|i| i.status == AbsorptionStatus::Absorbed)
            .count();
        let n_pending = items.len() - n_absorbed;
        info!(
            %new_tag,
            total = items.len(),
            absorbed = n_absorbed,
            pending = n_pending,
            "baseline port plan ready"
        );
        Ok(Some(PortPlan {
            new_tag,
            current_tag,
            items,
        }))
    }
}

/// 解析 `v<major>.<minor>.<patch>` 形式的 release tag；带预发布/构建后缀
/// （`-rc.1`、`+build`）时只比数值三元组。非 release tag 返回 None。
fn parse_release_version(tag: &str) -> Option<(u64, u64, u64)> {
    let core = tag.strip_prefix('v')?;
    let core = core.split('+').next().unwrap();
    let core = core.split('-').next().unwrap();
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    async fn git(dir: &Path, args: &[&str]) -> String {
        let output = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    /// 初始仓库：一个提交 + v0.5.7 tag，模拟私版 seed 基线。
    async fn setup_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init"]).await;
        git(dir.path(), &["config", "user.email", "test@test.com"]).await;
        git(dir.path(), &["config", "user.name", "Test"]).await;
        tokio::fs::write(dir.path().join("lib.rs"), "fn v1() {}\n")
            .await
            .unwrap();
        git(dir.path(), &["add", "."]).await;
        git(dir.path(), &["commit", "-m", "initial"]).await;
        git(dir.path(), &["tag", "v0.5.7"]).await;
        dir
    }

    /// 在 v0.5.7 之上做一条晋级变更并打 promote tag。
    async fn make_promoted(dir: &Path, change_id: &str, file: &str, body: &str) {
        tokio::fs::write(dir.join(file), body).await.unwrap();
        git(dir, &["add", "."]).await;
        git(dir, &["commit", "-m", &format!("change {change_id}")]).await;
        let msg = format!(
            "change_id={change_id}\nlevel=l1_rollout\neval=Adopt z=2.3"
        );
        git(dir, &["tag", "-a", "-m", &msg, &format!("promote/{change_id}")]).await;
    }

    /// 从指定 tag 起 detached 建新提交并打新 release tag（模拟上游演进）。
    async fn upstream_release(dir: &Path, base: &str, tag: &str, file: &str, body: &str) {
        git(dir, &["checkout", "-q", base]).await;
        tokio::fs::write(dir.join(file), body).await.unwrap();
        git(dir, &["add", "."]).await;
        git(dir, &["commit", "-m", &format!("upstream {tag}")]).await;
        git(dir, &["tag", tag]).await;
    }

    #[tokio::test]
    async fn no_new_tag_returns_none() {
        let dir = setup_repo().await;
        let porter = BaselinePorter::new(dir.path());
        // 当前版本已是最新。
        let plan = porter.plan("0.5.7").await.unwrap();
        assert!(plan.is_none());
    }

    #[tokio::test]
    async fn new_tag_no_promoted_changes_yields_empty_plan() {
        let dir = setup_repo().await;
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "upstream.rs",
            "fn upstream() {}\n",
        )
        .await;
        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.new_tag, "v0.5.8");
        assert_eq!(plan.current_tag, "v0.5.7");
        assert!(plan.items.is_empty());
    }

    #[tokio::test]
    async fn promoted_change_absorbed_when_upstream_has_same_patch() {
        let dir = setup_repo().await;
        // 私版晋级一条变更。
        make_promoted(
            dir.path(),
            "chg-1",
            "feature.rs",
            "fn promoted() {}\n",
        )
        .await;
        // 上游独立合入了同一个 patch（cherry-pick 产生不同 commit、相同 patch-id）。
        git(dir.path(), &["checkout", "-q", "v0.5.7"]).await;
        git(dir.path(), &["cherry-pick", "promote/chg-1"]).await;
        git(dir.path(), &["tag", "v0.5.8"]).await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].status, AbsorptionStatus::Absorbed);
        assert_eq!(plan.items[0].change.change_id, "chg-1");
        assert_eq!(plan.pending().count(), 0);
    }

    #[tokio::test]
    async fn promoted_change_pending_when_upstream_lacks_it() {
        let dir = setup_repo().await;
        make_promoted(
            dir.path(),
            "chg-1",
            "feature.rs",
            "fn promoted() {}\n",
        )
        .await;
        // 上游只做了无关改动。
        upstream_release(
            dir.path(),
            "v0.5.7",
            "v0.5.8",
            "other.rs",
            "fn other() {}\n",
        )
        .await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].status, AbsorptionStatus::Pending);
        assert_eq!(plan.pending().count(), 1);
    }

    #[tokio::test]
    async fn promoted_change_ancestor_of_new_tag_is_absorbed() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-1", "feature.rs", "fn promoted() {}\n").await;
        // 新 release tag 直接打在晋级提交之上（新基线已包含该历史）。
        git(dir.path(), &["tag", "v0.6.0", "promote/chg-1"]).await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items[0].status, AbsorptionStatus::Absorbed);
    }

    #[tokio::test]
    async fn list_promoted_parses_tag_metadata() {
        let dir = setup_repo().await;
        make_promoted(dir.path(), "chg-7", "feature.rs", "fn promoted() {}\n").await;
        let porter = BaselinePorter::new(dir.path());
        let changes = porter.list_promoted().await.unwrap();
        assert_eq!(changes.len(), 1);
        let c = &changes[0];
        assert_eq!(c.change_id, "chg-7");
        assert_eq!(c.tag, "promote/chg-7");
        assert_eq!(c.level.as_deref(), Some("l1_rollout"));
        assert_eq!(c.eval_summary.as_deref(), Some("Adopt z=2.3"));
        assert!(!c.commit.is_empty());
    }

    #[tokio::test]
    async fn mixed_absorbed_and_pending_classified_independently() {
        let dir = setup_repo().await;
        // 两条私版晋级。
        make_promoted(dir.path(), "chg-a", "a.rs", "fn a() {}\n").await;
        make_promoted(dir.path(), "chg-b", "b.rs", "fn b() {}\n").await;
        // 上游只吸收了 chg-a（cherry-pick），chg-b 上游没有。
        git(dir.path(), &["checkout", "-q", "v0.5.7"]).await;
        git(dir.path(), &["cherry-pick", "promote/chg-a"]).await;
        tokio::fs::write(dir.path().join("upstream.rs"), "fn up() {}\n")
            .await
            .unwrap();
        git(dir.path(), &["add", "."]).await;
        git(dir.path(), &["commit", "-m", "upstream v0.5.8"]).await;
        git(dir.path(), &["tag", "v0.5.8"]).await;

        let porter = BaselinePorter::new(dir.path());
        let plan = porter.plan("0.5.7").await.unwrap().expect("plan expected");
        assert_eq!(plan.items.len(), 2);
        let by_id: std::collections::HashMap<&str, AbsorptionStatus> = plan
            .items
            .iter()
            .map(|i| (i.change.change_id.as_str(), i.status))
            .collect();
        assert_eq!(by_id["chg-a"], AbsorptionStatus::Absorbed);
        assert_eq!(by_id["chg-b"], AbsorptionStatus::Pending);
    }

    #[test]
    fn parse_release_version_variants() {
        assert_eq!(parse_release_version("v0.5.7"), Some((0, 5, 7)));
        assert_eq!(parse_release_version("v1.2.3-rc.1"), Some((1, 2, 3)));
        assert_eq!(parse_release_version("v10.0.1+build.42"), Some((10, 0, 1)));
        assert_eq!(parse_release_version("v1.2"), None);
        assert_eq!(parse_release_version("promote/chg-1"), None);
        assert_eq!(parse_release_version("not-a-tag"), None);
    }
}
