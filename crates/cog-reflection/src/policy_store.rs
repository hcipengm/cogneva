//! 产物级进化：策略产物的
//! 「读取 → 评估 → 修改 → 保存新版本 → 热替换」循环。
//!
//! 与源码级进化（change_pipeline）互补：产物级进化不触碰 `.rs`，只演化
//! 策略产物——元策略、阈值、评分权重、规则、配置参数。
//!
//! 设计要点：
//! - **版本链**：每版记录 parent_hash，hash = sha256(parent_hash ‖ canonical payload)，
//!   篡改任何历史版本都会被 `verify_chain` 检出（对应设计"加密、签名"的完整性语义）；
//! - **热替换**：`activate` 切换 active 指针，运行中的系统下次读取即生效；
//! - **统计显著才升级**：`ArtifactEvolution` 复用 4.2 eval_harness 的
//!   two-proportion z-test，仅当候选显著优于当前版本（Adopt）才保存并热替换
//!   （"拒绝无统计显著的改动"）。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use cog_core::{SFError, SFResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

/// 策略产物的一个版本。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyArtifact {
    /// 策略名（如 `orchestrator.retry`、`guardrail.pii`、`meta_learning.mode`）。
    pub name: String,
    pub version: u64,
    /// 策略参数本体（元策略/阈值/权重/规则的 JSON 对象）。
    pub payload: serde_json::Value,
    /// 上一版本 hash；创世版本为 "genesis"。
    pub parent_hash: String,
    /// sha256(parent_hash ‖ canonical payload) —— 版本链完整性锚点。
    pub hash: String,
    pub created_at: DateTime<Utc>,
    /// 修改理由（评估结论/审批记录，审计可解释性）。
    pub reason: String,
}

impl PolicyArtifact {
    fn compute_hash(parent_hash: &str, payload: &serde_json::Value) -> String {
        // canonical：serde_json Map 默认 BTreeMap 序，序列化稳定
        let canonical = serde_json::to_string(payload).unwrap_or_default();
        let mut h = Sha256::new();
        h.update(parent_hash.as_bytes());
        h.update(canonical.as_bytes());
        format!("{:x}", h.finalize())
    }

    fn new(
        name: &str,
        version: u64,
        payload: serde_json::Value,
        parent_hash: &str,
        reason: &str,
    ) -> Self {
        Self {
            name: name.to_string(),
            version,
            hash: Self::compute_hash(parent_hash, &payload),
            payload,
            parent_hash: parent_hash.to_string(),
            created_at: Utc::now(),
            reason: reason.to_string(),
        }
    }
}

/// 版本化策略产物存储：`{dir}/{name}/{version:06}.json` + `{dir}/{name}/ACTIVE`。
#[derive(Debug, Clone)]
pub struct PolicyStore {
    dir: PathBuf,
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 原子写：临时文件 → fsync 文件 → rename → fsync 目录。
///
/// 版本文件与 ACTIVE 都走这里。只 rename 不 fsync 目录时，掉电后目录项可能丢，
/// 于是「版本文件已经落盘」只是进程视角，恢复时可能读到 ACTIVE 指向一个不存在的
/// 版本。文件先持久化，再写 ACTIVE，顺序也不能反。
async fn write_atomic(path: &Path, contents: impl AsRef<[u8]>) -> SFResult<()> {
    let contents = contents.as_ref();
    let dir = path
        .parent()
        .ok_or_else(|| SFError::IO(format!("no parent directory for {}", path.display())))?;
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| SFError::IO(format!("create {}: {e}", dir.display())))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("policy");
    let seq = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{name}.{}.{seq}.tmp", std::process::id()));
    {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| SFError::IO(format!("open {}: {e}", tmp.display())))?;
        file.write_all(contents)
            .await
            .map_err(|e| SFError::IO(format!("write {}: {e}", tmp.display())))?;
        file.sync_all()
            .await
            .map_err(|e| SFError::IO(format!("fsync {}: {e}", tmp.display())))?;
    }
    tokio::fs::rename(&tmp, path).await.map_err(|e| {
        SFError::IO(format!(
            "rename {} -> {}: {e}",
            tmp.display(),
            path.display()
        ))
    })?;
    let dir_file = tokio::fs::File::open(dir)
        .await
        .map_err(|e| SFError::IO(format!("open dir {}: {e}", dir.display())))?;
    dir_file
        .sync_all()
        .await
        .map_err(|e| SFError::IO(format!("fsync dir {}: {e}", dir.display())))?;
    Ok(())
}

impl PolicyStore {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
        }
    }

    fn policy_dir(&self, name: &str) -> PathBuf {
        // 防路径穿越：策略名只允许 [A-Za-z0-9._-]
        let safe: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.dir.join(safe)
    }

    fn version_path(&self, name: &str, version: u64) -> PathBuf {
        self.policy_dir(name).join(format!("{version:06}.json"))
    }

    fn active_path(&self, name: &str) -> PathBuf {
        self.policy_dir(name).join("ACTIVE")
    }

    /// ACTIVE 文件里点名的版本，**不去打开那个版本文件**。
    ///
    /// 与 [`Self::load_active`] 分开是因为两者失败的成因不同：这里只回答「指针说
    /// 的是第几版」，版本文件在不在是下一步的事。两格一起读才分得清「没激活过」
    /// （指针为空）和「指针指着一个不存在的版本」（指针有一版、产物没有）。
    ///
    /// 没有 ACTIVE 文件是 `Ok(None)`；文件在但读不出来、或内容不是版本号，是 `Err`
    /// ——文件存在却给不出一个版本，不是一个空库。
    pub async fn declared_active_version(&self, name: &str) -> SFResult<Option<u64>> {
        let active = self.active_path(name);
        let version_str = match tokio::fs::read_to_string(&active).await {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(SFError::IO(format!("read {}: {e}", active.display())));
            }
        };
        let version = version_str
            .trim()
            .parse()
            .map_err(|e| SFError::Validation(format!("corrupt ACTIVE for {name}: {e}")))?;
        Ok(Some(version))
    }

    /// 读取当前激活版本（热替换后下一次读取即拿到新版本）。
    ///
    /// 「没有 ACTIVE」是 `Ok(None)`；「ACTIVE 指着一个不存在的版本」是 `Err`。
    /// 后者曾经也报 `Ok(None)`，于是掉了一个版本文件的库与一个从没激活过的库在
    /// 读数面上同形：进程安静地回落到默认参数，而读它的人看到的是「还没进化过」。
    /// 指针与产物对不上是库坏了，不是库空的。
    pub async fn load_active(&self, name: &str) -> SFResult<Option<PolicyArtifact>> {
        let Some(version) = self.declared_active_version(name).await? else {
            return Ok(None);
        };
        match self.load_version(name, version).await? {
            Some(artifact) => Ok(Some(artifact)),
            None => Err(SFError::Validation(format!(
                "ACTIVE for {name} names v{version:06}, but that version file is missing"
            ))),
        }
    }

    /// 读一个版本文件：不存在是 `Ok(None)`，存在但读不出来是 `Err`。
    ///
    /// 两种「读不到」曾经都退化成 `Ok(None)`：文件被删了与文件在那儿却没有读权限，
    /// 对一个正要拿它做恢复核对的人来说是两件事——前者是包不完整，后者是这条路
    /// 本身坏了，而后者被读成「这个版本不在包里」时，人修的是包。
    pub async fn load_version(&self, name: &str, version: u64) -> SFResult<Option<PolicyArtifact>> {
        let path = self.version_path(name, version);
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(SFError::IO(format!("read {}: {e}", path.display()))),
        };
        let artifact: PolicyArtifact = serde_json::from_str(&text).map_err(|e| {
            SFError::Validation(format!("corrupt artifact {}: {e}", path.display()))
        })?;
        Ok(Some(artifact))
    }

    pub async fn list_versions(&self, name: &str) -> SFResult<Vec<u64>> {
        let dir = self.policy_dir(name);
        let mut versions = Vec::new();
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            return Ok(versions);
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(stem) = entry
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .map(String::from)
            {
                if let Ok(v) = stem.parse::<u64>() {
                    versions.push(v);
                }
            }
        }
        versions.sort_unstable();
        Ok(versions)
    }

    /// 保存新版本（version = 最新 + 1）并立即热替换为 active。
    pub async fn save_new_version(
        &self,
        name: &str,
        payload: serde_json::Value,
        reason: &str,
    ) -> SFResult<PolicyArtifact> {
        let versions = self.list_versions(name).await?;
        let version = versions.last().map(|v| v + 1).unwrap_or(1);
        let parent_hash = match versions.last() {
            Some(&v) => self
                .load_version(name, v)
                .await?
                .map(|a| a.hash)
                .unwrap_or_else(|| "genesis".into()),
            None => "genesis".into(),
        };
        let artifact = PolicyArtifact::new(name, version, payload, &parent_hash, reason);

        let dir = self.policy_dir(name);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| SFError::IO(format!("create {}: {e}", dir.display())))?;
        let text = serde_json::to_string_pretty(&artifact)
            .map_err(|e| SFError::Validation(format!("serialize artifact: {e}")))?;
        write_atomic(&self.version_path(name, version), text).await?;
        self.activate(name, version).await?;
        Ok(artifact)
    }

    /// 热替换：切换 active 指针到已存在的版本（也用于回滚）。
    pub async fn activate(&self, name: &str, version: u64) -> SFResult<()> {
        if self.load_version(name, version).await?.is_none() {
            return Err(SFError::Validation(format!(
                "cannot activate {name} v{version}: version does not exist"
            )));
        }
        write_atomic(&self.active_path(name), version.to_string()).await
    }

    /// 校验版本链完整性：每版 hash 必须等于 sha256(parent_hash ‖ payload)，
    /// 且 parent_hash 必须等于前一版 hash。
    pub async fn verify_chain(&self, name: &str) -> SFResult<bool> {
        let versions = self.list_versions(name).await?;
        let mut expected_parent = "genesis".to_string();
        for v in versions {
            let Some(a) = self.load_version(name, v).await? else {
                return Ok(false);
            };
            if a.parent_hash != expected_parent {
                return Ok(false);
            }
            if a.hash != PolicyArtifact::compute_hash(&a.parent_hash, &a.payload) {
                return Ok(false);
            }
            expected_parent = a.hash;
        }
        Ok(true)
    }
}

/// 候选策略在评估任务集上的效果样本。
#[derive(Debug, Clone)]
pub struct PolicyCandidate {
    pub payload: serde_json::Value,
    /// 该候选在评估任务集上的成功/失败序列。
    pub outcomes: Vec<bool>,
    pub reason: String,
}

/// 人工门提议的评估结论（z-test 判定 + 人类可读的评估摘要）。
#[derive(Debug, Clone)]
pub struct PolicyProposal {
    pub name: String,
    pub verdict: crate::EvalVerdict,
    /// 两比例 z 统计量；创世提议（无 active 版本）为 0。
    pub z: f64,
    /// 候选相对基线的成功率差（小数，0.18 = +18%）。
    pub uplift: f64,
    /// 形如 "Adopt z=2.31 uplift +18.0%" 的评估结论。
    pub eval_summary: String,
    /// 当前 active 版本号（无 active 为 None，即创世提议）。
    pub current_version: Option<u64>,
}

/// 产物级进化引擎：用统计显著性决定候选策略是否替换当前版本。
pub struct ArtifactEvolution {
    store: PolicyStore,
    /// 已通过 z-test 但等待人工审批的候选（按策略名暂存，同名覆盖）。
    pending: std::sync::Mutex<std::collections::HashMap<String, PolicyCandidate>>,
}

impl ArtifactEvolution {
    pub fn new(store: PolicyStore) -> Self {
        Self {
            store,
            pending: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn store(&self) -> &PolicyStore {
        &self.store
    }

    /// 人工门评估（与源码级 `manual_approve` 对齐）：计算 z-test 判定，
    /// **Adopt 也不立即热替换**，而是把候选暂存，等待 `approve`。
    /// Reject / Inconclusive 不暂存，仅返回判定。
    pub async fn evaluate(
        &self,
        name: &str,
        baseline_outcomes: &[bool],
        candidate: PolicyCandidate,
    ) -> SFResult<PolicyProposal> {
        let active = self.store.load_active(name).await?;
        let current_version = active.as_ref().map(|a| a.version);

        let s_base = baseline_outcomes.iter().filter(|&&x| x).count();
        let s_cand = candidate.outcomes.iter().filter(|&&x| x).count();
        let uplift = s_cand as f64 / candidate.outcomes.len().max(1) as f64
            - s_base as f64 / baseline_outcomes.len().max(1) as f64;

        let (verdict, z) = if active.is_none() {
            // 创世提议：无基线可比，直接 Adopt（与 evolve 的创世语义一致）。
            (crate::EvalVerdict::Adopt, 0.0)
        } else {
            let (z, significant) = crate::eval_harness::two_proportion_z_test(
                s_base,
                baseline_outcomes.len(),
                s_cand,
                candidate.outcomes.len(),
            );
            let verdict = if !significant {
                crate::EvalVerdict::Inconclusive
            } else if z > 0.0 {
                crate::EvalVerdict::Adopt
            } else {
                crate::EvalVerdict::Reject
            };
            (verdict, z)
        };

        let eval_summary = if active.is_none() {
            format!("Adopt (genesis) — {}", candidate.reason)
        } else {
            format!("{:?} z={z:.2} uplift {:+.1}%", verdict, uplift * 100.0)
        };

        if matches!(verdict, crate::EvalVerdict::Adopt) {
            self.pending
                .lock()
                .map_err(|e| SFError::Internal(format!("pending lock: {e}")))?
                .insert(name.to_string(), candidate);
        }

        Ok(PolicyProposal {
            name: name.to_string(),
            verdict,
            z,
            uplift,
            eval_summary,
            current_version,
        })
    }

    /// 审批通过：把暂存候选保存为新版本并热替换。
    /// 无暂存候选（未评估或已被否决）时报错。
    pub async fn approve(&self, name: &str) -> SFResult<PolicyArtifact> {
        let candidate = self
            .pending
            .lock()
            .map_err(|e| SFError::Internal(format!("pending lock: {e}")))?
            .remove(name)
            .ok_or_else(|| {
                SFError::Validation(format!(
                    "no staged policy candidate for {name}; run evaluate first"
                ))
            })?;
        self.store
            .save_new_version(name, candidate.payload, &candidate.reason)
            .await
    }

    /// 评估候选并决定是否升级：
    /// - 无 active 版本：首个候选直接成为创世版本；
    /// - 有 active：候选 outcomes 必须显著优于基线 outcomes（z-test, α=0.05）
    ///   才保存新版本并热替换；否则拒绝，保持当前版本。
    ///
    /// 返回 (新激活的版本, 判定)。
    pub async fn evolve(
        &self,
        name: &str,
        baseline_outcomes: &[bool],
        candidate: &PolicyCandidate,
    ) -> SFResult<(PolicyArtifact, crate::EvalVerdict)> {
        let active = self.store.load_active(name).await?;

        if active.is_none() {
            let artifact = self
                .store
                .save_new_version(name, candidate.payload.clone(), &candidate.reason)
                .await?;
            return Ok((artifact, crate::EvalVerdict::Adopt));
        }

        let s_base = baseline_outcomes.iter().filter(|&&x| x).count();
        let s_cand = candidate.outcomes.iter().filter(|&&x| x).count();
        // z = p_candidate - p_baseline；z > 0 且显著 → 候选显著更优。
        let (z, significant) = crate::eval_harness::two_proportion_z_test(
            s_base,
            baseline_outcomes.len(),
            s_cand,
            candidate.outcomes.len(),
        );
        let verdict = if !significant {
            crate::EvalVerdict::Inconclusive
        } else if z > 0.0 {
            crate::EvalVerdict::Adopt
        } else {
            crate::EvalVerdict::Reject
        };
        match verdict {
            crate::EvalVerdict::Adopt => {
                let uplift = s_cand as f64 / candidate.outcomes.len().max(1) as f64
                    - s_base as f64 / baseline_outcomes.len().max(1) as f64;
                let artifact = self
                    .store
                    .save_new_version(
                        name,
                        candidate.payload.clone(),
                        &format!(
                            "{}（z={z:.2}, uplift={:+.1}%）",
                            candidate.reason,
                            uplift * 100.0
                        ),
                    )
                    .await?;
                Ok((artifact, crate::EvalVerdict::Adopt))
            }
            verdict => {
                let current = active.expect("checked above");
                Ok((current, verdict))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn version_chain_and_hot_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PolicyStore::new(tmp.path());

        let v1 = store
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"margin": 0.05}),
                "init",
            )
            .await
            .unwrap();
        assert_eq!(v1.version, 1);
        assert_eq!(v1.parent_hash, "genesis");

        let v2 = store
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"margin": 0.08}),
                "tighten margin",
            )
            .await
            .unwrap();
        assert_eq!(v2.version, 2);
        assert_eq!(v2.parent_hash, v1.hash);

        // 热替换后 active 即新版本
        let active = store
            .load_active("meta_learning.mode")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.version, 2);
        assert_eq!(active.payload["margin"], serde_json::json!(0.08));

        // 回滚到 v1
        store.activate("meta_learning.mode", 1).await.unwrap();
        let active = store
            .load_active("meta_learning.mode")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.version, 1);

        assert!(store.verify_chain("meta_learning.mode").await.unwrap());
    }

    #[tokio::test]
    async fn atomic_write_leaves_no_tmp_and_active_is_exact() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PolicyStore::new(tmp.path());
        store
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"margin": 0.05}),
                "init",
            )
            .await
            .unwrap();

        let dir = tmp.path().join("meta_learning.mode");
        let names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !names.iter().any(|n| n.ends_with(".tmp")),
            "temporary file left behind: {names:?}"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("ACTIVE")).unwrap().trim(),
            "1"
        );

        let active = store
            .load_active("meta_learning.mode")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.version, 1);
    }

    #[tokio::test]
    async fn chain_detects_tampering() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PolicyStore::new(tmp.path());
        store
            .save_new_version("guardrail.pii", serde_json::json!({"strict": true}), "init")
            .await
            .unwrap();
        store
            .save_new_version(
                "guardrail.pii",
                serde_json::json!({"strict": false}),
                "relax",
            )
            .await
            .unwrap();

        // 篡改 v1 的 payload
        let v1_path = tmp.path().join("guardrail.pii").join("000001.json");
        let mut v1: PolicyArtifact =
            serde_json::from_str(&std::fs::read_to_string(&v1_path).unwrap()).unwrap();
        v1.payload = serde_json::json!({"strict": false, "backdoor": true});
        std::fs::write(&v1_path, serde_json::to_string_pretty(&v1).unwrap()).unwrap();

        assert!(!store.verify_chain("guardrail.pii").await.unwrap());
    }

    #[tokio::test]
    async fn activate_rejects_missing_version() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PolicyStore::new(tmp.path());
        let err = store.activate("nope", 3).await.unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    /// 「没有 ACTIVE」与「ACTIVE 指着一个不存在的版本」是两种库：前者是还没激活过，
    /// 后者是坏了。都返回 `Ok(None)` 的话，后者会被读成前者，进程安静地跑在默认参数上。
    #[tokio::test]
    async fn an_active_pointer_to_a_missing_version_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PolicyStore::new(tmp.path());

        assert!(store
            .declared_active_version("guard.pii")
            .await
            .unwrap()
            .is_none());
        assert!(store.load_active("guard.pii").await.unwrap().is_none());

        let dir = tmp.path().join("guard.pii");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ACTIVE"), "7\n").unwrap();
        assert_eq!(
            store.declared_active_version("guard.pii").await.unwrap(),
            Some(7),
            "指针自己说的是哪一版要读得出来，判词才能指出缺的是哪一份"
        );
        let err = store.load_active("guard.pii").await.unwrap_err();
        assert!(
            err.to_string().contains("v000007") && err.to_string().contains("missing"),
            "错误要点名指针上的版本：{err}"
        );
    }

    /// 指针在、版本文件也在、但内容不是产物：这是坏库，不是空库。
    #[tokio::test]
    async fn a_corrupt_active_pointer_is_an_error_not_an_empty_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PolicyStore::new(tmp.path());
        let dir = tmp.path().join("guard.pii");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ACTIVE"), "not-a-version").unwrap();
        assert!(store.declared_active_version("guard.pii").await.is_err());
        assert!(store.load_active("guard.pii").await.is_err());
    }

    #[tokio::test]
    async fn evolve_adopts_only_statistically_significant() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PolicyStore::new(tmp.path());
        let evo = ArtifactEvolution::new(store);

        // 创世版本
        let (genesis, verdict) = evo
            .evolve(
                "routing.llm",
                &[],
                &PolicyCandidate {
                    payload: serde_json::json!({"model": "baseline"}),
                    outcomes: vec![],
                    reason: "initial".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(verdict, crate::EvalVerdict::Adopt);
        assert_eq!(genesis.version, 1);

        // 样本不足 → Inconclusive，不升级
        let baseline = vec![true; 10];
        let weak = PolicyCandidate {
            payload: serde_json::json!({"model": "candidate-weak"}),
            outcomes: vec![true; 8],
            reason: "marginal".into(),
        };
        let (current, verdict) = evo.evolve("routing.llm", &baseline, &weak).await.unwrap();
        assert_ne!(verdict, crate::EvalVerdict::Adopt);
        assert_eq!(current.version, 1, "不显著不得保存新版本");

        // 显著更优 → Adopt，热替换 v2
        let baseline: Vec<bool> = std::iter::repeat_n(true, 20)
            .chain(std::iter::repeat_n(false, 20))
            .collect();
        let strong = PolicyCandidate {
            payload: serde_json::json!({"model": "candidate-strong"}),
            outcomes: vec![true; 40],
            reason: "clear win".into(),
        };
        let (new, verdict) = evo.evolve("routing.llm", &baseline, &strong).await.unwrap();
        assert_eq!(verdict, crate::EvalVerdict::Adopt);
        assert_eq!(new.version, 2);
        assert!(new.reason.contains("z="));

        let active = evo
            .store()
            .load_active("routing.llm")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            active.payload["model"],
            serde_json::json!("candidate-strong")
        );
    }

    #[test]
    fn policy_name_sanitized() {
        let store = PolicyStore::new("/tmp/x");
        assert_eq!(
            store.policy_dir("../../etc/passwd"),
            Path::new("/tmp/x/.._.._etc_passwd")
        );
    }

    #[tokio::test]
    async fn evaluate_stages_and_approve_activates() {
        let tmp = tempfile::tempdir().unwrap();
        let evo = ArtifactEvolution::new(PolicyStore::new(tmp.path()));

        // 创世提议：Adopt 但不激活，approve 后才落 v1
        let proposal = evo
            .evaluate(
                "routing.llm",
                &[],
                PolicyCandidate {
                    payload: serde_json::json!({"model": "baseline"}),
                    outcomes: vec![],
                    reason: "initial".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(proposal.verdict, crate::EvalVerdict::Adopt);
        assert_eq!(proposal.current_version, None);
        assert!(
            evo.store()
                .load_active("routing.llm")
                .await
                .unwrap()
                .is_none(),
            "审批前不得热替换"
        );
        let v1 = evo.approve("routing.llm").await.unwrap();
        assert_eq!(v1.version, 1);

        // 显著更优候选：evaluate 暂存，active 仍是 v1；approve 后 v2 热替换
        let baseline: Vec<bool> = std::iter::repeat_n(true, 20)
            .chain(std::iter::repeat_n(false, 20))
            .collect();
        let proposal = evo
            .evaluate(
                "routing.llm",
                &baseline,
                PolicyCandidate {
                    payload: serde_json::json!({"model": "candidate-strong"}),
                    outcomes: vec![true; 40],
                    reason: "clear win".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(proposal.verdict, crate::EvalVerdict::Adopt);
        assert!(proposal.eval_summary.contains("z="));
        assert_eq!(
            evo.store()
                .load_active("routing.llm")
                .await
                .unwrap()
                .unwrap()
                .version,
            1,
            "人工门：审批前 active 不变"
        );
        let v2 = evo.approve("routing.llm").await.unwrap();
        assert_eq!(v2.version, 2);

        // 弱候选：Inconclusive 不暂存，approve 报错
        let weak = evo
            .evaluate(
                "routing.llm",
                &[true; 10],
                PolicyCandidate {
                    payload: serde_json::json!({"model": "candidate-weak"}),
                    outcomes: vec![true; 8],
                    reason: "marginal".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(weak.verdict, crate::EvalVerdict::Inconclusive);
        let err = evo.approve("routing.llm").await.unwrap_err();
        assert!(err.to_string().contains("no staged policy candidate"));
    }
}
