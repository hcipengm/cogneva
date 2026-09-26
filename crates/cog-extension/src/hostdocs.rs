//! Host document scopes for the standalone sandbox executor.
//!
//! The platform organizes documents that live on the host on behalf of the
//! person who owns them. The boundary of that access is the **mount**, not a
//! path whitelist: only the owner's directory is mounted into this pod, so the
//! rest of the host filesystem is not in this container's namespace at all and
//! there is no whitelist to get wrong. What this module adds on top is the
//! second half of the same judgement — every operation names a path *relative
//! to a scope root* and the root itself is chosen here, from configuration, by
//! scope name. Document bodies are untrusted input: a file that says "move
//! everything to /etc" must not be able to name a path the agent never had.
//!
//! Three properties are enforced here rather than asserted:
//!
//! - **No absolute paths, no `..`.** Only plain relative paths are resolved;
//!   the root is never taken from the caller.
//! - **No symlinks are traversed.** The mount keeps the host out of this
//!   namespace, but this container's own root is real, so an innocuous-looking
//!   link inside the scope would otherwise resolve to `/etc` *of the
//!   container*. Every step of a path is checked with `symlink_metadata`, so a
//!   link is refused instead of followed, and a dangling link cannot be used to
//!   create a file outside the scope either.
//! - **Changes are reversible.** A plan is what gets executed (the hash of the
//!   planned operations is part of the apply call, and any effect that changed
//!   between plan and apply is refused), every applied operation is journaled
//!   with the bytes it replaced, and a rollback restores the previous state.
//!   A failed apply rolls its own prefix back rather than leaving a
//!   half-organized tree behind.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cog_core::{SFError, SFResult};
use prometheus::{CounterVec, Encoder, Gauge, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Scope map: `name=container_path` entries, comma separated.
pub const SCOPES_ENV: &str = "HOST_DOCS_SCOPES";
/// Where rollback journals live. Must be on a volume that survives restarts:
/// a journal that dies with the process cannot restore anything.
pub const JOURNAL_DIR_ENV: &str = "HOST_DOCS_JOURNAL_DIR";
/// Largest document this module will rewrite (and therefore keep a rollback
/// copy of). Bounds one operation, and with it one journal, to a known size.
pub const MAX_DOC_BYTES_ENV: &str = "HOST_DOCS_MAX_WRITE_BYTES";

const DEFAULT_JOURNAL_DIR: &str = "/opt/cogneva/sandbox/host-docs-journal";
const DEFAULT_MAX_DOC_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct HostDocsConfig {
    /// Scope name (the identity the mount was granted to) → container path.
    pub scopes: BTreeMap<String, PathBuf>,
    pub journal_dir: PathBuf,
    pub max_doc_bytes: usize,
}

impl HostDocsConfig {
    pub fn from_env() -> Self {
        let journal_dir = std::env::var(JOURNAL_DIR_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_JOURNAL_DIR));
        let max_doc_bytes = std::env::var(MAX_DOC_BYTES_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_DOC_BYTES);
        Self::from_spec(
            std::env::var(SCOPES_ENV).ok().as_deref(),
            journal_dir,
            max_doc_bytes,
        )
    }

    /// Parse the scope map. An entry that is malformed, duplicated or not an
    /// absolute container path is dropped with a warning: a scope that resolves
    /// somewhere unintended is worse than a scope that is absent, because the
    /// absent one fails loudly on first use.
    pub fn from_spec(spec: Option<&str>, journal_dir: PathBuf, max_doc_bytes: usize) -> Self {
        let mut scopes = BTreeMap::new();
        for entry in spec.unwrap_or("").split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((name, path)) = entry.split_once('=') else {
                warn!(entry = %entry, "ignoring host document scope without `name=path`; expected name=container_path");
                continue;
            };
            let (name, path) = (name.trim(), path.trim());
            if name.is_empty() || path.is_empty() {
                warn!(entry = %entry, "ignoring host document scope with an empty name or path");
                continue;
            }
            if !Path::new(path).is_absolute() {
                warn!(entry = %entry, "ignoring host document scope whose path is not absolute; a relative root would resolve against the process cwd");
                continue;
            }
            if scopes
                .insert(name.to_string(), PathBuf::from(path))
                .is_some()
            {
                warn!(scope = %name, "duplicate host document scope name; the later entry wins");
            }
        }
        Self {
            scopes,
            journal_dir,
            max_doc_bytes,
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.scopes.is_empty()
    }
}

/// One organizing step. The set is closed and every variant names locations
/// relative to the scope root; there is no variant that carries a destination
/// outside it, so "where may this write" has no answer that a document can
/// influence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum HostDocOp {
    /// Create one directory. Parents are not created implicitly: an auditable
    /// plan lists every directory it makes.
    Mkdir { path: String },
    /// Move or rename inside the scope. The destination must not exist.
    Rename { from: String, to: String },
    /// Create or replace a document's contents.
    Write { path: String, content: String },
}

impl HostDocOp {
    fn kind(&self) -> &'static str {
        match self {
            Self::Mkdir { .. } => "mkdir",
            Self::Rename { .. } => "rename",
            Self::Write { .. } => "write",
        }
    }

    /// The relative locations this operation names, in the order the paths
    /// matter (source before destination).
    fn rel_paths(&self) -> Vec<&String> {
        match self {
            Self::Mkdir { path } => vec![path],
            Self::Rename { from, to } => vec![from, to],
            Self::Write { path, .. } => vec![path],
        }
    }
}

/// What an operation would do to the scope, decided by looking at the scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// The target does not exist yet.
    Create,
    /// The target exists and would be rewritten.
    Replace,
    /// An existing path would move to a vacant one.
    Move,
    /// An existing directory is already there.
    Present,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedOp {
    pub op: HostDocOp,
    /// Absolute container paths the operation resolves to, one per named path.
    /// Echoed so the reviewer sees where inside the scope each step lands.
    pub resolved: Vec<String>,
    pub effect: Effect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocPlan {
    pub scope: String,
    /// Hash of the planned operation set. Apply must present this value back,
    /// so what gets executed is the set that was reviewed.
    pub plan_hash: String,
    pub ops: Vec<PlannedOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedOp {
    pub resolved: String,
    pub effect: Effect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocApply {
    pub journal_id: String,
    pub applied: Vec<AppliedOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDocRollback {
    pub journal_id: String,
    pub restored: usize,
}

/// A journal entry: what was done, and what it takes to undo it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum JournalRecord {
    Mkdir {
        path: String,
        /// False when the directory already existed, in which case rollback
        /// leaves it alone.
        created: bool,
    },
    Rename {
        from: String,
        to: String,
    },
    Write {
        path: String,
        /// False when the file did not exist, in which case rollback removes
        /// it instead of restoring contents.
        existed: bool,
        /// Backup file name under the journal's `orig/` directory.
        backup: Option<String>,
    },
}

struct HostDocsMetrics {
    registry: Registry,
    ops: CounterVec,
    rollbacks: CounterVec,
}

impl HostDocsMetrics {
    fn new(scope_count: usize) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let scopes = Gauge::new(
            "sandbox_host_docs_scopes",
            "Host document scopes configured on this executor",
        )?;
        let ops = CounterVec::new(
            Opts::new(
                "sandbox_host_docs_ops_total",
                "Host document operations by kind and outcome",
            ),
            &["kind", "outcome"],
        )?;
        let rollbacks = CounterVec::new(
            Opts::new(
                "sandbox_host_docs_rollbacks_total",
                "Host document rollbacks by outcome",
            ),
            &["outcome"],
        )?;
        registry.register(Box::new(scopes.clone()))?;
        registry.register(Box::new(ops.clone()))?;
        registry.register(Box::new(rollbacks.clone()))?;
        // The scope count is a configuration fact, not a moving reading: it is
        // published once so an operator can see whether the capability is on
        // at all without reading the pod's env.
        scopes.set(scope_count as f64);
        Ok(Self {
            registry,
            ops,
            rollbacks,
        })
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        if TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .is_ok()
        {
            String::from_utf8_lossy(&buf).into_owned()
        } else {
            String::new()
        }
    }

    fn count(&self, kind: &str, outcome: &str) {
        self.ops.with_label_values(&[kind, outcome]).inc();
    }

    fn count_rollback(&self, outcome: &str) {
        self.rollbacks.with_label_values(&[outcome]).inc();
    }
}

pub struct HostDocs {
    cfg: HostDocsConfig,
    metrics: HostDocsMetrics,
    /// Serializes applies so two plans cannot interleave their journals and
    /// perform decisions.
    apply_lock: Mutex<()>,
    journal_seq: AtomicU64,
}

impl HostDocs {
    pub fn new(cfg: HostDocsConfig) -> SFResult<Arc<Self>> {
        let metrics = HostDocsMetrics::new(cfg.scopes.len()).map_err(|e| {
            SFError::Config(format!("host document metrics registration failed: {e}"))
        })?;
        info!(
            scopes = cfg.scopes.len(),
            journal = %cfg.journal_dir.display(),
            max_doc_bytes = cfg.max_doc_bytes,
            "host document scopes configured"
        );
        Ok(Arc::new(Self {
            cfg,
            metrics,
            apply_lock: Mutex::new(()),
            journal_seq: AtomicU64::new(0),
        }))
    }

    pub fn metrics(&self) -> String {
        self.metrics.render()
    }

    /// Resolve a scope name to its configured root. An unknown name is a hard
    /// error: silently falling back to "some" root would hand a task the
    /// wrong person's documents.
    fn scope_root(&self, scope: &str) -> SFResult<&Path> {
        self.cfg
            .scopes
            .get(scope)
            .map(|p| p.as_path())
            .ok_or_else(|| {
                let known: Vec<&str> = self.cfg.scopes.keys().map(|k| k.as_str()).collect();
                SFError::Validation(format!(
                    "unknown host document scope {scope:?}; configured scopes: {known:?}"
                ))
            })
    }

    /// The plan a caller reviews. Effects are decided now, and apply refuses
    /// any operation whose effect no longer matches, so a review cannot be
    /// invalidated by a change that lands in between.
    pub fn plan(&self, scope: &str, ops: &[HostDocOp]) -> SFResult<HostDocPlan> {
        let planned = self.plan_ops(scope, ops)?;
        Ok(HostDocPlan {
            scope: scope.to_string(),
            plan_hash: plan_hash(scope, ops),
            ops: planned,
        })
    }

    fn plan_ops(&self, scope: &str, ops: &[HostDocOp]) -> SFResult<Vec<PlannedOp>> {
        if ops.is_empty() {
            return Err(SFError::Validation(
                "an organizing plan with no operations is refused".into(),
            ));
        }
        let root = self.scope_root(scope)?;
        let mut planned = Vec::with_capacity(ops.len());
        for op in ops {
            let mut resolved = Vec::new();
            for rel in op.rel_paths() {
                let p = resolve_within_scope(root, rel)?;
                if let HostDocOp::Write { content, .. } = op {
                    if content.len() > self.cfg.max_doc_bytes {
                        return Err(SFError::Validation(format!(
                            "document {} is {} bytes; the write ceiling is {} bytes",
                            p.display(),
                            content.len(),
                            self.cfg.max_doc_bytes
                        )));
                    }
                }
                resolved.push(p.display().to_string());
            }
            planned.push(PlannedOp {
                effect: effect_of(op, &resolved)?,
                resolved,
                op: op.clone(),
            });
        }
        Ok(planned)
    }

    /// Execute a reviewed plan. The operation set must hash to the plan the
    /// caller was given, and every effect must still hold; the first operation
    /// that fails rolls back the ones already applied.
    pub async fn apply(&self, plan: &HostDocPlan) -> SFResult<HostDocApply> {
        let ops: Vec<HostDocOp> = plan.ops.iter().map(|p| p.op.clone()).collect();
        let expected = plan_hash(&plan.scope, &ops);
        if expected != plan.plan_hash {
            return Err(SFError::Validation(
                "plan hash does not match the operations it carries; apply the plan as it was returned"
                    .into(),
            ));
        }
        let _guard = self.apply_lock.lock().await;
        let recomputed = self.plan_ops(&plan.scope, &ops)?;
        for (planned, now) in plan.ops.iter().zip(recomputed.iter()) {
            if planned.resolved != now.resolved || planned.effect != now.effect {
                return Err(SFError::Validation(format!(
                    "the scope changed since the plan was made: {} is now {:?} (planned {:?}); re-plan before applying",
                    now.resolved.join(" -> "),
                    now.effect,
                    planned.effect
                )));
            }
        }

        let journal_id = self.new_journal_id(&plan.plan_hash);
        let dir = self.cfg.journal_dir.join(&journal_id);
        tokio::fs::create_dir_all(dir.join("orig"))
            .await
            .map_err(|e| {
                SFError::Config(format!(
                    "cannot open a rollback journal at {}: {e}",
                    dir.display()
                ))
            })?;
        let mut records: Vec<JournalRecord> = Vec::new();
        let mut applied: Vec<AppliedOp> = Vec::new();
        for planned in &plan.ops {
            match self.apply_one(planned, &dir, records.len()).await {
                Ok(record) => {
                    records.push(record);
                    let resolved = planned.resolved.last().cloned().unwrap_or_default();
                    applied.push(AppliedOp {
                        resolved,
                        effect: planned.effect,
                    });
                    self.metrics.count(planned.op.kind(), "applied");
                }
                Err(e) => {
                    self.metrics.count(planned.op.kind(), "refused");
                    return Err(match self.undo(&dir, &records).await {
                        Ok(()) => {
                            // Nothing survives a failed apply, so the journal
                            // has no restore value: drop it with its backups.
                            let _ = tokio::fs::remove_dir_all(&dir).await;
                            SFError::Validation(format!(
                                "{e}; the {} operation(s) applied before it were rolled back, so the scope is unchanged",
                                records.len()
                            ))
                        }
                        Err(re) => {
                            // A partial undo is not a state this module can
                            // describe, let alone finish: leave the records on
                            // disk so the leftovers are discoverable instead of
                            // becoming silent.
                            let _ = write_journal(&dir, &records).await;
                            SFError::Agent(format!(
                                "{e}; rolling back the applied prefix failed ({re}); journal {journal_id} holds what was done ({}) and needs a manual look",
                                dir.display()
                            ))
                        }
                    });
                }
            }
        }
        write_journal(&dir, &records).await?;
        info!(journal = %journal_id, scope = %plan.scope, applied = applied.len(), "host document plan applied");
        Ok(HostDocApply {
            journal_id,
            applied,
        })
    }

    /// Perform one operation and record what it takes to undo it.
    ///
    /// Every decision about "what is there right now" is taken from the
    /// filesystem at the moment the operation runs, never from the plan: an
    /// earlier step of the same plan may have created or moved a file into this
    /// path, so a plan-time `Create` can be a real replacement by the time it
    /// is executed — and rollback has to know whose contents it overwrote.
    async fn apply_one(
        &self,
        planned: &PlannedOp,
        journal: &Path,
        index: usize,
    ) -> SFResult<JournalRecord> {
        match &planned.op {
            HostDocOp::Mkdir { .. } => {
                let path = PathBuf::from(&planned.resolved[0]);
                match tokio::fs::create_dir(&path).await {
                    Ok(()) => Ok(JournalRecord::Mkdir {
                        path: planned.resolved[0].clone(),
                        created: true,
                    }),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        let md = tokio::fs::metadata(&path).await.map_err(|e| {
                            SFError::Validation(format!("cannot inspect {}: {e}", path.display()))
                        })?;
                        if !md.is_dir() {
                            return Err(SFError::Validation(format!(
                                "{} exists and is not a directory",
                                path.display()
                            )));
                        }
                        // Somebody else made it; rollback must not delete a
                        // directory this plan did not create.
                        Ok(JournalRecord::Mkdir {
                            path: planned.resolved[0].clone(),
                            created: false,
                        })
                    }
                    Err(e) => Err(SFError::Validation(format!(
                        "cannot create {}: {e}",
                        path.display()
                    ))),
                }
            }
            HostDocOp::Rename { .. } => {
                let (from, to) = (
                    PathBuf::from(&planned.resolved[0]),
                    PathBuf::from(&planned.resolved[1]),
                );
                tokio::fs::rename(&from, &to).await.map_err(|e| {
                    SFError::Validation(format!(
                        "cannot move {} to {}: {e}",
                        from.display(),
                        to.display()
                    ))
                })?;
                Ok(JournalRecord::Rename {
                    from: planned.resolved[0].clone(),
                    to: planned.resolved[1].clone(),
                })
            }
            HostDocOp::Write { content, .. } => {
                let path = PathBuf::from(&planned.resolved[0]);
                let existing = match tokio::fs::symlink_metadata(&path).await {
                    Ok(md) => {
                        if md.is_dir() {
                            return Err(SFError::Validation(format!(
                                "{} is a directory",
                                path.display()
                            )));
                        }
                        if md.len() as usize > self.cfg.max_doc_bytes {
                            return Err(SFError::Validation(format!(
                                "{} is {} bytes; keeping a rollback copy is capped at {} bytes",
                                path.display(),
                                md.len(),
                                self.cfg.max_doc_bytes
                            )));
                        }
                        true
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                    Err(e) => {
                        return Err(SFError::Validation(format!(
                            "cannot inspect {}: {e}",
                            path.display()
                        )))
                    }
                };
                let backup = if existing {
                    let name = format!("orig/{index}");
                    tokio::fs::copy(&path, journal.join(&name))
                        .await
                        .map_err(|e| {
                            SFError::Validation(format!(
                                "cannot keep a rollback copy of {}: {e}",
                                path.display()
                            ))
                        })?;
                    Some(name)
                } else {
                    None
                };
                tokio::fs::write(&path, content.as_bytes())
                    .await
                    .map_err(|e| {
                        SFError::Validation(format!("cannot write {}: {e}", path.display()))
                    })?;
                Ok(JournalRecord::Write {
                    path: planned.resolved[0].clone(),
                    existed: existing,
                    backup,
                })
            }
        }
    }

    /// Undo the records of an apply that failed part-way. The records are not
    /// on disk yet (a journal file is written only once every operation
    /// succeeded), so this walks the in-memory list in reverse order.
    async fn undo(&self, dir: &Path, records: &[JournalRecord]) -> SFResult<()> {
        for record in records.iter().rev() {
            restore_record(dir, record).await?;
        }
        Ok(())
    }

    /// Restore the state before an apply, in reverse order.
    pub async fn rollback(&self, journal_id: &str) -> SFResult<HostDocRollback> {
        validate_journal_id(journal_id)?;
        let _guard = self.apply_lock.lock().await;
        let dir = self.cfg.journal_dir.join(journal_id);
        let records = read_journal(&dir).await?;
        let mut restored = 0usize;
        for record in records.iter().rev() {
            restore_record(&dir, record).await.map_err(|e| {
                self.metrics.count_rollback("partial");
                SFError::Validation(format!(
                    "rollback of journal {journal_id} stopped at {record:?}: {e}"
                ))
            })?;
            restored += 1;
        }
        let done = dir.join("journal.rolled-back.json");
        tokio::fs::rename(dir.join("journal.json"), &done)
            .await
            .map_err(|e| {
                SFError::Config(format!("cannot mark journal {journal_id} rolled back: {e}"))
            })?;
        self.metrics.count_rollback("restored");
        info!(journal = %journal_id, restored, "host document journal rolled back");
        Ok(HostDocRollback {
            journal_id: journal_id.to_string(),
            restored,
        })
    }

    fn new_journal_id(&self, plan_hash: &str) -> String {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let seq = self.journal_seq.fetch_add(1, Ordering::Relaxed);
        let digest = &plan_hash[..plan_hash.len().min(12)];
        format!("{millis}-{seq}-{digest}")
    }
}

/// Stable fingerprint of a planned operation set. Apply recomputes it from the
/// operations it was handed, so a plan that was edited after review cannot be
/// executed under the reviewed hash.
fn plan_hash(scope: &str, ops: &[HostDocOp]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(scope.as_bytes());
    for op in ops {
        hasher.update(b"\n");
        // `to_string` on a tag-carrying enum with struct variants emits fields
        // in declaration order, so the same operation always hashes the same.
        hasher.update(serde_json::to_string(op).unwrap_or_default().as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn effect_of(op: &HostDocOp, resolved: &[String]) -> SFResult<Effect> {
    let meta = |p: &str| std::fs::symlink_metadata(p);
    match op {
        HostDocOp::Mkdir { .. } => match meta(&resolved[0]) {
            Ok(md) if md.is_dir() => Ok(Effect::Present),
            // A file where a directory is expected is not "already there":
            // saying so would let a plan look like a no-op and then fail.
            Ok(_) => Err(SFError::Validation(format!(
                "{} exists and is not a directory",
                resolved[0]
            ))),
            Err(_) => Ok(Effect::Create),
        },
        HostDocOp::Rename { .. } => {
            if meta(&resolved[0]).is_err() {
                return Err(SFError::Validation(format!(
                    "nothing to move at {}",
                    resolved[0]
                )));
            }
            if meta(&resolved[1]).is_ok() {
                return Err(SFError::Validation(format!(
                    "{} already exists; a move never overwrites",
                    resolved[1]
                )));
            }
            Ok(Effect::Move)
        }
        HostDocOp::Write { .. } => {
            if let Ok(md) = meta(&resolved[0]) {
                if md.is_dir() {
                    return Err(SFError::Validation(format!(
                        "{} is a directory",
                        resolved[0]
                    )));
                }
                Ok(Effect::Replace)
            } else {
                Ok(Effect::Create)
            }
        }
    }
}

/// Resolve one caller-named location inside a scope root.
///
/// Only plain relative paths are accepted and no symlink is ever followed, so
/// neither `..` nor a link can name a location outside the scope — including
/// the container's own filesystem, which is real even when the host is not
/// reachable.
fn resolve_within_scope(root: &Path, rel: &str) -> SFResult<PathBuf> {
    let raw = rel.trim();
    if raw.is_empty() {
        return Err(SFError::Validation(
            "empty path in a document operation".into(),
        ));
    }
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(SFError::Validation(format!(
            "absolute path refused: {rel}; document operations name locations relative to the scope root"
        )));
    }
    let mut components: Vec<&std::ffi::OsStr> = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(name),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(SFError::Validation(format!(
                    "path {rel} contains `..`; document operations stay inside the scope root"
                )))
            }
            other => {
                return Err(SFError::Validation(format!(
                    "path {rel} contains an unsupported component ({other:?})"
                )))
            }
        }
    }
    if components.is_empty() {
        return Err(SFError::Validation(format!(
            "path {rel} names the scope root itself"
        )));
    }
    refuse_symlink(root, root)?;
    let mut current = root.to_path_buf();
    for name in components {
        current.push(name);
        refuse_symlink(root, &current)?;
    }
    Ok(current)
}

/// A symlink anywhere along the path — including a dangling one, whose target
/// would otherwise be created by a write — is refused rather than followed.
fn refuse_symlink(root: &Path, path: &Path) -> SFResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(md) if md.file_type().is_symlink() => Err(SFError::Validation(format!(
            "{} is a symbolic link; links are not followed inside a document scope (scope root {})",
            path.display(),
            root.display()
        ))),
        _ => Ok(()),
    }
}

fn validate_journal_id(id: &str) -> SFResult<()> {
    let path = Path::new(id);
    let mut components = path.components();
    let plain = !id.trim().is_empty()
        && !path.is_absolute()
        && matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none();
    if plain {
        Ok(())
    } else {
        Err(SFError::Validation(format!(
            "invalid journal id {id:?}; expected the id returned by an apply"
        )))
    }
}

async fn write_journal(dir: &Path, records: &[JournalRecord]) -> SFResult<()> {
    let body = serde_json::to_vec_pretty(records)
        .map_err(|e| SFError::Config(format!("cannot serialize a journal: {e}")))?;
    tokio::fs::write(dir.join("journal.json"), body)
        .await
        .map_err(|e| SFError::Config(format!("cannot write journal in {}: {e}", dir.display())))
}

async fn read_journal(dir: &Path) -> SFResult<Vec<JournalRecord>> {
    let rolled_back = dir.join("journal.rolled-back.json");
    if rolled_back.exists() {
        return Err(SFError::Validation(format!(
            "journal {} was already rolled back",
            dir.display()
        )));
    }
    let body = tokio::fs::read(dir.join("journal.json"))
        .await
        .map_err(|e| {
            SFError::Validation(format!("cannot read journal in {}: {e}", dir.display()))
        })?;
    serde_json::from_slice(&body)
        .map_err(|e| SFError::Config(format!("journal in {} is unreadable: {e}", dir.display())))
}

async fn restore_record(dir: &Path, record: &JournalRecord) -> SFResult<()> {
    match record {
        JournalRecord::Mkdir { path, created } => {
            if *created {
                tokio::fs::remove_dir(path).await.map_err(|e| {
                    SFError::Validation(format!("cannot remove the created directory {path}: {e}"))
                })?;
            }
            Ok(())
        }
        JournalRecord::Rename { from, to } => tokio::fs::rename(to, from)
            .await
            .map_err(|e| SFError::Validation(format!("cannot move {to} back to {from}: {e}"))),
        JournalRecord::Write {
            path,
            existed,
            backup,
        } => match (existed, backup) {
            (true, Some(name)) => tokio::fs::copy(dir.join(name), path)
                .await
                .map(|_| ())
                .map_err(|e| {
                    SFError::Validation(format!(
                        "cannot restore the previous contents of {path}: {e}"
                    ))
                }),
            (false, _) => tokio::fs::remove_file(path).await.map_err(|e| {
                SFError::Validation(format!("cannot remove the created file {path}: {e}"))
            }),
            (true, None) => Err(SFError::Config(format!(
                "journal for {path} says it existed but kept no copy"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_scope() -> (tempfile::TempDir, HostDocsConfig) {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("documents");
        std::fs::create_dir_all(&root).unwrap();
        let cfg = HostDocsConfig::from_spec(
            Some(&format!("alice={}", root.display())),
            dir.path().join("journal"),
            DEFAULT_MAX_DOC_BYTES,
        );
        (dir, cfg)
    }

    fn docs(cfg: &HostDocsConfig) -> Arc<HostDocs> {
        HostDocs::new(cfg.clone()).expect("host docs")
    }

    fn scope_root(cfg: &HostDocsConfig) -> PathBuf {
        cfg.scopes.get("alice").expect("scope").clone()
    }

    #[test]
    fn scope_spec_drops_entries_it_cannot_trust() {
        let cfg = HostDocsConfig::from_spec(
            Some("alice=/docs/alice, broken, bob=relative/path, carol=, =/docs/x, dave=/docs/dave"),
            PathBuf::from("/tmp/j"),
            DEFAULT_MAX_DOC_BYTES,
        );
        let names: Vec<&String> = cfg.scopes.keys().collect();
        assert_eq!(
            names,
            vec!["alice", "dave"],
            "a malformed or relative entry must not become a scope"
        );
    }

    #[test]
    fn absent_spec_means_the_capability_is_off() {
        let cfg = HostDocsConfig::from_spec(None, PathBuf::from("/tmp/j"), DEFAULT_MAX_DOC_BYTES);
        assert!(!cfg.is_enabled());
        let docs = docs(&cfg);
        let err = docs
            .plan("alice", &[HostDocOp::Mkdir { path: "a".into() }])
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown host document scope"),
            "{err}"
        );
    }

    #[test]
    fn absolute_paths_and_parent_components_are_refused_with_a_reason() {
        let (_d, cfg) = temp_scope();
        let docs = docs(&cfg);
        for (path, needle) in [
            ("/etc/passwd", "absolute path refused"),
            ("../../etc/passwd", "contains `..`"),
            ("a/../../b", "contains `..`"),
            ("", "empty path"),
        ] {
            let err = docs
                .plan("alice", &[HostDocOp::Mkdir { path: path.into() }])
                .unwrap_err();
            assert!(err.to_string().contains(needle), "{path}: {err}");
        }
    }

    #[test]
    fn a_symlink_inside_the_scope_is_refused_not_followed() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        let docs = docs(&cfg);
        // A link out of the scope: even though the mount hides the host, this
        // container's own /etc is real, so following it would escape.
        std::os::unix::fs::symlink("/etc", root.join("elsewhere")).unwrap();
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "elsewhere/passwd".into(),
                    content: "x".into(),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("symbolic link"), "{err}");
        // A dangling link: writing through it would create the target file,
        // which is exactly the escape the check has to close.
        std::os::unix::fs::symlink(root.join("missing"), root.join("dangling")).unwrap();
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "dangling".into(),
                    content: "x".into(),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("symbolic link"), "{err}");
    }

    #[tokio::test]
    async fn plan_effects_match_what_apply_does() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::write(root.join("old.txt"), b"before").unwrap();
        std::fs::create_dir(root.join("keep")).unwrap();
        let docs = docs(&cfg);
        let ops = vec![
            HostDocOp::Mkdir {
                path: "archive".into(),
            },
            HostDocOp::Rename {
                from: "old.txt".into(),
                to: "archive/old.txt".into(),
            },
            HostDocOp::Write {
                path: "summary.md".into(),
                content: "sum".into(),
            },
            HostDocOp::Mkdir {
                path: "keep".into(),
            },
        ];
        let plan = docs.plan("alice", &ops).unwrap();
        assert_eq!(
            plan.ops.iter().map(|p| p.effect).collect::<Vec<_>>(),
            vec![
                Effect::Create,
                Effect::Move,
                Effect::Create,
                Effect::Present
            ]
        );
        let applied = docs.apply(&plan).await.unwrap();
        assert_eq!(applied.applied.len(), 4);
        assert!(root.join("archive/old.txt").exists());
        assert!(!root.join("old.txt").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("summary.md")).unwrap(),
            "sum"
        );
        // A resolved path is always inside the scope, which is what makes the
        // plan reviewable at all.
        for planned in &plan.ops {
            for resolved in &planned.resolved {
                assert!(
                    Path::new(resolved).starts_with(&root),
                    "{resolved} escaped {}",
                    root.display()
                );
            }
        }
    }

    #[tokio::test]
    async fn apply_refuses_a_plan_whose_operations_were_edited() {
        let (_d, cfg) = temp_scope();
        let docs = docs(&cfg);
        let mut plan = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "a.txt".into(),
                    content: "one".into(),
                }],
            )
            .unwrap();
        plan.ops[0].op = HostDocOp::Write {
            path: "a.txt".into(),
            content: "two".into(),
        };
        let err = docs.apply(&plan).await.unwrap_err();
        assert!(err.to_string().contains("plan hash"), "{err}");
    }

    #[tokio::test]
    async fn apply_refuses_when_the_scope_changed_since_the_plan() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        let docs = docs(&cfg);
        let plan = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "a.txt".into(),
                    content: "one".into(),
                }],
            )
            .unwrap();
        assert_eq!(plan.ops[0].effect, Effect::Create);
        // Something appears at the planned location between review and apply.
        std::fs::write(root.join("a.txt"), b"somebody else's work").unwrap();
        let err = docs.apply(&plan).await.unwrap_err();
        assert!(
            err.to_string().contains("the scope changed since the plan"),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("a.txt")).unwrap(),
            "somebody else's work",
            "a refused apply must not touch the file"
        );
    }

    #[tokio::test]
    async fn rollback_restores_replaced_created_moved_and_made_paths() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::write(root.join("notes.txt"), b"original contents").unwrap();
        std::fs::create_dir(root.join("keep")).unwrap();
        let docs = docs(&cfg);
        let plan = docs
            .plan(
                "alice",
                &[
                    HostDocOp::Mkdir {
                        path: "archive".into(),
                    },
                    HostDocOp::Rename {
                        from: "notes.txt".into(),
                        to: "archive/notes.txt".into(),
                    },
                    HostDocOp::Write {
                        path: "archive/notes.txt".into(),
                        content: "rewritten".into(),
                    },
                    HostDocOp::Write {
                        path: "new.md".into(),
                        content: "created".into(),
                    },
                ],
            )
            .unwrap();
        let applied = docs.apply(&plan).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("archive/notes.txt")).unwrap(),
            "rewritten"
        );
        let report = docs.rollback(&applied.journal_id).await.unwrap();
        assert_eq!(report.restored, 4);
        assert!(!root.join("new.md").exists(), "created file removed");
        assert!(!root.join("archive").exists(), "created directory removed");
        assert_eq!(
            std::fs::read_to_string(root.join("notes.txt")).unwrap(),
            "original contents",
            "the moved file came back with its previous contents"
        );
        // Rolling back twice must not undo somebody's later work.
        let err = docs.rollback(&applied.journal_id).await.unwrap_err();
        assert!(err.to_string().contains("already rolled back"), "{err}");
    }

    #[tokio::test]
    async fn a_failed_operation_rolls_back_the_applied_prefix() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::create_dir(root.join("dir")).unwrap();
        let docs = docs(&cfg);
        let ops = vec![
            HostDocOp::Write {
                path: "kept.txt".into(),
                content: "one".into(),
            },
            // The second write lands in a directory that exists while the plan
            // is made and disappears before apply, so the operation itself
            // fails even though its effect still holds. The write before it
            // must not survive.
            HostDocOp::Write {
                path: "dir/second.txt".into(),
                content: "two".into(),
            },
        ];
        let plan = docs.plan("alice", &ops).unwrap();
        std::fs::remove_dir(root.join("dir")).unwrap();
        let err = docs.apply(&plan).await.unwrap_err();
        assert!(err.to_string().contains("rolled back"), "{err}");
        assert!(
            !root.join("kept.txt").exists(),
            "the applied prefix must not survive a failed plan"
        );
    }

    #[tokio::test]
    async fn a_write_larger_than_the_ceiling_is_refused_at_plan_time() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("documents");
        std::fs::create_dir_all(&root).unwrap();
        let cfg = HostDocsConfig::from_spec(
            Some(&format!("alice={}", root.display())),
            dir.path().join("journal"),
            16,
        );
        let docs = docs(&cfg);
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Write {
                    path: "big.txt".into(),
                    content: "x".repeat(17),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("ceiling"), "{err}");
    }

    #[tokio::test]
    async fn a_move_never_overwrites_an_existing_destination() {
        let (_d, cfg) = temp_scope();
        let root = scope_root(&cfg);
        std::fs::write(root.join("a.txt"), b"a").unwrap();
        std::fs::write(root.join("b.txt"), b"b").unwrap();
        let docs = docs(&cfg);
        let err = docs
            .plan(
                "alice",
                &[HostDocOp::Rename {
                    from: "a.txt".into(),
                    to: "b.txt".into(),
                }],
            )
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(std::fs::read_to_string(root.join("b.txt")).unwrap(), "b");
    }

    #[tokio::test]
    async fn a_journal_id_that_is_not_a_name_is_refused() {
        let (_d, cfg) = temp_scope();
        let docs = docs(&cfg);
        for id in ["../escape", "/absolute", "a/b", ""] {
            let err = docs.rollback(id).await.unwrap_err();
            assert!(
                err.to_string().contains("invalid journal id"),
                "{id}: {err}"
            );
        }
    }
}
