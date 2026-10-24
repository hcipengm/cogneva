//! Writing a dataset: the bytes go to an object store, the row goes to the
//! catalog, and both pieces of that have to be true of each other.
//!
//! The two stores fail independently. A write that lands in the bucket but not
//! in the catalog leaves bytes nobody can name -- present, paid for, and
//! invisible to every reader that asks the catalog what exists. A row that
//! lands without its bytes is worse: it tells a restore to fetch something that
//! is not there, and a restore that trusts the catalog would report success
//! having moved nothing. So the digest is computed from exactly the bytes handed
//! to the backend, the row records that digest, and a failed registration takes
//! its own object back out when no row claims it.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use cog_core::{ObjectBackend, SFError, SFResult};

use crate::postgres::dataset_catalog::{sha256_of, DatasetCatalog, DatasetEntry, NewDataset};

/// What a producer says about a version before it is written.
#[derive(Debug, Clone)]
pub struct DatasetWrite {
    pub name: String,
    pub version: String,
    /// Where the bytes came from, in whatever form would let a person fetch
    /// them again. The catalog exists so this question has an answer later.
    pub source: String,
    /// Record count, when the dataset is tabular. `None` for a blob: zero would
    /// claim a table with no rows.
    pub row_count: Option<i64>,
    /// The interval the data covers, when it covers one. Both bounds together
    /// or neither.
    pub coverage: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub media_type: Option<String>,
}

/// A catalog plus the object store it describes.
pub struct DatasetStore {
    objects: Arc<dyn ObjectBackend>,
    catalog: Arc<dyn DatasetCatalog>,
}

impl DatasetStore {
    pub fn new(objects: Arc<dyn ObjectBackend>, catalog: Arc<dyn DatasetCatalog>) -> Self {
        Self { objects, catalog }
    }

    /// Write one version.
    ///
    /// Re-registering the same content is a no-op that returns the existing
    /// row: the numbers are the same dataset, and rewriting the bytes would
    /// only create a second copy of something already stored.
    pub async fn put(&self, spec: DatasetWrite, bytes: &[u8]) -> SFResult<DatasetEntry> {
        let digest = sha256_of(bytes);
        let key = object_key(&spec.name, &spec.version, &digest);
        self.put_at(&key, spec, bytes).await
    }

    /// Write one version at a key the caller names.
    ///
    /// For a producer whose key namespace already exists and is read by code
    /// outside the catalog -- an upload route that hands out ids, for instance.
    /// The caller owes the key's uniqueness to that namespace: two writers
    /// racing on one key write to the same object, and the row records only
    /// whichever one registered first. [`Self::put`] derives the key from the
    /// content and has no such duty.
    pub async fn put_at(
        &self,
        key: &str,
        spec: DatasetWrite,
        bytes: &[u8],
    ) -> SFResult<DatasetEntry> {
        if spec.name.trim().is_empty() || spec.version.trim().is_empty() {
            return Err(SFError::Validation(
                "a dataset needs a name and a version: without either, nothing can ask for these \
                 bytes again"
                    .into(),
            ));
        }
        let digest = sha256_of(bytes);
        if let Some(held) = self.catalog.find(&spec.name, &spec.version).await? {
            if held.content_sha256 != digest {
                return Err(SFError::Conflict(format!(
                    "dataset {} version {} already holds different bytes ({} vs {}); a version that \
                     means two things cannot be restored from",
                    spec.name, spec.version, held.content_sha256, digest
                )));
            }
            return Ok(held);
        }

        self.objects.put(key, bytes).await?;

        let (coverage_from, coverage_to) = match spec.coverage {
            Some((from, to)) => (Some(from), Some(to)),
            None => (None, None),
        };
        let registered = self
            .catalog
            .register(NewDataset {
                name: spec.name.clone(),
                version: spec.version.clone(),
                object_key: key.to_string(),
                content_sha256: digest,
                bytes: bytes.len() as i64,
                row_count: spec.row_count,
                coverage_from,
                coverage_to,
                source: spec.source,
                media_type: spec.media_type,
            })
            .await;

        match registered {
            Ok(entry) => Ok(entry),
            Err(err) => {
                // Leave no object that no row names. Skipped when a row (this
                // writer's own, from a retry) already points at this key: the
                // bytes are then claimed and deleting them would break it.
                let claimed = matches!(
                    self.catalog.find(&spec.name, &spec.version).await,
                    Ok(Some(row)) if row.object_key == key
                );
                if !claimed {
                    if let Err(e) = self.objects.delete(key).await {
                        tracing::error!(
                            object = %key,
                            error = %e,
                            "dataset registration failed and its object could not be removed; \
                             the store now holds bytes no catalog row names"
                        );
                    }
                }
                Err(err)
            }
        }
    }

    /// One version's row, without the bytes.
    pub async fn find(&self, name: &str, version: &str) -> SFResult<Option<DatasetEntry>> {
        self.catalog.find(name, version).await
    }

    /// One version's row and bytes.
    ///
    /// `None` when the catalog has no such version. A row whose object is
    /// missing is an error, not `None`: the catalog is where "this dataset
    /// exists" is recorded, so an absent object is a broken promise rather than
    /// a version nobody wrote.
    pub async fn open(
        &self,
        name: &str,
        version: &str,
    ) -> SFResult<Option<(DatasetEntry, Vec<u8>)>> {
        let Some(entry) = self.catalog.find(name, version).await? else {
            return Ok(None);
        };
        match self.objects.get(&entry.object_key).await? {
            Some(bytes) => Ok(Some((entry, bytes))),
            None => Err(SFError::Internal(format!(
                "dataset {name} version {version} is catalogued at {} but the object store has no \
                 such object",
                entry.object_key
            ))),
        }
    }

    pub async fn latest(&self, name: &str) -> SFResult<Option<DatasetEntry>> {
        self.catalog.latest(name).await
    }

    pub async fn versions(&self, name: &str, limit: usize) -> SFResult<Vec<DatasetEntry>> {
        self.catalog.list(name, limit).await
    }

    /// Walk the catalog and check every version against the bytes actually in
    /// the store.
    ///
    /// This is what a migration is judged by. "The process came up" says the
    /// binary starts against whatever the store happens to hold; it says nothing
    /// about whether the objects that were supposed to come across did, and a
    /// restore that moved no data answers exactly the same way as one that moved
    /// all of it. So: the digest the row recorded against the digest of the
    /// bytes now under that key, size against what was registered.
    ///
    /// One unreadable object does not stop the walk: a pass that gives up on the
    /// first failure reports one problem and hides the rest, and after a
    /// migration the count of what is wrong is the part that decides.
    pub async fn verify(&self, limit: usize) -> SFResult<DatasetVerification> {
        let entries = self.catalog.all(limit).await?;
        let mut checks = Vec::with_capacity(entries.len());
        for entry in entries {
            let outcome = match self.objects.get(&entry.object_key).await {
                Ok(Some(bytes)) => {
                    let actual = sha256_of(&bytes);
                    let actual_bytes = bytes.len() as i64;
                    if actual == entry.content_sha256 && actual_bytes == entry.bytes {
                        DatasetOutcome::Verified
                    } else {
                        DatasetOutcome::Mismatched {
                            recorded: entry.content_sha256.clone(),
                            actual,
                            recorded_bytes: entry.bytes,
                            actual_bytes,
                        }
                    }
                }
                Ok(None) => DatasetOutcome::Missing,
                Err(e) => DatasetOutcome::Unreadable(e.to_string()),
            };
            checks.push(DatasetCheck {
                name: entry.name,
                version: entry.version,
                object_key: entry.object_key,
                outcome,
            });
        }
        Ok(DatasetVerification { checks })
    }
}

/// What one catalogued version turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetOutcome {
    /// The object is the one the row describes.
    Verified,
    /// The object is there and is not what the row describes. Both digests and
    /// both lengths travel: a reader has to be able to see whether this is a
    /// different file or the same file damaged, and a length alone cannot say.
    Mismatched {
        recorded: String,
        actual: String,
        recorded_bytes: i64,
        actual_bytes: i64,
    },
    /// The catalog says this version exists and the store has nothing there.
    Missing,
    /// The store could not be asked. Distinct from [`Self::Missing`]: a store
    /// that is down has not been shown to be empty.
    Unreadable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetCheck {
    pub name: String,
    pub version: String,
    pub object_key: String,
    pub outcome: DatasetOutcome,
}

/// The result of one verification pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetVerification {
    /// In catalog order: newest first.
    pub checks: Vec<DatasetCheck>,
}

impl DatasetVerification {
    pub fn checked(&self) -> usize {
        self.checks.len()
    }

    pub fn verified(&self) -> usize {
        self.count(|o| matches!(o, DatasetOutcome::Verified))
    }

    pub fn failed(&self) -> usize {
        self.checked() - self.verified()
    }

    fn count(&self, pred: impl Fn(&DatasetOutcome) -> bool) -> usize {
        self.checks.iter().filter(|c| pred(&c.outcome)).count()
    }

    /// Whether the pass proved anything.
    ///
    /// An empty catalog is not a pass. Zero datasets verified is the same
    /// reading as a migration that carried nothing, and that is exactly the case
    /// a green verdict must not cover -- so the caller is told no, with
    /// [`Self::checked`] at zero to say why.
    pub fn all_verified(&self) -> bool {
        self.checked() > 0 && self.failed() == 0
    }
}

/// `datasets/<name>/<version>/sha256-<first 12 hex>`.
///
/// The short digest is enough to separate two contents of one version; the full
/// one lives in the row, which is what a reader compares against. Names and
/// versions keep their bytes as given, so a name with a slash nests one level
/// deeper rather than turning into a different name.
fn object_key(name: &str, version: &str, digest: &str) -> String {
    let short = digest
        .strip_prefix("sha256:")
        .map(|hex| &hex[..hex.len().min(12)])
        .unwrap_or("unknown");
    format!("datasets/{name}/{version}/sha256-{short}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeObjects {
        written: Mutex<HashMap<String, Vec<u8>>>,
        deletes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ObjectBackend for FakeObjects {
        async fn put(&self, key: &str, data: &[u8]) -> SFResult<String> {
            self.written
                .lock()
                .unwrap()
                .insert(key.to_string(), data.to_vec());
            Ok(format!("fake://{key}"))
        }
        async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
            Ok(self.written.lock().unwrap().get(key).cloned())
        }
        async fn delete(&self, key: &str) -> SFResult<()> {
            self.deletes.lock().unwrap().push(key.to_string());
            self.written.lock().unwrap().remove(key);
            Ok(())
        }
        async fn presign_url(&self, key: &str, _expiry_secs: u64) -> SFResult<String> {
            Ok(format!("fake://{key}"))
        }
        async fn exists(&self, key: &str) -> SFResult<bool> {
            Ok(self.written.lock().unwrap().contains_key(key))
        }
        async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>> {
            let prefix = prefix.unwrap_or_default();
            Ok(self
                .written
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    /// A catalog that keeps rows in memory and refuses to break its one rule.
    #[derive(Default)]
    struct FakeCatalog {
        rows: Mutex<Vec<DatasetEntry>>,
        refuse: Mutex<bool>,
    }

    #[async_trait]
    impl DatasetCatalog for FakeCatalog {
        async fn register(&self, entry: NewDataset) -> SFResult<DatasetEntry> {
            if *self.refuse.lock().unwrap() {
                return Err(SFError::Database("catalog is down".into()));
            }
            let mut rows = self.rows.lock().unwrap();
            if let Some(held) = rows
                .iter()
                .find(|r| r.name == entry.name && r.version == entry.version)
            {
                if held.content_sha256 != entry.content_sha256 {
                    return Err(SFError::Conflict(
                        "different bytes under one version".into(),
                    ));
                }
                return Ok(held.clone());
            }
            let row = DatasetEntry {
                name: entry.name,
                version: entry.version,
                object_key: entry.object_key,
                content_sha256: entry.content_sha256,
                bytes: entry.bytes,
                row_count: entry.row_count,
                coverage_from: entry.coverage_from,
                coverage_to: entry.coverage_to,
                source: entry.source,
                media_type: entry.media_type,
                created_at: Utc::now(),
            };
            rows.push(row.clone());
            Ok(row)
        }

        async fn find(&self, name: &str, version: &str) -> SFResult<Option<DatasetEntry>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.name == name && r.version == version)
                .cloned())
        }

        async fn latest(&self, name: &str) -> SFResult<Option<DatasetEntry>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|r| r.name == name)
                .cloned())
        }

        async fn list(&self, name: &str, limit: usize) -> SFResult<Vec<DatasetEntry>> {
            let mut out: Vec<_> = self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.name == name)
                .rev()
                .take(limit)
                .cloned()
                .collect();
            out.reverse();
            Ok(out)
        }

        async fn all(&self, limit: usize) -> SFResult<Vec<DatasetEntry>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .rev()
                .take(limit)
                .cloned()
                .collect())
        }
    }

    fn spec(version: &str) -> DatasetWrite {
        DatasetWrite {
            name: "papers".into(),
            version: version.into(),
            source: "crawler:arxiv".into(),
            row_count: None,
            coverage: None,
            media_type: Some("application/pdf".into()),
        }
    }

    fn store() -> (DatasetStore, Arc<FakeObjects>, Arc<FakeCatalog>) {
        let objects = Arc::new(FakeObjects::default());
        let catalog = Arc::new(FakeCatalog::default());
        (
            DatasetStore::new(objects.clone(), catalog.clone()),
            objects,
            catalog,
        )
    }

    #[tokio::test]
    async fn the_row_records_the_digest_of_the_bytes_that_were_written() {
        let (store, objects, _) = store();
        let bytes = b"%PDF-1.7 first edition";
        let entry = store.put(spec("2026-10-24"), bytes).await.expect("put");

        let written = objects.written.lock().unwrap();
        let (key, object) = written.iter().next().expect("one object written");
        assert_eq!(key, &entry.object_key, "目录行指向的不是写下去的那个键");
        assert_eq!(object.as_slice(), bytes);
        assert_eq!(entry.content_sha256, sha256_of(bytes));
        assert_eq!(entry.bytes, bytes.len() as i64);
        assert!(entry.describes(bytes));
    }

    #[tokio::test]
    async fn the_same_content_twice_is_one_row_and_one_object() {
        let (store, objects, _) = store();
        let first = store.put(spec("v1"), b"same bytes").await.expect("first");
        let second = store.put(spec("v1"), b"same bytes").await.expect("second");
        assert_eq!(first, second, "同样的字节登记两次必须是同一行");
        assert_eq!(objects.written.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn different_bytes_under_a_taken_version_are_refused_before_anything_is_written() {
        let (store, objects, _) = store();
        store.put(spec("v1"), b"original").await.expect("first");
        let refused = store.put(spec("v1"), b"impostor").await;
        assert!(
            matches!(refused, Err(SFError::Conflict(_))),
            "一个版本被两组字节占用必须被拒：{refused:?}"
        );
        let written = objects.written.lock().unwrap();
        assert_eq!(written.len(), 1, "被拒的那次不该往桶里写任何东西");
        assert_eq!(
            written.values().next().unwrap().as_slice(),
            b"original",
            "原来那一份被覆盖了"
        );
    }

    #[tokio::test]
    async fn a_failed_registration_takes_its_object_back_out() {
        let (store, objects, catalog) = store();
        *catalog.refuse.lock().unwrap() = true;
        let refused = store.put(spec("v1"), b"bytes").await;
        assert!(refused.is_err(), "{refused:?}");
        assert!(
            objects.written.lock().unwrap().is_empty(),
            "登记失败却把字节留在桶里，就是一份谁也点不到名的数据"
        );
        assert_eq!(objects.deletes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_catalog_row_whose_object_is_gone_is_an_error_not_an_absence() {
        let (store, objects, _) = store();
        store.put(spec("v1"), b"bytes").await.expect("put");
        let key = objects
            .written
            .lock()
            .unwrap()
            .keys()
            .next()
            .cloned()
            .expect("one object");
        objects
            .delete(&key)
            .await
            .expect("delete behind the catalog's back");

        let opened = store.open("papers", "v1").await;
        assert!(
            matches!(opened, Err(SFError::Internal(_))),
            "账上有行、桶里没东西，必须报错而不是当作没有这个版本：{opened:?}"
        );
    }

    #[tokio::test]
    async fn a_pass_over_untouched_datasets_verifies_every_one() {
        let (store, _, _) = store();
        store.put(spec("v1"), b"first").await.expect("put v1");
        store.put(spec("v2"), b"second").await.expect("put v2");

        let report = store.verify(100).await.expect("verify");
        assert_eq!(report.checked(), 2);
        assert_eq!(report.verified(), 2);
        assert!(report.all_verified(), "{report:?}");
    }

    /// The corruption that a byte count cannot see: same length, different
    /// content.
    #[tokio::test]
    async fn an_object_replaced_by_one_of_the_same_length_still_reads_as_mismatched() {
        let (store, objects, _) = store();
        let entry = store.put(spec("v1"), b"first night").await.expect("put");
        {
            let mut written = objects.written.lock().unwrap();
            written.insert(entry.object_key.clone(), b"FIRST night".to_vec());
        }

        let report = store.verify(100).await.expect("verify");
        assert_eq!(report.checked(), 1);
        assert_eq!(report.verified(), 0);
        assert!(!report.all_verified());
        match &report.checks[0].outcome {
            DatasetOutcome::Mismatched {
                recorded,
                actual,
                recorded_bytes,
                actual_bytes,
            } => {
                assert_ne!(recorded, actual, "同样长度的替换没被摘要抓到");
                assert_eq!(recorded_bytes, actual_bytes, "这次坏的不是长度");
            }
            other => panic!("读成了 {other:?}，应当是 Mismatched"),
        }
    }

    #[tokio::test]
    async fn a_row_whose_object_is_gone_reads_as_missing_and_the_walk_continues() {
        let (store, objects, _) = store();
        let gone = store.put(spec("v1"), b"lost").await.expect("put v1");
        store.put(spec("v2"), b"kept").await.expect("put v2");
        objects
            .delete(&gone.object_key)
            .await
            .expect("delete behind the catalog's back");

        let report = store.verify(100).await.expect("verify");
        assert_eq!(
            report.checked(),
            2,
            "一个坏对象让整趟停下，其余的就没人量了"
        );
        assert_eq!(report.verified(), 1);
        let missing = report
            .checks
            .iter()
            .find(|c| matches!(c.outcome, DatasetOutcome::Missing))
            .expect("丢的那个读成了 Missing");
        assert_eq!(missing.version, "v1");
    }

    #[tokio::test]
    async fn an_empty_catalog_is_not_a_green_pass() {
        let (store, _, _) = store();
        let report = store.verify(100).await.expect("verify");
        assert_eq!(report.checked(), 0);
        assert!(
            !report.all_verified(),
            "一个数据集也没量到却判绿：这正是「进程起来了」冒充「迁移成功」的样子"
        );
    }

    #[tokio::test]
    async fn a_version_nobody_wrote_reads_as_absent() {
        let (store, _, _) = store();
        let opened = store.open("papers", "never").await.expect("no row is fine");
        assert!(opened.is_none());
    }
}
