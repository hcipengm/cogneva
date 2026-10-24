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
                .filter(|r| r.name == name)
                .next_back()
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
    async fn a_version_nobody_wrote_reads_as_absent() {
        let (store, _, _) = store();
        let opened = store.open("papers", "never").await.expect("no row is fine");
        assert!(opened.is_none());
    }
}
