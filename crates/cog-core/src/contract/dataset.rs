//! 数据集的契约：一份版本的元数据、一次写入的声明、一遍核对的结果。
//!
//! 写一份数据集有两半：字节进对象存储，行进目录，两半必须互相为真。两半会各自
//! 失败——写进桶却没进行，就是一批谁也点不出名字的字节（在、付费、对所有问目录的
//! 读者不可见）；有行没字节更糟，它让一次恢复去取不存在的东西，而信任目录的恢复会
//! 报告「搬完了」。所以摘要从交给后端的那份字节算出来，行里记的就是它，注册失败时
//! 没有行认领的那份对象要被收回。
//!
//! 形状与 trait 在这里，对象存储与目录的接法留在存储 crate：调用方问的是「这份
//! 数据集在不在、是不是它」，不是「它在哪个桶、哪张表」。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::SFResult;

/// 一个版本的元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetEntry {
    pub name: String,
    pub version: String,
    pub object_key: String,
    pub content_sha256: String,
    pub bytes: i64,
    /// How many records the dataset holds, when it is tabular. Absent for a
    /// blob: zero would say "a table with no rows", which is a different fact.
    pub row_count: Option<i64>,
    /// The interval the dataset covers, when it has one. Both bounds are
    /// present together or not at all.
    pub coverage_from: Option<DateTime<Utc>>,
    pub coverage_to: Option<DateTime<Utc>>,
    /// Where the bytes came from, in whatever form would let a person fetch
    /// them again.
    pub source: String,
    pub media_type: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl DatasetEntry {
    /// Whether a byte string is the one this row describes.
    ///
    /// Compares the digest the row recorded against one computed from the bytes
    /// a reader actually has. Length is not a substitute: a truncated file and
    /// a file with a hole can both keep their length.
    pub fn describes(&self, bytes: &[u8]) -> bool {
        self.bytes == bytes.len() as i64 && self.content_sha256 == sha256_of(bytes)
    }
}

/// A digest in the one spelling every row, object key and check uses.
pub fn sha256_of(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

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

/// 一份数据集的两半：对象存储里的字节，目录里的行。
///
/// trait 而不是具体结构：网关要能在对象存储或数据库不在时仍然回答「这里没有管
/// 数据集」，而不是链接一个实现才知道能不能问。
#[async_trait]
pub trait DatasetStore: Send + Sync {
    /// Write one version, deriving the object key from the content.
    ///
    /// Re-registering the same content is a no-op that returns the existing
    /// row: the numbers are the same dataset, and rewriting the bytes would
    /// only create a second copy of something already stored.
    async fn put(&self, spec: DatasetWrite, bytes: &[u8]) -> SFResult<DatasetEntry>;

    /// Write one version at a key the caller names.
    ///
    /// For a producer whose key namespace already exists and is read by code
    /// outside the catalog -- an upload route that hands out ids, for instance.
    /// The caller owes the key's uniqueness to that namespace: two writers
    /// racing on one key write to the same object, and the row records only
    /// whichever one registered first. [`Self::put`] derives the key from the
    /// content and has no such duty.
    async fn put_at(&self, key: &str, spec: DatasetWrite, bytes: &[u8]) -> SFResult<DatasetEntry>;

    /// One version's row, without the bytes.
    async fn find(&self, name: &str, version: &str) -> SFResult<Option<DatasetEntry>>;

    /// One version's row and bytes.
    ///
    /// `None` when the catalog has no such version. A row whose object is
    /// missing is an error, not `None`: the catalog is where "this dataset
    /// exists" is recorded, so an absent object is a broken promise rather than
    /// a version nobody wrote.
    async fn open(&self, name: &str, version: &str) -> SFResult<Option<(DatasetEntry, Vec<u8>)>>;

    /// The most recently registered version of a dataset, or `None`.
    async fn latest(&self, name: &str) -> SFResult<Option<DatasetEntry>>;

    /// The newest `limit` versions of a dataset, newest first.
    async fn versions(&self, name: &str, limit: usize) -> SFResult<Vec<DatasetEntry>>;

    /// Walk the catalog and check every version against the bytes actually in
    /// the store.
    async fn verify(&self, limit: usize) -> SFResult<DatasetVerification>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 同一份字节同一个摘要，长度相同但内容不同的一对不是同一份。
    /// 一条只比长度的判据会把「截断」与「有洞」都放过去。
    #[test]
    fn a_row_describes_the_bytes_it_was_written_from_and_no_others() {
        let bytes = b"a,b\n1,2\n";
        let entry = DatasetEntry {
            name: "t".into(),
            version: "v1".into(),
            object_key: "k".into(),
            content_sha256: sha256_of(bytes),
            bytes: bytes.len() as i64,
            row_count: Some(1),
            coverage_from: None,
            coverage_to: None,
            source: "test".into(),
            media_type: None,
            created_at: Utc::now(),
        };
        assert!(entry.describes(bytes));
        assert!(!entry.describes(b"a,b\n1,3\n"));
        assert!(!entry.describes(b"a,b\n1,2"));
    }

    /// 空目录不算通过：那里没有一条读数。
    #[test]
    fn an_empty_pass_proves_nothing() {
        let pass = DatasetVerification { checks: Vec::new() };
        assert_eq!(pass.checked(), 0);
        assert!(!pass.all_verified());
    }
}
