//! Behaviour of the raw-log hot/warm/cold migrator: what it is allowed to take
//! and what it has to leave alone.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cog_core::{ObjectBackend, RawLogIndexStore, RawLogQuery, StorageTier, TierPolicy};
use cog_storage::{MemoryObjectBackend, MemoryRawLogIndexStore, TierMigrator};

fn policy() -> TierPolicy {
    TierPolicy {
        hot_duration: Duration::from_secs(86_400),
        warm_duration: Duration::from_secs(604_800),
        warm_compression_level: 3,
        cold_compression_level: 9,
        scan_interval: Duration::from_secs(3_600),
        cold_key_prefix: "raw".into(),
    }
}

/// Write `name` under `base_dir/<stream>/` and backdate it so it has aged out
/// of whatever tier the test is exercising.
fn aged_file(base_dir: &std::path::Path, stream: &str, name: &str, age: Duration) -> std::path::PathBuf {
    let stream_dir = base_dir.join(stream);
    std::fs::create_dir_all(&stream_dir).unwrap();
    let path = stream_dir.join(name);
    std::fs::write(&path, b"{\"recorded_by\":\"tier-test\"}\n").unwrap();
    let file = std::fs::File::options().write(true).open(&path).unwrap();
    file.set_modified(SystemTime::now() - age).unwrap();
    path
}

fn migrator(
    base_dir: &std::path::Path,
) -> (
    TierMigrator,
    Arc<MemoryObjectBackend>,
    Arc<MemoryRawLogIndexStore>,
) {
    let objects = Arc::new(MemoryObjectBackend::new());
    let index = Arc::new(MemoryRawLogIndexStore::new());
    let migrator = TierMigrator::new(
        base_dir,
        policy(),
        objects.clone() as Arc<dyn ObjectBackend>,
        index.clone() as Arc<dyn RawLogIndexStore>,
    );
    (migrator, objects, index)
}

/// A rotation that has aged past the hot window is compressed in place and
/// indexed, and the uncompressed source is gone.
#[tokio::test]
async fn an_aged_rotation_is_compressed_in_place_and_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let path = aged_file(dir.path(), "transport_raw", "2026-09-14.jsonl", Duration::from_secs(172_800));
    let (migrator, _objects, index) = migrator(dir.path());

    let stats = migrator.run_once().await.unwrap();

    assert_eq!(stats.warm_promotions, 1);
    assert!(!path.exists(), "the uncompressed source should be gone");
    assert!(dir.path().join("transport_raw/2026-09-14.jsonl.zst").exists());

    let rows = index.query(&RawLogQuery::default()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].stream_name, "transport_raw");
    assert_eq!(rows[0].tier, StorageTier::Warm);
}

/// The audit chain is one flat file the logger never rotates. Archiving it
/// would move it out from under its writer and truncate the chain's readable
/// history, so a file with no date in its name is not eligible however old it
/// is.
#[tokio::test]
async fn an_unrotated_file_is_left_alone_however_old() {
    let dir = tempfile::tempdir().unwrap();
    let path = aged_file(dir.path(), "audit", "audit.jsonl", Duration::from_secs(2_592_000));
    let (migrator, objects, index) = migrator(dir.path());

    let stats = migrator.run_once().await.unwrap();

    assert_eq!(stats.warm_promotions, 0);
    assert!(path.exists(), "a live append stream must stay where its writer put it");
    assert!(!dir.path().join("audit/audit.jsonl.zst").exists());
    assert_eq!(stats.skipped, 1);
    assert!(index.query(&RawLogQuery::default()).await.unwrap().is_empty());
    assert!(objects.get("raw/audit/date=2026-08-21/audit.jsonl.zst").await.unwrap().is_none());
}
