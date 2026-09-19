//! Behaviour of the raw-log hot/warm/cold migrator: what it is allowed to take
//! and what it has to leave alone.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cog_core::{
    MetricsBackend, ObjectBackend, RawFileFormat, RawLogIndexStore, RawLogQuery, ShutdownSignal,
    StorageTier, TierPolicy,
};
use cog_storage::{
    MemoryMetricsBackend, MemoryObjectBackend, MemoryRawLogIndexStore, TierMigrator,
};

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
fn aged_file(
    base_dir: &std::path::Path,
    stream: &str,
    name: &str,
    age: Duration,
) -> std::path::PathBuf {
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
    let path = aged_file(
        dir.path(),
        "transport_raw",
        "2026-09-14.jsonl",
        Duration::from_secs(172_800),
    );
    let (migrator, _objects, index) = migrator(dir.path());

    let stats = migrator.run_once().await.unwrap();

    assert_eq!(stats.warm_promotions, 1);
    assert!(!path.exists(), "the uncompressed source should be gone");
    assert!(dir
        .path()
        .join("transport_raw/2026-09-14.jsonl.zst")
        .exists());

    let rows = index.query(&RawLogQuery::default()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].stream_name, "transport_raw");
    assert_eq!(rows[0].tier, StorageTier::Warm);
}

/// One day can hold two files for one stream when the configured format changed
/// mid-day: the logger opens a second file rather than re-encoding the first.
/// The index key includes the format, so each keeps its own row. Keyed on
/// `(stream, date)` alone, the second promotion overwrote the first row and the
/// first file's archived name was recorded nowhere.
#[tokio::test]
async fn two_formats_on_one_date_keep_separate_rows() {
    let dir = tempfile::tempdir().unwrap();
    aged_file(
        dir.path(),
        "system_raw",
        "2026-09-14.jsonl",
        Duration::from_secs(172_800),
    );
    aged_file(
        dir.path(),
        "system_raw",
        "2026-09-14.proto.bin",
        Duration::from_secs(172_800),
    );
    let (migrator, _objects, index) = migrator(dir.path());

    let stats = migrator.run_once().await.unwrap();

    assert_eq!(stats.warm_promotions, 2, "both files must be promoted");
    assert!(dir.path().join("system_raw/2026-09-14.jsonl.zst").exists());
    assert!(dir
        .path()
        .join("system_raw/2026-09-14.proto.bin.zst")
        .exists());

    let rows = index.query(&RawLogQuery::default()).await.unwrap();
    assert_eq!(rows.len(), 2, "one row per file, not one row per date");
    let formats: std::collections::HashSet<RawFileFormat> = rows.iter().map(|r| r.format).collect();
    assert_eq!(formats.len(), 2, "the two rows must differ in format");
    for row in &rows {
        assert!(
            std::path::Path::new(&row.file_path).exists(),
            "every index row must name a file that is really there: {}",
            row.file_path
        );
    }
}

/// Warm-tier compression appends `.zst` to the extension it found, which for a
/// `proto.bin` file is the same name `ProtoZstd` writes. A deployment that
/// switched between those two formats on one day therefore has a live file where
/// the compression wants to write. Overwriting it would destroy that day's
/// records while every step reported success.
#[tokio::test]
async fn compression_does_not_overwrite_a_file_it_did_not_write() {
    let dir = tempfile::tempdir().unwrap();
    let stream_dir = dir.path().join("system_raw");
    std::fs::create_dir_all(&stream_dir).unwrap();
    let source = aged_file(
        dir.path(),
        "system_raw",
        "2026-09-14.proto.bin",
        Duration::from_secs(172_800),
    );
    let occupied = stream_dir.join("2026-09-14.proto.bin.zst");
    std::fs::write(&occupied, b"a different format's records").unwrap();
    let (migrator, _objects, index) = migrator(dir.path());

    let stats = migrator.run_once().await.unwrap();

    assert_eq!(
        stats.errors, 1,
        "the collision must be reported, not resolved"
    );
    assert_eq!(stats.warm_promotions, 0);
    assert_eq!(
        std::fs::read(&occupied).unwrap(),
        b"a different format's records",
        "the occupying file must be left untouched"
    );
    assert!(
        source.exists(),
        "the source must stay where it is rather than be deleted unarchived"
    );
    assert!(index
        .query(&RawLogQuery::default())
        .await
        .unwrap()
        .is_empty());
}

/// The audit chain is one flat file the logger never rotates. Archiving it
/// would move it out from under its writer and truncate the chain's readable
/// history, so a file with no date in its name is not eligible however old it
/// is.
#[tokio::test]
async fn an_unrotated_file_is_left_alone_however_old() {
    let dir = tempfile::tempdir().unwrap();
    let path = aged_file(
        dir.path(),
        "audit",
        "audit.jsonl",
        Duration::from_secs(2_592_000),
    );
    let (migrator, objects, index) = migrator(dir.path());

    let stats = migrator.run_once().await.unwrap();

    assert_eq!(stats.warm_promotions, 0);
    assert!(
        path.exists(),
        "a live append stream must stay where its writer put it"
    );
    assert!(!dir.path().join("audit/audit.jsonl.zst").exists());
    assert_eq!(stats.skipped, 1);
    assert!(index
        .query(&RawLogQuery::default())
        .await
        .unwrap()
        .is_empty());
    assert!(objects
        .get("raw/audit/date=2026-08-21/audit.jsonl.zst")
        .await
        .unwrap()
        .is_none());
}

/// A rotation past the warm window leaves the local disk entirely: the payload
/// goes to the object backend under a date-partitioned key, the index row says
/// cold, and the local file is gone. The upload is verified before the source
/// is deleted, so a backend that silently drops the object cannot lose the file.
#[tokio::test]
async fn an_aged_rotation_older_than_the_warm_window_moves_to_cold() {
    let dir = tempfile::tempdir().unwrap();
    let path = aged_file(
        dir.path(),
        "transport_raw",
        "2026-09-01.jsonl",
        Duration::from_secs(1_555_200),
    );
    let (migrator, objects, index) = migrator(dir.path());

    let stats = migrator.run_once().await.unwrap();

    assert_eq!(stats.cold_promotions, 1);
    assert_eq!(stats.warm_promotions, 0);
    assert!(
        !path.exists(),
        "the source should be gone after a verified upload"
    );
    assert!(
        !dir.path()
            .join("transport_raw/2026-09-01.jsonl.zst")
            .exists(),
        "cold promotion must not leave a warm copy behind"
    );

    let payload = objects
        .get("raw/transport_raw/date=2026-09-01/2026-09-01.jsonl.zst")
        .await
        .unwrap()
        .expect("the payload should be in the object backend");
    assert_eq!(
        zstd::stream::decode_all(&payload[..]).unwrap(),
        b"{\"recorded_by\":\"tier-test\"}\n"
    );

    let rows = index.query(&RawLogQuery::default()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tier, StorageTier::Cold);
    assert!(
        rows[0]
            .file_path
            .ends_with("raw/transport_raw/date=2026-09-01/2026-09-01.jsonl.zst"),
        "index should point at the uploaded object, got {}",
        rows[0].file_path
    );
}

/// The spawned loop has to make progress within the process's lifetime, and a
/// process that is replaced every few tens of minutes never reaches the far end
/// of a one-hour interval. A first pass that waits for one would therefore
/// never run at all on a deployment that ships often — so it runs at once, and
/// a pass that moves nothing still counts itself so the cadence is observable.
#[tokio::test]
async fn the_spawned_loop_passes_without_waiting_a_full_interval() {
    let dir = tempfile::tempdir().unwrap();
    aged_file(
        dir.path(),
        "transport_raw",
        "2026-09-14.jsonl",
        Duration::from_secs(172_800),
    );
    let (migrator, _objects, _index) = migrator(dir.path());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let migrator = Arc::new(migrator.with_metrics(metrics.clone() as Arc<dyn MetricsBackend>));

    let shutdown = ShutdownSignal::new();
    // Far longer than the test will run: only an immediate first pass can
    // produce anything before this deadline.
    let handle = migrator.spawn(shutdown.clone());

    let mut pass = 0.0;
    for _ in 0..200 {
        let totals = metrics
            .query_counter_totals("tier_migration_total")
            .await
            .unwrap();
        pass = totals
            .iter()
            .filter(|s| s.labels.get("tier").map(String::as_str) == Some("pass"))
            .map(|s| s.value)
            .sum();
        if pass > 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(pass > 0.0, "the loop produced no pass within its interval");

    shutdown.trigger();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let totals = metrics
        .query_counter_totals("tier_migration_total")
        .await
        .unwrap();
    let warm: f64 = totals
        .iter()
        .filter(|s| s.labels.get("tier").map(String::as_str) == Some("warm"))
        .map(|s| s.value)
        .sum();
    assert_eq!(warm, 1.0, "the pass should have reported its promotion");
}
