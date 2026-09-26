//! Hot/Warm/Cold tier migration for raw-data files.

use chrono::{DateTime, NaiveDate, Utc};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cog_core::{
    MetricsBackend, ObjectBackend, RawFileFormat, RawLogIndexEntry, RawLogIndexStore, SFError,
    SFResult, ShutdownSignal, StorageTier, TierMigratorConfig, TierPolicy,
};

/// Loop name reported through the background-loop liveness family.
pub const TIER_MIGRATION_LOOP: &str = "storage_tier_migration";

/// Background migrator. Use [`TierMigrator::spawn`] to start a periodic loop
/// or [`TierMigrator::run_once`] for an explicit pass (used by tests).
pub struct TierMigrator {
    pub base_dir: PathBuf,
    pub policy: TierPolicy,
    pub object_backend: Arc<dyn ObjectBackend>,
    pub index_store: Arc<dyn RawLogIndexStore>,
    pub metrics: Option<Arc<dyn MetricsBackend>>,
}

/// Build a [`TierPolicy`] from the binary-level [`TierMigratorConfig`].
/// Moved from `cog-core` so the domain-kernel stays free of conversion logic.
pub fn tier_policy_from_config(cfg: &TierMigratorConfig) -> TierPolicy {
    TierPolicy {
        hot_duration: std::time::Duration::from_secs(cfg.hot_duration_secs),
        warm_duration: std::time::Duration::from_secs(cfg.warm_duration_secs),
        warm_compression_level: cfg.warm_compression_level,
        cold_compression_level: cfg.cold_compression_level,
        scan_interval: std::time::Duration::from_secs(cfg.scan_interval_secs),
        cold_key_prefix: cfg.cold_key_prefix.clone(),
    }
}

impl TierMigrator {
    pub fn new(
        base_dir: impl Into<PathBuf>,
        policy: TierPolicy,
        object_backend: Arc<dyn ObjectBackend>,
        index_store: Arc<dyn RawLogIndexStore>,
    ) -> Self {
        Self {
            base_dir: base_dir.into(),
            policy,
            object_backend,
            index_store,
            metrics: None,
        }
    }

    /// Attach a metrics backend so each pass emits
    /// `tier_migration_total{tier="warm|cold|skipped|error"}`.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Spawn the periodic migration loop. Returns a join-handle the caller
    /// can drop when the program exits; cancellation is signalled by the
    /// shared [`ShutdownSignal`].
    pub fn spawn(self: Arc<Self>, shutdown: ShutdownSignal) -> tokio::task::JoinHandle<()> {
        let scan_interval = self.policy.scan_interval;
        // The first pass runs at once rather than after one interval. The
        // loop only makes progress while the process lives, and a
        // deployment that ships often replaces it well inside an interval
        // — waiting a full one would mean the pass never runs at all.
        // Scanning the same state twice costs one listing; missing every
        // pass costs the feature.
        cog_core::loop_health::spawn(
            TIER_MIGRATION_LOOP,
            cog_core::loop_health::Cadence::Periodic(scan_interval),
            shutdown.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let migrator = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move {
                    let mut interval = tokio::time::interval(scan_interval);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        beat.beat();
                        tokio::select! {
                            _ = interval.tick() => {
                                match migrator.run_once().await {
                                    Ok(stats) => migrator.emit_metrics(&stats).await,
                                    Err(e) => {
                                        tracing::warn!("TierMigrator pass failed: {}", e);
                                        migrator.emit_error_metric().await;
                                    }
                                }
                            }
                            _ = shutdown.wait() => {
                                tracing::info!("TierMigrator shutting down");
                                break;
                            }
                        }
                    }
                }
            },
        )
    }

    async fn emit_metrics(&self, stats: &MigrationStats) {
        let Some(ref mb) = self.metrics else { return };
        // A pass that moves nothing still has to say so. The outcome counters
        // below are all zero on a quiet pass, so without this the loop is
        // indistinguishable from a loop that never ran — which is exactly the
        // state it was in before. Counting passes makes the cadence visible.
        for (tier, count) in [
            ("pass", 1),
            ("warm", stats.warm_promotions),
            ("cold", stats.cold_promotions),
            ("skipped", stats.skipped),
            ("error", stats.errors),
        ] {
            if count == 0 {
                continue;
            }
            let mut labels = std::collections::HashMap::new();
            labels.insert("tier".into(), tier.into());
            if let Err(e) = mb
                .record_counter(
                    cog_core::metric_names::TIER_MIGRATION_TOTAL,
                    count as f64,
                    labels,
                )
                .await
            {
                tracing::warn!("tier_migration_total emit failed: {}", e);
            }
        }
    }

    async fn emit_error_metric(&self) {
        let Some(ref mb) = self.metrics else { return };
        let mut labels = std::collections::HashMap::new();
        labels.insert("tier".into(), "error".into());
        let _ = mb
            .record_counter(cog_core::metric_names::TIER_MIGRATION_TOTAL, 1.0, labels)
            .await;
    }

    /// Run one full pass: scan every stream subdirectory, migrate eligible
    /// files, and update the index store. Errors for individual files are
    /// logged and skipped so a single bad file does not abort the pass.
    pub async fn run_once(&self) -> SFResult<MigrationStats> {
        let mut stats = MigrationStats::default();

        let mut entries = match tokio::fs::read_dir(&self.base_dir).await {
            Ok(e) => e,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(stats),
            Err(err) => return Err(SFError::IO(err.to_string())),
        };

        while let Some(stream_entry) = entries
            .next_entry()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?
        {
            let stream_path = stream_entry.path();
            if !stream_path.is_dir() {
                continue;
            }
            let stream_name = match stream_entry.file_name().to_str() {
                Some(n) => n.to_string(),
                None => continue,
            };

            let mut files = match tokio::fs::read_dir(&stream_path).await {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("read_dir({}) failed: {}", stream_path.display(), e);
                    continue;
                }
            };

            while let Some(file_entry) = files
                .next_entry()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?
            {
                let path = file_entry.path();
                if !path.is_file() {
                    continue;
                }

                match self.migrate_file(&stream_name, &path).await {
                    Ok(Some(action)) => match action {
                        MigrationAction::ToWarm => stats.warm_promotions += 1,
                        MigrationAction::ToCold => stats.cold_promotions += 1,
                    },
                    Ok(None) => stats.skipped += 1,
                    Err(e) => {
                        tracing::warn!("migrate_file({}) failed: {}", path.display(), e);
                        stats.errors += 1;
                    }
                }
            }
        }

        Ok(stats)
    }

    async fn migrate_file(&self, stream: &str, path: &Path) -> SFResult<Option<MigrationAction>> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let modified = metadata
            .modified()
            .map_err(|e| SFError::IO(e.to_string()))?;
        let modified: DateTime<Utc> = modified.into();
        let age = Utc::now() - modified;

        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| SFError::IO(format!("invalid file name {}", path.display())))?
            .to_string();

        // Only the logger's own rotations are eligible. It names them
        // `YYYY-MM-DD.<ext>`. Anything else under a stream directory is a live
        // append stream — the audit chain is one flat `audit.jsonl`, not a
        // rotation — and moving it out from under its writer both cuts the
        // stream and leaves the writer appending to an unlinked file.
        let Some(log_date) = parse_log_date(&file_name) else {
            return Ok(None);
        };
        // The format is part of the file's key, so a name whose format cannot
        // be read is a file this migrator has no key for. Uploading it under a
        // guessed key would put two distinct files on one index row.
        let Some(format) = RawFileFormat::from_file_name(&file_name) else {
            return Ok(None);
        };

        match cog_core::tier_for_age(age, self.policy.hot_duration, self.policy.warm_duration) {
            // ── Cold-tier promotion ───────────────────────────────
            StorageTier::Cold => Ok(Some(
                self.promote_to_cold(stream, path, log_date, format).await?,
            )),
            // ── Warm-tier promotion ───────────────────────────────
            // Already-compressed files are past this transition.
            StorageTier::Warm if !is_compressed(&file_name) => Ok(Some(
                self.promote_to_warm(stream, path, log_date, format).await?,
            )),
            StorageTier::Warm | StorageTier::Hot => Ok(None),
        }
    }

    async fn promote_to_warm(
        &self,
        stream: &str,
        path: &Path,
        log_date: NaiveDate,
        format: RawFileFormat,
    ) -> SFResult<MigrationAction> {
        let raw = tokio::fs::read(path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let compressed = zstd::stream::encode_all(&raw[..], self.policy.warm_compression_level)
            .map_err(|e| SFError::IO(format!("zstd encode failed: {}", e)))?;

        let warm_path = path.with_extension(format!(
            "{}.zst",
            path.extension().and_then(|s| s.to_str()).unwrap_or("jsonl"),
        ));

        // `proto.bin` + `.zst` is the same name as `ProtoZstd` gives a file, so
        // a deployment that switched between those two formats on one day has a
        // live file where this compression wants to write. Overwriting it would
        // destroy that day's records with every step reporting success — so an
        // existing destination is only accepted when it already holds this exact
        // payload (a resumed pass), and is an error otherwise.
        match tokio::fs::read(&warm_path).await {
            Ok(existing) if existing != compressed => {
                return Err(SFError::IO(format!(
                    "warm-tier destination {} already holds different data; refusing to overwrite",
                    warm_path.display()
                )));
            }
            // A byte-identical destination is a pass resuming after it wrote the
            // copy. The index upsert below still has to run: the crash may have
            // landed between the write and the upsert.
            Ok(_) => {}
            Err(_) => {
                tokio::fs::write(&warm_path, &compressed)
                    .await
                    .map_err(|e| SFError::IO(e.to_string()))?;
            }
        }
        // Only delete the source after the compressed copy is durable.
        tokio::fs::remove_file(path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;

        let checksum = blake3::hash(&compressed).to_hex().to_string();
        let metadata = tokio::fs::metadata(&warm_path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let modified: DateTime<Utc> = metadata
            .modified()
            .map_err(|e| SFError::IO(e.to_string()))?
            .into();

        let entry = RawLogIndexEntry {
            hour: 0,
            event_count: 0,
            stream_name: stream.into(),
            log_date,
            format,
            file_path: warm_path.to_string_lossy().into_owned(),
            tier: StorageTier::Warm,
            size_bytes: compressed.len() as u64,
            checksum,
            start_time: log_date
                .and_hms_opt(0, 0, 0)
                .map(|n| n.and_utc())
                .unwrap_or(modified),
            end_time: modified,
            created_at: Utc::now(),
        };
        self.index_store.upsert(entry).await?;
        Ok(MigrationAction::ToWarm)
    }

    async fn promote_to_cold(
        &self,
        stream: &str,
        path: &Path,
        log_date: NaiveDate,
        format: RawFileFormat,
    ) -> SFResult<MigrationAction> {
        // Read whatever's on disk (already-compressed warm file or raw hot file).
        let raw = tokio::fs::read(path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        // If it's not yet compressed, compress at the cold level.
        let payload = if is_compressed(&file_name) {
            raw
        } else {
            zstd::stream::encode_all(&raw[..], self.policy.cold_compression_level)
                .map_err(|e| SFError::IO(format!("zstd encode failed: {}", e)))?
        };
        let cold_name = if is_compressed(&file_name) {
            file_name.clone()
        } else {
            format!("{}.zst", file_name)
        };
        let key = format!(
            "{}/{}/date={}/{}",
            self.policy.cold_key_prefix.trim_end_matches('/'),
            stream,
            log_date,
            cold_name
        );

        let uri = self.object_backend.put(&key, &payload).await?;
        // Verify upload before deleting the source.
        if !self.object_backend.exists(&key).await? {
            return Err(SFError::IO(format!(
                "cold-tier verification failed for key {}",
                key
            )));
        }

        let checksum = blake3::hash(&payload).to_hex().to_string();
        let entry = RawLogIndexEntry {
            hour: 0,
            event_count: 0,
            stream_name: stream.into(),
            log_date,
            format,
            file_path: uri,
            tier: StorageTier::Cold,
            size_bytes: payload.len() as u64,
            checksum,
            start_time: log_date
                .and_hms_opt(0, 0, 0)
                .map(|n| n.and_utc())
                .unwrap_or_else(Utc::now),
            end_time: log_date
                .and_hms_opt(23, 59, 59)
                .map(|n| n.and_utc())
                .unwrap_or_else(Utc::now),
            created_at: Utc::now(),
        };
        self.index_store.upsert(entry).await?;

        tokio::fs::remove_file(path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        Ok(MigrationAction::ToCold)
    }
}

/// Tally returned from [`TierMigrator::run_once`].
#[derive(Debug, Default, Clone)]
pub struct MigrationStats {
    pub warm_promotions: u64,
    pub cold_promotions: u64,
    pub skipped: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Copy)]
enum MigrationAction {
    ToWarm,
    ToCold,
}

pub fn is_compressed(file_name: &str) -> bool {
    file_name.ends_with(".zst") || file_name.ends_with(".zstd")
}

pub fn parse_log_date(file_name: &str) -> Option<NaiveDate> {
    // FileRawLogger names files `YYYY-MM-DD.jsonl[.zst]`.
    let stem = file_name.split('.').next()?.to_string();
    NaiveDate::parse_from_str(&stem, "%Y-%m-%d").ok()
}
