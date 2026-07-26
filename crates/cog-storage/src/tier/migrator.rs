//! Hot/Warm/Cold tier migration for raw-data files.

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cog_core::{
    MetricsBackend, ObjectBackend, RawLogIndexEntry, RawLogIndexStore, SFError, SFResult,
    ShutdownSignal, StorageTier, TierMigratorConfig, TierPolicy,
};

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
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.policy.scan_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Skip the initial immediate tick.
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match self.run_once().await {
                            Ok(stats) => self.emit_metrics(&stats).await,
                            Err(e) => {
                                tracing::warn!("TierMigrator pass failed: {}", e);
                                self.emit_error_metric().await;
                            }
                        }
                    }
                    _ = shutdown.wait() => {
                        tracing::info!("TierMigrator shutting down");
                        break;
                    }
                }
            }
        })
    }

    async fn emit_metrics(&self, stats: &MigrationStats) {
        let Some(ref mb) = self.metrics else { return };
        for (tier, count) in [
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
                .record_counter("tier_migration_total", count as f64, labels)
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
        let _ = mb.record_counter("tier_migration_total", 1.0, labels).await;
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
        let hot_age = ChronoDuration::from_std(self.policy.hot_duration)
            .map_err(|e| SFError::Config(e.to_string()))?;
        let warm_age = ChronoDuration::from_std(self.policy.warm_duration)
            .map_err(|e| SFError::Config(e.to_string()))?;

        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| SFError::IO(format!("invalid file name {}", path.display())))?
            .to_string();

        let log_date = parse_log_date(&file_name).unwrap_or_else(|| modified.date_naive());

        // ── Cold-tier promotion ───────────────────────────────
        if age >= hot_age + warm_age {
            return Ok(Some(self.promote_to_cold(stream, path, log_date).await?));
        }
        // ── Warm-tier promotion ───────────────────────────────
        if age >= hot_age && !is_compressed(&file_name) {
            return Ok(Some(self.promote_to_warm(stream, path, log_date).await?));
        }

        Ok(None)
    }

    async fn promote_to_warm(
        &self,
        stream: &str,
        path: &Path,
        log_date: NaiveDate,
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

        tokio::fs::write(&warm_path, &compressed)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
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
