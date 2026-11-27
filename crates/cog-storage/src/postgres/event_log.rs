//! 那条流唯一的写口：一份 SQL、一个 id 工厂、一次信封检查。
//!
//! 各族的构造器在 cog-core 里拼信封；这里只落库。新增一族事实时不该再复制一份
//! `INSERT`——复制出来的每一份都要各自与迁移保持逐列一致，而它们不会一起红。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use cog_core::{check as check_envelope, EnvelopeFields, EventLog, SFError, SFResult};

const INSERT: &str = "\
INSERT INTO cogneva_event (\
    event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
    idempotency_key, causation_id, batch_id, session_id, actor, writer, scope, \
    payload_version, attributes, at, recorded_at) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW()) \
ON CONFLICT (writer, idempotency_key) DO NOTHING";

/// 在调用方的事务/连接上写一条。唯一写口在事务路径上的形态。
///
/// 任务是成批写的（定义、转移、尝试在一个事务里），不能用另开的连接——那会让
/// 「这一条事件」与它描述的那次转移分属两个事务。所以写口的 SQL 抽成这个函数，
/// trait 实现与事务路径都走它；知道 `cogneva_event` 列的地方仍然只有这一个文件。
pub async fn append_on(
    conn: &mut sqlx::PgConnection,
    fields: &EnvelopeFields,
    at: DateTime<Utc>,
) -> cog_core::SFResult<()> {
    check_envelope(fields).map_err(|defect| {
        SFError::Database(format!(
            "{} refused before it was written: {}",
            fields.kind,
            defect.as_str()
        ))
    })?;
    sqlx::query(INSERT)
        .bind(&fields.event_id)
        .bind(&fields.kind)
        .bind(&fields.subject_kind)
        .bind(&fields.subject_id)
        .bind(&fields.unit_of_work)
        .bind(&fields.step)
        .bind(&fields.outcome)
        .bind(fields.attempt)
        .bind(&fields.idempotency_key)
        .bind(&fields.causation_id)
        .bind(&fields.batch_id)
        .bind(&fields.session_id)
        .bind(&fields.actor)
        .bind(&fields.writer)
        .bind(&fields.scope)
        .bind(fields.payload_version)
        .bind(&fields.attributes)
        .bind(at)
        .execute(&mut *conn)
        .await
        .map_err(|e| SFError::Database(format!("append {}: {e}", fields.kind)))?;
    Ok(())
}

/// 唯一写口的 Postgres 实现。
pub struct EventLogStore {
    pool: PgPool,
    writer: String,
    ids: std::sync::Mutex<cog_core::EventIdFactory>,
    /// 写一条要多久：p99 是「存储还跟不跟得上」的写入面读数。没接指标后端时
    /// 不记，append 的行为不变。
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
}

#[async_trait]
impl cog_core::EventStore for EventLogStore {
    fn writer(&self) -> &str {
        &self.writer
    }

    fn next_event_id(&self) -> String {
        <Self as cog_core::EventLog>::next_event_id(self)
    }

    async fn append(&self, event: cog_core::FactEvent) -> SFResult<()> {
        let event_id = event
            .event_id
            .clone()
            .unwrap_or_else(|| <Self as cog_core::EventLog>::next_event_id(self));
        let fields = event.payload.envelope(&event_id, &self.writer);
        let at = event
            .at
            .or_else(|| event.payload.at())
            .unwrap_or_else(Utc::now);
        <Self as cog_core::EventLog>::append(self, fields, at).await
    }
}

impl EventLogStore {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        let mut seed = [0u8; 10];
        seed.copy_from_slice(&uuid::Uuid::new_v4().into_bytes()[..10]);
        Self {
            pool,
            writer: writer.into(),
            ids: std::sync::Mutex::new(cog_core::EventIdFactory::seeded(now_ms, seed)),
            metrics: None,
        }
    }

    /// 接上指标后端：之后每一次 append 都记一条耗时。
    pub fn with_metrics(mut self, metrics: Option<Arc<dyn cog_core::MetricsBackend>>) -> Self {
        self.metrics = metrics;
        self
    }
}

/// 记下一次 append 的耗时。单独成函数是为了它能被单独判：`append` 走的是库，
/// 而这条读数只关于「记没记」，与库在不在无关。
pub(crate) async fn observe_append(
    metrics: Option<&Arc<dyn cog_core::MetricsBackend>>,
    kind: &str,
    secs: f64,
) {
    let Some(metrics) = metrics else {
        return;
    };
    let labels = HashMap::from([("kind".to_string(), kind.to_string())]);
    if let Err(e) = metrics
        .record_histogram(cog_core::metric_names::EVENT_APPEND_SECONDS, secs, labels)
        .await
    {
        // 记不下来不是 append 的失败：这一条事实仍然写进去了。
        tracing::debug!(error = %e, "append 的耗时没有记下来");
    }
}

#[async_trait]
impl EventLog for EventLogStore {
    fn writer(&self) -> &str {
        &self.writer
    }

    fn next_event_id(&self) -> String {
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        self.ids
            .lock()
            .expect("the event id generator is not shared across panics")
            .next(now_ms)
    }

    /// 一条事件的每一列都从 `fields` 落：因果、批号、尝试、版本不再有第二条入口。
    async fn append(&self, fields: EnvelopeFields, at: DateTime<Utc>) -> SFResult<()> {
        let started = std::time::Instant::now();
        let result = async {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| SFError::Database(format!("acquire a connection: {e}")))?;
            append_on(&mut conn, &fields, at).await
        }
        .await;
        observe_append(
            self.metrics.as_ref(),
            &fields.kind,
            started.elapsed().as_secs_f64(),
        )
        .await;
        result
    }
}

#[cfg(test)]
mod append_reading_tests {
    use super::observe_append;
    use chrono::{Duration, Utc};
    use std::sync::Arc;

    /// 接上指标后端时，一次 append 的耗时要落成一条带 `kind` 的直方图样本。
    ///
    /// 没有这一条，`append_pressure` 判词要的写入延迟读数在树上是空的——判词
    /// 会把这一条永远记成「量不到」，而「量不到」与「存储跟得上」在面板上是
    /// 两句话。
    #[tokio::test]
    async fn an_append_observation_reaches_the_histogram() {
        let metrics = Arc::new(crate::MemoryMetricsBackend::new());
        let as_trait: Arc<dyn cog_core::MetricsBackend> = metrics.clone();
        observe_append(Some(&as_trait), "change_landed", 0.0123).await;
        let now = Utc::now();
        let samples = as_trait
            .query_histogram_range(
                cog_core::metric_names::EVENT_APPEND_SECONDS.as_str(),
                now - Duration::seconds(5),
                now + Duration::seconds(5),
            )
            .await
            .expect("histogram query");
        assert_eq!(samples.len(), 1, "一次 append 应当落一条样本");
        assert!((samples[0].value - 0.0123).abs() < 1e-9);
        assert_eq!(
            samples[0].labels.get("kind"),
            Some(&"change_landed".to_string())
        );
    }

    /// 没接指标后端时不记，append 的调用点不需要知道这件事。
    #[tokio::test]
    async fn without_a_backend_nothing_is_recorded() {
        observe_append(None, "change_landed", 0.01).await;
    }
}
