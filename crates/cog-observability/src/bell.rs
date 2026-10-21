//! 站内铃铛：把告警的 firing 边沿写成一条按人寻址的通知。
//!
//! 告警有两个读者面，读的是两张表：告警面板读 `alerts`（当前状态），铃铛读
//! `notifications`（到达过什么、谁读过）。一个只写 `alerts` 的部署，人能打开面板
//! 看见红点，但不会被告知；而"没被通知"和"没有发生"在读数面上一模一样。
//!
//! 这个模块只做一件事：给定一条边沿与一个收件人，写一行。谁该收（实例属主）、
//! 写失败算什么，都由调用方按它自己的上下文决定；行长什么样只有这里一份。

use std::sync::Arc;

use tracing::warn;

use crate::alert_store::NewAlert;

/// 一条通知需要的两端：能落库的库，与能寻址的人。
///
/// 合成一个值，是因为任何一半单独存在都不是出口：只有库没有收件人会写出一行
/// 给虚构的人，只有收件人没有库则网关永远读不到。装配点因此只在两端都在时给出
/// `Some`，"接了一半"不会长得像一条能用的出口。
pub struct InAppNotifier {
    store: Arc<dyn cog_core::NotificationStore>,
    users: Arc<dyn cog_core::UserStore>,
}

impl InAppNotifier {
    pub fn new(
        store: Arc<dyn cog_core::NotificationStore>,
        users: Arc<dyn cog_core::UserStore>,
    ) -> Self {
        Self { store, users }
    }
}

/// 写一条并如实返回是否落了行。
///
/// 调用方要靠这个布尔决定"要不要把这一条记成已通知"：先记后写会在写失败时
/// 永久丢掉一条通知，而"写失败"与"没人该知道"在表上没有区别。
pub async fn write_one(
    store: &dyn cog_core::NotificationStore,
    user_id: &str,
    alert: &NewAlert,
    metrics: Option<&Arc<dyn cog_core::MetricsBackend>>,
) -> bool {
    let notification = cog_core::Notification {
        id: uuid::Uuid::new_v4().to_string(),
        user_id: user_id.to_string(),
        ntype: cog_core::NotificationType::System,
        title: format!("告警：{}", alert.rule),
        body: format!("[{}] {}", alert.severity, alert.message),
        is_read: false,
        created_at: chrono::Utc::now(),
        read_at: None,
    };
    match store.create(notification).await {
        Ok(()) => {
            crate::alerts::record_notification_center(metrics, crate::alerts::OUTCOME_DELIVERED, 1)
                .await;
            true
        }
        Err(e) => {
            warn!(
                rule = %alert.rule,
                error = %e,
                "could not write the notification for a firing alert; the alert row \
                 is there, the bell will not ring for it"
            );
            crate::alerts::record_notification_center(
                metrics,
                crate::alerts::OUTCOME_WRITE_FAILED,
                1,
            )
            .await;
            false
        }
    }
}

/// 扫表循环要问的两个问题：还有哪些没通知，以及这条已经通知了。
///
/// 抽成 trait 是为了让"写成功才标记"这条顺序能被测到：写反了（先标记后写）在
/// 写失败的那一轮就永久丢掉一条通知，而这件事在真库上要制造一次写入失败才看得见。
#[async_trait::async_trait]
pub trait PendingQueue: Send + Sync {
    async fn pending(&self, limit: i64) -> anyhow::Result<Vec<NewAlert>>;
    async fn mark(&self, dedup_key: &str) -> anyhow::Result<bool>;
}

#[async_trait::async_trait]
impl PendingQueue for crate::alert_store::PostgresAlertStore {
    async fn pending(&self, limit: i64) -> anyhow::Result<Vec<NewAlert>> {
        self.unnotified_firing(limit).await
    }
    async fn mark(&self, dedup_key: &str) -> anyhow::Result<bool> {
        self.mark_notified(dedup_key).await
    }
}

/// 把"在 firing、还没通知过人"的告警发出去，返回这一轮写了多少条。
///
/// 单点生产者：告警从四条路径写进 `alerts`（supervisor 事件桥、PromQL 的 infra
/// watcher、配置声明检查、安全网关），在这里统一变成通知。放在循环里扫表而不是
/// 在每个写入点直发，换来三件事：接上通知之前就已在响的存量会被补上、写失败的
/// 条目下一轮自动重试、以及"还没通知"这件事本身在表上是一列可数的东西。
///
/// 解析收件人失败时一条都不写、也不标记：下一轮重试，并且读数面上记
/// `write_failed`，因为"没有用户"与"没有告警"在铃铛里长得一样。
pub async fn drain_pending(
    store: &dyn PendingQueue,
    notifier: &InAppNotifier,
    metrics: Option<&Arc<dyn cog_core::MetricsBackend>>,
    limit: i64,
) -> anyhow::Result<usize> {
    let pending = store.pending(limit).await?;
    if pending.is_empty() {
        return Ok(0);
    }
    let owner = match notifier.users.find_owner().await {
        Ok(Some(owner)) => owner,
        Ok(None) => {
            warn!(
                alerts = pending.len(),
                "firing alerts have no owner to notify: this instance has no user yet, so \
                 the alert panel has them and the bell does not"
            );
            crate::alerts::record_notification_center(
                metrics,
                crate::alerts::OUTCOME_WRITE_FAILED,
                pending.len(),
            )
            .await;
            return Ok(0);
        }
        Err(e) => {
            warn!(
                error = %e,
                alerts = pending.len(),
                "could not resolve the owner a firing alert should be sent to"
            );
            crate::alerts::record_notification_center(
                metrics,
                crate::alerts::OUTCOME_WRITE_FAILED,
                pending.len(),
            )
            .await;
            return Ok(0);
        }
    };
    drain_for(
        store,
        notifier.store.as_ref(),
        &owner.id.to_string(),
        &pending,
        metrics,
    )
    .await
}

/// 给定收件人，把一批告警写成通知并逐条标记。
///
/// 拆出来是为了能测顺序：**写成功才标记**。反过来（先标记后写）在写失败的那一轮
/// 就把这条永久记成"已通知"，铃铛再也不会响；而这个错误在真库上要制造一次写入
/// 失败才看得见，所以顺序由这一格的测试守着。
pub async fn drain_for(
    queue: &dyn PendingQueue,
    store: &dyn cog_core::NotificationStore,
    user_id: &str,
    pending: &[NewAlert],
    metrics: Option<&Arc<dyn cog_core::MetricsBackend>>,
) -> anyhow::Result<usize> {
    let mut written = 0;
    for alert in pending {
        if write_one(store, user_id, alert, metrics).await {
            queue.mark(&alert.dedup_key).await?;
            written += 1;
        }
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 只记标记的队列：断言收到的是"哪些 dedup_key 被标成已通知"。
    #[derive(Default)]
    struct Marks {
        marked: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl PendingQueue for Marks {
        async fn pending(&self, _limit: i64) -> anyhow::Result<Vec<NewAlert>> {
            Ok(Vec::new())
        }
        async fn mark(&self, dedup_key: &str) -> anyhow::Result<bool> {
            self.marked.lock().unwrap().push(dedup_key.to_string());
            Ok(true)
        }
    }

    /// 只收通知行的库；`refuse` 让它表现成写入失败。
    #[derive(Default)]
    struct Rows {
        written: std::sync::Mutex<Vec<cog_core::Notification>>,
        refuse: bool,
    }

    #[async_trait::async_trait]
    impl cog_core::NotificationStore for Rows {
        async fn list(
            &self,
            _user_id: &str,
            _filter: cog_core::NotificationFilter,
        ) -> cog_core::SFResult<cog_core::NotificationList> {
            Ok(cog_core::NotificationList {
                items: Vec::new(),
                unread_count: 0,
                next_cursor: None,
                has_more: false,
            })
        }
        async fn mark_read(&self, _user_id: &str, _id: &str) -> cog_core::SFResult<bool> {
            Ok(false)
        }
        async fn mark_all_read(&self, _user_id: &str) -> cog_core::SFResult<usize> {
            Ok(0)
        }
        async fn create(&self, notification: cog_core::Notification) -> cog_core::SFResult<()> {
            if self.refuse {
                return Err(cog_core::SFError::IO("the table refused the row".into()));
            }
            self.written.lock().unwrap().push(notification);
            Ok(())
        }
    }

    /// One counter write as the backend saw it: name, delta, labels.
    type CounterWrite = (String, f64, std::collections::HashMap<String, String>);

    /// 只记计数器写入的读数面，供断言"写失败留了一格"。
    #[derive(Default)]
    struct Counters {
        writes: std::sync::Mutex<Vec<CounterWrite>>,
    }

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for Counters {
        async fn record_gauge(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.writes
                .lock()
                .unwrap()
                .push((name.as_str().to_string(), value, labels));
            Ok(())
        }
        async fn record_histogram(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: std::collections::HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_gauge_latest(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }
        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    fn alert(rule: &str) -> NewAlert {
        NewAlert {
            rule: rule.into(),
            dedup_key: format!("{rule}:i1"),
            severity: "warning".into(),
            message: "still true".into(),
            labels: serde_json::json!({"source": "infra_watch"}),
        }
    }

    /// 每一行都是发给同一个人、点名规则的未读通知，id 是持久库认的 UUID。
    #[tokio::test]
    async fn each_pending_alert_becomes_one_addressed_row() {
        let queue = Marks::default();
        let rows = Rows::default();
        let recipient = uuid::Uuid::new_v4().to_string();
        let written = drain_for(
            &queue,
            &rows,
            &recipient,
            &[alert("llm_upstream_pool_down"), alert("task_dead_letter")],
            None,
        )
        .await
        .unwrap();

        assert_eq!(written, 2);
        let rows = rows.written.lock().unwrap();
        assert_eq!(rows.len(), 2);
        for (row, rule) in rows
            .iter()
            .zip(["llm_upstream_pool_down", "task_dead_letter"])
        {
            assert_eq!(row.user_id, recipient, "收件人是实例属主");
            assert!(row.title.contains(rule), "标题点名规则");
            assert!(!row.is_read);
            assert!(uuid::Uuid::parse_str(&row.id).is_ok(), "持久库只收 UUID");
        }
        assert_eq!(
            *queue.marked.lock().unwrap(),
            vec![
                "llm_upstream_pool_down:i1".to_string(),
                "task_dead_letter:i1".to_string()
            ],
            "每一条写成功的都要标记"
        );
    }

    /// 写失败必须在读数面上留一格，并且和"送达"用同一个 family 的两个 outcome:
    /// 只记送达的话，一个铃铛从不响的部署与一个从无告警的部署读数相同。
    #[tokio::test]
    async fn a_refused_row_is_counted_as_a_write_failure() {
        let queue = Marks::default();
        let rows = Rows {
            written: Default::default(),
            refuse: true,
        };
        let counters = Arc::new(Counters::default());
        let backend: Arc<dyn cog_core::MetricsBackend> = counters.clone();
        drain_for(
            &queue,
            &rows,
            &uuid::Uuid::new_v4().to_string(),
            &[alert("llm_upstream_pool_down")],
            Some(&backend),
        )
        .await
        .unwrap();

        let writes = counters.writes.lock().unwrap();
        assert!(
            writes.iter().any(|(name, value, labels)| name
                == cog_core::metric_names::ALERT_NOTIFICATIONS_TOTAL.as_str()
                && *value == 1.0
                && labels.get("channel")
                    == Some(&crate::alerts::CHANNEL_NOTIFICATION_CENTER.to_string())
                && labels.get("outcome") == Some(&crate::alerts::OUTCOME_WRITE_FAILED.to_string())),
            "写失败要有一格：{writes:?}"
        );
    }

    /// 送达记在同一个 family 的另一个 outcome 上。
    #[tokio::test]
    async fn a_written_row_is_counted_as_delivered() {
        let queue = Marks::default();
        let rows = Rows::default();
        let counters = Arc::new(Counters::default());
        let backend: Arc<dyn cog_core::MetricsBackend> = counters.clone();
        drain_for(
            &queue,
            &rows,
            &uuid::Uuid::new_v4().to_string(),
            &[alert("llm_upstream_pool_down")],
            Some(&backend),
        )
        .await
        .unwrap();

        let writes = counters.writes.lock().unwrap();
        assert!(
            writes.iter().any(|(name, value, labels)| name
                == cog_core::metric_names::ALERT_NOTIFICATIONS_TOTAL.as_str()
                && *value == 1.0
                && labels.get("channel")
                    == Some(&crate::alerts::CHANNEL_NOTIFICATION_CENTER.to_string())
                && labels.get("outcome") == Some(&crate::alerts::OUTCOME_DELIVERED.to_string())),
            "送达要记在同一个 family 上：{writes:?}"
        );
    }

    /// 写失败的那一条不能被标记：否则下一轮不会重试，这条通知永久丢失。
    #[tokio::test]
    async fn a_refused_row_is_left_for_the_next_round() {
        let queue = Marks::default();
        let rows = Rows {
            written: Default::default(),
            refuse: true,
        };
        let written = drain_for(
            &queue,
            &rows,
            &uuid::Uuid::new_v4().to_string(),
            &[alert("llm_upstream_pool_down")],
            None,
        )
        .await
        .unwrap();

        assert_eq!(written, 0);
        assert!(
            queue.marked.lock().unwrap().is_empty(),
            "没写成的条目不标记，下一轮还会被扫到"
        );
    }
}
