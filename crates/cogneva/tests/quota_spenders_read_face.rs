//! 当天谁在花：按人、按租户的当日花销读面。
//!
//! 额度写入面收的是一个 id——可运维在看见「谁在花」之前，不知道该给谁写上限。
//! 这一读就是那个「看见」：它的窗口必须是额度判定数的那个 UTC 日，否则读出来
//! 的排行与「谁会被拒」不是同一件事——一个人今天花光额度、另一个人三天里慢慢花
//! 光，在「最近 72 小时」的排行里长得一模一样。
//!
//! 四条判据，每条都对应一种曾经会静默错误的读法：
//!
//! - 排行按 token 降序、总数是全部行之和，不是前几行之和；
//! - 每行带的是**声明过的**上限，不是判定时退到的默认值——后者会让每一行都读成
//!   「配过上限」，而运维真正要问的是「谁还没配」；
//! - 没有花销路径的层级直接拒答，不答一份空账：空账会被读成「今天没人花」；
//! - 这一版没有上限库时，响应自己说 `ceilings_readable: false`，而不是让每一行
//!   的空值冒充「没人声明过上限」。
//!
//! 判据跑的是真路由器：路由挂没挂上、权限层放不放行，都是这里才能红的东西。

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{
    HierarchyDecision, HierarchyManager, LlmUsageLedger, QuotaContext, QuotaLimits, QuotaScope,
    SFResult,
};
use cog_gateway::{create_router, GatewayState};

use crate::router_state;

/// 只答按主体汇总的那两读；其余读口一律不该被这条路由碰到。
struct FakeSpend {
    people: Vec<(String, u64)>,
    workspaces: Vec<(String, u64)>,
    /// 读面交给账本的起点。判据用它确认窗口是 UTC 日，不是「最近 N 小时」。
    since: AtomicI64,
}

impl FakeSpend {
    fn with(people: Vec<(&str, u64)>, workspaces: Vec<(&str, u64)>) -> Arc<Self> {
        Arc::new(Self {
            people: people
                .into_iter()
                .map(|(id, tokens)| (id.to_string(), tokens))
                .collect(),
            workspaces: workspaces
                .into_iter()
                .map(|(id, tokens)| (id.to_string(), tokens))
                .collect(),
            since: AtomicI64::new(0),
        })
    }
}

#[async_trait::async_trait]
impl LlmUsageLedger for FakeSpend {
    async fn spend_by_person_since(&self, since_unix: i64) -> Result<Vec<(String, u64)>, String> {
        self.since.store(since_unix, Ordering::SeqCst);
        Ok(self.people.clone())
    }

    async fn spend_by_workspace_since(
        &self,
        since_unix: i64,
    ) -> Result<Vec<(String, u64)>, String> {
        self.since.store(since_unix, Ordering::SeqCst);
        Ok(self.workspaces.clone())
    }

    async fn record(&self, _: &cog_core::LlmUsageRecord) -> Result<(), String> {
        unimplemented!("只读替身")
    }
    async fn record_exchange(&self, _: &cog_core::LlmExchangeRecord) -> Result<(), String> {
        unimplemented!("只读替身")
    }
    async fn upstream_window(
        &self,
        _: &str,
        _: u64,
    ) -> Result<cog_core::UpstreamWindowReading, String> {
        unimplemented!("只读替身")
    }
    async fn burned_between(&self, _: &str, _: i64, _: i64) -> Result<u64, String> {
        unimplemented!("只读替身")
    }
    async fn record_window(&self, _: &cog_core::StoredWindow) -> Result<(), String> {
        unimplemented!("只读替身")
    }
    async fn stored_window(
        &self,
        _: &str,
        _: u64,
    ) -> Result<Option<cog_core::StoredWindow>, String> {
        unimplemented!("只读替身")
    }
    async fn stored_windows(&self, _: &str) -> Result<Vec<cog_core::StoredWindow>, String> {
        unimplemented!("只读替身")
    }
    async fn spent_since(&self, _: &str, _: i64) -> Result<u64, String> {
        unimplemented!("只读替身")
    }
    async fn spend_by_actor_since(
        &self,
        _: i64,
    ) -> Result<(u64, std::collections::BTreeMap<String, u64>), String> {
        unimplemented!("只读替身")
    }
    async fn spend_by_upstream_and_actor(
        &self,
    ) -> Result<Vec<cog_core::UpstreamActorSpend>, String> {
        unimplemented!("只读替身")
    }
    async fn task_cost_quantile(&self, _: i64, _: f64) -> Result<Option<(u64, u64, i64)>, String> {
        unimplemented!("只读替身")
    }
    async fn task_has_started(&self, _: &str) -> Result<bool, String> {
        unimplemented!("只读替身")
    }
    async fn spend_by_task(&self, _: &[String]) -> Result<Vec<(String, u64)>, String> {
        unimplemented!("只读替身")
    }
    async fn spend_by_task_since(
        &self,
        _: &[String],
        _: i64,
    ) -> Result<Vec<(String, u64)>, String> {
        unimplemented!("只读替身")
    }
    async fn task_attribution(&self, _: &str) -> Result<Option<cog_core::TaskAttribution>, String> {
        unimplemented!("只读替身")
    }
    async fn unattributed_between(
        &self,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, String> {
        unimplemented!("只读替身")
    }
    async fn outcomes_between(
        &self,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::OutcomeCount>, String> {
        unimplemented!("只读替身")
    }
    async fn metering_between(
        &self,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::MeteringCount>, String> {
        unimplemented!("只读替身")
    }
    async fn skips_between(
        &self,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::SkippedWorkLine>, String> {
        unimplemented!("只读替身")
    }
    async fn verdicts_between(
        &self,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::SpendVerdictLine>, String> {
        unimplemented!("只读替身")
    }
}

/// 一个只记得住「谁声明过上限」的层级库。
struct FakeCeilings {
    user: Vec<(String, QuotaLimits)>,
    workspace: Vec<(String, QuotaLimits)>,
}

impl FakeCeilings {
    fn with(user: Vec<(&str, QuotaLimits)>, workspace: Vec<(&str, QuotaLimits)>) -> Arc<Self> {
        Arc::new(Self {
            user: user
                .into_iter()
                .map(|(id, limits)| (id.to_string(), limits))
                .collect(),
            workspace: workspace
                .into_iter()
                .map(|(id, limits)| (id.to_string(), limits))
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl HierarchyManager for FakeCeilings {
    async fn check(&self, _: &QuotaContext, _: u64) -> HierarchyDecision {
        unimplemented!("这一读不问判定")
    }
    async fn set_used(&self, _: QuotaScope, _: &str, _: u64) -> SFResult<()> {
        unimplemented!("只读替身")
    }
    async fn history(
        &self,
        _: QuotaScope,
        _: &str,
        _: u32,
    ) -> SFResult<Vec<cog_core::UsageHistoryEntry>> {
        unimplemented!("只读替身")
    }
    async fn limits(&self, scope: QuotaScope, target_id: &str) -> SFResult<Option<QuotaLimits>> {
        let table = match scope {
            QuotaScope::User => &self.user,
            QuotaScope::Workspace => &self.workspace,
            // 这两级根本没有花销路径，路由在到达这里之前就拒了。
            other => unimplemented!("{other:?} 没有读面"),
        };
        Ok(table
            .iter()
            .find(|(id, _)| id == target_id)
            .map(|(_, limits)| *limits))
    }
    async fn set_limits(&self, _: QuotaScope, _: &str, _: QuotaLimits) -> SFResult<()> {
        unimplemented!("只读替身")
    }
    async fn clear_limits(&self, _: QuotaScope, _: &str) -> SFResult<()> {
        unimplemented!("只读替身")
    }
}

fn state(ledger: Arc<FakeSpend>, ceilings: Option<Arc<FakeCeilings>>) -> Arc<GatewayState> {
    router_state::with_quota_stores(
        ledger,
        ceilings.map(|ceilings| ceilings as Arc<dyn HierarchyManager>),
    )
}

async fn get(state: Arc<GatewayState>, path: &str) -> (StatusCode, serde_json::Value) {
    let token = router_state::admin_bearer(&state, "spenders-probe").await;
    let response = create_router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("路由器接这一发");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// 一个人今天花了多少、离自己写的上限还有多远，按花销降序排在一起。
#[tokio::test]
async fn todays_spend_is_listed_per_person_beside_the_ceiling_they_declared() {
    let ledger = FakeSpend::with(
        vec![("u-quiet", 50), ("u-heavy", 900), ("u-mid", 300)],
        vec![],
    );
    let ceilings = FakeCeilings::with(vec![("u-heavy", QuotaLimits::new(800, 1000))], vec![]);
    let before = chrono::Utc::now().timestamp();
    let (status, body) = get(
        state(ledger.clone(), Some(ceilings)),
        "/api/v1/observability/token-usage/spenders",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    assert_eq!(body["scope"], "user");
    assert_eq!(body["total"], 1250, "总数是全部行之和，不是排在前面的几行");
    assert_eq!(body["ceilings_readable"], true);
    let rows = body["spenders"].as_array().expect("排行是数组");
    let order: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(
        order,
        vec!["u-heavy", "u-mid", "u-quiet"],
        "降序：读这一面就是来找花销最大的人的"
    );
    assert_eq!(rows[0]["tokens"], 900);
    assert_eq!(
        rows[0]["hard_limit"], 1000,
        "带上声明过的硬上限：没有它，排行只说谁花得多，不说谁快被拒了"
    );
    assert_eq!(rows[0]["remaining"], 100);
    assert_eq!(
        rows[1]["hard_limit"],
        serde_json::Value::Null,
        "没人声明过上限就是空，不是默认值——后者会让每一行都读成「配过」"
    );

    let since = ledger.since.load(Ordering::SeqCst);
    let now = chrono::Utc::now().timestamp();
    assert_eq!(
        since,
        cog_core::day::day_start_unix(now),
        "窗口必须与额度判定数的那个 UTC 日起点逐字相同"
    );
    assert!(since >= cog_core::day::day_start_unix(before));
}

/// 租户那一级读的是同一本账的另一个折叠，上限也从同一处来。
#[tokio::test]
async fn a_workspace_scope_reads_tenant_totals_with_their_own_ceilings() {
    let ledger = FakeSpend::with(
        vec![("u-heavy", 900)],
        vec![("ws-small", 20), ("ws-big", 700)],
    );
    let ceilings = FakeCeilings::with(
        vec![("u-heavy", QuotaLimits::new(800, 1000))],
        vec![("ws-big", QuotaLimits::new(500, 600))],
    );
    let (status, body) = get(
        state(ledger, Some(ceilings)),
        "/api/v1/observability/token-usage/spenders?scope=ws",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    assert_eq!(body["scope"], "ws");
    assert_eq!(body["total"], 720, "按人那一行的 900 不进租户这一读");
    let rows = body["spenders"].as_array().expect("排行是数组");
    assert_eq!(rows[0]["id"], "ws-big");
    assert_eq!(rows[0]["hard_limit"], 600);
    assert_eq!(
        rows[0]["remaining"], 0,
        "已经越过硬上限的租户，余量是 0 而不是负数"
    );
    assert_eq!(
        rows[1]["hard_limit"],
        serde_json::Value::Null,
        "人那一级的上限不许漏到租户这一读里来"
    );
}

/// 没有花销路径的层级拒答；答一份空账会被读成「今天没人花」。
#[tokio::test]
async fn a_level_no_call_is_charged_to_is_refused_rather_than_read_as_an_empty_day() {
    let ledger = FakeSpend::with(vec![("u-1", 10)], vec![]);
    for scope in ["team", "org", "global"] {
        let (status, body) = get(
            state(ledger.clone(), None),
            &format!("/api/v1/observability/token-usage/spenders?scope={scope}"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "{scope} 级没有任何花销路径，读出空账就是一句「没人花」的假话：{body}"
        );
        assert_eq!(
            body["code"], "quota_scope_not_enforced",
            "写面拒同一级用的是同一个词，读面不许换一个"
        );
    }

    let (status, body) = get(
        state(ledger, None),
        "/api/v1/observability/token-usage/spenders?scope=nonsense",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["code"], "unknown_quota_scope",
        "认不出的名字不许被读成默认的那一级"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("user, ws"),
        "报错要点名这一版能读的层级：{}",
        body["message"]
    );
}

/// 没有上限库时，响应自己说读不到上限，而不是让每行的空值冒充「没配过」。
#[tokio::test]
async fn a_state_without_a_ceiling_store_says_it_could_not_read_ceilings() {
    let ledger = FakeSpend::with(vec![("u-1", 10)], vec![]);
    let (status, body) = get(
        state(ledger, None),
        "/api/v1/observability/token-usage/spenders",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["ceilings_readable"], false,
        "这个部署没有上限库：每行的空值是「读不到」，不是「没人声明过」"
    );
    assert_eq!(body["spenders"][0]["hard_limit"], serde_json::Value::Null);
    assert_eq!(body["spenders"][0]["remaining"], serde_json::Value::Null);
}
