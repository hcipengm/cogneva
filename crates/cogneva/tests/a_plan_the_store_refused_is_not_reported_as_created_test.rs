//! A plan the shared store refused is not reported as created.
//!
//! `/api/v1/action-plan` promises the plan is persisted: the module doc says the
//! shared key-value store is what makes a plan survive a restart. The store
//! wrapper dropped every failure on the floor -- a refused `hset` was
//! discarded, a serialization failure was written as the empty string -- while
//! the route answered `201 Created` with the plan's new id. A caller reading
//! that reply would look for the plan after the next restart and not find it.
//!
//! The reads had the mirror shape: `get`/`list` fell back to the in-memory map
//! whenever the shared store could not be read. That map only holds what this
//! process has touched, so a failed read was answered as "that plan is not
//! there" or "there are no plans", which is a statement the store never made.
//! The suite below points the routes at a store that refuses every hash
//! operation: create must not come back as created, and neither read may come
//! back as an absence.
//!
//! The last two cases are about the same wrapper's third statement: a delete.
//! It asked the in-memory map what it removed, so a plan that lived only in the
//! shared store (every plan, after a restart) was deleted and reported as `404`
//! -- the reply said nothing was there while the row was gone. And a delete the
//! store refused was answered from the map as if it had happened.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use std::collections::HashMap;
use tokio::sync::Mutex;
use tower::ServiceExt;

use cog_core::{KeyValueStore, SFError, SFResult};
use cog_gateway::action_plan::{ActionPlanStore, StoredActionPlan};

use crate::router_state;

/// A shared store that refuses every hash operation. Every other method panics,
/// so a route that leans on something this suite does not model fails loudly
/// instead of passing by accident.
struct RefusesHashOperations;

fn refused() -> SFError {
    SFError::Database("connection refused".into())
}

/// A shared store that keeps the one hash the plans live under and serves it.
struct ServesThePlanHash {
    rows: Mutex<HashMap<String, String>>,
}

impl ServesThePlanHash {
    fn new() -> Self {
        Self {
            rows: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl KeyValueStore for RefusesHashOperations {
    async fn get(&self, _key: &str) -> SFResult<Option<String>> {
        unimplemented!()
    }
    async fn set(&self, _key: &str, _value: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn set_ex(&self, _key: &str, _value: &str, _ttl_secs: u64) -> SFResult<()> {
        unimplemented!()
    }
    async fn del(&self, _key: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn expire(&self, _key: &str, _ttl_secs: i64) -> SFResult<()> {
        unimplemented!()
    }
    async fn ttl(&self, _key: &str) -> SFResult<i64> {
        unimplemented!()
    }
    async fn incr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unimplemented!()
    }
    async fn decr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unimplemented!()
    }
    async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> SFResult<i64> {
        unimplemented!()
    }
    async fn sadd(&self, _key: &str, _member: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn srem(&self, _key: &str, _member: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn smembers(&self, _key: &str) -> SFResult<Vec<String>> {
        unimplemented!()
    }
    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> SFResult<()> {
        Err(refused())
    }
    async fn hget(&self, _key: &str, _field: &str) -> SFResult<Option<String>> {
        Err(refused())
    }
    async fn hdel(&self, _key: &str, _field: &str) -> SFResult<()> {
        Err(refused())
    }
    async fn hvals(&self, _key: &str) -> SFResult<Vec<String>> {
        Err(refused())
    }
    async fn hgetall(&self, _key: &str) -> SFResult<Vec<(String, String)>> {
        Err(refused())
    }
    async fn hlen(&self, _key: &str) -> SFResult<u64> {
        Err(refused())
    }
}

#[async_trait::async_trait]
impl KeyValueStore for ServesThePlanHash {
    async fn get(&self, _key: &str) -> SFResult<Option<String>> {
        unimplemented!()
    }
    async fn set(&self, _key: &str, _value: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn set_ex(&self, _key: &str, _value: &str, _ttl_secs: u64) -> SFResult<()> {
        unimplemented!()
    }
    async fn del(&self, _key: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn expire(&self, _key: &str, _ttl_secs: i64) -> SFResult<()> {
        unimplemented!()
    }
    async fn ttl(&self, _key: &str) -> SFResult<i64> {
        unimplemented!()
    }
    async fn incr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unimplemented!()
    }
    async fn decr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unimplemented!()
    }
    async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> SFResult<i64> {
        unimplemented!()
    }
    async fn sadd(&self, _key: &str, _member: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn srem(&self, _key: &str, _member: &str) -> SFResult<()> {
        unimplemented!()
    }
    async fn smembers(&self, _key: &str) -> SFResult<Vec<String>> {
        unimplemented!()
    }
    async fn hset(&self, _key: &str, field: &str, value: &str) -> SFResult<()> {
        self.rows
            .lock()
            .await
            .insert(field.to_string(), value.to_string());
        Ok(())
    }
    async fn hget(&self, _key: &str, field: &str) -> SFResult<Option<String>> {
        Ok(self.rows.lock().await.get(field).cloned())
    }
    async fn hdel(&self, _key: &str, field: &str) -> SFResult<()> {
        self.rows.lock().await.remove(field);
        Ok(())
    }
    async fn hvals(&self, _key: &str) -> SFResult<Vec<String>> {
        Ok(self.rows.lock().await.values().cloned().collect())
    }
    async fn hgetall(&self, _key: &str) -> SFResult<Vec<(String, String)>> {
        Ok(self
            .rows
            .lock()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    async fn hlen(&self, _key: &str) -> SFResult<u64> {
        Ok(self.rows.lock().await.len() as u64)
    }
}

fn refusing_state() -> Arc<cog_gateway::GatewayState> {
    router_state::with_action_plan_store(ActionPlanStore::with_store(Arc::new(
        RefusesHashOperations,
    )))
}

fn serving_state(store: Arc<ServesThePlanHash>) -> Arc<cog_gateway::GatewayState> {
    router_state::with_action_plan_store(ActionPlanStore::with_store(store))
}

async fn body_of(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    String::from_utf8_lossy(&bytes).into_owned()
}

/// 建计划：存储没存下，答复不许说建好了。
#[tokio::test]
async fn a_plan_the_store_refused_is_not_reported_as_created() {
    let state = refusing_state();
    let token = router_state::admin_bearer(&state, "action-plan-probe").await;
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/action-plan")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "goal": "keep this plan across a restart" }).to_string(),
                ))
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");

    let status = response.status();
    let body = body_of(response).await;
    assert!(
        !status.is_success(),
        "存储拒了这次写入，计划没有落下，答复却说自己建好了：{status} {body}"
    );
}

/// 列计划：存储读不出来，答复不许是一张空表。
#[tokio::test]
async fn a_listing_the_store_could_not_read_is_not_an_empty_listing() {
    let state = refusing_state();
    let token = router_state::admin_bearer(&state, "action-plan-probe").await;
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/action-plan")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");

    let status = response.status();
    let body = body_of(response).await;
    assert!(
        !status.is_success(),
        "读不到存储时答空表，读者会把「问不到」读成「没有计划」：{status} {body}"
    );
    assert!(
        !body.contains("\"plans\":[]"),
        "空表正是这一面在「一条都没有」时给的读数：{body}"
    );
}

/// 读一份计划：存储读不出来，答复不许是「没有这份计划」。
#[tokio::test]
async fn a_plan_that_could_not_be_read_is_not_reported_as_missing() {
    let state = refusing_state();
    let token = router_state::admin_bearer(&state, "action-plan-probe").await;
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/action-plan/p1")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");

    let status = response.status();
    let body = body_of(response).await;
    assert_ne!(
        status,
        StatusCode::NOT_FOUND,
        "存储没答，答复却替它说「没有这份计划」：{status} {body}"
    );
    assert!(
        !status.is_success(),
        "读不到存储，答复却给出一份计划：{status} {body}"
    );
}

/// 删一份只活在共享存储里的计划：删掉了就说删掉了。
///
/// 重启之后内存表是空的，而这正是共享存储存在的理由。答复从内存表取证，
/// 会把一次确实发生的删除说成「没有这份计划」。
#[tokio::test]
async fn a_plan_that_lived_only_in_the_shared_store_is_deleted_and_says_so() {
    let served = Arc::new(ServesThePlanHash::new());
    let plan = StoredActionPlan {
        id: "p1".into(),
        plan: cog_core::ActionPlan {
            goal: "written before the last restart".into(),
            ..Default::default()
        },
        created_at: chrono::Utc::now(),
        validation: None,
    };
    served
        .hset(
            "sf:action_plans",
            "p1",
            &serde_json::to_string(&plan).expect("the plan serializes"),
        )
        .await
        .expect("the seeding write lands");

    let state = serving_state(served.clone());
    let token = router_state::admin_bearer(&state, "action-plan-probe").await;
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/action-plan/p1")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");

    let status = response.status();
    let body = body_of(response).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "这一行确实删掉了，答复却说没有这份计划：{status} {body}"
    );
    assert_eq!(
        served
            .hget("sf:action_plans", "p1")
            .await
            .expect("the store answers"),
        None,
        "答复说删掉了，存储里那一行却还在"
    );
}

/// 删计划：存储拒了这次删除，答复不许说删掉了。
#[tokio::test]
async fn a_delete_the_store_refused_is_not_reported_as_deleted() {
    let state = refusing_state();
    let token = router_state::admin_bearer(&state, "action-plan-probe").await;
    let response = cog_gateway::create_router(state)
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/action-plan/p1")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");

    let status = response.status();
    let body = body_of(response).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "存储拒了这次删除，计划还在，答复却说自己处理完了：{status} {body}"
    );
}
