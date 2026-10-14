//! The quota's decisions have to reach the process's metrics face.
//!
//! What is asserted here is the whole chain the reading needs and nothing else:
//! a decision moves a counter, the counter is what the observable reports, the
//! reported family renders under the name the alert rule reads, and the handle
//! the reader holds is the same one the deciding manager counts into — the last
//! one because the gateway calls the manager through a *clone* of it
//! (`Arc<dyn cog_core::QuotaManager>`), so a manager whose counters were
//! per-instance rather than shared would leave the scrape reporting zero for a
//! quota that is refusing requests all day.
//!
//! The store is in memory: the manager speaks the core key-value contract, so
//! the suite runs without Redis on a developer machine, in CI and in the
//! evolution sandbox instead of skipping wherever no server is up.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cog_core::{KeyValueStore, Observable};
use cog_quota::{QuotaManager, QuotaObservable};

/// An in-memory [`KeyValueStore`] holding the shapes the quota manager uses:
/// strings with an optional expiry it does not honour, and the integer
/// increments `finalize` applies to the daily counters. The set and hash
/// operations are the driver's, and no test here reaches them.
#[derive(Default)]
struct FakeStore {
    strings: Mutex<HashMap<String, String>>,
}

#[async_trait::async_trait]
impl KeyValueStore for FakeStore {
    async fn get(&self, key: &str) -> cog_core::SFResult<Option<String>> {
        Ok(self.strings.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> cog_core::SFResult<()> {
        self.strings
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, _ttl_secs: u64) -> cog_core::SFResult<()> {
        self.set(key, value).await
    }

    async fn del(&self, key: &str) -> cog_core::SFResult<()> {
        self.strings.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> cog_core::SFResult<i64> {
        Ok(-1)
    }

    async fn incr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        let mut strings = self.strings.lock().unwrap();
        let next = strings
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
            + by;
        strings.insert(key.to_string(), next.to_string());
        Ok(next)
    }

    async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> cog_core::SFResult<i64> {
        unreachable!("the quota manager expires counters separately")
    }

    async fn decr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        self.incr(key, -by).await
    }

    async fn sadd(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("the quota manager does not use sets")
    }

    async fn srem(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("the quota manager does not use sets")
    }

    async fn smembers(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("the quota manager does not use sets")
    }

    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> cog_core::SFResult<()> {
        unreachable!("the quota manager does not use hash fields")
    }

    async fn hget(&self, _key: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        unreachable!("the quota manager does not use hash fields")
    }

    async fn hdel(&self, _key: &str, _field: &str) -> cog_core::SFResult<()> {
        unreachable!("the quota manager does not use hash fields")
    }

    async fn hvals(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("the quota manager does not use hash fields")
    }

    async fn hgetall(&self, _key: &str) -> cog_core::SFResult<Vec<(String, String)>> {
        unreachable!("the quota manager does not read whole hashes")
    }

    async fn hlen(&self, _key: &str) -> cog_core::SFResult<u64> {
        unreachable!("the quota manager does not use hash fields")
    }
}

fn manager(daily_limit: u64) -> QuotaManager {
    QuotaManager::new(Arc::new(FakeStore::default()), daily_limit)
}

/// The value of the cell carrying `allowed=<value>`, or `None` when the family
/// published no such cell — which is the failure the assertion names: a
/// refusal count that is absent reads downstream exactly like a quota nobody
/// has ever asked.
fn cell(metrics: &[cog_core::RawMetric], allowed: &str) -> Option<f64> {
    metrics
        .iter()
        .find(|m| {
            m.name == cog_quota::observable::PRE_CHECKS_TOTAL
                && m.labels.get("allowed").map(String::as_str) == Some(allowed)
        })
        .map(|m| m.value)
}

#[tokio::test]
async fn a_decision_moves_the_counter_that_reports_it() {
    let quota = manager(100);
    let observable = QuotaObservable::new(quota.readings());

    // An empty process reports both cells at zero rather than reporting
    // nothing: "no refusals" is the reading an operator needs to tell a quiet
    // quota from an unwired one.
    let before = observable.collect_metrics("").await.unwrap();
    assert_eq!(cell(&before, "true"), Some(0.0), "{before:?}");
    assert_eq!(cell(&before, "false"), Some(0.0), "{before:?}");

    assert!(quota.pre_check("u1", None, 50).await.allowed);
    // 50 of the 100 are gone, so a check costing 100 cannot be admitted.
    assert!(!quota.pre_check("u1", None, 100).await.allowed);

    let after = observable.collect_metrics("").await.unwrap();
    assert_eq!(cell(&after, "true"), Some(1.0), "{after:?}");
    assert_eq!(cell(&after, "false"), Some(1.0), "{after:?}");
}

#[tokio::test]
async fn the_counters_are_shared_with_every_clone_of_the_manager() {
    // The gateway holds its own clone of the manager and refuses through that
    // one, while the observable was built over the original. Per-instance
    // counters would leave this reading at zero through every refusal.
    let quota = manager(100);
    let observable = QuotaObservable::new(quota.readings());
    let as_the_gateway_holds_it: Arc<dyn cog_core::QuotaManager> = Arc::new(quota.clone());

    assert!(
        !as_the_gateway_holds_it
            .pre_check("u1", None, 101)
            .await
            .allowed
    );

    let metrics = observable.collect_metrics("").await.unwrap();
    assert_eq!(cell(&metrics, "false"), Some(1.0), "{metrics:?}");
}

#[tokio::test]
async fn the_family_renders_as_a_counter_under_the_name_the_rule_reads() {
    let quota = manager(100);
    let observable = QuotaObservable::new(quota.readings());
    assert!(!quota.pre_check("u1", None, 101).await.allowed);

    let rendered = cog_core::observability_text::render_raw_metrics(
        &observable.collect_metrics("").await.unwrap(),
    );

    assert!(
        rendered.contains(&format!(
            "# TYPE {} counter",
            cog_quota::observable::PRE_CHECKS_TOTAL
        )),
        "the rule reads this family with a range function, which is only defined on a \
         counter: {rendered}"
    );
    assert!(
        rendered.contains(&format!(
            "{}{{allowed=\"false\"}} 1",
            cog_quota::observable::PRE_CHECKS_TOTAL
        )),
        "{rendered}"
    );
}

#[tokio::test]
async fn the_reading_is_the_same_under_every_dimension() {
    // The collector asks an observable only the dimensions it declares. Nothing
    // is declared here, so this pins the pair the declaration rests on: an
    // empty declaration is the truth only while the reading does not vary by
    // dimension -- otherwise the collector would be asking once and publishing
    // one dimension's number for all of them.
    let quota = manager(100);
    let observable = QuotaObservable::new(quota.readings());
    quota.pre_check("u1", None, 10).await;

    assert!(observable.available_dimensions().is_empty());
    let under_a_dimension = observable.collect_metrics("D1").await.unwrap();
    let under_none = observable.collect_metrics("").await.unwrap();
    assert_eq!(cell(&under_a_dimension, "true"), cell(&under_none, "true"));
}
