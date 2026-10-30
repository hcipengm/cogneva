//! 按任务预算的管理面：声明、读取、撤销、判定、记账。
//!
//! 与层级那套一样跑在内存存储上（[`cog_core::KeyValueStore`] 契约），不需要 Redis：
//! 同一份用例在开发机、CI 与进化沙箱里跑的是同一件事，而不是在没有服务的地方跳过。
//!
//! 这一组用例要证明的是几件**没有这件东西时会静默成立**的事：
//!
//! - 没人声明过预算的任务放行，且 `remaining` 是 `None` 而不是 0——把「没有上限」
//!   读成「上限为零」会让每一件没被预算管着的工作都被拒掉；
//! - 记过账之后同日再判，超了硬上限就拒，并且带着到下一个 UTC 午夜的等待；
//! - 计数器身上真的挂着一个当天到期的 TTL：没有它，一条日上限会变成终身上限，
//!   而读数上它与「今天花得多」一模一样；
//! - 改上限**不是**发一笔新额度：已经花掉的那部分照样算数；
//! - 存储里那条记录解析不了时是错误，不是「没有预算」——后者等于静默撤掉一条预算。

use cog_core::{KeyValueStore, QuotaLimits};
use cog_quota::TaskBudgetManager;
use std::sync::{Arc, Mutex};

/// 内存存储：字符串、计数器与过期时间。
///
/// 集合与哈希没实现——预算这一面只用字符串、计数与 TTL，碰到别的形状就说明这一段
/// 代码用了一个它没打算用的能力。
#[derive(Default)]
struct FakeStore {
    strings: Mutex<std::collections::HashMap<String, String>>,
    /// 每个计数器被写时带的 TTL（秒）。`incr_ex` 只在建键那一次设过期，这里照抄。
    ttls: Mutex<std::collections::HashMap<String, u64>>,
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

    async fn set_ex(&self, key: &str, value: &str, ttl_secs: u64) -> cog_core::SFResult<()> {
        self.set(key, value).await?;
        self.ttls.lock().unwrap().insert(key.to_string(), ttl_secs);
        Ok(())
    }

    async fn del(&self, key: &str) -> cog_core::SFResult<()> {
        self.strings.lock().unwrap().remove(key);
        self.ttls.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, key: &str, ttl_secs: i64) -> cog_core::SFResult<()> {
        if ttl_secs > 0 {
            self.ttls
                .lock()
                .unwrap()
                .insert(key.to_string(), ttl_secs as u64);
        }
        Ok(())
    }

    async fn ttl(&self, key: &str) -> cog_core::SFResult<i64> {
        Ok(self
            .ttls
            .lock()
            .unwrap()
            .get(key)
            .map(|v| *v as i64)
            .unwrap_or(-1))
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

    async fn decr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        self.incr(key, -by).await
    }

    async fn incr_ex(&self, key: &str, by: i64, ttl_secs: u64) -> cog_core::SFResult<i64> {
        let created = {
            let strings = self.strings.lock().unwrap();
            !strings.contains_key(key)
        };
        let next = self.incr(key, by).await?;
        // 只在建键那一次设过期，与契约一致：后来的自增不刷新它。
        if created {
            self.ttls.lock().unwrap().insert(key.to_string(), ttl_secs);
        }
        Ok(next)
    }

    async fn sadd(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("预算这一面不用集合")
    }

    async fn srem(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("预算这一面不用集合")
    }

    async fn smembers(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("预算这一面不用集合")
    }

    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> cog_core::SFResult<()> {
        unreachable!("预算这一面不用哈希")
    }

    async fn hget(&self, _key: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        unreachable!("预算这一面不用哈希")
    }

    async fn hdel(&self, _key: &str, _field: &str) -> cog_core::SFResult<()> {
        unreachable!("预算这一面不用哈希")
    }

    async fn hvals(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("预算这一面不用哈希")
    }

    async fn hgetall(&self, _key: &str) -> cog_core::SFResult<Vec<(String, String)>> {
        unreachable!("预算这一面不用哈希")
    }

    async fn hlen(&self, _key: &str) -> cog_core::SFResult<u64> {
        unreachable!("预算这一面不用哈希")
    }
}

/// 一个正午：用例里所有「现在」都从这里推，免得跨日在午夜附近跑出两种结果。
const NOON: i64 = 1_600_000_000 - (1_600_000_000 % 86_400) + 12 * 3600;

fn manager() -> TaskBudgetManager {
    TaskBudgetManager::new(Arc::new(FakeStore::default()))
}

#[tokio::test]
async fn a_task_without_a_declared_budget_has_no_ceiling_and_is_let_through() {
    let m = manager();
    let d = m.check("task-a", 10_000, NOON).await.unwrap();
    assert!(d.allowed, "没有预算的任务不该被拒");
    assert_eq!(d.limits, None);
    assert_eq!(
        d.remaining, None,
        "没有上限时没有「还剩多少」，0 会被读成「已经花完」"
    );
    assert!(!d.warning);
    assert_eq!(d.wait_secs, None);
}

#[tokio::test]
async fn charges_accumulate_and_the_ceiling_refuses_the_call_that_would_cross_it() {
    let m = manager();
    m.set_budget("task-b", QuotaLimits::new(80, 100))
        .await
        .unwrap();

    assert_eq!(m.charge("task-b", 60, NOON).await.unwrap(), 60);
    assert!(
        m.check("task-b", 40, NOON).await.unwrap().allowed,
        "60+40 恰好到界，放行"
    );

    assert_eq!(m.charge("task-b", 40, NOON).await.unwrap(), 100);
    let d = m.check("task-b", 1, NOON).await.unwrap();
    assert!(!d.allowed);
    assert_eq!(d.spent_today, 100);
    assert_eq!(d.remaining, Some(0));
    assert_eq!(
        d.wait_secs,
        Some(43_200),
        "正午被拒，等到下一个 UTC 午夜还有半天"
    );
}

#[tokio::test]
async fn the_counter_carries_a_ttl_that_ends_at_the_next_utc_midnight() {
    let store = Arc::new(FakeStore::default());
    let m = TaskBudgetManager::new(store.clone());
    m.set_budget("task-c", QuotaLimits::new(80, 100))
        .await
        .unwrap();
    m.charge("task-c", 10, NOON).await.unwrap();

    // 计数器的过期必须落在当天末尾：没有它，这条日上限就是终身上限，
    // 而「今天花得多」与「这条预算再也不会重置」在读数上同形。
    assert_eq!(
        store.ttl("quota:task:used:task-c").await.unwrap(),
        43_200,
        "正午记的账，计数器活到当天结束"
    );
    // 限值键**不**带 TTL：预算是对这件工作整体说的，午夜不该把声明本身也抹掉。
    assert_eq!(
        store.ttl("quota:task:limits:task-c").await.unwrap(),
        -1,
        "预算声明没有过期时间"
    );
}

#[tokio::test]
async fn raising_the_ceiling_is_not_a_fresh_grant() {
    let m = manager();
    m.set_budget("task-d", QuotaLimits::new(80, 100))
        .await
        .unwrap();
    m.charge("task-d", 90, NOON).await.unwrap();

    m.set_budget("task-d", QuotaLimits::new(200, 300))
        .await
        .unwrap();

    let d = m.check("task-d", 0, NOON).await.unwrap();
    assert_eq!(d.spent_today, 90, "改上限不该把今天已经花掉的那部分抹掉");
    assert_eq!(d.limits, Some(QuotaLimits::new(200, 300)));
    assert!(d.allowed);
}

#[tokio::test]
async fn clearing_the_budget_returns_the_task_to_unbudgeted() {
    let m = manager();
    m.set_budget("task-e", QuotaLimits::new(1, 1))
        .await
        .unwrap();
    m.charge("task-e", 5, NOON).await.unwrap();
    assert!(!m.check("task-e", 0, NOON).await.unwrap().allowed);

    m.clear_budget("task-e").await.unwrap();
    let d = m.check("task-e", 0, NOON).await.unwrap();
    assert!(d.allowed);
    assert_eq!(d.limits, None);
    // 撤销预算不动计数器：再声明一次时，今天已经花掉的仍然算数。
    assert_eq!(d.spent_today, 5);
}

#[tokio::test]
async fn a_stored_budget_that_does_not_parse_is_an_error_not_an_absent_budget() {
    let store = Arc::new(FakeStore::default());
    store
        .set("quota:task:limits:task-f", "{not json")
        .await
        .unwrap();
    let m = TaskBudgetManager::new(store);

    let err = m.check("task-f", 0, NOON).await;
    assert!(
        err.is_err(),
        "读不出那条记录时要说「坏了」，不能说「没人声明过」——后者等于静默撤掉一条预算"
    );
}

#[tokio::test]
async fn the_request_count_is_kept_beside_the_tokens() {
    let m = manager();
    m.charge("task-g", 1, NOON).await.unwrap();
    m.charge("task-g", 2, NOON).await.unwrap();
    assert_eq!(m.requests_today("task-g").await, 2);
    assert_eq!(m.spent_today("task-g").await, 3);
}
