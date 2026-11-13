//! A key-value store that keeps everything in this process.
//!
//! The quota hierarchy reads and writes its cells through `KeyValueStore`, so a
//! suite that judges a quota reading needs one that is there. What it must not
//! be is a store with opinions: a suite is judging the hierarchy's arithmetic,
//! and a fixture that drops writes, reorders reads or expires on its own would
//! be judging itself.
//!
//! Methods the hierarchy never calls are `unreachable!()` rather than no-ops: a
//! silent `Ok(())` would let a future caller believe it stored something, and
//! the question "did the hierarchy start using collections?" would be answered
//! by a passing suite instead of a panic naming the method.

use std::collections::HashMap;
use std::sync::Mutex;

use cog_core::KeyValueStore;

// 同目录其他夹具的做法：一份 `common/` 会被每个用它的测试二进制各编一遍，
// 只在其中几个里用到的类型在其余二进制里看起来是死的。
#[allow(dead_code)]
#[derive(Default)]
pub struct MemoryStore {
    rows: Mutex<HashMap<String, String>>,
}

#[async_trait::async_trait]
impl KeyValueStore for MemoryStore {
    async fn get(&self, key: &str) -> cog_core::SFResult<Option<String>> {
        Ok(self.rows.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> cog_core::SFResult<()> {
        self.rows
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, _ttl_secs: u64) -> cog_core::SFResult<()> {
        self.set(key, value).await
    }

    async fn del(&self, key: &str) -> cog_core::SFResult<()> {
        self.rows.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> cog_core::SFResult<()> {
        // 过期由键的 TTL 决定，而这个替身不实现时间：套件关心的读写在同一个
        // 瞬间完成，日界一到真正归零的是存储上的键，不是这里的算术。
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> cog_core::SFResult<i64> {
        Ok(-1)
    }

    async fn incr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        let mut rows = self.rows.lock().unwrap();
        let next = rows
            .get(key)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
            + by;
        rows.insert(key.to_string(), next.to_string());
        Ok(next)
    }

    async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> cog_core::SFResult<i64> {
        unreachable!("额度层级不用带过期的自增")
    }

    async fn decr(&self, key: &str, by: i64) -> cog_core::SFResult<i64> {
        self.incr(key, -by).await
    }

    async fn sadd(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("额度层级不用集合")
    }

    async fn srem(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        unreachable!("额度层级不用集合")
    }

    async fn smembers(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("额度层级不用集合")
    }

    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> cog_core::SFResult<()> {
        unreachable!("额度层级不用哈希字段")
    }

    async fn hget(&self, _key: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        unreachable!("额度层级不用哈希字段")
    }

    async fn hdel(&self, _key: &str, _field: &str) -> cog_core::SFResult<()> {
        unreachable!("额度层级不用哈希字段")
    }

    async fn hvals(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        unreachable!("额度层级不用哈希字段")
    }

    async fn hgetall(&self, _key: &str) -> cog_core::SFResult<Vec<(String, String)>> {
        unreachable!("额度层级不整读哈希")
    }

    async fn hlen(&self, _key: &str) -> cog_core::SFResult<u64> {
        unreachable!("额度层级不用哈希字段")
    }
}
