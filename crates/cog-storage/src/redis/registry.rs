//! Redis-backed and in-memory [`AgentRegistry`] implementations.
//! Moved from `cog-core` so that `cog-core` only contains the trait + data
//! structures and concrete implementations live in `sf-db`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisError};
use std::collections::HashMap;
use std::sync::RwLock;

use cog_core::{AgentRegistration, AgentRegistry, EmployeeRecord, SFError, SFResult};

/// 注册表里一个 agent 的 key 前缀，后面接 agent id。
///
/// 注册表的读写都在这个 crate 里，可「这个会话还有没有主人」这个问题的另外几个
/// 读者在 Lua 里：WAL 回收与收件箱回收都拿它判断一个会话是不是还活着。脚本里
/// 再拼一遍前缀时，改名只改一侧不会有任何报错——脚本会对每一个会话都答「没有
/// 主人」，于是把还有主人的日志与收件箱当成孤儿删掉。所以前缀从这里作为参数传进
/// 脚本，不在脚本里写第二遍。
pub const AGENT_KEY_PREFIX: &str = "orchestrator:agents:";

fn agent_key(agent_id: &str) -> String {
    format!("{AGENT_KEY_PREFIX}{agent_id}")
}

const REGISTRY_INDEX_KEY: &str = "orchestrator:agents:index";

/// 名册行住在哪。
///
/// 与注册表不是同一份：注册表每一条都带 TTL，进程一死键就没了；名册行是持久
/// 的，退役也不删。这正是「离职」与「从来没有过这个人」在读数上分得开的唯一
/// 凭据，所以它不能挂在注册表那条会过期的键上。
///
/// 键里不带 TTL，读的是 `hgetall`；写只有两处，都在注册表的写点旁边——建行随
/// `register`，盖墓碑随 `deregister` 与过期回收。名册因此没有第二个写点，也
/// 不会漏掉某一类会调模型的智能体。
pub const EMPLOYEES_KEY: &str = "cogneva:employees";

/// 工号的计数器。
///
/// 只增、不重置、不回收：工号是发出去的编制，不是每次读的时候重新算出来的
/// 读数。重置它会让两个不同的人顶着同一个工号——而工号唯一正是「员工不重名」
/// 这条要求唯一站得住的地方。
///
/// 它不放在 [`EMPLOYEES_KEY`] 那张 hash 里：hash 装的是行，计数器装的是下一
/// 个号，两者混在一起时 `hgetall` 会把计数器当成一个人的行读出来。
pub const EMPLOYEE_SEQ_KEY: &str = "cogneva:employees:seq";

// ─── Redis implementation ───

/// Redis-backed [`AgentRegistry`].
pub struct RedisAgentRegistry {
    connection: ConnectionManager,
    ttl_seconds: u64,
}

impl RedisAgentRegistry {
    pub fn new(connection: ConnectionManager) -> Self {
        Self {
            connection,
            ttl_seconds: 30,
        }
    }

    pub fn with_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    pub async fn from_url(url: &str) -> SFResult<Self> {
        let client = redis::Client::open(url).map_err(|e| SFError::Redis(e.to_string()))?;
        let connection = crate::redis::connect(&client)
            .await
            .map_err(|e| SFError::Redis(e.to_string()))?;
        Ok(Self::new(connection))
    }

    /// Seconds left on one unit's presence key, with Redis' own codes: `-2`
    /// when the key does not exist, `-1` when it exists without an expiry.
    ///
    /// [`AgentRegistry::get`] answers "who is registered with this name" and
    /// collapses "no key" into `None` alongside every other reason a row could
    /// not be read. A caller deciding whether a unit is *still* present has to
    /// tell a missing key from a live one, and the index is no help: a retired
    /// name leaves it only when some reader walks the roster and finds the
    /// record gone, so membership is "nothing has looked since it went", not a
    /// liveness reading.
    pub async fn ttl_secs(&self, agent_id: &str) -> SFResult<i64> {
        let key = agent_key(agent_id);
        let mut conn = self.connection.clone();
        conn.ttl(&key)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))
    }

    /// How long a record may sit without a heartbeat before its own contents
    /// stop being evidence of life.
    ///
    /// The key's expiry is supposed to answer this: Redis deletes the key, and
    /// reading `None` is the proof. That equivalence only holds for keys that
    /// carry a TTL, and a key written by `SET` followed by `EXPIRE` is left
    /// without one when the writer dies in between. Those records are not
    /// hypothetical: a live cluster held 20 registrations at `TTL -1` with a
    /// three-day-old `last_heartbeat`, and every reader listing the registry
    /// (the agent listing route among them) reported them as live agents that
    /// could be addressed. A caller routing work to one would be queueing for a
    /// process that is not there, which is the one thing a liveness answer must
    /// not say.
    ///
    /// Three missed beats, floored at 90s: the floor keeps the bound meaningful
    /// for a registry configured with a very short TTL, and three beats means a
    /// single lost heartbeat does not retire a working agent.
    fn stale_after_secs(&self) -> i64 {
        (self.ttl_seconds as i64).saturating_mul(3).max(90)
    }

    /// Whether the record itself says its writer has stopped beating.
    fn is_stale(&self, reg: &AgentRegistration) -> bool {
        let age = Utc::now().signed_duration_since(reg.last_heartbeat);
        age.num_seconds() > self.stale_after_secs()
    }

    /// Drop a record that is no longer evidence of life: the key and its id in
    /// the index. Best effort — the caller has already decided to report the
    /// agent absent, and a failed cleanup must not turn that answer into an
    /// error. The index entry goes with the key because `list` reconstructs the
    /// roster from the index, so a surviving id there is the same dead name
    /// arriving through the other door.
    async fn forget(&self, agent_id: &str) {
        let mut conn = self.connection.clone();
        let key = agent_key(agent_id);
        let _: Result<(), RedisError> = conn.del(&key).await;
        let _: Result<(), RedisError> = conn.srem(REGISTRY_INDEX_KEY, agent_id).await;
        // 键没了就是这个进程不再服务的证据，名册要在同一刻记下这一笔：否则
        // 一个崩掉的员工永远停在「在职、但此刻不在岗」，而「离职」这一格只有
        // 走过 `deregister` 的人才写得出来——崩掉的那些恰好是最该被看见的。
        self.tombstone_employee(agent_id).await;
    }

    /// 名册上记一笔：这个人此刻在。建行只写一次 `first_seen`。
    ///
    /// 三个事实在一段脚本里合成一次写：是不是第一次见（决定 `first_seen`）、
    /// 现在的用途标签、以及「上一次服役留下的墓碑要擦掉」。分成三条命令的
    /// 话，中间那一刻读名册的人会看到一个既退役又在心跳的人。
    async fn touch_employee(&self, registration: &AgentRegistration) {
        let mut conn = self.connection.clone();
        let now = Utc::now().to_rfc3339();
        let script = redis::Script::new(
            "local cur = redis.call('HGET', KEYS[1], ARGV[1]) \
             local row \
             if cur then \
                 row = cjson.decode(cur) \
             else \
                 row = { first_seen = ARGV[2] } \
             end \
             if row.number == nil or row.number == cjson.null then \
                 row.number = redis.call('INCR', KEYS[2]) \
             end \
             row.agent_id = ARGV[1] \
             row.role = ARGV[3] \
             row.last_seen = ARGV[2] \
             row.retired_at = nil \
             redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(row)) \
             return 1",
        );
        // 名册写失败不能挡住注册：注册是「这个进程现在能不能被叫到」，名册是
        // 「这个人在不在册」。前者失败是停摆，后者失败是一行读数的缺口，两件
        // 事的价钱不一样，所以这里只留一条日志。
        if let Err(e) = script
            .key(EMPLOYEES_KEY)
            .key(EMPLOYEE_SEQ_KEY)
            .arg(&registration.agent_id)
            .arg(&now)
            .arg(&registration.role)
            .invoke_async::<i64>(&mut conn)
            .await
        {
            tracing::warn!(
                agent_id = %registration.agent_id,
                error = %e,
                "the employee row could not be written; this person is registered but not on the roster"
            );
        }
    }

    /// 盖一块墓碑：这个人不再服役，但他在册上这一行留着。
    ///
    /// 没有行就什么都不做——对从没注册过的名字写一行，等于凭空造出一个人，
    /// 而名册最贵的错误之一就是「有名字、没人」。
    async fn tombstone_employee(&self, agent_id: &str) {
        let mut conn = self.connection.clone();
        let now = Utc::now().to_rfc3339();
        let script = redis::Script::new(
            "local cur = redis.call('HGET', KEYS[1], ARGV[1]) \
             if not cur then return 0 end \
             local row = cjson.decode(cur) \
             row.retired_at = ARGV[2] \
             row.last_seen = ARGV[2] \
             redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(row)) \
             return 1",
        );
        if let Err(e) = script
            .key(EMPLOYEES_KEY)
            .arg(agent_id)
            .arg(&now)
            .invoke_async::<i64>(&mut conn)
            .await
        {
            tracing::warn!(
                agent_id = %agent_id,
                error = %e,
                "the retirement could not be written to the roster; this person stays on it as active"
            );
        }
    }

    /// Take the index entry off a name whose record has already gone.
    ///
    /// The expiry is the writer's, so a record usually disappears without any
    /// reader being involved: Redis deletes the key, and the name stays an
    /// index member for good. Membership is what `list` walks, so every such
    /// name is charged to every reader on every pass — `list` answers "who is
    /// registered" by reading one key per member, and a name whose key is gone
    /// can never answer again. Measured 2026-10-26: 283 members, 20 of them
    /// with a live record.
    ///
    /// The check and the removal are one script because `register` writes the
    /// record *before* it adds the member: a check made outside the script can
    /// land between those two commands, see no record, and drop the member of
    /// an agent that is registering at that very moment — a live agent gone
    /// from the roster while its key is right there. Inside one script the
    /// interleaving is either "member not yet added" (nothing to remove) or
    /// "record already there" (left alone).
    ///
    /// Best effort, like `forget`: the caller is answering "is this agent
    /// here", and a failed cleanup must not turn that answer into an error.
    async fn take_index_entry_if_record_is_gone(&self, agent_id: &str) {
        let mut conn = self.connection.clone();
        let script = redis::Script::new(
            "if redis.call('EXISTS', KEYS[1]) == 0 then \
                 return redis.call('SREM', KEYS[2], ARGV[1]) \
             end \
             return 0",
        );
        let _: Result<i64, RedisError> = script
            .key(agent_key(agent_id))
            .key(REGISTRY_INDEX_KEY)
            .arg(agent_id)
            .invoke_async(&mut conn)
            .await;
    }
}

#[async_trait]
impl AgentRegistry for RedisAgentRegistry {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
        let key = agent_key(&registration.agent_id);
        let payload = serde_json::to_string(registration).map_err(SFError::Serialization)?;
        let mut conn = self.connection.clone();
        // One command, not `SET` then `EXPIRE`: between those two a process can
        // die and leave a record that never expires, which every reader then
        // lists as a live agent forever. See `stale_after_secs`.
        let _: () = conn
            .set_ex(&key, payload, self.ttl_seconds)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let _: () = conn
            .sadd(REGISTRY_INDEX_KEY, &registration.agent_id)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        // 名册行跟着注册走，不另设入口：凡是会调模型的智能体都要先注册，所以
        // 「谁在册上」与「谁会注册」在这里是同一件事，不可能只做对一半。
        self.touch_employee(registration).await;
        Ok(())
    }

    async fn heartbeat(&self, agent_id: &str) -> SFResult<()> {
        let key = agent_key(agent_id);
        let mut conn = self.connection.clone();
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let Some(raw) = raw else {
            return Err(SFError::Agent(format!(
                "heartbeat: agent {agent_id} not registered (key expired?)"
            )));
        };
        let mut reg: AgentRegistration =
            serde_json::from_str(&raw).map_err(SFError::Serialization)?;
        reg.last_heartbeat = Utc::now();
        let payload = serde_json::to_string(&reg).map_err(SFError::Serialization)?;
        // Atomic, for the same reason as `register`: a beat that lands on a
        // record whose TTL was lost is exactly the moment to re-arm it, and
        // splitting the write from the expiry reopens the window it closes.
        let _: () = conn
            .set_ex(&key, payload, self.ttl_seconds)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> SFResult<()> {
        let key = agent_key(agent_id);
        let mut conn = self.connection.clone();
        let _: () = conn
            .del(&key)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let _: () = conn
            .srem(REGISTRY_INDEX_KEY, agent_id)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        // 注册表上删键，名册上盖墓碑。这两件事分开写不是冗余：注册记录回答
        // 「现在叫得到吗」，名册回答「这个人存在过吗」，删掉前者不该抹掉后者。
        self.tombstone_employee(agent_id).await;
        Ok(())
    }

    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        let key = agent_key(agent_id);
        let mut conn = self.connection.clone();
        // Both facts in one round trip: whether the key says anything, and
        // whether its writer declared when that answer stops being true.
        let (raw, ttl): (Option<String>, i64) = redis::pipe()
            .get(&key)
            .ttl(&key)
            .query_async(&mut conn)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let Some(s) = raw else {
            self.take_index_entry_if_record_is_gone(agent_id).await;
            return Ok(None);
        };
        if ttl == -2 {
            // Expired between the two commands in the pipeline.
            self.take_index_entry_if_record_is_gone(agent_id).await;
            return Ok(None);
        }
        let reg: AgentRegistration = serde_json::from_str(&s).map_err(SFError::Serialization)?;
        // A key that carries its own expiry is the writer's declaration of how
        // long this record is good for, and Redis enforces it. Judging that
        // declaration a second time against *this reader's* `stale_after_secs`
        // is applying one process's scale to another's fact: a presence record
        // written to live fifteen minutes was being deleted by a governance
        // reader configured for thirty-second agent registrations after ninety
        // seconds, so every face whose owner beats slower than that window
        // flipped to `owner_idle/unknown_unit` between two heartbeats.
        //
        // Records without an expiry are the crash window (`SET` before the
        // `EXPIRE`): they declare nothing, so the last heartbeat is all there
        // is, and an old one is not evidence of life.
        if ttl == -1 && self.is_stale(&reg) {
            self.forget(agent_id).await;
            return Ok(None);
        }
        Ok(Some(reg))
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        let mut conn = self.connection.clone();
        let ids: Vec<String> = conn
            .smembers(REGISTRY_INDEX_KEY)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            // `get` already drops the ones that are not evidence of life, both
            // the expired key and the record whose heartbeat went quiet, and it
            // takes the index entry with them -- so this loop is one question,
            // not two answers that could disagree.
            if let Some(reg) = AgentRegistry::get(self, &id).await? {
                out.push(reg);
            }
        }
        Ok(out)
    }

    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all.into_iter().filter(|r| r.role == role).collect())
    }

    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all
            .into_iter()
            .filter(|r| r.capabilities.contains(&capability.to_string()))
            .collect())
    }

    async fn employees(&self) -> SFResult<Vec<EmployeeRecord>> {
        let mut conn = self.connection.clone();
        let rows: HashMap<String, String> = conn
            .hgetall(EMPLOYEES_KEY)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for (agent_id, text) in rows {
            match serde_json::from_str::<EmployeeRecord>(&text) {
                Ok(row) => out.push(row),
                // 读不出来的行不能悄悄丢掉：名册少一个人与名册上本来没有这个
                // 人是两件事，而丢掉之后它们在读面上长得一模一样。
                Err(e) => tracing::warn!(
                    agent_id = %agent_id,
                    error = %e,
                    "an employee row does not parse; it stays out of the roster until someone looks"
                ),
            }
        }
        out.sort_by_key(|row| std::cmp::Reverse(row.last_seen));
        Ok(out)
    }

    async fn employee(&self, agent_id: &str) -> SFResult<Option<EmployeeRecord>> {
        let mut conn = self.connection.clone();
        let text: Option<String> = conn
            .hget(EMPLOYEES_KEY, agent_id)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(match text {
            None => None,
            Some(text) => match serde_json::from_str(&text) {
                Ok(row) => Some(row),
                Err(e) => {
                    tracing::warn!(
                        agent_id,
                        error = %e,
                        "an employee row does not parse; the caller will read this person as not on the roster"
                    );
                    None
                }
            },
        })
    }
}

// ─── In-memory implementation ───

#[derive(Debug, Default)]
struct MemoryStore {
    agents: HashMap<String, (AgentRegistration, DateTime<Utc>)>, // (reg, expires_at)
    /// 名册行与注册记录分开存，理由与 Redis 那一侧相同：退休不删行。
    employees: HashMap<String, EmployeeRecord>,
    /// 工号计数器，语义与 Redis 里的 [`EMPLOYEE_SEQ_KEY`] 完全一致：只增、
    /// 不回收。两套实现各写一遍时最怕的就是这条语义只落在一边——测试里
    /// 工号能回收、线上不能，等于没测。
    next_number: u32,
}

/// In-memory [`AgentRegistry`] for unit tests.  TTL is honoured logically:
/// `get` / `list` filter out entries whose `expires_at` has passed.
pub struct MemoryAgentRegistry {
    store: RwLock<MemoryStore>,
    ttl_seconds: u64,
}

impl MemoryAgentRegistry {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(MemoryStore::default()),
            ttl_seconds: 30,
        }
    }

    pub fn with_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    fn expires_at(&self) -> DateTime<Utc> {
        Utc::now() + chrono::Duration::seconds(self.ttl_seconds as i64)
    }
}

impl Default for MemoryAgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AgentRegistry for MemoryAgentRegistry {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        store.agents.insert(
            registration.agent_id.clone(),
            (registration.clone(), self.expires_at()),
        );
        let now = Utc::now();
        if let Some(row) = store.employees.get_mut(&registration.agent_id) {
            row.role = registration.role.clone();
            row.last_seen = now;
            // 回来上班的人不再是离职：墓碑擦掉，`first_seen` 与工号都留着
            // ——工号是发出去的编制，换一次上岗不该换号。
            row.retired_at = None;
        } else {
            store.next_number += 1;
            let number = store.next_number;
            store.employees.insert(
                registration.agent_id.clone(),
                EmployeeRecord {
                    agent_id: registration.agent_id.clone(),
                    number: Some(number),
                    role: registration.role.clone(),
                    first_seen: now,
                    last_seen: now,
                    retired_at: None,
                },
            );
        }
        Ok(())
    }

    async fn heartbeat(&self, agent_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        let entry = store.agents.get_mut(agent_id).ok_or_else(|| {
            SFError::Agent(format!(
                "heartbeat: agent {agent_id} not registered (key expired?)"
            ))
        })?;
        if entry.1 < Utc::now() {
            // Already expired. Remove and reject — caller must re-register.
            store.agents.remove(agent_id);
            return Err(SFError::Agent(format!(
                "heartbeat: agent {agent_id} ttl expired"
            )));
        }
        entry.0.last_heartbeat = Utc::now();
        entry.1 = Utc::now() + chrono::Duration::seconds(self.ttl_seconds as i64);
        Ok(())
    }

    async fn deregister(&self, agent_id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        store.agents.remove(agent_id);
        if let Some(row) = store.employees.get_mut(agent_id) {
            let now = Utc::now();
            row.retired_at = Some(now);
            row.last_seen = now;
        }
        Ok(())
    }

    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        match store.agents.get(agent_id) {
            Some((reg, expires)) if *expires >= Utc::now() => Ok(Some(reg.clone())),
            _ => Ok(None),
        }
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        let now = Utc::now();
        Ok(store
            .agents
            .values()
            .filter(|(_, exp)| *exp >= now)
            .map(|(r, _)| r.clone())
            .collect())
    }

    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all.into_iter().filter(|r| r.role == role).collect())
    }

    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>> {
        let all = self.list().await?;
        Ok(all
            .into_iter()
            .filter(|r| r.capabilities.contains(&capability.to_string()))
            .collect())
    }

    async fn employees(&self) -> SFResult<Vec<EmployeeRecord>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        let mut rows: Vec<EmployeeRecord> = store.employees.values().cloned().collect();
        rows.sort_by_key(|row| std::cmp::Reverse(row.last_seen));
        Ok(rows)
    }

    async fn employee(&self, agent_id: &str) -> SFResult<Option<EmployeeRecord>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("memory registry lock poisoned".into()))?;
        Ok(store.employees.get(agent_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// 这个前缀是线上的事实：注册表里已经有按这个拼法存在的 key，而回收脚本
    /// 拿它判断「这个会话还有没有主人」。改它等于让两边同时读一份空注册表；
    /// 这里钉的是拼法本身，不是「函数等于自己」。
    #[test]
    fn the_registry_key_prefix_does_not_drift() {
        assert_eq!(AGENT_KEY_PREFIX, "orchestrator:agents:");
        assert_eq!(agent_key("a-1"), "orchestrator:agents:a-1");
    }

    #[tokio::test]
    async fn memory_registry_register_get_list_heartbeat() {
        let reg: Arc<dyn AgentRegistry> = Arc::new(MemoryAgentRegistry::new());
        let agent_id = crate::generate_agent_id("host-1", "10.0.0.1", "planner", "uuid-1");
        let r = AgentRegistration::new(
            agent_id.clone(),
            "host-1",
            "10.0.0.1",
            "planner",
            "ws-1",
            vec!["code".into()],
            cog_core::ResourceInfo {
                cpu_cores: 4,
                memory_gb: 8,
            },
        );

        reg.register(&r).await.unwrap();
        let got = reg.get(&r.agent_id).await.unwrap().expect("registered");
        assert_eq!(got.agent_id, r.agent_id);
        assert_eq!(got.role, "planner");

        let all = reg.list().await.unwrap();
        assert_eq!(all.len(), 1);

        // heartbeat advances last_heartbeat
        let before = got.last_heartbeat;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        reg.heartbeat(&r.agent_id).await.unwrap();
        let after = reg.get(&r.agent_id).await.unwrap().unwrap();
        assert!(after.last_heartbeat > before);

        // deregister removes
        reg.deregister(&r.agent_id).await.unwrap();
        assert!(reg.get(&r.agent_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_registry_ttl_expiry_filters_stale_entries() {
        let reg: Arc<dyn AgentRegistry> = Arc::new(MemoryAgentRegistry::new().with_ttl_seconds(0));
        let agent_id = crate::generate_agent_id("host-1", "10.0.0.1", "planner", "uuid-1");
        let r = AgentRegistration::new(
            agent_id,
            "host-1",
            "10.0.0.1",
            "planner",
            "ws-1",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        reg.register(&r).await.unwrap();
        // ttl_seconds = 0 means already expired by now.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(reg.get(&r.agent_id).await.unwrap().is_none());
        assert_eq!(reg.list().await.unwrap().len(), 0);
        // heartbeat after expiry must fail
        let err = reg.heartbeat(&r.agent_id).await;
        assert!(err.is_err());
    }

    fn reg_row(agent_id: &str) -> AgentRegistration {
        AgentRegistration::new(
            agent_id,
            "host-1",
            "10.0.0.1",
            "worker",
            "ws-1",
            vec![],
            cog_core::ResourceInfo::default(),
        )
    }

    /// 注册即上名册，并且当场发一个工号。
    ///
    /// 名册行是注册的副作用，不是调用方自愿的另一笔写：哪一类智能体绕过了
    /// 这一步，名册上就少一个人——探针与小队的角色正是这么消失的。
    #[tokio::test]
    async fn registering_puts_a_person_on_the_roster_with_a_number() {
        let reg = MemoryAgentRegistry::new();
        reg.register(&reg_row("probe-1")).await.unwrap();
        reg.register(&reg_row("probe-2")).await.unwrap();
        assert_eq!(reg.employees().await.unwrap().len(), 2);
        let a = reg.employee("probe-1").await.unwrap().expect("名册行");
        let b = reg.employee("probe-2").await.unwrap().expect("名册行");
        assert!(a.number.is_some() && b.number.is_some(), "名册行没有工号");
        assert_ne!(a.number, b.number, "两个员工共用一个工号");
    }

    /// 离职留行：注销之后这个人还在名册上，工号与入职时间都不变，只多一块
    /// 墓碑。删行等于把「离职」与「从来没有过这个人」读成同一件事。
    #[tokio::test]
    async fn retiring_keeps_the_row_and_the_number() {
        let reg = MemoryAgentRegistry::new();
        reg.register(&reg_row("employee:1")).await.unwrap();
        let before = reg.employee("employee:1").await.unwrap().expect("名册行");
        reg.deregister("employee:1").await.unwrap();
        let after = reg.employee("employee:1").await.unwrap().expect("名册行");
        assert!(after.retired_at.is_some(), "注销之后名册行没有墓碑");
        assert_eq!(after.number, before.number, "退役换了工号");
        assert_eq!(after.first_seen, before.first_seen, "退役重置了入职时间");
        // 回来上班：墓碑擦掉，号还是那个号。
        reg.register(&reg_row("employee:1")).await.unwrap();
        let again = reg.employee("employee:1").await.unwrap().expect("名册行");
        assert!(again.retired_at.is_none(), "回来上班的人还挂着离职");
        assert_eq!(again.number, before.number, "回来上班换了工号");
    }

    /// 同一个 id 反复注册不吃新号：工号是发出去的编制，不是每次注册的计数。
    #[tokio::test]
    async fn a_second_registration_does_not_consume_a_new_number() {
        let reg = MemoryAgentRegistry::new();
        reg.register(&reg_row("employee:1")).await.unwrap();
        let first = reg.employee("employee:1").await.unwrap().unwrap().number;
        reg.register(&reg_row("employee:1")).await.unwrap();
        reg.register(&reg_row("employee:1")).await.unwrap();
        reg.register(&reg_row("employee:2")).await.unwrap();
        let second = reg.employee("employee:2").await.unwrap().unwrap().number;
        assert_eq!(
            reg.employee("employee:1").await.unwrap().unwrap().number,
            first
        );
        assert_eq!(
            first.unwrap() + 1,
            second.unwrap(),
            "重复注册把后面的号吃掉了"
        );
    }
}
