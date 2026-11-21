//! 没记上的失败登录要说出声。
//!
//! 登录限流是一对写和读：失败时把计数加一（`incr_ex`），检查时读回来比上限。
//! 读侧是明确设计的「读不到就当 0」——存储读失败不能变成谁也登不进来——可那
//! 意味着写入失败不只是少了一格数：那个身份在存储拒写期间**不受上限约束**。
//! 写侧的返回值原本被丢掉，于是攻击者撞库最凶的那一拍，日志里一个字都没有。
//!
//! 成功的登录要清计数；删除被拒时计数还在，这个身份会带着旧账在窗口到期前被
//! 自己的历史挡在门外——同样一句话也不说。
//!
//! 这里给限流器一个拒写 `incr_ex` / `del` 的存储，读日志：两次都必须点得出是
//! 哪个身份、存储回了什么；反面对照里存储正常应答时，不得凭空报这两句话。

use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

use cog_core::{KeyValueStore, SFError, SFResult};
use cog_gateway::auth::{LoginRateLimiter, LOGIN_MAX_ATTEMPTS, LOGIN_WINDOW_SECONDS};

use std::collections::HashMap;

/// 限流器会碰到的三个方法：读计数、自增并带过期、删计数。正常时按内存实现
/// 应答；`refuse_writes` 打开时两个写方法回错。
///
/// 没列出来的方法用 `unreachable!()` 而不是 `Ok(())`：限流器日后若开始用集合
/// 或哈希，这里当场炸出是哪个方法，而不是让一套静默的空实现替它回答。
struct Counting {
    rows: Mutex<HashMap<String, String>>,
    refuse_writes: bool,
}

impl Counting {
    fn answering() -> Self {
        Self {
            rows: Mutex::new(HashMap::new()),
            refuse_writes: false,
        }
    }

    fn refusing() -> Self {
        Self {
            rows: Mutex::new(HashMap::new()),
            refuse_writes: true,
        }
    }
}

fn refused(what: &str) -> SFError {
    SFError::Database(format!("the store refused the {what}"))
}

#[async_trait::async_trait]
impl KeyValueStore for Counting {
    async fn get(&self, key: &str) -> SFResult<Option<String>> {
        Ok(self.rows.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, _key: &str, _value: &str) -> SFResult<()> {
        unreachable!("the limiter does not overwrite a counter")
    }

    async fn set_ex(&self, _key: &str, _value: &str, _ttl_secs: u64) -> SFResult<()> {
        unreachable!("the limiter does not set a counter with a TTL")
    }

    async fn del(&self, key: &str) -> SFResult<()> {
        if self.refuse_writes {
            return Err(refused("delete"));
        }
        self.rows.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> SFResult<()> {
        unreachable!("the counter carries its own expiry")
    }

    async fn ttl(&self, _key: &str) -> SFResult<i64> {
        unreachable!("the limiter does not read a TTL")
    }

    async fn incr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unreachable!("the limiter counts with incr_ex, so the key always has an expiry")
    }

    async fn decr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unreachable!("nothing decrements a login counter")
    }

    async fn incr_ex(&self, key: &str, by: i64, _ttl_secs: u64) -> SFResult<i64> {
        if self.refuse_writes {
            return Err(refused("increment"));
        }
        let mut rows = self.rows.lock().unwrap();
        let next = rows
            .get(key)
            .and_then(|raw| raw.parse::<i64>().ok())
            .unwrap_or(0)
            + by;
        rows.insert(key.to_string(), next.to_string());
        Ok(next)
    }

    async fn sadd(&self, _key: &str, _member: &str) -> SFResult<()> {
        unreachable!("the limiter keeps counters, not sets")
    }

    async fn srem(&self, _key: &str, _member: &str) -> SFResult<()> {
        unreachable!("the limiter keeps counters, not sets")
    }

    async fn smembers(&self, _key: &str) -> SFResult<Vec<String>> {
        unreachable!("the limiter keeps counters, not sets")
    }

    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> SFResult<()> {
        unreachable!("the limiter keeps counters, not hashes")
    }

    async fn hget(&self, _key: &str, _field: &str) -> SFResult<Option<String>> {
        unreachable!("the limiter keeps counters, not hashes")
    }

    async fn hdel(&self, _key: &str, _field: &str) -> SFResult<()> {
        unreachable!("the limiter keeps counters, not hashes")
    }

    async fn hvals(&self, _key: &str) -> SFResult<Vec<String>> {
        unreachable!("the limiter keeps counters, not hashes")
    }

    async fn hgetall(&self, _key: &str) -> SFResult<Vec<(String, String)>> {
        unreachable!("the limiter keeps counters, not hashes")
    }

    async fn hlen(&self, _key: &str) -> SFResult<u64> {
        unreachable!("the limiter keeps counters, not hashes")
    }
}

/// 一条日志事件里这场判决关心的那几个字段。
#[derive(Clone, Debug)]
struct Recorded {
    message: String,
    identifier: Option<String>,
    key: Option<String>,
    error: Option<String>,
}

#[derive(Default)]
struct PickFields {
    message: Option<String>,
    identifier: Option<String>,
    key: Option<String>,
    error: Option<String>,
}

impl PickFields {
    fn set(&mut self, name: &str, plain: String) {
        let slot = match name {
            "message" => &mut self.message,
            "identifier" => &mut self.identifier,
            "key" => &mut self.key,
            "error" => &mut self.error,
            _ => return,
        };
        *slot = Some(plain);
    }
}

impl Visit for PickFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%x` 到访客手里时还带着 debug 的引号，在这里摘掉。
        let text = format!("{value:?}");
        let plain = text
            .strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .map(str::to_string)
            .unwrap_or(text);
        self.set(field.name(), plain);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field.name(), value.to_string());
    }
}

struct Capture {
    events: Arc<Mutex<Vec<Recorded>>>,
}

impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut pick = PickFields::default();
        event.record(&mut pick);
        self.events.lock().unwrap().push(Recorded {
            message: pick.message.unwrap_or_default(),
            identifier: pick.identifier,
            key: pick.key,
            error: pick.error,
        });
    }
}

/// 在一段 future 的整个生命周期里装上截获订阅器，拿回它印出的日志。
///
/// 订阅器是线程局部的，所以这段 future 跑在同一线程的运行时上：这就是限流器
/// 自己那句 `warn!` 落进捕获里的原因。
fn with_capture<F, T>(future: F) -> (T, Vec<Recorded>)
where
    F: std::future::Future<Output = T>,
{
    let events: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture {
        events: events.clone(),
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the current-thread runtime builds");
    let output = tracing::subscriber::with_default(subscriber, move || runtime.block_on(future));
    let events = events.lock().unwrap().clone();
    (output, events)
}

fn limiter(store: Arc<dyn KeyValueStore>) -> LoginRateLimiter {
    LoginRateLimiter::new(store, LOGIN_MAX_ATTEMPTS, LOGIN_WINDOW_SECONDS)
}

fn refusing_limiter() -> LoginRateLimiter {
    limiter(Arc::new(Counting::refusing()))
}

/// 计数写不进去：日志必须点出是哪个身份、存储回了什么。
#[test]
fn a_failure_the_store_refused_to_count_is_named_in_the_log() {
    let limiter = refusing_limiter();
    let (_, events) = with_capture(async move { limiter.record_failure("alice").await });

    let named = events
        .iter()
        .find(|e| e.key.as_deref() == Some("login_attempts:alice"))
        .unwrap_or_else(|| panic!("这一拍限流是关着的，日志里却没有任何一条点名：{events:#?}"));
    assert!(
        named
            .error
            .as_deref()
            .is_some_and(|e| e.contains("increment")),
        "只说「没记上」说不出存储回了什么，排查的人还得再猜一遍：{named:#?}"
    );
    assert!(
        !named.message.is_empty(),
        "点得出名字却说不出后果——读者不知道这一拍限流是开着的：{named:#?}"
    );
}

/// 清计数被拒：登录成了，旧账还在，日志必须说。
#[test]
fn a_successful_login_whose_cleanup_the_store_refused_is_named_in_the_log() {
    let limiter = refusing_limiter();
    let (_, events) = with_capture(async move { limiter.clear("alice").await });

    let named = events
        .iter()
        .find(|e| e.identifier.as_deref() == Some("alice"))
        .unwrap_or_else(|| panic!("登录成功了但旧计数没清掉，日志里没有这一笔：{events:#?}"));
    assert!(
        named.error.as_deref().is_some_and(|e| e.contains("delete")),
        "说不出删除被拒的原因：{named:#?}"
    );
}

/// 反面对照：存储正常应答时不得凭空报这两句话——否则上面两条测的只是日志
/// 通道通不通，不是「拒写被说出来」。
#[test]
fn a_counted_failure_is_not_reported_as_a_problem() {
    let store: Arc<dyn KeyValueStore> = Arc::new(Counting::answering());
    let limiter = limiter(store.clone());
    let probe = store.clone();
    let (limited, events) = with_capture(async move {
        for _ in 0..LOGIN_MAX_ATTEMPTS {
            limiter.record_failure("alice").await;
        }
        let limited = limiter.is_limited("alice").await;
        limiter.clear("alice").await;
        limited
    });

    assert!(
        limited,
        "计数写进去了却读不到上限，说明这条对照根本没走到真的写路径"
    );
    let reported: Vec<_> = events
        .iter()
        .filter(|e| {
            e.key.as_deref() == Some("login_attempts:alice")
                || e.identifier.as_deref() == Some("alice")
        })
        .collect();
    assert!(
        reported.is_empty(),
        "存储一切正常，日志却说没记上/没清掉：{reported:#?}"
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the runtime builds");
    assert_eq!(
        runtime
            .block_on(probe.get("login_attempts:alice"))
            .expect("the in-memory store answers"),
        None,
        "清计数是成功的，键却还在"
    );
}
