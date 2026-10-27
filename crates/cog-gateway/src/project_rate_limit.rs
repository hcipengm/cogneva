//! 项目面读端点的限速。
//!
//! 为什么会需要这一层：`/api/v1/projects` 与它下面那四个视图每次都要把整个任务
//! 图读进内存再折一遍，成本随任务数线性上涨，而调用方没有天然的节制者——一个
//! 卡在刷新循环里的看板页面每分钟能打出成百上千次请求。没有限速时，最先撑不住
//! 的不是那个页面，是共享同一个进程的其他所有面。
//!
//! 计数放在进程里而不是共享 KV：这一层的语义是「这个部署的一份副本每秒愿意为
//! 一个调用方折几次图」，它约束的是本进程的 CPU，而不是全平台的配额。跨副本
//! 的公平分摊是入口网关的事，这里替它做不了，也不该假装做了——写成共享 KV 会
//! 让一次请求多出一跳网络，而它防的恰恰是请求太多。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use axum::extract::Request;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// 每个调用方每分钟允许的读次数。默认值与集群里其他读面的量级对齐：一个正常
/// 刷新的看板每分钟十几次，这个数是它的几十倍，够任何真人用，也够拦住环。
pub const RATE_ENV: &str = "COGNEVA_PROJECT_READ_PER_MIN";
pub const DEFAULT_PER_MIN: u32 = 600;

/// 一条调用方的桶。
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// 每调用方一个令牌桶。
///
/// 用桶而不是固定窗口：固定窗口在一分钟的交界处允许两倍的突发，而这一层要挡
/// 的正是突发。桶的容量取一分钟的量，所以「一直按上限读」与「安静一分钟之后
/// 攒一波」拿到的是同一份总预算。
#[derive(Debug)]
pub struct ProjectReadLimiter {
    per_min: u32,
    buckets: Mutex<HashMap<String, Bucket>>,
}

/// 桶超过这个数就先清一遍过期的：调用方 id 的数量不该成为一处无界增长的内存。
const BUCKET_SWEEP_AT: usize = 4096;

impl ProjectReadLimiter {
    pub fn new(per_min: u32) -> Self {
        Self {
            per_min: per_min.max(1),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// 从环境读速率。读坏了按默认值走，并把坏值记进日志——一个拼错的变量
    /// 不该让这一层静默消失，也不该让所有读请求一律被拒。
    pub fn from_env() -> Self {
        match std::env::var(RATE_ENV) {
            Ok(raw) => match raw.trim().parse::<u32>() {
                Ok(n) if n > 0 => Self::new(n),
                _ => {
                    tracing::warn!(
                        variable = RATE_ENV,
                        value = %raw,
                        "project read rate is not a positive integer; using the default"
                    );
                    Self::new(DEFAULT_PER_MIN)
                }
            },
            Err(_) => Self::new(DEFAULT_PER_MIN),
        }
    }

    /// 取一个令牌。满了回 `Err(还要等几秒)`。
    pub fn take(&self, caller: &str) -> Result<(), u64> {
        let capacity = self.per_min as f64;
        let per_sec = self.per_min as f64 / 60.0;
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if buckets.len() > BUCKET_SWEEP_AT {
            // 满桶的桶等于没被用过，扔了不丢信息。
            buckets.retain(|_, b| b.tokens < capacity);
        }
        let bucket = buckets.entry(caller.to_string()).or_insert(Bucket {
            tokens: capacity,
            last: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * per_sec).min(capacity);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let wait = ((1.0 - bucket.tokens) / per_sec).ceil().max(1.0);
            Err(wait as u64)
        }
    }
}

/// 进程级的那一个限速器。
///
/// 与项目面的读数（`project_control::readings`）同一个形状：进程级单例，接线是
/// 隐式的。放进 `GatewayState` 的话，每一个构造点都得记得带上它，而漏掉的那一
/// 处不会报错——它会安静地不限速。
pub fn limiter() -> &'static Arc<ProjectReadLimiter> {
    static LIMITER: OnceLock<Arc<ProjectReadLimiter>> = OnceLock::new();
    LIMITER.get_or_init(|| Arc::new(ProjectReadLimiter::from_env()))
}

/// 限速中间件。挂在已完成认证的那一层里面，才能按调用方身份计数。
pub async fn guard(req: Request, next: Next) -> Response {
    let caller = req
        .extensions()
        .get::<cog_core::Claims>()
        .map(|c| c.sub.clone())
        .unwrap_or_else(|| "anonymous".to_string());
    match limiter().take(&caller) {
        Ok(()) => next.run(req).await,
        Err(retry_after) => {
            let body = serde_json::json!({
                "error": "TooManyRequests",
                "code": "rate_limited",
                "message": format!(
                    "too many project reads from this caller: at most {} per minute",
                    limiter().per_min
                ),
            });
            let mut response = (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
            if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一分钟的预算用完就拒，并且说出还要等多久。
    #[test]
    fn a_caller_gets_one_minutes_budget_and_then_waits() {
        let limiter = ProjectReadLimiter::new(3);
        for _ in 0..3 {
            assert!(limiter.take("u1").is_ok());
        }
        let wait = limiter.take("u1").expect_err("第四次必须被拒");
        assert!(wait >= 1, "回执要说还得等几秒，实际 {wait}");
    }

    /// 两个调用方的预算互不相干：一个人刷爆自己的，不该把同事一起挡在外面。
    #[test]
    fn one_callers_refusal_does_not_spend_anothers_budget() {
        let limiter = ProjectReadLimiter::new(1);
        assert!(limiter.take("u1").is_ok());
        assert!(limiter.take("u1").is_err());
        assert!(limiter.take("u2").is_ok(), "别人的桶是满的");
    }

    /// 攒下来的时间是能换回令牌的——否则安静一小时之后也还是被拒。
    #[test]
    fn a_quiet_caller_refills() {
        let limiter = ProjectReadLimiter::new(60);
        assert!(limiter.take("u1").is_ok());
        assert!(limiter.take("u1").is_ok());
        {
            let mut buckets = limiter.buckets.lock().unwrap();
            let b = buckets.get_mut("u1").unwrap();
            b.tokens = 0.0;
            b.last = Instant::now() - std::time::Duration::from_secs(2);
        }
        assert!(
            limiter.take("u1").is_ok(),
            "两秒换回两个令牌（每分钟 60 个）"
        );
    }
}
