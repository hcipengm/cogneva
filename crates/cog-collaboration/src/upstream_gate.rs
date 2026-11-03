//! 调用侧那一半的池级熔断：池子说这一面没有成员能服务时，不再起新的执行。
//!
//! 网关于是已经有自己那一半了——一面没有可用成员，它当场回 503，一个上游都不试。
//! 那次拒绝本身很便宜，不花一个 token。贵的在调用侧：一次执行从起对象、装 ralph
//! 环、建 agent，到第一次 planner 调用被拒，整条链已经跑过了，然后把这次终止记进
//! `ralph_terminations_total`。也就是说，池子的熔断省下的是上游的钱，没省下本机的
//! 时间，也没省下这块面自己的读数。
//!
//! 2026-11-03 的现场：池子被拒了几天，队列里压着 3581 件任务，而每一次拒绝都被
//! 一字不差地重放——每分钟约 12 次起一个环、死一个环。于是
//! `ralph_terminations_total{class="paced"}` 在额度为零的时候一路涨，而
//! [`crate::squad::classify`] 里写着这一面本该给出的判据恰恰相反：额度为零时终止
//! 数应当趋近于零。那条判据读不出来，不是因为它写错了，是因为根本没有一段代码在
//! 「池子说没人能服务」之后停下过。
//!
//! 判据只有一条，而且不许发明数：**拒绝自己说了还有多久值得再问**。池级拒绝把
//! 那个时长写在 `[retry_after_secs=N]` 里，闸门就关这么久；到点放一次探查，被服务
//! 成功就收回。没说时长的拒绝不开闸——没有数可等，编一个出来就违背了「界来自上游
//! 的一次测量」。
//!
//! 闸门只认池级的话，不认单个成员的话。一个成员的额度用尽（上游自己回的 429）
//! 不是「这一面没人能服务」的证据：池里可能还有另一个成员，换过去就能跑。池级
//! 的那句话有机器可读的一格——`face_has_no_serving_member`，网关写、这里读，两边
//! 认同一个常量。
//!
//! 按面分闸还是按池分闸：网关自己挑面，调用侧在派发时并不知道这一次会落到哪一面，
//! 所以闸门是进程级的一只。代价是「一面被拒、另一面还活着」时会把另一面也停一拍；
//! 那一拍到期后的第一次探查（一次真正的执行）成功就会把闸门收回，错得可恢复。

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use cog_core::contract::llm::{render_retry_after, retry_after_hint_in, FACE_WITHOUT_A_MEMBER};
use cog_core::contract::outcome::UPSTREAM_UNAVAILABLE_PREFIX;
use cog_core::SFError;

/// 一次拒绝该不该把闸门关上、关上多久。
///
/// 纯函数：拒绝文本进，等待时长出。判据只有两句——它得是池级的那句话，而且它得
/// 自己说了下次值得再问的时刻。分开成函数是为了让这两句被单独钉住：一句被放宽，
/// 单成员的 429 就会把整个进程停住；另一句被放宽，闸门就会用一个编出来的数关着。
pub fn trip_from(refusal: &str) -> Option<Duration> {
    if !refusal.contains(FACE_WITHOUT_A_MEMBER) {
        return None;
    }
    retry_after_hint_in(refusal).map(Duration::from_secs)
}

/// 闸门关着时交回去的那一笔失败。
///
/// 它必须让调度侧把它读成「环境付账」而不是“这一件活判死了”，否则闸门一关就把
/// 队列里的活一批判死。调度侧的两条判据是“失败可重试”与“拒绝自报了等待”，所以
/// 这段文本两样都得带上：前缀声明它属于哪一类，尾部渲染出等待。三样拼在一处而
/// 不各写各的，是因为读它的那一侧按同样的三个字面量认人。
pub fn refusal(wait: Duration) -> SFError {
    // 向上取整到秒并至少留一秒：闸门关着的剩余时间可能不到一秒，渲染成 0 会被
    // 读成“没有等待”，调度侧于是按自己的策略退避，和这里关闸的意图分成两条路。
    let secs = (wait.as_secs_f64().ceil() as u64).max(1);
    SFError::Agent(format!(
        "{UPSTREAM_UNAVAILABLE_PREFIX}: environment_error: the pool reports no member serving \
         the requested face, so no execution was started; the same pool is not asked again \
         before {secs}s {}",
        render_retry_after(secs)
    ))
}

/// 关着的这一段时间，以及关闸时上游自报的那个等待。
#[derive(Default)]
struct Shut {
    until: Option<Instant>,
    /// 关闸时的那一个数。只为一件事：读日志的人知道这一刻是被谁的多长的等待挡住的。
    named: Option<Duration>,
    /// 关闸期间被挡回去的执行数。不是指标，是日志与测试要看的那一格。
    turned_away: u64,
}

/// 进程级的一只闸门。
///
/// 时间从外面进（`now`），不在这里取 `Instant::now()`：判据要能在测试里把时间推过去，
/// 而不是靠睡够 60 秒。
#[derive(Default)]
pub struct UpstreamGate {
    shut: Mutex<Shut>,
}

impl UpstreamGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// 池子拒绝了。关上 `wait`，并且只延长不缩短：关闸期间又来一次拒绝，说明池子
    /// 还没回来，按新的时刻算。
    pub fn refused(&self, now: Instant, wait: Duration) {
        let mut shut = self.shut.lock().unwrap_or_else(|e| e.into_inner());
        let until = now + wait;
        if shut.until.map(|u| until > u).unwrap_or(true) {
            shut.until = Some(until);
            shut.named = Some(wait);
        }
    }

    /// 一次执行被服务成功了：池子这一面有成员能干活，闸门收回。
    pub fn served(&self) {
        let mut shut = self.shut.lock().unwrap_or_else(|e| e.into_inner());
        shut.until = None;
        shut.named = None;
    }

    /// 现在还能不能起；不能的话还要等多久。
    ///
    /// 到期就不算关着——到期的那一刻放过去的那一次就是这一次探查。它成功会调用
    /// [`Self::served`]，失败会调用 [`Self::refused`] 重新关上。
    pub fn wait_left(&self, now: Instant) -> Option<Duration> {
        let mut shut = self.shut.lock().unwrap_or_else(|e| e.into_inner());
        match shut.until {
            Some(until) if now < until => {
                shut.turned_away += 1;
                Some(until - now)
            }
            Some(_) => {
                // 到点即开封：关着的那一段到此为止，`named` 也一起放掉。留着它会让
                // 「还关着吗」与「关过吗」在后来的读者眼里重新变成两个问题。
                shut.until = None;
                shut.named = None;
                None
            }
            None => None,
        }
    }

    /// 关闸以来挡回去的次数。给日志用，也让“停下来了”这件事在测试里读得出来。
    pub fn turned_away(&self) -> u64 {
        self.shut
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .turned_away
    }

    /// 关闸时上游自报的等待。
    pub fn named_wait(&self) -> Option<Duration> {
        self.shut.lock().unwrap_or_else(|e| e.into_inner()).named
    }
}

fn global() -> &'static UpstreamGate {
    static GATE: OnceLock<UpstreamGate> = OnceLock::new();
    GATE.get_or_init(UpstreamGate::new)
}

/// 现在还能不能起新的执行；不能的话还要等多久。
pub fn blocked_now() -> Option<Duration> {
    global().wait_left(Instant::now())
}

/// 池子刚拒绝了这一次执行。
pub fn refused(wait: Duration) {
    let gate = global();
    gate.refused(Instant::now(), wait);
    tracing::warn!(
        wait_secs = wait.as_secs(),
        "池级闸门关闭：这一面没有成员能服务，暂停起新的执行到上游自报的时刻"
    );
}

/// 一次执行被服务成功了：收回闸门。
pub fn served() {
    let gate = global();
    // 只有关过才值得留一行：没关过的时候每一次成功都写一行会把日志淹掉。这一读
    // 不能走 `wait_left`——那个函数会记一次“被挡回去”，而这一次并没有被挡。
    if gate.named_wait().is_some() {
        tracing::info!("池级闸门收回：这一面又有成员能服务了");
    }
    gate.served();
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::contract::outcome::is_deterministic_failure;

    /// 池级拒绝的原样文本（2026-11-03 从进化进程的日志里取的）。
    fn pool_refusal(retry_after: u64) -> String {
        format!(
            "Agent execution error: {UPSTREAM_UNAVAILABLE_PREFIX}: environment_error: LLM \
             upstream refused (server_error): LLM stream error: API error (HTTP 503): \
             {{\"error\":\"所有 LLM 上游当前不可用\",\"face\":\"anthropic\",\
             \"quota_window_secs\":604800,\"reason\":\"{FACE_WITHOUT_A_MEMBER}\",\
             \"retry_after_seconds\":{retry_after}}} [retry_after_secs={retry_after}]"
        )
    }

    /// 池级拒绝把闸门关上，关的就是它自己说的那个时长。
    #[test]
    fn a_pool_refusal_closes_the_gate_for_the_wait_it_named() {
        assert_eq!(
            trip_from(&pool_refusal(274)),
            Some(Duration::from_secs(274))
        );
    }

    /// 一个成员自己的 429 不是「这一面没人能服务」：池里可能还有另一个成员，
    /// 换过去就能跑。拿它关闸会把整个进程停在一个只坏了一家的池子上。
    #[test]
    fn one_members_rate_limit_does_not_close_the_gate() {
        let member_level = format!(
            "Agent execution error: {UPSTREAM_UNAVAILABLE_PREFIX}: environment_error: LLM \
             upstream refused (rate_limited): LLM stream error: API error (HTTP 429): \
             {{\"error\":{{\"type\":\"rate_limit_error\",\"message\":\"You've reached your \
             weekly usage limit\"}}}}"
        );
        assert_eq!(trip_from(&member_level), None);
    }

    /// 池级拒绝但没说下次值得再问的时刻：没有数可等，编一个就违背了“界来自上游
    /// 的一次测量”。这一笔交给调度侧按它自己的策略处理，闸门不动。
    #[test]
    fn a_pool_refusal_that_names_no_wait_does_not_invent_one() {
        let no_wait = format!(
            "Agent execution error: {UPSTREAM_UNAVAILABLE_PREFIX}: environment_error: \
             LLM upstream refused (server_error): {{\"reason\":\"{FACE_WITHOUT_A_MEMBER}\"}}"
        );
        assert_eq!(trip_from(&no_wait), None);
    }

    /// 关着的这一段里不起新的执行；到点放行一次，也就是那一次探查。
    #[test]
    fn nothing_starts_while_shut_and_exactly_one_probe_goes_out_when_it_opens() {
        let gate = UpstreamGate::new();
        let t0 = Instant::now();
        gate.refused(t0, Duration::from_secs(60));

        assert_eq!(
            gate.wait_left(t0 + Duration::from_secs(1)),
            Some(Duration::from_secs(59))
        );
        assert_eq!(
            gate.wait_left(t0 + Duration::from_secs(59)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(gate.turned_away(), 2);
        assert_eq!(gate.named_wait(), Some(Duration::from_secs(60)));

        // 到点：这一拍放过去的就是探查。
        assert_eq!(gate.wait_left(t0 + Duration::from_secs(60)), None);
        // 没有人再来关它之前，后面每一条都能起——闸门不是“到点后又关上一次”。
        assert_eq!(gate.wait_left(t0 + Duration::from_secs(61)), None);
    }

    /// 探查失败：闸门重新关上，且关闸期间的第二次拒绝只延长不缩短。
    #[test]
    fn a_failed_probe_shuts_the_gate_again_and_a_second_refusal_only_extends() {
        let gate = UpstreamGate::new();
        let t0 = Instant::now();
        gate.refused(t0, Duration::from_secs(60));
        assert_eq!(gate.wait_left(t0 + Duration::from_secs(60)), None);

        gate.refused(t0 + Duration::from_secs(60), Duration::from_secs(60));
        assert_eq!(
            gate.wait_left(t0 + Duration::from_secs(61)),
            Some(Duration::from_secs(59))
        );

        // 关闸期间又收到一次只有 5 秒的拒绝：它不能把已经关到的 119 秒缩短。
        gate.refused(t0 + Duration::from_secs(61), Duration::from_secs(5));
        assert_eq!(
            gate.wait_left(t0 + Duration::from_secs(62)),
            Some(Duration::from_secs(58))
        );
    }

    /// 一次成功的执行收回闸门：池子这一面又有成员能服务了。
    #[test]
    fn a_served_call_opens_the_gate() {
        let gate = UpstreamGate::new();
        let t0 = Instant::now();
        gate.refused(t0, Duration::from_secs(3600));
        assert!(gate.wait_left(t0 + Duration::from_secs(1)).is_some());

        gate.served();
        assert_eq!(gate.wait_left(t0 + Duration::from_secs(1)), None);
        assert_eq!(gate.named_wait(), None);
    }

    /// 闸门交回去的那一笔，调度侧必须读成「环境付账」而不是“这件活判死了”。
    ///
    /// 这是本模块唯一的跨层契约，也是它能工作的全部理由：调度侧按两个独立判据决定
    /// 回投还是判死——失败可重试、以及拒绝自报了等待。少任何一个，闸门一关就会把
    /// 队列里的活一批判死，比它要修的“空转”更糟。
    #[test]
    fn the_refusal_it_hands_back_is_one_the_scheduler_requeues() {
        let text = refusal(Duration::from_millis(1500)).to_string();

        assert_eq!(
            retry_after_hint_in(&text),
            Some(2),
            "不足一秒的等待要向上取整：渲染成 0 会被读成「没有等待」"
        );
        assert!(
            !is_deterministic_failure(&text),
            "这一笔必须可重试，否则判死：{text}"
        );
        assert_eq!(
            crate::squad::classify::classify(&text),
            crate::squad::classify::UPSTREAM_UNAVAILABLE_CLASS,
            "它属于上游不可用那一类，不是别的：{text}"
        );
    }
}
