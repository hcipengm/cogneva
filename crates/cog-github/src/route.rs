//! 远程走哪条路，由这条路上一次实测回答，不由「哪个环境变量设了」回答。
//!
//! 一条路由三个东西拼出来：网关透传、SSH、带 token 的 HTTPS、匿名 HTTPS。哪些路
//! 此刻**存在**是配置说的（有没有配网关基址、有没有 token），但哪条路**先走**不是
//! 配置说的——那是实测说的。每条路自己带着上一次的结果与耗时，选路时按它排：
//!
//! 1. 上次成功过的，按上一次的耗时从短到长；
//! 2. 从没试过的，按候选的声明顺序（每一条都要被试到一次，否则它永远是「没试过」）；
//! 3. 上次失败过的，排在最后。
//!
//! 这个顺序每用一次就重排一次：一次失败立刻把那条路压到后面，一次成功立刻把它按
//! 耗时插进去。所以「设了网关基址」最多让网关那条路**参与**，不让它永远排第一；反
//! 过来，一条路也不会因为没有谁记得它就永远不被试——没试过的排在失败过的之前。
//!
//! 失败的判据只认「这条路没走通」：push 输给了别人（non-fast-forward）不算路不通，
//! 那是这条路走通了而目标分支动了，记进这里会让一条好路被一次争抢压下去。

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

/// 网关透传那条路的名字。
pub const ROUTE_PROXY: &str = "proxy";
/// SSH 那条路的名字。
pub const ROUTE_SSH: &str = "ssh";
/// 带 token 的 HTTPS 那条路的名字。
pub const ROUTE_TOKEN: &str = "token";
/// 匿名 HTTPS 那条路的名字。
pub const ROUTE_ANONYMOUS: &str = "anonymous";

/// 一条此刻存在的路：它的读数键（`<host>:<route>`）与它拼出来的 URL。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// 读数键。路由名只在 `<host>:` 之后，所以同一族路由在两个主机上的读数
    /// 不会互相顶掉——gitee 的网关通、github 的网关不通是两件事。
    pub key: String,
    /// 这条路拼出来的 git remote URL。
    pub url: String,
}

impl Candidate {
    /// URL 里那条路的名字（`proxy`／`ssh`／`token`／`anonymous`）。
    pub fn route(&self) -> &str {
        self.key
            .rsplit_once(':')
            .map(|(_, route)| route)
            .unwrap_or(&self.key)
    }
}

/// 一条路的实测：试过几次、成功几次、上一次成没成、上一次多久、上一次为什么没成。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reading {
    /// 这条路上发生过的尝试次数（含失败）。
    pub attempts: u64,
    /// 其中走通的次数。
    pub successes: u64,
    /// 上一次走通了没有。没试过时为 `false`，用 `attempts == 0` 区分。
    pub last_ok: bool,
    /// 上一次从开始到有结果用了多少毫秒。
    pub last_ms: u128,
    /// 上一次没走通的原因；走通时清空。
    pub last_error: Option<String>,
}

/// 每一条路自己的读数。进程内、按读数键排，读的时候整份拿走。
#[derive(Debug, Clone, Default)]
pub struct RouteBook {
    readings: BTreeMap<String, Reading>,
}

impl RouteBook {
    /// 记一次走通。
    pub fn record_ok(&mut self, key: &str, ms: u128) {
        let entry = self.readings.entry(key.to_string()).or_default();
        entry.attempts += 1;
        entry.successes += 1;
        entry.last_ok = true;
        entry.last_ms = ms;
        entry.last_error = None;
    }

    /// 记一次没走通。原因留在这一行里：一条路为什么被压到后面，读的时候要答得出来。
    pub fn record_fail(&mut self, key: &str, ms: u128, error: impl Into<String>) {
        let entry = self.readings.entry(key.to_string()).or_default();
        entry.attempts += 1;
        entry.last_ok = false;
        entry.last_ms = ms;
        entry.last_error = Some(error.into());
    }

    /// 这条路至今的读数；没试过时为 `None`。
    pub fn reading(&self, key: &str) -> Option<&Reading> {
        self.readings.get(key)
    }

    /// 读数的文本行，一行一条路。**没试过**的路不在这份输出里——它不是
    /// 「成功 0 次」，它是「还没有读数」，这两件事在选中它的时候判得不一样。
    pub fn lines(&self) -> Vec<String> {
        self.readings
            .iter()
            .map(|(key, reading)| {
                format!(
                    "route\t{key}\t{}\t{}\t{}\t{}",
                    reading.attempts, reading.successes, reading.last_ok, reading.last_ms
                )
            })
            .collect()
    }
}

/// 选路：手上的候选按上面那三条排开。
///
/// 排序是稳定的：同一档里保持候选自己的声明顺序，所以「从没试过」的那几条每次
/// 都按配置写的顺序被试，读的人不会看见两条路来回换位。
pub fn ordered<'a>(candidates: &'a [Candidate], book: &RouteBook) -> Vec<&'a Candidate> {
    let mut ranked: Vec<(u8, u128, &'a Candidate)> = candidates
        .iter()
        .map(|candidate| {
            let (tier, ms) = match book.reading(&candidate.key) {
                None => (1u8, 0u128),
                Some(reading) if reading.last_ok => (0, reading.last_ms),
                Some(reading) => (2, reading.last_ms),
            };
            (tier, ms, candidate)
        })
        .collect();
    ranked.sort_by_key(|(tier, ms, _)| (*tier, *ms));
    ranked
        .into_iter()
        .map(|(_, _, candidate)| candidate)
        .collect()
}

/// 此刻存在的路，按声明顺序。
///
/// 这里只回答「有哪些路可以走」，不回答「先走哪条」——后者由 [`ordered`] 拿读数
/// 排。所以「网关基址配了」得到的是一条候选，不是一条优先级。
pub fn candidates(
    slug: &str,
    repo: &str,
    proxy_base: Option<&str>,
    use_ssh: bool,
    token: Option<&str>,
    anonymous_fallback: bool,
) -> Vec<Candidate> {
    let host = if slug == crate::GITHUB_PLATFORM {
        "github.com"
    } else {
        "gitee.com"
    };
    let mut out = Vec::new();
    if let Some(base) = proxy_base.filter(|base| !base.is_empty()) {
        out.push(Candidate {
            key: format!("{slug}:{ROUTE_PROXY}"),
            url: format!("{}/{slug}/{repo}.git", base.trim_end_matches('/')),
        });
    }
    if use_ssh && slug == crate::GITHUB_PLATFORM {
        out.push(Candidate {
            key: format!("{slug}:{ROUTE_SSH}"),
            url: format!("ssh://git@github.com:22/{repo}.git"),
        });
    }
    if let Some(token) = token.filter(|token| !token.is_empty()) {
        let user = if slug == crate::GITHUB_PLATFORM {
            "x-access-token"
        } else {
            "oauth2"
        };
        out.push(Candidate {
            key: format!("{slug}:{ROUTE_TOKEN}"),
            url: format!("https://{user}:{token}@{host}/{repo}.git"),
        });
    }
    if anonymous_fallback {
        out.push(Candidate {
            key: format!("{slug}:{ROUTE_ANONYMOUS}"),
            url: format!("https://{host}/{repo}.git"),
        });
    }
    out
}

/// 本进程的读数簿。推送与克隆落在同一个进程里，所以它们看到的顺序是同一份。
static BOOK: OnceLock<Mutex<RouteBook>> = OnceLock::new();

fn book() -> &'static Mutex<RouteBook> {
    BOOK.get_or_init(|| Mutex::new(RouteBook::default()))
}

/// 记一次走通到本进程的读数簿。
pub fn note_ok(key: &str, ms: u128) {
    book()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .record_ok(key, ms);
}

/// 记一次没走通到本进程的读数簿。
pub fn note_fail(key: &str, ms: u128, error: &str) {
    book()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .record_fail(key, ms, error);
}

/// 本进程此刻的读数快照。
pub fn snapshot() -> RouteBook {
    book().lock().unwrap_or_else(|e| e.into_inner()).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(candidates: &[Candidate], book: &RouteBook) -> Vec<String> {
        ordered(candidates, book)
            .into_iter()
            .map(|candidate| candidate.route().to_string())
            .collect()
    }

    fn all() -> Vec<Candidate> {
        candidates("github", "o/r", Some("http://gw"), true, Some("t"), true)
    }

    /// 网关基址配了，只是让它成为一条候选；它没被量过，就不会压过一条量过、走通了的路。
    #[test]
    fn a_configured_gateway_is_a_candidate_not_a_priority() {
        let all = all();
        let mut book = RouteBook::default();
        book.record_fail("github:proxy", 30, "connection refused");
        book.record_ok("github:token", 120);
        assert_eq!(
            keys(&all, &book),
            vec!["token", "ssh", "anonymous", "proxy"]
        );
    }

    /// 走通过的两条按耗时排，快的那条在前；没试过的排在已知能走的之后。
    #[test]
    fn measured_routes_are_ordered_by_their_own_latency() {
        let all = all();
        let mut book = RouteBook::default();
        book.record_ok("github:token", 120);
        book.record_ok("github:proxy", 40);
        assert_eq!(
            keys(&all, &book),
            vec!["proxy", "token", "ssh", "anonymous"]
        );
    }

    /// 一次失败立刻把那条路压到所有没试过的路之后：坏掉的路不会因为配过就一直先走。
    #[test]
    fn a_failed_route_sinks_below_every_untried_one() {
        let all = all();
        let mut book = RouteBook::default();
        book.record_fail("github:proxy", 10, "timeout");
        assert_eq!(
            keys(&all, &book),
            vec!["ssh", "token", "anonymous", "proxy"]
        );
    }

    /// 失败过的路排在最后，但仍在队列里：别的路都没走通时它还会被试一次，
    /// 不会因为失败过一次就永远不再量它。
    #[test]
    fn a_failed_route_is_still_retried_when_nothing_else_is_left() {
        let all = all();
        let mut book = RouteBook::default();
        book.record_fail("github:proxy", 10, "timeout");
        let order = ordered(&all, &book);
        assert_eq!(order.last().unwrap().route(), "proxy");
    }

    /// 同一档里的顺序就是候选声明顺序（稳定排序），不会两条路来回换位。
    #[test]
    fn ties_keep_the_declared_order() {
        let all = all();
        let book = RouteBook::default();
        assert_eq!(
            keys(&all, &book),
            vec!["proxy", "ssh", "token", "anonymous"]
        );
    }

    /// 没试过不是「成功 0 次」：试过但失败的路有读数，没试过的路没有读数，
    /// 两者排在不同的档里。
    #[test]
    fn never_tried_is_not_a_recorded_failure() {
        let mut book = RouteBook::default();
        book.record_fail("github:token", 5, "denied");
        assert!(book.reading("github:ssh").is_none());
        assert_eq!(book.reading("github:token").unwrap().successes, 0);
        assert_eq!(book.reading("github:token").unwrap().attempts, 1);
        assert_eq!(book.lines().len(), 1);
    }

    /// 只配了 token 的镜像没有匿名回退：匿名 URL 建出来的 remote 谁也推不上去。
    #[test]
    fn a_mirror_without_a_route_has_no_anonymous_fallback() {
        assert!(candidates("gitee", "o/r", None, false, None, false).is_empty());
        let with_token = candidates("gitee", "o/r", None, false, Some("t"), false);
        assert_eq!(with_token.len(), 1);
        assert_eq!(with_token[0].route(), ROUTE_TOKEN);
        assert!(with_token[0].url.contains("oauth2:t@gitee.com"));
    }

    /// 同一个路由名在两个主机上的读数互不影响。
    #[test]
    fn one_route_name_keeps_a_reading_per_host() {
        let mut book = RouteBook::default();
        book.record_ok("github:proxy", 10);
        book.record_fail("gitee:proxy", 99, "refused");
        assert!(book.reading("github:proxy").unwrap().last_ok);
        assert!(!book.reading("gitee:proxy").unwrap().last_ok);
    }
}
