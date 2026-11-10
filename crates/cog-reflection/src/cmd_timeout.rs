//! 「子进程超时」这一句话：生产者写它，判据也认它。
//!
//! 判据在部署器里：含这句话的错误被读成「我们没看到集群」，而不是「看清楚了
//! 集群有问题」。方向错了要付代价——一次 apiserver 握手超时被读成版本缺陷，
//! 刚推上去的好版本会被回滚，而回滚只退镜像，修不好一次网络抖动。
//!
//! 所以这句话不许各写一遍。它原来由十来处各自拼 `format!("{cmdline} timed out
//! after {secs}s")`，判据按其中一段 `contains`；改一处措辞，判据里那一类错误就
//! 悄悄消失，消失的方向正好是「把抖动读成缺陷」。压成下面两个之后，生产者只给
//! 命令行与秒数，判据认的是同一个常量——漂移没有任何一处可以落脚。
//!
//! 带后缀的版本（超时之后还有「kubectl 是被杀的」这类补充）用 [`timed_out`]
//! 的返回值再拼，而不是另写一句：补充的部分可以不同，被认的那一段必须相同。

/// 判据与生产者共同的那一段。判据那一侧把它放进 `contains` 的模式表里。
pub(crate) const TIMED_OUT_AFTER: &str = "timed out after";

/// `cmdline` 在 `timeout_secs` 秒后仍未结束时，写给它下游的那句话。
pub(crate) fn timed_out(cmdline: &str, timeout_secs: u64) -> String {
    format!("{cmdline} {}", timed_out_after(timeout_secs))
}

/// 没有命令行可写时（等的是任务状态、不是子进程）的那半句。
///
/// 单独留一个入口而不是让调用方自己拼「空命令行加空格」：被认的那一段仍然只有
/// 一份，前导空格这类差别也不会漂到判据里。
pub(crate) fn timed_out_after(timeout_secs: u64) -> String {
    format!("{TIMED_OUT_AFTER} {timeout_secs}s")
}

/// 判据与生产者共同的那一段：子进程被杀的原因是**它不再动**，不是它跑够了钟点。
///
/// 与 [`TIMED_OUT_AFTER`] 同一族、同一个用处：这类错误说的是「我们没等到答案」，
/// 而不是「答案说这一版坏了」。区别只在判据用什么认出「没动」——时钟停了多久，
/// 还是它多久没写一个字节。下游把两种都读成「本轮没有观测」，所以两者都要进
/// 同一张模式表，各写一遍就会有一边悄悄从判据里消失。
pub(crate) const WENT_SILENT: &str = cog_core::progress::WENT_SILENT;

/// `cmdline` 在 `silent` 里没有新产出、而这段静默已经超过实测窗口 `window` 时，
/// 写给它下游的那句话。
///
/// 句子本身住在 [`cog_core::progress`]——看门狗在那里产出这个结局，别的 crate
/// （编排、执行器、沙箱）也要写同一句话，放在这里它们就够不着了。
pub(crate) fn went_silent(
    cmdline: &str,
    silent: std::time::Duration,
    window: std::time::Duration,
) -> String {
    cog_core::progress::went_silent(cmdline, silent, window)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reading_a_verdict_matches_is_in_every_produced_sentence() {
        let sentence = timed_out("kubectl get deploy", 30);
        assert!(
            sentence.contains(TIMED_OUT_AFTER),
            "判据按这一段认它，生产者写出来却不含这一段，超时就从判据里消失了：{sentence}"
        );
        assert!(sentence.contains("30s"), "秒数要写进去：{sentence}");

        let stalled = went_silent(
            "kubectl get deploy",
            std::time::Duration::from_secs(63),
            std::time::Duration::from_secs(41),
        );
        assert!(
            stalled.contains(WENT_SILENT),
            "停住的判词也要能被判据认出来：{stalled}"
        );
        assert!(
            stalled.contains("63s") && stalled.contains("41s"),
            "{stalled}"
        );
    }
}
