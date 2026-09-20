//! CI 结论契约。
//!
//! 「这个 rev 的 CI 过了没有」有两个消费面：自产变更的落地通道（能合就合）
//! 与主线部署器（能滚就滚）。两处独立实现同一套判据就会各自漂移成一个
//! 版本，所以折判据放在这里，两边只负责取信号。
//!
//! 三态且刻意不对称：`None` 是「没有证据」，不是「大概没事」。取不到
//! （网络不可达、仓库无权限、检查还没跑完）时两个消费面都必须按缺证据
//! 处理——落地侧等下一轮，部署侧照常推进：把读不到当成失败会让一次上游
//! API 抖动停掉整条主线跟踪。

/// 代码平台 API 基址的 env 名。业务进程零 token，基址指向安全网关的透传
/// 端点。「CI 结论从哪里读」是落地通道与主线部署器共用的事实，两处各写
/// 一遍字符串就会在改名时只改掉一半。
pub const GITHUB_API_BASE_ENV: &str = "COGNEVA_GITHUB_API_BASE";
pub const GITEE_API_BASE_ENV: &str = "COGNEVA_GITEE_API_BASE";

/// 单条检查结论是否算通过。
///
/// `neutral` 与 `skipped` 算通过：它们是按路径过滤或带条件跳过的 job 报出的
/// 结论，平台自身的必需检查判定也把它们当作已满足。其余（`failure` /
/// `cancelled` / `timed_out` / `action_required` / 将来新增的字符串）一律算
/// 不通过——新增一个通过态字符串是上游平台的语义变化，默认安全侧才能暴露它。
pub fn ci_conclusion_passes(conclusion: &str) -> bool {
    matches!(conclusion, "success" | "neutral" | "skipped")
}

/// 把收集到的检查信号折成一个结论；`None` 表示没有证据。
///
/// - 一条检查都没看到：没有证据（仓库没开 CI，或查询失败）；
/// - 还有检查在跑：没有证据（此刻下结论等于拿半个结果判死刑）；
/// - 否则：全部通过才算通过。
pub fn fold_ci_signals(saw_signal: bool, pending: bool, conclusions: &[String]) -> Option<bool> {
    if !saw_signal {
        return None;
    }
    if pending {
        return None;
    }
    Some(conclusions.iter().all(|c| ci_conclusion_passes(c)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_signal_is_no_evidence() {
        assert_eq!(fold_ci_signals(false, false, &[]), None);
    }

    #[test]
    fn a_running_check_withholds_the_verdict() {
        assert_eq!(fold_ci_signals(true, true, &[]), None);
        assert_eq!(fold_ci_signals(true, true, &["success".into()]), None);
    }

    #[test]
    fn all_passing_conclusions_pass() {
        assert_eq!(
            fold_ci_signals(true, false, &["success".into(), "skipped".into()]),
            Some(true)
        );
    }

    #[test]
    fn any_non_passing_conclusion_fails() {
        assert_eq!(
            fold_ci_signals(true, false, &["success".into(), "failure".into()]),
            Some(false)
        );
    }

    #[test]
    fn an_unrecognized_conclusion_is_not_a_pass() {
        // 平台的通过态集合变化时，默认必须落到不通过这一侧。
        assert_eq!(
            fold_ci_signals(true, false, &["mystery".into()]),
            Some(false)
        );
        assert!(!ci_conclusion_passes("mystery"));
    }
}
