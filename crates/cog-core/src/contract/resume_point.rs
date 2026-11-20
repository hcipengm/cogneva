//! principle: feedback_a_verdict_that_writes_no_state_is_invisible_to_a_reader_that_selects_by_state
//! 续跑点：一条 `unit_of_work` 的步子走到哪儿了，以及下一步是第几拍。
//!
//! 事件流是只追加的，所以「跑到哪儿了」不是一个字段，是从这一条工作的事件里**算**出来
//! 的：按 `step` 找第一个没有以 `ok` 结束的步，就是该继续的那一步；同一 `step` 已经有
//! 过的尝试次数加一，就是这一拍该用的 `attempt`。
//!
//! 为什么是纯函数：续跑点要能在没有数据库、没有时钟、没有进程状态的地方被算出来，
//! 否则「重启之后从哪儿接着跑」只能靠现场那台机器回答。它是这一族唯一能被穷举测试
//! 的部分——0 到 7 步的每一种前缀都该算出同一个第一步。
//!
//! 三条口径写死在这里：
//!
//! - **`ok` 是唯一的完成词**。别的结局（失败、拒收、超时）都是「这一步还没过」，
//!   它们不推进续跑点，只把这一拍的尝试次数抬高。
//! - **步子里有洞就是没跑**。步 1、2、4 都有事件而 3 没有，续跑点是 3——不是 4：
//!   一条跳过了第三步的日志读起来像「跑完了」，而账上那一格是空的。
//! - **同一 `step` 重放不改变续跑点**，只改变 `attempt`。这是幂等键的同一个道理：
//!   重试用的是同一拍，不是新的一拍。

/// 一条事件里与续跑有关的那几列。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepEvent {
    /// 这一步在第几拍上跑的；从 1 起。
    pub attempt: u32,
    /// 这一步的结局词。`ok` 是唯一的完成词。
    pub outcome: String,
    /// 这一步的名字（`unit_of_work` 之内的步，比如 `write`、`verify`）。
    pub step: String,
}

/// 续跑点：该继续的那一步，与它已经用过几拍。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePoint {
    /// 这一步的名字；`None` 表示这一条工作的每一步都已经是 `ok`。
    pub step: Option<String>,
    /// 下一步该用的 `attempt`：这一步已有过的最大尝试次数加一。
    pub attempt: u32,
}

impl ResumePoint {
    /// 从一条工作的事件里算出续跑点。
    ///
    /// `order` 是这条工作**应该有**的步序（例如 `["plan", "write", "verify"]`），
    /// 由调用方给出：事件流里没有「计划了哪几步」这件事，只有真跑过的步，而从跑过的
    /// 步反推计划会把漏掉的步读成不存在。
    pub fn of(order: &[&str], events: &[StepEvent]) -> Self {
        for step in order {
            let mut highest: Option<u32> = None;
            let mut done = false;
            for event in events.iter().filter(|e| e.step == *step) {
                highest = Some(highest.map_or(event.attempt, |h| h.max(event.attempt)));
                if event.outcome == "ok" {
                    done = true;
                }
            }
            if !done {
                return Self {
                    step: Some((*step).to_string()),
                    attempt: highest.unwrap_or(0) + 1,
                };
            }
        }
        Self {
            step: None,
            attempt: 0,
        }
    }

    /// 这一条工作是不是每一步都跑完了。
    pub fn finished(&self) -> bool {
        self.step.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(step: &str, attempt: u32, outcome: &str) -> StepEvent {
        StepEvent {
            step: step.to_string(),
            attempt,
            outcome: outcome.to_string(),
        }
    }

    /// 每一步都可能停在 `ok` 之前：前缀 0 到 3 步，续跑点必须是第一个没 `ok` 的步。
    ///
    /// 这是这道门的主用例：它把一个「按步推进」的算法在每一种前缀上都问了一遍，
    /// 而任何一处「读起来像跑完了」的实现会在其中一个前缀上答错。
    #[test]
    fn every_prefix_resumes_at_the_first_step_without_ok() {
        // 0 到 7 步，覆盖一步、多步与全空三种边界：判据说的是「0~7 步前缀」。
        const ORDER: [&str; 7] = ["plan", "write", "verify", "test", "gate", "push", "report"];
        for prefix in 0..=7 {
            let events: Vec<StepEvent> = ORDER[..prefix]
                .iter()
                .map(|step| ev(step, 1, "ok"))
                .collect();
            let point = ResumePoint::of(&ORDER, &events);
            if prefix == ORDER.len() {
                assert!(point.finished(), "全 ok 了还算续跑点：{point:?}");
            } else {
                assert_eq!(
                    point.step.as_deref(),
                    Some(ORDER[prefix]),
                    "前缀 {prefix} 应当停在 {}；实得 {point:?}",
                    ORDER[prefix]
                );
                assert_eq!(point.attempt, 1, "这一步一拍都没跑过，下一次是第 1 拍");
            }
        }
    }

    /// 非 `ok` 的结局不推进续跑点，只抬高下一拍的编号。
    #[test]
    fn a_failed_step_is_retried_as_the_next_attempt() {
        let events = [ev("plan", 1, "ok"), ev("write", 1, "failed")];
        let point = ResumePoint::of(&["plan", "write"], &events);
        assert_eq!(point.step.as_deref(), Some("write"));
        assert_eq!(point.attempt, 2, "失败过一拍，下一次是第 2 拍");
    }

    /// 步子里有洞就是没跑到：1、3 有事件而 2 没有，续跑点是 2，不是 4。
    #[test]
    fn a_hole_in_the_sequence_is_where_the_run_resumes() {
        let events = [ev("plan", 1, "ok"), ev("verify", 1, "ok")];
        let point = ResumePoint::of(&["plan", "write", "verify"], &events);
        assert_eq!(
            point.step.as_deref(),
            Some("write"),
            "跳过的那一步读起来像跑完了，而账上那一格是空的"
        );
        assert_eq!(point.attempt, 1);
    }

    /// 同一拍重放不改变续跑点，只把那一拍的编号抬高。
    #[test]
    fn replaying_a_step_does_not_move_the_resume_point() {
        let events = [
            ev("plan", 1, "ok"),
            ev("write", 1, "error"),
            ev("write", 1, "error"),
        ];
        let point = ResumePoint::of(&["plan", "write"], &events);
        assert_eq!(point.step.as_deref(), Some("write"));
        assert_eq!(point.attempt, 2, "重放的是同一拍，不是新的一拍");
    }
}
