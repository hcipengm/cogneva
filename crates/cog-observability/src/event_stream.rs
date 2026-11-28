//! 事实面的正本：一条只追加的事件流。
//!
//! 形状由三样决定：**这一行是「发生了」还是「规定要这样」**（发生了的进来）、
//! **没有它做不了什么**（信封每个字段都为某个能力存在，见迁移 117 的逐列注释）、
//! **续跑要能从事里读出下一步**（`unit_of_work` + `step` + `outcome` + `attempt`）。
//!
//! 排序的唯一依据是 `event_id`（ULID）：`at` 会因时钟回拨与多写者漂移而不再全序，
//! 所以 `at` 只作可读时间，续跑与投影重建都按 `event_id` 走。
//!
//! 这里先落三件事：身份生成器、信封的类型与「续跑点」的纯函数。写路径与投影随后按面
//! 接上；续跑算法先写成不碰数据库的函数，因为它要能被一条测试穷举 0~7 步的完成前缀。

pub use cog_core::EventIdFactory;

/// 一件事走到哪一步。顺序就是续跑读的顺序。
pub const STEPS: [&str; 6] = [
    "planned",
    "generated",
    "evaluated",
    "verified",
    "landed",
    "deployed",
];

/// 一件事的一条事件，按续跑需要的最小集合。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFacts {
    pub step: String,
    pub outcome: String,
    pub attempt: i32,
    /// 这条事件的 `event_id`，给续跑点当 `cause`。
    pub event_id: String,
}

/// 续跑要回到哪里。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePoint {
    pub step: String,
    pub attempt: i32,
    /// 上一次尝试的那条事件；`None` 表示这一步还没开始过。
    pub cause: Option<String>,
}

/// 从一件事已经发生的事件里算出下一步该做什么。
///
/// 只看**结局**，不看次数、不看时长：某一步有 `ok` 就跳过；否则回到那一步的最后一次
/// 尝试 + 1。`aborted` / `failed` / `started` 都算「这一步没成」，区别在于 `started`
/// 的那次尝试可能留有中间产物——那由第 4 条（候选落库）接着用，不影响这里判下一步。
///
/// 六步都有 `ok` 时返回 `None`（这件事做完了）。
pub fn resume_point(events: &[EventFacts]) -> Option<ResumePoint> {
    for step in STEPS {
        let mut last: Option<&EventFacts> = None;
        let mut done = false;
        for event in events.iter().filter(|event| event.step == step) {
            if event.outcome == "ok" {
                done = true;
            }
            let later = last.is_none_or(|previous| event.attempt >= previous.attempt);
            if later {
                last = Some(event);
            }
        }
        if done {
            continue;
        }
        return Some(ResumePoint {
            step: step.to_string(),
            attempt: last.map_or(1, |event| event.attempt + 1),
            cause: last.map(|event| event.event_id.clone()),
        });
    }
    None
}

/// 载荷字段目录住在 `cog-core`：写者（存储层）与读者（解码器）分属两个 crate，
/// 「这一版载荷有什么」是它们之间的契约。这里只是转出去，调用点不必知道它住哪。
pub use cog_core::{payload_fields, PayloadField, PAYLOAD_CATALOG};

#[cfg(test)]
mod tests {
    use super::*;

    fn event(step: &str, outcome: &str, attempt: i32) -> EventFacts {
        EventFacts {
            step: step.to_string(),
            outcome: outcome.to_string(),
            attempt,
            event_id: format!("{step}-{attempt}-{outcome}"),
        }
    }

    /// 一条事件都没有：从第一步的第一次开始。这是「重启后从规划重来」唯一合法的情形
    /// ——那件事确实还没开始过。
    #[test]
    fn nothing_done_starts_at_the_first_step() {
        assert_eq!(
            resume_point(&[]),
            Some(ResumePoint {
                step: "planned".into(),
                attempt: 1,
                cause: None
            })
        );
    }

    /// 每一步的每一个完成前缀都要给出同一个下一步：这是续跑的穷举。
    #[test]
    fn every_finished_prefix_resumes_at_the_next_step() {
        for done in 0..STEPS.len() {
            let events: Vec<EventFacts> = STEPS[..done]
                .iter()
                .map(|step| event(step, "ok", 1))
                .collect();
            let point = resume_point(&events).expect("an unfinished unit has a next step");
            assert_eq!(point.step, STEPS[done], "after {done} steps");
            assert_eq!(point.attempt, 1, "a step never tried starts at one");
            assert_eq!(point.cause, None);
        }
        let all: Vec<EventFacts> = STEPS.iter().map(|step| event(step, "ok", 1)).collect();
        assert_eq!(resume_point(&all), None, "six ok steps is a finished unit");
    }

    /// 失败过的那一步：回到它的下一次尝试，并指着那条失败事件。
    #[test]
    fn a_failed_step_resumes_at_its_next_attempt() {
        let events = vec![
            event("planned", "ok", 1),
            event("generated", "failed", 1),
            event("generated", "failed", 2),
        ];
        assert_eq!(
            resume_point(&events),
            Some(ResumePoint {
                step: "generated".into(),
                attempt: 3,
                cause: Some("generated-2-failed".into())
            })
        );
    }

    /// 开始了没结束：也要从下一次尝试继续——这正是「跑到一半不再算失败」的那一半。
    #[test]
    fn a_started_step_is_not_done() {
        let events = vec![event("planned", "started", 1)];
        assert_eq!(
            resume_point(&events),
            Some(ResumePoint {
                step: "planned".into(),
                attempt: 2,
                cause: Some("planned-1-started".into())
            })
        );
    }

    /// 后来成功的那一步即使先前失败过也不再重来。
    #[test]
    fn a_step_that_succeeded_later_is_done() {
        let events = vec![
            event("planned", "failed", 1),
            event("planned", "ok", 2),
            event("generated", "ok", 1),
        ];
        assert_eq!(
            resume_point(&events).map(|point| point.step),
            Some("evaluated".into())
        );
    }

    /// 续跑按身份读：写入时时钟回拨过，读回来第一次尝试仍在最后一次尝试之前，
    /// 续跑点的 `cause` 指向真正最后写的那一条。
    #[test]
    fn a_resume_reads_rolled_back_writes_in_identity_order() {
        let mut factory = EventIdFactory::seeded(1_000, [0; 10]);
        // 第一笔规划成功，之后生成连试三次都失败；读数里时钟回拨了两次。
        let read_ms = [1_000u64, 999, 1_000, 998];
        let written: Vec<(u64, EventFacts)> = read_ms
            .iter()
            .enumerate()
            .map(|(index, at_ms)| {
                (
                    *at_ms,
                    EventFacts {
                        step: if index == 0 { "planned" } else { "generated" }.to_string(),
                        outcome: if index == 0 { "ok" } else { "failed" }.to_string(),
                        attempt: if index == 0 { 1 } else { index as i32 },
                        event_id: factory.next(*at_ms),
                    },
                )
            })
            .collect();

        let mut by_identity = written.clone();
        by_identity.sort_by(|left, right| left.1.event_id.cmp(&right.1.event_id));
        assert_eq!(by_identity, written, "identity order is write order");

        // 按 `at` 排读出来的「最后一次尝试」是第 2 次：这正是身份要承担顺序的理由。
        let mut by_wall_clock = written.clone();
        by_wall_clock.sort_by_key(|(at_ms, _)| *at_ms);
        assert_eq!(by_wall_clock.last().expect("four writes").1.attempt, 2);

        let events: Vec<EventFacts> = by_identity.into_iter().map(|(_, e)| e).collect();
        let point = resume_point(&events).expect("generation never succeeded");
        assert_eq!(point.step, "generated");
        assert_eq!(point.attempt, 4);
        assert_eq!(
            point.cause.as_deref(),
            Some(written[3].1.event_id.as_str()),
            "the cause is the last written event, not the latest wall clock"
        );
    }
}
