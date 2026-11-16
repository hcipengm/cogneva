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

/// Crockford base32：ULID 的字母表。没有 I / L / O / U——它们在手抄时与 1 / 1 / 0 / V
/// 分不开，而 `event_id` 是要被人念、被人贴进日志的。
const ALPHABET: [char; 32] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J',
    'K', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z',
];

/// `event_id` 的生成器：26 位 ULID，前 10 位毫秒时刻、后 16 位随机量。
///
/// 为什么要单调：时钟会回拨——NTP 校正、容器迁移、多写者各自的时钟都会让后写的一条
/// 拿到更早的毫秒。身份本身带时间前缀时，回拨直接让字典序与写入序反过来，于是
/// 「续跑读最后一条」读到的是更早的那一条、投影重放会覆盖新的结果。所以新的毫秒不晚于
/// 上一笔时**沿用上一笔的毫秒**并把随机量加一：`event_id` 的字典序因此永远等于写入序，
/// 而 `at` 退回它本来的角色——可读的墙上时间，不再承担顺序。
#[derive(Debug, Clone)]
pub struct EventIdFactory {
    last_ms: u64,
    random: [u8; 10],
}

impl EventIdFactory {
    /// 从一枚种子起。种子只要求多样，不要求秘密：`event_id` 不是凭证。
    pub fn seeded(at_ms: u64, seed: [u8; 10]) -> Self {
        Self {
            last_ms: at_ms,
            random: seed,
        }
    }

    /// 造下一枚身份。`at_ms` 是这一刻读到的毫秒；回拨时它被上一笔的毫秒顶掉。
    pub fn next(&mut self, at_ms: u64) -> String {
        if at_ms > self.last_ms {
            self.last_ms = at_ms;
        }
        let mut id = String::with_capacity(26);
        encode_u48(self.last_ms, &mut id);
        encode_u80(&self.random, &mut id);
        increment(&mut self.random);
        id
    }
}

/// 48 位的毫秒时刻编成前 10 位：每个字符 5 位，高位在前，最上面 2 位是 0。
fn encode_u48(value: u64, out: &mut String) {
    for shift in (0..10).rev() {
        out.push(ALPHABET[((value >> (shift * 5)) & 31) as usize]);
    }
}

/// 80 位的随机量编成后 16 位：每 5 位一组，高位在前。
fn encode_u80(bytes: &[u8; 10], out: &mut String) {
    for group in 0..16 {
        let mut index = 0usize;
        for bit in 0..5 {
            let position = group * 5 + bit;
            let bit_value = (bytes[position / 8] >> (7 - (position % 8))) & 1;
            index = (index << 1) | bit_value as usize;
        }
        out.push(ALPHABET[index]);
    }
}

/// 80 位随机量加一，按大端进位。同一毫秒内的多笔事件靠它分开并保持有序。
fn increment(bytes: &mut [u8; 10]) {
    for byte in bytes.iter_mut().rev() {
        let (next, carry) = byte.overflowing_add(1);
        *byte = next;
        if !carry {
            return;
        }
    }
}

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

    fn decode_first_ten(id: &str) -> u64 {
        id.chars().take(10).fold(0u64, |acc, character| {
            let digit = ALPHABET
                .iter()
                .position(|candidate| *candidate == character)
                .expect("an id is written in the alphabet");
            acc * 32 + digit as u64
        })
    }

    /// 时钟回拨：后写的一条读到更早的毫秒。身份必须仍然按写入序递增——否则按身份
    /// 读回来的「最后一条」是更早写的那条，续跑与投影重放都会用过期的结果覆盖新的。
    #[test]
    fn a_clock_rollback_does_not_reverse_the_identity_order() {
        let mut factory = EventIdFactory::seeded(2_000, [0; 10]);
        let read_ms = [2_000u64, 1_999, 1_500, 1_998];
        let ids: Vec<String> = read_ms.iter().map(|ms| factory.next(*ms)).collect();

        // 前提：这串读数按 at 排就不是写入序了——不然这条测试什么也没证明。
        assert!(
            read_ms.windows(2).any(|pair| pair[1] < pair[0]),
            "the sequence must roll the clock back to mean anything"
        );
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "identity order must follow write order: {ids:?}"
        );
        for id in &ids {
            assert_eq!(id.chars().count(), 26);
            assert!(id.chars().all(|character| ALPHABET.contains(&character)));
        }
    }

    /// 同一毫秒里的多笔：身份不同、且仍按写入序递增。随机量加一的那条路。
    #[test]
    fn ids_written_within_one_millisecond_are_distinct_and_ordered() {
        let mut factory = EventIdFactory::seeded(1_700_000_000_000, [0; 10]);
        let ids: Vec<String> = (0..64).map(|_| factory.next(1_700_000_000_000)).collect();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "same-millisecond ids must stay ordered"
        );
        assert_eq!(
            ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
            64
        );
    }

    /// 前 10 位就是那个毫秒时刻本身：`at` 与身份前缀说的是同一件事，回拨时前者说旧值、
    /// 后者说「这一笔不早于上一笔」。
    #[test]
    fn the_identity_prefix_is_the_read_millisecond_unless_the_clock_rolled_back() {
        let mut factory = EventIdFactory::seeded(1_234, [0; 10]);
        assert_eq!(decode_first_ten(&factory.next(1_234)), 1_234);
        assert_eq!(decode_first_ten(&factory.next(1_300)), 1_300);
        // 回拨：`at` 读到 900，身份前缀停在 1_300——它不是新的时间，是新的一条。
        assert_eq!(decode_first_ten(&factory.next(900)), 1_300);
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
