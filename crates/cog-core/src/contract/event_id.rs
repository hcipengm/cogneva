//! 事件流顺序身份的生成器：26 位 ULID，前 10 位毫秒时刻、后 16 位随机量。
//!
//! 为什么顺序由身份承担、而不由时间承担：`at` 会回拨——NTP 校正、容器迁移、多写者
//! 各自的时钟都会让后写的一条拿到更早的毫秒。身份本身带时间前缀时，回拨直接让字典序
//! 与写入序反过来：续跑读到的「最后一条」是更早的那条，投影重放会拿旧结果覆盖新结果。
//!
//! 所以生成器记住**已经发出去的那个毫秒**：新的毫秒不晚于它时沿用旧值，把随机量加一。
//! `event_id` 的字典序因此永远等于写入序，而 `at` 退回它本来的角色——可读的墙上时间。
//!
//! 它住在 core 里，因为写事件流的地方不止一处（观测面的读数与存储面的候选队列各是一个
//! 写者），而「什么算一笔更晚的事件」这件事只能有一份答案。

/// Crockford base32：ULID 的字母表。没有 I / L / O / U——它们在手抄时与 1 / 1 / 0 / V
/// 分不开，而 `event_id` 是要被人念、被人贴进日志的。
const ALPHABET: [char; 32] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J',
    'K', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z',
];

/// 顺序身份的生成器。见模块注释。
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
