//! 「这张表保留多少天」这个旋钮怎么读。
//!
//! 三张归档表各有一个这样的旋钮，各读自己的环境变量：
//! `COGNEVA_TASK_EVENT_KEEP_DAYS`（任务事件流）、`COGNEVA_LLM_EXCHANGE_KEEP_DAYS`
//! （提示与回包）、`COGNEVA_LLM_USAGE_KEEP_DAYS`（用量账）。三份解析一字不差，
//! 只有**读到 0** 那一条分支不同——而那条分支恰恰是它们唯一真正需要各自表态的
//! 地方。抄写不携带表态：谁把哪一句改成另一句，编译不失败、测试不失败，那个旋钮
//! 就从「关掉清扫」变成「按默认三十天清扫」或者反过来，而且没有任何读数会说出这
//! 件事发生过。
//!
//! 所以读的规矩只有一份（[`keep_days_or_default`] 与 [`keep_days_or_disable`]
//! 共用下面那条解析），**0 算什么意思**变成选哪个函数——写在调用点上，一个词，
//! 不需要读者去比对两段几乎相同的代码。两种含义今天都在树上，各有各的理由：
//!
//! - 落回默认（[`keep_days_or_default`]）：0 视为写坏了。保留期是零天意味着每一趟
//!   都把表清空，而「一张被清空的表」与「保留策略正常工作」在读数上同形——那正是
//!   这个旋钮要防的事，不该由它自己制造。
//! - 关掉清扫（[`keep_days_or_disable`]）：0 是显式关掉这一趟清扫。读者从偏移零
//!   开始回放整条流，删掉前缀就答成一个看起来像「这次运行刚开始」的缺口，于是这个
//!   旋钮宁可让人明说「不扫」，也不接受一个近似零的天数。
//!
//! 这两句对同一个输入给出相反的处置，本模块不替任何一方裁决；它保证的是这件事被
//! 写成一个写在调用点上的词，而不是一次抄写留下的巧合。要统一含义，改的是那张登记
//! 表（`a_retention_knob_says_what_zero_means_test`）与被登记的那一处，不是这里的
//! 读法。
//!
//! 两条共用的规矩：两头空白剥掉；读不出数字、或低于 `min_days`（负数都在内）都落
//! 回 `default_days`。旋钮写坏了不该把一张表清空。

/// 把原始读数读成一个数：剥空白，读不出来就是 `None`。
///
/// 这一条是两条策略唯一的共同底座；`0` 在这里还是 `Some(0)`，怎么处置由上面那两
/// 个函数各自决定。
fn read_days(raw: Option<&str>) -> Option<i64> {
    raw.and_then(|v| v.trim().parse::<i64>().ok())
}

/// 一个保留天数旋钮：读不出、低于下限、或者读到 0，都落回默认值。
///
/// 给「0 是写坏了」的那一类旋钮：返回值永远是一个生效的保留天数，调用方不必处理
/// 「没有天数」这一支。
pub fn keep_days_or_default(raw: Option<&str>, default_days: i64, min_days: i64) -> i64 {
    match read_days(raw) {
        Some(days) if days >= min_days => days,
        _ => default_days,
    }
}

/// 一个保留天数旋钮：读到 0 是显式关掉，返回 `None`；其余同上。
///
/// 给「不扫」本身是一个正当状态的那一类旋钮。关掉之后读数照发——这张表现在有
/// 多大、最老一行多旧，仍然答得出来，所以「关掉」不等于「不知道」。
pub fn keep_days_or_disable(raw: Option<&str>, default_days: i64, min_days: i64) -> Option<i64> {
    match read_days(raw) {
        Some(0) => None,
        Some(days) if days >= min_days => Some(days),
        _ => Some(default_days),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT: i64 = 30;
    const MIN: i64 = 1;

    /// 两种表态只在 `0` 上分道：同一个正数读数必须给出同一个天数。
    #[test]
    fn the_two_policies_disagree_on_zero_and_nowhere_else() {
        for raw in [
            None,
            Some(""),
            Some(" 7 "),
            Some("1"),
            Some("90"),
            Some("-7"),
            Some("not a number"),
            Some("1.5"),
        ] {
            assert_eq!(
                Some(keep_days_or_default(raw, DEFAULT, MIN)),
                keep_days_or_disable(raw, DEFAULT, MIN),
                "{raw:?} 只在 0 上允许两种表态不同"
            );
        }
    }

    /// 那个唯一的零点：两侧各写死一条，改哪一条都会红。
    #[test]
    fn zero_means_whatever_the_knob_says_it_means() {
        assert_eq!(
            keep_days_or_default(Some("0"), DEFAULT, MIN),
            DEFAULT,
            "落回默认的那一类：0 天等于每一趟清空，与「保留正常工作」在读数上同形"
        );
        assert_eq!(
            keep_days_or_disable(Some("0"), DEFAULT, MIN),
            None,
            "关掉的那一类：0 是显式关掉，不是「按最短保留期清扫」"
        );
    }

    /// 一个读得出来的正数按它自己的数走，包括它恰好等于下限。
    #[test]
    fn a_readable_retention_is_honoured() {
        assert_eq!(keep_days_or_default(Some("7"), DEFAULT, MIN), 7);
        assert_eq!(keep_days_or_default(Some(" 90 "), DEFAULT, MIN), 90);
        assert_eq!(keep_days_or_default(Some("1"), DEFAULT, MIN), 1);
        assert_eq!(keep_days_or_disable(Some("7"), DEFAULT, MIN), Some(7));
        assert_eq!(keep_days_or_disable(Some(" 90 "), DEFAULT, MIN), Some(90));
    }

    /// 读不出来与低于下限落回默认，在任何表态下都不是「关掉」的另一种写法。
    #[test]
    fn a_value_that_cannot_be_read_falls_back() {
        for raw in [None, Some(""), Some("-7"), Some("nope")] {
            assert_eq!(keep_days_or_default(raw, DEFAULT, MIN), DEFAULT);
            assert_eq!(keep_days_or_disable(raw, DEFAULT, MIN), Some(DEFAULT));
        }
    }

    /// 下限是调用方给的：低于它的正数落回默认，而不是被当成合法的小保留期。
    #[test]
    fn the_floor_belongs_to_the_knob() {
        assert_eq!(keep_days_or_default(Some("2"), DEFAULT, 3), DEFAULT);
        assert_eq!(keep_days_or_default(Some("3"), DEFAULT, 3), 3);
        assert_eq!(keep_days_or_disable(Some("2"), DEFAULT, 3), Some(DEFAULT));
        assert_eq!(keep_days_or_disable(Some("3"), DEFAULT, 3), Some(3));
    }
}
