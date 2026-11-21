//! principle: feedback_a_bound_whose_only_raise_me_signal_is_pass_failure_never_asks_to_be_raised
//!
//! 追加压力：**什么时候该把写入口前移**（PG 收得住就一直是 PG）。
//!
//! 四条可机检的触发条件，任何一条为真才谈切换；四条都为假时，NATS 写路径**不许存在**
//! ——一个平时就开着的第二条写路径，等于让「同一笔事实有两个写者」在没有故障的时候也
//! 成立，而它的代价（对账、去重、顺序）与故障时的收益不成比例。
//!
//! 判据的形状是「读数 → 判词」，不是「配置项」：谁来问都得到同一个答案，而答案里点名
//! 是哪一条读数触发的。**表大不是判据**：实测的那张表按当前增速离换机器还很远，而行数
//! 与字节数本来就不在这四条里——它们答不了「追加跟不跟得上」。

/// 四条触发条件各自需要的那一个读数。
///
/// 每一项都是 `Option`：读不到与该条件为假必须分得开——把「量不到」读成「没触发」，
/// 正好会让该切换的时候什么都不发生。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct AppendReadings {
    /// 追加的 p99 毫秒数，与 UI 刷新周期的一半比。
    pub append_p99_ms: Option<f64>,
    /// 一个 UI 刷新周期是多少毫秒。
    pub ui_refresh_ms: Option<f64>,
    /// 事件流的滞后（秒），以及它与上一个窗口相比是不是在涨。
    pub stream_lag_seconds: Option<f64>,
    pub stream_lag_rising: Option<bool>,
    /// vacuum 或最老事务年龄（秒）是不是追不上。
    pub vacuum_falling_behind: Option<bool>,
    /// `store_growth` 的余量（天）与保留目标（天）。
    pub growth_headroom_days: Option<f64>,
    pub retention_target_days: Option<f64>,
}

/// 一条触发条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pressure {
    /// 追加 p99 持续超过 UI 刷新周期的一半。
    AppendLatency,
    /// 事件流滞后持续上升。
    StreamLag,
    /// vacuum / 最老事务年龄追不上。
    Vacuum,
    /// 余量按当前增速撑不到保留目标。
    Growth,
}

/// 判词：要不要切换，以及是哪几条读数在说。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// 为真的那几条；空表示不切。
    pub triggered: Vec<Pressure>,
    /// 读不到的那几条（它们既不为真也不为假）。
    pub unread: Vec<Pressure>,
}

impl Verdict {
    /// 该不该开写入口前移。**只有真触发才返回 true**：读不到不算。
    pub fn switch(&self) -> bool {
        !self.triggered.is_empty()
    }
}

/// 用一份读数判一次。
pub fn judge(readings: &AppendReadings) -> Verdict {
    let mut triggered = Vec::new();
    let mut unread = Vec::new();

    match (readings.append_p99_ms, readings.ui_refresh_ms) {
        (Some(p99), Some(refresh)) => {
            if p99 > refresh / 2.0 {
                triggered.push(Pressure::AppendLatency);
            }
        }
        _ => unread.push(Pressure::AppendLatency),
    }

    match (readings.stream_lag_seconds, readings.stream_lag_rising) {
        (Some(_), Some(true)) => triggered.push(Pressure::StreamLag),
        (Some(_), Some(false)) => {}
        _ => unread.push(Pressure::StreamLag),
    }

    match readings.vacuum_falling_behind {
        Some(true) => triggered.push(Pressure::Vacuum),
        Some(false) => {}
        None => unread.push(Pressure::Vacuum),
    }

    match (
        readings.growth_headroom_days,
        readings.retention_target_days,
    ) {
        (Some(headroom), Some(target)) => {
            if headroom < target {
                triggered.push(Pressure::Growth);
            }
        }
        _ => unread.push(Pressure::Growth),
    }

    Verdict { triggered, unread }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quiet() -> AppendReadings {
        AppendReadings {
            append_p99_ms: Some(2.0),
            ui_refresh_ms: Some(20.0),
            stream_lag_seconds: Some(0.5),
            stream_lag_rising: Some(false),
            vacuum_falling_behind: Some(false),
            growth_headroom_days: Some(3650.0),
            retention_target_days: Some(30.0),
        }
    }

    /// 四条都为假时，写入口前移**不许**存在——这是这条判据的主半边。
    #[test]
    fn nothing_triggers_means_no_second_write_path() {
        let verdict = judge(&quiet());
        assert!(verdict.triggered.is_empty());
        assert!(verdict.unread.is_empty());
        assert!(!verdict.switch(), "四条都为假却要求切换");
    }

    /// 任一条为真就必须开，并且点名是哪一条。
    #[test]
    fn any_trigger_names_itself() {
        let mut slow = quiet();
        slow.append_p99_ms = Some(19.0);
        assert_eq!(judge(&slow).triggered, vec![Pressure::AppendLatency]);

        let mut lagging = quiet();
        lagging.stream_lag_rising = Some(true);
        assert_eq!(judge(&lagging).triggered, vec![Pressure::StreamLag]);

        let mut vacuum = quiet();
        vacuum.vacuum_falling_behind = Some(true);
        assert_eq!(judge(&vacuum).triggered, vec![Pressure::Vacuum]);

        let mut growing = quiet();
        growing.growth_headroom_days = Some(10.0);
        assert_eq!(judge(&growing).triggered, vec![Pressure::Growth]);
    }

    /// 读不到不读成「没触发」：那会让该切的时候什么都不发生，而且没有任何读数说清。
    #[test]
    fn a_reading_that_cannot_be_taken_is_not_a_false_trigger() {
        let mut blind = quiet();
        blind.append_p99_ms = None;
        let verdict = judge(&blind);
        assert!(verdict.triggered.is_empty());
        assert_eq!(verdict.unread, vec![Pressure::AppendLatency]);
        assert!(
            !verdict.switch(),
            "量不到不等于要切，也不等于没事——它是第三种"
        );
    }

    /// 表大不是判据：行数/字节数根本不在读数里，判词自然答不出它。
    #[test]
    fn the_size_of_the_table_is_not_one_of_the_triggers() {
        assert_eq!(AppendReadings::default().growth_headroom_days, None);
    }
}
