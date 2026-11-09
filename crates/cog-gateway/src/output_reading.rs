//! 产出面的三态读数：`measured` / `unwired` / `unread`。
//!
//! 一层产出这一拍读到了没有，不是「读到了多少」一个问题。`measured` 印值；`unwired`
//! 说清缺的是哪一层（这个部署没有那层，或者这个进程没挂那份读数）；`unread` 说清这一拍
//! 为什么读失败。三态要做的修复各不相同——一个去接线，一个去查读失败——折成一个数就
//! 分不出该修哪儿；而把 `unwired`／`unread` 印成 0 会被读成「量过了，是零」。
//!
//! 与能力面（[`crate::capabilities::SourceReading`]）和工作台
//! （[`crate::workbench::ReadingState`]）是同一个形状，只是这里带值。产出面
//! （[`crate::output`]）用的就是这一份定义：`output` 模块把它 `pub use` 出去，所以
//! `crate::output::Reading` 与 `crate::output_reading::Reading` 是同一个类型，不是两份，
//! 不会有一处改了三态、另一处还按旧的三态印。

use serde::Serialize;

/// 一层产出这一拍读到了没有。
///
/// 与能力面同一个形状：`measured` 印值，`unwired` 说清缺哪一层，`unread` 说清这一拍
/// 为什么读失败。三态要做的修复各不相同，折成一个数就分不出该修哪儿。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Reading<T> {
    Measured { value: T },
    Unwired { because: String },
    Unread { because: String },
}

impl<T> Reading<T> {
    pub fn measured(value: T) -> Self {
        Reading::Measured { value }
    }

    pub fn unwired(because: impl Into<String>) -> Self {
        Reading::Unwired {
            because: because.into(),
        }
    }

    pub fn unread(because: impl Into<String>) -> Self {
        Reading::Unread {
            because: because.into(),
        }
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Reading::Measured { value } => Some(value),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 量到了才经 `value()` 交值。
    #[test]
    fn a_measured_reading_carries_its_value() {
        assert_eq!(Reading::measured(3usize).value(), Some(&3));
    }

    /// 没接上的那层不许经 `value()` 交出 0：0 是一个读数，会被读成「量过了，是零」。
    #[test]
    fn an_unwired_layer_does_not_answer_zero() {
        let reading = Reading::<usize>::unwired("这个部署没有那一层");
        assert_eq!(reading.value(), None);
        assert_eq!(
            reading,
            Reading::Unwired {
                because: "这个部署没有那一层".into()
            }
        );
    }

    /// 读失败也不许交出 0：这一拍没读到，不等于这一拍是零。
    #[test]
    fn an_unread_layer_does_not_answer_zero() {
        let reading = Reading::<usize>::unread("连不上库");
        assert_eq!(reading.value(), None);
        match reading {
            Reading::Unread { because } => assert!(because.contains("连不上库")),
            other => panic!("读失败该是 unread，实际是 {other:?}"),
        }
    }

    /// 三态在线上就是三个词、各带自己的字段，与能力面、工作台说的是同一套。
    #[test]
    fn the_three_states_are_three_words_on_the_wire() {
        let measured = serde_json::to_value(Reading::measured(2usize)).unwrap();
        assert_eq!(measured["state"], "measured");
        assert_eq!(measured["value"], 2);

        let unwired = serde_json::to_value(Reading::<usize>::unwired("缺一层")).unwrap();
        assert_eq!(unwired["state"], "unwired");
        assert!(unwired.get("value").is_none());

        let unread = serde_json::to_value(Reading::<usize>::unread("读失败")).unwrap();
        assert_eq!(unread["state"], "unread");
        assert!(unread.get("value").is_none());
    }
}
