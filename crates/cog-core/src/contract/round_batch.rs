//! principle: feedback_the_unit_of_work_is_the_batch_not_the_turn
//! 定批：这一轮要做的是**哪几条**，在开工那一刻钉进事件流。
//!
//! 带外 worker 的 `round.py` 就是这么做的：开一轮写一条 `round_opened`，载荷里是台账
//! 那一版的 rev 与**这一批的 id 清单**。清单不进文件、不抄副本——清单抄一份到别处就
//! 多一份会漂的副本，而正本只有事件流。
//!
//! 平台的进化循环今天没有这一步：它每拍读一次 `change_ledger` 的 pending，挑哪些做、
//! 做几条，都由循环临场决定。于是同一个病在平台侧重演——「这一轮做了个我挑的子集」，
//! 而且事后没有任何一条读数能说清当时那一批是什么。把同一件事做成契约，两侧读的是
//! 同一张表、同一种事件。
//!
//! 三条形状：
//!
//! - **批次是 id 清单，不是「pending 的全集」**：全集是读的那一刻算出来的，会随着别人
//!   往账上写新行而变；钉住的清单不会。「这一轮该做的」必须是一个定值，否则「做完了
//!   没有」永远问不出来。
//! - **一轮只有一条开着的**：最后一条没有配对 `round_closed` 的 `round_opened` 就是
//!   当前那一轮。配对按 `subject_id`（这一轮的身份），不按时间先后猜。
//! - **幂等键是这一轮的身份**：同一次开工重放写的是同一条事件，不是第二条轮次。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// 一轮开工：批次清单与两个坐标。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoundOpened {
    /// 这一轮的身份。重放保护与 `round_closed` 的配对都靠它。
    pub round: String,
    /// 这一批是从台账的哪一版读出来的（git rev）；平台自己的变更账在库里，没有 rev，
    /// 于是这一格是空串——**空串不是「没有台账」**，是「这本台账没有版本坐标」。
    pub ledger_rev: String,
    /// 开工那一刻代码树的 rev，缺口判据从这一版算起。
    pub code_rev: String,
    /// 这一批要做的那几条，按台账里的次序。空清单是一次**没有内容**的开工，不是
    /// 「默认全量」：全集由调用方读出来再钉进来，读取的那一步不在这里。
    pub batch: Vec<String>,
    /// 发起方身份。
    pub actor: String,
    /// 写入口取自己的钟填进来的时刻。
    pub at: DateTime<Utc>,
}

impl RoundOpened {
    /// 幂等键：这一轮的身份。
    pub fn idempotency_key(&self) -> String {
        self.round.clone()
    }

    /// 载荷。键名与带外 `round.py` 写的那一条**逐字相同**：两侧读同一张表，键名不同
    /// 就等于两个形状，而它们只会有一个是对的。
    pub fn attributes(&self) -> serde_json::Value {
        serde_json::json!({
            "ledger_rev": self.ledger_rev,
            "code_rev": self.code_rev,
            "batch": self.batch,
        })
    }
}

/// 往事件流里记一轮，并回答当前开着的那一轮是哪一轮。
#[async_trait]
pub trait RoundEvents: Send + Sync {
    fn writer(&self) -> &str;

    /// 记一条 `round_opened`。实现必须幂等：同一轮重放不写第二条。
    async fn open(&self, round: &RoundOpened) -> SFResult<()>;

    /// 当前开着的那一轮；没有就是 `None`。
    ///
    /// `None` 与「读不到」是两件事：读不到必须报错，不能答 `None`——一个读不出来的流
    /// 会把自己读成「这一轮没人开工」，而缺口的判据随后就永远为真。
    async fn current(&self) -> SFResult<Option<RoundOpened>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round() -> RoundOpened {
        RoundOpened {
            round: "r1".into(),
            ledger_rev: "abc".into(),
            code_rev: "def".into(),
            batch: vec!["S1".into(), "S2".into()],
            actor: "orchestrator".into(),
            at: Utc::now(),
        }
    }

    /// 载荷的键名与带外那一版逐字相同：两侧读同一张表，键名不同就是两个形状。
    #[test]
    fn the_payload_uses_the_keys_the_host_side_writes() {
        let attrs = round().attributes();
        assert_eq!(attrs["ledger_rev"], "abc");
        assert_eq!(attrs["code_rev"], "def");
        assert_eq!(attrs["batch"], serde_json::json!(["S1", "S2"]));
    }

    /// 批次是**定值清单**，不是「读的那一刻的全集」：它随事件走，不随账本走。
    #[test]
    fn the_batch_is_the_list_that_was_pinned() {
        let mut r = round();
        r.batch.push("S3".into());
        assert_eq!(
            r.attributes()["batch"],
            serde_json::json!(["S1", "S2", "S3"])
        );
    }

    /// 幂等键是这一轮的身份：重放的是同一轮，不是新的一轮。
    #[test]
    fn the_idempotency_key_is_the_round_identity() {
        assert_eq!(round().idempotency_key(), "r1");
    }
}
