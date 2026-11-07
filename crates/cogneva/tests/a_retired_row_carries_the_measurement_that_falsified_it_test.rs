//! principle: feedback_a_demand_a_later_measurement_falsified_is_retired_with_that_measurement_not_implemented
//!
//! 账行的形态**不是**「文档说的就一定对」。一句写在文档里的要求，可能被它之后的一次实测
//! 证伪：文档说某处是 `offset 30m`，而照这个字面、这个范围去树上查，那块面上的东西已经
//! 不在了，或者本来就是被读错了。这时正确的处置是**退休**这一行——连同证伪它的那次测量
//! 一起记下来——而不是把它当成一件「实现完了」的活。
//!
//! 两件事必须同时成立，本门判的就是这两件：
//!
//! 1. 退休是**自己的**终点状态，与「落地」并列而不是它的别名。`ChangeState::Retired` 与
//!    `ChangeState::Landed` 都让一行离开待办集，但它们是两个答案：一个是「树上的样子已经
//!    满足这句要求」，另一个是「这句要求本身不该再被问」。把它们读成同一个，被证伪的要求
//!    就会以「已完成」的样子从账上消失，从此没有任何一行读数说它被证伪过——而这正是它
//!    与真正落地那批行唯一需要分得开的地方。
//! 2. 退休**带着证伪它的那次测量与理由**。`ChangeMeasurement::retired` 的第二个参数就是
//!    理由（`RetireReason` 的闭集），`at_revision` 是读到这次证伪的那棵树。一个可以不带
//!    理由、不带修订就构造出来的退休与删除同形：行没了，谁也没法再跟它争论它凭什么没的。
//!    序列化出去之后这三个键（`state`／`retire_reason`／`measured_rev`）是下游那个读者
//!    唯一能读到的东西，所以它们一并钉在这里。
//!
//! 随之为真的还有一条：被投递过的行不因为投递过就占着线。`holds_the_line()` 问的是「投递
//! 了一个还没被兑现的承诺」，退休的行不欠这个承诺——它已经被撤回了，哪怕 diff 确实到过线上。
//!
//! 判不了什么：
//!
//! - 那次测量**对不对**。树上到底有没有这个东西、文档是被实测证伪还是被读错，本门一个字都
//!   判不了：它只看这个结论在账上长成什么形状。
//! - 谁该退休哪一行。哪一条要求该被撤回是判断，不是形状。
//! - 存储层的列。理由与修订落在哪个 column、jsonb 里的键叫什么，由 PostgreSQL 那边的用例
//!   对着真库判；本门钉的是值上的这一半。

use chrono::Utc;
use cog_core::{ChangeIdentity, ChangeLedgerRow, ChangeMeasurement, ChangeState, RetireReason};

/// 读到那次证伪的树。三处用例共用一份，免得一处换了字面、另一处还在断言旧的。
const REVISION: &str = "0123456789ab";

/// 被证伪的那条要求本身：它就是账上的键，三部分分开存。
fn falsified() -> ChangeIdentity {
    ChangeIdentity::new("grep", "crates", "offset 30m")
}

/// 账上的一行，除状态与投递修订外取同一个形状。
///
/// 退休那一份由调用方补上理由与时刻——理由不在这里给默认值，正是为了让一个「退休了却说
/// 不出为什么」的行没法在这里被顺手造出来。
fn row(state: ChangeState, delivered_rev: Option<&str>) -> ChangeLedgerRow {
    let now = Utc::now();
    ChangeLedgerRow {
        identity: falsified(),
        state,
        title: Some("a document asks for offset 30m".into()),
        assertion: Some("crates carries what the document asks for".into()),
        sources: Vec::new(),
        first_seen_at: now,
        measured_at: Some(now),
        measured_rev: Some(REVISION.into()),
        delivered_rev: delivered_rev.map(str::to_string),
        landed_at: None,
        unsettled_reason: None,
        retire_reason: None,
        retired_at: None,
    }
}

/// 两个终点状态是两个答案，不是一个答案的两种写法。
#[test]
fn the_two_terminal_states_are_different_answers() {
    assert_ne!(ChangeState::Landed, ChangeState::Retired);
    assert!(!ChangeState::Landed.is_outstanding());
    assert!(!ChangeState::Retired.is_outstanding());
    for owed in [
        ChangeState::Conflict,
        ChangeState::Pending,
        ChangeState::Partial,
        ChangeState::Undecided,
    ] {
        assert!(
            owed.is_outstanding(),
            "{owed:?} is still work somebody owes"
        );
    }
    assert_ne!(
        ChangeState::Landed.as_str(),
        ChangeState::Retired.as_str(),
        "two states the readings group separately must not print one word"
    );
    assert_eq!(ChangeState::ALL.len(), 6);
    assert_eq!(
        ChangeState::ALL[4..],
        [ChangeState::Landed, ChangeState::Retired],
        "the two terminal states are the last two, side by side"
    );
    for state in ChangeState::ALL {
        assert_eq!(state.as_str().parse::<ChangeState>(), Ok(state));
    }
}

/// 退休带着它的理由，别的状态不带。
#[test]
fn a_retirement_carries_its_reason() {
    let retired = ChangeMeasurement::retired(falsified(), RetireReason::Superseded);
    assert_eq!(retired.state, ChangeState::Retired);
    assert_eq!(retired.retire_reason, Some(RetireReason::Superseded));
    assert!(retired.unsettled_reason.is_none());
    assert!(
        retired.measured_rev.is_none(),
        "a reason says why, not what was read: the revision is the next call's"
    );

    let pending = ChangeMeasurement::new(falsified(), ChangeState::Pending);
    assert_ne!(pending.state, ChangeState::Retired);
    assert!(pending.retire_reason.is_none());

    let landed = ChangeMeasurement::new(falsified(), ChangeState::Landed);
    assert_eq!(landed.state, ChangeState::Landed);
    assert!(
        landed.retire_reason.is_none(),
        "an implemented answer has nothing retired about it to explain"
    );
}

/// 证伪它的那次测量随退休一起走，落进账之后还能被重读。
#[test]
fn the_falsifying_measurement_travels_with_the_retirement() {
    let retired =
        ChangeMeasurement::retired(falsified(), RetireReason::Superseded).at_revision(REVISION);
    assert_eq!(retired.measured_rev.as_deref(), Some(REVISION));
    assert_eq!(
        retired.state,
        ChangeState::Retired,
        "naming the revision must not move the state"
    );
    assert_eq!(
        retired.retire_reason,
        Some(RetireReason::Superseded),
        "naming the revision must not drop the reason"
    );

    let stored = serde_json::to_value(&retired).expect("a measurement is what the store writes");
    assert_eq!(stored["state"], "retired");
    assert_eq!(stored["retire_reason"], "superseded");
    assert_eq!(stored["measured_rev"], REVISION);
}

/// 理由的闭集每个词只拼一次，且写出去能读回来。
#[test]
fn every_retire_reason_is_spelt_once() {
    const EXPECTED: [&str; 5] = [
        "superseded",
        "fixture",
        "out_of_scope",
        "surface_moved",
        "answered_elsewhere",
    ];
    assert_eq!(RetireReason::ALL.len(), EXPECTED.len());
    let words: Vec<&str> = RetireReason::ALL
        .iter()
        .map(|reason| reason.as_str())
        .collect();
    assert_eq!(words, EXPECTED, "the readings are grouped by this list");

    let mut distinct = words.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        words.len(),
        "two reasons print the same word, so a count of them answers one question twice: {words:?}"
    );

    for reason in RetireReason::ALL {
        assert_eq!(reason.as_str().parse::<RetireReason>(), Ok(reason));
    }
    assert!("not_a_reason".parse::<RetireReason>().is_err());
}

/// 退休的行不占线，哪怕有一个 diff 真的到过线上。
#[test]
fn a_retired_row_does_not_hold_the_line() {
    let mut retired = row(ChangeState::Retired, Some(REVISION));
    retired.retire_reason = Some(RetireReason::SurfaceMoved);
    retired.retired_at = Some(Utc::now());

    assert!(!retired.state.is_outstanding());
    assert_eq!(retired.retire_reason, Some(RetireReason::SurfaceMoved));
    assert!(
        retired.landed_at.is_none(),
        "a retired row was never measured as satisfied, and must not read as if it were"
    );
    assert!(
        !retired.holds_the_line(),
        "a delivered diff on a retired row is not a promise the line is carrying: the demand \
         it was delivered for has been taken back"
    );

    let owed = row(ChangeState::Pending, Some(REVISION));
    assert!(
        owed.holds_the_line(),
        "an outstanding delivered change is exactly what does hold the line"
    );
}

/// 退休的理由是状态的一部分，不是事后挂上去的标签。
#[test]
#[should_panic(expected = "ChangeMeasurement::retired")]
fn a_retirement_without_a_reason_cannot_be_built() {
    // 唯一能构造退休的入口是 `retired`，而它要一个理由，所以下面两行跑不到。
    // 真要能跑到，就是这条原则在树上失效的那一刻：一行退休了，却说不出为什么。
    let built = ChangeMeasurement::new(falsified(), ChangeState::Retired);
    assert_eq!(
        built.state,
        ChangeState::Retired,
        "a retirement was built with no reason at all"
    );
}
