//! principle: feedback_a_demand_a_later_measurement_falsified_is_retired_with_that_measurement_not_implemented
//!
//! 账行的形态**不是**「文档说的就一定对」。一句写在文档里的要求，可能被它之后的一次实测
//! 证伪：文档说某处是 `offset 30m`，而照这个字面、这个范围去树上查，那块面上的东西已经
//! 不在了，或者本来就是被读错了。这时正确的处置是**判它出账**——连同证伪它的那次测量与
//! 理由一起记下来——而不是把它当成一件「实现完了」的活。
//!
//! 两件事必须同时成立，本门判的就是这两件：
//!
//! 1. 被判出账**不是**落地，也不是第三种状态。账上只有 `pending` 与 `landed`：判出账的
//!    行仍然是 `pending`——它的身份还欠着一句话，谁也没有把它做出来。曾经它有自己的状态
//!    `retired`，而那个状态让被判方自己就能把一行挪出待办集：判定的形状与「完成了」的
//!    形状在读数上一模一样。判定现在住在 `retire_reason` 里，状态仍是「还欠着」。
//! 2. 判定**带着证伪它的那次测量与理由**。`ChangeMeasurement::retired` 的第二个参数就是
//!    理由（`RetireReason` 的闭集），`at_revision` 是读到这次证伪的那棵树。一个可以不带
//!    理由、不带修订就构造出来的判定与删除同形：行还在，却谁也没法再跟它争论它凭什么被
//!    判出账。序列化出去之后这三个键（`state`／`retire_reason`／`measured_rev`）是下游
//!    那个读者唯一能读到的东西，所以它们一并钉在这里。
//!
//! 随之为真的还有一条：被投递过的行不因为投递过就占着线。`holds_the_line()` 问的是「投递
//! 了一个还没被兑现的承诺」，被判出账的行不欠这个承诺——它已经被撤回了，哪怕 diff 确实
//! 到过线上。
//!
//! 判不了什么：
//!
//! - 那次测量**对不对**。树上到底有没有这个东西、文档是被实测证伪还是被读错，本门一个字都
//!   判不了：它只看这个结论在账上长成什么形状。
//! - 谁该判哪一行出账。哪一条要求该被撤回是判断，不是形状。
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

/// 账上只有两个状态，而被判出账的那一行不在「已完成」那一边。
#[test]
fn a_judged_out_row_is_still_owed() {
    assert_eq!(ChangeState::ALL.len(), 2);
    assert!(!ChangeState::Landed.is_outstanding());
    assert!(
        ChangeState::Pending.is_outstanding(),
        "a row the census judged out of the account is not the same as one that was made"
    );
    for state in ChangeState::ALL {
        assert_eq!(state.as_str().parse::<ChangeState>(), Ok(state));
    }
    for gone in ["retired", "undecided", "conflict", "partial"] {
        assert_eq!(
            gone.parse::<ChangeState>(),
            Ok(ChangeState::Pending),
            "an old word must read as owed work, never as done: {gone}"
        );
    }
}

/// 判定带着它的理由，落地的行不带。
#[test]
fn a_judgement_carries_its_reason() {
    let judged = ChangeMeasurement::retired(falsified(), RetireReason::Superseded);
    assert_eq!(judged.state, ChangeState::Pending);
    assert_eq!(judged.retire_reason, Some(RetireReason::Superseded));
    assert!(judged.unsettled_reason.is_none());
    assert!(
        judged.measured_rev.is_none(),
        "a reason says why, not what was read: the revision is the next call's"
    );

    let pending = ChangeMeasurement::new(falsified(), ChangeState::Pending);
    assert!(
        pending.retire_reason.is_none(),
        "a plain pending measurement says nothing about the row's place in the account"
    );

    let landed = ChangeMeasurement::new(falsified(), ChangeState::Landed);
    assert_eq!(landed.state, ChangeState::Landed);
    assert!(
        landed.retire_reason.is_none(),
        "an implemented answer has nothing retired about it to explain"
    );
}

/// 证伪它的那次测量随判定一起走，落进账之后还能被重读。
#[test]
fn the_falsifying_measurement_travels_with_the_judgement() {
    let judged =
        ChangeMeasurement::retired(falsified(), RetireReason::Superseded).at_revision(REVISION);
    assert_eq!(judged.measured_rev.as_deref(), Some(REVISION));
    assert_eq!(
        judged.state,
        ChangeState::Pending,
        "naming the revision must not move the state"
    );
    assert_eq!(
        judged.retire_reason,
        Some(RetireReason::Superseded),
        "naming the revision must not drop the reason"
    );

    let stored = serde_json::to_value(&judged).expect("a measurement is what the store writes");
    assert_eq!(stored["state"], "pending");
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

/// 被判出账的行不占线，哪怕有一个 diff 真的到过线上。
#[test]
fn a_judged_out_row_does_not_hold_the_line() {
    let mut judged = row(ChangeState::Pending, Some(REVISION));
    judged.retire_reason = Some(RetireReason::SurfaceMoved);
    judged.retired_at = Some(Utc::now());

    assert_eq!(judged.retire_reason, Some(RetireReason::SurfaceMoved));
    assert!(
        judged.landed_at.is_none(),
        "a judged-out row was never measured as satisfied, and must not read as if it were"
    );
    assert!(
        !judged.holds_the_line(),
        "a delivered diff on a judged-out row is not a promise the line is carrying: the demand \
         it was delivered for has been taken back"
    );

    let owed = row(ChangeState::Pending, Some(REVISION));
    assert!(
        owed.holds_the_line(),
        "an outstanding delivered change is exactly what does hold the line"
    );
}

/// 判定的理由不可能事后才挂上去，也不可能与「还欠着」以外的东西同时成立。
#[test]
fn a_plain_measurement_cannot_carry_a_reason() {
    // 唯一能带理由的入口是 `retired` / `undecided`，两个都要一个理由参数。`new` 构造出来
    // 的那一份两个理由都是空的——真要能带，就是一条判定没有经过任何入口就进了账。
    let built = ChangeMeasurement::new(falsified(), ChangeState::Pending);
    assert!(
        built.retire_reason.is_none() && built.unsettled_reason.is_none(),
        "a measurement built without a reason must not carry one"
    );
    let landed = ChangeMeasurement::new(falsified(), ChangeState::Landed);
    assert!(
        landed.retire_reason.is_none() && landed.unsettled_reason.is_none(),
        "a landed row cannot carry a reason about why it is still owed"
    );
}
