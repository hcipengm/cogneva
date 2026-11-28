//! 员工的编制与离场：两条事实，名册是它们的投影。
//!
//! 工号发出去的那一刻、一个人离场的那一刻，都写进同一条只追加流。名册今天存在
//! Redis／etcd 上，是这两条事实的投影：投影坏了从流重建，反过来不行。
//!
//! 与意图、待办同形：一个 trait、两条 record，信封在这一处拼一次。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{EnvelopeFields, SFResult};

/// 这条事实说的是谁：员工。
pub const EMPLOYEE_SUBJECT_KIND: &str = "employee";
/// 工号发出去。
pub const NUMBER_ALLOCATED: &str = "employee_number_allocated";
/// 一个人离场（工号不回收）。
pub const RETIRED: &str = "employee_retired";

/// 工号那一条的身份：人＋号。同一个人被发到同一个号是重投，不是第二条事实。
pub fn number_idempotency(agent_id: &str, number: u32) -> String {
    format!("{agent_id}:number:{number}")
}

/// 离场那一条的身份：人＋那一刻。同一个人离场两次是两条事实（出去又回来，
/// 再走一次），不拿「人」单独做键。
pub fn retired_idempotency(agent_id: &str, at: DateTime<Utc>) -> String {
    format!("{agent_id}:retired:{}", at.timestamp_millis())
}

fn base_fields(
    kind: &str,
    subject_id: &str,
    idempotency_key: String,
    writer: &str,
    event_id: &str,
    attributes: serde_json::Value,
) -> EnvelopeFields {
    EnvelopeFields {
        event_id: event_id.to_string(),
        kind: kind.to_string(),
        subject_kind: EMPLOYEE_SUBJECT_KIND.to_string(),
        subject_id: subject_id.to_string(),
        unit_of_work: subject_id.to_string(),
        step: if kind == NUMBER_ALLOCATED {
            "allocated".into()
        } else {
            "retired".into()
        },
        outcome: "ok".into(),
        idempotency_key,
        actor: subject_id.to_string(),
        writer: writer.to_string(),
        scope: "employee".into(),
        attributes: attributes.to_string(),
        causation_id: None,
        batch_id: None,
        session_id: None,
        attempt: 1,
        payload_version: 1,
    }
}

/// 工号那一条的信封。
pub fn envelope_of_number(
    agent_id: &str,
    person: &str,
    number: u32,
    writer: &str,
    event_id: &str,
) -> EnvelopeFields {
    base_fields(
        NUMBER_ALLOCATED,
        agent_id,
        number_idempotency(agent_id, number),
        writer,
        event_id,
        serde_json::json!({ "number": number, "person": person }),
    )
}

/// 离场那一条的信封。
pub fn envelope_of_retired(
    agent_id: &str,
    at: DateTime<Utc>,
    writer: &str,
    event_id: &str,
) -> EnvelopeFields {
    base_fields(
        RETIRED,
        agent_id,
        retired_idempotency(agent_id, at),
        writer,
        event_id,
        serde_json::json!({ "at": at.to_rfc3339() }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 两个 kind 都在闭集里，否则写入口在信封检查处就被拒。
    #[test]
    fn both_employee_facts_are_in_the_closed_set() {
        assert!(crate::is_known_kind(NUMBER_ALLOCATED));
        assert!(crate::is_known_kind(RETIRED));
    }

    /// 信封过得了检查，且工号与人都写在载荷里。
    #[test]
    fn the_number_envelope_carries_the_person_and_the_number() {
        let fields = envelope_of_number("employee:7", "employee:7", 7, "deployment", "e-1");
        assert!(crate::contract::event_envelope::check(&fields).is_ok());
        let attributes: serde_json::Value = serde_json::from_str(&fields.attributes).unwrap();
        assert_eq!(attributes["number"], 7);
        assert_eq!(attributes["person"], "employee:7");
    }

    /// 同一人同号是同一行；同一人再离场一次（不同时刻）是另一条事实。
    #[test]
    fn the_same_number_is_one_row_and_a_second_retirement_is_another() {
        assert_eq!(
            number_idempotency("employee:7", 7),
            number_idempotency("employee:7", 7)
        );
        let a = DateTime::from_timestamp(100, 0).unwrap();
        let b = DateTime::from_timestamp(200, 0).unwrap();
        assert_ne!(
            retired_idempotency("employee:7", a),
            retired_idempotency("employee:7", b)
        );
    }
}
