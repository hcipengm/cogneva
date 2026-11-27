//! 员工两条事实的写者：`cogneva_event` 上的 `employee_number_allocated` 与
//! `employee_retired`。名册是它们的投影。

#[cfg(test)]
mod tests {
    use super::*;

    /// 载荷拼出来的信封过得了检查：kind 在闭集里、subject 指向这个人。
    #[test]
    fn the_number_fact_has_a_complete_envelope() {
        let payload = cog_core::EventPayload::EmployeeNumberAllocated {
            agent_id: "employee:7".into(),
            person: "employee:7".into(),
            number: 7,
        };
        let fields = payload.envelope("e-1", "probe");
        assert!(cog_core::contract::event_envelope::check(&fields).is_ok());
        assert_eq!(fields.kind, "employee_number_allocated");
        assert_eq!(fields.subject_kind, "employee");
    }
}
