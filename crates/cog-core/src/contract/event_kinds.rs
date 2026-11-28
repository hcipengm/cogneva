//! kind 词表的两套写法：旧行上的细词，与聚合词＋`step`。
//!
//! 19 个细词里有很多是同一件事的几步（任务的定义、转移、租约、回收；员工发号、
//! 离场、注册、心跳）。历史不改写：已经落库的那些行仍写细词。此后新写的事实写
//! 聚合词（`task`／`employee`），用 `step` 说是哪一步。
//!
//! 读的人不该知道这个分界。所以问「这一族有哪些词」只问 [`kinds_of`]，它把两套
//! 写法一起答出来；问「这一条属于哪一族」只问 [`aggregate_of`]。分界只在这个文件
//! 里出现一次——读侧各自拼一份 `kind IN (...)` 的时候，新写的那一步就会从某个读面
//! 上静默消失，而那不会红。

/// 一个聚合词覆盖的全部 kind 词（旧细词在前，聚合词在后）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KindFamily {
    pub aggregate: &'static str,
    /// 旧细词，后一个是新聚合词本身；两者读同一族。
    pub kinds: &'static [&'static str],
}

/// 每一族。每个 [`crate::KINDS`] 里的词在这张表里恰好出现一次。
pub const KIND_FAMILIES: &[KindFamily] = &[
    KindFamily {
        aggregate: "employee",
        kinds: &["employee_number_allocated", "employee_retired", "employee"],
    },
    KindFamily {
        aggregate: "message",
        kinds: &["message_sent"],
    },
    KindFamily {
        aggregate: "intent",
        kinds: &["intent_received", "intent_triaged"],
    },
    KindFamily {
        aggregate: "task",
        kinds: &["task_defined", "task_transition", "task"],
    },
    KindFamily {
        aggregate: "round",
        kinds: &["round_opened", "round_closed"],
    },
    KindFamily {
        aggregate: "change",
        kinds: &[
            "change_candidate",
            "change_measured",
            "change_accepted",
            "change_rejected",
            "change_judged",
            "change_delivered",
            "change_declared",
            "change_landed",
        ],
    },
    // 两类调用各留一个词：读面与部分索引按它们分开建，合并会把「这一条是模型还是
    // 工具」从索引里拿掉，而这正是两侧各自最常问的一格。
    KindFamily {
        aggregate: "llm_call",
        kinds: &["llm_call"],
    },
    KindFamily {
        aggregate: "tool_call",
        kinds: &["tool_call"],
    },
    KindFamily {
        aggregate: "llm_skip",
        kinds: &["llm_skip"],
    },
    KindFamily {
        aggregate: "spend_verdict",
        kinds: &["spend_verdict"],
    },
    KindFamily {
        aggregate: "build",
        kinds: &["build_planned"],
    },
    KindFamily {
        aggregate: "principle",
        kinds: &["principle_judged", "principle_declared"],
    },
    KindFamily {
        aggregate: "key",
        kinds: &["key"],
    },
    KindFamily {
        aggregate: "redaction",
        kinds: &["redacted"],
    },
    KindFamily {
        aggregate: "storage_pressure",
        kinds: &["storage_pressure"],
    },
];

/// 这一条属于哪一族。认不出的词原样返回：读侧拿它去查，查不到就是查不到，
/// 不替它编一个族名。
pub fn aggregate_of(kind: &str) -> &str {
    for family in KIND_FAMILIES {
        if family.kinds.contains(&kind) {
            return family.aggregate;
        }
    }
    kind
}

/// 问一族时该查哪些 kind 词（旧细词＋聚合词）。认不出就是 `None`。
///
/// 读侧把它交给 SQL：`kind = ANY($1)`。写死一个词的读者读不到另一套写法写下的
/// 那一步，而那种缺失在读数上读作「没有发生过」。
pub fn kinds_of(aggregate: &str) -> Option<&'static [&'static str]> {
    KIND_FAMILIES
        .iter()
        .find(|family| family.aggregate == aggregate)
        .map(|family| family.kinds)
}

/// 这一版代码读得懂某一版的载荷吗。
///
/// 只认第 1 版：加一版时这里加一行，读侧才敢按新形状解码。读不懂的版本由读侧标
/// `undecodable`，原文留在库里——不猜、不丢。
pub fn payload_version_is_readable(kind: &str, version: i32) -> bool {
    version == 1 && crate::is_known_kind(kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 族表是一张划分：闭集里每个词恰好出现一次，没有词被两个族认领。
    #[test]
    fn the_families_partition_the_closed_set() {
        let mut seen: Vec<&str> = Vec::new();
        for family in KIND_FAMILIES {
            for kind in family.kinds {
                assert!(
                    !seen.contains(kind),
                    "{kind} 被两个族认领：{:?}",
                    aggregate_of(kind)
                );
                assert!(
                    crate::is_known_kind(kind),
                    "{kind} 不在闭集里，族表不该先于闭集多一个词"
                );
                assert_eq!(aggregate_of(kind), family.aggregate);
                seen.push(kind);
            }
        }
        let mut closed: Vec<&str> = crate::KINDS.to_vec();
        closed.sort_unstable();
        seen.sort_unstable();
        assert_eq!(seen, closed, "族表与闭集不是同一组词");
    }

    /// 两套写法问同一族时拿到同一张词表：只查旧词的读者看不到新写的那一步。
    #[test]
    fn both_spellings_are_read_as_one_family() {
        assert_eq!(
            kinds_of("task"),
            Some(["task_defined", "task_transition", "task"].as_slice())
        );
        assert_eq!(aggregate_of("task_transition"), "task");
        assert_eq!(aggregate_of("task"), "task");
        assert_eq!(aggregate_of("employee_number_allocated"), "employee");
        assert_eq!(aggregate_of("employee"), "employee");
        assert_eq!(kinds_of("no_such_family"), None);
        assert_eq!(aggregate_of("no_such_kind"), "no_such_kind");
    }

    /// 版本函数：这一版只认第 1 版，且只对闭集里的词作数。
    #[test]
    fn the_readable_versions_are_named() {
        assert!(payload_version_is_readable("task", 1));
        assert!(payload_version_is_readable("task_defined", 1));
        assert!(!payload_version_is_readable("task", 2));
        assert!(!payload_version_is_readable("no_such_kind", 1));
    }
}

/// 一类事件「谁在写、谁在读」的普查行。
pub struct EventClassRow {
    /// `KINDS` 里的那个词。
    pub kind: &'static str,
    /// 生产里的写者：写成树里的一个路径；`None` 是这一类还没有写者。
    pub writer: Option<&'static str>,
    /// 生产里的读者：写成树里的一个路径；`None` 是这一类还没有读面。
    pub reader: Option<&'static str>,
    /// 写者或读者缺席时，为什么；两边都在时可以为空。
    pub why: &'static str,
}

/// 每一类事件的写者与读者。判据在
/// `crates/cogneva/tests/every_event_class_names_its_writer_and_reader_test.rs`。
///
/// 缺席比在场更需要写下来：一类没有人写、或没有人读，读这件事的人不该去数表，
/// 而该在这一行读到「还没有」和原因。
pub const EVENT_CLASS_CENSUS: &[EventClassRow] = &[
    EventClassRow {
        kind: "employee_number_allocated",
        writer: Some("crates/cog-storage/src/redis/registry.rs"),
        reader: Some("crates/cog-storage/src/redis/registry.rs"),
        why: "",
    },
    EventClassRow {
        kind: "employee_retired",
        writer: Some("crates/cog-storage/src/redis/registry.rs"),
        reader: Some("crates/cog-storage/src/redis/registry.rs"),
        why: "",
    },
    EventClassRow {
        kind: "employee",
        writer: Some("crates/cog-storage/src/redis/registry.rs"),
        reader: Some("crates/cog-storage/src/redis/registry.rs"),
        why: "",
    },
    EventClassRow {
        kind: "message_sent",
        writer: Some("crates/cog-gateway/src/chat.rs"),
        reader: Some("crates/cog-storage/src/postgres/dialogue.rs"),
        why: "",
    },
    EventClassRow {
        kind: "intent_received",
        writer: Some("crates/cog-gateway/src/intent.rs"),
        reader: Some("crates/cog-storage/src/postgres/intent_events.rs"),
        why: "",
    },
    EventClassRow {
        kind: "intent_triaged",
        writer: Some("crates/cogneva/src/conversation.rs"),
        reader: Some("crates/cog-storage/src/postgres/intent_events.rs"),
        why: "",
    },
    EventClassRow {
        kind: "task_defined",
        writer: Some("crates/cog-storage/src/postgres/task_ledger.rs"),
        reader: Some("crates/cog-storage/src/postgres/task_ledger.rs"),
        why: "",
    },
    EventClassRow {
        kind: "task_transition",
        writer: Some("crates/cog-storage/src/postgres/task_ledger.rs"),
        reader: Some("crates/cog-storage/src/postgres/task_ledger.rs"),
        why: "",
    },
    EventClassRow {
        kind: "task",
        writer: Some("crates/cog-storage/src/postgres/state_backend.rs"),
        reader: Some("crates/cog-storage/src/postgres/task_ledger.rs"),
        why: "",
    },
    EventClassRow {
        kind: "round_opened",
        writer: Some("crates/cog-storage/src/postgres/round_events.rs"),
        reader: Some("crates/cog-storage/src/postgres/round_events.rs"),
        why: "",
    },
    EventClassRow {
        kind: "round_closed",
        writer: Some("crates/cog-storage/src/postgres/round_events.rs"),
        reader: Some("crates/cog-storage/src/postgres/round_events.rs"),
        why: "",
    },
    EventClassRow {
        kind: "change_candidate",
        writer: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        reader: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "change_measured",
        writer: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        reader: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "change_accepted",
        writer: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        reader: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "change_rejected",
        writer: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        reader: Some("crates/cog-storage/src/postgres/change_candidate_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "change_declared",
        writer: Some("crates/cog-storage/src/postgres/change_ledger_store.rs"),
        reader: Some("crates/cog-storage/src/postgres/change_ledger_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "change_landed",
        writer: Some("crates/cog-github/src/landing.rs"),
        reader: Some("crates/cog-storage/src/postgres/landing_states.rs"),
        why: "",
    },
    EventClassRow {
        kind: "llm_call",
        writer: Some("crates/cog-gateway/src/security_gateway.rs"),
        reader: Some("crates/cog-observability/src/usage_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "tool_call",
        writer: Some("crates/cog-agent/src/agent_kernel/runtime.rs"),
        reader: Some("crates/cog-storage/src/postgres/tool_call_events.rs"),
        why: "",
    },
    EventClassRow {
        kind: "principle_judged",
        writer: Some("crates/cog-storage/src/postgres/principle_ledger_store.rs"),
        reader: Some("crates/cog-storage/src/postgres/principle_ledger_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "principle_declared",
        writer: Some("crates/cog-storage/src/postgres/principle_ledger_store.rs"),
        reader: Some("crates/cog-storage/src/postgres/principle_ledger_store.rs"),
        why: "",
    },
    EventClassRow {
        kind: "key",
        writer: Some("crates/cog-storage/src/postgres/key_events.rs"),
        reader: Some("crates/cog-storage/src/postgres/key_events.rs"),
        why: "",
    },
    EventClassRow {
        kind: "llm_skip",
        writer: Some("crates/cog-observability/src/skip_store.rs"),
        reader: None,
        why: "读面还读表（S39 未落）",
    },
    EventClassRow {
        kind: "spend_verdict",
        writer: Some("crates/cog-observability/src/spend_verdict.rs"),
        reader: None,
        why: "读面还读表（S39 未落）",
    },
    EventClassRow {
        kind: "change_delivered",
        writer: Some("crates/cog-storage/src/postgres/change_ledger_store.rs"),
        reader: None,
        why: "读面还读表（N09 未落）",
    },
    EventClassRow {
        kind: "change_judged",
        writer: Some("crates/cog-storage/src/postgres/change_ledger_store.rs"),
        reader: None,
        why: "读面还读表（N09 未落）",
    },
    EventClassRow {
        kind: "redacted",
        writer: Some("crates/cog-storage/src/postgres/redaction.rs"),
        reader: Some("crates/cog-storage/src/postgres/redaction_read.rs"),
        why: "",
    },
    EventClassRow {
        kind: "build_planned",
        writer: Some("crates/cogneva/src/cell_plan.rs"),
        reader: Some("crates/cogneva/src/build_plan_readings.rs"),
        why: "",
    },
    EventClassRow {
        kind: "storage_pressure",
        // 生产里的写者是定期问判词的那一支（`StoragePressureWatch`，每五分钟一次、
        // 只在判词集合变了时落一条）；手工入口 `crates/cogneva/src/storage_pressure.rs`
        // 是同一个写者名的另一条路，写的是同一族事实。
        writer: Some("crates/cog-storage/src/append_pressure_watch.rs"),
        // 读面在宿主读数里（另一棵树里的 `cogneva-storage-pressure-reading.sh`：
        // 两条事实的年龄、触发与量不到的条件、「最近的提出还在等回答」）。它不在本仓的
        // 树里，所以这一格按「本仓没有读者」写，原因点名读面在哪——不拿一个本仓路径顶上。
        reader: None,
        why: "读面是另一棵树里的宿主读数（`cogneva-storage-pressure-reading.sh`），不在本仓树里",
    },
];
