//! 事件载荷的字段目录：每种 kind 的每一版里有哪些字段、每个字段是什么意思。
//!
//! 信封（`cogneva_event` 的列）回答「这是谁的一条什么事」；载荷是 text，回答
//! 「这一类事自己的内容」。载荷存 text 是为了逐字节保留（内容寻址与「擦除后仍能
//! 证明原字节」都靠这份字节），代价是「里面有什么」不能靠类型回答——于是需要这份
//! 目录：读的人不必去读代码，`undecodable` 也说得出缺了什么。
//!
//! 它与写者的关系由测试对账：写者写出去的键必须在这里逐个有名字，多一个少一个都红。
//!
//! 它住在 core 而不是某个实现 crate：写者在 `cog-storage`，读者在别处，两边都只依赖
//! core——这份目录正是它们之间那条契约。

/// 一种 kind 的一版载荷里有哪些字段、每个字段是什么意思。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadField {
    pub kind: &'static str,
    pub version: i32,
    /// (字段名, 它说的是什么)。
    pub fields: &'static [(&'static str, &'static str)],
}

/// 这一版代码认识的每一版载荷。
pub const PAYLOAD_CATALOG: &[PayloadField] = &[
    PayloadField {
        kind: "change_candidate",
        version: 1,
        fields: &[
            ("kind", "产物种类（code_change / skill_refinement …）"),
            ("status", "记下这条候选时它自己的状态词"),
            ("description", "这条变更要做什么"),
            ("content", "diff 正文：`.diff` 只是它的导出"),
            ("created_at", "生成时刻（RFC 3339）"),
            ("eval_summary", "评估门的判词与散文；null 表示没有评估"),
            ("tiering", "路由档位；null 表示没算过"),
        ],
    },
    PayloadField {
        kind: "change_measured",
        version: 1,
        fields: &[
            ("status", "这一步之后它自己的状态词"),
            ("note", "为什么变成这个状态；空串表示只是一次推进"),
        ],
    },
    PayloadField {
        kind: "change_accepted",
        version: 1,
        fields: &[("reason", "定案原因（落地在哪、为什么算成）")],
    },
    PayloadField {
        kind: "change_rejected",
        version: 1,
        fields: &[("reason", "拒绝原因；这一格是「为什么没成」的唯一载体")],
    },
];

/// 某一版载荷的字段目录；没有登记就是「这一版代码不认识它」。
pub fn payload_fields(kind: &str, version: i32) -> Option<&'static [(&'static str, &'static str)]> {
    PAYLOAD_CATALOG
        .iter()
        .find(|entry| entry.kind == kind && entry.version == version)
        .map(|entry| entry.fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 目录里的每个 kind 都能被查到，且没有重复登记。
    #[test]
    fn every_catalogued_kind_is_findable_and_listed_once() {
        let mut seen = std::collections::BTreeSet::new();
        for entry in PAYLOAD_CATALOG {
            assert!(
                seen.insert((entry.kind, entry.version)),
                "{} v{} is catalogued twice",
                entry.kind,
                entry.version
            );
            assert_eq!(
                payload_fields(entry.kind, entry.version),
                Some(entry.fields)
            );
            assert!(
                payload_fields(entry.kind, entry.version + 1).is_none(),
                "this build has no decoder for {} v{}",
                entry.kind,
                entry.version + 1
            );
        }
        assert!(payload_fields("no_such_kind", 1).is_none());
    }
}
