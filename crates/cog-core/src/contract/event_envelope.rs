//! principle: feedback_a_verdict_that_writes_no_state_is_invisible_to_a_reader_that_selects_by_state
//! 写进事件流之前，信封要先过一遍：哪几格不许空，哪些 kind 已经在闭集里。
//!
//! 信封的列各有各的用处，但**空着的那一列**与「这一列不适用」在读数上同形：一条
//! `kind` 没有 `subject_id` 的事件，读者按身份去取时读不到它；一条没有 `writer` 的
//! 事件，幂等约束那半边是空的，重放保护随之失效。所以写入口先问一遍，不合格就不写。
//!
//! 这里的判据只覆盖**写入口自己能判**的部分：几格不许空。至于「每列要有读者」，那是
//! 一条树判据（见 `crates/cogneva` 里读迁移列名的那条），不在这里——一个值合不合法
//! 与一列有没有人用是两件事，混在一起写会让两边都说不清。

/// 一条事件写进流之前的形状。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeFields {
    pub event_id: String,
    pub kind: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub unit_of_work: String,
    pub step: String,
    pub outcome: String,
    pub idempotency_key: String,
    pub actor: String,
    pub writer: String,
    pub scope: String,
    pub attributes: String,
}

/// 事件流里**全部**的 kind，一处列出。
///
/// 每个写入口只知道自己那一种，于是「全集」从来没有出现过——普查问「有哪些 kind」
/// 时，答案取决于谁碰巧写了一段。这一格把全集收在一处：新 kind 要么加进来，要么
/// 写入口当场被拒（[`check`] 会拒 `Unknown`）。
/// 名单按写入方分：轮次（`round_*`）、计划（`batch_planned`）、变更候选四个阶段
/// （`change_*`）、两类调用事实（`llm_call`／`tool_call`）。
pub const KINDS: [&str; 10] = [
    "round_opened",
    "round_closed",
    "batch_planned",
    "change_candidate",
    "change_measured",
    "change_accepted",
    "change_rejected",
    "llm_call",
    "tool_call",
    // 擦除：一段正文被销毁了，而事件流里留下「谁在什么时候、因为什么、销毁了哪一段」。
    // 它**不改写**被擦那条事件：历史照旧，指向正文的哈希还在，只是那个哈希现在指不到
    // 可读的字节——这正是「擦除」与「改历史」的区别。
    "redacted",
];

/// 这个 kind 在闭集里吗。
pub fn is_known_kind(kind: &str) -> bool {
    KINDS.contains(&kind)
}

/// 写入口拒绝它的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeDefect {
    /// 这一格不许空。
    Empty(&'static str),
    /// 这个 kind 不在闭集里。
    UnknownKind(String),
    /// `attributes` 要是 JSON 对象：一个字符串包着 JSON 在读侧解析两遍，而两遍里
    /// 有一遍出错时就只有一半读者读得到。
    AttributesNotAnObject,
}

impl EnvelopeDefect {
    pub fn as_str(&self) -> &'static str {
        match self {
            EnvelopeDefect::Empty(column) => column,
            EnvelopeDefect::UnknownKind(_) => "kind",
            EnvelopeDefect::AttributesNotAnObject => "attributes",
        }
    }
}

/// 写入口的判据：这些格子空着就不许写。
///
/// 名单是**能力**反过来写的：`subject_id` 缺了读者按身份取不到这条事件；`writer` 缺了
/// 幂等约束失去一半；`idempotency_key` 缺了重放会写第二条；`kind`/`outcome`/`step` 缺了
/// 续跑与统计都答不了。没有进这张名单的列，要么库里有默认值（`at`/`attempt`/
/// `payload_version`），要么本来就是可空的（`causation_id`/`batch_id`）。
pub fn check(fields: &EnvelopeFields) -> Result<(), EnvelopeDefect> {
    for (name, value) in [
        ("event_id", &fields.event_id),
        ("kind", &fields.kind),
        ("subject_kind", &fields.subject_kind),
        ("subject_id", &fields.subject_id),
        ("unit_of_work", &fields.unit_of_work),
        ("step", &fields.step),
        ("outcome", &fields.outcome),
        ("idempotency_key", &fields.idempotency_key),
        ("actor", &fields.actor),
        ("writer", &fields.writer),
    ] {
        if value.trim().is_empty() {
            return Err(EnvelopeDefect::Empty(name));
        }
    }
    if !is_known_kind(&fields.kind) {
        return Err(EnvelopeDefect::UnknownKind(fields.kind.clone()));
    }
    let parsed: serde_json::Value = serde_json::from_str(&fields.attributes)
        .map_err(|_| EnvelopeDefect::AttributesNotAnObject)?;
    if !parsed.is_object() {
        return Err(EnvelopeDefect::AttributesNotAnObject);
    }
    // `scope` 可以为空串（信封的默认值就是空串），但**不可是两个路径**：
    // 一个装两条路径的 scope 读起来像一个，读者会只按其中一条去匹配。
    if fields.scope.contains(',') || fields.scope.contains(' ') && !fields.scope.is_empty() {
        return Err(EnvelopeDefect::Empty("scope"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> EnvelopeFields {
        EnvelopeFields {
            event_id: "01H".into(),
            kind: "llm_call".into(),
            subject_kind: "llm_call".into(),
            subject_id: "call-1".into(),
            unit_of_work: "call-1".into(),
            step: "call".into(),
            outcome: "ok".into(),
            idempotency_key: "call-1".into(),
            actor: "a".into(),
            writer: "w".into(),
            scope: "".into(),
            attributes: "{}".into(),
        }
    }

    #[test]
    fn a_complete_envelope_passes() {
        assert_eq!(check(&good()), Ok(()));
    }

    /// 逐格把每个必填列清空，都必须被拒，并且点名是哪一格。
    #[test]
    fn every_required_cell_is_required_by_name() {
        for column in [
            "event_id",
            "kind",
            "subject_kind",
            "subject_id",
            "unit_of_work",
            "step",
            "outcome",
            "idempotency_key",
            "actor",
            "writer",
        ] {
            let mut fields = good();
            match column {
                "event_id" => fields.event_id.clear(),
                "kind" => fields.kind.clear(),
                "subject_kind" => fields.subject_kind.clear(),
                "subject_id" => fields.subject_id.clear(),
                "unit_of_work" => fields.unit_of_work.clear(),
                "step" => fields.step.clear(),
                "outcome" => fields.outcome.clear(),
                "idempotency_key" => fields.idempotency_key.clear(),
                "actor" => fields.actor.clear(),
                "writer" => fields.writer.clear(),
                _ => unreachable!(),
            }
            assert_eq!(
                check(&fields),
                Err(EnvelopeDefect::Empty(column)),
                "{column} 空着的时候没被拒"
            );
        }
    }

    /// `attributes` 必须是对象：一段包着 JSON 的字符串会让一半读者解析两遍。
    #[test]
    fn attributes_that_are_not_an_object_are_refused() {
        let mut fields = good();
        fields.attributes = "\"{\\\"a\\\":1}\"".into();
        assert_eq!(check(&fields), Err(EnvelopeDefect::AttributesNotAnObject));
        fields.attributes = "not json".into();
        assert_eq!(check(&fields), Err(EnvelopeDefect::AttributesNotAnObject));
    }

    /// 一个装两条路径的 scope 不是一个 scope。
    #[test]
    fn a_scope_holding_two_paths_is_refused() {
        let mut fields = good();
        fields.scope = "crates/a,crates/b".into();
        assert_eq!(check(&fields), Err(EnvelopeDefect::Empty("scope")));
        fields.scope = "crates/a crates/b".into();
        assert_eq!(check(&fields), Err(EnvelopeDefect::Empty("scope")));
        fields.scope = "crates/a".into();
        assert_eq!(check(&fields), Ok(()));
    }
}

#[cfg(test)]
mod kind_closure_tests {
    use super::*;

    /// 闭集是唯一的一处：写进去的每一种 kind 都得在 `KINDS` 里。
    #[test]
    fn a_kind_outside_the_closed_set_is_refused() {
        let mut fields = EnvelopeFields {
            event_id: "01H".into(),
            kind: "llm_call".into(),
            subject_kind: "llm_call".into(),
            subject_id: "c1".into(),
            unit_of_work: "u1".into(),
            step: "call".into(),
            outcome: "ok".into(),
            idempotency_key: "c1".into(),
            actor: "a".into(),
            writer: "w".into(),
            scope: "".into(),
            attributes: "{}".into(),
        };
        assert_eq!(check(&fields), Ok(()), "闭集里的 kind 应当通过");
        fields.kind = "something_new".into();
        assert_eq!(
            check(&fields),
            Err(EnvelopeDefect::UnknownKind("something_new".into())),
            "集外的 kind 必须被拒，否则「有哪些 kind」又要靠谁碰巧写过一段"
        );
    }

    /// 每种 kind 只出现一次，且都是写下划线小写：两个拼写就是两族事件。
    #[test]
    fn every_kind_is_spelt_once() {
        let mut seen = KINDS.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), KINDS.len(), "闭集里有拼写重复的 kind");
        for kind in KINDS {
            assert!(
                kind.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{kind} 的拼写与其余几族不一致"
            );
        }
    }
}
