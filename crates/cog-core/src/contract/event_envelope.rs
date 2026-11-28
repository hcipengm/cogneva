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

/// 一条事件写进流之前的形状：迁移 117/119/129 建出的**每一列**都在这里。
///
/// 此前 `causation_id`/`batch_id` 走 `append_caused` 的旁路参数、`attempt`/
/// `payload_version` 走库默认——于是「这一条的信封是什么」要看两处代码加一条 DDL
/// 才答得全。现在一处：构造器填，写口只落。
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
    /// 这条事实由哪一条事件直接触发；`None` 是一线（没有上游因果）。
    pub causation_id: Option<String>,
    /// 同一批的批号；`None` 不属于任何一批。
    pub batch_id: Option<String>,
    /// 这一条属于哪一段会话；`None` 是「这条入口没有会话」（外部情报、A2A 提交）。
    ///
    /// 会话此前只在载荷里；按会话读一段对话要走表达式、没有列索引。它是信封的
    /// 一列之后，读侧按列分组、按索引取。
    pub session_id: Option<String>,
    /// 同一件事的第几次尝试，从 1 起。
    pub attempt: i32,
    /// 载荷的版本（与 `event_payload` 的目录对账）。
    pub payload_version: i32,
    pub actor: String,
    pub writer: String,
    pub scope: String,
    pub attributes: String,
}

impl Default for EnvelopeFields {
    /// 只给「其余格子」的默认值：必填格仍是空串，过不了 [`check`]。
    /// 两格计数默认 1：第一次尝试、第一版载荷。
    fn default() -> Self {
        Self {
            event_id: String::new(),
            kind: String::new(),
            subject_kind: String::new(),
            subject_id: String::new(),
            unit_of_work: String::new(),
            step: String::new(),
            outcome: String::new(),
            idempotency_key: String::new(),
            causation_id: None,
            batch_id: None,
            session_id: None,
            // cap: fixed — 「从 1 起」是这个计数的定义，不是一台机器的容量
            // not-a-bound: 「从 1 起」是这个计数的定义，不是一台机器的容量
            attempt: 1,
            // cap: fixed — 目录里的第一版；版本号由 `event_payload` 的目录给定
            // not-a-bound: 目录里的第一版；版本号由 `event_payload` 的目录给定
            payload_version: 1,
            actor: String::new(),
            writer: String::new(),
            scope: String::new(),
            attributes: String::new(),
        }
    }
}

/// 事件流里**全部**的 kind，一处列出。
///
/// 每个写入口只知道自己那一种，于是「全集」从来没有出现过——普查问「有哪些 kind」
/// 时，答案取决于谁碰巧写了一段。这一格把全集收在一处：新 kind 要么加进来，要么
/// 写入口当场被拒（[`check`] 会拒 `Unknown`）。
/// 名单按写入方分：轮次（`round_*`）、变更候选四个阶段
/// （`change_*`）、两类调用事实（`llm_call`／`tool_call`）、意图的到达与接手
/// （`intent_*`）。
pub const KINDS: [&str; 29] = [
    "round_opened",
    "round_closed",
    "change_candidate",
    "change_measured",
    "change_accepted",
    "change_rejected",
    // 一次 reading：一行台账的 title／assertion 与一条引用从哪来。判决与交付已在流上，
    // 但「它当时说的是什么」只在表里——台账要能整行从流上重建，这一条得先落。
    "change_declared",
    // 落地账的一步：一次落地走到哪、CI 怎么看、为什么退。文件从此只是缓存。
    "change_landed",
    "llm_call",
    "tool_call",
    // 任务的每一次状态转移也是事实：它今天只在 `task_transition` 表里有一份，
    // 于是「任务停在哪一步、从哪接着跑」要读那张表——而流才是正本。
    "task_transition",
    // 一条任务被定义（或改了一版）：任务行是这一族事实的投影。转移说的是它怎么动，
    // 定义说的是它是什么——两样都在流上，删掉 `task_defined` 表能从流里重建。
    "task_defined",
    // 一条原则被判成什么（enforced / unenforced / 只有人能判）。原则账是这条事实
    // 的投影：删掉 `principle_ledger` 表，从流里读得出每条原则最后一次被判成什么。
    "principle_judged",
    // 擦除：一段正文被销毁了，而事件流里留下「谁在什么时候、因为什么、销毁了哪一段」。
    // 它**不改写**被擦那条事件：历史照旧，指向正文的哈希还在，只是那个哈希现在指不到
    // 可读的字节——这正是「擦除」与「改历史」的区别。
    "redacted",
    // 意图到达：一句进入流水线的话，原文与来源。与之配对的是它什么时候被哪一条待办
    // 接手，记在第二条事件上而不是改写这一条——到达是事实，接手是后来发生的事。
    "intent_received",
    "intent_triaged",
    // 员工的编制：工号发出去的那一刻、以及一个人离场的那一刻。名册是这两条事实的
    // 投影，不是自己的正本；「谁什么时候来的、谁什么时候走的」从流里读得出来。
    "employee_number_allocated",
    "employee_retired",
    // 一条消息被发出：谁发的、发给谁、正文、渠道。会话是它的投影；队列与走线不是
    // 事实——投递失败、重投、没人读，都是读面的事，不改写这一条。
    "message_sent",
    // 两个**聚合词**：此后新写的任务事实与员工事实用它们，`step` 说这一步是什么
    // （`leased`／`reclaimed`、`registered`……）。旧词（`task_defined`、
    // `task_transition`、`employee_number_allocated`……）不改写历史，读侧经
    // [`crate::contract::event_kinds`] 的族表一起读回来。
    "task",
    "employee",
    // 一条原则被声明：slug、载体路径与 sha256、门路径、检查方式、优先级。声明也是
    // 事实——`principle_ledger_declared` 表从它重建，而不是只靠一次推进入口。
    "principle_declared",
    // 密钥生命周期的三个动作：签发、轮换、吊销。聚合词 `key`，`step` 说是哪一个；
    // 公钥与指纹进载荷，私钥永远不进事件。
    "key",
    // 一次「省下」与一次「窗口判词」也是事实：它们回答「这段为什么没花钱」，
    // 而花销、结局、拒绝三面都答不出「这里本会花、却没有」。
    "llm_skip",
    "spend_verdict",
    // 一次「对着树量了这条变更」：量出来是什么状态、在哪个 rev 上量的。账本行是
    // 这条事实的投影；没有它，账本的每一格都只能从表里读，表坏了就没有第二份。
    "change_judged",
    // 一次交付：一条变更的 diff 到了一个主线 rev。它是「到了线上」这一格的事实，
    // 与「对着树量成 landed」是两件事。
    "change_delivered",
    // 一次工作区测试计划的决定：哪些格重跑、哪些复用、由多少条改动路径算来。
    // 「省了什么」从此有事实可读，不再只活在钩子的 stderr 里。
    "build_planned",
    // 一次存储压力的提出：四条触发条件里哪几条为真、哪几条量不到，以及这一刻的读数。
    // 判词只在有人问它时才存在；这一条把「该换存储」的那一次提出留在流上。
    "storage_pressure",
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
    /// 计数格从 1 起：0 读起来像「没有尝试过」，而这条事件已经发生了。
    NotPositive(&'static str),
}

impl EnvelopeDefect {
    pub fn as_str(&self) -> &'static str {
        match self {
            EnvelopeDefect::Empty(column) => column,
            EnvelopeDefect::UnknownKind(_) => "kind",
            EnvelopeDefect::AttributesNotAnObject => "attributes",
            EnvelopeDefect::NotPositive(column) => column,
        }
    }
}

/// 写入口的判据：这些格子空着就不许写。
///
/// 名单是**能力**反过来写的：`subject_id` 缺了读者按身份取不到这条事件；`writer` 缺了
/// 幂等约束失去一半；`idempotency_key` 缺了重放会写第二条；`kind`/`outcome`/`step` 缺了
/// 续跑与统计都答不了。可空的两格（`causation_id`/`batch_id`）不进名单；`at` 由写入口
/// 取钟；两个计数格（`attempt`/`payload_version`）要从 1 起。
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
    if fields.attempt < 1 {
        return Err(EnvelopeDefect::NotPositive("attempt"));
    }
    if fields.payload_version < 1 {
        return Err(EnvelopeDefect::NotPositive("payload_version"));
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
            causation_id: None,
            batch_id: None,
            session_id: None,
            // cap: fixed — 「从 1 起」是这个计数的定义，不是一台机器的容量
            // not-a-bound: 「从 1 起」是这个计数的定义，不是一台机器的容量
            attempt: 1,
            // cap: fixed — 目录里的第一版；版本号由 `event_payload` 的目录给定
            // not-a-bound: 目录里的第一版；版本号由 `event_payload` 的目录给定
            payload_version: 1,
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

    /// 计数格从 1 起：0 读起来像「没有尝试过」，而这条事件已经发生了。
    #[test]
    fn a_counter_below_one_is_refused() {
        let mut fields = good();
        fields.attempt = 0;
        assert_eq!(check(&fields), Err(EnvelopeDefect::NotPositive("attempt")));
        fields = good();
        fields.payload_version = 0;
        assert_eq!(
            check(&fields),
            Err(EnvelopeDefect::NotPositive("payload_version"))
        );
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
            causation_id: None,
            batch_id: None,
            session_id: None,
            // cap: fixed — 「从 1 起」是这个计数的定义，不是一台机器的容量
            // not-a-bound: 「从 1 起」是这个计数的定义，不是一台机器的容量
            attempt: 1,
            // cap: fixed — 目录里的第一版；版本号由 `event_payload` 的目录给定
            // not-a-bound: 目录里的第一版；版本号由 `event_payload` 的目录给定
            payload_version: 1,
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
