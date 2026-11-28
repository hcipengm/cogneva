//! 事实只有四样：员工、任务、消息、事件。事件是唯一的正本，它的载荷是一个枚举。
//!
//! （载荷里有哪些字段由 `event_payload` 的目录回答；这个文件回答「载荷有哪几种形状」。）
//!
//! 意图、待办定义、员工编制、消息、轮次、判词、调用、变更、擦除——这些不是各自独立
//! 的「事实种类」，而是同一条流上的不同载荷。此前每一种都带一个 trait、一个信封构造
//! 器、一个服务注册：同一件事写了七八遍，改一处漏七处。这里把载荷收成一个枚举，信封
//! 由 [`EventPayload::envelope`] 一处拼，落库只有一个方法 [`EventStore::append`]。
//!
//! 变体**不合并**：一次任务的 `defined`/`leased`/`transitioned`/`terminal`/`reclaimed`
//! 各是一个变体，各带自己那几个字段。把它们并成「整份 Task 快照」一个变体，读的人就
//! 分不出「这一条改了什么」，而「改了什么」正是流存在的理由。
//!
//! 读面（名册、任务、会话、台账）是投影：按载荷的字段查索引重建，不另存副本。

use chrono::{DateTime, Utc};

use crate::{
    ChangeCandidate, EnvelopeFields, IntentRow, LlmCall, RoundOpened, Task, ToolCallEvent,
};

/// 这条事件是什么。
///
/// 变体名就是 `kind` 列上的那个词；加一种事实就是加一个变体，闭集由编译器与
/// [`crate::KINDS`] 的测试一起守。
#[derive(Debug, Clone, PartialEq)]
pub enum EventPayload {
    /// 工号发出去。
    EmployeeNumberAllocated {
        agent_id: String,
        person: String,
        number: u32,
    },
    /// 一个人离场（工号不回收）。
    EmployeeRetired { agent_id: String },
    /// 一条消息被发出。
    ///
    /// 正文可以外置：`artifact` 在时 `text` 为空，字节在 `artifact://<ns>/<id>`
    /// 指向的那一份里；擦除销毁那一份并追加 `redacted`，这条事件与它的哈希不动。
    MessageSent {
        from: String,
        to: String,
        kind: String,
        text: String,
        session: Option<String>,
        causation: Option<String>,
        /// 正文的存放处（`artifact://<namespace>/<id>`）；`None` 表示正文内联在 `text`。
        artifact: Option<String>,
    },
    /// 一句意图到达。
    IntentRecorded(IntentRow),
    /// 这一句已经有人接手（或判成「只是话」）。
    IntentTriaged {
        identity: String,
        task_id: String,
        actor: String,
    },
    /// 一版任务定义。
    TaskDefined {
        task: Box<Task>,
        rev: i64,
        workspace_id: String,
    },
    /// 一次任务状态转移。
    TaskTransition {
        task_id: String,
        /// 从哪个状态来；第一次转移没有「来处」。
        from: Option<String>,
        to: String,
        actor: String,
        cause: String,
        /// 这一条在任务自己的转移序列里排第几。
        seq: i64,
    },
    /// 一条任务的租约发出去／换到另一个员工手上。
    ///
    /// 续租（同一个 owner、只是到期时刻往后挪）不写这一条：租约的到期时刻是读数，
    /// 「谁接下了这条活」才是事实。
    TaskLeased {
        task_id: String,
        owner: String,
        expires_at: DateTime<Utc>,
        actor: String,
    },
    /// 一个员工注册上来（进程报出自己的身份与所在）。
    ///
    /// 心跳不进流：它是一条**读数**（这个进程此刻还活着），每几十秒一次会让事件流
    /// 的写入速率变成心跳速率。名册的存活由注册表的 TTL 回答，不由事件条数回答。
    EmployeeRegistered {
        agent_id: String,
        role: String,
        workspace_id: String,
        hostname: String,
    },
    /// 一轮开工：这一批要做的那几条。
    RoundOpened(RoundOpened),
    /// 一轮收工。
    ///
    /// `status` 是闭集里的一个词（今天只有 `closed`）；`elapsed_ms` 从开工那条事件
    /// 算起。`iterations` 不在这里——没有量得到的来源，不编。
    RoundClosed {
        round: String,
        actor: String,
        status: String,
        elapsed_ms: Option<u64>,
    },
    /// 一条变更候选被生成。
    ChangeCandidate {
        candidate: ChangeCandidate,
        actor: String,
        scope: String,
    },
    /// 候选被量了一次（状态推进）。
    ChangeMeasured {
        artifact_id: String,
        status: String,
        note: String,
        actor: String,
        scope: String,
    },
    /// 候选落地。
    ChangeAccepted {
        artifact_id: String,
        reason: String,
        actor: String,
        scope: String,
    },
    /// 候选被拒。
    ChangeRejected {
        artifact_id: String,
        reason: String,
        actor: String,
        scope: String,
    },
    /// 一次 LLM 调用。
    LlmCall(LlmCall),
    /// 一次工具调用。
    ToolCall(ToolCallEvent),
    /// 一条原则被判成什么。
    ///
    /// `state`/`platform_gate` 是 `None` 时读作「只有人能判」：表里这两格清空，
    /// 流里留下一次同形的判决事件——不是「没有判过」。
    PrincipleJudged {
        slug: String,
        state: Option<String>,
        platform_gate: Option<String>,
        measured_rev: Option<String>,
    },
    /// 一条原则被声明（谁、载体在哪一版、门在哪）。
    PrincipleDeclared(crate::PrincipleReading),
    /// 一把签名钥匙被签发。
    KeyIssued {
        key_id: String,
        purpose: String,
        public_key_b64: String,
        fingerprint: String,
        actor: String,
    },
    /// 一把钥匙被轮换：新钥匙接替旧的，旧的还留一段重叠期验旧包。
    KeyRotated {
        key_id: String,
        previous_key_id: String,
        public_key_b64: String,
        fingerprint: String,
        overlap_until: Option<String>,
        actor: String,
    },
    /// 一把钥匙被吊销：此后它签的包拒载。
    KeyRevoked {
        key_id: String,
        reason: String,
        actor: String,
    },
    /// 一个调用点选择不调用（省下一次花销）。
    LlmSkip {
        stage: String,
        reason: String,
        at_millis: i64,
    },
    /// 一个窗口的花销判词：某个调用点在这一窗里该不该花。
    SpendVerdict {
        window_start: String,
        window_end: String,
        actor: String,
        verdict: String,
        skip_reason: String,
        skips: i64,
        calls: i64,
        tokens_input: i64,
        tokens_output: i64,
    },
    /// 一次变更测量：这条变更对着树量出来是什么状态。
    ChangeJudged {
        kind: String,
        scope: String,
        literal: String,
        state: String,
        measured_rev: Option<String>,
        retire_reason: Option<String>,
        unsettled_reason: Option<String>,
        parent_observation: Option<String>,
    },
    /// 一次 reading：一行变更账的 title／assertion 从哪来，以及一条引用
    /// （哪份文档、它当时的判决、要求的档与面）。
    ///
    /// 判决（`change_judged`）与交付（`change_delivered`）已经在流上，但「它当时说的是
    /// 什么」只在表里——台账要能整行从流上重建，这一条得先落。
    ChangeDeclared {
        kind: String,
        scope: String,
        literal: String,
        title: Option<String>,
        assertion: Option<String>,
        doc: String,
        verdict: Option<String>,
        priority: String,
        block: String,
        first_seen_rev: Option<String>,
    },
    /// 一次落地走到哪：这次变更落在哪个分支、落到哪个 rev、此刻在生命周期的哪一步、
    /// 为什么退。落地账的文件从此只是它的缓存。
    ChangeLanded {
        change_id: String,
        base: String,
        landed_rev: String,
        state: String,
        retired_reason: Option<String>,
        /// 整条落地记录的 JSON（`LandingRecord`）。读面用它重建在飞记录，
        /// 盘上那份文件因此不再是通道的记忆。老行没有这一格，读作 `None`。
        record: Option<String>,
        /// 这条记录已经离开通道（CI 收口、撤回或退场）：折叠时跳过它。
        /// 老行没有这一格，读作 `false`。
        closed: bool,
    },
    /// 一条待办的判据（终态）被换过，带着理由。判据只在下一次计划时定下，
    /// 换它是一件要留痕的事：后面每一圈都对着**同一段话**说，所以那一段话
    /// 换了没有、为什么换，必须从流上读得出来。
    TerminalRevised {
        item_id: String,
        previous: String,
        terminal: String,
        reason: String,
        actor: String,
    },
    /// 一次工作区测试计划怎么决定：哪些格重跑、哪些复用、由多少条改动路径算来。
    ///
    /// 构建计划是树上的事，但它决定「这一次跑了什么、省了什么」——没有这一条，
    /// 「复用与真跑的条数」只活在钩子的 stderr 里，下一次读不到。
    BuildPlanned {
        /// 决定重跑的格（包名）。
        rerun: Vec<String>,
        /// 复用的格数。
        reused: usize,
        /// 这次改动涉及的路径数。
        changed: usize,
        /// 谁算的：`cargo-hook` 或 `check-before-push`。
        source: String,
    },
    /// 一次存储压力的提出：哪几条读数触发了、哪几条量不到，以及那一刻各是多少。
    ///
    /// 判词只在有人问它时才存在（`cogneva storage-pressure`）。这一条把「该换存储了」
    /// 的**提出**留在流上：没有它，一次退出码 2 只在调用者的终端里存在过。
    StoragePressureRaised {
        /// 触发的条件名，逗号分隔。
        triggered: String,
        /// 量不到的条件名，逗号分隔。
        unread: String,
        /// 这一刻的读数（JSON：四个判据格各一格）。
        readings: String,
        /// 提出时刻（毫秒）：一次提出一条事实，不被下一次同判词去重掉。
        at_millis: u64,
    },
    /// 对一次「该换存储了」的提出的处理：决定继续用（`keep`）还是前移写入口（`front`），
    /// 为什么，谁做的。提出与决定是两条事实：一次提出可以被不同的决定回答。
    StoragePressureDecided {
        /// `keep`（继续用）或 `front`（前移写入口）。
        decision: String,
        /// 为什么这么定；一句话，读的人据此复核。
        reason: String,
        /// 做出决定的人/进程。
        actor: String,
        /// 决定时刻（毫秒）。
        at_millis: u64,
    },
    /// 一条变更的 diff 交付到了某个 rev。
    ChangeDelivered {
        kind: String,
        scope: String,
        literal: String,
        rev: String,
    },
    /// 一段正文被擦除。
    Redacted {
        /// 大对象身份；外置到对象/raw 存储的正文没有它，改用 `artifact` 点名。
        oid: Option<u32>,
        /// 被销毁的那份正文的存放处（`artifact://…`）；与大对象二选一。
        artifact: Option<String>,
        sha256: String,
        bytes: u64,
        reason: String,
        actor: String,
    },
}

/// 一类事件回答的是哪一类问题。
///
/// 领域事实说「世界里发生了什么」（任务、变更、消息、钥匙……）；遥测说「这一次
/// 调用花了什么」（模型调用、工具调用）。两者进同一条流，但读的人问的不是同一个
/// 问题：折叠当前值的时候遥测不参与，按花费读的时候领域事实不参与。
///
/// 分类写在这里并且**穷尽匹配**：新加一个变体而不给它分类，编译就红——这比在
/// 每一处读面各自按名字认「哪些是遥测」可靠，后者会随着新变体静默走散。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventClass {
    Domain,
    Telemetry,
}

impl EventPayload {
    pub fn class(&self) -> EventClass {
        match self {
            Self::LlmCall(..) | Self::ToolCall(..) => EventClass::Telemetry,
            Self::EmployeeNumberAllocated { .. }
            | Self::EmployeeRetired { .. }
            | Self::MessageSent { .. }
            | Self::IntentRecorded(..)
            | Self::IntentTriaged { .. }
            | Self::TaskDefined { .. }
            | Self::TaskTransition { .. }
            | Self::TaskLeased { .. }
            | Self::EmployeeRegistered { .. }
            | Self::RoundOpened { .. }
            | Self::RoundClosed { .. }
            | Self::ChangeCandidate { .. }
            | Self::ChangeMeasured { .. }
            | Self::ChangeAccepted { .. }
            | Self::ChangeRejected { .. }
            | Self::PrincipleJudged { .. }
            | Self::PrincipleDeclared { .. }
            | Self::KeyIssued { .. }
            | Self::KeyRotated { .. }
            | Self::KeyRevoked { .. }
            | Self::LlmSkip { .. }
            | Self::SpendVerdict { .. }
            | Self::ChangeJudged { .. }
            | Self::ChangeDeclared { .. }
            | Self::ChangeLanded { .. }
            | Self::TerminalRevised { .. }
            | Self::ChangeDelivered { .. }
            | Self::BuildPlanned { .. }
            | Self::StoragePressureRaised { .. }
            | Self::StoragePressureDecided { .. }
            | Self::Redacted { .. } => EventClass::Domain,
        }
    }
}

/// 已知 kind 的类别；闭集之外的 kind 答 `None`（老构建写下的，不猜）。
///
/// 这一格是从闭集推出来的，不是另一张名单：遥测只有两条调用，其余每一个在闭集里
/// 的 kind 都是领域事实。新加一个 kind 而忘了把它算进 `KINDS`，`kinds()` 的覆盖门
/// 会先红；忘了想它属于哪一类，读面上它会落进 `unknown` 且门会点名。
pub fn class_of_kind(kind: &str) -> Option<EventClass> {
    if kind == "llm_call" || kind == "tool_call" {
        return Some(EventClass::Telemetry);
    }
    crate::contract::event_envelope::KINDS
        .contains(&kind)
        .then_some(EventClass::Domain)
}

/// 信封里除 `event_id`/`writer` 之外的那些列。
///
/// 一个具名结构而不是九元组：构造器里 `actor` 与 `cause` 写反过一次，位置参数不会报错。
struct Parts {
    subject_kind: String,
    subject_id: String,
    unit_of_work: String,
    step: String,
    outcome: String,
    idempotency_key: String,
    actor: String,
    scope: String,
    attributes: String,
}

impl EventPayload {
    /// `kind` 列上的词。
    pub fn kind(&self) -> &'static str {
        match self {
            EventPayload::EmployeeNumberAllocated { .. } => "employee_number_allocated",
            EventPayload::EmployeeRetired { .. } => "employee_retired",
            EventPayload::MessageSent { .. } => "message_sent",
            EventPayload::BuildPlanned { .. } => "build_planned",
            EventPayload::IntentRecorded(_) => "intent_received",
            EventPayload::IntentTriaged { .. } => "intent_triaged",
            EventPayload::TaskDefined { .. } => "task_defined",
            EventPayload::TaskTransition { .. } => "task_transition",
            // 聚合词：`step` 说这一步是什么。
            EventPayload::TaskLeased { .. } => "task",
            EventPayload::EmployeeRegistered { .. } => "employee",
            EventPayload::RoundOpened(_) => "round_opened",
            EventPayload::RoundClosed { .. } => "round_closed",
            EventPayload::ChangeCandidate { .. } => "change_candidate",
            EventPayload::ChangeMeasured { .. } => "change_measured",
            EventPayload::ChangeAccepted { .. } => "change_accepted",
            EventPayload::ChangeRejected { .. } => "change_rejected",
            EventPayload::LlmCall(_) => "llm_call",
            EventPayload::ToolCall(_) => "tool_call",
            EventPayload::PrincipleJudged { .. } => "principle_judged",
            EventPayload::PrincipleDeclared(_) => "principle_declared",

            EventPayload::Redacted { .. } => "redacted",
            EventPayload::KeyIssued { .. }
            | EventPayload::KeyRotated { .. }
            | EventPayload::KeyRevoked { .. } => "key",
            EventPayload::LlmSkip { .. } => "llm_skip",
            EventPayload::SpendVerdict { .. } => "spend_verdict",
            EventPayload::ChangeJudged { .. } => "change_judged",
            EventPayload::ChangeDelivered { .. } => "change_delivered",
            EventPayload::TerminalRevised { .. } => "task",
            EventPayload::ChangeDeclared { .. } => "change_declared",
            EventPayload::ChangeLanded { .. } => "change_landed",
            EventPayload::StoragePressureRaised { .. }
            | EventPayload::StoragePressureDecided { .. } => "storage_pressure",
        }
    }

    /// 幂等键：同一件事重放写同一条。
    pub fn idempotency_key(&self) -> String {
        self.fields().idempotency_key
    }

    /// `step` 列上的词：同一族里说这一步是什么。
    pub fn step(&self) -> String {
        self.fields().step
    }

    /// 因果根：这条事实由哪一条事件直接触发。
    pub fn causation(&self) -> Option<String> {
        match self {
            EventPayload::MessageSent { causation, .. } => causation.clone(),
            EventPayload::ToolCall(call) => call.causation.clone(),
            _ => None,
        }
    }

    /// 这一段属于哪一段会话；`None` 是这条入口没有会话。
    pub fn session(&self) -> Option<String> {
        match self {
            EventPayload::MessageSent { session, .. } => session.clone(),
            EventPayload::IntentRecorded(row) => row.session.clone(),
            EventPayload::ToolCall(call) => call.session.clone(),
            EventPayload::LlmCall(call) => call.session.clone(),
            _ => None,
        }
    }

    /// 批号：这条事实属于哪一批；`None` 不属于任何一批。
    pub fn batch(&self) -> Option<String> {
        match self {
            EventPayload::LlmCall(call) => call.batch_id.clone(),
            _ => None,
        }
    }

    /// 这条事实发生在哪一刻（载荷自己带的时刻）；`None` 由写入口取钟。
    pub fn at(&self) -> Option<DateTime<Utc>> {
        match self {
            EventPayload::IntentRecorded(row) => Some(row.at),
            _ => None,
        }
    }

    /// 这一条的信封。`event_id` 由写入口发，载荷只负责「这条事实说什么」。
    ///
    /// 因果与批号也在这里：载荷知道它挂在哪条事件上、属于哪一批，写口不再收第二组
    /// 参数——一条事件的每一列只从一个地方来。
    pub fn envelope(&self, event_id: &str, writer: &str) -> EnvelopeFields {
        let parts = self.fields();
        EnvelopeFields {
            event_id: event_id.to_string(),
            kind: self.kind().to_string(),
            subject_kind: parts.subject_kind,
            subject_id: parts.subject_id,
            unit_of_work: parts.unit_of_work,
            step: parts.step,
            outcome: parts.outcome,
            idempotency_key: parts.idempotency_key,
            causation_id: self.causation(),
            batch_id: self.batch(),
            session_id: self.session(),
            attempt: 1,
            payload_version: 1,
            actor: parts.actor,
            writer: writer.to_string(),
            scope: parts.scope,
            attributes: parts.attributes,
        }
    }

    fn fields(&self) -> Parts {
        match self {
            EventPayload::EmployeeNumberAllocated {
                agent_id,
                person,
                number,
            } => Parts {
                subject_kind: "employee".into(),
                subject_id: agent_id.clone(),
                unit_of_work: agent_id.clone(),
                step: "allocated".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{agent_id}:number:{number}"),
                actor: agent_id.clone(),
                scope: "employee".into(),
                attributes: serde_json::json!({ "number": number, "person": person }).to_string(),
            },
            EventPayload::EmployeeRetired { agent_id } => Parts {
                subject_kind: "employee".into(),
                subject_id: agent_id.clone(),
                unit_of_work: agent_id.clone(),
                step: "retired".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{agent_id}:retired:{}", Utc::now().timestamp_millis()),
                actor: agent_id.clone(),
                scope: "employee".into(),
                attributes: serde_json::json!({ "at": Utc::now().to_rfc3339() }).to_string(),
            },
            EventPayload::MessageSent {
                from,
                to,
                kind,
                text,
                session,
                causation,
                artifact,
            } => Parts {
                subject_kind: "message".into(),
                subject_id: if from.is_empty() {
                    to.clone()
                } else {
                    from.clone()
                },
                unit_of_work: to.clone(),
                step: "sent".into(),
                outcome: "ok".into(),
                idempotency_key: crate::contract::message_events::message_idempotency(
                    from,
                    to,
                    kind,
                    // 正文外置时 text 为空：拿存放处的引用算幂等，否则两条不同的
                    // 外置消息会撞在同一个空串上。
                    if text.is_empty() {
                        artifact.as_deref().unwrap_or("")
                    } else {
                        text
                    },
                    session.as_deref(),
                    causation.as_deref(),
                    Utc::now(),
                ),
                actor: if from.is_empty() {
                    "unknown".into()
                } else {
                    from.clone()
                },
                scope: "message".into(),
                attributes: serde_json::json!({
                    "from": from,
                    "to": to,
                    "kind": kind,
                    "text": text,
                    "session": session,
                    "artifact": artifact,
                    "at": Utc::now().to_rfc3339(),
                })
                .to_string(),
            },
            EventPayload::IntentRecorded(row) => Parts {
                subject_kind: "intent".into(),
                subject_id: row.identity.clone(),
                unit_of_work: row.identity.clone(),
                step: "received".into(),
                outcome: "ok".into(),
                idempotency_key: row.idempotency_key(),
                actor: if row.said_by.is_empty() {
                    "unknown".into()
                } else {
                    row.said_by.clone()
                },
                scope: "intent".into(),
                attributes: row.attributes().to_string(),
            },
            EventPayload::IntentTriaged {
                identity,
                task_id,
                actor,
            } => Parts {
                subject_kind: "intent".into(),
                subject_id: identity.clone(),
                unit_of_work: identity.clone(),
                step: "triaged".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{identity}:triaged"),
                actor: if actor.is_empty() {
                    "unknown".into()
                } else {
                    actor.clone()
                },
                scope: "intent".into(),
                attributes: serde_json::json!({ "task_id": task_id }).to_string(),
            },
            EventPayload::TaskDefined {
                task,
                rev,
                workspace_id,
            } => Parts {
                subject_kind: "task".into(),
                subject_id: task.id.clone(),
                unit_of_work: task.id.clone(),
                step: "defined".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{}:defined:{rev}", task.id),
                actor: task
                    .submitted_by
                    .clone()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| "storage".into()),
                scope: "task".into(),
                attributes:
                    serde_json::json!({ "rev": rev, "workspace_id": workspace_id, "task": task })
                        .to_string(),
            },
            EventPayload::TaskTransition {
                task_id,
                from,
                to,
                actor,
                cause,
                seq,
            } => Parts {
                subject_kind: "task".into(),
                subject_id: task_id.clone(),
                unit_of_work: task_id.clone(),
                step: to.clone(),
                outcome: "ok".into(),
                idempotency_key: format!("{task_id}:{seq}"),
                actor: actor.clone(),
                scope: "task".into(),
                attributes: serde_json::json!({
                    "from": from,
                    "to": to,
                    "actor": actor,
                    "cause": cause,
                })
                .to_string(),
            },
            EventPayload::TaskLeased {
                task_id,
                owner,
                expires_at,
                actor,
            } => Parts {
                subject_kind: "task".into(),
                subject_id: task_id.clone(),
                unit_of_work: task_id.clone(),
                step: "leased".into(),
                outcome: "ok".into(),
                idempotency_key: format!(
                    "{task_id}:leased:{owner}:{}",
                    expires_at.timestamp_millis()
                ),
                actor: actor.clone(),
                scope: "task".into(),
                attributes: serde_json::json!({
                    "owner": owner,
                    "expires_at": expires_at.to_rfc3339(),
                })
                .to_string(),
            },
            EventPayload::EmployeeRegistered {
                agent_id,
                role,
                workspace_id,
                hostname,
            } => Parts {
                subject_kind: "employee".into(),
                subject_id: agent_id.clone(),
                unit_of_work: agent_id.clone(),
                step: "registered".into(),
                outcome: "ok".into(),
                // 重投同一个注册是同一件事：身份＋岗位＋所在。
                idempotency_key: format!("{agent_id}:registered:{workspace_id}:{hostname}"),
                actor: agent_id.clone(),
                scope: "employee".into(),
                attributes: serde_json::json!({
                    "role": role,
                    "workspace_id": workspace_id,
                    "hostname": hostname,
                })
                .to_string(),
            },
            EventPayload::RoundOpened(round) => Parts {
                subject_kind: "round".into(),
                subject_id: round.round.clone(),
                unit_of_work: round.round.clone(),
                step: "open".into(),
                outcome: "ok".into(),
                idempotency_key: round.idempotency_key(),
                actor: round.actor.clone(),
                scope: "round".into(),
                attributes: round.attributes().to_string(),
            },
            EventPayload::RoundClosed {
                round,
                actor,
                status,
                elapsed_ms,
            } => Parts {
                subject_kind: "round".into(),
                subject_id: round.clone(),
                unit_of_work: round.clone(),
                step: "close".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{round}:closed"),
                actor: actor.clone(),
                scope: "round".into(),
                attributes: serde_json::json!({
                    "status": status,
                    "elapsed_ms": elapsed_ms,
                })
                .to_string(),
            },
            EventPayload::ChangeCandidate {
                candidate,
                actor,
                scope,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: candidate.artifact_id.clone(),
                unit_of_work: candidate.artifact_id.clone(),
                step: "generated".into(),
                outcome: "ok".into(),
                idempotency_key: format!("change_candidate:{}", candidate.artifact_id),
                actor: actor.clone(),
                scope: scope.clone(),
                attributes: crate::contract::reflection::candidate_attributes(candidate)
                    .to_string(),
            },
            EventPayload::ChangeMeasured {
                artifact_id,
                status,
                note,
                actor,
                scope,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: artifact_id.clone(),
                unit_of_work: artifact_id.clone(),
                step: "verified".into(),
                outcome: "ok".into(),
                idempotency_key: format!("change_measured:{artifact_id}:{status}"),
                actor: actor.clone(),
                scope: scope.clone(),
                attributes: serde_json::json!({ "status": status, "note": note }).to_string(),
            },
            EventPayload::ChangeAccepted {
                artifact_id,
                reason,
                actor,
                scope,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: artifact_id.clone(),
                unit_of_work: artifact_id.clone(),
                step: "landed".into(),
                outcome: "ok".into(),
                idempotency_key: format!("change_accepted:{artifact_id}"),
                actor: actor.clone(),
                scope: scope.clone(),
                attributes: serde_json::json!({ "reason": reason }).to_string(),
            },
            EventPayload::ChangeRejected {
                artifact_id,
                reason,
                actor,
                scope,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: artifact_id.clone(),
                unit_of_work: artifact_id.clone(),
                step: "evaluated".into(),
                outcome: "failed".into(),
                idempotency_key: format!("change_rejected:{artifact_id}"),
                actor: actor.clone(),
                scope: scope.clone(),
                attributes: serde_json::json!({ "reason": reason }).to_string(),
            },
            EventPayload::LlmCall(call) => Parts {
                subject_kind: "llm_call".into(),
                subject_id: call.call_id.clone(),
                unit_of_work: call.call_id.clone(),
                step: "call".into(),
                outcome: "ok".into(),
                idempotency_key: call.idempotency_key(),
                actor: call.actor.clone(),
                scope: call.model.clone(),
                attributes: call.attributes().to_string(),
            },
            EventPayload::ToolCall(call) => Parts {
                subject_kind: "tool_call".into(),
                subject_id: call.call_id.clone(),
                unit_of_work: call.unit_of_work.clone(),
                step: "call".into(),
                outcome: call.outcome.clone(),
                idempotency_key: call.idempotency_key(),
                actor: call.actor.clone(),
                scope: call.tool.clone(),
                attributes: call.attributes().to_string(),
            },
            EventPayload::PrincipleJudged {
                slug,
                state,
                platform_gate,
                measured_rev,
            } => Parts {
                subject_kind: "principle".into(),
                subject_id: slug.clone(),
                unit_of_work: slug.clone(),
                step: "measured".into(),
                outcome: "ok".into(),
                idempotency_key: format!(
                    "{slug}:judged:{}:{}",
                    state.as_deref().unwrap_or("human-only"),
                    measured_rev.as_deref().unwrap_or("-")
                ),
                actor: "principle-ledger".into(),
                scope: "principle".into(),
                attributes: serde_json::json!({
                    "state": state,
                    "platform_gate": platform_gate,
                    "measured_rev": measured_rev,
                })
                .to_string(),
            },
            EventPayload::PrincipleDeclared(row) => Parts {
                subject_kind: "principle".into(),
                subject_id: row.slug.clone(),
                unit_of_work: row.slug.clone(),
                step: "declared".into(),
                outcome: "ok".into(),
                // 声明内容任何一格变了都是另一条事实：幂等键取整行摘要，
                // 不能只取载体哈希（门路径或优先级变了、载体没变也会撞）。
                idempotency_key: {
                    use sha2::{Digest, Sha256};
                    let digest =
                        Sha256::digest(serde_json::to_string(row).unwrap_or_default().as_bytes());
                    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
                    format!("{}:declared:{}", row.slug, &hex[..26])
                },
                actor: "principle-ledger".into(),
                scope: "principle".into(),
                attributes: serde_json::to_value(row)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|_| "{}".into()),
            },
            EventPayload::KeyIssued {
                key_id,
                purpose,
                public_key_b64,
                fingerprint,
                actor,
            } => Parts {
                subject_kind: "key".into(),
                subject_id: key_id.clone(),
                unit_of_work: key_id.clone(),
                step: "issued".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{key_id}:issued:{fingerprint}"),
                actor: actor.clone(),
                scope: "key".into(),
                attributes: serde_json::json!({
                    "key_id": key_id,
                    "purpose": purpose,
                    "public_key": public_key_b64,
                    "fingerprint": fingerprint,
                })
                .to_string(),
            },
            EventPayload::KeyRotated {
                key_id,
                previous_key_id,
                public_key_b64,
                fingerprint,
                overlap_until,
                actor,
            } => Parts {
                subject_kind: "key".into(),
                subject_id: key_id.clone(),
                unit_of_work: key_id.clone(),
                step: "rotated".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{key_id}:rotated:{fingerprint}"),
                actor: actor.clone(),
                scope: "key".into(),
                attributes: serde_json::json!({
                    "key_id": key_id,
                    "previous_key_id": previous_key_id,
                    "public_key": public_key_b64,
                    "fingerprint": fingerprint,
                    "overlap_until": overlap_until,
                })
                .to_string(),
            },
            EventPayload::KeyRevoked {
                key_id,
                reason,
                actor,
            } => Parts {
                subject_kind: "key".into(),
                subject_id: key_id.clone(),
                unit_of_work: key_id.clone(),
                step: "revoked".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{key_id}:revoked"),
                actor: actor.clone(),
                scope: "key".into(),
                attributes: serde_json::json!({ "reason": reason }).to_string(),
            },

            EventPayload::LlmSkip {
                stage,
                reason,
                at_millis,
            } => Parts {
                subject_kind: "call_site".into(),
                subject_id: stage.clone(),
                unit_of_work: stage.clone(),
                step: "skipped".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{stage}:skip:{at_millis}:{reason}"),
                actor: stage.clone(),
                scope: "usage".into(),
                attributes: serde_json::json!({ "stage": stage, "reason": reason }).to_string(),
            },
            EventPayload::SpendVerdict {
                window_start,
                window_end,
                actor,
                verdict,
                skip_reason,
                skips,
                calls,
                tokens_input,
                tokens_output,
            } => Parts {
                subject_kind: "call_site".into(),
                subject_id: actor.clone(),
                unit_of_work: actor.clone(),
                step: "judged".into(),
                outcome: "ok".into(),
                idempotency_key: format!(
                    "{window_start}:{window_end}:{actor}:{verdict}:{skip_reason}"
                ),
                actor: actor.clone(),
                scope: "usage".into(),
                attributes: serde_json::json!({
                    "window_start": window_start,
                    "window_end": window_end,
                    "verdict": verdict,
                    "skip_reason": skip_reason,
                    "skips": skips,
                    "calls": calls,
                    "tokens_input": tokens_input,
                    "tokens_output": tokens_output,
                })
                .to_string(),
            },

            EventPayload::ChangeJudged {
                kind,
                scope,
                literal,
                state,
                measured_rev,
                retire_reason,
                unsettled_reason,
                parent_observation,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: format!("{kind}|{scope}|{literal}"),
                unit_of_work: format!("{kind}|{scope}|{literal}"),
                step: "judged".into(),
                outcome: "ok".into(),
                idempotency_key: {
                    use sha2::{Digest, Sha256};
                    let digest = Sha256::digest(
                        serde_json::json!({
                            "kind": kind,
                            "scope": scope,
                            "literal": literal,
                            "state": state,
                            "measured_rev": measured_rev,
                            "retire_reason": retire_reason,
                            "unsettled_reason": unsettled_reason,
                            "parent_observation": parent_observation,
                        })
                        .to_string()
                        .as_bytes(),
                    );
                    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
                    format!("{kind}|{scope}|{literal}:judged:{}", &hex[..26])
                },
                actor: "change-ledger".into(),
                scope: "change".into(),
                attributes: serde_json::json!({
                    "kind": kind,
                    "scope": scope,
                    "literal": literal,
                    "state": state,
                    "measured_rev": measured_rev,
                    "retire_reason": retire_reason,
                    "unsettled_reason": unsettled_reason,
                    "parent_observation": parent_observation,
                })
                .to_string(),
            },

            EventPayload::StoragePressureRaised {
                triggered,
                unread,
                readings,
                at_millis,
            } => Parts {
                subject_kind: "storage_pressure".into(),
                subject_id: "pressure".into(),
                unit_of_work: "storage-pressure".into(),
                step: "raised".into(),
                outcome: "triggered".into(),
                idempotency_key: format!("storage_pressure:{at_millis}"),
                actor: "cogneva".into(),
                scope: "storage".into(),
                attributes: serde_json::json!({
                    "triggered": triggered,
                    "unread": unread,
                    "readings": readings,
                    "at_millis": at_millis,
                })
                .to_string(),
            },
            EventPayload::StoragePressureDecided {
                decision,
                reason,
                actor,
                at_millis,
            } => Parts {
                subject_kind: "storage_pressure".into(),
                subject_id: "pressure".into(),
                unit_of_work: "storage-pressure".into(),
                step: "decided".into(),
                outcome: "ok".into(),
                idempotency_key: format!("storage_pressure:decided:{at_millis}"),
                actor: actor.clone(),
                scope: "storage".into(),
                attributes: serde_json::json!({
                    "decision": decision,
                    "reason": reason,
                    "at_millis": at_millis,
                })
                .to_string(),
            },
            EventPayload::BuildPlanned {
                rerun,
                reused,
                changed,
                source,
            } => Parts {
                subject_kind: "build".into(),
                subject_id: format!("{source}"),
                // 一次计划挂在产出它的那一趟上：`source` 是钩子还是推送前的门。
                unit_of_work: format!("build-plan:{source}"),
                step: "planned".into(),
                outcome: "ok".into(),
                idempotency_key: format!("build_planned:{source}:{}:{}", rerun.join(","), changed),
                actor: "cargo-hook".into(),
                scope: "build".into(),
                attributes: serde_json::json!({
                    "rerun": rerun,
                    "reused": reused,
                    "changed": changed,
                    "source": source,
                })
                .to_string(),
            },
            EventPayload::ChangeDelivered {
                kind,
                scope,
                literal,
                rev,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: format!("{kind}|{scope}|{literal}"),
                unit_of_work: format!("{kind}|{scope}|{literal}"),
                step: "delivered".into(),
                outcome: "ok".into(),
                idempotency_key: format!("{kind}|{scope}|{literal}:delivered:{rev}"),
                actor: "change-ledger".into(),
                scope: "change".into(),
                attributes: serde_json::json!({
                    "kind": kind,
                    "scope": scope,
                    "literal": literal,
                    "rev": rev,
                })
                .to_string(),
            },
            EventPayload::ChangeLanded {
                change_id,
                base,
                landed_rev,
                state,
                retired_reason,
                record,
                closed,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: change_id.clone(),
                unit_of_work: change_id.clone(),
                step: "landed".into(),
                outcome: "ok".into(),
                idempotency_key: format!("change_landed:{change_id}:{state}:{landed_rev}:{closed}"),
                actor: "cog-github".into(),
                scope: "landing".into(),
                attributes: serde_json::json!({
                    "change_id": change_id,
                    "base": base,
                    "landed_rev": landed_rev,
                    "state": state,
                    "retired_reason": retired_reason,
                    "record": record,
                    "closed": closed,
                })
                .to_string(),
            },
            EventPayload::ChangeDeclared {
                kind,
                scope,
                literal,
                title,
                assertion,
                doc,
                verdict,
                priority,
                block,
                first_seen_rev,
            } => Parts {
                subject_kind: "change".into(),
                subject_id: format!("{kind}|{scope}|{literal}"),
                unit_of_work: format!("{kind}|{scope}|{literal}"),
                step: "declared".into(),
                outcome: "ok".into(),
                // 同一份内容重放写同一条；文档改了说法就是另一条（重建取最新）。
                idempotency_key: {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    for part in [
                        kind.as_str(),
                        scope.as_str(),
                        literal.as_str(),
                        doc.as_str(),
                        title.as_deref().unwrap_or(""),
                        assertion.as_deref().unwrap_or(""),
                        verdict.as_deref().unwrap_or(""),
                        priority.as_str(),
                        block.as_str(),
                    ] {
                        hasher.update(part.as_bytes());
                        hasher.update([0u8]);
                    }
                    format!(
                        "{kind}|{scope}|{literal}:declared:{doc}:{:x}",
                        hasher.finalize()
                    )
                },
                actor: "change-ledger".into(),
                scope: "change".into(),
                attributes: serde_json::json!({
                    "kind": kind,
                    "scope": scope,
                    "literal": literal,
                    "title": title,
                    "assertion": assertion,
                    "doc": doc,
                    "verdict": verdict,
                    "priority": priority,
                    "block": block,
                    "first_seen_rev": first_seen_rev,
                })
                .to_string(),
            },
            EventPayload::TerminalRevised {
                item_id,
                previous,
                terminal,
                reason,
                actor,
            } => Parts {
                subject_kind: "item".into(),
                subject_id: item_id.clone(),
                unit_of_work: item_id.clone(),
                step: "terminal_revised".into(),
                outcome: "ok".into(),
                // 同一份新终态＋同一条理由重放写同一条；换了一段话就是另一条。
                idempotency_key: {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(terminal.as_bytes());
                    hasher.update([0u8]);
                    hasher.update(reason.as_bytes());
                    format!("terminal_revised:{item_id}:{:x}", hasher.finalize())
                },
                actor: actor.clone(),
                scope: "task".into(),
                attributes: serde_json::json!({
                    "item_id": item_id,
                    "previous": previous,
                    "terminal": terminal,
                    "reason": reason,
                    "actor": actor,
                })
                .to_string(),
            },
            EventPayload::Redacted {
                oid,
                artifact,
                sha256,
                bytes,
                reason,
                actor,
            } => {
                // 身份二选一：大对象用 oid，外置正文用存放处的引用。两者都缺时
                // 只留哈希——「擦掉的是哪一段」仍然读得出来。
                let body_id = oid
                    .map(|oid| format!("body:{oid}"))
                    .or_else(|| artifact.clone())
                    .unwrap_or_else(|| format!("body:sha256:{sha256}"));
                Parts {
                    subject_kind: "body".into(),
                    subject_id: body_id.clone(),
                    unit_of_work: body_id.clone(),
                    step: "redact".into(),
                    outcome: "ok".into(),
                    idempotency_key: format!("redact:{body_id}:{sha256}"),
                    actor: actor.clone(),
                    scope: "body".into(),
                    attributes: serde_json::json!({
                        "body_oid": oid,
                        "artifact": artifact,
                        "sha256": sha256,
                        "bytes": bytes,
                        "reason": reason,
                    })
                    .to_string(),
                }
            }
        }
    }
}

/// 一条要写下的事实：载荷、身份、哪一刻。
#[derive(Debug, Clone)]
pub struct FactEvent {
    pub payload: EventPayload,
    /// 写入口发的事件 id；没有就由写入口现发一个。
    pub event_id: Option<String>,
    /// 这一条发生/入库的时刻；`None` 取写入口的钟。
    pub at: Option<DateTime<Utc>>,
}

impl FactEvent {
    pub fn new(payload: EventPayload) -> Self {
        Self {
            payload,
            event_id: None,
            at: None,
        }
    }
}

/// 落地事实的读口：每条变更最后一条 `change_landed` 的属性 JSON（按 seq 升序）。
///
/// 写口只追加（[`EventStore`]），读口是另一件事：落地通道要的是「每条变更此刻
/// 最后一条记录」。把它做成 trait，是为了让 cog-github 不必依赖存储层——由存储
/// 插件发布、装配处接给落地账。
#[async_trait::async_trait]
pub trait LandingFactReader: Send + Sync {
    async fn latest_landing_attributes(&self) -> crate::SFResult<Vec<String>>;
}

/// 唯一的写口：一条流、一个方法。
#[async_trait::async_trait]
pub trait EventStore: Send + Sync {
    fn writer(&self) -> &str;
    fn next_event_id(&self) -> String;
    async fn append(&self, event: FactEvent) -> crate::SFResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 每种载荷的信封都过得了检查：kind 在闭集里、必填格不空。
    ///
    /// 同时是**双向覆盖**的样本身份表：这里少一种载荷、或 `KINDS` 里多一个没有载荷
    /// 的词，两边的集合对不上就红。
    pub(crate) fn samples() -> Vec<EventPayload> {
        let at = DateTime::from_timestamp(100, 0).unwrap();
        vec![
            EventPayload::EmployeeNumberAllocated {
                agent_id: "employee:7".into(),
                person: "employee:7".into(),
                number: 7,
            },
            EventPayload::EmployeeRetired {
                agent_id: "employee:7".into(),
            },
            EventPayload::MessageSent {
                artifact: None,
                from: "employee:7".into(),
                to: "alice".into(),
                kind: "answer".into(),
                text: "改好了".into(),
                session: Some("s-1".into()),
                causation: Some("e-1".into()),
            },
            EventPayload::IntentRecorded(IntentRow::new(
                "chat",
                "id-1",
                "alice",
                "employee:7",
                "改掉",
                None,
            )),
            EventPayload::IntentTriaged {
                identity: "id-1".into(),
                task_id: String::new(),
                actor: "conversation:no-work".into(),
            },
            EventPayload::TaskDefined {
                task: Box::new(Task::new(
                    "t-1",
                    crate::TaskType::DagNode,
                    serde_json::json!({}),
                )),
                rev: 1,
                workspace_id: "ws-1".into(),
            },
            EventPayload::TaskTransition {
                task_id: "t-1".into(),
                from: Some("pending".into()),
                to: "running".into(),
                actor: "worker".into(),
                cause: "leased".into(),
                seq: 1,
            },
            EventPayload::TaskLeased {
                task_id: "t-1".into(),
                owner: "employee:7".into(),
                expires_at: at,
                actor: "store".into(),
            },
            EventPayload::EmployeeRegistered {
                agent_id: "employee:7".into(),
                role: "generator".into(),
                workspace_id: "ws-1".into(),
                hostname: "pod-a".into(),
            },
            EventPayload::RoundOpened(RoundOpened {
                round: "r-1".into(),
                ledger_rev: "rev-1".into(),
                code_rev: "rev-1".into(),
                batch: vec!["L1".into()],
                actor: "owner".into(),
                at,
            }),
            EventPayload::RoundClosed {
                round: "r-1".into(),
                actor: "owner".into(),
                status: "closed".into(),
                elapsed_ms: Some(1_000),
            },
            EventPayload::ChangeCandidate {
                candidate: ChangeCandidate {
                    artifact_id: "c-1".into(),
                    kind: "code_change".into(),
                    description: "改一门".into(),
                    content: "diff".into(),
                    created_at: at,
                    recorded_status: "awaiting_review".into(),
                    eval_summary: None,
                    tiering: None,
                    unit_of_work: None,
                    status: "candidate".into(),
                    decision_reason: None,
                    event_id: "e-c1".into(),
                },
                actor: "reflection".into(),
                scope: "reflection".into(),
            },
            EventPayload::ChangeMeasured {
                artifact_id: "c-1".into(),
                status: "compiled".into(),
                note: String::new(),
                actor: "reflection".into(),
                scope: "reflection".into(),
            },
            EventPayload::ChangeAccepted {
                artifact_id: "c-1".into(),
                reason: "landed".into(),
                actor: "reflection".into(),
                scope: "reflection".into(),
            },
            EventPayload::ChangeRejected {
                artifact_id: "c-1".into(),
                reason: "tests red".into(),
                actor: "reflection".into(),
                scope: "reflection".into(),
            },
            EventPayload::LlmCall(LlmCall {
                call_id: "call-1".into(),
                upstream: "pool-a".into(),
                api_style: "anthropic".into(),
                model: "claude-sonnet-5-5".into(),
                result: "ok".into(),
                input_tokens: 10,
                output_tokens: 20,
                cached_tokens: 0,
                cache_created_tokens: None,
                origin: "interactive".into(),
                owner: Some("alice".into()),
                latency_ms: Some(120),
                task_id: None,
                actor: "alice".into(),
                cost_micros: None,
                price_book: None,
                batch_id: Some("b-1".into()),
                session: None,
                pace: None,
                tokens_reasoning: None,
                refusal_kind: None,
                retry_after_secs: None,
                usage_outcome: None,
                estimated_input: None,
                refused_scope: None,
                finish_reason: None,
                ttft_ms: None,
                prompt_manifest: None,
                prompt_dropped: None,
                at,
            }),
            EventPayload::ToolCall(ToolCallEvent {
                call_id: "tool-1".into(),
                unit_of_work: "t-1".into(),
                tool: "read_file".into(),
                tool_description: None,
                schema_sha256: None,
                arguments_sha256: "abc".into(),
                arguments_bytes: 3,
                result_sha256: Some("def".into()),
                result_bytes: 3,
                outcome: "ok".into(),
                latency_ms: Some(3),
                path: Some("src/lib.rs".into()),
                artifact: None,
                session: None,
                causation: Some("e-1".into()),
                actor: "worker".into(),
                at,
            }),
            EventPayload::PrincipleJudged {
                slug: "no-hardcoded-knobs".into(),
                state: Some("enforced".into()),
                platform_gate: Some("tunable-registry".into()),
                measured_rev: Some("rev-1".into()),
            },
            EventPayload::PrincipleDeclared(crate::PrincipleReading {
                slug: "no-hardcoded-knobs".into(),
                source: "list-principles.sh".into(),
                lead: Some("数值不硬编码".into()),
                carrier_path: Some("memory/principles/x.md".into()),
                carrier_sha256: Some("abc".into()),
                host_gate: Some("tunable_registry_contract_test".into()),
                where_red: Some("crates/cogneva/tests/x.rs".into()),
                check_kind: Some(crate::PrincipleCheckKind::Tree),
                priority: Some(crate::ChangePriority::P1),
            }),
            EventPayload::LlmSkip {
                stage: "planner".into(),
                reason: "no new work".into(),
                at_millis: 1,
            },
            EventPayload::SpendVerdict {
                window_start: "2026-11-25T00:00:00Z".into(),
                window_end: "2026-11-25T01:00:00Z".into(),
                actor: "agent:generator".into(),
                verdict: "documented_skip".into(),
                skip_reason: "no new work".into(),
                skips: 1,
                calls: 0,
                tokens_input: 0,
                tokens_output: 0,
            },
            EventPayload::ChangeJudged {
                kind: "source".into(),
                scope: "crates/x".into(),
                literal: "a rule".into(),
                state: "landed".into(),
                measured_rev: Some("rev-1".into()),
                retire_reason: None,
                unsettled_reason: None,
                parent_observation: None,
            },
            EventPayload::BuildPlanned {
                rerun: vec!["cog-core".into()],
                reused: 3,
                changed: 1,
                source: "cargo-hook".into(),
            },
            EventPayload::ChangeDelivered {
                kind: "source".into(),
                scope: "crates/x".into(),
                literal: "a rule".into(),
                rev: "rev-1".into(),
            },
            EventPayload::StoragePressureDecided {
                decision: "keep".into(),
                reason: "the host still has years of headroom".into(),
                actor: "owner".into(),
                at_millis: 2,
            },
            EventPayload::StoragePressureRaised {
                triggered: "growth".into(),
                unread: "stream_lag".into(),
                readings: "{\"growth_headroom_days\":10.0}".into(),
                at_millis: 1,
            },
            EventPayload::ChangeLanded {
                change_id: "c-1".into(),
                base: "main".into(),
                landed_rev: "rev-1".into(),
                state: "pushed".into(),
                retired_reason: None,
                record: Some("{}".into()),
                closed: false,
            },
            EventPayload::ChangeDeclared {
                kind: "source".into(),
                scope: "crates/x".into(),
                literal: "a rule".into(),
                title: Some("一条规则".into()),
                assertion: Some("函数 f 不再返回 None".into()),
                doc: "specs/a.md".into(),
                verdict: Some("must".into()),
                priority: "P1".into(),
                block: "none".into(),
                first_seen_rev: Some("rev-1".into()),
            },
            EventPayload::KeyIssued {
                key_id: "k-1".into(),
                purpose: "principle-bundle".into(),
                public_key_b64: "cHVibGlj".into(),
                fingerprint: "fp-1".into(),
                actor: "owner".into(),
            },
            EventPayload::TerminalRevised {
                item_id: "S19".into(),
                previous: "the old sentence".into(),
                terminal: "the new sentence".into(),
                reason: "the old one judged the wrong surface".into(),
                actor: "owner".into(),
            },
            EventPayload::Redacted {
                artifact: None,
                oid: Some(7),
                sha256: "abc".into(),
                bytes: 10,
                reason: "owner asked".into(),
                actor: "owner".into(),
            },
        ]
    }

    /// 每种载荷的信封都过得了检查：kind 在闭集里、必填格不空。
    #[test]
    fn every_payload_builds_a_complete_envelope() {
        for payload in samples() {
            let fields = payload.envelope("e-1", "probe");
            assert!(
                crate::contract::event_envelope::check(&fields).is_ok(),
                "{:?} 的信封过不了检查",
                payload.kind()
            );
        }
    }

    /// 会话在信封上，不在载荷里：有会话的那几种载荷把会话写进 `session_id`，
    /// 没有会话的一线事实留 `None`——两者在列上分得开。
    #[test]
    fn a_payload_that_has_a_session_puts_it_on_the_envelope() {
        for payload in samples() {
            let fields = payload.envelope("e-1", "probe");
            let expected = payload.session();
            assert_eq!(
                fields.session_id,
                expected,
                "{} 的会话没有进信封",
                payload.kind()
            );
        }
        assert!(samples()
            .iter()
            .any(|payload| payload.session().as_deref() == Some("s-1")));
        assert!(samples()
            .iter()
            .filter(|payload| payload.kind() == "task_defined")
            .all(|payload| payload.session().is_none()));
    }

    /// 有目录的载荷，写出去的键必须与目录逐字一致。
    #[test]
    fn a_catalogued_payload_writes_exactly_the_catalogued_keys() {
        for payload in samples() {
            let Some(fields) = crate::payload_fields_for(payload.kind(), &payload.step(), 1) else {
                continue;
            };
            let catalogued: std::collections::BTreeSet<String> = fields
                .iter()
                .map(|(field, _)| (*field).to_string())
                .collect();
            let envelope = payload.envelope("e-1", "probe");
            let written: std::collections::BTreeSet<String> =
                serde_json::from_str::<serde_json::Value>(&envelope.attributes)
                    .expect("attributes are an object")
                    .as_object()
                    .expect("attributes are an object")
                    .keys()
                    .cloned()
                    .collect();
            assert_eq!(
                written,
                catalogued,
                "{} 写出去的键与目录不一致",
                payload.kind()
            );
        }
    }

    /// 目录覆盖闭集：每个 kind 要么有目录（至少一步），要么在豁免表里写明为什么。
    #[test]
    fn every_kind_is_either_catalogued_or_exempt_with_a_reason() {
        const EXEMPT: &[(&str, &str)] = &[(
            "task_transition",
            "step 是目标状态（running/completed/…），不是固定的一步，目录按动态 step 列不出来",
        )];
        let catalogued: std::collections::BTreeSet<&str> = crate::PAYLOAD_CATALOG
            .iter()
            .map(|entry| entry.kind)
            .collect();
        let mut missing = Vec::new();
        for kind in crate::contract::event_envelope::KINDS {
            if !catalogued.contains(kind) && !EXEMPT.iter().any(|(exempt, _)| *exempt == kind) {
                missing.push(kind);
            }
        }
        assert!(
            missing.is_empty(),
            "这些 kind 既没有目录也不在豁免表里：{missing:?}"
        );
        for (kind, why) in EXEMPT {
            assert!(crate::is_known_kind(kind), "豁免表点名了闭集外的 {kind}");
            assert!(!why.trim().is_empty(), "{kind} 的豁免没写理由");
        }
    }

    /// 双向覆盖：闭集里每个词都有一个载荷，每个载荷的词都在闭集里。
    ///
    /// 单向不够——闭集多一个没人写的词，读面按它去查永远查不到；载荷多一个不在闭集
    /// 的词，写入口当场拒。两边各数一遍，少的那个当场红。
    #[test]
    fn the_closed_set_and_the_payload_enum_cover_each_other() {
        let words: std::collections::BTreeSet<&str> =
            samples().iter().map(|payload| payload.kind()).collect();
        let closed: std::collections::BTreeSet<&str> =
            crate::contract::event_envelope::KINDS.into_iter().collect();
        assert_eq!(
            words,
            closed,
            "载荷枚举与闭集不是同一组词：只在载荷里 {:?}，只在闭集里 {:?}",
            words.difference(&closed).collect::<Vec<_>>(),
            closed.difference(&words).collect::<Vec<_>>()
        );
        // 一个聚合词下面可以有几个变体（`task` 的 leased/reclaimed），但它们
        // 的 `(kind, step)` 必须各不相同：同一步两种载荷，读的人就分不出这一条
        // 是哪一种。
        let steps: std::collections::BTreeSet<(String, String)> = samples()
            .iter()
            .map(|payload| (payload.kind().to_string(), payload.step()))
            .collect();
        assert_eq!(
            steps.len(),
            samples().len(),
            "同一个 (kind, step) 被两种载荷各写了一遍"
        );
    }
}
