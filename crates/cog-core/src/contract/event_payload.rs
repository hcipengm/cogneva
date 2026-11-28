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
    /// 同一聚合词下说是哪一步（`task`/employee/key 这些词各有多步）。
    pub step: &'static str,
    pub version: i32,
    /// (字段名, 它说的是什么)。
    pub fields: &'static [(&'static str, &'static str)],
}

/// 这一版代码认识的每一版载荷。
pub const PAYLOAD_CATALOG: &[PayloadField] = &[
    PayloadField {
        kind: "change_candidate",
        step: "generated",
        version: 1,
        fields: &[
            ("kind", "产物种类（code_change / skill_refinement …）"),
            ("status", "记下这条候选时它自己的状态词"),
            ("description", "这条变更要做什么"),
            ("content", "diff 正文：`.diff` 只是它的导出"),
            ("created_at", "生成时刻（RFC 3339）"),
            ("eval_summary", "评估门的判词与散文；null 表示没有评估"),
            ("tiering", "路由档位；null 表示没算过"),
            (
                "unit_of_work",
                "发起这条变更的任务 id；null 表示没有目标来源",
            ),
        ],
    },
    PayloadField {
        kind: "change_measured",
        step: "verified",
        version: 1,
        fields: &[
            ("status", "这一步之后它自己的状态词"),
            ("note", "为什么变成这个状态；空串表示只是一次推进"),
        ],
    },
    PayloadField {
        kind: "change_accepted",
        step: "landed",
        version: 1,
        fields: &[("reason", "定案原因（落地在哪、为什么算成）")],
    },
    PayloadField {
        kind: "change_rejected",
        step: "evaluated",
        version: 1,
        fields: &[("reason", "拒绝原因；这一格是「为什么没成」的唯一载体")],
    },
    PayloadField {
        kind: "change_judged",
        step: "judged",
        version: 1,
        fields: &[
            ("kind", "变更身份的第一格"),
            ("scope", "变更身份的第二格"),
            ("literal", "变更身份的第三格"),
            ("state", "对着树量出来的状态词"),
            ("measured_rev", "在哪个 rev 上量的；null 是量的时候没有 rev"),
            ("retire_reason", "退休原因；只有退休行有"),
            ("unsettled_reason", "定不下来的原因；只有未决行有"),
            ("parent_observation", "量到的那一句现场；null 是没有"),
        ],
    },
    PayloadField {
        kind: "change_delivered",
        step: "delivered",
        version: 1,
        fields: &[
            ("kind", "变更身份的第一格"),
            ("scope", "变更身份的第二格"),
            ("literal", "变更身份的第三格"),
            ("rev", "diff 到达的主线 rev"),
        ],
    },
    PayloadField {
        kind: "llm_call",
        step: "call",
        version: 1,
        fields: &[
            ("upstream", "服务这次调用的上游池身份"),
            ("api_style", "线协议族（openai / anthropic）"),
            ("model", "上游配置里那个真模型名"),
            ("result", "平台的结果词（ok、refused_by_budget、error …）"),
            ("input_tokens", "输入 token（不含缓存命中的部分）"),
            ("output_tokens", "输出 token"),
            ("cached_tokens", "命中前缀缓存的输入 token"),
            (
                "cache_created_tokens",
                "写进缓存的那部分 token；null 表示上游没报",
            ),
            ("origin", "调用来源（interactive / gateway_pool …）"),
            ("owner", "这次调用记在谁头上；null 是没有身份"),
            ("latency_ms", "这次调用花了多久；null 是落账时已不在手里"),
            ("task_id", "为哪条任务花的；null 不是任务发起的"),
            ("pace", "配速那一拍的读数（JSON 文本）"),
            ("cost_micros", "这一笔花掉的钱（微单位）；null 是没有定价过"),
            (
                "price_book",
                "算这笔钱用的价目版本；与 cost_micros 同生同灭",
            ),
            ("session", "属于哪一段会话；null 是没有会话"),
            ("batch_id", "属于哪一批；null 是单独一次调用"),
            (
                "tokens_reasoning",
                "输出里有多少是模型想出来的；null 是上游没说",
            ),
            ("refusal_kind", "拒绝的原因分类；只有拒绝行有"),
            ("retry_after_secs", "什么时候可以再来；只有拒绝行有"),
            ("usage_outcome", "计量结局（read/absent/…）"),
            ("estimated_input", "发请求前的输入估算；null 是没有估算"),
            ("refused_scope", "哪一级额度拒的（人/工作区/任务）"),
            (
                "finish_reason",
                "模型侧为什么停了（stop/length/tool_calls…）",
            ),
            ("ttft_ms", "首字时延（毫秒）；null 是没量到"),
            ("prompt_manifest", "提示清单：每段 sha256 的 JSON 数组"),
            ("prompt_dropped", "装配时挤掉了多少条更早的消息"),
        ],
    },
    PayloadField {
        kind: "tool_call",
        step: "call",
        version: 1,
        fields: &[
            ("tool", "工具名"),
            ("tool_description", "模型当时看到的工具说明"),
            ("schema_sha256", "模型当时看到的参数 schema 的 sha256"),
            ("arguments_sha256", "参数字节的 sha256"),
            ("arguments_bytes", "参数字节数"),
            ("result_sha256", "结果字节的 sha256；失败为 null"),
            ("result_bytes", "结果字节数"),
            ("outcome", "结局词（ok / error）"),
            ("latency_ms", "调用耗时；拿不到为 null"),
            ("path", "点名的文件路径；与文件无关为 null"),
            ("artifact", "正文的存放处（artifact://…）"),
            ("session", "属于哪一段会话"),
            ("causation", "触发它的那条意图事件 id"),
        ],
    },
    PayloadField {
        kind: "llm_skip",
        step: "skipped",
        version: 1,
        fields: &[
            ("stage", "选择不调用的那一步（角色名或自评阶段）"),
            ("reason", "为什么选择不调用；调用方给的自由文本，按它分组读"),
        ],
    },
    PayloadField {
        kind: "spend_verdict",
        step: "judged",
        version: 1,
        fields: &[
            ("window_start", "这一窗的起点（RFC 3339）"),
            ("window_end", "这一窗的终点（RFC 3339）"),
            // `actor` 不进 attributes：它走信封自己的 `actor` 列（每一条事实都有）。
            (
                "verdict",
                "necessary / documented_skip / unexpected_absent / unsettled",
            ),
            ("skip_reason", "判词对着的那笔省下；空表示没有"),
            ("skips", "这一窗里省下的次数"),
            ("calls", "这一窗里的调用次数"),
            ("tokens_input", "这一窗的输入 token"),
            ("tokens_output", "这一窗的输出 token"),
        ],
    },
    PayloadField {
        kind: "intent_received",
        step: "received",
        version: 1,
        fields: &[
            ("source", "从哪儿进来的（chat / external_intel / …）"),
            ("said_by", "说这句话的人；空串是没有身份"),
            ("addressed_to", "说给谁听（员工身份）；空串是没有指名"),
            ("text", "原话，一个字节都不改写"),
            ("parent", "由哪一条派生；null 是一线入口"),
            ("session", "属于哪一段会话；null 是没有会话"),
            ("model_text", "模型实际读到的那一段；null 表示与 text 相同"),
        ],
    },
    PayloadField {
        kind: "intent_triaged",
        step: "triaged",
        version: 1,
        fields: &[("task_id", "接手的那条待办；空串是「只是话」")],
    },
    PayloadField {
        kind: "message_sent",
        step: "sent",
        version: 1,
        fields: &[
            ("from", "谁发的"),
            ("to", "发给谁"),
            ("kind", "渠道（answer / failure / attachment …）"),
            ("text", "正文或附件指针"),
            ("session", "属于哪一段会话"),
            ("at", "发出时刻（RFC 3339）"),
            (
                "artifact",
                "正文的存放处（`artifact://<namespace>/<id>`；空表示正文在 text 里）",
            ),
        ],
    },
    PayloadField {
        kind: "task_defined",
        step: "defined",
        version: 1,
        fields: &[
            ("rev", "第几版"),
            ("workspace_id", "属于哪个工作区"),
            ("task", "整条定义"),
        ],
    },
    PayloadField {
        kind: "change_landed",
        step: "landed",
        version: 1,
        fields: &[
            ("change_id", "哪一条变更"),
            ("base", "落在哪个分支"),
            ("landed_rev", "落到的提交（还没落时为空）"),
            ("state", "生命周期的哪一步"),
            ("retired_reason", "为什么退（可为空）"),
            (
                "record",
                "整条落地记录的 JSON（读面据此重建在飞记录；老行为空）",
            ),
            ("closed", "这条记录是否已经离开通道（老行读作 false）"),
        ],
    },
    PayloadField {
        kind: "storage_pressure",
        step: "raised",
        version: 1,
        fields: &[
            ("triggered", "触发的压力条件（逗号分隔）"),
            ("unread", "量不到的条件（逗号分隔）"),
            ("readings", "这一刻四条读数各自的值的 JSON"),
            ("at_millis", "提出的时刻（毫秒）：一次提出一条事实"),
        ],
    },
    PayloadField {
        kind: "storage_pressure",
        step: "decided",
        version: 1,
        fields: &[
            ("decision", "keep（继续用）或 front（前移写入口）"),
            ("reason", "为什么这么定"),
            ("at_millis", "决定时刻（毫秒）"),
        ],
    },
    PayloadField {
        kind: "change_declared",
        step: "declared",
        version: 1,
        fields: &[
            ("kind", "变更身份的 kind 一段"),
            ("scope", "变更身份的 scope 一段"),
            ("literal", "变更身份的 literal 一段"),
            ("title", "这条变更叫什么（可为空）"),
            ("assertion", "它断言的是什么（可为空）"),
            ("doc", "这份 reading 来自哪份文档"),
            ("verdict", "那份文档当时的判决（可为空）"),
            ("priority", "那份文档要求的档"),
            ("block", "那份文档要求的处置面"),
            ("first_seen_rev", "第一次看见它时的 rev（可为空）"),
        ],
    },
    PayloadField {
        kind: "task",
        step: "leased",
        version: 1,
        fields: &[
            ("owner", "接下这条活的人"),
            ("expires_at", "租到什么时候（RFC 3339）"),
        ],
    },
    PayloadField {
        kind: "task",
        step: "terminal_revised",
        version: 1,
        fields: &[
            ("item_id", "待办的标记"),
            ("previous", "换之前那段终态"),
            ("terminal", "换之后的终态：完整描述做完之后世界是什么样"),
            ("reason", "为什么换；空理由不是一次修订"),
            ("actor", "谁换的"),
        ],
    },
    PayloadField {
        kind: "employee_number_allocated",
        step: "allocated",
        version: 1,
        fields: &[("number", "工号"), ("person", "这一行背后的人")],
    },
    PayloadField {
        kind: "employee_retired",
        step: "retired",
        version: 1,
        fields: &[("at", "离场时刻（RFC 3339）")],
    },
    PayloadField {
        kind: "employee",
        step: "registered",
        version: 1,
        fields: &[
            ("role", "岗位"),
            ("workspace_id", "所在工作区"),
            ("hostname", "所在机器"),
        ],
    },
    PayloadField {
        kind: "round_opened",
        step: "open",
        version: 1,
        fields: &[
            ("ledger_rev", "这一批从台账哪一版读出"),
            ("code_rev", "开工那一刻代码树的 rev"),
            ("batch", "这一批要做的那几条"),
        ],
    },
    PayloadField {
        kind: "build_planned",
        step: "planned",
        version: 1,
        fields: &[
            ("rerun", "决定重跑的格（包名）"),
            ("reused", "复用的格数"),
            ("changed", "这次改动涉及的路径数"),
            ("source", "谁算的：cargo-hook 或 check-before-push"),
        ],
    },
    PayloadField {
        kind: "round_closed",
        step: "close",
        version: 1,
        fields: &[
            ("status", "闭集里的状态词（今天只有 closed）"),
            ("elapsed_ms", "从开工那一刻算起的毫秒；读不到为 null"),
        ],
    },
    PayloadField {
        kind: "principle_judged",
        step: "measured",
        version: 1,
        fields: &[
            ("state", "enforced / unenforced；null 是只有人能判"),
            ("platform_gate", "平台门的名字；null 是没有"),
            ("measured_rev", "在哪个 rev 上量的；null 是没有"),
        ],
    },
    PayloadField {
        kind: "principle_declared",
        step: "declared",
        version: 1,
        fields: &[
            ("slug", "原则身份"),
            ("source", "哪一份读数命名了它"),
            ("lead", "规则自己的那一句"),
            ("carrier_path", "载体在 memory 里的路径"),
            ("carrier_sha256", "载体正文的哈希"),
            ("host_gate", "主机侧门名；null 是没有"),
            ("where_red", "门在树里的位置；null 是没有"),
            ("check_kind", "检查方式（shape/tree/live/judgement）"),
            ("priority", "优先级词"),
        ],
    },
    PayloadField {
        kind: "key",
        step: "issued",
        version: 1,
        fields: &[
            ("key_id", "钥匙身份"),
            ("purpose", "用途（signing / encryption）"),
            ("public_key", "公钥（base64）"),
            ("fingerprint", "指纹"),
        ],
    },
    PayloadField {
        kind: "key",
        step: "rotated",
        version: 1,
        fields: &[
            ("key_id", "新钥匙身份"),
            ("previous_key_id", "被接替的旧钥匙"),
            ("public_key", "新公钥（base64）"),
            ("fingerprint", "新指纹"),
            ("overlap_until", "重叠期到什么时候；null 是没有"),
        ],
    },
    PayloadField {
        kind: "key",
        step: "revoked",
        version: 1,
        fields: &[("reason", "为什么吊销")],
    },
    PayloadField {
        kind: "redacted",
        step: "redact",
        version: 1,
        fields: &[
            ("body_oid", "正文的大对象 oid；外置正文为 null"),
            (
                "artifact",
                "外置正文的存放处（artifact://…）；大对象正文为 null",
            ),
            ("sha256", "正文的哈希"),
            ("bytes", "字节数"),
            ("reason", "为什么擦除"),
        ],
    },
];

/// 某一版载荷的字段目录；没有登记就是「这一版代码不认识它」。
pub fn payload_fields(kind: &str, version: i32) -> Option<&'static [(&'static str, &'static str)]> {
    PAYLOAD_CATALOG
        .iter()
        .find(|entry| entry.kind == kind && entry.version == version)
        .map(|entry| entry.fields)
}

/// 某一步的字段目录：聚合词下每一步的形状可以不同。
pub fn payload_fields_for(
    kind: &str,
    step: &str,
    version: i32,
) -> Option<&'static [(&'static str, &'static str)]> {
    PAYLOAD_CATALOG
        .iter()
        .find(|entry| entry.kind == kind && entry.step == step && entry.version == version)
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
                seen.insert((entry.kind, entry.step, entry.version)),
                "{} {} v{} is catalogued twice",
                entry.kind,
                entry.step,
                entry.version
            );
            assert_eq!(
                payload_fields_for(entry.kind, entry.step, entry.version),
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
