//! 每一张会被 `UPDATE` 的表，都要说清自己是哪一种形状。
//!
//! 存储终态的第一条不变式是「每张表只有两种形状」——但它最初只写了两种：追加表
//! （有 `at`，永不 UPDATE）与投影表（有水位、重建命令、重建测试、读者声明）。实测
//! 2026-11-16：94 张表里 73 张代码里从不 UPDATE，20 张原地更新；这 20 张里只有一张是
//! 投影，其余是**权威文档**——行本身就是那段事实的当前值，不是任何东西的派生量。
//! `users` 是这张表最清楚的例子：改一个邮箱不是新事件，是同一个人的新描述；把它算成
//! 「投影」就得说它从哪儿重算，而那个地方不存在。
//!
//! 所以判据第三种形状：**权威文档**，并且要求它把「为什么它是真源而不是派生」写出来。
//! 写不出这句话的表，要么改成追加表（每次变化一行，当前值由读者折），要么是真该重算
//! 的投影（那就补水位、重建命令与重建测试）。
//!
//! 判据在 `crates/cogneva/tests/every_updated_table_declares_its_shape_test.rs`：它从
//! DDL 里算出表，从生产代码里算出被 `UPDATE` 的表，两头比——多一行（表已经不改了）、
//! 少一行（新加了一张原地更新的表）都红。

// 库里的有损面：引用动作与分区摘除的判词（人口由 `scripts/db-loss-census.py` 现算）。
//
// 默认无损：一行数据只有在「不丢会让功能或容量撑不住」时才可以随主体一起消失，理由写在
// 这里，和这条约束同处一棵树。外键的 SET NULL 在「事实」表上一律不收——审计/账单的主体
// 标识必须留存（迁移 140 已去掉这两条外键）；留下来的 CASCADE 都是「从属行」。
// db-loss: change_ledger_sources.change_ledger_sources_kind_scope_literal_fkey — loss — 这些身份行是 change_ledger 的派生投影（重建整表重写），不丢会让账读到不存在的来源、两本账分家（功能坏），重建可恢复
// db-loss: files.files_user_id_fkey — loss — 文件属于人：人删了文件随之走；不丢会留下孤儿字节没有读者能归属（功能坏），删人这件事本身由擦除事实记录
// db-loss: notifications.notifications_user_id_fkey — loss — 通知不是事实，是投递给那个人的消息；人没了；不丢会留下没有收件人的消息（功能坏）
// db-loss: platform_identities.platform_identities_user_id_fkey — loss — 身份绑定属于人；人删了绑定必须一起走；不丢会让同一 id 再被分配时读到旧绑定（功能坏）
// db-loss: user_auth_methods.user_auth_methods_user_id_fkey — loss — 凭证属于人；人删了凭证必须一起走；不丢会留下没有主体的凭证（功能坏）
// db-loss: workspace_members.workspace_members_user_id_fkey — loss — 成员行是「谁在哪个工作区」的关系；不丢会留下任一端不存在的成员行（功能坏）
// db-loss: workspace_members.workspace_members_workspace_id_fkey — loss — 同上：不丢会留下任一端不存在的成员行（功能坏）
// db-loss: audit_logs — keep — 维护器只摘空分区（recut_if_empty：同一事务里先数再摘）；非空的保留路径是 DELETE 清扫，由树里有损普查判归档
// db-loss: billing_records — keep — 同上：摘的只有空分区
// db-loss: explainability_part — keep — 同上：摘的只有空分区
// db-loss: raw_log_index — keep — 同上；它是原始日志上的索引，原始行在别处
/// 事实表：一行就是那段事实的**唯一一份**，删掉就再也拿不回来。
///
/// 这张名单的用途只有一个：让「事实面不许有到点删」成为一条机器判得出的判据
/// （`crates/cogneva/tests/no_fact_table_is_deleted_on_a_schedule_test.rs`）。名单本身
/// 不是证据——判据还会去看那条 `DELETE` 所在的函数里有没有「归档写进去、读回来、
/// 逐字节比过」这一步。名单漏了一张表，判据就在那张表上失效，所以加一张事实表时
/// 要回到这里加一行。
///
/// 两类表不在这里：**投影**（可重建，删了能算回来，判据是水位与重建测试）与
/// **权威文档**（行本身就是当前值，改它就是那条事实本身，见 [`MUTABLE_TABLES`]）。
pub const FACT_TABLES: &[&str] = &[
    "cogneva_event",
    "cog_events",
    "cog_observability_events",
    "cog_observability_logs",
    // 记忆条目的两份归档：删之前先写一份读得回来的副本，副本本身只追加。
    // `summary_entries`／`schema_entries` 本身会被 UPDATE（`first_seen`/`last_seen`/
    // `observed_by`），它们是**文档**，不是这里的追加事实——它们的类目在 `TABLE_CENSUS`
    // 里写的是 Legacy（正本已迁到事件流），退休前仍按文档更新。
    "summary_entries_archive",
    "schema_entries_archive",
];

/// 一张会被 `UPDATE` 的表，以及它为什么是这种形状。
#[derive(Debug, Clone, Copy)]
pub enum TableShape {
    /// 从别处重算出来的表：水位列、重建命令、读者声明。四样缺一不可。
    Projection {
        /// 读它的地方，写成树里的一个路径。
        reader: &'static str,
        /// 「我读到哪了」的那一列；`None` 表示整表重算（没有增量，也就没有水位）。
        watermark: Option<&'static str>,
        /// 从真源重建它的地方，写成树里的一个路径。
        rebuild: &'static str,
        /// 它由**哪些事实**重算出来。与 `projection_readers` 里同一张表的那一格
        /// 说的是同一句话，判据逐字比对——一份表两份声明各说各的，是这一族最老的病。
        rebuilt_from: &'static str,
        /// 那条重算测试在树里的路径。声明「能重算」而拿不出一条重算测试，
        /// 在库面上与「不能重算」同形——测试就是这句话的证据。
        rebuild_test: &'static str,
    },
    /// 权威文档：行就是那段事实的当前值。
    Document {
        /// 为什么它是真源、而不是从别处重算出来的量。
        why: &'static str,
    },
}

/// 一张表的声明。
#[derive(Debug, Clone, Copy)]
pub struct DeclaredTable {
    pub table: &'static str,
    pub shape: TableShape,
}

/// 生产代码里会被 `UPDATE` 的每一张表。
///
/// 「会被 UPDATE」不是从注释里读的，是判据从生产代码的 SQL 字面量里量出来的
/// （`#[cfg(test)]` 之后的部分不算：那里的 `UPDATE` 是夹具）。
pub const MUTABLE_TABLES: &[DeclaredTable] = &[
    DeclaredTable {
        table: "task_current",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/task_ledger.rs",
            watermark: Some("as_of_seq"),
            rebuild: "crates/cog-storage/src/postgres/task_ledger.rs",
            rebuilt_from: "task 台账里只追加的转移行（每一条任务的完整历史）",
            rebuild_test: "crates/cog-storage/tests/task_projection_rebuilds_from_the_ledger.rs",
        },
    },
    DeclaredTable {
        table: "cog_metric_counter_totals",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
            watermark: Some("as_of"),
            rebuild: "crates/cog-observability/src/metric_projection.rs",
            rebuilt_from: "Prometheus 里 `<name>_total` 那条序列",
            rebuild_test: "crates/cogneva/tests/metric_projections_rebuild_from_prometheus.rs",
        },
    },
    DeclaredTable {
        table: "cog_metric_histogram_buckets",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
            watermark: Some("as_of"),
            rebuild: "crates/cog-observability/src/metric_projection.rs",
            rebuilt_from: "Prometheus 里 `<name>_bucket` 那条序列的每一档",
            rebuild_test: "crates/cogneva/tests/metric_projections_rebuild_from_prometheus.rs",
        },
    },
    DeclaredTable {
        table: "cog_metric_histogram_sums",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
            watermark: Some("as_of"),
            rebuild: "crates/cog-observability/src/metric_projection.rs",
            rebuilt_from: "Prometheus 里 `<name>_sum` 那条序列",
            rebuild_test: "crates/cogneva/tests/metric_projections_rebuild_from_prometheus.rs",
        },
    },
    DeclaredTable {
        table: "alert_assertions",
        shape: TableShape::Document {
            why: "一条断言是「这次评估里这一条规则说了什么」：同一个断言键被下一轮评估覆盖，而不是每轮追加一行——读者问的是「现在这条规则怎么说」。",
        },
    },
    DeclaredTable {
        table: "change_ledger_sources",
        shape: TableShape::Document {
            why: "一份变更的引用按 `doc` 唯一：同一份文档改口时更新那一行，而不是再记一条；引用集就是这一张表的当前内容。",
        },
    },
    DeclaredTable {
        table: "cog_checkpoints",
        shape: TableShape::Document {
            why: "一个检查点是「这里能恢复」的当前值：同一个 id 再写一次就覆盖，恢复要读的是最新那一份。",
        },
    },
    DeclaredTable {
        table: "cog_context_boards",
        shape: TableShape::Document {
            why: "一块上下文板是这一轮装配出来的当前内容：再装配一次就覆盖掉同一块板。\
                  它不是历史——读者要的是「这一轮喂给模型的是什么」，而上一轮的板子\
                  已经被下一轮取代，留下来只会让两个版本都被当成当前值。",
        },
    },
    DeclaredTable {
        table: "cog_dag_state",
        shape: TableShape::Document {
            why: "一个 DAG 的当前状态：`state` 是它的当前值，而它怎么走到这里的在事件里。\
                  这张表存在的理由是「这一个 DAG 现在能不能继续推进」要一次读出，\
                  不从事件流折——折一次要读完整条历史，而调度每轮都问。",
        },
    },
    DeclaredTable {
        table: "cog_observability_metrics",
        shape: TableShape::Document {
            why: "上报的读数按 (名字, 标签) 唯一：同一序列再报一次就覆盖成新值，行就是那条序列的当前读数。",
        },
    },
    DeclaredTable {
        table: "cog_observability_snapshots",
        shape: TableShape::Document {
            why: "一次观测的快照按身份唯一：同一个快照再写一次就覆盖成新的那一次结果。\
                  它描述的是「最近一次观测看到了什么」，而不是每次观测各留一行。",
        },
    },
    DeclaredTable {
        table: "cog_observability_squads",
        shape: TableShape::Document {
            why: "一个 squad 的当前成员与状态：再上报一次就覆盖同一行。\
                  成员进出的历史不在库里，这一行回答的是「这个 squad 现在有谁」。",
        },
    },
    DeclaredTable {
        table: "cog_snapshots",
        shape: TableShape::Document {
            why: "一个执行快照：同一个 `snapshot_id` 再存一次就覆盖，恢复读的是最新那一份。",
        },
    },
    DeclaredTable {
        table: "cogneva_owner_leases",
        shape: TableShape::Document {
            why: "一把租约的当前持有者与到期时刻：续约改的就是同一行，历史不在库里。\
                  换手必须先赢下这一行的条件更新——那正是一个追加表做不到的地方：\
                  「谁现在持有」要一个能被独占的当前值，而不是一串候选。",
        },
    },
    DeclaredTable {
        table: "explainability",
        shape: TableShape::Document {
            why: "一条决策解释按 `record_id` 唯一：同一条再写一次就覆盖，它描述的是那一次决策。",
        },
    },
    DeclaredTable {
        table: "progress_bounds",
        shape: TableShape::Document {
            why: "一条进度轴的当前边界：跑的过程中边界会被推高，行是那个边界的当前值。\
                  读者问的是「这条轴现在到哪了」，历史每一跳对它没有意义。",
        },
    },
    DeclaredTable {
        table: "raw_log_index",
        shape: TableShape::Document {
            why: "一份原始日志文件的索引按 (流, 日期, 格式) 唯一：同一天同一流换了格式只是多一行，同一个文件再扫一次就覆盖它自己那一行。",
        },
    },
    DeclaredTable {
        table: "task_plan",
        shape: TableShape::Document {
            why: "一个任务的当前计划：重规划覆盖它，而计划的历史在台账里、不在这张表。\
                  这一行只为「派发时读到的计划是哪一版」服务，版本号由台账给。",
        },
    },
    DeclaredTable {
        table: "alerts",
        shape: TableShape::Document {
            why: "一条告警的生命周期（firing → resolved）就是这一行本身：没有事件日志，\
                  `fired_at` / `resolved_at` / `state` 是那段历史的当前值。",
        },
    },
    DeclaredTable {
        table: "backup_drill_attempts",
        shape: TableShape::Document {
            why: "一次演练的尝试是一段有始有终的过程；`finished_at` 只有一次写入，\
                  在那之前每次播种都改同一行。",
        },
    },
    DeclaredTable {
        table: "chat_turn_watermarks",
        shape: TableShape::Document {
            why: "一行是「这段会话的轮折到流上哪个位置」的当前值：折一次改一次。它不是投影\n                  （没有从别处重算的整份内容），而是一份投影的读到哪儿了——丢了它，下一次\n                  对这些会话整段重折一遍，读出来的东西一样。",
        },
    },
    DeclaredTable {
        table: "change_ledger",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/change_ledger_store.rs",
            watermark: Some("as_of_seq"),
            rebuild: "crates/cog-storage/src/lib.rs",
            rebuilt_from: "由 `change_judged`／`change_declared`／`change_delivered` 三类事实折回来（每个身份取最后一条；声明补 title／assertion 与每条引用）",
            rebuild_test: "crates/cog-storage/tests/a_change_account_rebuilds_from_the_stream.rs",
        },
    },
    DeclaredTable {
        table: "change_ledger_declared",
        shape: TableShape::Document {
            why: "折叠队列：一行被取走时写 `folded_at`。把「谁取走了哪一行」放在行上，\
                  是为了让认领与标记是同一条 `FOR UPDATE SKIP LOCKED` 语句。",
        },
    },
    DeclaredTable {
        table: "change_ledger_remeasure",
        shape: TableShape::Document {
            why: "重测队列，与 `change_ledger_declared` 同一个理由：认领与标记一条语句。",
        },
    },
    DeclaredTable {
        table: "change_ledger_retired",
        shape: TableShape::Document {
            why: "退休队列，与 `change_ledger_declared` 同一个理由：认领与标记一条语句。",
        },
    },
    DeclaredTable {
        table: "cog_agent_states",
        shape: TableShape::Document {
            why: "每个 agent 的当前状态：最新那一行就是答案。它不是任何一张事件表折出来的——\
                  agent 的生死由它自己的心跳与租约决定，而那条历史在读数与日志里，\
                  库里留一行是为了「这个 agent 现在是什么状态」不必去读日志。",
        },
    },
    DeclaredTable {
        table: "cog_evolution_promotions",
        shape: TableShape::Document {
            why: "一次提升只有一个结局：`outcome` / `decided_at` 由终局写入，之后不再改。",
        },
    },
    DeclaredTable {
        table: "notifications",
        shape: TableShape::Document {
            why: "收件箱：`is_read` / `read_at` 是收件人自己做的那一件事，行是它的当前值。",
        },
    },
    DeclaredTable {
        table: "platform_identities",
        shape: TableShape::Document {
            why: "一个身份与一个外部提供者之间的绑定：令牌会过期、会刷新，而绑定本身不产生\
                  事件——它就是「这个人可以用这个提供者登录」的当前值。刷新令牌是改描述，\
                  不是新事实。",
        },
    },
    DeclaredTable {
        table: "principle_ledger",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/principle_ledger_store.rs",
            // 整表重算：`rebuild_principle_states` 按 `principle_judged` 事件重写全表，
            // 没有增量读，也就没有「读到哪」的水位。
            watermark: None,
            rebuild: "crates/cog-storage/src/lib.rs",
            rebuilt_from: "由 `principle_judged` 事件重建（每个 slug 取最后一条判词）",
            rebuild_test: "crates/cog-storage/tests/a_principle_verdict_rebuilds_from_the_stream.rs",
        },
    },
    DeclaredTable {
        table: "principle_ledger_declared",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/principle_ledger_store.rs",
            watermark: None,
            rebuild: "crates/cog-storage/src/lib.rs",
            rebuilt_from: "由 `principle_declared` 事件重建",
            rebuild_test: "crates/cog-storage/tests/a_principle_verdict_rebuilds_from_the_stream.rs",
        },
    },
    DeclaredTable {
        table: "principle_refusals",
        shape: TableShape::Document {
            why: "拒绝计数：同一条原则被拒绝一次就把 `count` 加一，行的意义就是那个累加值。\
                  每一条计数背后有历史，但读者要的是「这条被拒了多少次」这一个数，\
                  而且它要被 `UPDATE` 一次性加，不能先读再写。",
        },
    },
    DeclaredTable {
        table: "resource_events",
        shape: TableShape::Document {
            why: "同一事件重复报到只累加 `count` 并写 `folded_at`：账要的是一条事件一行，\
                  不是每次报到一行。",
        },
    },
    DeclaredTable {
        table: "runtime_principle",
        shape: TableShape::Document {
            why: "运行时原则的当前状态由人的决定改写（`decided_at` / `state`）：\
                  提议与决定都发生在这一行上。",
        },
    },
    DeclaredTable {
        table: "schema_entries",
        shape: TableShape::Document {
            why: "知识条目是一份会被修正的文档：同一个实体再被观测一次，新的置信度与来源就\
                  合并进同一行，而不是追加一条。真源是这些条目的合起来的那份知识，\
                  而它只在这一张表里；没有任何别的地方能重算出它。",
        },
    },
    DeclaredTable {
        table: "summary_entries",
        shape: TableShape::Document {
            why: "摘要条目是一份会被修正的文档：同一段原文再抽取一次，新的摘要与相关条目就\
                  合并进同一行，而不是追加一条。向量与正文一起改，所以它不能被当成\
                  追加表——两个读者会看到同一段的两个版本。",
        },
    },
    DeclaredTable {
        table: "task_attempt",
        shape: TableShape::Document {
            why: "一次尝试有开始有结束：`ended_at` / `outcome` 由结束那一刻写入。",
        },
    },
    DeclaredTable {
        table: "users",
        shape: TableShape::Document {
            why: "一个人是权威文档：改邮箱、改显示名、停用一个账号都不是新事件，是同一个人的\
                  新描述。没有任何一张表能重算出「这个人现在叫什么」——如果这一行不是真源，\
                  那句重算从哪里读？",
        },
    },
];

/// 普查里一张表的归属。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CensusClass {
    /// 事件流本身：只追加、就是正本，没有「重建」这回事。
    Stream,
    /// 可从事件重建的投影：点名那条重建路线与它由什么重算。
    Projection {
        rebuild: &'static str,
        rebuilt_from: &'static str,
    },
    /// 纯传输配置／读数：不存事实，删了不会丢掉任何「发生过什么」。
    Config { why: &'static str },
    /// 旧主存：行本身就是那份事实，但还没有迁到流上，也没有重建路线。
    ///
    /// 这是**明确的不合格**，不是第四种合法形状：这一格只为了让普查现在就跑起来
    /// ——每一张没归类的表都在这里点名，新加的表无处可藏；数字只许减，不许增。
    ///
    /// `successor` 点名它已经在流上有的那一类事实（有就是「只差删掉这一张」）；
    /// `why` 写清楚：为什么还留着，或者它由什么重算。未判的表 `successor` 为空、
    /// `why` 照实说「尚未逐张判」——欠账要能一张一张数。
    Legacy {
        successor: Option<&'static str>,
        why: &'static str,
    },
}

/// 普查里的一行。
pub struct CensusRow {
    pub table: &'static str,
    pub class: CensusClass,
}

/// 迁移与 store 的 DDL 里建的每一张表。
///
/// 加一张表就必须在这里加一行——判据从 DDL 里算表，与这张名单两头比：少一行是
/// 「新表没人归类」，多一行是「名单没跟上表」，都红。
pub const TABLE_CENSUS: &[CensusRow] = &[
    CensusRow {
        table: "agent_messages",
        class: CensusClass::Legacy {
            successor: Some("message_sent"),
            why: "这一族的行已由 `message_sent` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "agent_performance",
        class: CensusClass::Legacy {
            successor: None,
            why: "统计面：今天没有一条从流上重算它的命令，所以仍按旧主存记（欠账仍在，不冒充投影）",
        },
    },
    CensusRow {
        table: "agents",
        class: CensusClass::Legacy {
            successor: Some("employee"),
            why: "名册已由 `employee` 事实折出来；这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）",
        },
    },
    CensusRow {
        table: "alert_assertions",
        class: CensusClass::Legacy {
            successor: None,
            why: "告警判定的落点：与 `alerts` 同一族，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "alerts",
        class: CensusClass::Legacy {
            successor: None,
            why: "告警行是那段历史的当前值：告警的触发与消解在流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "audit_logs",
        class: CensusClass::Legacy {
            successor: None,
            why: "审计与原始日志（按月分区）：它们是那段字节的正本，流不承载",
        },
    },
    CensusRow {
        table: "audit_logs_y2026m05",
        class: CensusClass::Legacy {
            successor: None,
            why: "审计与原始日志（按月分区）：它们是那段字节的正本，流不承载",
        },
    },
    CensusRow {
        table: "audit_logs_y2026m06",
        class: CensusClass::Legacy {
            successor: None,
            why: "审计与原始日志（按月分区）：它们是那段字节的正本，流不承载",
        },
    },
    CensusRow {
        table: "backup_drill_attempts",
        class: CensusClass::Legacy {
            successor: None,
            why: "备份执行的记录：流上没有对应的事实种类，它是那段过程的正本（`MUTABLE_TABLES` 里按文档登记）",
        },
    },
    CensusRow {
        table: "backup_drills",
        class: CensusClass::Legacy {
            successor: None,
            why: "备份执行的记录：流上没有对应的事实种类，它是那段过程的正本（`MUTABLE_TABLES` 里按文档登记）",
        },
    },
    CensusRow {
        table: "backup_runs",
        class: CensusClass::Legacy {
            successor: None,
            why: "备份执行的记录：流上没有对应的事实种类，它是那段过程的正本（`MUTABLE_TABLES` 里按文档登记）",
        },
    },
    CensusRow {
        table: "billing_records",
        class: CensusClass::Legacy {
            successor: None,
            why: "计费与充值的原始行：钱的记录本身，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "billing_records_y2026m05",
        class: CensusClass::Legacy {
            successor: None,
            why: "计费与充值的原始行：钱的记录本身，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "billing_records_y2026m06",
        class: CensusClass::Legacy {
            successor: None,
            why: "计费与充值的原始行：钱的记录本身，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "change_dispatch_state",
        class: CensusClass::Config {
            why: "派发簿记是「这批派到哪」的进度读数。",
        },
    },
    CensusRow {
        table: "chat_turn_watermarks",
        class: CensusClass::Legacy {
            successor: None,
            why: "按会话记录读到哪一回合：投影的增量水位本身是当前值。",
        },
    },
    CensusRow {
        table: "summary_entries_archive",
        class: CensusClass::Stream,
    },
    CensusRow {
        table: "schema_entries_archive",
        class: CensusClass::Stream,
    },
    CensusRow {
        table: "change_ledger",
        class: CensusClass::Projection {
            // 这一格按「生产代码里找得到的那段字」判（`rebuild_from_events` 在
            // `lib.rs` 的 `rebuild_change_ledger` 里被调用），不是路径。
            rebuild: "rebuild_from_events",
            rebuilt_from: "由 `change_judged`／`change_declared`／`change_delivered` 折回来",
        },
    },
    CensusRow {
        table: "change_ledger_declared",
        class: CensusClass::Legacy {
            successor: Some("change_declared"),
            why: "折叠队列：被取走的每一条都落成一条 `change_declared` 事实，队列本身只是收件台",
        },
    },
    CensusRow {
        table: "change_ledger_event",
        class: CensusClass::Legacy {
            successor: Some("change_judged"),
            why: "这一族的行已由 `change_judged` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "change_ledger_remeasure",
        class: CensusClass::Legacy {
            successor: None,
            why: "重测收件台：行是「谁希望重问一次」的待办，不是事实；问出来的答案落成 `change_judged`",
        },
    },
    CensusRow {
        table: "change_ledger_retired",
        class: CensusClass::Legacy {
            successor: Some("change_judged"),
            why: "退场标记折成 `change_judged` 的 `retire_reason`：状态与原因都在那一条事实里",
        },
    },
    CensusRow {
        table: "change_ledger_sources",
        class: CensusClass::Legacy {
            successor: Some("change_declared"),
            why: "引用是 `change_declared` 事实的重算结果：重建时按 (identity, doc) 从声明写回",
        },
    },
    CensusRow {
        table: "change_reopen",
        class: CensusClass::Legacy {
            successor: None,
            why: "重开留痕：记的是「这一条落地又被重开」这件事本身，折叠时盖进由 `change_judged` 折出的那一行",
        },
    },
    CensusRow {
        table: "chat_sessions",
        class: CensusClass::Legacy {
            successor: None,
            why: "会话由 `intent_received` 与 `message_sent` 折出来（`dialogue_for_session`）；这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有），也没有单一类事实可点名",
        },
    },
    CensusRow {
        table: "chat_turns",
        class: CensusClass::Projection {
            rebuild: "rebuild_chat_turns",
            rebuilt_from: "由意图/消息/工具调用事件重建",
        },
    },
    CensusRow {
        table: "cog_agent_states",
        class: CensusClass::Legacy {
            successor: None,
            why: "agent 状态快照的正本：那份状态由 agent 自己写，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_attempt_journal",
        class: CensusClass::Legacy {
            successor: None,
            why: "一次尝试的日志是那段运行的正本：`FACT_TABLES` 在册，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_checkpoints",
        class: CensusClass::Legacy {
            successor: None,
            why: "检查点：一次任务执行中途的状态由写入方自己记，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_context_boards",
        class: CensusClass::Legacy {
            successor: None,
            why: "上下文板：一次执行带着的那几格状态由写入方自己记，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_dag_state",
        class: CensusClass::Legacy {
            successor: None,
            why: "DAG 运行状态的正本：那段编排状态由运行方自己记，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_events",
        class: CensusClass::Stream,
    },
    CensusRow {
        table: "cog_evolution_promotions",
        class: CensusClass::Legacy {
            successor: None,
            why: "晋级记录的正本：晋级这件事由演化侧写，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_guard_audit_log",
        class: CensusClass::Legacy {
            successor: None,
            why: "守卫审计行是那段历史的当前值：流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_hook_events",
        class: CensusClass::Legacy {
            successor: None,
            why: "钩子事件的正本：钩子触发本身没有事件种类（它是机制，不是事实）",
        },
    },
    CensusRow {
        table: "cog_llm_skip",
        class: CensusClass::Legacy {
            successor: Some("llm_skip"),
            why: "这一族的行已由 `llm_skip` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "cog_llm_spend_verdict",
        class: CensusClass::Legacy {
            successor: Some("spend_verdict"),
            why: "这一族的行已由 `spend_verdict` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "cog_llm_usage_rollup",
        class: CensusClass::Legacy {
            successor: Some("llm_call"),
            why: "小时折叠是 `llm_call` 事实的重算结果（用量行本身也是它的投影），重建时按同一批事实再算一遍",
        },
    },
    CensusRow {
        table: "cog_metric_counter_totals",
        class: CensusClass::Projection {
            rebuild: "rebuild-metric-projections",
            rebuilt_from: "按 Prometheus 的 `<name>_total` 序列整表重写",
        },
    },
    CensusRow {
        table: "cog_metric_histogram_buckets",
        class: CensusClass::Projection {
            rebuild: "rebuild-metric-projections",
            rebuilt_from: "按 Prometheus 的 `<name>_bucket` 序列整表重写",
        },
    },
    CensusRow {
        table: "cog_metric_histogram_sums",
        class: CensusClass::Projection {
            rebuild: "rebuild-metric-projections",
            rebuilt_from: "按 Prometheus 的 `<name>_sum` 序列整表重写",
        },
    },
    CensusRow {
        table: "cog_metrics_samples",
        class: CensusClass::Legacy {
            successor: None,
            why: "指标样本就是那段时间的正本：流上没有对应的事实种类，重算不出过去的值",
        },
    },
    CensusRow {
        table: "cog_observability_events",
        class: CensusClass::Stream,
    },
    CensusRow {
        table: "cog_observability_logs",
        class: CensusClass::Stream,
    },
    CensusRow {
        table: "cog_observability_metrics",
        class: CensusClass::Legacy {
            successor: None,
            why: "观测侧写的指标行：今天没有从流上重算它的命令（欠账仍在，不冒充投影）",
        },
    },
    CensusRow {
        table: "cog_observability_raw_index",
        class: CensusClass::Legacy {
            successor: None,
            why: "观测原始索引的正本：那段索引由上报侧写，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_observability_snapshots",
        class: CensusClass::Legacy {
            successor: None,
            why: "观测快照的正本：那份快照由上报侧写，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_observability_squads",
        class: CensusClass::Legacy {
            successor: None,
            why: "观测里的小队状态：上报侧写的侧写，今天没有从流上重算它的命令",
        },
    },
    CensusRow {
        table: "cog_snapshots",
        class: CensusClass::Legacy {
            successor: None,
            why: "快照记录本身：它是那份快照的索引，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cog_taste_intents",
        class: CensusClass::Legacy {
            successor: None,
            why: "品味意图的正本：那句意图由写入方记，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "cogneva_event",
        class: CensusClass::Stream,
    },
    CensusRow {
        table: "cogneva_owner_leases",
        class: CensusClass::Legacy {
            successor: None,
            why: "租约的当前持有者只在库里：它是并发原语的真值，不是由别的表算出来的。",
        },
    },
    CensusRow {
        table: "datasets",
        class: CensusClass::Legacy {
            successor: None,
            why: "上传数据集的登记行：内容与它同一份，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "decision_stats_groups",
        class: CensusClass::Legacy {
            successor: None,
            why: "统计与解释面：今天没有一条从流上重算它的命令，所以仍按旧主存记（欠账仍在，不冒充投影）",
        },
    },
    CensusRow {
        table: "explainability",
        class: CensusClass::Legacy {
            successor: None,
            why: "统计与解释面：今天没有一条从流上重算它的命令，所以仍按旧主存记（欠账仍在，不冒充投影）",
        },
    },
    CensusRow {
        table: "explainability_part",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：分片解释没有读者",
        },
    },
    CensusRow {
        table: "explainability_y2026m05",
        class: CensusClass::Legacy {
            successor: None,
            why: "统计与解释面：今天没有一条从流上重算它的命令，所以仍按旧主存记（欠账仍在，不冒充投影）",
        },
    },
    CensusRow {
        table: "explainability_y2026m06",
        class: CensusClass::Legacy {
            successor: None,
            why: "统计与解释面：今天没有一条从流上重算它的命令，所以仍按旧主存记（欠账仍在，不冒充投影）",
        },
    },
    CensusRow {
        table: "files",
        class: CensusClass::Legacy {
            successor: None,
            why: "`crates/` 的生产代码里没有一条 SQL 读或写它：上传内容的索引在别处，这一张已不在生产路径上",
        },
    },
    CensusRow {
        table: "gateway_llm_exchange",
        class: CensusClass::Legacy {
            successor: Some("llm_call"),
            why: "这一族的行已由 `llm_call` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "gateway_llm_usage",
        class: CensusClass::Legacy {
            successor: Some("llm_call"),
            why: "这一族的行已由 `llm_call` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "gateway_llm_window",
        class: CensusClass::Config {
            why: "配速窗口是读数，按调用事件可重算。",
        },
    },
    CensusRow {
        table: "memory_raw_sources",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：Raw 层的内容与它的来源都由记忆条目自己带",
        },
    },
    CensusRow {
        table: "memory_schema_entries",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：模式条目在 `schema_entries` 上",
        },
    },
    CensusRow {
        table: "memory_summary_entries",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：摘要条目在 `summary_entries` 上",
        },
    },
    CensusRow {
        table: "messages",
        class: CensusClass::Legacy {
            successor: Some("message_sent"),
            why: "这一族的行已由 `message_sent` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "messages_y2026m05",
        class: CensusClass::Legacy {
            successor: Some("message_sent"),
            why: "这一族的行已由 `message_sent` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "messages_y2026m06",
        class: CensusClass::Legacy {
            successor: Some("message_sent"),
            why: "这一族的行已由 `message_sent` 事实承载：读面切过去之后，这张表只剩删掉这一步",
        },
    },
    CensusRow {
        table: "model_pricing",
        class: CensusClass::Config {
            why: "价目表是人写下来的价格，不是发生的事。",
        },
    },
    CensusRow {
        table: "notifications",
        class: CensusClass::Legacy {
            successor: None,
            why: "通知是事实：`FACT_TABLES` 在册、有自己的保留与读者；流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "pge_stage_product",
        class: CensusClass::Legacy {
            successor: None,
            why: "PGE 的阶段产物：中间结果存在这里以便续跑；流上没有对应的事实种类，重算等于重跑一次生成",
        },
    },
    CensusRow {
        table: "platform_identities",
        class: CensusClass::Legacy {
            successor: None,
            why: "权威文档：行就是那段事实的当前值（`MUTABLE_TABLES` 里按文档登记），不是从流重算出来的",
        },
    },
    CensusRow {
        table: "principle_debt",
        class: CensusClass::Legacy {
            successor: None,
            why: "`crates/` 的生产代码里没有一条 SQL 读或写它：原则欠账的判词在 `principle_judged` 上",
        },
    },
    CensusRow {
        table: "principle_ledger",
        class: CensusClass::Projection {
            rebuild: "rebuild_principle_states",
            rebuilt_from: "由 `principle_judged` 事件重建",
        },
    },
    CensusRow {
        table: "principle_ledger_declared",
        class: CensusClass::Projection {
            rebuild: "rebuild_principle_declarations",
            rebuilt_from: "由 `principle_declared` 事件重建",
        },
    },
    CensusRow {
        table: "principle_producer_pushes",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：产出点由名册按路径登记，不靠推送表",
        },
    },
    CensusRow {
        table: "principle_refusals",
        class: CensusClass::Legacy {
            successor: None,
            why: "拒绝的收件台：折进 `principle_ledger` 的拒绝计数；这一项在流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "progress_bounds",
        class: CensusClass::Legacy {
            successor: None,
            why: "进度边界的当前值由执行过程当场推高；历史每一跳不在库里。",
        },
    },
    CensusRow {
        table: "projection_snapshot",
        class: CensusClass::Config {
            why: "投影读取水位缓存，可从投影自己重算。",
        },
    },
    CensusRow {
        table: "raw_log_index",
        class: CensusClass::Legacy {
            successor: None,
            why: "审计与原始日志（按月分区）：它们是那段字节的正本，流不承载",
        },
    },
    CensusRow {
        table: "raw_log_index_y2026m05",
        class: CensusClass::Legacy {
            successor: None,
            why: "审计与原始日志（按月分区）：它们是那段字节的正本，流不承载",
        },
    },
    CensusRow {
        table: "raw_log_index_y2026m06",
        class: CensusClass::Legacy {
            successor: None,
            why: "审计与原始日志（按月分区）：它们是那段字节的正本，流不承载",
        },
    },
    CensusRow {
        table: "recharge_records",
        class: CensusClass::Legacy {
            successor: None,
            why: "计费与充值的原始行：钱的记录本身，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "resource_events",
        class: CensusClass::Legacy {
            successor: None,
            why: "资源用量的原始事件：那段测量只有这一份，流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "runtime_principle",
        class: CensusClass::Legacy {
            successor: None,
            why: "哪条原则此刻生效是运行时状态，不是传输配置。",
        },
    },
    CensusRow {
        table: "schema_entries",
        class: CensusClass::Legacy {
            successor: None,
            why: "记忆模式条目的正本：`FACT_TABLES` 在册，删之前先归档；流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "schema_migrations",
        class: CensusClass::Config {
            why: "迁移账，基础设施记录。",
        },
    },
    CensusRow {
        table: "summary_entries",
        class: CensusClass::Legacy {
            successor: None,
            why: "记忆摘要条目的正本：`FACT_TABLES` 在册，删之前先归档；流上没有对应的事实种类",
        },
    },
    CensusRow {
        table: "task_attempt",
        class: CensusClass::Legacy {
            successor: None,
            why: "一次尝试的开始与结束由执行路径当场写入；行是那次尝试的当前记录。",
        },
    },
    CensusRow {
        table: "task_current",
        class: CensusClass::Projection {
            rebuild: "fn project",
            rebuilt_from: "由任务的全部转移事件折叠",
        },
    },
    CensusRow {
        table: "task_defined",
        class: CensusClass::Projection {
            rebuild: "rebuild_task_definitions",
            rebuilt_from: "由 `task_defined` 事件重建",
        },
    },
    CensusRow {
        table: "task_dependencies",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：任务之间的先后由任务的转移事实表达",
        },
    },
    CensusRow {
        table: "task_plan",
        class: CensusClass::Legacy {
            successor: None,
            why: "计划正文的落点（planner 的产物）：流上的任务事实只记定义与转移，计划正文不在这里",
        },
    },
    CensusRow {
        table: "task_transition",
        class: CensusClass::Projection {
            rebuild: "fn project",
            rebuilt_from: "由 `task_transition` 事件重建（project 按任务重放）",
        },
    },
    CensusRow {
        table: "tasks",
        class: CensusClass::Legacy {
            successor: Some("task_defined"),
            why: "旧任务行：`crates/` 的生产代码里没有一条 SQL 读或写它；任务的定义与转移已在 `task_defined`／`task_transition` 上",
        },
    },
    CensusRow {
        table: "token_face_beat",
        class: CensusClass::Config {
            why: "令牌面的心跳是读数。",
        },
    },
    CensusRow {
        table: "user_auth_methods",
        class: CensusClass::Legacy {
            successor: None,
            why: "权威文档：行就是那段事实的当前值（`MUTABLE_TABLES` 里按文档登记），不是从流重算出来的",
        },
    },
    CensusRow {
        table: "user_biometric_keys",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：没有一条路径在用生物密钥",
        },
    },
    CensusRow {
        table: "user_devices",
        class: CensusClass::Legacy {
            successor: None,
            why: "这一张在 `crates/` 的生产代码里没有一条 SQL 读或写它（`FROM`／`INTO`／`UPDATE`／`JOIN` 都没有）：没有一条路径在用设备名录",
        },
    },
    CensusRow {
        table: "user_quotas",
        class: CensusClass::Config {
            why: "配额是策略值；配额变化若成为事实应进流，今天只存当前值。",
        },
    },
    CensusRow {
        table: "users",
        class: CensusClass::Legacy {
            successor: None,
            why: "权威文档：行就是那段事实的当前值（`MUTABLE_TABLES` 里按文档登记），不是从流重算出来的",
        },
    },
    CensusRow {
        table: "workspace_members",
        class: CensusClass::Legacy {
            successor: None,
            why: "权威文档：行就是那段事实的当前值（`MUTABLE_TABLES` 里按文档登记），不是从流重算出来的",
        },
    },
    CensusRow {
        table: "workspaces",
        class: CensusClass::Legacy {
            successor: None,
            why: "权威文档：行就是那段事实的当前值（`MUTABLE_TABLES` 里按文档登记），不是从流重算出来的",
        },
    },
    CensusRow {
        table: "write_windows",
        class: CensusClass::Config {
            why: "允许写入的时间窗是部署配置。",
        },
    },
];

/// 一份「证据载体」：被事件或指针指到的字节存在哪里、能不能被删。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarrierRetention {
    /// 永久保留：被指针指到的字节不许按时间/容量清掉。
    Permanent,
    /// 先归档（不可变、可读回校验）再删热副本；`archive` 是归档去处的前缀。
    ArchiveThenDelete { archive: &'static str },
    /// 现在还是「到点直接删」——明确的不合格，张数只许减。
    UnarchivedSweep,
}

/// 普查里的一份证据载体。
pub struct EvidenceCarrier {
    /// 人读的名字。
    pub name: &'static str,
    /// 载体在哪：表名、对象键前缀或 `artifact://`。
    pub carrier: &'static str,
    /// 谁的指针会指到它。
    pub pointed_by: &'static str,
    pub retention: CarrierRetention,
}

/// 每一份被指到的字节。指针能活过保留期，只有当它指向的载体永久。
///
/// 判据在 `crates/cogneva/tests/every_evidence_carrier_declares_its_retention_test.rs`。
pub const EVIDENCE_CARRIERS: &[EvidenceCarrier] = &[
    EvidenceCarrier {
        name: "事件流",
        carrier: "cogneva_event",
        pointed_by: "每一行事实自己",
        retention: CarrierRetention::Permanent,
    },
    EvidenceCarrier {
        name: "工具调用正文",
        carrier: "artifact://",
        pointed_by: "`llm_call` / `tool_call` 的 `artifact` 指针",
        retention: CarrierRetention::Permanent,
    },
    EvidenceCarrier {
        name: "会话附件",
        carrier: "attachments/",
        pointed_by: "`message_sent(kind=attachment)` 的 `key`",
        retention: CarrierRetention::Permanent,
    },
    EvidenceCarrier {
        name: "原则密封包",
        carrier: "principles/",
        pointed_by: "提示段清单里的载体引用（待建 EV28）",
        retention: CarrierRetention::Permanent,
    },
    EvidenceCarrier {
        name: "LLM 留底正文",
        carrier: "gateway_llm_exchange",
        pointed_by: "调用事实自身的留底；不是别人的指针",
        // 2026-11-25 复读：保留循环只测量，不删；`archive_batch` 是将来真要删时
        // 的唯一入口（写归档→读回逐字节比→再删），现在没有生产调用者。
        retention: CarrierRetention::Permanent,
    },
];

/// 一个从流上直接读答案的读口：函数名、所在文件、它读哪几类事实。
///
/// 投影表那一侧已有 `PROJECTION_READERS`；这一张补的是**不经过表、直接从流上折**
/// 的读口。判据在 `crates/cogneva/tests/every_storage_reader_is_in_the_census_test.rs`：
/// 生产代码里每一个读风格的 `pub async fn` 都要在这里有一行。
pub struct StreamReader {
    pub function: &'static str,
    pub path: &'static str,
    pub reads: &'static str,
}

/// 从流上直接读答案的那几个口。
pub const STREAM_READERS: &[StreamReader] = &[
    StreamReader {
        function: "dialogue_for_session",
        path: "crates/cog-storage/src/postgres/dialogue.rs",
        reads: "intent_received、message_sent",
    },
    StreamReader {
        function: "rebuild_chat_turns",
        path: "crates/cog-storage/src/postgres/chat_turn_rebuild.rs",
        reads: "intent_received、message_sent、tool_call",
    },
    StreamReader {
        function: "rebuild_task_projection",
        path: "crates/cog-storage/src/postgres/state_backend.rs",
        reads: "task_defined、task_transition",
    },
    StreamReader {
        function: "item_definition",
        path: "crates/cog-storage/src/postgres/item_events.rs",
        reads: "task_defined、task_transition、task（terminal_revised）",
    },
    StreamReader {
        function: "lease_from_stream",
        path: "crates/cog-storage/src/postgres/task_ledger.rs",
        reads: "task",
    },
    StreamReader {
        function: "keys",
        path: "crates/cog-storage/src/postgres/key_events.rs",
        reads: "key",
    },
    StreamReader {
        function: "redactions_for",
        path: "crates/cog-storage/src/postgres/redaction_read.rs",
        reads: "redacted",
    },
    StreamReader {
        function: "calls_in_batch",
        path: "crates/cog-storage/src/postgres/llm_call_events.rs",
        reads: "llm_call",
    },
    StreamReader {
        function: "prefix_breaks_in_session",
        path: "crates/cog-storage/src/postgres/llm_call_events.rs",
        reads: "llm_call",
    },
    StreamReader {
        function: "terminal_revisions_for",
        path: "crates/cog-storage/src/postgres/terminal_revisions.rs",
        reads: "task（terminal_revised 那一步）",
    },
    StreamReader {
        function: "landing_states",
        path: "crates/cog-storage/src/postgres/landing_states.rs",
        reads: "change_landed",
    },
    StreamReader {
        function: "read_table_account",
        path: "crates/cog-storage/src/postgres/change_ledger_store.rs",
        reads:
            "表 change_ledger（一条投影：由 change_judged╱change_declared╱change_delivered 折出来）",
    },
    StreamReader {
        function: "rebuild_principle_states",
        path: "crates/cog-storage/src/postgres/principle_ledger_store.rs",
        reads: "principle_judged",
    },
    StreamReader {
        function: "rebuild_principle_declarations",
        path: "crates/cog-storage/src/postgres/principle_ledger_store.rs",
        reads: "principle_declared",
    },
];
