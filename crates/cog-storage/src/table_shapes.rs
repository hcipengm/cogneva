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

/// 一张会被 `UPDATE` 的表，以及它为什么是这种形状。
#[derive(Debug, Clone, Copy)]
pub enum TableShape {
    /// 从别处重算出来的表：水位列、重建命令、读者声明。四样缺一不可。
    Projection {
        /// 读它的地方，写成树里的一个路径。
        reader: &'static str,
        /// 「我读到哪了」的那一列。
        watermark: &'static str,
        /// 从真源重建它的地方，写成树里的一个路径。
        rebuild: &'static str,
        /// 它由**哪些事实**重算出来。与 `projection_readers` 里同一张表的那一格
        /// 说的是同一句话，判据逐字比对——一份表两份声明各说各的，是这一族最老的病。
        rebuilt_from: &'static str,
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
            watermark: "as_of_seq",
            rebuild: "crates/cog-storage/src/postgres/task_ledger.rs",
            rebuilt_from: "task 台账里只追加的转移行（每一条任务的完整历史）",
        },
    },
    DeclaredTable {
        table: "cog_metric_counter_totals",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
            watermark: "as_of",
            rebuild: "crates/cog-observability/src/metric_projection.rs",
            rebuilt_from: "Prometheus 里 `<name>_total` 那条序列",
        },
    },
    DeclaredTable {
        table: "cog_metric_histogram_buckets",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
            watermark: "as_of",
            rebuild: "crates/cog-observability/src/metric_projection.rs",
            rebuilt_from: "Prometheus 里 `<name>_bucket` 那条序列的每一档",
        },
    },
    DeclaredTable {
        table: "cog_metric_histogram_sums",
        shape: TableShape::Projection {
            reader: "crates/cog-storage/src/postgres/metrics_backend.rs",
            watermark: "as_of",
            rebuild: "crates/cog-observability/src/metric_projection.rs",
            rebuilt_from: "Prometheus 里 `<name>_sum` 那条序列",
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
        table: "change_ledger",
        shape: TableShape::Document {
            why: "行是账本的当前状态，每一次改写都在同一条语句里写一条 \
                  `change_ledger_event`；但还没有一条从事件日志重建当前状态的命令——\
                  在写出来之前，这一行以文档身份存在，而不是自称投影。",
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
        shape: TableShape::Document {
            why: "每个原则的当前测量值：`measured_at` 与判词一起被下一次测量覆盖，\
                  历史在 `principle_refusals` 与事件里。",
        },
    },
    DeclaredTable {
        table: "principle_ledger_declared",
        shape: TableShape::Document {
            why: "折叠队列，与 `change_ledger_declared` 同一个理由：认领与标记一条语句。",
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
