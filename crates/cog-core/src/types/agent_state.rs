use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Agent lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Init,
    Registered,
    Active,
    Idle,
    Completing,
    Inactive,
    Suspect,
    Dead,
}

/// Checkpoint for task recovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskCheckpoint {
    pub task_id: String,
    pub snapshot_id: String,
    pub event_offset: u64,
    pub timestamp: DateTime<Utc>,
}

/// Context board for shared task state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ContextBoard {
    pub task_id: String,
    pub fields: HashMap<String, String>,
    pub updated_at: DateTime<Utc>,
}

/// The board field under which one role's resume point is recorded.
///
/// The writer is the process that holds the task (it writes checkpoints on a
/// cadence) and the reader is the dispatcher that builds the task's agents, so
/// the spelling cannot live in either crate: two spellings of a field name do
/// not fail, they leave the reader looking at a board that never carries a
/// pointer, and the whole chain reads as "nothing was ever resumable".
///
/// The field is keyed by role because a task's progress is per agent — each
/// role's context is its own — and the role is the part of the agent id
/// (`{task_id}-{role}`) that says which one a checkpoint belongs to.
pub const CHECKPOINT_FIELD_PREFIX: &str = "checkpoint:";

/// The board field holding `role`'s resume point for a task.
pub fn checkpoint_field(role: &str) -> String {
    format!("{CHECKPOINT_FIELD_PREFIX}{role}")
}

/// The name a squad running `task_id` is known by.
pub const SQUAD_ID_PREFIX: &str = "squad:";

/// The squad name for a task.
pub fn squad_id_for(task_id: &str) -> String {
    format!("{SQUAD_ID_PREFIX}{task_id}")
}

/// The name of the agent that plays `role` for `task_id`.
///
/// Task and agent are related by this name alone — there is no mapping table —
/// and the two ends that depend on it live in different crates: the task holder
/// derives it to decide which agents are its own, and the dispatcher derives it
/// to build those agents. Two spellings do not fail; they leave each end looking
/// at a set of agents the other never had, which reads exactly like a task that
/// had nothing to resume from.
pub fn agent_id_for(task_id: &str, role: &str) -> String {
    format!("{}-{role}", squad_id_for(task_id))
}

/// 数字员工名册：人名的唯一来源。
///
/// 列表只管"好不好认"：唯一性由 id 尾部那 8 位短码承担（见 [`unregistered_label`]
/// 与 `cog_agent::identity` 的 id 派生），因此名册不需要很长。**长度不能随手改**
/// ——选取是 `哈希 % 名字数`，增删一个名字会让所有员工换名，按旧名记下的归属就
/// 读不回同一个人。
pub const NAME_POOL: &[&str] = &[
    "Alice", "Ralph", "Nova", "Kai", "Vera", "Orion", "Sage", "Echo", "Iris", "Leo", "Mira",
    "Quinn", "Ada", "Felix", "Luna", "Max",
];

// ─── 工号 ＋ 姓名 ────────────────────────────────────────────────────────
//
// 上一套展示名是「人名#8 位机器码」（`Luna#1822ab0e`）：
// 人名池只有 16 个，撞名是常态，尾巴负责把人分开。读的人看到的仍然是一串
// 十六进制，等于把「谁在干这件事」这个问题交还给了日志。
//
// 现在这张名册按真实公司的办法发人名：**工号**（全局唯一、只增、永不复用）
// 在前，**姓名**（由工号算出来的三字中文名）在后，展示成 `E0412 林清和`。
// 两条性质由此成立：
//
// 1. 展示名永不重复——工号唯一，而工号是展示名的一部分。姓名本身偶有同姓
//    同名（池子是 61×48×48 ≈ 14 万种组合，两千人里大约有十来对），这与
//    真实公司一致，且工号当场把它们分开。
// 2. 一个人换 Pod、换槽位、重装之后还是同一个工号——工号存在名册行里
//    建立一次，之后只读。名字因此从「由 id 哈希出来的读数」变成「发出去的
//    编制」，重算一遍不会换人。
//
// 工号**不回收**：退役的人保留工号，正如离职员工保留工号；回收会让历史里
// 两条不相干的事顶着同一个号。

/// 工号前缀。读作 employee。
pub const EMPLOYEE_NUMBER_PREFIX: &str = "E";

/// 姓。与 [`NAME_POOL`] 是两个用途：那一份是进程身份（提交签名要它稳定到
/// 跨重装），这一份只给展示名用，改它不会动任何已发出去的工号。
pub const SURNAMES: &[&str] = &[
    "赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈", "蒋", "沈", "韩", "杨", "朱", "秦",
    "许", "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏", "陶", "姜", "谢", "邹", "柏",
    "章", "云", "苏", "潘", "葛", "范", "彭", "鲁", "韦", "马", "苗", "方", "俞", "任", "袁", "柳",
    "林", "高", "罗", "梁", "宋", "唐", "于", "董", "萧", "程", "邓", "崔", "曾",
];

/// 名的第一个字。
pub const GIVEN_FIRST: &[&str] = &[
    "清", "明", "远", "宁", "安", "嘉", "子", "亦", "承", "景", "知", "若", "语", "思", "慕", "文",
    "星", "晨", "月", "白", "青", "松", "竹", "兰", "溪", "川", "岑", "屿", "舟", "帆", "昭", "昱",
    "晗", "宸", "玥", "珩", "瑄", "珂", "瑞", "朗", "熙", "澈", "澜", "淇", "湘", "洛", "沅", "芷",
];

/// 名的第二个字。
pub const GIVEN_SECOND: &[&str] = &[
    "和", "川", "山", "海", "宁", "安", "然", "言", "行", "远", "舒", "朗", "溪", "洲", "城", "野",
    "序", "章", "明", "亮", "光", "辉", "阳", "春", "秋", "冬", "夏", "雨", "雪", "风", "云", "星",
    "月", "花", "叶", "林", "森", "木", "石", "岩", "峰", "江", "河", "湖", "泉", "庭", "苑", "园",
];

fn pool_pick(digest: &[u8], at: usize, pool: &'static [&'static str]) -> &'static str {
    let v = u32::from_be_bytes([digest[at], digest[at + 1], digest[at + 2], digest[at + 3]]);
    pool[(v as usize) % pool.len()]
}

/// 工号对应的姓名。纯函数，同一个工号永远是同一个名字。
///
/// 名册行里只存工号，姓名随读随算：这样姓名永远对得上工号，也不会出现
/// 「行里存着旧名字、算法已经改过」这种两边不一致。
pub fn employee_name(number: u32) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("cogneva-employee-name\n{number}\n").as_bytes());
    let mut name = String::new();
    name.push_str(pool_pick(&digest, 0, SURNAMES));
    name.push_str(pool_pick(&digest, 4, GIVEN_FIRST));
    name.push_str(pool_pick(&digest, 8, GIVEN_SECOND));
    name
}

/// 名册、对话、日志里给人的那一行：`E0412 林清和`。
pub fn employee_label(number: u32) -> String {
    format!(
        "{prefix}{number:04} {name}",
        prefix = EMPLOYEE_NUMBER_PREFIX,
        name = employee_name(number)
    )
}

/// 还没发工号的 id 怎么显示。
///
/// 这一格只该在「这个人还没走过注册（因此还没进名册）」时读到。它明确写出
/// 「待编」而不是退回机器码：读的人应当看到一条待办，而不是一串十六进制
/// ——后者看起来像名字，于是没人去补那一次编号。
pub fn unregistered_label(agent_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(agent_id.as_bytes());
    let selector = u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8]));
    let persona = NAME_POOL[(selector as usize) % NAME_POOL.len()];
    format!("待编 {persona}")
}

#[cfg(test)]
mod naming_tests {
    use super::*;

    /// 工号唯一 ⇒ 展示名唯一。这是「员工不要重名」这条要求在代码里的判据：
    /// 只要这个循环里出现一次相等，名册上就有一个读不出是谁的名字。
    #[test]
    fn every_employee_number_has_its_own_label() {
        let mut seen = std::collections::HashSet::new();
        for n in 1..=5000u32 {
            assert!(seen.insert(employee_label(n)), "工号 {n} 与别人重名");
        }
    }

    /// 工号是发出去的编制，不是每次都重新算的读数：同一个号必须是同一个名字。
    #[test]
    fn the_same_number_is_the_same_person() {
        assert_eq!(employee_label(412), employee_label(412));
        assert_ne!(employee_label(412), employee_label(413));
    }

    #[test]
    fn the_label_reads_as_number_then_name() {
        let label = employee_label(412);
        let (number, name) = label.split_once(' ').expect("工号与姓名之间有一个空格");
        assert_eq!(number, "E0412");
        assert_eq!(name.chars().count(), 3);
        let first = name.chars().next().expect("姓名非空");
        assert!(
            SURNAMES.iter().any(|s| s.starts_with(first)),
            "{name} 的姓不在姓池里"
        );
    }

    /// 姓名池要够大：两千个在职员工里同姓同名的应当是个位数的量级。
    /// 池子被改小了这条会红——那正是「重名太高」重新出现的时刻。
    #[test]
    fn two_thousand_employees_rarely_share_a_name() {
        let mut names = std::collections::HashMap::new();
        for n in 1..=2000u32 {
            *names.entry(employee_name(n)).or_insert(0usize) += 1;
        }
        let repeats = names.values().filter(|c| **c > 1).count();
        assert!(repeats <= 30, "{repeats} 个姓名在两千人里重复出现");
    }
}

/// Event for event sourcing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub offset: u64,
    pub task_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}
