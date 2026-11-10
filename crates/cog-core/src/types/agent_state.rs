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

/// The prefix every employee's checkout is named with.
///
/// The prefix is also how the executor tells an employee's tree from a task's
/// leftover one: an employee's tree comes back on the next turn and must not be
/// reaped for idleness, while a task's tree is spent by the round that made it.
pub const WORKTREE_LANE_PREFIX: &str = "employee-";

/// The longest a checkout's name may be.
///
/// A lane is used as a directory name (`<workspaces>/<lane>`) and as a branch
/// name (`work/<lane>`), so what bounds it is the filesystem's limit on one
/// path component -- 255 bytes on Linux -- with the branch's own prefix taken
/// off. Exported because the executor's router validates the same names and
/// would otherwise keep a ceiling of its own: the tighter of the two silently
/// becomes the real one, and an id past it loses its checkout rather than
/// failing anywhere a reader could see.
// cap: bound — Linux NAME_MAX (255 bytes per path component) less the `work/` a branch name adds; a platform ceiling, not a capacity reading of this deployment, so no reading moves it
pub const MAX_CHECKOUT_NAME_CHARS: usize = 255 - "work/".len();

/// How much of an agent id the lane name keeps.
///
/// Subtracted from [`MAX_CHECKOUT_NAME_CHARS`] rather than written as a round
/// number, because the prefix is what consumes the budget: a lane whose prefix
/// grew must shorten by exactly that much.
const LANE_NAME_MAX_CHARS: usize = MAX_CHECKOUT_NAME_CHARS - WORKTREE_LANE_PREFIX.len();

/// `-` plus the eight hex digits that end a truncated lane.
// cap: bound — 一个 `-` 和八位十六进制；它数的是指纹的写法，不是这台机器能承受多少
const TRUNCATION_TAIL_CHARS: usize = 9;

/// A stable 32-bit fingerprint of the whole id, used only to end a truncated
/// lane.
///
/// Written out instead of taking `std::collections::hash_map::DefaultHasher`,
/// whose algorithm is deliberately unspecified: a toolchain upgrade would
/// compute a different name for the same employee and hand it an empty tree.
fn lane_fingerprint(text: &str) -> u32 {
    // FNV-1a.
    let mut hash: u32 = 0x811c_9dc5;
    for byte in text.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// The checkout an employee works in, named after the employee.
///
/// One derivation, two readers that cannot import each other: the run writes
/// this name into the system prefix so the model knows where its files land,
/// and the executor routes every request by it. Two spellings do not fail --
/// they put a turn's files in one tree while the prompt names another, and
/// every later read of that tree then reads a checkout the model was never
/// told about.
///
/// The id is folded to the byte set a worktree directory accepts, because agent
/// ids carry characters a path should not: a task's agent is spelled
/// `squad:<task>-<role>`, and a chat employee's id is a uuid.
///
/// A name that has to be shortened ends with a fingerprint of the full id.
/// Truncating alone aliases: two ids sharing their first `LANE_NAME_MAX_CHARS`
/// characters would land in one tree, and nothing downstream could tell that
/// two people were writing there.
pub fn worktree_lane(agent_id: &str) -> String {
    let folded: String = agent_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect();
    // A leading dot names a hidden directory, which the router refuses; taking
    // it off here means the refusal never has to be discovered as a failed
    // call, and the prompt still names the tree that will be used.
    let folded = folded.trim_start_matches('.').to_string();
    if folded.chars().count() <= LANE_NAME_MAX_CHARS {
        return format!("{WORKTREE_LANE_PREFIX}{folded}");
    }
    let kept: String = folded
        .chars()
        .take(LANE_NAME_MAX_CHARS - TRUNCATION_TAIL_CHARS)
        .collect();
    format!(
        "{WORKTREE_LANE_PREFIX}{kept}-{:08x}",
        lane_fingerprint(&folded)
    )
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

    /// 车道名只由 id 决定，且落在目录名允许的那一组字节里。
    ///
    /// 任务里的执行体是 `squad:<task>-<role>`，冒号不是目录名该有的字节；折完
    /// 之后名字仍然认得出是谁（任务号与角色都在），只是换了几个分隔符。
    #[test]
    fn a_lane_name_is_folded_from_the_agent_id() {
        assert_eq!(
            worktree_lane("squad:DSC-1A2B-generator"),
            "employee-squad-DSC-1A2B-generator"
        );
        assert_eq!(
            worktree_lane("kai.a9ab11fc"),
            "employee-kai.a9ab11fc",
            "合法的字节不该被动，读者要能一眼认出是谁"
        );
        // 前导点会被路由当成隐藏目录拒绝；去掉它，而不是让调用去撞那道拒绝。
        assert_eq!(worktree_lane(".hidden"), "employee-hidden");
        // 超长 id 截断在文件系统给单个名字的上限以内：车道既是目录名，也是
        // `work/<lane>` 这条分支名，两处都得装得下。
        let long = worktree_lane(&"x".repeat(400));
        assert!(
            long.len() + "work/".len() <= 255,
            "车道名 {} 字节，加上分支前缀就超过文件系统的单名上限了",
            long.len()
        );
        // 截断不许制造别名：两个 id 前 300 个字符一样，也必须落进两棵树。
        // 截断后系统上看不出这是两个人，两份改动会互相覆盖而且不报错。
        let shared = "y".repeat(300);
        let one = worktree_lane(&format!("{shared}-a"));
        let two = worktree_lane(&format!("{shared}-b"));
        assert_ne!(one, two, "两条长 id 落进了同一棵树：{one}");
        // 同一个 id 永远同一个名字：系统前缀与路由各算一次，不能算出两个结果。
        assert_eq!(worktree_lane("squad:t-role"), worktree_lane("squad:t-role"));
    }

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
