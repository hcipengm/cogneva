//! principle: feedback_no_hardcoding_config_surface
//!
//! 同一个 crate 里，同一个值写了两个名字。`cog-gateway` 把 Secret 里那个键叫
//! `git-ssh-private-key`，一处写 `SECRET_SSH_KEY`、另一处写
//! `SECRET_SSH_PRIVATE_KEY`，一个负责写入、一个负责读回；`cog-reflection` 的
//! `source` 标签同时有 `LABEL_SOURCE` 与 `BUDGET_SOURCE_LABEL` 两个名字，写侧的
//! 那一半用前一个、面板里那一半用后一个。两处都在同一个 crate 里，编译通过，
//! 谁也没拦着谁。
//!
//! 漂移的后果取决于这一对是「同一件东西」还是「碰巧同形」：
//!
//! - 同一件东西：改了一个名字的**值**，另一处照旧用旧值写/读，写入的那一端与
//!   读回的那一端悄悄分岔——Secret 键改名后读回的是空串，标签改名后 PromQL 回
//!   一个空向量，两个都和「这里本来就什么都没有」一模一样；
//! - 碰巧同形：两个值本来该各自演化，共用一个拼写让人不敢动其中任何一个，或者
//!   动了其中一个以为另一个会跟着动。
//!
//! 两种都要有人看过一眼，所以判据是**登记**而不是禁止：同一个 crate 里同一个
//! 值有两个以上名字时，要么让其中一个引用另一个（编译器接管），要么在
//! [`SAME_VALUE_DIFFERENT_THING`] 里写清它们为什么同形却不是同一件东西。
//!
//! 判据：
//!
//! 1. 生产代码里没有未登记的「一个值两个名字」；
//! 2. 登记不许过期：登记的那一对必须还在，否则删掉——留着一条不对应任何重复的
//!    许可，下一次真的写重了同一个值就没有判据拦了；
//! 3. 普查有下限：读到的声明数与 crate 数太少时本门自己红，免得「一个都没读
//!    到」与「一个重复都没有」是同一个读数。
//!
//! 判不了什么：
//!
//! - 值的**含义**。`"ok"` 这一组里 `OK` 是 HTTP 状态码、`READ_OK` 是读数结局，
//!   本门只知道它们同形；
//! - 别名（`const X: &str = crate::Y;`）不算声明，它本身就是「只留一处」的答案；
//! - 跨 crate 的同值异名不在这里判：同一个词在不同的 crate 里很可能是两件事，
//!   强行合并会把两族读数焊在一起。跨 crate 的那一面按**名字**判，见
//!   `word_constants_with_the_same_name_say_the_same_word_test`；
//! - 只认单行的 `const NAME: &str = "value";`。值写在下一行的声明读不到，而这种
//!   写法在树上没有（实测 0 处），一旦出现会体现为普查数下降。
//!
//! 实测（2026-10-27，全部 crate 的 `src/`）：733 个源文件、845 个短值声明；
//! 连测试夹具一起读是 29 组同值异名，只看生产代码是 27 组。落地当天合并了
//! 两组真重复（Secret 里那个 SSH 键、`cog-reflection` 的 `source` 标签），
//! 其余 27 组逐条登记在 [`SAME_VALUE_DIFFERENT_THING`] 里。cog-agent 的 `state`
//! 与 `role` 是后来把指标标签名收进常量时才出现的第 28、29 组：标签键与检查点键，
//! 同形不同事。
//!
//! 消融（真树）：往 `crates/cog-storage/src/` 放一个只写着
//! `const ABLATION_PROBE: &str = "consumed";` 的文件——红，点名 cog-storage 与
//! `ABLATION_PROBE, INBOX_ORPHAN_STATE_CONSUMED`；删掉它——绿。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::routes::{repo_root, walk};
use common::rust_production::production_text;

/// 同一个 crate 里同值异名、但不是同一件东西的那些组。
///
/// 每一条写的是「为什么它们同形却不是一件事」。没有写进这里的重复一律当缺陷：
/// 要么合并，要么在这里说明理由。
const SAME_VALUE_DIFFERENT_THING: &[(&str, &str, &str)] = &[
    (
        "cog-agent",
        "role",
        "指标上的 `role` 标签与检查点 JSON 里的 `role` 键是两个通道：前者是别人\
         按它选读数的一格，后者是重启后读回自己以哪个角色在跑。合并会让改一个\
         指标标签去动检查点格式",
    ),
    (
        "cog-agent",
        "state",
        "指标上的 `state` 标签与检查点 JSON 里的 `state` 键是两个通道：前者是\
         别人按它选读数的一格，后者是重启后读回自己读到哪儿的载荷。合并会让\
         改一个指标标签去动检查点格式",
    ),
    (
        "cog-collaboration",
        "dropped",
        "看板镜像的一次投放被丢掉、ralph 历史的一次写入被丢掉：两个通道各自的\
         「丢弃」结局，落在两族的同一格上，谁也不读对方的",
    ),
    (
        "cog-collaboration",
        "not_asked_disabled",
        "独立评审没被问、主持人没被问：两条路径各自的结局，落在同一格上",
    ),
    (
        "cog-core",
        "cogneva",
        "二进制名、默认命名空间、主容器名、主 Deployment 名与工作负载名是五个\
         不同的东西：改二进制名不该牵动命名空间，改容器名不该牵动 Deployment",
    ),
    (
        "cog-core",
        "decision",
        "任务类型（这一行怎么被认出来）与载荷键（这一行把内容放哪）是两个通道，\
         读写两侧各读同一个声明即可；取值相同是设计使然，合并不带来约束",
    ),
    (
        "cog-core",
        "decision_event",
        "同上：任务类型与载荷键是两个通道，不是同一个词",
    ),
    (
        "cog-core",
        "face",
        "同上：任务类型与载荷键是两个通道，不是同一个词",
    ),
    (
        "cog-core",
        "handover",
        "同上：任务类型与载荷键是两个通道，不是同一个词",
    ),
    (
        "cog-core",
        "vote",
        "同上：任务类型与载荷键是两个通道，不是同一个词",
    ),
    (
        "cog-gateway",
        "ok",
        "`OK` 是接口错误码里的成功格，`READ_OK` 是集群读数的一条结局；\
         两个词汇各自取值，同形而已",
    ),
    (
        "cog-memory",
        "memory_decay",
        "循环名（指标上的 `loop` 标签值）与租约角色名是两个通道：改名不该\
         牵动另一个人手里的租约",
    ),
    (
        "cog-orchestrator",
        "backup_runs",
        "同上：循环名与租约角色名是两个通道",
    ),
    (
        "cog-orchestrator",
        "change_ledger",
        "循环名、租约角色名与派发任务类型是三个通道；同一个循环在这三处各\
         出现一次是设计使然",
    ),
    (
        "cog-orchestrator",
        "governance_decision_close",
        "同上：循环名与租约角色名是两个通道",
    ),
    (
        "cog-orchestrator",
        "governance_face_drift",
        "同上：循环名与租约角色名是两个通道",
    ),
    (
        "cog-orchestrator",
        "orchestrator_sop_dispatch",
        "同上：循环名与租约角色名是两个通道",
    ),
    (
        "cog-orchestrator",
        "principle_ledger",
        "循环名、租约角色名与读数来源是三个通道：同一个循环在三处各出现一次\
         是设计使然",
    ),
    (
        "cog-orchestrator",
        "replan",
        "改动编号前缀、任务类型与载荷键是三个通道；任务类型与载荷键那一对\
         与本仓别处同形，前缀是另一件事",
    ),
    (
        "cog-orchestrator",
        "resource_ledger",
        "同上：循环名与租约角色名是两个通道",
    ),
    (
        "cog-reflection",
        "unmeasured",
        "`DIM_UNMEASURED` 是资源维度那一格的取值，`OUTCOME_UNMEASURED` 是读数\
         结局那一格的取值：两族读数各自的「没量过」，谁也不读对方的",
    ),
    (
        "cog-supervisor",
        "autonomous_event_loop",
        "循环名（指标上的 `loop` 标签值）与消息总线上的消费者名是两个通道：\
         改名不该让另一个静默变成「这个消费者从没跑过」",
    ),
    (
        "cog-supervisor",
        "supervisor_alert_store",
        "同上：循环名与消费者名是两个通道",
    ),
    (
        "cog-supervisor",
        "supervisor_event_aggregator",
        "同上：循环名与消费者名是两个通道",
    ),
    (
        "cogneva",
        "/workspace",
        "两个镜像各自的容器布局：官方判分脚本落点的那个 `/workspace` 与 pack\
         镜像的工作根同形，但改一个镜像的布局不该牵动另一个",
    ),
    (
        "cogneva",
        "cogneva",
        "命令行默认命名空间与 Helm 发布里的服务名是两个东西：改服务名不该\
         牵动命名空间",
    ),
    (
        "cogneva",
        "unknown",
        "计划读数里「说不清」与修复读数里「说不清」是两族各自的空格",
    ),
];

/// 普查下限。低于这里，本门自己红：读不到与没有是同一个读数。
const MIN_DECLARATIONS: usize = 500;
const MIN_CRATES: usize = 15;

/// 一个 `const NAME: &str = "value";` 声明。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Declaration {
    krate: String,
    name: String,
    value: String,
}

/// 短值：标签键、任务类型、Secret 键这类词，而不是整句文案或有分隔符的标识。
///
/// 判据是「一个词」的形状：小写字母、数字与 `_ . - /`，不超过 40 字节。整句文案
/// 与带空格的字符串不进这条判据——它们同值异名时，两处的措辞差异本身就是信号，
/// 不需要本门去登记。
fn is_word(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 40
        && value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-' | b'/')
        })
}

/// 一行里的声明，`None` 表示这行不是（或读不全）声明。
fn declaration_on(line: &str) -> Option<(String, String)> {
    let at = line.find("const ")?;
    // 注释掉的声明不是声明：`// const X: &str = "y";` 被算进来，一处注掉的旧
    // 声明就会让本门报一个树上并不存在的重复。
    if line[..at].contains("//") {
        return None;
    }
    let rest = &line[at + "const ".len()..];
    let (name, tail) = rest.split_once(':')?;
    let name = name.trim();
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return None;
    }
    let tail = tail.trim_start();
    let tail = tail
        .strip_prefix("&'static str")
        .or_else(|| tail.strip_prefix("&str"))?;
    let tail = tail.trim_start().strip_prefix('=')?.trim_start();
    let tail = tail.strip_prefix('"')?;
    let end = tail.find('"')?;
    let value = &tail[..end];
    // 转义与别名（`crate::X`）都不是这个词面判据能读的形状。
    if value.contains('\\') || value.contains("::") {
        return None;
    }
    Some((name.to_string(), value.to_string()))
}

/// 一棵树里所有生产代码的声明。
fn declarations(root: &Path) -> Vec<Declaration> {
    let crates_dir = root.join("crates");
    let Ok(entries) = std::fs::read_dir(&crates_dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let krate = entry.file_name().to_string_lossy().into_owned();
        let src = entry.path().join("src");
        if !src.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        walk(&src, &["rs"], &mut files);
        files.sort();
        for path in files {
            let text = common::routes::read(&path);
            for line in production_text(&text).lines() {
                let Some((name, value)) = declaration_on(line) else {
                    continue;
                };
                if !is_word(&value) {
                    continue;
                }
                out.push(Declaration {
                    krate: krate.clone(),
                    name,
                    value,
                });
            }
        }
    }
    out
}

/// 同一 crate 里同一个值被写成了不止一个名字的那些组，以及读过几处声明、几个 crate。
fn duplicates(root: &Path) -> (Vec<(String, String, BTreeSet<String>)>, usize, usize) {
    let declared = declarations(root);
    let krates: BTreeSet<&str> = declared.iter().map(|d| d.krate.as_str()).collect();
    let mut by_group: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for declaration in &declared {
        by_group
            .entry((declaration.krate.clone(), declaration.value.clone()))
            .or_default()
            .insert(declaration.name.clone());
    }
    let dups = by_group
        .into_iter()
        .filter(|(_, names)| names.len() > 1)
        .map(|((krate, value), names)| (krate, value, names))
        .collect();
    (dups, declared.len(), krates.len())
}

fn registered(krate: &str, value: &str) -> bool {
    SAME_VALUE_DIFFERENT_THING
        .iter()
        .any(|(k, v, _)| *k == krate && *v == value)
}

#[test]
fn one_value_has_one_name_inside_its_crate() {
    let root = repo_root();
    let (dups, declared, krates) = duplicates(&root);
    assert!(
        declared >= MIN_DECLARATIONS,
        "只读到 {declared} 处短值声明（下限 {MIN_DECLARATIONS}）：读得太少，\
         「没有重复」就是个空承诺"
    );
    assert!(
        krates >= MIN_CRATES,
        "只读到 {krates} 个 crate 的源码（下限 {MIN_CRATES}）"
    );
    let unregistered: Vec<String> = dups
        .iter()
        .filter(|(krate, value, _)| !registered(krate, value))
        .map(|(krate, value, names)| {
            format!(
                "  {krate} 里 {value:?} 写成了 {} 个名字：{}",
                names.len(),
                names.iter().cloned().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    assert!(
        unregistered.is_empty(),
        "这些值在同一个 crate 里各写了不止一遍。留下重复，改一处不会让另一处红：\n{}\n\
         要么让其中一处引用另一处（同一个 crate 里引用就是编译器接管），要么在 \
         SAME_VALUE_DIFFERENT_THING 里写清为什么它们同形却不是同一件东西：",
        unregistered.join("\n")
    );
}

#[test]
fn a_registration_that_no_longer_matches_a_duplicate_is_stale() {
    let root = repo_root();
    let (dups, _, _) = duplicates(&root);
    let stale: Vec<&str> = SAME_VALUE_DIFFERENT_THING
        .iter()
        .filter(|(krate, value, _)| {
            !dups
                .iter()
                .any(|(k, v, names)| k == krate && v == value && names.len() > 1)
        })
        .map(|(_, value, _)| *value)
        .collect();
    assert!(
        stale.is_empty(),
        "这些登记已经不再对应任何重复：{stale:?}。登记是给一处现场发的许可，\
         现场没了许可就该收回——留着它，下一次有人真的写重了同一个值就没有判据拦了。"
    );
}

/// 对照：同一个 crate 里同值异名必须被报出来。少了这条，`duplicates` 哪天退化成
/// 「什么都读不出来」，上面那条判据仍然全绿。
#[test]
fn a_duplicate_inside_one_crate_is_reported() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("crates/cog-x/src");
    std::fs::create_dir_all(&src).expect("mkdir");
    std::fs::write(
        src.join("lib.rs"),
        "pub const SECRET_SSH_KEY: &str = \"git-ssh-private-key\";\n\
         pub(crate) const SECRET_SSH_PRIVATE_KEY: &str = \"git-ssh-private-key\";\n",
    )
    .expect("write");
    let (dups, declared, krates) = duplicates(tmp.path());
    assert_eq!(declared, 2);
    assert_eq!(krates, 1);
    assert_eq!(dups.len(), 1, "{dups:?}");
    assert_eq!(dups[0].1, "git-ssh-private-key");
    assert_eq!(
        dups[0].2.iter().cloned().collect::<Vec<_>>(),
        vec!["SECRET_SSH_KEY", "SECRET_SSH_PRIVATE_KEY"]
    );
}

/// 对照：跨 crate 的同值异名不在这里报——那是另一套词汇，合并会把两族读数焊在一起。
#[test]
fn the_same_word_in_two_crates_is_not_reported_here() {
    let tmp = tempfile::tempdir().expect("tempdir");
    for (krate, name) in [("cog-a", "SOURCE_LABEL"), ("cog-b", "LABEL_SOURCE")] {
        let src = tmp.path().join(format!("crates/{krate}/src"));
        std::fs::create_dir_all(&src).expect("mkdir");
        std::fs::write(
            src.join("lib.rs"),
            format!("pub const {name}: &str = \"source\";\n"),
        )
        .expect("write");
    }
    let (dups, declared, _) = duplicates(tmp.path());
    assert_eq!(declared, 2);
    assert!(dups.is_empty(), "{dups:?}");
}

/// 对照：注释掉的声明不是声明。少了这条，一处注掉的旧声明会让本门报一个树上
/// 并不存在的重复，而「报了但找不到」比不报更费人。
#[test]
fn a_commented_out_declaration_is_not_a_declaration() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("crates/cog-x/src");
    std::fs::create_dir_all(&src).expect("mkdir");
    std::fs::write(
        src.join("lib.rs"),
        "// const OLD_NAME: &str = \"consumed\";\n\
         pub const INBOX_ORPHAN_STATE_CONSUMED: &str = \"consumed\";\n",
    )
    .expect("write");
    let (dups, declared, _) = duplicates(tmp.path());
    assert_eq!(declared, 1);
    assert!(dups.is_empty(), "{dups:?}");
}

/// 对照：测试夹具里的重复不算生产代码。
#[test]
fn a_duplicate_inside_a_test_module_is_not_production() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("crates/cog-x/src");
    std::fs::create_dir_all(&src).expect("mkdir");
    std::fs::write(
        src.join("lib.rs"),
        "pub const OUTCOME_LABEL: &str = \"outcome\";\n\
         #[cfg(test)]\n\
         mod tests {\n\
             const OUTCOME_LABEL_AGAIN: &str = \"outcome\";\n\
         }\n",
    )
    .expect("write");
    let (dups, declared, _) = duplicates(tmp.path());
    assert_eq!(declared, 1);
    assert!(dups.is_empty(), "{dups:?}");
}
