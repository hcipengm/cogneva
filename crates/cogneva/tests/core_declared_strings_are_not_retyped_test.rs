//! principle: feedback_no_hardcoding_config_surface
//! `cog_core` is where a string that more than one crate has to agree on is
//! declared: a route, an env name, a config pointer, a metric, a header, a
//! component. Declaring it there is only half the job — the other half is that
//! nobody types it again. A second copy does not fail when the first one
//! changes: the reader of the old string keeps looking for something that is no
//! longer written, and every one of those readers answers an absence the same
//! way it answers "nothing happened".
//!
//! So the values are read out of the declaring crates at test time and every
//! other crate's production code is searched for them. A site that legitimately
//! names a *different* thing with the same spelling is listed below with the
//! reason; a listed site that stops existing is reported too, because an
//! exemption outliving its site is a licence nobody reviewed.
//!
//! 实测（2026-10-27）：声明侧 137 个带分隔符的字符串；加固前全树有 22 处
//! 重写，本轮把其中 6 处改回读声明，其余按「同形不同物」逐条登记在 EXEMPT。
//!
//! 两种拼法——整字面量与带内标记——各是一道 [`Wall`]，共用一个 [`Report`]：
//! 读法、减法与过期判据只有一份，在 `common::declared_once`。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::common;

use common::declared_once::{Declared, Exemption, Report, Site, Wall};
use common::routes::repo_root;

use common::rust_production::{production_sources, relative_name};

/// Where the shared strings are declared.
///
/// The contract crate is the usual home, but not the only one: a crate that
/// owns a format gives it one declaration and lets everyone else read it from
/// there — the day key a rotating log file is named with is declared in the
/// storage crate, beside the writer and the parser that had to agree on it.
/// Any crate that declares a value is skipped *for that value only*: it is the
/// owner, so it may write the word; the rest of the workspace may not.
const DECLARATION_DIRS: &[&str] = &["crates/cog-core/src", "crates/cog-storage/src"];

const SUBJECT_LITERAL: &str = "声明过的字符串";

/// What a red means: a rename reaches one copy and not the other, and the
/// reader of the old spelling answers an absence exactly like "nothing
/// happened".
const WHY_LITERAL: &str = "只改一处时不会报错：读旧字符串的那一侧照旧去找一个再也不出现的东西，\
     而「没找到」与「本来就没有」在读数上同形。";

const SUBJECT_MARKER: &str = "带内标记";

/// A marker is the word a producer *prepends* to a reason. The whole-literal
/// law cannot see it; two copies that disagree turn a refusal into a retry.
const WHY_MARKER: &str = "生产者与读取者各写一遍时，改一处另一处照旧编译：拒绝被当成可重试的故障、\
     终止性失败被排进重试，两种都读作「什么都没有失败」。";

/// The crate a repo-relative path belongs to (`crates/cog-core/src/x.rs` ->
/// `crates/cog-core`).
fn owning_crate(file: &str) -> &str {
    match file.split_once('/') {
        Some((_, rest)) => match rest.split_once('/') {
            Some((krate, _)) => {
                let end = "crates/".len() + krate.len();
                &file[..end]
            }
            None => file,
        },
        None => file,
    }
}

/// A site that spells a declared value for a reason of its own.
///
/// Most of these name something that is not the declared string: a field of a
/// structured payload, a container name inside a pod, a command a person types.
/// Reading the declaration there would be a coupling nobody asked for, which is
/// why they are written down rather than quietly skipped.
///
/// The build-time names are the exception: same thing, written by a program
/// that cannot read the declaration. A build script and the compile that reads
/// its `env!` output are two programs; linking the contract crate into either
/// would put its whole dependency tree in front of every build. The spelling is
/// still held — the env-name census pins each of those files by name — so a
/// rename turns both gates red rather than one.
const EXEMPT: &[Exemption] = &[
    Exemption::new(
        "crates/cog-agent/src/agent_kernel/runtime.rs",
        "guardrail_blocked",
        "错误载荷里的布尔字段名与错误分类标签，不是拼进 reason 文本的那个前缀；\
         理由前缀由 cog_core::contract::outcome 自己读",
    ),
    Exemption::new(
        "crates/cog-collaboration/src/observable.rs",
        "upstream_unavailable",
        "自审跳过原因这一族自己的取值，与失败原因前缀同形但由跳过面消费",
    ),
    Exemption::new(
        "crates/cog-gateway/src/llm_admin.rs",
        "security-gateway",
        "写回给向导的出处字段，说明这份配置是谁落的盘",
    ),
    Exemption::new(
        "crates/cog-gateway/src/llm_admin.rs",
        "llm_config",
        "审计的类别名，回答「这次改动落在哪份配置上」；那个同形声明是池里一个上游的\
         来源字段。两者同形不同物：跟着改会让账上的旧类别从此读不出来",
    ),
    Exemption::new(
        "crates/cog-reflection/src/config.rs",
        "security-gateway",
        "Pod 里那个容器的名字，不是工作负载的组件标签",
    ),
    Exemption::new(
        "crates/cog-reflection/src/config.rs",
        "sandbox-executor",
        "同上：容器名",
    ),
    Exemption::new(
        "crates/cogneva/src/cli.rs",
        "security-gateway",
        "命令行子命令名，是人敲的字符串，跟着标签改名会把脚本打断",
    ),
    Exemption::new(
        "crates/cogneva/src/cli.rs",
        "sandbox-executor",
        "同上：子命令名",
    ),
    Exemption::new(
        "crates/cogneva/src/main.rs",
        "cogneva-backup-",
        "子命令名 `cogneva-backup-drill`，是 argv 里那个词：它与备份包名的前缀同形，\
         但没有人按它去找一个包。跟着包名一起改会把运维脚本和文档里敲的那个词打断，\
         而包名前缀的读者（发布目录与恢复路径）根本看不到子命令表",
    ),
    Exemption::new(
        "crates/cogneva/src/backup.rs",
        "cluster-registry",
        "换机损失清单里的一行，写的是这一行的主题而不是选择器",
    ),
    Exemption::new(
        "crates/cog-gateway/src/security_gateway.rs",
        "%Y-%m-%d",
        "解析调用方送进来的时间戳，是接受两种外部写法，不是我们自己写出去的名字",
    ),
    Exemption::new(
        "crates/cog-gateway/src/backup_admin.rs",
        "cogneva-backup-",
        "导出 Job 的对象名（`cogneva-backup-export-<毫秒>`），不是包文件名；\
         它由 apiserver 收下，没有第二个读者去按这个名字找东西",
    ),
    Exemption::new(
        "crates/cogneva/src/backup_crypto.rs",
        "cogneva-backup-",
        "HKDF 的 info 域分隔串，已经烤进盘上每一个包的密钥派生里；\
         它不是包名，跟着包名一起改会让已有的包再也解不开",
    ),
    Exemption::new(
        "crates/cogneva/src/work.rs",
        "external-worker",
        "`HOSTNAME` 读不到时给单元起的显示名，不是注册用的角色字段；\
         那条构造里角色读的是常量，显示名改了只换一个标签",
    ),
    Exemption::new(
        "crates/cog-llm/src/model.rs",
        "openai-responses",
        "`ApiType::OpenAIResponses` 的序列化词，写它的是 `#[serde(rename = ...)]` 属性——\
         那个位置只接受字面量。它与 `cog_core::contract::llm::RESPONSES_API_STYLE` 同物，\
         其余读写点（`plugin.rs` 的分派与 `provider/responses.rs` 的报文标签）已改回读\
         声明，只剩这一处拼写；两者分家由 `model.rs` 的 \
         `the_responses_serialisation_word_is_the_declared_style` 判红",
    ),
    Exemption::new(
        "crates/cogneva/build.rs",
        "COGNEVA_GIT_REVISION",
        "构建期名字的写这一侧：构建脚本是宿主程序，链接 cog-core 会把整棵契约\
         依赖树编到每一次构建前面。名字在这里声明一次、经 `cargo:rustc-env`\
         注入，读回它的是 build_info.rs 那两处 `env!` 字面量——三处任一改名，\
         这两条豁免与构建期名字的普查同时过期",
    ),
    Exemption::new(
        "crates/cogneva/build.rs",
        "COGNEVA_VERSION_ID",
        "同上：构建脚本注入、编译期读回，两个程序各自需要这个名字",
    ),
    Exemption::new(
        "crates/cogneva/src/build_info.rs",
        "COGNEVA_GIT_REVISION",
        "构建期名字：`env!` 只接受字面量，读它的是编译器而不是运行期，\
         所以这一处必须自己拼一遍；写它的是 `build.rs`（另一个程序），\
         把名字换成读 `cog_core::env` 的常量在这里编译不过",
    ),
    Exemption::new(
        "crates/cogneva/src/build_info.rs",
        "COGNEVA_VERSION_ID",
        "同上：`env!` 的字面量要求",
    ),
    Exemption::new(
        "crates/cog-storage/src/postgres/pge_stage_store.rs",
        "change_identity",
        "表里的列名，读它的是 SQL（DDL、插入列表、`try_get` 都按列名走），\
         与任务输入键 `cog_core::contract::pge_stage::INPUT_CHANGE_IDENTITY` 同形\
         却不同物：列名改了要迁表，输入键改了只改派发那一侧，改名时两处不必同步",
    ),
    Exemption::new(
        "crates/cog-collaboration/src/squad/ralph/ralph_loop.rs",
        "ralph_iteration",
        "喂给模型的上下文键名，不是那个事件类型：读它的是提示模板，占位符在模板\
         那一侧，改事件名时跟着改只会让模板安静地取不到值——模板不报错，它把占位符\
         当成空串渲染，循环照旧跑完",
    ),
];

/// A value worth holding to one spelling: long enough and shaped like a path,
/// an env name, a metric, a header or a label rather than a word two unrelated
/// call sites may both use.
fn structural(value: &str) -> bool {
    value.len() >= 8
        && value
            .chars()
            .any(|c| matches!(c, '/' | ':' | '.' | '_' | '-'))
}

/// One `pub const NAME: &str = "value"` in a declaring crate, with the file
/// that writes it.
struct Declaration {
    file: String,
    name: String,
    value: String,
}

/// The `pub const NAME: &str = "value"` declarations in the declaring crates.
fn declarations(root: &Path) -> Vec<Declaration> {
    let mut files: Vec<PathBuf> = production_sources(root)
        .into_iter()
        .filter(|path| {
            DECLARATION_DIRS
                .iter()
                .any(|dir| path.starts_with(root.join(dir)))
        })
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "声明目录 {DECLARATION_DIRS:?} 下一个源文件都没有：这些值没人声明了，本门没有题目"
    );
    let mut out = Vec::new();
    for path in files {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, &path);
        for line in text.lines() {
            let Some(at) = line.find("pub const ") else {
                continue;
            };
            let rest = &line[at + "pub const ".len()..];
            let Some((name, tail)) = rest.split_once(':') else {
                continue;
            };
            if !tail.trim_start().starts_with("&str = \"") {
                continue;
            }
            let Some(start) = tail.find('"') else {
                continue;
            };
            let Some(end) = tail[start + 1..].find('"') else {
                continue;
            };
            out.push(Declaration {
                file: file.clone(),
                name: name.trim().to_string(),
                value: tail[start + 1..start + 1 + end].to_string(),
            });
        }
    }
    out
}

/// A name that declares an in-band marker: a wire word a producer *prepends* to
/// a reason rather than one it writes on its own.
///
/// These need their own census. The whole-literal wall looks for the value as a
/// complete literal, which is the right question for a route or a label; a
/// marker at its producer is always followed by the explanation it labels, so a
/// producer that retyped it never shows up there.
fn marker_name(name: &str) -> bool {
    name.ends_with("_PREFIX") || name.ends_with("_MARKER")
}

/// The wall over whole literals: one declaration per value, owned by the crate
/// that declares it.
fn wall_over_whole_literals(root: &Path) -> Wall<'static> {
    let mut by_value: BTreeMap<String, Declared> = BTreeMap::new();
    for declaration in declarations(root) {
        if !structural(&declaration.value) {
            continue;
        }
        by_value
            .entry(declaration.value.clone())
            .or_insert_with(|| {
                Declared::new(declaration.name, declaration.value)
                    .owned_by(owning_crate(&declaration.file))
            });
    }
    Wall::new(SUBJECT_LITERAL, WHY_LITERAL)
        .declared(by_value.into_values().collect())
        .floor(100)
        .ceiling(2000)
}

/// The wall over in-band markers: a prefix spell, owned by the one file that
/// writes the declaration.
fn marker_wall(root: &Path) -> Wall<'static> {
    let mut by_value: BTreeMap<String, Declared> = BTreeMap::new();
    for declaration in declarations(root) {
        if !marker_name(&declaration.name) || !structural(&declaration.value) {
            continue;
        }
        by_value
            .entry(declaration.value.clone())
            .or_insert_with(|| {
                Declared::new(declaration.name, declaration.value)
                    .as_prefix()
                    .owned_by(declaration.file)
            });
    }
    Wall::new(SUBJECT_MARKER, WHY_MARKER)
        .declared(by_value.into_values().collect())
        .floor(4)
        .ceiling(2000)
}

/// Both walls' sites, so an exemption can be stale against either spelling.
fn both_walls(root: &Path) -> (Vec<Site>, Vec<Site>) {
    (
        wall_over_whole_literals(root).sites(root),
        marker_wall(root).sites(root),
    )
}

#[test]
fn every_shared_string_is_spelled_in_the_place_it_is_declared() {
    let root = repo_root();
    let (literal, marker) = both_walls(&root);
    let mut all = literal.clone();
    all.extend(marker.iter().cloned());
    Report::new(SUBJECT_LITERAL, WHY_LITERAL, &literal, EXEMPT)
        .staleness_against(&all)
        .assert_clean();
    Report::new(SUBJECT_MARKER, WHY_MARKER, &marker, EXEMPT)
        .staleness_against(&all)
        .assert_clean();
}

/// 声明 crate 里同一个值被声明成两个名字时，这两个名字各自代表什么。
///
/// 只有两种可能：它们说的是同一件东西（那就该只留一处，另一处改成读它），或者
/// 只是碰巧同形（登记理由）。无论哪种，重复本身都得有人看过一眼——没人登记时，
/// 改其中一个名字不需要动另一个，而两次「成功」的改动加在一起，就是同一个文件
/// 在部署里叫两个名字。
const SAME_WORD_DIFFERENT_THING: &[(&str, &str)] = &[
    (
        "cogneva",
        "二进制名、默认命名空间、主容器名、主 Deployment 名与工作负载名是五个不同的\
         东西：改二进制名不该牵动命名空间，改容器名不该牵动 Deployment",
    ),
    (
        "face",
        "任务类型（这一行怎么被认出来）与载荷键（这一行把内容放哪）是两个通道，\
         读写两侧各读同一个声明即可；取值相同是设计使然，合并不带来约束",
    ),
    ("handover", "同上：任务类型与载荷键是两个通道，不是同一个词"),
    ("decision", "同上：任务类型与载荷键是两个通道，不是同一个词"),
    ("vote", "同上：任务类型与载荷键是两个通道，不是同一个词"),
    (
        "decision_event",
        "同上：任务类型与载荷键是两个通道，不是同一个词",
    ),
    (
        "empty",
        "两族读数各自的「空」：检索结果落在 empty 格，收件箱孤儿状态也落在 empty 格。\
         它们是两个指标家族里的标签值，谁也不读对方的",
    ),
    (
        "state",
        "两族读数各自的「状态」：收件箱孤儿的状态与连接池里连接的状态。它们是两个\
         指标家族里的标签名，各自的闭集不同，改一族不牵动另一族；合并会让「池多一种\
         状态」看起来像是「孤儿多一种状态」",
    ),
];

/// 一个值在声明 crate 里只有一个名字。
///
/// 跨 crate 的判据只看得见「别的 crate 重写了这个名字」，看不见「声明 crate
/// 自己写了两遍」：`cogneva.json` 曾经同时是配置文件名、ConfigMap 的键与项目
/// 声明的文档名，三处各自写死的字面量谁也拦不住谁——改名可以只成功一处，而
/// 读那份文件的一侧照旧去找一个再也不出现的名字。
#[test]
fn a_value_has_one_name_inside_the_declaration_crate() {
    let root = repo_root();
    let mut names: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for declaration in declarations(&root) {
        names
            .entry(declaration.value)
            .or_default()
            .insert(declaration.name);
    }

    let unregistered: Vec<String> = names
        .iter()
        .filter(|(value, declared)| {
            declared.len() > 1
                && !SAME_WORD_DIFFERENT_THING
                    .iter()
                    .any(|(same, _)| *same == value.as_str())
        })
        .map(|(value, declared)| {
            format!(
                "  {value:?} 声明成了 {} 个名字：{}",
                declared.len(),
                declared.iter().cloned().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    assert!(
        unregistered.is_empty(),
        "这些值在声明 crate 里各写了不止一遍。留下重复，改一处不会让另一处红：\n{}\n\
         要么让其中一处读另一处，要么在 SAME_WORD_DIFFERENT_THING 里写清为什么\
         它们同形却不是同一件东西：",
        unregistered.join("\n")
    );

    let stale: Vec<&str> = SAME_WORD_DIFFERENT_THING
        .iter()
        .filter(|(value, _)| names.get(*value).is_none_or(|declared| declared.len() < 2))
        .map(|(value, _)| *value)
        .collect();
    assert!(
        stale.is_empty(),
        "这些登记已经不再对应任何重复：{stale:?}。登记是给一处现场发的许可，\
         现场没了许可就该收回——留着它，下一次有人真的写重了同一个值就没有判据拦了。"
    );
}
