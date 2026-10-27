//! principle: feedback_no_hardcoding_config_surface
//!
//! 指标标签的名字被重复声明成一个常量，而不是从一处导出：`OUTCOME_LABEL` 在
//! 八个文件里各写一遍，`DEPLOYMENT_LABEL` 在六个文件里各写一遍，
//! `SOURCE_LABEL` 在两个 crate 里各写一遍。写成常量本身是对的——总比在
//! `with_label("outcome", …)` 里散着拼好——但八份声明之间没有任何东西比较。
//!
//! 漂移的后果在读数面，而且是最难认出的那一种：改了一处，那一族指标就把自己
//! 归到一个别人不写的标签名下。按 `outcome="failed"` 汇总的面板读不到它，
//! PromQL 对匹配不到的标签一声不吭——空向量与「这一族什么都没发生」是同一个
//! 读数。
//!
//! 所以这里按名字判：**同名即同词**。两个 crate 里都叫 `OUTCOME_LABEL` 的常量，
//! 必须都写着 `"outcome"`。代码平台的词一样：`GITHUB_PLATFORM` 在网关与
//! `cog-github` 各声明一份，两份之间没有任何东西比较。
//!
//! 三条判据：
//!
//! 1. 同一族（见 [`word_family`]）的声明必须写着同一个词。族按去掉了 `LABEL_`
//!    前缀 / `_LABEL` 后缀的名字算，所以 `LABEL_OUTCOME` 与 `OUTCOME_LABEL` 是
//!    同一族——只比同名的话，这两拨声明各自内部一致、彼此差一个词，读数一样
//!    读成空向量；
//! 2. 生产代码的标签键位置**不许出现字符串字面量**：`with_label(…)`、
//!    `labels.insert/get/remove` 后面紧跟的那个键，只能是一个声明。不要求全树
//!    共用一个常量——每个写它的 crate 各自声明一份即可：同一拼写在别处很可能是
//!    另一件事，强行共用会把两族不相干的读数焊在一起。这条不留「没人声明过就
//!    不管」的口子：裸拼本身就是第二个拼法，第一个是它下一次被写进另一个文件
//!    的时候；
//! 3. 同一批词若被逐字写进 `HashMap::from([...])` / `BTreeMap::from([...])` 的
//!    **键**位，也是第二个拼法，照报。这里只认已经声明成常量的词：HTTP 头、JSON
//!    字段名、SQL 列名走的是同一个形状，它们是另一套词汇，门不替它们立规矩；
//! 4. 普查本身要有下限：读得太少、「没有矛盾」就是个空承诺。
//!
//! 判不了什么：
//!
//! - 标签名与它被用在哪个指标上对不对。把 `outcome` 记到 `state` 那一格，
//!   本门看不出来；
//! - 标签键拼进别的元组形状里（`slots.push(("outcome", …))`）、`.cell("outcome",
//!   …)` 这类测试辅助。实测把「任意元组」都算进来，会连 SQL 列名
//!   （`row.try_get("outcome")`）与 serde 字段名一起判成红，而它们确实是另一套
//!   词汇。**map 构造器的键**例外，已经判了：`HashMap::from([("outcome", …)])` /
//!   `BTreeMap::from([...])` 里键位上是本树已声明的词就报——判据是「这个词有名字」，
//!   没声明过的字符串在那个位置上太常见（HTTP 头、JSON 字段名），不判。这个补丁
//!   是树上真有一处漏网才写的：`cog-agent/src/archive.rs` 的 `tool_labels` 写端
//!   还是 `"tool"`，读端两处已经引用 `LABEL_TOOL`；
//! - 读侧写成 `self.base_labels.get("job")` 这种非 `labels.` 接收者的字面量键。
//!   同一个文件里写侧引用常量、读侧拼字面量时，两处读的是同一个键，改名不会
//!   出错，所以本门不管；但这也是「同一个词两种写法」，靠就地改齐（实测 16 个
//!   文件里有这种写法，已逐个改成引用常量）。
//!
//! 为什么只限名字命中 [`word_family`] 的 `&str` 常量：同名的 `&str` 常量在别处
//! **合法地**各不相同——`USAGE`、`CLAIM_ENV`、`ROOT_ENV`、`INSERT_SQL` 都是每个
//! 模块自己那一份，名字撞上而词不同是正当的（实测 30 组这样同名不同词）。标签名
//! 与平台名不是：它们在两处必须是同一个词，否则读数合不到一起、路由挑不中。
//!
//! 实测（2026-10-27）：生产代码里 81 个命中该判据的 `&str` 常量、41 个族。判据 2
//! 在门槛还是「三个以上 crate 声明过」时命中的两个词是 `outcome`（10 个 crate 各
//! 声明一次，21 处逐字拼写的生产代码，全部改成引用常量）与 `source`（`cog-core`、
//! `cog-extension`、`cog-reflection` 三处声明，7 处逐字拼写，同上）。`cog-reflection`
//! 的五个模块级 `OUTCOME_LABEL` 与网关的一处改成指向本 crate 常量的别名——编译器
//! 拴住的比门拴住的更早报错。
//!
//! 门槛放到「任何 crate 声明过」之后，工作面是一次量出来的：321 处逐字写进标签位
//! 的拼写、95 个词；其中 45 处落在 8 个已被声明过的词上，逐 crate 改成引用声明
//! （cog-agent、cog-collaboration、cog-core、cog-gateway、cog-memory、
//! cog-observability、cog-orchestrator、cog-reflection 各新增 1～5 个 `LABEL_*`
//! 常量，`cog-reflection` 另有 3 处改指向本 crate 已有的 `DIR_LABEL` / `KIND_LABEL` /
//! `STATE_LABEL`）。
//!
//! 剩下的 131 处在 67 个没人声明过的词上（`message` 13、`consumer` 7、`agent_id` 6、
//! `tier` 6、`resource` 5…），把口径改成「字面量一律不许」之后一次做完：14 个 crate
//! 各加一批 `LABEL_*` 常量（cog-observability 一次 29 个，cog-reflection 22 个），
//! 全部发射点改成引用。做完再量，生产代码里标签位上的字面量是 0——这是本门从
//! 「声明过的词不许裸拼」走到「裸拼本身就不许」的依据，不是反过来先立规矩再补现场。
//!
//! 消融（都在真实树上做，跑的是本门自己的测试二进制）：把 `cog-collaboration` 的
//! 一处 `with_label(crate::LABEL_OUTCOME, …)` 改回字面量，本门红并点名该文件与行号、
//! 列出十个已声明该词的 crate；改回绿。把 `cog-observability` 的 `LABEL_OUTCOME`
//! 改成 `"result"`，本门红并点名 `OUTCOME` 这一族的全部十处声明（归族判据生效）；
//! 改回绿。另外两处消融见此前：`cog-memory` 的 `OUTCOME_LABEL` 改 `"result"`
//! 红、`cog-gateway` 的 `GITEE_PLATFORM` 改 `"gitee2"` 红，改回均绿。
//!
//! 门槛降到「任何 crate 声明过」之后又消融了一次：把 `cog-agent` 的
//! `with_label(crate::LABEL_STATE, …)` 改回 `"state"`，本门红并点名
//! `crates/cog-agent/src/observable.rs` 第 684 行、列出声明了 `state` 的四个 crate
//! （cog-agent、cog-orchestrator、cog-reflection、cog-storage）；改回绿。同一棵树
//! 一测就还揪出一处漏网的（`cog-memory/src/ingestor.rs` 的 `"operation"`），已改。
//!
//! 口径收紧成「字面量一律不许」之后，两条分支各消融一次，探针放
//! `crates/bootstrap/src/zz_ablation_probe.rs`（不被任何模块引用，不进编译，门照样读得
//! 到）：第 3 行 `with_label("intent", …)` 红，点名文件与行号并列出声明了 `intent` 的
//! cog-github、cog-reflection；第 4 行 `with_label("nobody_declared_this_word", …)` 同样
//! 红，报错改成「还没有常量，先声明一个再引用」——旧口径下这一处是绿的，这正是收口
//! 要挡的那一类。删掉探针，同一棵树 15 个用例全绿。
//!
//! 扫描面按 O(词 × 文件) 重读过一次全部文件，实测这套测试耗时 66 秒；改成每个文件
//! 只解析一遍、再按词过滤之后同一台机器 6 秒，判据不变。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::routes::{repo_root, walk};
use common::rust_production::production_text;

/// 这个常量名属于哪一族词——同一族的声明必须写着同一个词。
///
/// 名字里带 `LABEL`：指标标签名。两个 crate 里都叫 `OUTCOME_LABEL` 的常量必须都
/// 写着 `"outcome"`，否则按旧名字汇总的面板读到一个空向量。
///
/// 名字以 `_PLATFORM` 结尾：代码平台的词。两个 crate 各声明一份，路由、存储的配置
/// 与上报都按它挑分支；改一处会让那条路径落到兜底分支上。
///
/// 名字以 `_TRAILER` 结尾：写进提交信息的 git trailer 键。写出端与读回端在两个
/// crate 里，读回端读不到就当作「这条提交没有 trailer」——提交照样落地，只是台账
/// 从此对不上。这一族不是假想的：`CHANGE_ID_TRAILER` 在被本门收进来之前就已经
/// 分成两半，一处写 `"Change-Id"`、一处写 `"Change-Id: "`，而没有任何东西在比较。
///
/// 标签族按**去掉了 `LABEL_` 前缀 / `_LABEL` 后缀的名字**归族：同一个词的两种命名
/// 顺序（`LABEL_OUTCOME` 与 `OUTCOME_LABEL`）是同一族，必须写着同一个词。不归一
/// 的话，树上那八处 `OUTCOME_LABEL` 与一处 `LABEL_OUTCOME` 互相看不见——两拨声明
/// 各自内部一致，彼此差一个词，一样是空向量。
///
/// 只判名字，是因为「这是同一个词的两份声明」在代码里没有别的形状可认。为什么不是
/// 所有同名 `&str` 常量：标签族之外同名不同词是正当的（见文件头实测）。为什么是
/// 后缀而不是 `PLATFORM` 子串：`PLATFORM_BASE_ENV`、`PLATFORM_REGISTRY_FILES` 这类
/// 前缀命名是各模块自己的环境变量名与路径，撞名而不同值是正当的。
fn word_family(name: &str) -> Option<&str> {
    if let Some(stem) = name.strip_prefix("LABEL_") {
        return Some(stem);
    }
    if let Some(stem) = name.strip_suffix("_LABEL") {
        return Some(stem);
    }
    if name.contains("LABEL") || name.ends_with("_PLATFORM") || name.ends_with("_TRAILER") {
        return Some(name);
    }
    None
}

/// 一段生产代码里所有命中 [`is_word_constant`]、类型是 `&str` 的常量。
///
/// 类型不是 `&str` 的跳过（`ALERT_LABEL_VALUE_MAX_CHARS: usize` 就是一个名字
/// 撞上的数常量，它不是一个词）。类型对、值读不出来的按错误返回，不静默跳过：
/// 一处漏读就是一处不比较，而「少读了一个」与「那个常量没有矛盾」在结果上
/// 分不开。
fn word_constants(source: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("const ") {
        let keyword = cursor + offset;
        let at = keyword + "const ".len();
        cursor = at;
        // 注释掉的声明不是任何一个词的发源地：看这一行在它之前有没有 `//`。
        // 这里不调 `strip_comments`——那把 `'` 当字符串引号，遇到 `&'static str`
        // 会把等号后面的字面量一起吞掉。
        let line_start = source[..keyword].rfind('\n').map_or(0, |p| p + 1);
        if source[line_start..keyword].contains("//") {
            continue;
        }
        let name: String = source[at..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        // 不是「`const` 后面跟一个名字」的那些出现：`as *const ()` 这样的类型
        // 转换、`macro_rules!` 里的 `pub const $ident`。它们不是声明，跳过。
        if name.is_empty() || word_family(&name).is_none() {
            continue;
        }
        let Some(colon) = source[at..].find(':').map(|o| at + o) else {
            return Err(format!("`{name}` 后面没有 `:`：读不出来按红处理"));
        };
        let Some(eq) = source[colon..].find('=').map(|o| colon + o) else {
            return Err(format!("`{name}` 那一行没有 `=`：读不出来按红处理"));
        };
        let ty = source[colon + 1..eq].trim();
        if ty != "&str" && ty != "&'static str" {
            continue;
        }
        let rest = source[eq + 1..].trim_start();
        if !rest.starts_with('"') {
            // 转抄另一个常量（`pub const CLAIM_LABEL: &str =
            // cog_core::claim_footprint::CLAIM_LABEL;`）：两边由编译器拴着，
            // 不可能各自漂移，所以它不贡献自己的词。其余形状读不出来，按红。
            let value: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                .collect();
            if value.contains("::") && !value.is_empty() {
                continue;
            }
            return Err(format!(
                "`{name}` 是 `&str` 常量，但值既不是字面量也不是另一个常量的路径\
                 （等号后面是 `{}`）：读不出来按红处理",
                rest.chars().take(16).collect::<String>()
            ));
        }
        let Some(close) = rest[1..].find('"').map(|o| o + 1) else {
            return Err(format!("`{name}` 的字面量没有收尾：读不出来按红处理"));
        };
        out.push((name, rest[1..close].to_string()));
    }
    Ok(out)
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// `crates/*/src` 下的每一个 Rust 文件。
fn sources(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(root.join("crates")) else {
        return files;
    };
    for entry in entries.flatten() {
        let src = entry.path().join("src");
        if src.is_dir() {
            walk(&src, &["rs"], &mut files);
        }
    }
    files.sort();
    files
}

/// 标签**键**位置的判据不区分「这个词有没有人声明过」。
///
/// 谁声明过只是报错时多说一句「引用哪个常量」，不是判不判的条件。裸拼的词只要
/// 出现在标签位上就算红：它本身就是第二个拼法，第一个是它下一次在另一个文件里被
/// 再拼一遍的时候——那时两处版本各写各的、改一处不会有任何报错，读数面读到的是
/// 一族空向量。声明的粒度也不要求全树共用一份：每个写它的 crate 各自声明即可，
/// 同一拼写在别处完全可能是另一件事，强行共用会把两族不相干的读数焊在一起。
///
/// 下面这四个是「标签**键**位置上的调用」——字面量紧跟在它们后面（中间允许换行
/// 与缩进），就是在标签位上把词拼了一遍，而不是引用声明。
const LABEL_KEY_MARKERS: [&str; 4] = [
    "with_label(",
    "labels.insert(",
    "labels.get(",
    "labels.remove(",
];

/// 一个文件里，被逐字写进标签键位置的词，连同它在文件里的行号。
///
/// 一次走完整个文件，而不是按词各扫一遍：词有几打、文件有上千个，按词遍历是
/// O(词 × 文件) 次解析，实测 66 秒；一次扫完再按词过滤，判据一样，一秒级。
///
/// 判不了什么：把标签键拼进一个 map 构造器的元组里（`HashMap::from([("outcome",
/// …)])`）、`slots.push(("outcome", …))` 这类写法，本门看不出来——它们的形状与
/// 普通的元组和 JSON 字段名一致，凡是把它们也算进来的判据，实测都会连 SQL 列名
/// （`row.try_get("outcome")`）与序列化字段一起判成红。
fn inline_label_sites(text: &str) -> Vec<(usize, String)> {
    let production = production_text(text);
    let numbers = production_line_numbers(text);
    let mut out = Vec::new();
    for (offset, _) in production.match_indices('"') {
        let before = &production[..offset];
        if !LABEL_KEY_MARKERS
            .iter()
            .any(|marker| before.trim_end().ends_with(marker))
        {
            continue;
        }
        let after = &production[offset + 1..];
        let Some(end) = after.find('"') else {
            continue;
        };
        let word = &after[..end];
        // 报的是文件里的行号，不是生产文本里的第几行：跳过测试模块会让两者错开
        // 一大截，照着一个对不上的行号去改，改中的是别人的行。
        let index = before.matches('\n').count();
        let line = numbers.get(index).copied().unwrap_or(index + 1);
        let site = (line, word.to_string());
        if !out.contains(&site) {
            out.push(site);
        }
    }
    out
}

/// 一个文件里，被逐字写成 `HashMap::from([...])` / `BTreeMap::from([...])` 条目
/// 键的词，连同它在文件里的行号。
///
/// 这一扫补的是 [`inline_label_sites`] 判不了的那个形状：标签集是在 map 构造器的
/// 元组里逐条拼出来的（`HashMap::from([("tool".to_string(), tool.to_string())])`）。
/// 它长得和 HTTP 头、JSON 字段名、SQL 列名一样，所以这里**只认已经在本树里声明成
/// 常量的词**——没声明过的词照旧归读者判断，但只要是某处 `LABEL_*` 的取值，它在
/// map 里裸写就是第二个拼法：读回端按常量取键，写端按字面量放键，改常量不会有
/// 任何报错，读数面读到的是一族空向量。实测这一形状在树上真有一处
/// （`cog-agent/src/archive.rs` 的 `tool_labels`，读端已经改成 `LABEL_TOOL` 而写端
/// 还是字面量），是本门在此之前漏掉的。
///
/// 只看每条目的**键**（元组的第一个位置），值里出现的字符串不算；字符串字面量
/// 整体跳过，所以值里带括号不会把层数算错。
fn map_literal_keys(text: &str) -> Vec<(usize, String)> {
    let production = production_text(text);
    let numbers = production_line_numbers(text);
    let bytes = production.as_bytes();
    let mut out = Vec::new();
    for marker in ["HashMap::from([", "BTreeMap::from(["] {
        for (start, _) in production.match_indices(marker) {
            // `([` 之后就是第一条目所在的层数。
            let mut depth = 2i32;
            let mut expecting_key = false;
            let mut i = start + marker.len();
            while i < bytes.len() {
                match bytes[i] as char {
                    '"' => {
                        let Some(end) = production[i + 1..].find('"') else {
                            break;
                        };
                        if expecting_key {
                            let word = production[i + 1..i + 1 + end].to_string();
                            let index = production[..i].matches('\n').count();
                            let line = numbers.get(index).copied().unwrap_or(index + 1);
                            let site = (line, word);
                            if !out.contains(&site) {
                                out.push(site);
                            }
                        }
                        expecting_key = false;
                        i += end + 2;
                        continue;
                    }
                    '(' => {
                        // 元组在构造器这一层打开：下一个非空字符若是字符串，它就是键。
                        expecting_key = depth == 2;
                        depth += 1;
                    }
                    ')' => depth -= 1,
                    ']' => {
                        depth -= 1;
                        if depth < 2 {
                            break;
                        }
                    }
                    c if !c.is_whitespace() => expecting_key = false,
                    _ => {}
                }
                i += 1;
            }
        }
    }
    out
}

/// 生产文本每一行在文件里的行号，与 `production_text` 的行一一对应。
///
/// `production_lines` 只给内容不给行号，所以按顺序在原文里走一遍：生产行是原文
/// 行的子序列，顺序一致，逐行对齐即可。
fn production_line_numbers(text: &str) -> Vec<usize> {
    let production = production_text(text);
    let file_lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    let mut cursor = 0usize;
    for line in production.lines() {
        while cursor < file_lines.len() && file_lines[cursor] != line {
            cursor += 1;
        }
        if cursor == file_lines.len() {
            break;
        }
        out.push(cursor + 1);
        cursor += 1;
    }
    out
}

fn crate_of(where_: &str) -> Option<&str> {
    where_.strip_prefix("crates/")?.split('/').next()
}

fn contradictions(root: &Path) -> Vec<String> {
    let mut complaints = Vec::new();
    let mut declarations: Vec<(String, String, String)> = Vec::new();
    let mut files: Vec<(String, String)> = Vec::new();

    for path in sources(root) {
        let where_ = relative(root, &path);
        let Ok(raw) = std::fs::read_to_string(&path) else {
            complaints.push(format!("  `{where_}` 读不出来：读不出来按红处理"));
            continue;
        };
        let text = production_text(&raw);
        match word_constants(&text) {
            Ok(found) => {
                for (name, word) in found {
                    declarations.push((name, word, where_.clone()));
                }
            }
            Err(complaint) => complaints.push(format!("  `{where_}`：{complaint}")),
        }
        files.push((where_, raw));
    }

    let mut by_family: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
    let mut crates_of_word: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (name, word, where_) in &declarations {
        let family = word_family(name).unwrap_or(name.as_str()).to_string();
        by_family
            .entry(family)
            .or_default()
            .entry(word.clone())
            .or_default()
            .push(format!("`{name}`（{where_}）"));
        if let Some(krate) = crate_of(where_) {
            crates_of_word
                .entry(word.clone())
                .or_default()
                .insert(krate.to_string());
        }
    }

    for (family, words) in &by_family {
        if words.len() < 2 {
            continue;
        }
        let spellings: Vec<String> = words
            .iter()
            .map(|(word, sites)| format!("`{word}`（{}）", sites.join("、")))
            .collect();
        complaints.push(format!(
            "  `{family}` 这一族被声明了 {} 次，写出了 {} 个不同的词：{}。\n    同一族的声明 \
             必须是同一个词：标签名不同则那一族指标归到别人不写的标签名下，读数面上与\
             「这一族什么都没发生」是同一个空向量；平台名不同则那条路径落到兜底分支；\
             trailer 键不同则读回端当作没有这条 trailer，台账从此对不上",
            words.values().map(Vec::len).sum::<usize>(),
            words.len(),
            spellings.join("；")
        ));
    }

    let declared: usize = declarations.len();
    if declared < 30 || by_family.len() < 20 {
        complaints.push(format!(
            "普查只读到 {declared} 个命中本门判据的 `&str` 常量、{} 个不同名字：读得太少，\
             本门判不了这一棵树，按红处理",
            by_family.len()
        ));
    }

    // 标签键位置上不许有字面量：写标签时只能引用某个声明。
    for (where_, raw) in &files {
        for (line, word) in inline_label_sites(raw) {
            let declared = crates_of_word
                .get(&word)
                .map(|krates| {
                    format!(
                        "这个词在本树里已经声明成常量（{}），引用它即可",
                        krates.iter().cloned().collect::<Vec<_>>().join("、")
                    )
                })
                .unwrap_or_else(|| {
                    "这个词在本树里还没有常量，先在本 crate 里声明一个再引用".into()
                });
            complaints.push(format!(
                "  `{where_}` 第 {line} 行把 `\"{word}\"` 直接写在标签位上。{declared}：\
                 两处各自拼一遍，改一处不会有任何报错，而读数面读到的是一族空向量"
            ));
        }
    }

    // 标签集在 map 构造器里逐条拼出来的形状：键位上是已经声明过的词，就是第二个
    // 拼法。只认声明过的词——没声明过的字符串在同一个位置上太常见（HTTP 头、
    // JSON 字段名），一律判红的话这条判据会被正当用法淹没。
    for (where_, raw) in &files {
        for (line, word) in map_literal_keys(raw) {
            let Some(krates) = crates_of_word.get(&word) else {
                continue;
            };
            complaints.push(format!(
                "  `{where_}` 第 {line} 行在 map 构造器里把 `\"{word}\"` 直接写成键。\
                 这个词已经声明成常量（{}）：读回端按常量取键、写端按字面量放键，\
                 改声明不会有任何报错，读数面读到的是一族空向量",
                krates.iter().cloned().collect::<Vec<_>>().join("、")
            ));
        }
    }

    complaints
}

#[test]
fn a_label_constant_declared_twice_says_the_same_word_both_times() {
    let complaints = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "同名常量必须写着同一个词；它们是各文件各写一遍的，编译器不比较，改一处不会有\
         任何报错。\n{}",
        complaints.join("\n")
    );
}

// ── 夹具：临时树里放几个 crate，逐条验证红与绿 ──

fn fixture_complaints(files: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    contradictions(dir.path())
        .into_iter()
        .filter(|c| !c.contains("普查只读到"))
        .collect()
}

const ONE: &str = "crates/one/src/a.rs";
const TWO: &str = "crates/two/src/b.rs";

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
        (TWO, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

#[test]
fn a_label_constant_renamed_in_one_crate_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
        (TWO, "pub const OUTCOME_LABEL: &str = \"result\";\n"),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("OUTCOME_LABEL") && c.contains("result")),
        "{complaints:?}"
    );
}

/// 一个读不出来的声明不能被跳过：跳过就等于不比较，而不比较与「没有矛盾」
/// 在结果上是同一个绿。
#[test]
fn a_declaration_that_is_not_a_literal_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const OUTCOME_LABEL: &str = pick_label();\n"),
        (TWO, "pub const OUTCOME_LABEL: &str = \"outcome\";\n"),
    ]);
    assert!(
        complaints.iter().any(|c| c.contains("OUTCOME_LABEL")),
        "{complaints:?}"
    );
}

/// 注释里的同名常量不算一次声明：它不是任何一个词的发源地。
#[test]
fn a_commented_out_declaration_does_not_count() {
    let complaints = fixture_complaints(&[(
        ONE,
        "// pub const OUTCOME_LABEL: &str = \"outcome\";\npub const X_LABEL: &str = \"x\";\n",
    )]);
    assert!(
        !complaints.iter().any(|c| c.contains("OUTCOME_LABEL")),
        "{complaints:?}"
    );
}

/// 转抄另一个常量的声明不是第二个词的来源：两边由编译器拴着，不可能各自漂移。
#[test]
fn a_re_export_of_another_constant_is_not_a_second_spelling() {
    let complaints = fixture_complaints(&[
        (
            ONE,
            "pub const CLAIM_LABEL: &str = crate::other::CLAIM_LABEL;\n",
        ),
        (TWO, "pub const CLAIM_LABEL: &str = \"claim\";\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

/// 三处独立声明了 `"outcome"`，第四处却把同一个词直接写进标签位：那不是第二次
/// 声明，而是没有引用任何声明，两者之间的比较也就无从谈起。
#[test]
fn a_shared_word_spelled_inline_in_a_label_slot_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const LABEL_OUTCOME: &str = \"outcome\";\n"),
        (TWO, "pub const LABEL_OUTCOME: &str = \"outcome\";\n"),
        (
            "crates/three/src/c.rs",
            "pub const LABEL_OUTCOME: &str = \"outcome\";\n\
             pub fn publish(m: &mut M) { m.with_label(\"outcome\", \"ok\"); }\n",
        ),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("\"outcome\"") && c.contains("crates/three/src/c.rs")),
        "{complaints:?}"
    );
}

/// 声明过一次就够了：一个 crate 把某个词写成常量，就已经把它认成了词汇，
/// 别处再逐字拼它就是第二个拼法。
#[test]
fn a_literal_label_key_is_reported_in_a_crate_that_declares_the_word_elsewhere() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const KIND_LABEL: &str = \"kind\";\n"),
        (
            "crates/three/src/c.rs",
            "pub fn publish(m: &mut M) { m.with_label(\"kind\", \"pod\"); }\n",
        ),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("\"kind\"") && c.contains("crates/three/src/c.rs")),
        "{complaints:?}"
    );
}

/// 没人声明过的词也一样报：裸拼本身就是第二个拼法——第一个拼法是它下一次被
/// 写进另一个文件的时候。判据不再问「有没有人给它起过名字」，只问「这里是不是
/// 拼了一遍」；本门也就在同一处告出门槛之外的现场该补什么（先声明，再引用）。
#[test]
fn a_literal_label_key_is_reported_even_when_nothing_declares_it() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const OTHER_WORD: &str = \"something_else\";\n"),
        (
            "crates/three/src/c.rs",
            "pub fn publish(m: &mut M) { m.with_label(\"intent\", \"x\"); }\n",
        ),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("\"intent\"") && c.contains("crates/three/src/c.rs")),
        "{complaints:?}"
    );
}

/// 标签集在 map 构造器里逐条拼出来的形状，`with_label` 一族看不见；这里按键位
/// 上的词判，只要它已经在本树里声明成常量就报。
#[test]
fn a_declared_word_spelled_as_a_map_key_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const LABEL_TOOL: &str = \"tool\";\n"),
        (
            "crates/three/src/c.rs",
            "pub fn labels(tool: &str) -> Map {\n    \
             HashMap::from([(\"tool\".to_string(), tool.to_string())])\n}\n",
        ),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("\"tool\"") && c.contains("crates/three/src/c.rs")),
        "{complaints:?}"
    );
}

/// 同一个位置上没声明过的词不判：HTTP 头、JSON 字段名都从这个形状里过，一律判红
/// 的话这条判据会被正当用法淹没。
#[test]
fn an_undeclared_word_as_a_map_key_is_not_policed() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const LABEL_TOOL: &str = \"tool\";\n"),
        (
            "crates/three/src/c.rs",
            "pub fn headers() -> Map {\n    \
             HashMap::from([(\"Retry-After\".to_string(), \"45\".to_string())])\n}\n",
        ),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

/// 同一个词的两种命名顺序归一族：只比「同名」的话，`LABEL_OUTCOME` 与
/// `OUTCOME_LABEL` 各自内部一致、彼此差一个词，一样读成空向量。
#[test]
fn the_two_orders_of_one_label_name_are_compared() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const LABEL_OUTCOME: &str = \"outcome\";\n"),
        (TWO, "pub const OUTCOME_LABEL: &str = \"result\";\n"),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("`OUTCOME`") && c.contains("result")),
        "{complaints:?}"
    );
}

/// 代码平台的词一样按名字判：两处同名而词不同就是红。
#[test]
fn a_platform_word_renamed_in_one_crate_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const GITEE_PLATFORM: &str = \"gitee\";\n"),
        (TWO, "pub const GITEE_PLATFORM: &str = \"gitee2\";\n"),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("GITEE_PLATFORM") && c.contains("gitee2")),
        "{complaints:?}"
    );
}

/// 以 `PLATFORM_` 开头的常量是各模块自己的环境变量名与路径，不是某个词的正身：
/// 撞名而不同值是正当的，不许把它读成红。
#[test]
fn a_platform_prefixed_setting_is_not_a_word() {
    let complaints = fixture_complaints(&[
        (ONE, "pub const PLATFORM_REGISTRY_FILES: &str = \"*.rs\";\n"),
        (TWO, "pub const PLATFORM_REGISTRY_FILES: &str = \"*.go\";\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

/// 提交信息上的 trailer 键一样按名字判：写出端与读回端各声明一份。
#[test]
fn a_trailer_key_renamed_in_one_crate_is_reported() {
    let complaints = fixture_complaints(&[
        (ONE, "const CHANGE_ID_TRAILER: &str = \"Change-Id\";\n"),
        (TWO, "const CHANGE_ID_TRAILER: &str = \"Change-Id: \";\n"),
    ]);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("CHANGE_ID_TRAILER") && c.contains("Change-Id: ")),
        "{complaints:?}"
    );
}

/// 名字撞上但不是词的常量（数上限、切片）不归本门管，也不许把它读成红。
#[test]
fn a_const_that_is_not_a_word_is_ignored() {
    let complaints = fixture_complaints(&[
        (
            ONE,
            "pub const ALERT_LABEL_VALUE_MAX_CHARS: usize = 1024;\n",
        ),
        (TWO, "pub const ALERT_LABEL_VALUE_MAX_CHARS: u32 = 32;\n"),
    ]);
    assert_eq!(complaints, Vec::<String>::new());
}

/// 普查本身也要有下限：一个常量都没读到时，「没有矛盾」是个空承诺。
#[test]
fn a_tree_that_declares_nothing_is_not_green() {
    let dir = tempfile::tempdir().expect("temp dir");
    let full = dir.path().join(ONE);
    std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
    std::fs::write(full, "pub fn nothing() {}\n").expect("write");
    let complaints = contradictions(dir.path());
    assert!(
        complaints.iter().any(|c| c.contains("普查只读到")),
        "{complaints:?}"
    );
}
