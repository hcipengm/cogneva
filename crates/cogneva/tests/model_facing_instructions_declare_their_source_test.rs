//! A string a model reads says where it comes from: a declared key, or the
//! code contract it moves with.
//!
//! `prompts/*.yaml` holds five keys and two gates reconcile them with the code:
//! `deploy/scripts/tests/prompt-declaration-wiring.test.sh` compares the set of
//! keys the code asks for with the set the files declare, and
//! `declared_prompts_match_code_fallbacks` compares the declared text with the
//! fallback the code carries. Both read *keys*. A prompt typed straight into a
//! call site never becomes a key, so both gates stay green while `prompts/`
//! stops describing what the model is told.
//!
//! 实测（2026-11-16）：生产源码里另有 107 条写给模型的指令文本，`prompts/`
//! 只声明了 5 条。分布是工具描述 24 条、评测 harness 17 条，其余约 62 条是
//! 任务与工件契约：规划指令、diff 语法与验证契约、告警修复 goal、面漂移 goal。
//! 改 `prompts/` 的任何一个字都不影响它们，而没有一道门看见它们。
//! 本门按承载形状普查到的**点**比文本多（一条文本可以在几处出现，一个函数里的每
//! 行字面量各算一点——门要报的是「哪一行没表态」）：2026-11-17 量到 330 个载体点、
//! 27 个函数、7342 字节。读数由测试自己印出来，`cargo test … -- --nocapture` 就是
//! 重新量一次；别的地方不要手抄这三个数。
//!
//! 字节数有一道只降不升的预算（`the_prompt_surface_only_shrinks`）：普查回答「每一行
//! 表态了吗」，预算回答「一共还剩多少」。没有它，删掉的字会一段一段长回来——每一段
//! 都能给出一个像样的理由，而没有任何一次改动会红。工具描述那一面不在本门的普查里
//! （形状是工具签名，不是契约字段），它的预算在 `crates/cog-agent/src/tools.rs` 里，
//! 同样只降不升。
//!
//! 为什么门判的是「就地表明身份」而不是「必须搬进 prompts/」：这两类东西的
//! 寿命不一样。系统提示词是策略，运维改了就该生效，所以它有一条声明、一份
//! 兜底、一道对账门。diff 语法与 JSON 契约是**解析器读的那份形状**，改了它们
//! 就是改代码契约——把它们搬进一个可以热改的文件，等于让模型被告知的形状和
//! 解析器接受的形状各改各的。所以每条指令只需要回答一个问题：它是声明（跟着
//! `prompts/` 走），还是契约（跟着读它的代码走）。回答写在指令所在函数的注释里。
//!
//! 判据：
//!
//! * 生产半边里每条模型可见的指令字面量都落在下面的承载形状上，并且它所在的
//!   函数有一行 `// prompt: …`；
//! * `declared <key>` 点名的 key 必须真的在 `prompts/*.yaml` 里——一个拼错的
//!   key 读起来和真的一样，只有拿声明面去对才分得开；
//! * 那一行只能是下面五种之一，每种都要点得出一个**真实存在**的东西：
//!   - `declared <key>`：文本来自 `prompts/` 的这条 key，key 必须真的在声明面里；
//!   - `contract — enforced by <路径>::<符号>`：违反了它，点名的这个符号会红。
//!     门去读那个文件：符号不在里面就红；文件不在 `tests/` 下、也没有任何测试标记
//!     （它根本不会红）也红——「点名一个真实名字」不算点名一个真实检查。
//!     写不出检查的散文**落不了地**，这正是逼着把「说了但没人判」的段落删掉的那一侧；
//!   - `consumed by <路径>::<符号>`：点名的读者读的就是这个形状，没有东西会红，
//!     但这段话不能和它脱节（改了格式而解析器没改，两边就此各说各话）。这类是
//!     删或补检查的工作清单：能补成 `enforced by` 的就补，补不上的就该删；
//!   - `fixed — <协议名>`：线格式由外部协议定死，树里没有检查可点，但要点名是哪
//!     一条协议（Messages API、MCP JSON-RPC）；
//!   - `not-an-instruction — <理由>`：形状命中了，但它不是给模型的（探针的 `"hi"`、
//!     空消息占位、逐字转发的 schema）。
//!
//!   理由短不短不作判据，**点不点得出东西**才是：一个词本身不算表态。
//!
//! 它看不见什么（写得比它看得见的更重要）：
//!
//! * 由 `format!` 拼出来的片段：字面量骨架若作为实参出现就会被计入，但拼进
//!   模板里再去别处组装的那半截，静态上无法判断它会到哪里——`signal_watcher`
//!   的 goal 正是这种形状，它的函数仍要求表态，因为骨架就在那个函数里；
//! * 用了 `DECISIVE_KEYS` 之外、又不在三个契约字段上的 JSON 键的契约：门把一个含
//!   决定性键的函数整段当提示词面（`grounding`、`diff_grammar` 这些键只会出现在
//!   写给模型的契约里），**也**把一个已经被声明成契约的函数整段算进去（`refusal_guidance`
//!   这类按 cause 返回整段文字的函数就是这种形状），**也**把往契约 `instructions`
//!   里放文字的函数（直接下标赋值，或交给 `append_instruction` 拼）整段算进去——
//!   所以一份另起键名、还没被声明的契约仍然看不见。补它的办法是把新键加进
//!   `DECISIVE_KEYS`、或让它写进 `instructions`，而不是放宽判词；
//! * 工具结果的文案（`"note": "…"` 这类回给模型读的说明）、YAML/TS/JSON 资产
//!   里的文本、`skills/` 下的技能文件；
//! * 由数据拼出来的文本：门判的是写在源码里的字面量，不是运行时才出现的字符串；
//! * `Message::user` / `Message::assistant` 里参数不是字面量的那些：那一侧绝大多数
//!   是把数据递进去（模型自己的答复、工具结果、任务正文），按调用点算会把普查变成
//!   数 `Message::user(`。系统消息相反——参数写成常量或 `format!` 也照算，否则把
//!   散文挪进一个常量名就能让它从普查里消失。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::common;

use common::lex::{strip, Dialect};
use common::routes::repo_root;
use common::rust_production::{production_numbered_lines_keeping_comments, production_sources};

/// 承载形状：模型可见指令在源码里的两种落点。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Carrier {
    /// `Message::system/user/assistant(` 后面跟一个字面量实参。
    Message,
    /// 任务契约的 `goal:` / `instructions:` / `acceptance:` 赋一个字面量
    /// （结构体初始化，或 JSON 字面量的同名字段）。
    ContractField,
}

/// 一个命中的载体点：文件、行号、形状、它所在的函数。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Site {
    file: String,
    line: usize,
    carrier: Carrier,
    function: String,
}

const MESSAGE_CALLS: [&str; 3] = ["Message::system(", "Message::user(", "Message::assistant("];
const CONTRACT_FIELDS: [&str; 3] = ["goal", "instructions", "acceptance"];

/// 这些键只在「写给模型读的契约」里出现，别的地方不会用它们当字段名。一个函数
/// 里出现其中任何一个，就把它当成提示词面：函数里的每个字符串字面量都要表态。
///
/// 为什么要这条，而不只认 `goal`/`instructions` 三个词：删掉之前
/// `change_generation_contract()` 的散文（实测 2026-11-17：13 KB）全挂在 `grounding`、
/// `diff_grammar`、`verification` 这些键上，三个词的形状一个都认不出来——最大的那份
/// 提示词恰好是门看不见的那一份。这些键如今只剩 `artifact_instructions` 一处还在用，
/// 但清单留着：这是这类形状的清单，不是当前树上的形状清单。
const DECISIVE_KEYS: [&str; 9] = [
    "artifact_instructions",
    "diff_grammar",
    "grounding",
    "refusal_causes",
    "plan_targets",
    "repo_contracts",
    "creating_a_file",
    "input_schema",
    "system_prompt",
];

fn starts_literal(rest: &str) -> bool {
    let rest = rest.trim_start();
    rest.starts_with('"')
        || rest.starts_with("r\"")
        || rest.starts_with("r#")
        || rest.starts_with("concat!")
}

/// `Message::…(` 之后的那半行（用来判实参是不是另起一行）。
fn message_arg(code: &str) -> Option<&str> {
    let mut best: Option<(usize, usize)> = None; // (位置, 调用长度)
    for call in MESSAGE_CALLS {
        if let Some(at) = code.find(call) {
            if best.is_none_or(|(earliest, _)| at < earliest) {
                best = Some((at, call.len()));
            }
        }
    }
    best.map(|(at, len)| &code[at + len..])
}

/// 这一行是不是在往契约的 `instructions` 里放东西：`contract["instructions"] = …`。
///
/// 契约常常是一段一段拼出来的，键既不在结构体里、也不在 `json!({…})` 的字面量
/// 里——规划器与评估器那两份最大的散文正是这样写进去的，而按点判的两条形状都看
/// 不见它。认这一种形状的函数，整段算提示词面。
///
/// 只认 `instructions`：`input["goal"]`、`input["acceptance"]` 这种下标赋值在循环
/// 里到处都是，写进去的是这一轮的数据，不是写给模型的规则。
fn assigns_contract_field(code: &str) -> bool {
    let form = "[\"instructions\"]";
    code.find(form)
        .map(|at| code[at + form.len()..].trim_start().starts_with('='))
        .unwrap_or(false)
        // 文字不是直接放进去、而是交给拼接函数的调用方也算：`append_instruction`
        // 只做拼接，说明本身写在调用它的函数里。
        || code.contains("append_instruction(")
}

/// 这一行是不是一个载体点。
fn carrier_of(code: &str) -> Option<Carrier> {
    // 系统消息的参数是不是字面量都算：`Message::system(SOME_CONST)` 与 `format!`
    // 拼出来的那一段同样是写给模型的话，只是话写在别处。只认字面量，等于「把散文
    // 挪进一个常量名」就能让它从普查里消失——那不是把话说短了，是让门看不见。
    //
    // 用户与助手消息不这样放宽：那一侧绝大多数是把数据递进去（模型自己的答复、
    // 工具结果、任务正文本来的样子），按调用点算会把普查变成数 `Message::user(`。
    // 它们只在实参就是字面量时算——那时候它是写死在源码里的一句话。
    if let Some(rest) = message_arg(code) {
        let system = MESSAGE_CALLS[..1].iter().any(|call| code.contains(call));
        if system || rest.trim_start().is_empty() || starts_literal(rest) {
            return Some(Carrier::Message);
        }
    }
    for field in CONTRACT_FIELDS {
        // 结构体初始化：`goal: "…"`，字段名前面必须是 `{`、`,` 或行首，
        // 否则 `subgoal:` 这类同尾字段会误命中。
        let struct_form = format!("{field}:");
        if let Some(at) = code.find(&struct_form) {
            let before = code[..at].trim_end();
            let ok = before.is_empty()
                || before.ends_with('{')
                || before.ends_with(',')
                || before.ends_with('(');
            if ok && starts_literal(&code[at + struct_form.len()..]) {
                return Some(Carrier::ContractField);
            }
        }
        // JSON 字面量：`"goal": "…"`（键本身在字符串里，去注释后仍在）
        let json_form = format!("\"{field}\":");
        if let Some(at) = code.find(&json_form) {
            if starts_literal(&code[at + json_form.len()..]) {
                return Some(Carrier::ContractField);
            }
        }
        // 下标赋值：`contract["instructions"] = serde_json::json!("…")`。契约常常是
        // 一段一段拼起来的，键既不在结构体里、也不在 `json!({…})` 的字面量里——前
        // 两种形状都看不见它，而规划器与评估器那两份最大的散文正是这样写的。
        let index_form = format!("[\"{field}\"]");
        if let Some(at) = code.find(&index_form) {
            let rest = code[at + index_form.len()..].trim_start();
            if let Some(rest) = rest.strip_prefix('=') {
                let rest = rest.trim_start();
                let rest = rest
                    .strip_prefix("serde_json::json!(")
                    .or_else(|| rest.strip_prefix("json!("))
                    .unwrap_or(rest);
                if starts_literal(rest) {
                    return Some(Carrier::ContractField);
                }
            }
        }
    }
    None
}

/// 这一行是不是一句表态，是的话返回它的正文。
fn declaration_of(raw: &str) -> Option<String> {
    let at = raw.find("// prompt:")?;
    Some(raw[at + "// prompt:".len()..].trim().to_string())
}

/// 一句表态说的是什么。判词按**出现形式**判，不按裸子串判：`contract` 后面必须
/// 跟 `enforced by <路径>::<符号>`，光写「这是契约」不算表态。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Declaration {
    /// 正文来自 `prompts/` 的这条 key；key 必须真的在声明面里。
    Declared(String),
    /// 违反了它，点名的这个符号会红（测试、校验、解析器的拒绝）。
    EnforcedBy(String, String),
    /// 点名的这个符号读的就是这个形状；没有东西会红，但这段话不能和它脱节。
    ConsumedBy(String, String),
    /// 线格式由外部协议定死，树里没有检查可点，但要点名是哪条协议。
    Fixed(String),
    /// 形状命中了，但它不是给模型的（探针、空占位、转发的 schema）。
    NotAnInstruction(String),
}

/// 词后面的那半句。词必须**整词**出现：`fixedsomething` 不是 `fixed`。
fn after_word<'a>(body: &'a str, word: &str) -> Option<&'a str> {
    let tail = body.strip_prefix(word)?;
    match tail.chars().next() {
        None => Some(""),
        Some(c) if c.is_whitespace() || c == '-' || c == '—' || c == ':' => {
            Some(tail.trim_start_matches([' ', '-', '—', ':']).trim())
        }
        Some(_) => None,
    }
}

fn pointer(tail: &str) -> Option<(String, String)> {
    let (path, symbol) = tail.split_once("::")?;
    let path = path.trim();
    let symbol = symbol.trim();
    if path.is_empty() || symbol.is_empty() || symbol.contains(char::is_whitespace) {
        return None;
    }
    Some((path.to_string(), symbol.to_string()))
}

fn parse_declaration(text: &str) -> Option<Declaration> {
    let body = text.trim();
    if let Some(tail) = after_word(body, "declared") {
        // 恰一个词：key 里没有空白，后面再跟一句话就没法判这条 key 到哪儿为止。
        if tail.is_empty() || tail.contains(char::is_whitespace) {
            return None;
        }
        return Some(Declaration::Declared(tail.to_string()));
    }
    if let Some(tail) = after_word(body, "contract") {
        let tail = tail.strip_prefix("enforced by")?.trim();
        return pointer(tail).map(|(p, s)| Declaration::EnforcedBy(p, s));
    }
    if let Some(tail) = after_word(body, "consumed by") {
        return pointer(tail).map(|(p, s)| Declaration::ConsumedBy(p, s));
    }
    if let Some(reason) = after_word(body, "fixed").filter(|r| !r.is_empty()) {
        return Some(Declaration::Fixed(reason.to_string()));
    }
    if let Some(reason) = after_word(body, "not-an-instruction").filter(|r| !r.is_empty()) {
        return Some(Declaration::NotAnInstruction(reason.to_string()));
    }
    None
}

fn declares_something(text: &str) -> bool {
    parse_declaration(text).is_some()
}

/// `enforced by` 点的那个地方**真的会红**吗：它得落在测试域里（`#[test]`／
/// `#[cfg(test)]`），或者路径本身就在 `tests/` 下。
///
/// 只核「符号存在」是不够的——生产代码里的任何一个名字都存在，点一个不判这段
/// 文字的函数照样能过，于是「点名一个真实检查」退化成「点名一个真实名字」。
/// 点不出会红的东西，诚实的话就该写 `consumed by`：那类声明明说没有东西会红，
/// 自己就成了待补检查的清单。
fn can_fail(path: &str, text: &str) -> bool {
    // 「有没有测试标记」问的不是「产出面在哪里」：切法只有一份（`common::rust_production`），
    // 而标记表也放在那里，调用点不再各写一遍。
    path.contains("/tests/") || crate::common::rust_production::mentions_tests(text)
}

/// 这一行声明的函数名（`fn name(`）。
fn fn_name(code: &str) -> Option<String> {
    let at = code.find("fn ")?;
    let name: String = code[at + 3..]
        .chars()
        .take_while(|c| !c.is_whitespace() && *c != '(' && *c != '<')
        .collect();
    (!name.is_empty()).then_some(name)
}

type Row = (usize, String, String); // 行号、原样、去注释

/// 生产半边：每个文件的 (相对路径, 行)。
fn production(root: &Path) -> Vec<(String, Vec<Row>)> {
    let mut out = Vec::new();
    for entry in production_sources(root) {
        let rel = entry
            .strip_prefix(root)
            .unwrap_or(&entry)
            .to_string_lossy()
            .to_string();
        let Ok(text) = std::fs::read_to_string(&entry) else {
            continue;
        };
        let rows = production_numbered_lines_keeping_comments(&text)
            .into_iter()
            .map(|(number, raw)| (number, raw.to_string(), strip(raw, Dialect::Rust)))
            .collect();
        out.push((rel, rows));
    }
    out
}

fn census(root: &Path) -> Vec<Site> {
    // 一个函数被声明成契约，它自己就是提示词面：`refusal_guidance` 这种按 cause 返回
    // 一整段的函数，里面没有一个决定性键，靠键去认它就会漏掉——而漏掉的恰好是「被拒时
    // 才发出去的那份规则」，正是本门最该看见的地方。
    let declared: BTreeSet<(String, String)> = declarations(root).into_keys().collect();
    let mut sites = Vec::new();
    for (rel, rows) in production(root) {
        // 函数归属：一个 `fn name(` 行到下一个 `fn` 行算一个函数段。这比括号归属
        // 偏严（嵌套闭包、impl 里的第二个 fn 都算后一个），而这一侧的错误是多问
        // 一句，不是悄悄放过。
        let mut starts: Vec<(usize, String)> = vec![(0, "<top level>".to_string())];
        for (index, (_, _, code)) in rows.iter().enumerate() {
            if let Some(name) = fn_name(code) {
                if index == 0 {
                    starts[0] = (0, name);
                } else {
                    starts.push((index, name));
                }
            }
        }
        for (position, (begin, function)) in starts.iter().enumerate() {
            let end = starts
                .get(position + 1)
                .map(|(next, _)| *next)
                .unwrap_or(rows.len());
            let span = &rows[*begin..end];
            let contract = declared.contains(&(rel.clone(), function.clone()))
                || span.iter().any(|(_, _, code)| {
                    DECISIVE_KEYS.iter().any(|key| code.contains(key))
                        || assigns_contract_field(code)
                });
            if contract {
                // 契约函数：函数里的每个字符串字面量都是写给模型读的，全部计入。
                // 不计长度门槛——表态是按函数给的，门槛只改变报出来的行数，不改变
                // 判词，却会变成一个人定一个的旋钮。
                for (number, _, code) in span {
                    if code.contains('"') {
                        sites.push(Site {
                            file: rel.clone(),
                            line: *number,
                            carrier: Carrier::ContractField,
                            function: function.clone(),
                        });
                    }
                }
                continue;
            }
            for (index, (number, _, code)) in span.iter().enumerate() {
                let Some(carrier) = carrier_of(code) else {
                    continue;
                };
                // `Message::system(` 后实参换行时，下一行必须真的是字面量才算命中。
                let counted = if let Some(rest) = message_arg(code) {
                    !rest.trim_start().is_empty()
                        || span
                            .get(index + 1)
                            .map(|(_, _, next)| starts_literal(next))
                            .unwrap_or(false)
                } else {
                    true
                };
                if counted {
                    sites.push(Site {
                        file: rel.clone(),
                        line: *number,
                        carrier,
                        function: function.clone(),
                    });
                }
            }
        }
    }
    sites.sort();
    sites.dedup();
    sites
}

/// `prompts/*.yaml` 声明了哪些 key。`declared <key>` 里的 key 必须在这里面：
/// 一个拼错的 key 读起来和真的一样，只有拿声明面去对才分得开。
fn declared_keys(root: &Path) -> BTreeSet<String> {
    let dir = root.join("prompts");
    let mut out = BTreeSet::new();
    let entries = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}——`declared <key>` 要对的是这份目录", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let name = path.display().to_string();
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for doc in common::yaml::docs(&text, &name) {
            let Some(prompts) = doc.get("prompts").and_then(|v| v.as_mapping()) else {
                continue;
            };
            for key in prompts.keys() {
                if let Some(key) = key.as_str() {
                    out.insert(key.to_string());
                }
            }
        }
    }
    assert!(
        !out.is_empty(),
        "{} 里没有声明任何一个 key——对账的前提是这一面读得到",
        dir.display()
    );
    out
}

/// 每个函数的表态：函数头之上紧邻的 `// prompt:` 注释块。
fn declarations(root: &Path) -> BTreeMap<(String, String), String> {
    let mut out = BTreeMap::new();
    for (rel, rows) in production(root) {
        let mut pending: Vec<String> = Vec::new();
        for (_, raw, code) in &rows {
            if let Some(note) = declaration_of(raw) {
                pending.push(note);
                continue;
            }
            if code.trim().is_empty() {
                continue;
            }
            if let Some(name) = fn_name(code) {
                if let Some(note) = pending.iter().rev().find(|n| declares_something(n)) {
                    out.insert((rel.clone(), name), note.to_string());
                }
            }
            pending.clear();
        }
    }
    out
}

/// 门自己的读法也要有一条会红的用例：判据若宽到「出现词之一就算表态」，
/// 一句「这是契约」就能滑过去，而它和没写一样。
#[test]
fn the_words_are_read_the_way_the_header_says() {
    assert_eq!(
        parse_declaration("declared agent:default"),
        Some(Declaration::Declared("agent:default".to_string()))
    );
    // 后面再跟一句话就没法判 key 到哪儿为止。
    assert_eq!(parse_declaration("declared agent:default — note"), None);
    assert_eq!(
        parse_declaration("contract — enforced by crates/a/src/b.rs::f"),
        Some(Declaration::EnforcedBy(
            "crates/a/src/b.rs".to_string(),
            "f".to_string()
        ))
    );
    // 点不出检查的「契约」不是表态——这正是逼着删的那一侧。
    assert_eq!(parse_declaration("contract — the diff grammar"), None);
    assert_eq!(parse_declaration("contract — enforced by"), None);
    assert_eq!(
        parse_declaration("consumed by crates/a/src/b.rs::parse"),
        Some(Declaration::ConsumedBy(
            "crates/a/src/b.rs".to_string(),
            "parse".to_string()
        ))
    );
    assert_eq!(
        parse_declaration("fixed — MCP JSON-RPC"),
        Some(Declaration::Fixed("MCP JSON-RPC".to_string()))
    );
    assert_eq!(parse_declaration("fixed"), None);
    assert_eq!(
        parse_declaration("not-an-instruction — a health probe's \"hi\""),
        Some(Declaration::NotAnInstruction(
            "a health probe's \"hi\"".to_string()
        ))
    );
    assert_eq!(parse_declaration("not-an-instruction"), None);
    assert_eq!(parse_declaration("because the parser reads it"), None);
    assert!(!declares_something("contract — why"));
    // 整词：`fixed` 后面紧跟字母不是这个词。
    assert_eq!(parse_declaration("fixedsomething"), None);

    assert_eq!(
        declaration_of("    // prompt: contract — why"),
        Some("contract — why".to_string())
    );
    assert_eq!(declaration_of("    // something else"), None);

    // `enforced by` 点的必须是会红的地方：测试文件、或带测试标记的文件。
    // 生产代码里的普通函数不会因为这句话被违反而红。
    assert!(can_fail("crates/x/tests/y.rs", "whatever"));
    assert!(can_fail("crates/x/src/y.rs", "#[cfg(test)]\nmod tests {"));
    assert!(can_fail(
        "crates/x/src/y.rs",
        "    #[tokio::test]\n    async fn f()"
    ));
    assert!(!can_fail(
        "crates/x/src/y.rs",
        "pub fn parse(s: &str) -> Result<()>"
    ));

    // 三种承载形状各认一次；同尾字段（subgoal）与不跟字面量的实参都不算。
    assert_eq!(
        carrier_of("let m = Message::system(\"you are\");"),
        Some(Carrier::Message)
    );
    assert_eq!(
        carrier_of("let m = Message::system("),
        Some(Carrier::Message)
    );
    assert_eq!(carrier_of("let m = Message::user(text);"), None);
    assert_eq!(
        carrier_of("let t = Task { goal: \"do it\", ..d };"),
        Some(Carrier::ContractField)
    );
    assert_eq!(
        carrier_of("json!({\"acceptance\": \"tests pass\"})"),
        Some(Carrier::ContractField)
    );
    assert_eq!(carrier_of("subgoal: \"x\""), None);
    assert_eq!(carrier_of("goal: computed()"), None);

    // 契约按字段名拼起来的形状：直接往 `instructions` 里放、或交给拼接函数，两种
    // 都让整个函数成为提示词面；`input["goal"]` 这种写数据的不算。
    assert!(assigns_contract_field(
        "contract[\"instructions\"] = serde_json::json!("
    ));
    assert!(assigns_contract_field(
        "    append_instruction(&mut contract, note);"
    ));
    assert!(!assigns_contract_field(
        "input[\"goal\"] = serde_json::json!(goal);"
    ));
    assert!(!assigns_contract_field(
        "let seen = contract[\"instructions\"].as_str();"
    ));
}

/// 门看见的这些点，一共有多少字节的字面量。这是模型每一回合都要付的固定成本，
/// 所以它只降不升：删掉一段散文，这个数跟着降；要往上抬，得先说清新增的那段
/// 凭什么非有不可。
fn carried_bytes(root: &Path) -> usize {
    carried_over(production(root), &census(root))
}

fn carried_over(rows: Vec<(String, Vec<Row>)>, sites: &[Site]) -> usize {
    let points: BTreeSet<(String, usize)> = sites
        .iter()
        .map(|site| (site.file.clone(), site.line))
        .collect();
    let mut total = 0;
    for (rel, lines) in rows {
        for (number, raw, _) in lines {
            if points.contains(&(rel.clone(), number)) {
                // 一行里每个完整字面量占多少字节：起止由共享读法给出，这里只做减法。
                // 自己再写一遍词法扫描，就等于让「哪些字节算提示词」有两个答案。
                total += cog_test_support::literals_in(&raw)
                    .iter()
                    .map(|literal| literal.end - literal.start)
                    .sum::<usize>();
            }
        }
    }
    total
}

#[test]
fn the_prompt_surface_only_shrinks() {
    let root = repo_root();
    let measured = carried_bytes(&root);
    // cap: derived — 由树上的这些点现算（2026-11-17 实测 7342）；删字时跟着降，
    // 往上抬要在提交信息里说清新增的是哪一段、凭什么非有不可。2026-11-17 从 7042
    // 抬到 7484 不是字变多了，是普查面变宽了：系统消息按调用点算之后，参数写在
    // 常量或 format! 里的那一半也进了这份读数（此前 54 个点看不见）。
    // 2026-11-24 从 7760 抬到 7768（树上实测 7768）：两条线合流后 planner 的意图载荷多了
    // `said_by`／`addressed_to` 两格——判者判「这只是问一句，还是一件活」靠的正是「谁说
    // 的、说给谁」，两格合计 8 字节。
    // 2026-11-24 从 7342 抬到 7760（树上实测 7760）：判据审的请求多了两问（「现在」照读
    // 能不能落到同一处、「差距」是不是只差那一件），以及「先读这一轮工作树、读不出来
    // 不算通过」。三句都是三格这套判据非有不可的部分——少了它们，判者只判终态那一格。
    const BUDGET: usize = 7_768;
    assert!(
        measured <= BUDGET,
        "模型每回合要为这些点付 {measured} 字节，预算是 {BUDGET}：要么删掉长出来的那段，\
         要么抬预算并说清理由"
    );
    eprintln!(
        "prompt payload: {measured} bytes at {} carrier points",
        census(&root).len()
    );
}

/// 读数确实来自树，不是写死的：换一段更长的字面量，读数跟着变。
#[test]
fn the_payload_reading_follows_the_text_on_the_tree() {
    let measure = |body: &str| {
        let rows: Vec<(String, Vec<Row>)> = vec![(
            "crates/x/src/y.rs".to_string(),
            production_numbered_lines_keeping_comments(body)
                .into_iter()
                .map(|(number, raw)| (number, raw.to_string(), strip(raw, Dialect::Rust)))
                .collect(),
        )];
        let sites = vec![Site {
            file: "crates/x/src/y.rs".to_string(),
            line: 1,
            carrier: Carrier::Message,
            function: "f".to_string(),
        }];
        carried_over(rows, &sites)
    };
    let short = measure("fn f() { Message::system(\"short\") }");
    let long = measure("fn f() { Message::system(\"a much longer instruction text\") }");
    assert!(short > 0, "扫描器没在这个夹具上读到任何字面量");
    assert!(long > short, "读数没跟着树上的文本走：{short} -> {long}");
}

#[test]
fn every_model_facing_instruction_declares_its_source() {
    let root = repo_root();
    let sites = census(&root);
    assert!(
        !sites.is_empty(),
        "普查一个载体点都没读到——扫描器坏了，不是树上没有提示词"
    );
    // 读数从这儿印，不要手抄到别处：`cargo test … -- --nocapture` 就是重新量一次。
    // 文件头那句「330 个载体点、27 个函数」是这一行在某一天的输出。
    let functions: std::collections::BTreeSet<(&str, &str)> = sites
        .iter()
        .map(|s| (s.file.as_str(), s.function.as_str()))
        .collect();
    eprintln!(
        "prompt census: {} carrier points in {} functions",
        sites.len(),
        functions.len()
    );
    let declared = declarations(&root);
    let keys = declared_keys(&root);
    let mut bogus: Vec<String> = Vec::new();
    for ((file, function), note) in &declared {
        let at = format!("{file} 的 `{function}`");
        match parse_declaration(note) {
            Some(Declaration::Declared(key)) if !keys.contains(&key) => bogus.push(format!(
                "{at} 说 `declared {key}`，但 prompts/ 里没有这条 key"
            )),
            Some(Declaration::EnforcedBy(path, symbol))
            | Some(Declaration::ConsumedBy(path, symbol)) => {
                let enforced = matches!(parse_declaration(note), Some(Declaration::EnforcedBy(..)));
                let target = root.join(&path);
                match std::fs::read_to_string(&target) {
                    Err(e) => bogus.push(format!("{at} 点名了 {path}，但读不到它：{e}")),
                    Ok(text) if !text.contains(&symbol) => bogus.push(format!(
                        "{at} 点名了 {path}::{symbol}，但那个文件里找不到这个符号"
                    )),
                    // `enforced by` 说「违反它会红」，那么它点的这个地方得真会红。
                    // 不会红就不是 enforced by，是 consumed by——后者明说没人会红。
                    Ok(text) if enforced && !can_fail(&path, &text) => bogus.push(format!(
                        "{at} 说 `enforced by {path}::{symbol}`，但那个文件既不在 tests/ 下，\
                         也没有任何测试标记——它不会红，这段文字的身份应写成 `consumed by`"
                    )),
                    Ok(_) => {}
                }
            }
            Some(
                Declaration::Fixed(_) | Declaration::NotAnInstruction(_) | Declaration::Declared(_),
            ) => {}
            None => bogus.push(format!("{at} 的表态读不出来：{note}")),
        }
    }
    assert!(
        bogus.is_empty(),
        "{} 条表态点名了不存在的东西：\n{}",
        bogus.len(),
        bogus.join("\n")
    );
    let mut unmarked: Vec<String> = Vec::new();
    for site in &sites {
        let key = (site.file.clone(), site.function.clone());
        if !declared.contains_key(&key) {
            unmarked.push(format!(
                "{}:{} 在 `{}` 里（{:?}）",
                site.file, site.line, site.function, site.carrier
            ));
        }
    }
    assert!(
        unmarked.is_empty(),
        "{} 个函数带着模型可见的指令却没有表态（`// prompt: declared <key>` / \
         `// prompt: contract — enforced by <路径>::<符号>` / \
         `// prompt: consumed by <路径>::<符号>` / `// prompt: fixed — <协议>` / \
         `// prompt: not-an-instruction — <理由>`）：\n{}",
        unmarked.len(),
        unmarked.join("\n")
    );
}
