//! principle: feedback_a_process_has_one_outbound_client_and_every_caller_uses_it
//!
//! Gate: 一个进程里手工造出来的出站 HTTP 客户端只有一处，它的超时、连接池、代理、
//! user-agent 只从 `http_client` 段读一次。
//!
//! 这条原则的形状：同一进程里第二次手工构造 `reqwest::Client`，就是把超时、连接池、
//! 代理抄成第二份设置，而最常漏的正是**超时**——`reqwest::Client::new()` 根本没有
//! 总超时，`Client::builder()` 不写 `.timeout(` 也一样没有。两份里漏掉的那一份不会
//! 在任何测试或日志里显出来：它照样发得出去、照样收得回来，只有在对手不回话的时候
//! 才表现为一个永不返回的 `await`，而那时它已经是个卡住的 Job 或一条挂死的请求。
//!
//! 现场因此落成 `crates/cog-net/src/factory.rs` 一处：`builder(config)` 把
//! `http_client` 段读一次，`build_client` 在它上面加总超时，`build_streaming_client`
//! 只丢那一个（流式连接本来就该活过短交换的截止期，连接超时留着）。别处再建一个，
//! 就是把这组数字抄第二份。
//!
//! 所以判据落在树上，问三个问题：
//!
//! 1. `crates/*/src` 的生产正文里，凡是一条语句同时出现「构造写法」（`Client::new(`／
//!    `Client::builder(`／`ClientBuilder::new(`／`reqwest::ClientBuilder::`）且它指的
//!    真的是 reqwest 的客户端（`reqwest::Client`、或本文件从 reqwest 引入的裸
//!    `Client`），必须正好是下面声明的那几处，每处带一条理由。多一处，说明同一进程里
//!    又有了第二份超时／连接池／代理；少一处，说明声明的豁免已经过期——没人重新决定
//!    过的豁免不许活过它的理由。
//! 2. 那唯一一处工厂自己得对：`build_client` 的正文里有 `.timeout(`，
//!    `build_streaming_client` 的正文里没有，而两条都从 `builder(config)` 出。
//!    少了前半条，「唯一的手工构造地」也是一个不带截止期的构造地，原则的第一半就空了。
//! 3. 判据自己的两格：正例必须认出来，注释与 `//!` 文档、以及别人的
//!    `SomethingClient::new(` 与 `meilisearch_sdk::client::Client::new(` 必须不许响。
//!
//! 判不了什么：不判「一次 build 出来的 `Client` 被几个调用点复用」（那要数据流）；
//! 不判 `HttpClient` trait 的调用方是否都设了 `timeout_secs`（那是请求级，另一条判据）；
//! 不判 `#[cfg(test)]` 里的夹具——夹具的寿命由测试决定，不是进程的设置。

use crate::common;

use common::lex::{strip, Dialect};
use common::routes::{read, repo_root};
use common::rust_production::{production_line_mask, production_sources, relative_name};

use std::collections::BTreeMap;

/// 手工造一个 reqwest 客户端的写法。第二条带着自己的 `::`：`-> SFResult<reqwest::ClientBuilder>`
/// 是提到这个类型，不是造一个——`builder(config)` 返回它，值本身在 `Client::builder()` 那里。
const CONSTRUCTION_CALLS: [(&str, usize); 4] = [
    ("Client::new(", 0),
    ("Client::builder(", 0),
    ("ClientBuilder::new(", 0),
    ("reqwest::ClientBuilder::", "reqwest::".len()),
];

/// 生产正文里唯一允许手工造出站客户端的地方，每处一条理由。
///
/// 读法：多一处，说明同一进程里又有了第二份超时／连接池／代理；少一处，说明这里的
/// 理由已经不成立——要么那个客户端被删了却没删这一行，要么它改走了工厂。两种都要
/// 重新决定，改哪一边都行，就是不许留着一条替不存在的构造说话。
const DECLARED: [(&str, &str); 11] = [
    (
        "crates/cog-net/src/factory.rs",
        "唯一的手工构造地：超时、连接池、代理、user-agent 都在这里从 `http_client` 段读一次，\
         `build_client` 加总超时、`build_streaming_client` 只丢那一个——别处再建就是把这一组数字\
         抄第二份",
    ),
    (
        "crates/bootstrap/src/apt.rs",
        "装机器是独立二进制，跑在 `http_client` 段落盘之前（那一节正是它装出来的），进程里没有\
         装配层；镜像探活自带 `MIRROR_PROBE_TIMEOUT`，它挑的是镜像站，不是业务出口",
    ),
    (
        "crates/bootstrap/src/download.rs",
        "同一个独立二进制；`probe(url, within)` 的上界由调用方给，一条客户端要服务一族不同的\
         「多久算不通」，写成全局超时就把这个判据写死了",
    ),
    (
        "crates/bootstrap/src/main.rs",
        "同一个独立二进制；`maybe_warn_outdated` 只提示不升级，用 `OUTDATED_PROBE_TIMEOUT`，失败\
         即静默返回，不阻塞装机命门链路",
    ),
    (
        "crates/cog-extension/src/hostdocs_assist.rs",
        "cog-extension 只认 `cog_core::HttpClient` 契约，`cog-net` 不是它的依赖（工厂在装配层那一\
         侧）；这条审计通道的时限是一处常数 `REQUEST_TIMEOUT_SECS`，要共用得先加一条依赖边，\
         那不是一份门能做的决定",
    ),
    (
        "crates/cog-extension/src/runtime/remote.rs",
        "沙箱执行器那条是流式连接：长命令由服务端超时封顶，客户端只带 `CONNECT_TIMEOUT`——正是\
         `build_streaming_client` 的形状，同样卡在 cog-net 不是它的依赖上",
    ),
    (
        "crates/cog-gateway/src/git_identity.rs",
        "部署密钥登记发生在装配层交出 `dyn HttpClient` 之前（自举里的一步），时限是\
         `DEPLOY_KEY_REQUEST_TIMEOUT`；cog-gateway 不依赖 cog-net，只认契约",
    ),
    (
        "crates/cog-gateway/src/git_routing.rs",
        "选路探针刻意不带总超时：每次探针的上界由调用方按用途给，`SIGNAL_PROBE_TIMEOUT` 只是\
         连接与单次上限；带一条总截止期会把「短而硬」变成「长而软」，探测就量不到它要量的东西",
    ),
    (
        "crates/cog-gateway/src/llm_admin.rs",
        "三条准入探测各自的 `PROBE_TIMEOUT`／`STYLE_PROBE_TIMEOUT` 就是被量的那个判据；\
         `KubeClient` 那条还要 `add_root_certificate` 装集群 CA，工厂没有这个旋钮",
    ),
    (
        "crates/cog-gateway/src/security_gateway.rs",
        "网关自己那一对（短交换 + 流式），出口身份走 `default_headers`，工厂没有这个旋钮；\
         附件代理那条还要逐跳 host 白名单的 `redirect` 策略，跳数就是它的上界",
    ),
    (
        "crates/cog-observability/src/probes.rs",
        "探针构造体：时限与「跟不跟重定向」是探针自己的两个参数，值由调用方给——那是探针的\
         判据，不是 `http_client` 段的第二份抄法",
    ),
];

/// 唯一的手工构造地：超时、连接池、代理、user-agent 都在这里从 `http_client` 段读一次。
const FACTORY: &str = "crates/cog-net/src/factory.rs";

/// 一个名字前面那一段限定路径：`reqwest::Client` 的 `reqwest::`，裸 `Client` 的空串。
///
/// 走的是字符边界而不是字节：`crates/*/src` 的正文里到处是中文注释，按字节往回走会
/// 走进一个多字节字符的中间，切出来的是一个不合法的 `&str`。
fn qualifier(code: &str, ident_start: usize) -> &str {
    let mut start = ident_start;
    while let Some(prev) = code[..start].chars().next_back() {
        if prev.is_ascii_alphanumeric() || prev == '_' || prev == ':' {
            start -= prev.len_utf8();
        } else {
            break;
        }
    }
    &code[start..ident_start]
}

/// 这份文件是不是从 reqwest 引进了 `Client`／`ClientBuilder`。
///
/// 只给裸名字那条路用：`factory.rs` 里的 `Client::builder()` 靠 `use reqwest::Client;`
/// 才指得到 reqwest，而名字本身不带任何线索。反过来，`KeycloakClient::new(` 与
/// `meilisearch_sdk::client::Client::new(` 不需要这份表也认得出来——前者的限定路径是
/// 一个更长的标识符的一部分，后者的限定路径不是 `reqwest::`。
fn imports_a_reqwest_client(code: &str) -> bool {
    code.contains("use reqwest::Client")
        || code.contains("use reqwest::ClientBuilder")
        || code.contains("use reqwest::{")
}

/// 这个位置上的名字指的是不是一个 reqwest 客户端的构造。
fn names_a_reqwest_client(code: &str, ident_start: usize) -> bool {
    match qualifier(code, ident_start) {
        "reqwest::" => true,
        "" => imports_a_reqwest_client(code),
        _ => false,
    }
}

/// 这份正文里每一处「手工造出站客户端」，给出构造点（那个名字本身）所在的字节偏移。
///
/// 按**构造点**判而不是按裸子串判：`reqwest` 这几个字出现在一条语句里，与这条语句在
/// 造客户端是两件事——`vault.rs` 只在类型上用它，真正那次构造走 `crate::build_client`；
/// `meilisearch_sdk::client::Client::new(` 里也有 `Client::new(` 这几个字。按裸子串判
/// 会把它们算成又一处，于是一份门靠假条目撑着自己的非空，而真装机的那一行改个换行就漏了。
///
/// 去重的键是那个名字，不是它所在的语句：一条链式 `reqwest::Client::builder()…` 会被两条
/// needle 同时认出来（`Client::builder(` 与 `reqwest::ClientBuilder::`），它们指的是同一处；
/// 而一条结构体字面量里写两个 `reqwest::Client::builder()` 是**两处**客户端——按语句去重会
/// 把右半边吞掉，而「一对客户端」正是这类装配的样子（`security_gateway` 的 `client` 与
/// `stream_client` 就是这么写在一处的）。
///
/// 进门前先剥一遍注释：判的是「这条语句在造客户端」，不是「这几个字出现在这个文件里」。
/// 一条 `// 别在这里写 reqwest::Client::new()` 与一条 `/* Client::builder() */` 说的都是
/// 别这么做，读成构造就把这份门变成「谁写了注释谁红」。
fn constructions(source: &str) -> Vec<usize> {
    let code = strip(source, Dialect::Rust);
    let mut sites: Vec<usize> = Vec::new();
    for (needle, ident_offset) in CONSTRUCTION_CALLS {
        let mut from = 0;
        while let Some(rel) = code[from..].find(needle) {
            let at = from + rel;
            if names_a_reqwest_client(&code, at + ident_offset) {
                sites.push(at + ident_offset);
            }
            from = at + needle.len();
        }
    }
    sites.sort_unstable();
    sites.dedup();
    sites
}

fn line_of(text: &str, at: usize) -> usize {
    text[..at].bytes().filter(|b| *b == b'\n').count() + 1
}

/// 一个源文件的生产正文，注释已剥掉，行号仍是文件里的行号。
///
/// 夹具那半边就地空掉（[`production_line_mask`]）而不是删掉：删行会把第一处
/// `#[cfg(test)]` 之后的行号整段挪位，而这张表报的 `file:line` 要人去对照文件看。
fn production_code(source: &str) -> String {
    let mask = production_line_mask(source);
    let kept: Vec<&str> = source
        .lines()
        .enumerate()
        .map(|(index, line)| {
            if mask.get(index).copied().unwrap_or(false) {
                line
            } else {
                ""
            }
        })
        .collect();
    strip(&kept.join("\n"), Dialect::Rust)
}

/// 这一棵树里每一处手工造出站客户端的地方，键是仓库相对名。
fn builders_in_this_tree() -> BTreeMap<String, Vec<usize>> {
    let root = repo_root();
    let mut out = BTreeMap::new();
    for path in production_sources(&root) {
        let text = production_code(&read(&path));
        let sites = constructions(&text);
        if !sites.is_empty() {
            out.insert(
                relative_name(&root, &path),
                sites.iter().map(|at| line_of(&text, *at)).collect(),
            );
        }
    }
    out
}

#[test]
fn the_only_hand_built_outbound_clients_are_the_declared_ones() {
    let found = builders_in_this_tree();
    assert!(
        !found.is_empty(),
        "一个手工造出站客户端的地方都没读到：`{FACTORY}` 自己就在造，读到空集说明这份普查 \
         没有走到 crates/*/src，两张表比对下去全是绿的"
    );

    let declared: BTreeMap<&str, &str> = DECLARED.into_iter().collect();

    let mut extra = Vec::new();
    for (file, lines) in &found {
        if !declared.contains_key(file.as_str()) {
            extra.push(format!("{file}:{lines:?}"));
        }
    }
    assert!(
        extra.is_empty(),
        "这些地方自己造了出站客户端，而它们没有在 DECLARED 里：{extra:?}。同一进程里第二次 \
         手工构造 `reqwest::Client`，就是把超时、连接池、代理抄成第二份设置，而最常漏的正 \
         是超时——`Client::new()` 连一个都没有，漏掉的那一份只在对手不回话时露出一个永不 \
         返回的 `await`。改走 `cog_net::build_client`／装配层发布的 `dyn cog_core::HttpClient`， \
         确实不能共用的（独立二进制、流式那条不带整体截止期）把理由写进 DECLARED。"
    );

    let mut stale = Vec::new();
    for (file, why) in &declared {
        if !found.contains_key(*file) {
            stale.push(format!("{file}（{why}）"));
        }
    }
    assert!(
        stale.is_empty(),
        "DECLARED 里这些条目的理由已经不成立：{stale:?}——它们的生产正文里再没有手工造出站 \
         客户端。没人重新决定过的豁免不许活过它的理由：删掉这一条，别让它替一处不存在的构造说话。"
    );
}

/// 一个函数体的正交：从签名所在行之后到配对的 `}` 之前。
///
/// 括号账走 `brace_shapes`，与 `production_text` 同一台状态机：函数体里有
/// `format!("…: {e}")` 这样的字符串，按行数 `{` `}` 会把它算进去，而字符串里的括号
/// 不改变嵌套。
fn body_of(code: &str, signature: &str) -> String {
    let lines: Vec<&str> = code.lines().collect();
    let shapes = common::rust_production::brace_shapes(code);
    let start = lines
        .iter()
        .position(|line| line.contains(signature))
        .unwrap_or_else(|| panic!("正文里找不到 {signature}：工厂改名了，这条判据得跟着走"));

    let mut body = Vec::new();
    let mut depth = 0i32;
    let mut opened = false;
    for (index, line) in lines.iter().enumerate().skip(start) {
        let shape = shapes.get(index).copied().unwrap_or_default();
        depth += shape.delta;
        if shape.opens {
            opened = true;
            if depth <= 0 {
                break;
            }
            continue;
        }
        if !opened {
            continue;
        }
        if depth <= 0 {
            break;
        }
        body.push(*line);
    }
    body.join("\n")
}

/// 「最常漏的正是超时」这一半：工厂的两条入口从同一个 `builder(config)` 出来，只差那一个
/// 短交换的截止期。
///
/// 少了这条，上面那份表只证明「只有一处构造」，不证明那一处是对的——一处不带总超时的
/// 工厂照样能把整条原则的第一半变成空话，而它是绿的。
#[test]
fn the_one_factory_keeps_the_deadline_the_second_path_only_drops_it() {
    let factory = production_code(&read(&repo_root().join(FACTORY)));

    let short = body_of(&factory, "pub fn build_client(");
    let streaming = body_of(&factory, "pub fn build_streaming_client(");

    assert!(
        short.contains(".timeout("),
        "{FACTORY} 的 `build_client` 正文里没有 `.timeout(`：这一次构造出来的客户端没有 \
         总超时，`http_client.timeout_secs` 就没人读——`Client::builder()` 不写这一行，与 \
         `Client::new()` 一样是「对手不回话就永远等」。"
    );
    assert!(
        !streaming.contains(".timeout("),
        "{FACTORY} 的 `build_streaming_client` 正文里带了 `.timeout(`：流式连接本该活过短 \
         交换的截止期，带上它就会切断它存在的那个连接。要改的是这一条，不是让 `build_client` 也丢。"
    );
    for (name, body) in [
        ("build_client", &short),
        ("build_streaming_client", &streaming),
    ] {
        assert!(
            body.contains("builder(config)"),
            "{FACTORY} 的 `{name}` 不再从 `builder(config)` 出：两条入口各写一份设置，连接池、 \
             代理、user-agent 就有了第二份抄法，而这一处是声明里说「只读一次」的那一处。"
        );
    }
}

/// 判据自己的两格：一条要响、一条不许响。
///
/// 缺了它，「构造」的识别一旦退化成永远读不到，上面两条断言会一起变成空断言而仍然是
/// 绿的——一份认不出任何构造的普查，与一棵真的只有一处构造的树，读数是同一个空集。
#[test]
fn the_detector_knows_a_construction_from_a_mention() {
    let bare = "pub fn client() -> reqwest::Client { reqwest::Client::new() }";
    assert_eq!(
        constructions(bare).len(),
        1,
        "`reqwest::Client::new()` 必须认出来"
    );

    let chained = r#"
        pub fn client() -> reqwest::Client {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("a client")
        }
    "#;
    assert_eq!(
        constructions(chained).len(),
        1,
        "一条链式 `Client::builder()` 必须认出来，且 `.build()` 不再算第二处"
    );

    let imported = "use reqwest::Client;\nfn b() -> Client { Client::builder().build().unwrap() }";
    assert_eq!(
        constructions(imported).len(),
        1,
        "从 reqwest 引进来之后裸 `Client::builder()` 是同一种构造"
    );

    let commented = r#"
        // 别在这里写 reqwest::Client::new()，超时和连接池会是第二份
        /* 也不许 `Client::builder().build()` */
        pub fn f() {}
    "#;
    assert!(
        constructions(commented).is_empty(),
        "注释里写着的构造不是构造"
    );

    let doc = "//! 一次短交换不该自己 `Client::new()`。\npub fn f() {}";
    assert!(
        constructions(doc).is_empty(),
        "`//!` 文档里写着的构造不是构造"
    );

    let field = r#"
        pub struct Holder {
            http: reqwest::Client,
            stream: reqwest::Client,
        }
    "#;
    assert!(
        constructions(field).is_empty(),
        "只把 `reqwest::Client` 当类型的字段声明不是构造"
    );

    let other_client = r#"
        fn kc() -> KeycloakClient { KeycloakClient::new(config) }
        fn ms() -> Client { meilisearch_sdk::client::Client::new(host, key) }
    "#;
    assert!(
        constructions(other_client).is_empty(),
        "`SomethingClient::new(` 与 `meilisearch_sdk::client::Client::new(` 不是 reqwest 的 \
         客户端构造：按裸子串判会把它们算成第二处，表靠假条目撑非空，真装机的那一行改个换行就漏了"
    );
}
