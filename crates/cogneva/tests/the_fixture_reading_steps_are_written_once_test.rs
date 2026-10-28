//! 「怎么读一个文件」「怎么走一棵树」「怎么去掉注释」「怎么配平括号」「怎么取仓库
//! 相对名」各只许有一份实现。
//!
//! 这几条规则短到看起来不值得共享，所以它们在夹具里被抄了十二遍、两遍、六遍、两遍、
//! 两遍，取相对名那一条还额外被内联摊了十六处——连同那句解释「读不出来就 panic」的
//! 注释一起抄。抄的人不是懒：每个测试二进制
//! 都是一棵独立的 crate，`common` 不是自动可见的，手边最快的一条路就是复制。去注释
//! 那一条抄得更没声：六份里三种写法（按字节 / 按字符 / 数嵌套层数），谁抄的都不算错，
//! 错的是它们从此各走各的——Rust 的块注释**可以嵌套**，数层数的那一份与只看第一个
//! `*/` 的那一份，对同一段源码给出的「代码」不是同一段。
//!
//! 代价落在**改的时候**：`fsio::walk` 里那句「读不出来就 panic」是这条规则的全部
//! 价值（跳过一个读不出来的子目录不会让任何断言变红，剩下的那份普查往往仍高于下限，
//! 「少读了一块」与「那一块都合规」在判词上分不开），而抄件里那句话是注释，不是机制。
//! 谁要是在其中一份里把 `unwrap_or_else(panic!)` 换成 `flatten()` 或 `else { continue }`，
//! 另外十几份不会跟着变，也不会有任何东西发现它们已经不同——这正是这一族缺陷的样子：
//! **同一份规则被写多遍，而没有任何东西比较它们**。
//!
//! 配平括号与取相对名这两条抄得更隐蔽：抄件并不逐字相同，所以谁抄的时候都没觉得自己
//! 在抄。括号那一份有人按字符数、有人按字节数（网关源码里全是中文，两者是不同的数），
//! 有人跳过 `"…"` 里的括号、有人不跳；相对名那一份有人折叠开头的 `./`、有人不折。
//! 两种写法对同一段源码给出的答案不同，而两个方向都没有任何东西会红——落点全在
//! 「这一处读到的和那一处读到的不是同一个东西」这种只有改的时候才显形的地方。
//!
//! 判据分两种形状：逐字相同的那几份按**签名**判——
//! `fn read(path: &Path) -> String`、`fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>)`
//! 与 `fn strip_comments(text: &str) -> String`；名字唯一的那些按**定义名**判——
//! `fn matching_brace(`、`fn matching_paren(` 与 `fn relative_name(`，因为这三个名字
//! 本来就是 `common` 导出给全仓用的，定义名在别处出现按构造就是第二份实现，比签名更
//! 不容易漏（实测：这三种抄件的返回类型、参数类型、甚至函数名都各变过一次）。
//! 这比「凡 `read_dir` 即红」窄得多，是量过的：夹具与产出面里有几十份文件自己走目录，
//! 其中绝大多数是不同形状（只下一层、带着自己的过滤与解析、返回相对名字），那些不是
//! 这条规则的副本。这里只抓逐字抄过去的那一种。
//!
//! 走目录还有**第二种形状**，上面那些按签名 / 定义名判的判据全都看不见它：抄的人把
//! 函数改了名、换了参数、还各自带着自己的过滤与解析，于是十几份「递归下目录、顺手把
//! 文件收进表里」的实现各自成立，谁也不逐字像谁。它们共有的只有一件事——**一个函数
//! 体里同时出现 `read_dir` 与它自己的名字**。那正是「这棵树怎么走」这条规则的形状，
//! 而它只该有一个答案，所以判据直接认这个形状：夹具里不许再有第二处自走的递归。
//!
//! 还有**第三种形状**，前两种都看不见它：抄的人把函数名也换了。`is_word` 与
//! `is_component` 是同一个判据的两个名字，`split_arms` 与 `split_items` 也是；
//! 按签名判要求参数类型相同，按定义名判要求名字相同，而抄件两样都可以不同。它们
//! 唯一共有的是**函数体**：去掉注释、折叠空白之后逐字相同。所以这一面按体判——
//! 两个夹具里出现同一段体（哪怕叫法不同）就是同一条规则的第二份。
//!
//! 体相同的那几条今天多半已经搬进 `common::rust_text`（`snake_case`、`raw_string_end`、
//! `trailing_identifier`、`is_lower_word`）。剩下没搬的那些记在
//! `KNOWN_DUPLICATED_BODIES` 里，那张表只能变短：合并掉一条就删一行，留着会让
//! 「每一条都还数得出来」的断言先红。
//!
//! 断言各自单独会长回来：副本为零、递归只有一处、没有新的同体、共享模块还在真干活、
//! 还有人在用它。

mod common;

use std::path::PathBuf;

use common::lex::{blank, brace_delta, strip, Dialect};
use common::routes::{matching_brace, matching_paren, repo_root};
use common::rust_production::relative_name;

/// 读文件、走目录这两条的唯一定义。
const THE_ONE: &str = "crates/cogneva/tests/common/fsio.rs";

/// 去注释、配平括号这两条的定义在 `routes` 里——它们要读同一批源码，和注册路线的
/// 读法住在一起。
const ROUTES_HOME: &str = "crates/cogneva/tests/common/routes.rs";

/// 「哪里是字符串、哪里是注释」这一条的定义在 `lex` 里：块注释的嵌套、原始字符串、
/// 字符字面量与生命周期都在那一台状态机上。`routes::strip_comments` 与
/// `rust_production::brace_shapes` 都只是它的两个出口。
const LEX_HOME: &str = "crates/cogneva/tests/common/lex.rs";

/// 取仓库相对名那一条的定义在 `rust_production` 里：它先是「读一棵生产源码树」的一
/// 部分，后来才被别处借去用。
const PRODUCTION_HOME: &str = "crates/cogneva/tests/common/rust_production.rs";

/// 被抄走的那几行，逐字。前四个是**整条签名**（这几条规则的参数与返回类型在全仓只有
/// 这两种写法，所以签名是最窄也最准的形状）；后六个只到**定义名**为止。
///
/// 读文件占两个签名，因为这条规则被抄成了两种入口：收 `&Path` 的那条，和收「表里写着
/// 的相对名」的那条（`read(rel)` / `read(relative)`，二十三个夹具各写一遍
/// `repo_root().join(rel)` 加同一条 panic）。拷贝只在参数名上变过，所以两种都写上。
///
/// 为什么后六个不用整条签名：括号那一份在两处抄件里被写成了 `text: &str`，而共享的
/// 那一份收 `bytes: &[u8]`；相对名那一份在一处抄件里连名字都改成了 `relative`。签名
/// 按抄件当时的形状写下去，下一个抄的人换一种参数类型就漏了。这六个名字是 `common`
/// 导出的，别处再出现一次定义按构造就是第二份实现。
const COPIED_FORMS: [&str; 10] = [
    "fn read(path: &Path) -> String",
    "fn read(rel: &str) -> String",
    "fn read(relative: &str) -> String",
    "fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>)",
    "fn strip_comments(text: &str) -> String",
    "fn matching_paren(",
    "fn matching_brace(",
    "fn relative_name(",
    "fn walk_files(",
    "fn walk_all(",
];

/// 取相对名还有**内联**的形状：不写成函数，直接把那一串方法调用摊在调用点上。
/// 这是这一族抄得最多的一种——十六个夹具各摊一遍，谁都不觉得自己在抄，所以按定义名
/// 判的判据一个也看不见。
///
/// 只在**夹具**面上判：同一串 `.replace('\\', "/")` 在产出码里有十来处，那是把路径
/// 变成线上/日志里的名字，各有各的上下文，而共享的这份住在测试 harness 里——产出码够
/// 不着它。跨类边界不是副本。
const INLINE_FORMS: [&str; 1] = [r#".replace('\\', "/")"#];

/// 已经数出来、但还没合并的那些同体函数：按**函数体**比，不看名字。
///
/// 这一族抄件的名字最不可靠：`is_word` 与 `is_component` 是同一个判据的两个名字，
/// `split_arms` 与 `split_items` 也是。按签名、按定义名判的判据全都看不见它们，
/// 只有「两段函数体去掉空白之后逐字相同」这件事看得见。
///
/// 这张表只能变短，今天已经空了：这一族清干净，一格余量也没有。往后新长出来的同体
/// 直接红；真要豁免，就得往表里写名字、同时把下面「表必须是空的」那条断言改掉——
/// 两处改动都在评审里看得见，而不是悄悄多一行。
const KNOWN_DUPLICATED_BODIES: [&str; 0] = [];

/// 这条门自己。它把签名当字符串写着（判据要看的就是那几行），所以在真树上会被
/// 自己点名——**只摘这一面**：下面要求它至少还认得出自己的几份标本，判据哪天退化成
/// 什么都认不出，这一条先红，而不是让摘除悄悄掩盖掉一个瞎了的判据。
const THE_PREDICATE: &str =
    "crates/cogneva/tests/the_fixture_reading_steps_are_written_once_test.rs";

/// 跨 crate 的夹具支撑：几个 crate 的夹具都要的同一件事（哪个 PostgreSQL、哪个
/// Redis、没有的时候停还是跳过）。它判的是「这份文件里定义过的体，夹具里不许再写一
/// 遍」——所以它的位置本身就是判据的一部分：搬进它，就要先从所有夹具里删干净。
const SUPPORT_HOME: &str = "crates/cog-test-support/src/lib.rs";

/// 支撑 crate 里必须真有的入口：每搬一族进来，就在这上面加一个名字。
///
/// 它挡的是上一族留下的那种空转：`support_copies` 判的是「夹具里没有第二份」，而一份
/// 定义被删空的支撑 crate 让这条判据永远成立——判据只证明了「别处没有」，没证明「这里
/// 有」。
const SUPPORT_ENTRY_POINTS: [&str; 3] = [
    "pub fn database_url()",
    "pub fn redis_url()",
    "pub fn redis_url_required()",
];

/// 支撑 crate 负责的那两个环境变量：`crates/` 里除了 `SUPPORT_HOME`，不许再有一处代码
/// 直接读它们。把名字收进常量不算搬走——常量指向的还是同一个名字，而一处拼错的地方不会
/// 红：`env::var` 读一个没人设置的名字，与「本来就没配」在调用点上完全同形。
const SUPPORT_ENV_NAMES: [&str; 2] = ["COGNEVA_TEST_DATABASE_URL", "COGNEVA_TEST_REDIS_URL"];

/// 够到支撑 crate 的调用点下限。今天 70 处。这一面判的是「没有第二处在读」，把调用点
/// 全删了它也一样绿，所以另一头要有底：这些人真的在用那个 crate。
const MIN_SUPPORT_CALLSITES: usize = 60;

/// 夹具里造控制面的那两行只该写在它这里：建一个 `DagExecutor`、再包一层控制面。
///
/// 六份夹具各写了一遍，而这些夹具问的是「同一个控制面对同一个 workspace 怎么答」。
/// 一边把标签换掉、或者补上了事件总线而另一边没补，两份就不再描述同一个对象——而两份
/// 都还是绿的，因为没人比较它们。产出码不在这一面里：`cog-orchestrator` 自己造它是
/// 它的本职，跨类边界不是副本。
const ORCHESTRATOR_HOME: &str = "crates/cogneva/tests/common/orchestrator.rs";

/// 走它的调用点下限。今天 9 处。
const MIN_ORCHESTRATOR_CALLSITES: usize = 8;

// 普查面：`crates/**/*.rs`，夹具与产出面一起算。
//
// `src/` 必须在里面：这一族抄件上一轮就长在 `src/` 的单测模块里（`startup_guard.rs`
// 自己那三行 `read`），而只扫夹具的判据对它是瞎的。`fsio.rs` 不 import 任何兄弟
// 模块，正是为了让 `src/` 也能按 `#[path]` 编它——判据能覆盖 `src/`，前提是那份
// 规则本身挂得过去。

/// 引用共享读取的文件下限。今天 18 份，取一半。
const MIN_READERS: usize = 8;

/// 引用共享遍历的文件下限。今天 17 份，取一半。这个数原来只有 2：那时共享的 `walk`
/// 只服务两处「按扩展名收一棵子树」的普查，其余十几份各写各的递归——判据看不见它们，
/// 下限也就跟着那个缺口一起小。递归收敛到一份之后，这个数就是「还有人在用它」的真读数。
const MIN_WALKERS: usize = 8;

/// 引用共享去注释的文件下限。今天 15 份。
const MIN_STRIPPERS: usize = 8;

/// 一次普查：副本、自走的递归、读文件数、三种消费者各几份。
struct Census {
    copies: Vec<(String, usize, String)>,
    recursions: Vec<(String, usize, String)>,
    files: usize,
    readers: usize,
    walkers: usize,
    strippers: usize,
    /// 函数体逐字相同、却定义在不止一个夹具文件里的那些：名字 →（文件:行, 名字）。
    duplicate_bodies: Vec<(String, Vec<String>)>,
    /// 支撑 crate 里已经有一份、夹具里又写了一遍的：（文件:行 名字, 支撑 crate 里的名字）。
    support_copies: Vec<(String, String)>,
    /// 支撑 crate 之外，直接读它负责的那两个环境变量的地方：`文件:行 这一行`。
    env_reads: Vec<String>,
    /// 够到支撑 crate 的调用点（含 `use`），判据人口的下限靠它。
    support_callsites: usize,
    /// 夹具里自己造控制面的地方：`文件:行 这一行`。
    control_builds: Vec<String>,
    /// 走 `ORCHESTRATOR_HOME` 的调用点（含 `use`）。
    orchestrator_callsites: usize,
}

/// 这个文件里有哪些函数自己在往下走目录：函数体里同时出现 `read_dir` 与它自己的名字。
///
/// 按**函数体**判而不是按函数头判，是因为这一族抄件连名字、参数、过滤条件都换过——
/// 它们的定义行谁都不像谁，只有「我调我自己、而且我在列举目录」这件事是一样的。
/// 函数体的起点在行首的 `fn`（容忍前面的 `pub`）：那是一条定义的位置，而函数名出现
/// 在字符串里的时候，它前面不会是行首。
///
/// 括号与花括号的配平走共享的那两条：注释里写着签名不算，字符串里的 `{` 也不算。
fn recursions_in(file: &str, text: &str) -> Vec<(String, usize, String)> {
    let code = strip(text, Dialect::Rust);
    let bytes = code.as_bytes();
    let mut out = Vec::new();
    let mut lineno = 0usize;
    let mut line_start = 0usize;
    for line in code.split('\n') {
        lineno += 1;
        let this_line = line_start;
        line_start += line.len() + 1;
        let trimmed = line.trim_start();
        let (lead, rest) = match trimmed.strip_prefix("fn ") {
            Some(rest) => (line.len() - trimmed.len(), rest),
            None => match trimmed
                .strip_prefix("pub ")
                .and_then(|r| r.strip_prefix("fn "))
            {
                Some(rest) => (line.len() - trimmed.len() + 4, rest),
                None => continue,
            },
        };
        let Some(params) = rest.find('(') else {
            continue;
        };
        let called = rest[..params].trim();
        if called.is_empty()
            || !called
                .chars()
                .all(|c| c == '_' || c.is_ascii_alphanumeric())
        {
            continue;
        }
        let Some(closed) = matching_paren(bytes, this_line + lead + 3 + params) else {
            continue;
        };
        let Some(body_at) = code[closed..].find('{').map(|at| closed + at) else {
            continue;
        };
        let Some(body_end) = matching_brace(bytes, body_at) else {
            continue;
        };
        let body = &code[body_at..body_end];
        if body.contains("read_dir") && body.contains(&format!("{called}(")) {
            out.push((file.to_string(), lineno, called.to_string()));
        }
    }
    out
}

/// 这份文件里每个**自由函数**的函数体指纹：`(名字, 行号, 去掉空白后的函数体)`。
///
/// 只看行首顶格的 `fn`：`impl` 里的方法缩进着，它们各自实现同一个 trait 的不同部分，
/// 是「这个 mock 该长什么样」而不是「这条规则该怎么写」，搬不动也不该搬。
///
/// 空白折叠掉，是因为抄的人会重新排版：缩进、换行、参数折行都会变，规则本身不变。
fn free_function_bodies(text: &str) -> Vec<(String, usize, String)> {
    let code = strip(text, Dialect::Rust);
    let bytes = code.as_bytes();
    let mut out = Vec::new();
    let mut line_start = 0usize;
    for (index, line) in code.split('\n').enumerate() {
        let this_line = line_start;
        line_start += line.len() + 1;
        let rest = ["fn ", "pub fn ", "async fn ", "pub async fn "]
            .iter()
            .find_map(|head| line.strip_prefix(head));
        let Some(rest) = rest else {
            continue;
        };
        let lead = line.len() - rest.len();
        let Some(params) = rest.find('(') else {
            continue;
        };
        let called = rest[..params].trim();
        if called.is_empty()
            || !called
                .chars()
                .all(|c| c == '_' || c.is_ascii_alphanumeric())
        {
            continue;
        }
        let Some(closed) = matching_paren(bytes, this_line + lead + params) else {
            continue;
        };
        let Some(body_at) = code[closed..].find('{').map(|at| closed + at) else {
            continue;
        };
        let Some(body_end) = matching_brace(bytes, body_at) else {
            continue;
        };
        let body = code[body_at..body_end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        // 太短的（`fn x() { 1 }`、转发用的壳）不算一条规则：它们撞在一起是巧合，
        // 而共享一个两行的壳换不来任何一致性。
        if body.len() < 80 {
            continue;
        }
        out.push((called.to_string(), index + 1, body));
    }
    out
}

/// 这份文件从哪个 `::{…}` 里导入了哪些名字。
///
/// 跨行的导入也要读得到：夹具里常见的是
/// `use common::routes::{read, repo_root, strip_comments, walk};` 折成两三行。
fn imported_names(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("::{") {
        let start = cursor + at + 3;
        let Some(len) = text[start..].find('}') else {
            break;
        };
        for part in text[start..start + len].split(',') {
            let word = part.trim();
            if !word.is_empty() {
                out.push(word.to_string());
            }
        }
        cursor = start + len;
    }
    out
}

fn census(root: &std::path::Path, files_in: &[PathBuf]) -> Census {
    let mut copies = Vec::new();
    let mut recursions = Vec::new();
    let mut env_reads: Vec<String> = Vec::new();
    let mut support_callsites = 0usize;
    let mut control_builds: Vec<String> = Vec::new();
    let mut orchestrator_callsites = 0usize;
    let mut files = 0usize;
    let (mut readers, mut walkers, mut strippers) = (0usize, 0usize, 0usize);
    // 函数体 → 出现它的「文件:行 名字」。按体分组，所以同一个体换了名字也照样并到一起。
    let mut bodies: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    // 支撑 crate 那一面的两半：它自己定义了哪些体（体 → 名字），以及别的夹具都写了
    // 哪些体（「文件:行 名字」, 体）。两半都在这一趟里收，收完再配对——这样判据读的
    // 就是**这棵树**里的两份，而不是从盘上另取一份可能不存在的东西。
    let mut support_bodies: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    let mut fixture_bodies: Vec<(String, String)> = Vec::new();
    let shared = [THE_ONE, ROUTES_HOME, PRODUCTION_HOME];
    for path in files_in {
        let name = relative_name(root, path);
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        files += 1;
        // 递归只该住在共享的遍历里——**在夹具里**。面裁在这里不是图省事：产出码够不着
        // 测试 harness，那些遍历各带着自己的领域逻辑（拷贝备份树、收配置片段、找模板
        // 文件），把它们也算上就是让这条门红在没人能改对的地方——跨类边界不是副本。
        // 夹具则相反：同一个 crate 的夹具共享一个 `common`，第二处递归按构造就是可搬的。
        //
        // 这份门自己写着标本（那些字符串里也有 `fn …`），所以它自己也摘出去；它认得
        // 这个形状由下面那条消融单独证明。
        if name == SUPPORT_HOME {
            for (function, _, body) in free_function_bodies(&text) {
                support_bodies.insert(body, function);
            }
        }
        if name.contains("/tests/") && name != THE_PREDICATE && name != THE_ONE {
            recursions.extend(recursions_in(&name, &text));
            // 同体只在**本 crate 的夹具**之间比。跨 crate 的抄件（`database_url` 曾经在
            // 四个 crate 的四十二份夹具里，`migrate_once` 在三份里）是同一族，但它们的
            // 搬法不是「挪进别人的 `common`」，而是**另开一个跨 crate 的支撑 crate**
            // ——那一半由下面 `support_copies` 那一面判：支撑 crate 里定义过的体，夹具里
            // 不许再出现一遍。所以这里只管「同一棵树内、可搬而未搬」的那一半。
            if name.starts_with("crates/cogneva/tests/") {
                for (function, line, body) in free_function_bodies(&text) {
                    bodies
                        .entry(body)
                        .or_default()
                        .push(format!("{name}:{line} {function}"));
                }
            }
            // 支撑 crate 那一面：任何 crate 的夹具都算。跨 crate 的抄件正是要在这里红，
            // 它们的共同点是「两边都已经够得着同一份」——只差没人去够。配对放在这一趟
            // 走完之后：支撑 crate 排在遍历里的哪一位都好使。
            for (function, line, body) in free_function_bodies(&text) {
                fixture_bodies.push((format!("{name}:{line} {function}"), body));
            }
        }
        // 支撑 crate 负责的那两个名字：别处直接读它就是要红的那一处。这一面按**读的
        // 形状**判（`env::var("名字")`），不按名字出现在哪一行判——`expect` 的消息里
        // 写着同一个名字是在告诉人怎么设它，不是第二处在读。
        if name != SUPPORT_HOME {
            for (index, line) in text.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                if SUPPORT_ENV_NAMES
                    .iter()
                    .any(|var| line.contains(&format!("env::var(\"{var}\")")))
                {
                    env_reads.push(format!("{name}:{} {}", index + 1, line.trim()));
                }
            }
        }
        for form in [
            "cog_test_support::database_url",
            "cog_test_support::redis_url",
        ] {
            support_callsites += text.matches(form).count();
        }
        // 造控制面：只在夹具面上判，产出码里它是本职。这一面按**构造**判——两份抄件
        // 的变量名、缩进都可以不一样，但「在这里 new 一个控制面」这件事是一样的。
        // 这份门自己写着标本（那串判据里就有 `OrchestratorControlImpl::new`），
        // 所以它自己也摘出去——和上面那一族同一个理由。
        if name.contains("/tests/") && name != ORCHESTRATOR_HOME && name != THE_PREDICATE {
            for (index, line) in text.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                if line.contains("OrchestratorControlImpl::new") {
                    control_builds.push(format!("{name}:{} {}", index + 1, line.trim()));
                }
            }
        }
        orchestrator_callsites += text.matches("orchestrator::control").count();
        let imported = imported_names(&text);
        if name != THE_ONE {
            if imported.contains(&"read".to_string()) || text.contains("routes::read") {
                readers += 1;
            }
            if imported.contains(&"walk".to_string()) || text.contains("routes::walk") {
                walkers += 1;
            }
        }
        if !shared.contains(&name.as_str()) && text.contains("strip_comments") {
            strippers += 1;
        }
        // 内联那一种只在夹具面上判：`crates/<crate>/src/` 里的同一串字节是另一件事。
        let in_fixtures = name.contains("/tests/");
        for (index, line) in text.lines().enumerate() {
            // 注释行跳过：说明里写着这个签名，不等于又抄了一份。
            if line.trim_start().starts_with("//") {
                continue;
            }
            if shared.contains(&name.as_str()) {
                continue;
            }
            let copied = COPIED_FORMS.iter().any(|form| line.contains(form))
                || (in_fixtures && INLINE_FORMS.iter().any(|form| line.contains(form)));
            if copied {
                copies.push((name.clone(), index + 1, line.trim().to_string()));
            }
        }
    }
    let support_copies: Vec<(String, String)> = fixture_bodies
        .into_iter()
        .filter_map(|(site, body)| Some((site, support_bodies.get(&body)?.clone())))
        .collect();
    // 只在**不同文件**之间算重复：同一份文件里两个同体的小函数不是「规则写了两遍」。
    let duplicate_bodies = bodies
        .into_iter()
        .filter(|(_, at)| {
            let files: std::collections::BTreeSet<&str> = at
                .iter()
                .map(|one| one.split(':').next().unwrap_or(one))
                .collect();
            files.len() >= 2
        })
        .collect();
    Census {
        copies,
        recursions,
        files,
        readers,
        walkers,
        strippers,
        duplicate_bodies,
        support_copies,
        env_reads,
        support_callsites,
        control_builds,
        orchestrator_callsites,
    }
}

/// `crates/` 下每一个 `.rs`，不设下限——调用点自己带下限，也给假树留一条路。
fn all_files(root: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    common::routes::walk(root.join("crates").as_path(), &["rs"], &mut out);
    out.sort();
    out
}

#[test]
fn the_fixture_reading_steps_are_written_once() {
    let root = repo_root();
    let found = census(&root, &all_files(&root));

    // 下限落在题目上：读不到文件时，「副本为零」是一句关于空集的话。今天 1075 份。
    assert!(
        found.files >= 800,
        "只读到 {} 个 crate 源文件（下限 800）：遍历没走到，下面的「没有副本」不算数",
        found.files
    );
    // 摘掉这份门自己之前先问一句判据还认不认得出它。判据若是瞎的，下面的清单为空
    // 与「全仓只剩一处」同形——那正是这条门存在的理由。
    let own: Vec<&(String, usize, String)> = found
        .copies
        .iter()
        .filter(|(file, _, _)| file == THE_PREDICATE)
        .collect();
    let shapes = COPIED_FORMS.len() + INLINE_FORMS.len();
    assert!(
        own.len() >= shapes,
        "这条门自己至少写着 {shapes} 份标本（每一种形状各一行），读到 {} 处：{:?}。\
         少一份说明判据对那种形状已经瞎了，那么把它摘掉就只是在掩盖它",
        own.len(),
        own
    );

    let elsewhere: Vec<&(String, usize, String)> = found
        .copies
        .iter()
        .filter(|(file, _, _)| file != THE_PREDICATE)
        .collect();
    assert!(
        elsewhere.is_empty(),
        "这些地方又抄了一份共享的读取 / 遍历 / 去注释 / 配平括号 / 取相对名：{elsewhere:?}\n\
         改用 `common::routes::read`、`common::routes::walk`、\
         `common::routes::strip_comments`、`common::routes::matching_brace`（或 \
         `matching_paren`）与 `common::rust_production::relative_name`。抄件的注释里\
         也写着「读不出来就 panic」，但那是注释不是机制——把它换成 `else {{ continue }}` \
         的那一份不会红，也不会有人发现另外几份已经和它不一样了。"
    );

    // 递归这一面：整个普查面上只该有 `fsio` 那一处自走目录。剩下的实现都在它上面
    // 铺（按扩展名收、跳过某些目录、顺手解析），各有各的过滤，但「怎么往下走」只有
    // 一个答案。第二处一旦长出来，读不出来的子目录在一份里 panic、在另一份里被跳过，
    // 而两份都绿。
    assert!(
        found.recursions.is_empty(),
        "这些地方自己写了递归下目录的遍历（函数体里同时出现 `read_dir` 与自己的名字）：\
         {recursions:?}\n改用 `common::routes::walk`（按扩展名收）或 \
         `common::routes::walk_files`（要不收某个目录就传 `skip`）——递归本身只该有 \
         `{THE_ONE}` 里那一份。它们谁都不逐字像谁，所以按签名判的那几条对它们是瞎的，\
         而「读不出来就 panic」这句话在各自的抄件里都只是一句注释。",
        recursions = found.recursions
    );

    // 函数体这一面：两段体去掉空白之后逐字相同、却写在两个夹具文件里，就是同一条规则
    // 的第二份。这一族抄件按名字判全瞎——`is_word` 与 `is_component` 是同一个判据的
    // 两个名字，`split_arms` 与 `split_items` 也是；抄的人顺手改名，按定义名找永远找不全。
    let known = KNOWN_DUPLICATED_BODIES;
    assert!(
        known.is_empty(),
        "已知同体表又长回了 {} 条：这一族今天一条不剩，新长出来的同体应该搬进 `common/`，\
         而不是登记一笔",
        known.len()
    );
    let name_of = |site: &str| site.rsplit(' ').next().unwrap_or(site).to_string();
    let mut unknown: Vec<(&Vec<String>, &String)> = Vec::new();
    for (body, at) in &found.duplicate_bodies {
        if at
            .iter()
            .all(|site| known.contains(&name_of(site).as_str()))
        {
            continue;
        }
        unknown.push((at, body));
    }
    assert!(
        unknown.is_empty(),
        "这些地方把同一段函数体写了两遍（去掉空白后逐字相同）：{unknown:?}\n\
         搬进 `common/` 里对应的一份（读 Rust 文本的那几条在 `common::rust_text`，\
         走目录 / 读文件在 `common::routes`，普查产出的在 `common::rust_production`）。\
         名字不可靠：抄的人顺手改名，所以这条判据按体判、不按名字判。"
    );
    // 反过来问：表里每一条都还数得出来吗。合并掉之后忘了删表，这一问先红——那张表
    // 留着旧名字，下一次真的长回来时就会被它悄悄放过去。
    for name in known {
        assert!(
            found
                .duplicate_bodies
                .iter()
                .any(|(_, at)| at.iter().any(|site| name_of(site) == name)),
            "`{name}` 已经不在「两个夹具各写一遍」的清单里了：合并完就从 \
             KNOWN_DUPLICATED_BODIES 里删掉它，那张表今天应该是空的"
        );
    }

    // 跨 crate 那一面：支撑 crate 里已经有一份的规则，夹具里不许再写一遍。这一面没有
    // 豁免表——它今天一条也没有，而且每一份抄件都够得着同一个 `cog_test_support`，
    // 不存在「搬不动」的那种。
    assert!(
        found.support_copies.is_empty(),
        "这些函数在 `{SUPPORT_HOME}` 里已经有一份，夹具里又写了一遍：{:?}\n\
         改成 `use cog_test_support::…`：两份之间没有任何东西比较，一边改了另一边不会红。",
        found.support_copies
    );
    // 反过来问：支撑 crate 真的还定义着那个入口吗。它空了、或者被删成一个转发壳，
    // 上面那条就永远绿——判据只是证明了「别处没有」，没证明「这里有」。
    // 同一条规则的另一种形状：不写成函数、直接内联在用例里。按函数体判的那一面看不
    // 见它，而它恰恰是最容易长回来的那种——写的人不觉得自己在「定义一份夹具」。
    assert!(
        found.env_reads.is_empty(),
        "这些地方绕开了支撑 crate，直接读它负责的环境变量：{:?}\n\
         改成 `use cog_test_support::…`：第二处读同一个名字的时候，改了支撑 crate 里的\
         写法不会红，两边只是各读各的。",
        found.env_reads
    );
    assert!(
        found.support_callsites >= MIN_SUPPORT_CALLSITES,
        "只有 {} 处够到 `cog_test_support`（下限 {MIN_SUPPORT_CALLSITES}）：那么「夹具里\
         不许再写一遍」那几条读的是一个没人用的 crate，绿得没有题目",
        found.support_callsites
    );
    // 同一条规则的第三种形状：不是读地址，是**造对象**。六份夹具各写过一遍「建执行器、
    // 包一层控制面」，而它们问的是同一个控制面在同一个 workspace 上怎么答——其中一份
    // 补了事件总线、另一份没补，两份就描述的不是同一个对象了，而两份都还是绿的。
    assert!(
        found.control_builds.is_empty(),
        "这些夹具自己造了控制面：{:?}\n\
         改成 `use common::orchestrator::…`：两份写在两处的时候，改了其中一份的另一半\
         不会红。",
        found.control_builds
    );
    assert!(
        found.orchestrator_callsites >= MIN_ORCHESTRATOR_CALLSITES,
        "只有 {} 处走 `ORCHESTRATOR_HOME`（下限 {MIN_ORCHESTRATOR_CALLSITES}）：那么\
         「夹具里不许自己造」读的是一个没人用的构造函数",
        found.orchestrator_callsites
    );
    let support = std::fs::read_to_string(root.join(SUPPORT_HOME)).expect("支撑 crate 读得到");
    let missing: Vec<&str> = SUPPORT_ENTRY_POINTS
        .iter()
        .copied()
        .filter(|entry| !support.contains(entry))
        .collect();
    assert!(
        missing.is_empty(),
        "{SUPPORT_HOME} 里不再定义 {missing:?}：那么「夹具里不许再写一遍」就是指着\
         一份不干这件事的文件说的"
    );

    // 反过来问：被指认的那一处真的还在干这件事吗。少了这一问，把它换成空壳之后
    // 本判据照绿——它只证明了「别处没有」。
    let the_one = std::fs::read_to_string(root.join(THE_ONE)).expect("共享遍历读得到");
    assert!(
        the_one.contains("pub fn read(path: &Path) -> String")
            && the_one.contains("extensions: &[&str]"),
        "{THE_ONE} 里不再定义这两个函数：那么「只有一处」就是指着这份不干这件事的文件"
    );
    assert!(
        the_one.contains("panic!") && the_one.contains("read_dir"),
        "{THE_ONE} 里的遍历不再 panic：读不出来时安静地少读一块，判出来的结论与\
         「那一块都合规」同形"
    );
    // 而且只能有一处 `read_dir`：这一份文件里再长出一个递归，就是同一条规则在
    // 「唯一的那一份」内部又抄了一次——普查面上看不见它，因为这一份本来就被摘出去。
    assert_eq!(
        the_one.matches("read_dir").count(),
        1,
        "{THE_ONE} 里有不止一处 `read_dir`：共享的遍历自己又长出了第二个递归，\
         而它落在被摘出去的那份文件里，普查面上永远看不见"
    );
    let routes_home = std::fs::read_to_string(root.join(ROUTES_HOME)).expect("共享去注释读得到");
    assert!(
        routes_home.contains("pub fn strip_comments(text: &str) -> String"),
        "{ROUTES_HOME} 里不再定义 `strip_comments`：「只有一处」就是指着这份\
         不干这件事的文件"
    );
    let lex_home = std::fs::read_to_string(root.join(LEX_HOME)).expect("共享词法读得到");
    assert!(
        lex_home.contains("block_comment += 1")
            && lex_home.contains("raw_hashes")
            && lex_home.contains("fn scan(text: &str, dialect: Dialect) -> Scan"),
        "{LEX_HOME} 里的扫描不再按嵌套层数与原始字符串走：Rust 的块注释可以嵌套、\
         原始字符串里可以有任意括号，只看第一个 `*/` 或 `\"` 会把后半截读反，\
         而读反的那一半在判词上与「那里没有」同形"
    );
    // 括号那一份：两条入口都还在，而且都还走在同一个数层数的实现上。少这一问，
    // 把某一条改成「找最近的 `}`」之后，本判据只证明了「没有第二份」，不证明这一份对。
    assert!(
        routes_home.contains("pub fn matching_paren(bytes: &[u8], open: usize)")
            && routes_home.contains("pub fn matching_brace(bytes: &[u8], open: usize)"),
        "{ROUTES_HOME} 里不再定义这两条括号读取：「只有一处」就是指着这份不干这件事的文件"
    );
    assert!(
        routes_home.contains("fn matching_delimiter(")
            && routes_home.contains("matching_delimiter(bytes, open, b'{', b'}')"),
        "{ROUTES_HOME} 里两条括号读取不再走同一个数层数的实现：两条各写一遍，就是\
         这份文件内部又长出了第二份规则——同一条缺陷换了个位置"
    );
    // 相对名那一份：抄件缺的正是这行 `./` 折叠，所以共享的这一份必须还带着它。
    let production_home =
        std::fs::read_to_string(root.join(PRODUCTION_HOME)).expect("共享取相对名读得到");
    assert!(
        production_home.contains("pub fn relative_name(root: &Path, path: &Path) -> String"),
        "{PRODUCTION_HOME} 里不再定义 `relative_name`：「只有一处」就是指着这份\
         不干这件事的文件"
    );
    assert!(
        production_home.contains("strip_prefix(\"./\")"),
        "{PRODUCTION_HOME} 里的相对名不再折叠开头的 `./`：同一条路径在表里记成 \
         `crates/x.rs` 还是在比对时读成 `./crates/x.rs`，是两种答案，而两种都不会红"
    );

    assert!(
        found.readers >= MIN_READERS,
        "只有 {} 份文件在用共享的 `read`（下限 {MIN_READERS}）：规则定义了一处，却几乎\
         没人读它——那与没定义同形",
        found.readers
    );
    assert!(
        found.walkers >= MIN_WALKERS,
        "只有 {} 份文件在用共享的 `walk`（下限 {MIN_WALKERS}）：遍历被搬走了，\
         或者被绕开了",
        found.walkers
    );
    assert!(
        found.strippers >= MIN_STRIPPERS,
        "只有 {} 份文件在用共享的 `strip_comments`（下限 {MIN_STRIPPERS}）：\
         去注释的写法被搬走了，或者被绕开了",
        found.strippers
    );
}

/// 判据自检：真搭一棵树，把各种写法放进去，看它认不认得出。
///
/// 只断真树的那一条证明不了它还会响——真树今天恰好干净，而干净与瞎同形。
#[test]
fn a_copy_on_disk_is_reported() {
    let root = std::env::temp_dir().join(format!("census-read-walk-{}", std::process::id()));
    let dir = root.join("crates/probe/tests");
    std::fs::create_dir_all(&dir).expect("a scratch tests directory");

    std::fs::write(
        dir.join("consumer.rs"),
        "mod common;\nuse common::routes::{read, strip_comments, walk};\nfn f(p: &std::path::Path) { let _ = read(p); }\n",
    )
    .expect("a consumer");
    let found = census(&root, &all_files(&root));
    assert!(found.copies.is_empty(), "借共享的那一份不该被点名");
    assert_eq!(found.readers, 1, "读文件的人要数得出来");
    assert_eq!(found.walkers, 1, "走目录的人要数得出来");
    assert_eq!(found.strippers, 1, "去注释的人要数得出来");

    // 只在注释里写着签名：不是副本。
    std::fs::write(
        dir.join("mention.rs"),
        "// fn read(path: &Path) -> String { std::fs::read_to_string(path).unwrap() }\npub const X: u32 = 1;\n",
    )
    .expect("a mention");
    let found = census(&root, &all_files(&root));
    assert!(
        found.copies.is_empty(),
        "注释里的签名不该算成副本：{:?}",
        found.copies
    );

    // 抄过去的那几份，都要点名，且要带能翻到的行号。括号那一份与相对名那一份按抄件
    // 当时的样子写：参数是 `&str`，名字是 `relative_name`——判据要认的是这些形状。
    std::fs::write(
        dir.join("copy.rs"),
        "fn read(path: &Path) -> String {\n    std::fs::read_to_string(path).unwrap()\n}\n\nfn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {\n    let _ = (dir, extensions, out);\n}\n\nfn strip_comments(text: &str) -> String {\n    text.to_string()\n}\n\nfn matching_brace(text: &str, open: usize) -> Option<usize> {\n    let _ = (text, open);\n    None\n}\n\nfn relative_name(root: &Path, path: &Path) -> String {\n    let _ = root;\n    path.display().to_string()\n}\n\nfn inline(root: &Path, path: &Path) -> String {\n    path.strip_prefix(root).unwrap_or(path).to_string_lossy().replace('\\\\', \"/\")\n}\n\nfn walk_all(dir: &Path, out: &mut Vec<PathBuf>) {\n    let _ = (dir, out);\n}\n\nfn walk_files(dir: &Path, extensions: &[&str], skip: &[&str], out: &mut Vec<PathBuf>) {\n    let _ = (dir, extensions, skip, out);\n}\n\nfn read(rel: &str) -> String {\n    let _ = rel;\n    String::new()\n}\n\nfn read(relative: &str) -> String {\n    let _ = relative;\n    String::new()\n}\n",
    )
    .expect("ten copies");
    let found = census(&root, &all_files(&root));
    assert_eq!(
        found.copies.len(),
        10,
        "十份抄件都要数得出来：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 5),
        "报出来的位置要能直接翻到（`walk` 那一处在第 5 行）：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 9),
        "去注释那一份也要点名（第 9 行）：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 13),
        "括号那一份也要点名（第 13 行）：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 18),
        "相对名那一份也要点名（第 18 行）：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 24),
        "内联那一份也要点名（第 24 行：摊开的那一行，不是函数头）：按定义名判的\
         判据看不见它：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 27),
        "`walk_all` 那一份也要点名（第 27 行）：新长出来的第二个入口，按签名判的\
         判据认不出它：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 31),
        "`walk_files` 那一份也要点名（第 31 行）：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 35),
        "收相对名的那一份 `read(rel)` 也要点名（第 35 行）：它只是参数名换了，\
         按 `&Path` 那条签名判的判据看不见它：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 40),
        "`read(relative)` 那一份也要点名（第 40 行）：抄的人连参数名都换过一遍：{:?}",
        found.copies
    );

    // 抄件长在 `src/` 的单测模块里也要点名：这一族抄件的上一轮就长在那儿
    // （`startup_guard.rs` 的三行 shim），只扫夹具的判据对它一声不响。
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).expect("a scratch src directory");
    std::fs::write(
        src.join("unit.rs"),
        "fn read(path: &Path) -> String {\n    std::fs::read_to_string(path).unwrap()\n}\n",
    )
    .expect("a copy in a unit test module");
    let found = census(&root, &all_files(&root));
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/src/unit.rs" && *line == 1),
        "`src/` 里的抄件也要数得出来，否则这份门只看得见夹具：{:?}",
        found.copies
    );

    // 同一串内联字节长在 `src/` 里就不算：那是产出码把路径变成线上的名字，各用
    // 各的上下文，而共享的这份住在测试 harness 里，产出码够不着它。判据若把这一类
    // 也算上，它会红在没人能改对的地方。
    std::fs::write(
        src.join("inline.rs"),
        "pub fn wire(path: &std::path::Path) -> String {\n    path.display().to_string().replace('\\\\', \"/\")\n}\n",
    )
    .expect("production byte, not a copy");
    let found = census(&root, &all_files(&root));
    assert!(
        !found
            .copies
            .iter()
            .any(|(file, _, _)| file == "crates/probe/src/inline.rs"),
        "产出码里的 posix 折叠不是这一族的副本：{:?}",
        found.copies
    );

    std::fs::remove_dir_all(&root).ok();
}

/// 判据自检：**自走的递归**这个形状认不认得出。
///
/// 真树上今天一处都没有（递归只住在 `fsio` 里，而那一份被摘出去），而「一处都没有」
/// 与「判据瞎了」同形。这里搭一棵树，把三种容易混淆的写法放进去：借共享的、只下一层
/// 的、和整段注释掉的——它们都不该点名；只有自己调自己的那一个该点名。
#[test]
fn a_recursion_on_disk_is_reported() {
    let root = std::env::temp_dir().join(format!("census-recursion-{}", std::process::id()));
    let dir = root.join("crates/probe/tests");
    std::fs::create_dir_all(&dir).expect("a scratch tests directory");

    std::fs::write(
        dir.join("borrower.rs"),
        "use common::routes::walk;\nfn files(root: &Path, out: &mut Vec<PathBuf>) {\n    walk(&root.join(\"web\"), &[\"ts\", \"tsx\"], out);\n}\n",
    )
    .expect("a borrower");
    std::fs::write(
        dir.join("shallow.rs"),
        "fn declarations(dir: &Path, out: &mut Vec<PathBuf>) {\n    for entry in std::fs::read_dir(dir).expect(\"list\").flatten() {\n        let path = entry.path();\n        if path.extension().is_some_and(|e| e == \"yaml\") {\n            out.push(path);\n        }\n    }\n}\n",
    )
    .expect("a one-level listing");
    std::fs::write(
        dir.join("commented.rs"),
        "// fn collect_yaml(dir: &Path, out: &mut Vec<PathBuf>) {\n//     std::fs::read_dir(dir).expect(\"list\");\n//     collect_yaml(dir, out);\n// }\n",
    )
    .expect("a commented-out recursion");

    let found = census(&root, &all_files(&root));
    assert!(
        found.recursions.is_empty(),
        "借共享的、只下一层的、注释掉的那三种都不该点名：{:?}",
        found.recursions
    );

    // 自己调自己，名字、参数、过滤条件全换过——这一份必须点名，且要带得回行号。
    std::fs::write(
        dir.join("walker.rs"),
        "fn collect_yaml(dir: &Path, out: &mut Vec<PathBuf>) {\n    for entry in std::fs::read_dir(dir).expect(\"list\") {\n        let path = entry.expect(\"entry\").path();\n        if path.is_dir() {\n            collect_yaml(&path, out);\n        } else if path.extension().is_some_and(|e| e == \"yaml\") {\n            out.push(path);\n        }\n    }\n}\n",
    )
    .expect("a hand-rolled recursion");
    let found = census(&root, &all_files(&root));
    assert_eq!(
        found.recursions.len(),
        1,
        "只该点名自己调自己的那一个：{:?}",
        found.recursions
    );
    assert_eq!(
        found.recursions[0],
        (
            "crates/probe/tests/walker.rs".to_string(),
            1,
            "collect_yaml".to_string()
        ),
        "要点名到文件和行号，且报出那个自己调自己的函数名（它按定义名判不出来）"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// 判据自检：**同一段函数体写在两个夹具里**这个形状认不认得出。
///
/// 真树上今天只剩 `KNOWN_DUPLICATED_BODIES` 里那几条，而「一条新的都没有」与「判据
/// 瞎了」同形。这里搭一棵树，把该点名的那一种和三种不该点名的混在一起：借共享的、
/// 同一份文件里写两遍的、以及 `impl` 里缩进着的方法。抄的人会顺手改名，所以两份
/// 标本故意用两个不同的函数名。
#[test]
fn a_body_written_twice_is_reported() {
    let root = std::env::temp_dir().join(format!("census-bodies-{}", std::process::id()));
    let dir = root.join("crates/cogneva/tests");
    std::fs::create_dir_all(&dir).expect("a scratch tests directory");

    // 借共享的：一份定义都没有，不该点名。
    std::fs::write(
        dir.join("borrower.rs"),
        "use common::rust_text::snake_case;\nfn fold(names: &[String]) -> Vec<String> {\n    names.iter().map(|n| snake_case(n)).collect()\n}\n",
    )
    .expect("a borrower");
    // 同一份文件里写两遍：不是「规则写了两遍」，是这份文件里两个同名的形状。
    std::fs::write(
        dir.join("twice_in_one_file.rs"),
        "fn first(text: &str) -> String {\n    let mut out = String::new();\n    for (i, ch) in text.char_indices() {\n        if i > 0 { out.push('_'); }\n        out.push(ch);\n    }\n    out\n}\n\nfn second(text: &str) -> String {\n    let mut out = String::new();\n    for (i, ch) in text.char_indices() {\n        if i > 0 { out.push('_'); }\n        out.push(ch);\n    }\n    out\n}\n",
    )
    .expect("two copies in one file");
    // `impl` 里缩进着的方法：各实现各的 trait，搬不动也不该搬。
    std::fs::write(
        dir.join("mock.rs"),
        "struct Mock;\nimpl Quota for Mock {\n    async fn get_user_summary(&self, id: &str) -> Summary {\n        Summary { target_id: id.to_string(), target_type: \"user\".into(), total: u64::MAX }\n    }\n}\n",
    )
    .expect("a trait impl");

    let found = census(&root, &all_files(&root));
    assert!(
        found.duplicate_bodies.is_empty(),
        "借共享的、同一份文件里两遍的、`impl` 里的那三种都不该点名：{:?}",
        found.duplicate_bodies
    );

    // 一份长同一段体、两个不同的文件、两个不同的名字——按定义名判的判据全看不见它。
    let body = "    let mut out = Vec::new();\n    for line in text.lines() {\n        let trimmed = line.trim();\n        if trimmed.is_empty() || trimmed.starts_with('#') {\n            continue;\n        }\n        out.push(trimmed.to_string());\n    }\n    out\n";
    std::fs::write(
        dir.join("first.rs"),
        format!("fn kept_lines(text: &str) -> Vec<String> {{\n{body}}}\n"),
    )
    .expect("the first copy");
    std::fs::write(
        dir.join("second.rs"),
        format!("fn meaningful(text: &str) -> Vec<String> {{\n{body}}}\n"),
    )
    .expect("the second copy");

    let found = census(&root, &all_files(&root));
    assert_eq!(
        found.duplicate_bodies.len(),
        1,
        "只该点名那一对（换了名字的同一段体）：{:?}",
        found.duplicate_bodies
    );
    let (_, at) = &found.duplicate_bodies[0];
    assert!(
        at.iter()
            .any(|site| site.ends_with("first.rs:1 kept_lines"))
            && at
                .iter()
                .any(|site| site.ends_with("second.rs:1 meaningful")),
        "两份都要点名到文件与行号，哪怕它们的函数名不一样：{at:?}"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// 同一段字节，两种方言的读法不同——这正是方言必须由调用方说出来的原因。
///
/// Rust 的生命周期在「`'` 一律是引号」的读法下会开一段字符串，把后面的代码吃到下一个
/// `'` 为止；JavaScript 的长单引号字符串在 Rust 的窗口判据下认不出来。两边各自的错法
/// 都不会报错，只会少读——所以这里把两种方言在同一段文本上钉住。
#[test]
fn the_one_lexer_reads_rust_where_the_javascript_rule_does_not() {
    // `'a` 是生命周期，不是字符字面量的开头：它后面的代码必须还是代码。
    let source = "fn f<'a>(x: &'a str) -> u8 { let n = 1; n }";
    assert!(
        strip(source, Dialect::Rust).contains("let n = 1;"),
        "{:?}",
        strip(source, Dialect::Rust)
    );
    assert_eq!(
        brace_delta(source, Dialect::Rust),
        0,
        "一行的函数体里括号自平：{:?}",
        strip(source, Dialect::Rust)
    );

    // 原始字符串里的 `"` 与 `//` 都不是语法：整段是一条字面量。
    let raw = "let s = r#\"a \" b // c\"#; let after = 2;";
    let stripped = strip(raw, Dialect::Rust);
    assert!(
        stripped.contains("let after = 2;"),
        "原始字符串在它自己那个 `\"#` 上收尾，里面那个 `\"` 不算收尾：{stripped:?}"
    );

    // 反过来：JavaScript 的长单引号字符串必须整段认成字符串，里面的括号不进括号形状。
    let js = "const p = '/api/v1/things/{id}'; const q = 1;";
    assert_eq!(strip(js, Dialect::JavaScript), js);
    assert_eq!(
        brace_delta(js, Dialect::JavaScript),
        0,
        "字符串里的花括号不是嵌套"
    );

    // 空掉字面量那一份：注释与字符串都变成空格，换行与行数原样。
    let blanked = blank("let a = \"x\\ny\"; // note\nlet b = 2;", Dialect::Rust);
    assert_eq!(
        blanked.lines().count(),
        2,
        "行数要留住，否则报行号的读者指不到地方：{blanked:?}"
    );
    assert!(
        blanked.contains("let b = 2;") && !blanked.contains("note"),
        "{blanked:?}"
    );
}
