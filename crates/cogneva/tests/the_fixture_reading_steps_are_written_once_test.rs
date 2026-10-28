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
//! 断言各自单独会长回来：副本为零、递归只有一处、共享模块还在真干活、还有人在用它。

mod common;

use std::path::PathBuf;

use common::routes::{matching_brace, matching_paren, repo_root, strip_comments};
use common::rust_production::relative_name;

/// 读文件、走目录这两条的唯一定义。
const THE_ONE: &str = "crates/cogneva/tests/common/fsio.rs";

/// 去注释、配平括号这两条的定义在 `routes` 里——它们要读同一批源码，和注册路线的
/// 读法住在一起。
const ROUTES_HOME: &str = "crates/cogneva/tests/common/routes.rs";

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

/// 这条门自己。它把签名当字符串写着（判据要看的就是那几行），所以在真树上会被
/// 自己点名——**只摘这一面**：下面要求它至少还认得出自己的几份标本，判据哪天退化成
/// 什么都认不出，这一条先红，而不是让摘除悄悄掩盖掉一个瞎了的判据。
const THE_PREDICATE: &str =
    "crates/cogneva/tests/the_fixture_reading_steps_are_written_once_test.rs";

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
    let code = strip_comments(text);
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
    let mut files = 0usize;
    let (mut readers, mut walkers, mut strippers) = (0usize, 0usize, 0usize);
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
        if name.contains("/tests/") && name != THE_PREDICATE && name != THE_ONE {
            recursions.extend(recursions_in(&name, &text));
        }
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
    Census {
        copies,
        recursions,
        files,
        readers,
        walkers,
        strippers,
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
    assert!(
        routes_home.contains("depth") && routes_home.contains("chars[i] == '\\n'"),
        "{ROUTES_HOME} 里的去注释不再按嵌套层数走：Rust 的块注释可以嵌套，\
         只看第一个 `*/` 会把后半截注释当成代码读"
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
