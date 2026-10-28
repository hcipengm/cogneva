//! 脚本里「往上数几级就是仓库根」那行，数出来的必须是仓库根。
//!
//! `deploy/` 与 `.github/scripts/` 下一共六十余处同一个写法：
//!
//! ```text
//! ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
//! ```
//!
//! 以及三处 Python 的 `Path(__file__).resolve().parents[2]`。它们表达的是同一条
//! 规则——「本文件所在的目录往上两级即检出根」——而层级是**写死的数字**，与文件
//! 当前所在深度之间没有任何东西在比较。把其中一份脚本挪进子目录，那行不会报错：
//! 它算出来是 `deploy/`，脚本接着按「仓库根」去读 `deploy/crates/...` 这样不存在的
//! 路径。门读不到东西时通常报「没有题目」而不是「读错了树」——绿得和真的没有漂移
//! 一样。这不是假想的失败：上一笔修掉的 `/root/omc_workspace` 就是把一条逐机不同
//! 的路径写成了字面量，而它一直没被读出来，只因为那两条脚本平时没人跑。
//!
//! 为什么不抽成一份共享实现：这些脚本是被**逐个**调用、也被逐个拷进夹具的
//! （`cp "${checker}" "${work}/root/deploy/scripts/"`），一个 `source` 依赖会让
//! 那些夹具连同被测脚本一起搬不动；而且 `source` 那一行自己也要定位同级文件，
//! 于是「往上数几级」又出现一次，只是换了个位置。写死的层级留在这里，由这条门
//! 每读一次就核对一次，比搬一次实现少一半风险。
//!
//! 判据：**每一处上溯都必须正好落在仓库根上**。不是「落在一个含 Cargo.toml 的
//! 目录」，而是这一个检出——落点就是 `repo_root()` 算出来的那个目录。落点在
//! `deploy/`、落在根的上一级、或者文件挪了位置，都会红，并把算出来的落点写出来。
//!
//! 同一个意思有第二种落笔：先 `here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`，
//! 再从变量上溯（`"$here/../.."`）。两种都收，第二种多一道前提——那一份文件必须
//! 真的把那个变量赋成了自身目录，否则「从变量数几级」的前提就不成立。
//!
//! 刻意不收的形状：注释行（用法说明里常写着同样的命令，它不是在执行）；`.md`
//! （那不是能被跑起来的代码）；从**不认识**的变量上溯（它从哪儿来，这条门不知道，
//! 猜错的方向是悄无声息地少判一处）。
//!
//! 六十余处里有一处上溯的落点故意不是根：`deploy/scripts/tests/` 里的夹具退一级去
//! 找同级的门脚本。这类现场走 [`NOT_ROOT`] 逐条登记，落点也钉死——文件挪了位置
//! 许可就过期，门会红；反方向也查，许可没有对应的上溯时同样红。

use std::path::{Path, PathBuf};

mod common;

use common::routes::repo_root;

/// 人口的根。`deploy/` 是部署脚本，`.github/scripts/` 是 CI 自己的门——两处的
/// 「仓库根」指同一个目录，错法也一样。
const CENSUS_ROOTS: &[&str] = &["deploy", ".github"];

/// 人口下限：四处来源（deploy 的 .sh/.py、.github 的 .sh/.py）合起来是六十余处。
/// 少到这个数以下，说明扫描本身出了毛病——那与「全都正确」在输出上分不开。
const MIN_CLIMBS: usize = 50;

/// 上溯到**子目录**而不上溯到根的现场许可：`(文件, 几级, 应落到的相对路径, 为什么)`。
///
/// 填一条不是给自己开后门，是把「我不想数到根」这个意图写在一个读得到的地方——
/// 门槛就在这儿，别人加同款时会看见它、要么照抄理由、要么发现自己其实该数到根。
/// 落点也一并钉住：文件挪了位置，落点变了，这条许可就过期，门会红。
const NOT_ROOT: &[(&str, usize, &str, &str)] = &[(
    "deploy/scripts/tests/comment-capacity-figures.test.sh",
    1,
    "deploy/scripts",
    "夹具要的是同级的门脚本所在的目录（deploy/scripts），不是仓库根",
)];

/// 一处上溯：哪个文件第几行、数了几级、算出来的落点。
#[derive(Debug, Clone, PartialEq, Eq)]
struct Climb {
    file: String,
    line: usize,
    levels: usize,
    landing: PathBuf,
    /// 上溯是从一个变量数起的：那个变量在这一份文件里是不是真的指向本脚本的目录。
    /// `None` 表示写的是 `dirname "${BASH_SOURCE[0]}"` 本身，不需要再核。
    base_is_script_dir: Option<bool>,
}

/// 一处上溯在文本里的样子：数几级、从哪个变量数起（若有）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shape {
    levels: usize,
    via: Option<String>,
}

/// 被当作「本脚本所在目录」的变量名。只认这几个：别的变量可能指向别处，
/// 从它往上数几级证明不了什么，那属于这条门不猜的范围。
const SCRIPT_DIR_VARS: &[&str] = &["here", "HERE", "script_dir", "SCRIPT_DIR", "self_dir"];

fn candidate_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for top in CENSUS_ROOTS {
        let mut stack = vec![root.join(top)];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    // 构建产物与依赖目录不是这棵树自己的脚本。
                    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if name != "target" && name != "node_modules" && name != ".git" {
                        stack.push(path);
                    }
                    continue;
                }
                let keeps = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|e| e == "sh" || e == "bash" || e == "py");
                if keeps {
                    out.push(path);
                }
            }
        }
    }
    out.sort();
    out
}

/// 一串 `/..` 的级数，从 `at` 开始数。
fn segment_count(line: &str, at: usize) -> usize {
    let mut levels = 0usize;
    let mut cursor = at;
    while line[cursor..].starts_with("/..") {
        // `/..` 之后必须是行尾、引号、斜杠或空白，否则那是 `/..x` 这样的词。
        let next = cursor + 3;
        let boundary = line[next..]
            .chars()
            .next()
            .is_none_or(|c| c == '"' || c == '/' || c == ' ' || c == ')' || c == '\t');
        if !boundary {
            break;
        }
        levels += 1;
        cursor = next;
    }
    levels
}

/// 这一行里各处的上溯。
///
/// shell 有三种写法，前两种是同一件事的两种落笔：
///
///   - `"$(dirname "${BASH_SOURCE[0]}")/../.."` —— `")` 之后连着若干 `/..`；
///   - `"$here/../.."` —— 从脚本自身目录的变量数起，变量名须在
///     [`SCRIPT_DIR_VARS`] 里，且这一份文件真的把它赋成了自身目录（见 [`climbs`]）；
///   - Python 的 `parents[2]`。
///
/// 只认裸的字面级数：`parents[n]`、`"$unknown/../.."` 都不猜——猜错的方向是
/// 「悄无声息地少判一处」，那正是这条门要防的东西。
fn climbs_in(line: &str) -> Vec<Shape> {
    let mut out = Vec::new();
    let mut from = 0usize;
    while let Some(found) = line[from..].find("\")") {
        let at = from + found + 2;
        from = at;
        let levels = segment_count(line, at);
        if levels > 0 {
            out.push(Shape { levels, via: None });
        }
    }
    for var in SCRIPT_DIR_VARS {
        let token = format!("\"${var}");
        let mut from = 0usize;
        while let Some(found) = line[from..].find(&token) {
            let at = from + found + token.len();
            from = at;
            let levels = segment_count(line, at);
            if levels > 0 {
                out.push(Shape {
                    levels,
                    via: Some((*var).to_string()),
                });
            }
        }
    }
    let mut from = 0usize;
    while let Some(found) = line[from..].find("parents[") {
        let start = from + found + "parents[".len();
        let digits: String = line[start..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        from = start;
        if line[start + digits.len()..].starts_with(']') {
            if let Ok(levels) = digits.parse::<usize>() {
                out.push(Shape { levels, via: None });
            }
        }
    }
    out
}

/// 这一份文件里被赋成「本脚本所在目录」的变量名。
///
/// 赋值必须在一行里同时出现变量名、`=` 与 `dirname "${BASH_SOURCE[0]}"`。这一条
/// 是上一条判据的前提：`"$here/../.."` 只有在 `here` 真是本脚本目录时才等价于
/// 从自身位置出发；谁把 `here` 改成别的目录，这里就查不出来，那个上溯也不再被认。
fn script_dir_vars(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        for var in SCRIPT_DIR_VARS {
            let assign = format!("{var}=");
            if trimmed.starts_with(&assign) && line.contains("dirname \"${BASH_SOURCE[0]}\"") {
                out.push((*var).to_string());
            }
        }
    }
    out
}

/// 把 `dir` 往上数 `levels` 级；数不上去就是数到了文件系统根之上，返回 `None`。
fn climb(dir: &Path, levels: usize) -> Option<PathBuf> {
    let mut out = dir.to_path_buf();
    for _ in 0..levels {
        if !out.pop() {
            return None;
        }
    }
    Some(out)
}

fn climbs(root: &Path) -> Vec<Climb> {
    let mut out = Vec::new();
    for file in candidate_files(root) {
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        // 读不出来就报错：当成空文件等于让这份脚本悄悄退出人口，而「它没上溯」
        // 与「没读到它」在结果上同形。
        let text = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("{} 读不出来：{e}", file.display()));
        let Some(dir) = file.parent() else {
            continue;
        };
        let script_dirs = script_dir_vars(&text);
        for (index, line) in text.lines().enumerate() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            for shape in climbs_in(line) {
                let base_is_script_dir = shape
                    .via
                    .as_ref()
                    .map(|var| script_dirs.iter().any(|bound| bound == var));
                out.push(Climb {
                    file: rel.clone(),
                    line: index + 1,
                    levels: shape.levels,
                    landing: climb(dir, shape.levels).unwrap_or_default(),
                    base_is_script_dir,
                });
            }
        }
    }
    out
}

/// 一处上溯是否被 [`NOT_ROOT`] 里的一条许可放过，且落点与许可写的一致。
fn licensed(root: &Path, climb: &Climb) -> bool {
    NOT_ROOT.iter().any(|(file, levels, landing, _)| {
        climb.file == *file && climb.levels == *levels && climb.landing == root.join(landing)
    })
}

/// 真树：每一处上溯都落在仓库根上（或在许可里钉死的那一个子目录上），且这样的行
/// 多到能说明扫描真的在看东西。
#[test]
fn every_climb_lands_on_the_repo_root() {
    let root = repo_root();
    let found = climbs(&root);
    assert!(
        found.len() >= MIN_CLIMBS,
        "只数出 {} 处上溯（下限 {MIN_CLIMBS}）：扫描塌了，这条门此刻什么都没证明",
        found.len()
    );
    let off: Vec<String> = found
        .iter()
        .filter_map(|c| {
            if c.base_is_script_dir == Some(false) {
                return Some(format!(
                    "{}:{}: 从变量上溯，但这一份文件没有把那个变量赋成本脚本目录，落点无从推起",
                    c.file, c.line
                ));
            }
            if c.landing == root || licensed(&root, c) {
                return None;
            }
            Some(format!(
                "{}:{}: 往上 {} 级落到 {}，不是仓库根 {}",
                c.file,
                c.line,
                c.levels,
                c.landing.display(),
                root.display()
            ))
        })
        .collect();
    assert!(
        off.is_empty(),
        "这些地方的「往上数几级」已经不算出仓库根了——文件挪过位置，或者级数抄自别处：\n{}",
        off.join("\n")
    );
}

/// 许可的另一个方向：每条许可都得真的压着一处上溯。现场没了就收回，否则它会在
/// 下一个同名现场出现时替对方挡下这条门。
#[test]
fn a_licence_whose_site_is_gone_is_reported() {
    let root = repo_root();
    let found = climbs(&root);
    let stale: Vec<&str> = NOT_ROOT
        .iter()
        .filter(|(file, levels, landing, _)| {
            !found
                .iter()
                .any(|c| c.file == *file && c.levels == *levels && c.landing == root.join(landing))
        })
        .map(|(file, _, _, _)| *file)
        .collect();
    assert!(
        stale.is_empty(),
        "这些许可已经不对应任何一处上溯了（文件挪了、级数改了、或者那行被删了）：{stale:?}"
    );
}

fn levels(shapes: &[Shape]) -> Vec<usize> {
    shapes.iter().map(|s| s.levels).collect()
}

/// 判据按形状读，不按裸子串读。
#[test]
fn the_predicate_reads_climbs_and_not_their_neighbours() {
    assert_eq!(
        levels(&climbs_in(
            r#"ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)""#
        )),
        vec![2]
    );
    assert_eq!(
        levels(&climbs_in(
            r#"ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)""#
        )),
        vec![3]
    );
    assert_eq!(
        levels(&climbs_in("root = Path(__file__).resolve().parents[2]")),
        vec![2]
    );
    // 经变量的那一种：`"$here/../.."`。
    assert_eq!(
        levels(&climbs_in(r#"root=$(cd "$here/../.." && pwd)"#)),
        vec![2]
    );
    // 目录名里带 `..` 不是上溯：`a/..b` 的下一段不是 `..`，`parents[n]` 里写变量
    // 也不猜，不认识的变量同样不猜。
    assert_eq!(
        levels(&climbs_in(r#"cp x "$(dirname "$0")/..b/""#)),
        Vec::<usize>::new()
    );
    assert_eq!(
        levels(&climbs_in("root = Path(__file__).parents[depth]")),
        Vec::<usize>::new()
    );
    assert_eq!(levels(&climbs_in(r#"ROOT="$(pwd)""#)), Vec::<usize>::new());
    assert_eq!(
        levels(&climbs_in(r#"cd "$scratch/../.." && pwd"#)),
        Vec::<usize>::new(),
        "只有认识的脚本目录变量才数：别的变量从哪儿来，这条门不知道"
    );
}

/// 假树上的对照：挪一层、数多半级、数少半级都要红；位置对的不许红。
#[test]
fn a_file_that_moved_is_the_shape_this_gate_exists_for() {
    let scratch = std::env::temp_dir().join(format!(
        "cogneva-root-climb-{}-{}",
        std::process::id(),
        line!()
    ));
    let _ = std::fs::remove_dir_all(&scratch);

    let write = |rel: &str, body: &str| {
        let path = scratch.join(rel);
        std::fs::create_dir_all(path.parent().expect("有父目录")).expect("建目录");
        std::fs::write(&path, body).expect("写文件");
    };
    let line = r#"ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
"#;
    // 深度 2 的两级上溯：落点是假树根，对。
    write("deploy/scripts/ok.sh", line);
    // 深度 3 的两级上溯：落点是假树根下的 deploy/，错。
    write("deploy/scripts/sub/moved.sh", line);
    // 深度 2 的三级上溯：落到假树根之上，错。
    write(
        "deploy/scripts/deep.sh",
        &line.replace("/../..", "/../../.."),
    );
    // Python 的同一族。
    write(
        "deploy/scripts/sub/moved.py",
        "root = Path(__file__).resolve().parents[2]\n",
    );
    // 经变量的那一种：`here` 真赋成了自身目录、深度 2、数两级——对。
    write(
        "deploy/scripts/via_ok.sh",
        "here=\"$(cd \"$(dirname \"${BASH_SOURCE[0]}\")\" && pwd)\"\nroot=$(cd \"$here/../..\" && pwd)\n",
    );
    // 同一个写法，但这一份文件从没把 `here` 赋成自身目录：落点无从推起，错。
    write(
        "deploy/scripts/via_unbound.sh",
        "root=$(cd \"$here/../..\" && pwd)\n",
    );

    let found = climbs(&scratch);
    assert_eq!(found.len(), 6, "六处上溯都该被数出来：{found:?}");
    let off: Vec<&str> = found
        .iter()
        .filter(|c| c.landing != scratch || c.base_is_script_dir == Some(false))
        .map(|c| c.file.as_str())
        .collect();
    assert_eq!(
        off,
        vec![
            "deploy/scripts/deep.sh",
            "deploy/scripts/sub/moved.py",
            "deploy/scripts/sub/moved.sh",
            "deploy/scripts/via_unbound.sh"
        ],
        "只有位置对的那两份不该红：{found:?}"
    );

    let _ = std::fs::remove_dir_all(&scratch);
}
