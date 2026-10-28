//! 脚本里「往上数几级就是仓库根」那行，数出来的必须是仓库根。
//!
//! `deploy/` 与 `.github/scripts/` 下一共五十余处同一个写法：
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
//! 刻意不收的形状：注释行（用法说明里常写着同样的命令，它不是在执行）；`.md`
//! （那不是能被跑起来的代码）。收进来的形状里没有一处在今天例外——五十余处
//! 全部落在根上，所以这条门不需要豁免名单，也就不会因为豁免名单过期而变瞎。

use std::path::{Path, PathBuf};

mod common;

use common::routes::repo_root;

/// 人口的根。`deploy/` 是部署脚本，`.github/scripts/` 是 CI 自己的门——两处的
/// 「仓库根」指同一个目录，错法也一样。
const CENSUS_ROOTS: &[&str] = &["deploy", ".github"];

/// 人口下限：四处来源（deploy 的 .sh/.py、.github 的 .sh/.py）合起来是五十余处。
/// 少到这个数以下，说明扫描本身出了毛病——那与「全都正确」在输出上分不开。
const MIN_CLIMBS: usize = 40;

/// 一处上溯：哪个文件第几行、数了几级、算出来的落点。
#[derive(Debug, Clone, PartialEq, Eq)]
struct Climb {
    file: String,
    line: usize,
    levels: usize,
    landing: PathBuf,
}

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

/// 这一行里各处的上溯级数。
///
/// shell 的形状是 `"$(dirname "${BASH_SOURCE[0]}")/../.."`——`")` 之后连着若干
/// `/..`；Python 的形状是 `parents[2]`。两种都只认**裸的**字面级数：写成
/// `parents[n]` 或 `$(dirname "$d")/..` 里用变量的，这条门不猜。
fn climb_levels(line: &str) -> Vec<usize> {
    let mut out = Vec::new();
    let bytes = line.as_bytes();
    let mut from = 0usize;
    while let Some(found) = line[from..].find("\")") {
        let at = from + found + 2;
        from = at;
        let mut levels = 0usize;
        let mut cursor = at;
        while line[cursor..].starts_with("/..") {
            // `/..` 之后必须是行尾、引号、斜杠或空白，否则是 `/../x` 之外的词。
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
        if levels > 0 {
            out.push(levels);
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
                out.push(levels);
            }
        }
    }
    let _ = bytes;
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
        let text = std::fs::read_to_string(&file).unwrap_or_default();
        let Some(dir) = file.parent() else {
            continue;
        };
        for (index, line) in text.lines().enumerate() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            for levels in climb_levels(line) {
                out.push(Climb {
                    file: rel.clone(),
                    line: index + 1,
                    levels,
                    landing: climb(dir, levels).unwrap_or_default(),
                });
            }
        }
    }
    out
}

/// 真树：每一处上溯都落在仓库根上，且这样的行多到能说明扫描真的在看东西。
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
        .filter(|c| c.landing != root)
        .map(|c| {
            format!(
                "{}:{}: 往上 {} 级落到 {}，不是仓库根 {}",
                c.file,
                c.line,
                c.levels,
                c.landing.display(),
                root.display()
            )
        })
        .collect();
    assert!(
        off.is_empty(),
        "这些地方的「往上数几级」已经不算出仓库根了——文件挪过位置，或者级数抄自别处：\n{}",
        off.join("\n")
    );
}

/// 判据按形状读，不按裸子串读。
#[test]
fn the_predicate_reads_climbs_and_not_their_neighbours() {
    assert_eq!(
        climb_levels(r#"ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)""#),
        vec![2]
    );
    assert_eq!(
        climb_levels(r#"ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)""#),
        vec![3]
    );
    assert_eq!(
        climb_levels("root = Path(__file__).resolve().parents[2]"),
        vec![2]
    );
    // 目录名里带 `..` 不是上溯：`a/..b` 的下一段不是 `..`，`parents[n]` 里写变量
    // 也不猜。
    assert_eq!(
        climb_levels(r#"cp x "$(dirname "$0")/..b/""#),
        Vec::<usize>::new()
    );
    assert_eq!(
        climb_levels("root = Path(__file__).parents[depth]"),
        Vec::<usize>::new()
    );
    assert_eq!(climb_levels(r#"ROOT="$(pwd)""#), Vec::<usize>::new());
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

    let found = climbs(&scratch);
    assert_eq!(found.len(), 4, "四处上溯都该被数出来：{found:?}");
    let off: Vec<&str> = found
        .iter()
        .filter(|c| c.landing != scratch)
        .map(|c| c.file.as_str())
        .collect();
    assert_eq!(
        off,
        vec![
            "deploy/scripts/deep.sh",
            "deploy/scripts/sub/moved.py",
            "deploy/scripts/sub/moved.sh"
        ],
        "只有位置对的那一份不该红：{found:?}"
    );

    let _ = std::fs::remove_dir_all(&scratch);
}
