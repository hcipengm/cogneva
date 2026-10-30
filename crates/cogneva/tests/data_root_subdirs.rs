//! 数据根下每一个一级子目录都得有一句「换机时它怎么办」。
//!
//! 数据根是「换机有没有丢东西」的实际边界，而它没有任何一处被完整列过：新增一个
//! 子目录时，备份 Job 照旧绿、恢复演练照旧绿，包里就是少了一棵树。`warehouse/`
//! 就是这么进来的——代码里写 `{data_dir}/warehouse`，在它出现之前没有一份清单
//! 提过它。
//!
//! 判词的载体是 `cogneva::backup::DATA_ROOT_SUBDIRS`（每个子目录一行，紧挨着备份
//! 逻辑自己），普查面在这里现算：代码、应用配置、渲染清单三条来源的并集。两件事
//! 分开——**子目录由机器数出来，待遇由写那段路径的人当场写下来**。普查面漏了或
//! 判词漏了都红。
//!
//! 这里不读实盘：CI 没有那块卷，判词也不该由某台机器当下的目录树决定。同理，
//! `deploy/scripts/check-backup-coverage.sh` 只剩卷覆盖那一半——子目录这一半整体
//! 搬到了这里，脚本里不再有第二份清单。

use crate::common;

use cogneva::backup::{SubdirClass, DATA_ROOT_SUBDIRS};
use common::routes::{repo_root, walk};
use common::rust_production::production_text_keeping_comments;
use std::collections::BTreeMap;
use std::path::Path;

/// 应用配置里的数据根。读不到才用默认值——普查面跟着配置走，不跟着脚本里的常量走。
fn data_dir(root: &Path) -> String {
    let path = root.join("deploy/helm/cogneva/files/cogneva.json");
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(value) = doc
                .get("app")
                .and_then(|app| app.get("data_dir"))
                .and_then(|v| v.as_str())
            {
                if !value.is_empty() {
                    return value.trim_end_matches('/').to_string();
                }
            }
        }
    }
    "/var/lib/cogneva-data".to_string()
}

fn find_seq(haystack: &[char], needle: &[char]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    (0..=haystack.len() - needle.len()).find(|&i| haystack[i..i + needle.len()] == *needle)
}

fn is_segment(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// 一行里所有 `data_dir… .join(` 之后的实参名。
///
/// 接收者不一定是字面上的 `data_dir`（`state.data_dir`、`cfg.data_dir`、
/// `data_dir()` 都算），所以判据是「这一行出现 data_dir」由调用方先过一遍，这里
/// 只负责在 `data_dir` 之后找出紧跟的 `.join(`。
fn root_joins(line: &str, consts: &BTreeMap<String, String>) -> Vec<String> {
    // 整个按字符下标走：中文注释与字符串常量都是多字节，按字节下标切 `Vec<char>`
    // 会切在字符中间，认出来的名字是错位的。
    let chars: Vec<char> = line.chars().collect();
    let needle: Vec<char> = "ata_dir".chars().collect();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = find_seq(&chars[from..], &needle) {
        let at = from + rel;
        let end = at + needle.len();
        from = end;
        // 前缀必须是 `d` 或 `D`：`[Dd]ata_dir`。
        if at == 0 || !matches!(chars[at - 1], 'd' | 'D') {
            continue;
        }
        let rest = &chars[end..];
        let mut k = 0;
        if rest.len() >= 2 && rest[0] == '(' && rest[1] == ')' {
            k = 2;
        }
        // `[\s).]*\.join\(`：先吞空白 / `)` / `.`，但吞掉的每一个位置都要回头看
        // 是不是已经到 `.join(` 了（正则的回溯），否则 `data_dir.join(` 里的那个点
        // 会被 `[\s).]*` 吃掉、后面再也对不上。
        loop {
            if rest[k..].starts_with(&['.', 'j', 'o', 'i', 'n', '(']) {
                let arg_at = k + 6;
                if let Some(name) = parse_join_arg(&rest[arg_at..], consts) {
                    out.push(name);
                }
                break;
            }
            if k < rest.len() && (rest[k].is_whitespace() || rest[k] == ')' || rest[k] == '.') {
                k += 1;
                continue;
            }
            break;
        }
    }
    out
}

/// `.join(` 之后那一个实参：字面量，或同一文件里的 `const X: &str = "…";`。
///
/// 解析不出来的标识符（函数参数、跨 crate 常量）返回 `None`——猜一个名字写进
/// 普查面比漏掉更糟。
fn parse_join_arg(rest: &[char], consts: &BTreeMap<String, String>) -> Option<String> {
    let mut k = 0;
    while k < rest.len() && rest[k].is_whitespace() {
        k += 1;
    }
    if k < rest.len() && rest[k] == '"' {
        let mut j = k + 1;
        while j < rest.len() && rest[j] != '"' {
            if !is_segment(rest[j]) {
                return None;
            }
            j += 1;
        }
        if j >= rest.len() {
            return None;
        }
        let name: String = rest[k + 1..j].iter().collect();
        return ends_with_close_paren(&rest[j + 1..]).then_some(name);
    }
    if k < rest.len() && is_ident_start(rest[k]) {
        let mut j = k;
        while j < rest.len() && is_ident(rest[j]) {
            j += 1;
        }
        let ident: String = rest[k..j].iter().collect();
        let name = consts.get(&ident)?;
        return ends_with_close_paren(&rest[j..]).then(|| name.clone());
    }
    None
}

fn ends_with_close_paren(rest: &[char]) -> bool {
    let mut k = 0;
    while k < rest.len() && rest[k].is_whitespace() {
        k += 1;
    }
    k < rest.len() && rest[k] == ')'
}

/// 同一行里 `format!("{...}/x")` 拼出来的一级段。
fn format_segments(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    while i + 1 < chars.len() {
        if chars[i] == '"' && chars[i + 1] == '{' {
            let mut j = i + 2;
            while j < chars.len() && chars[j] != '}' && chars[j] != '"' {
                j += 1;
            }
            if j + 1 < chars.len() && chars[j] == '}' && chars[j + 1] == '/' {
                let mut k = j + 2;
                let mut name = String::new();
                while k < chars.len() && is_segment(chars[k]) {
                    name.push(chars[k]);
                    k += 1;
                }
                if !name.is_empty() {
                    out.push(name);
                }
                i = k;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// 只在 `COGNEVA_DATA_DIR` 那一族常量名上出现：上一行提到 `DATA_DIR`、这一行
/// 才 `.join("x")`——这一行本身看不出接收者是谁。
fn local_joins(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = line[from..].find(".join(\"") {
        let at = from + rel + ".join(\"".len();
        from = at;
        let rest: Vec<char> = line[at..].chars().collect();
        let mut k = 0;
        while k < rest.len() && is_segment(rest[k]) {
            k += 1;
        }
        if k > 0 && k + 1 < rest.len() && rest[k] == '"' && rest[k + 1] == ')' {
            out.push(rest[..k].iter().collect());
        }
    }
    out
}

fn consts_in(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let Some(at) = line.find("const ") else {
            continue;
        };
        let rest: Vec<char> = line[at + "const ".len()..].chars().collect();
        let mut k = 0;
        while k < rest.len() && is_ident(rest[k]) {
            k += 1;
        }
        if k == 0 {
            continue;
        }
        let name: String = rest[..k].iter().collect();
        // `: &str = "…";` 或 `: &'static str = "…";`
        let after: String = rest[k..].iter().collect();
        let after = after.trim_start();
        let Some(after) = after.strip_prefix(':') else {
            continue;
        };
        let after = after.trim_start();
        let after = after
            .strip_prefix("&'static str")
            .or_else(|| after.strip_prefix("&str"));
        let Some(after) = after else {
            continue;
        };
        let after = after.trim_start();
        let Some(after) = after.strip_prefix('=') else {
            continue;
        };
        let after = after.trim_start();
        let Some(after) = after.strip_prefix('"') else {
            continue;
        };
        let Some(close) = after.find('"') else {
            continue;
        };
        let value = &after[..close];
        if !value.is_empty() && value.chars().all(is_segment) {
            out.insert(name, value.to_string());
        }
    }
    out
}

/// 代码里由 `data_dir` / `COGNEVA_DATA_DIR` 拼出来的一级路径段。
fn rust_segments(root: &Path) -> BTreeMap<String, String> {
    let mut files = Vec::new();
    walk(&root.join("crates"), &["rs"], &mut files);
    let mut found = BTreeMap::new();
    for path in files {
        // 测试目录里的 `data_dir.join("…")` 是夹具，不是对数据根的判断：本文件
        // 自己就写着十来种形状当反例，把它们数进普查面等于自己给自己造子目录。
        if path.components().any(|c| c.as_os_str() == "tests") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let consts = consts_in(&text);
        // 测试块里的 data_dir 是临时目录：`..data`、`jwt-secret` 这类名字会把普查面
        // 变成一幅假的现场。切法用共享的那一份——切在第一个 `#[cfg(test)]` 上会把
        // 它之后的产出整段丢掉，而「那里没有」与「那里没读」在判词上是同一句话。
        let text = production_text_keeping_comments(&text);
        // 往回看四行而不是一行：`std::env::var("COGNEVA_DATA_DIR")` 后面常跟
        // `.ok().filter(…).unwrap_or_else(…)` 的链，join 落在第三、四行上。
        let mut recent: Vec<&str> = vec![""; 4];
        for line in text.lines() {
            let mentions = line.contains("data_dir") || line.contains("DATA_DIR");
            let window = mentions
                || recent
                    .iter()
                    .any(|l| l.contains("data_dir") || l.contains("DATA_DIR"));
            if window {
                for name in root_joins(line, &consts) {
                    found
                        .entry(name)
                        .or_insert_with(|| path.display().to_string());
                }
                for name in format_segments(line) {
                    found
                        .entry(name)
                        .or_insert_with(|| path.display().to_string());
                }
            }
            if recent.iter().any(|l| l.contains("DATA_DIR")) {
                for name in local_joins(line) {
                    found
                        .entry(name)
                        .or_insert_with(|| path.display().to_string());
                }
            }
            recent.remove(0);
            recent.push(line);
        }
    }
    found
}

/// 文本里所有 `<数据根>/<一级段>` 的一级段（配置值、渲染清单的 hostPath 都走这条）。
fn paths_under(text: &str, mount: &str) -> Vec<String> {
    let mut out = Vec::new();
    let needle = format!("{mount}/");
    for (idx, _) in text.match_indices(&needle) {
        let before = text[..idx].chars().next_back();
        if !matches!(before, Some(c) if c.is_whitespace() || ":\"'=".contains(c)) {
            continue;
        }
        let rest = &text[idx + needle.len()..];
        let name: String = rest.chars().take_while(|c| is_segment(*c)).collect();
        if !name.is_empty() {
            out.push(name);
        }
    }
    out
}

/// 普查面：代码、应用配置、渲染清单三条来源的并集。
///
/// 并集而不是任何一条单独成面：`git-remote` 只在渲染清单里出现，`warehouse` 只在
/// 代码里出现，而「代码里新加的子目录」和「部署里新挂的子目录」是两种各自独立
/// 会漏的分类对象。
fn census(root: &Path, mount: &str) -> BTreeMap<String, String> {
    let mut out = rust_segments(root);
    let config = root.join("deploy/helm/cogneva/files/cogneva.json");
    if let Ok(text) = std::fs::read_to_string(&config) {
        for name in paths_under(&text, mount) {
            out.entry(name)
                .or_insert_with(|| config.display().to_string());
        }
    }
    let mut rendered = Vec::new();
    walk(
        &root.join("deploy/rendered"),
        &["yaml", "yml"],
        &mut rendered,
    );
    for path in rendered {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for name in paths_under(&text, mount) {
            out.entry(name)
                .or_insert_with(|| path.display().to_string());
        }
    }
    out
}

#[test]
fn every_data_root_subdir_has_a_verdict() {
    let root = repo_root();
    let mount = data_dir(&root);
    let found = census(&root, &mount);

    // 普查面读空了：三条来源一条都没交回东西。这时未分类自然是空集，判词与「每个
    // 子目录都已分类」一字不差——它绿了，却什么都没看。配置路径改了、渲染物换了
    // 名字、代码里 `data_dir.join` 换了写法，都会落在这里。
    assert!(
        !found.is_empty(),
        "数据根子目录的普查面一条都没读到（代码 data_dir 的 join、cogneva.json 里的 \
         {mount}/*、渲染清单的挂载）。空普查与「都分类过」同形，按没看成判"
    );

    let mut unclassified = Vec::new();
    for (name, where_) in &found {
        let known = DATA_ROOT_SUBDIRS.iter().any(|row| row.name == name);
        if !known {
            unclassified.push(format!("{name}/（出现在 {where_}）"));
        }
    }
    assert!(
        unclassified.is_empty(),
        "这些一级子目录会被写进数据根，但 DATA_ROOT_SUBDIRS 里没有它们的判词——\
         换机后少了这一棵没人说得清能不能接受，往里加一行说明它带不带、为什么：{}",
        unclassified.join("，")
    );
}

#[test]
fn every_verdict_says_where_and_why() {
    for row in DATA_ROOT_SUBDIRS {
        assert!(!row.name.is_empty());
        assert!(
            !row.what.is_empty(),
            "{} 没写它是什么，读者没法判断这句话说的是不是它",
            row.name
        );
        assert!(
            !row.why.is_empty(),
            "{} 没写为什么——只写名字的条目等于把问题留给下一个人",
            row.name
        );
        assert!(!row.roots.is_empty(), "{} 没说会出现在哪个数据根", row.name);
        for r in row.roots {
            assert!(
                matches!(*r, "main" | "evolution"),
                "{} 的 roots 里有 {r}，数据根只有 main 与 evolution 两个",
                row.name
            );
        }
        // 对象层只能有一句判词，且必须是「带」。它是备份里唯一按约定去找 <data_dir>/
        // storage 的地方：把它标成可丢，等于换机后对象层静默消失而包里还写着绿。
        if row.name == cogneva::backup::OBJECTS_SUBDIR {
            assert_eq!(
                row.class,
                SubdirClass::Authoritative,
                "对象层不许被标成可丢"
            );
        }
    }
    let mut names: Vec<&str> = DATA_ROOT_SUBDIRS.iter().map(|r| r.name).collect();
    let before = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(
        before,
        names.len(),
        "同一棵子树有两行判词，读者不知道信哪一行"
    );
}

#[test]
fn the_census_reads_every_shape_the_code_uses() {
    // 普查面自己坏了（正则换成手写之后最容易坏的就是它）时，上面那条会读成「都
    // 分类过」。这几种写法是代码里真实出现过的形状，逐个钉住。
    let consts = consts_in("const WORKSPACE_SUBDIR: &str = \"workspace\";\n");
    assert_eq!(
        root_joins("let dir = data_dir.join(\"policies\");", &consts),
        vec!["policies".to_string()]
    );
    assert_eq!(
        root_joins("let dir = state.data_dir.join(WORKSPACE_SUBDIR);", &consts),
        vec!["workspace".to_string()],
        "常量名不解析就等于整段看不见"
    );
    assert_eq!(
        root_joins("let dir = data_dir().join(\"landing\");", &consts),
        vec!["landing".to_string()]
    );
    assert_eq!(
        format_segments("let p = format!(\"{}/warehouse\", cfg.data_dir);"),
        vec!["warehouse".to_string()]
    );
    assert_eq!(
        local_joins("let work = dir.join(\"git-workdir\");"),
        vec!["git-workdir".to_string()]
    );
    // 反例：`staging` 拼的是包里的目录，不是数据根下的。接收者必须本身就是数据根，
    // 不能「这一行提到了 data_dir」就算。
    assert!(
        root_joins("let d = staging.join(\"objects\");", &consts).is_empty(),
        "包里那一层被当成数据根下的一级段，普查面会多出从不存在的子目录"
    );
    // 反例：只认 data_dir 之后的第一个 join，`datasets` 是 eval 下的二级目录。
    assert_eq!(
        root_joins(
            "let p = Path::new(data_dir).join(EVAL_SUBDIR).join(\"datasets\");",
            &consts_in("const EVAL_SUBDIR: &str = \"eval\";\n")
        ),
        vec!["eval".to_string()],
        "二级目录被当成一级段，真正的 eval 会被顶掉"
    );
    assert_eq!(
        paths_under(
            "mountPath: /var/lib/cogneva-data/raw-logs",
            "/var/lib/cogneva-data"
        ),
        vec!["raw-logs".to_string()]
    );
}
