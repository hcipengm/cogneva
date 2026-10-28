//! 「这个部署里的具名容器，现在跑的是哪个镜像」——只有这一句问法。
//!
//! 这句话在树里写过三遍：拉取端判金丝雀要不要推进、部署器判四个目标当前在哪一版、
//! 滚动端回滚前快照当前镜像。三份是同一句 `jsonpath` 模板，只是容器名不同，于是
//! 改其中一份（字段路径挪了、改成 `containers[0]`）另外两份不会红。
//!
//! 而**问不到东西时 `kubectl` 不报错**：`jsonpath` 匹配不到就输出空串、退出码 0。
//! 三份对空串的处理又各不相同——滚动端判成错误，部署器把它推进 `deployed_images`
//! 再进 `classify_deployed`：空串不是 `main-<rev>`，于是记成一次 `non_main`；四个
//! 目标全空就得到 `Legacy`（「还没迁移到主线镜像」），一部分空就得到 `Mixed`
//! （「上一轮滚动还在飞」）。两种都是**关于集群状态的话**，而真实情况只是「这一问
//! 没问到」。`Legacy` 这一支的结论是继续推进：一个容器名对不上（chart 改了名字、
//! 镜像里的 jsonpath 随版本变了）就会让部署器在一个读不清的集群上把主线往前滚。
//!
//! 所以：问法一处，读法一处，空读数不许往下走。

use cog_core::{SFError, SFResult};

/// 问法本体：`{}` 位置填容器名。
///
/// 外层是**一层**花括号。这句模板在交给 `kubectl` 之前不再过任何 `format!`，而
/// `{{` 只有在格式串里才会折叠成 `{`：写成 `{{...}}` 就会原样进 argv，`kubectl`
/// 在第一个 `{` 上判 `unrecognized character in action: U+007B '{'`，这一轮读版本
/// 直接失败——快照读不到，一个目标也判不了。
pub const CONTAINER_IMAGE_JSONPATH: &str =
    "jsonpath={.spec.template.spec.containers[?(@.name==\"{}\")].image}";

/// 展开成 `kubectl -o` 的实参。
pub fn container_image_query(container: &str) -> String {
    CONTAINER_IMAGE_JSONPATH.replace("{}", container)
}

/// 把一次查询的输出读成一个镜像。
///
/// 空输出是一句「没问到」，不是「镜像为空」，也不是「还没迁移」。它必须当场成为
/// 错误并点名部署与容器：读端唯一能做的补救是去看名字对不上，而一个空串在下游
/// 会被解释成任何一种状态。
pub fn container_image(output: &str, deployment: &str, container: &str) -> SFResult<String> {
    let image = output.trim();
    if image.is_empty() {
        return Err(SFError::Agent(format!(
            "deployment/{deployment} has no image for container {container}: the query matched nothing \
             (container renamed, or the jsonpath no longer resolves); an empty reading is not an old image"
        )));
    }
    Ok(image.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rust_production::production_text;
    use std::path::Path;

    /// 普查的覆盖率下限：少于这么多源文件，这把尺子就没量到东西。
    ///
    /// 判据本身是「某句话只出现在一处」，而**一处都没有**的时候它同样成立——
    /// 走错目录、改过 crate 名、`read_dir` 出错，读出来的是空集，断言全绿。
    /// 所以下限长在 `collect` 里而不是长在调用点上：调用点上的下限只管得住那
    /// 一次调用，而这条下限要管的是「以后谁再加一份普查」。下限取实测文件数的
    /// 七成，删掉几个文件不会误报，整个目录读空一定报。
    const MIN_FILES: usize = 50;

    /// 空读数不许变成一个镜像。
    #[test]
    fn an_empty_reading_is_not_an_image() {
        for empty in ["", "   ", "\n"] {
            let err = container_image(empty, "cogneva", "cogneva")
                .expect_err("空读数被当成了一个镜像")
                .to_string();
            assert!(err.contains("cogneva"), "报错要点名是哪个部署/容器：{err}");
            assert!(err.contains("no image"), "报错要说清读不到的是镜像：{err}");
        }
    }

    /// 正常读数原样（去空白后）返回。
    #[test]
    fn a_reading_is_trimmed_not_rewritten() {
        assert_eq!(
            container_image("r:5000/cogneva:main-aa1111111111\n", "cogneva", "cogneva").unwrap(),
            "r:5000/cogneva:main-aa1111111111"
        );
    }

    /// 问法本身点名的是「按容器名挑」，不是「第一个容器」。
    #[test]
    fn the_query_asks_for_the_named_container() {
        let query = container_image_query("cogneva-a2a");
        assert!(query.starts_with("jsonpath="), "{query}");
        assert!(
            query.contains("containers[?(@.name==\"cogneva-a2a\")].image"),
            "{query}"
        );
    }

    /// 交给 `kubectl -o` 的那一个实参，就是它要解析的模板本身。
    ///
    /// 两层花括号在这里不是笔误的余地：这句模板当初是从 `format!` 的参数里搬出来
    /// 的，而 `{{` 只在格式串里才折叠成 `{`。搬成普通常量后若照抄 `{{...}}`，
    /// 实参就是 `jsonpath={{...}}`，`kubectl` 报 `unrecognized character in action:
    /// U+007B '{'`，整轮读版本失败。这条量的是交出去的字符串，不是源码长什么样。
    #[test]
    fn the_argument_handed_to_kubectl_is_one_layer_of_braces() {
        let query = container_image_query("cogneva-a2a");
        assert_eq!(
            query,
            "jsonpath={.spec.template.spec.containers[?(@.name==\"cogneva-a2a\")].image}",
        );
        assert_eq!(
            (query.matches('{').count(), query.matches('}').count()),
            (1, 1),
            "外层花括号只能有一层：{query}"
        );
    }

    /// 生产代码里这句话只许出现一次：任何一处再抄一遍，就是第二份会各自漂的约定。
    ///
    /// 判不了什么：用例那一半不读（测试自己会写这句话来造标本），所以判据的上限是
    /// 「生产代码里一处」。切在哪儿由共享切口算，不按「第一处 `#[cfg(test)]`」切——
    /// 那条更弱的规则会在文件中间有 test-only 项时把后面全部产出算成用例。
    #[test]
    fn the_question_is_written_once() {
        let needle = "containers[?(@.name==";
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        let mut sites = Vec::new();
        collect(&src, &mut files, &mut sites, needle);
        let expected = "cog-reflection/src/deployment_image.rs";
        assert_eq!(
            sites,
            vec![expected.to_string()],
            "「部署的具名容器跑哪个镜像」这句话出现了不止一处（或一处都不在原处）：\
             {sites:?}。第二份不会跟着第一份变，而漂移的表现是问不到东西时读到空串，\
             下游把它读成「旧镜像」或「滚动在飞」。改用 container_image_query。"
        );
    }

    /// 对照：第二份必须被报出来。少了这条，`collect` 哪天退化成什么都读不出来，
    /// 上面那条仍然全绿。
    #[test]
    fn a_second_copy_is_reported() {
        let needle = "containers[?(@.name==";
        let mut files = Vec::new();
        let mut sites: Vec<String> = Vec::new();
        for (name, text) in [
            (
                "cog-reflection/src/deployment_image.rs",
                "pub const Q: &str = \"jsonpath={{.spec.template.spec.containers[?(@.name==\\\"{}\\\")].image}}\";\n",
            ),
            (
                "cog-reflection/src/other.rs",
                "fn q(c: &str) -> String { format!(\"jsonpath={{.spec.template.spec.containers[?(@.name==\\\"{}\\\")].image}}\", c) }\n",
            ),
        ] {
            note(name, text, needle, &mut files, &mut sites);
        }
        assert_eq!(sites.len(), 2, "{sites:?}");
    }

    fn collect(dir: &Path, files: &mut Vec<String>, sites: &mut Vec<String>, needle: &str) {
        collect_from(dir, dir, files, sites, needle);
        assert!(
            files.len() >= MIN_FILES,
            "只读到 {} 个源文件（下限 {MIN_FILES}）：这把尺子没量到东西，\
             「只写一处」对空集同样成立，所以这次读不算读数",
            files.len()
        );
    }

    /// 对照：读空必须报出来。少了这条，`collect` 哪天退化成什么都不读，
    /// 上面那条仍然全绿——而它绿的原因恰恰是它没读到任何东西。
    #[test]
    #[should_panic(expected = "这把尺子没量到东西")]
    fn a_census_that_reads_almost_nothing_is_refused() {
        let empty = Path::new("/nonexistent-directory-for-the-census");
        let mut files = Vec::new();
        let mut sites = Vec::new();
        collect(empty, &mut files, &mut sites, "containers[?(@.name==");
    }

    fn collect_from(
        root: &Path,
        dir: &Path,
        files: &mut Vec<String>,
        sites: &mut Vec<String>,
        needle: &str,
    ) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        let mut paths: Vec<_> = entries.flatten().map(|e| e.path()).collect();
        paths.sort();
        for path in paths {
            if path.is_dir() {
                collect_from(root, &path, files, sites, needle);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            let rel = path.strip_prefix(root).unwrap_or(&path);
            let name = format!("cog-reflection/src/{}", rel.display());
            note(&name, &text, needle, files, sites);
        }
    }

    fn note(
        name: &str,
        text: &str,
        needle: &str,
        files: &mut Vec<String>,
        sites: &mut Vec<String>,
    ) {
        files.push(name.to_string());
        let production = production_text(text);
        if production.contains(needle) {
            sites.push(name.to_string());
        }
    }
}
