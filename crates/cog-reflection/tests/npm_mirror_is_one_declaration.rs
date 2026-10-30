//! Gate: the npm registry is one declaration, and both build paths read it.
//!
//! 同一版源码有两条前端构建的路：镜像构建时的 `webbuilder` 阶段，和 overlay 每次
//! 换二进制时重跑的那一趟。两边拉包的地址必须一致——不一致时没有任何读数会报错，
//! 两份前端产物各自看起来都是完整的，而包可能不是同一批。这条门就是那句「一致」
//! 的可执行形式。
//!
//! 两处在此之前各写一份字面量：`Dockerfile` 收一个 `NPM_REGISTRY` 构建参数
//! （由 `deploy/scripts/build-release-image.sh` 在 CN 模式里传），部署器里是一个
//! `const`。默认值恰好相等，但相等是巧合而不是判据——改一处、留一处，没有任何东西
//! 会红。现在还多一层：部署器读 `COGNEVA_NPM_REGISTRY`，让测过的主机把地址递进来，
//! 常量退化成默认值。默认值仍然要和镜像构建那一份对得上，否则「测不出来时用默认」
//! 这句话在两条路上是两个不同的默认。
//!
//! 这里不判镜像构建脚本「该不该在 CN 模式里才传」——那是引导器的选择；判的是
//! 凡它传了，传的就该是同一个地址。

use std::collections::BTreeSet;
use std::path::Path;

#[path = "../../cogneva/tests/common/root.rs"]
mod root;
#[path = "../../cogneva/tests/common/rust_production.rs"]
mod rust_production;

use root::repo_root;
use rust_production::{production_sources, relative_name};

/// 部署器这一侧的名字：环境变量、npm 的设置项、装着默认值的那个常量。
const KNOB: &str = "COGNEVA_NPM_REGISTRY";
const NPM_SETTING: &str = "NPM_CONFIG_REGISTRY";
const DEFAULT_CONST: &str = "const WEB_NPM_REGISTRY: &str = ";

/// 问生产源「谁认得这个旋钮」。
fn files_naming(root: &Path, needle: &str) -> BTreeSet<String> {
    let sources = production_sources(root);
    sources
        .iter()
        .filter(|path| {
            std::fs::read_to_string(path)
                .map(|text| text.contains(needle))
                .unwrap_or(false)
        })
        .map(|path| relative_name(root, path))
        .collect()
}

fn text_of(root: &Path, relative: &str) -> String {
    let path = root.join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|why| panic!("读不到 {}: {why}", path.display()))
}

/// 跟着一个标记走到它后面那个地址。
///
/// 两处载体写法不同：Rust 里是 `= "https://…"`，shell 里是 `NAME=https://…` 到行尾。
/// 都按「地址字符集」读到第一个不是地址的字符为止，于是同一条读法管两处。
///
/// 抽不出来时 panic，而不是返回空串：空串会让「两侧都是空」这件事看起来像对齐。
fn url_after(text: &str, marker: &str) -> String {
    let at = text
        .find(marker)
        .unwrap_or_else(|| panic!("{marker} 不在这份文件里了"));
    let rest = text[at + marker.len()..].trim_start_matches('"');
    let url: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '/' | '.' | '-' | '_'))
        .collect();
    assert!(!url.is_empty(), "{marker} 后面不是一个地址");
    url
}

/// 只有部署器这一个生产源认得这个旋钮。
///
/// 一个文件名不出来，说明这个常量被删了或改名了，后面几条判的都是别的东西；
/// 两个以上，说明「默认值只有一份」这句话已经不成立，而后面那条比对的只是其中一份。
#[test]
fn one_production_file_declares_the_npm_registry_knob() {
    let root = repo_root();
    let files = files_naming(&root, KNOB);
    assert_eq!(
        files.len(),
        1,
        "认得 {KNOB} 的生产源有 {} 份：{files:?}——这份门只比其中一份，多出来的每一份\
         都是没人比过的第二个默认",
        files.len()
    );
    let only = files.into_iter().next().expect("刚刚数过是一份");
    assert!(
        only.ends_with("mainline_deployer.rs"),
        "认得 {KNOB} 的不再是部署器（{only}）——先确认旋钮搬去了哪儿，再搬这条门"
    );
}

/// 部署器的默认值，和镜像构建那趟传下去的地址是同一个。
///
/// 这是这条门真正要拦的那一格：默认值改了一处、构图脚本还写着旧的那处，两条路
/// 从此拉的不是同一个源。两侧都非空才算比过。
#[test]
fn the_deployers_default_is_the_url_the_image_build_passes() {
    let root = repo_root();
    let deployer = text_of(&root, "crates/cog-reflection/src/mainline_deployer.rs");
    let declared = url_after(&deployer, DEFAULT_CONST);

    let script = text_of(&root, "deploy/scripts/build-release-image.sh");
    let passed = url_after(&script, "NPM_REGISTRY=");

    assert_eq!(
        declared, passed,
        "部署器的默认 npm registry 是 {declared}，而镜像构建传给 `NPM_REGISTRY` 的是 \
         {passed}——两条前端构建的路从此拉的不是同一个源，而两边产物各自看起来都完整"
    );
}

/// 构图脚本传的那个参数，Dockerfile 真的交给了 npm。
///
/// 参数名对得上、而 Dockerfile 根本没读它，是另一条同样静默的路：脚本里写着
/// `https://registry.npmmirror.com`，镜像照样去 npmjs 拉包。
#[test]
fn the_image_build_hands_the_argument_to_npm() {
    let root = repo_root();
    let dockerfile = text_of(&root, "Dockerfile");
    assert!(
        dockerfile.contains("ARG NPM_REGISTRY"),
        "Dockerfile 不再声明 `ARG NPM_REGISTRY`，构图脚本传的那个地址没有落点"
    );
    assert!(
        dockerfile.contains(r#"npm config set registry "$NPM_REGISTRY""#),
        "Dockerfile 收了 `NPM_REGISTRY` 却没把它设成 npm 的 registry——参数在，作用没有"
    );
}

/// overlay 这一趟用那个旋钮，而不是继续把常量钉死在命令行里。
///
/// 钉死的形式没有红过，因为默认值恰好是对的；只有在主机够不着默认地址那天才分家，
/// 而那天部署器的报错是构建失败，与「这台机器连不上外网」读起来是两件事。
#[test]
fn the_overlay_takes_the_registry_from_the_resolver() {
    let root = repo_root();
    let source = text_of(&root, "crates/cog-reflection/src/mainline_deployer.rs");
    // 在这一份文件里，测试模块不是产出面：它断言的是这段文本「长什么样」，把它的
    // 字符串算进下面的站点数，会让门去要求一条测试改写自己的措辞。
    let deployer = source
        .split("\n#[cfg(test)]\nmod tests {")
        .next()
        .expect("按定义总有第一段");

    let sites: Vec<&str> = deployer
        .lines()
        .filter(|line| line.contains(NPM_SETTING))
        .collect();
    assert_eq!(
        sites.len(),
        1,
        "传 {NPM_SETTING} 的地方有 {} 处：{sites:?}——多出来的每一处都是这条门看不见的一条路",
        sites.len()
    );
    let site = sites[0];
    assert!(
        site.contains("web_npm_registry()"),
        "overlay 这一趟把 {NPM_SETTING} 钉在常量上：{site}\n\
         这样主机把测过的地址放进 {KNOB} 也进不来"
    );
}
