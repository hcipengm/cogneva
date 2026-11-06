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

/// 两条前端构建的路都在 `npm ci` 之前先升级 npm。
///
/// node:22-slim 自带的 npm 有 "Exit handler never called!" 的已知 bug（npm/cli），
/// 在容器里稳定复现；全量构建早就绕开了它，而 overlay 这一趟——每次换版都会跑的那
/// 一条——没有。绕过它的那句写在一个 `sh -c` 字符串里，没有任何东西比对过两处，
/// 所以同一条已知失败在两条路上是一个绕开、一个没绕开，而两边产物各自看起来都完整。
///
/// 判两个方向：两边都还有这句，且都在 `npm ci` 之前——顺序反了等于没绕（`npm ci`
/// 才是会撞上那个 bug 的那一步）。
#[test]
fn both_build_paths_upgrade_npm_before_ci() {
    let root = repo_root();

    let dockerfile = text_of(&root, "Dockerfile");
    let upgrade = dockerfile
        .find("npm install -g npm@latest")
        .unwrap_or_else(|| panic!("Dockerfile 不再升级 npm，它会撞上 node:22-slim 自带的那个 bug"));
    let ci = dockerfile
        .find("npm ci")
        .unwrap_or_else(|| panic!("Dockerfile 里找不到 `npm ci`"));
    assert!(
        upgrade < ci,
        "Dockerfile 先 `npm ci` 再升级 npm——顺序反了等于没绕开那个 bug"
    );

    let source = text_of(&root, "crates/cog-reflection/src/mainline_deployer.rs");
    let deployer = source
        .split("\n#[cfg(test)]\nmod tests {")
        .next()
        .expect("按定义总有第一段");
    let overlay = deployer
        .find("cd /src/web && npm install -g npm@latest")
        .unwrap_or_else(|| {
            panic!(
                "overlay 这一趟没有在 `npm ci` 之前升级 npm——全量构建绕开的那个 bug，\
                 在这一条上一次也没绕"
            )
        });
    let after = &deployer[overlay..];
    assert!(
        after.contains("npm ci"),
        "overlay 的升级那句后面没有 `npm ci` 了——这一段形状变了，先确认现在跑的是什么"
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

/// 前端用哪个 node 版本，三处载体说的是同一个。
///
/// 同一个前端由两条路构建，而 node 版本写在三处：镜像构建的 `FROM`、把构建器推进
/// 集群 registry 的脚本里那个源镜像、以及推到 registry 的那个 tag。三处各自是个
/// 字面量，只有后两处被一条旧测试连在一起（脚本的 `tag=` 与部署器的常量）。升级
/// 工具链时改了 `FROM` 而没改 provisioning，overlay 就用旧 node 重建前端——产物照样
/// 完整，只是两份前端的工具链不同，这种差异没有任何读数会报。
#[test]
fn the_node_version_is_the_same_in_every_carrier() {
    let root = repo_root();
    let dockerfile = text_of(&root, "Dockerfile");
    let script = text_of(&root, "deploy/scripts/provision-web-builder-image.sh");

    let from = url_after(&dockerfile, "FROM node:");
    let source = url_after(
        &script,
        "source_image=\"${positional[1]:-docker.io/library/node:",
    );
    let tag = url_after(&script, "tag=\"");

    assert_eq!(
        from, source,
        "镜像构建用 node:{from}，而推进 registry 的构建器是 node:{source}——overlay 那一趟\
         会用旧 node 重建前端，两份产物各自都看起来完整"
    );
    assert_eq!(
        from, tag,
        "构建器是从 node:{from} 拿的，却推在 tag `{tag}` 上——注册表里那个名字说的不是\
         它装的东西"
    );
}

/// 发货镜像里装着判前端的那套工具链。
///
/// 沙盒执行器判 `web/src/**` 的变更时跑的是 `deploy/scripts/web-build.sh`，而这个
/// 脚本在没有 node/npm 时**退出 2**——「我跑不了」，不是「我判过了」。于是运行段少
/// 了那几行 COPY 时，前端这一族的准入条件在沙盒里一次都执行不了，而每一条 web 变更
/// 都会按「声明过条件、条件跑不起来」被拒；拒收的理由看起来像变更的问题，没有一个
/// 字提到镜像。这与 shellcheck 那一格同形：判据需要一个工具，工具不在镜像里时，
/// 缺工具被读成了判词。
///
/// 只判**运行段**（最后一个 `FROM` 之后）：`webbuilder` 阶段本来就有 node，它有没有
/// 并不说明发出去的镜像有没有。
#[test]
fn the_shipping_image_carries_the_frontend_toolchain() {
    let root = repo_root();
    let dockerfile = text_of(&root, "Dockerfile");
    let runtime = dockerfile
        .rsplit_once("\nFROM ")
        .map(|(_, tail)| tail)
        .expect("Dockerfile 里找不到运行段（最后一个 FROM）");

    for needed in [
        "COPY --from=webbuilder /usr/local/bin/node",
        "COPY --from=webbuilder /usr/local/lib/node_modules",
        "npm-cli.js",
    ] {
        assert!(
            runtime.contains(needed),
            "运行段里没有 `{needed}`：沙盒里的 web-build 会恒为退出码 2，前端这一族\
             一条变更都判不了，而拒收的理由会说成是变更的问题"
        );
    }
    assert!(
        runtime
            .lines()
            .any(|line| line.contains("node --version") && line.contains("npm --version")),
        "运行段没有在那几行 COPY 之后跑一次 `node --version && npm --version`：拷进来的\
         东西能不能用，只有构建时问一句才知道"
    );
}

/// 判格式化之前刷新工具链这一趟，读的镜像源在运行期要真的存在。
///
/// 两头都在的时候什么都不会响，缺一头才是病：少了运行段那两行 ENV，构建期那句
/// `export` 出不了 RUN，运行期的刷新（`rustup check` 与按版本号那一趟装）去打官方源
/// ——CN 侧那一次就是几十秒起步、撞上墙钟界，退化成「没刷成」，于是
/// `BUILTIN_TOOLCHAIN_CHECK` 那条环原样关回去；少了白名单里那两个键，子进程
/// `env_clear()` 之后也读不到它们，效果一模一样，而日志上只说「刷不成」，没有一个字
/// 提到是环境没递进去。
///
/// 运行段之后的判据与 [`the_shipping_image_carries_the_frontend_toolchain`] 同口径：
/// `webbuilder` 阶段有没有这些变量，不说明发出去的镜像有没有。
#[test]
fn the_formatter_refresh_can_read_its_toolchain_source_at_runtime() {
    let root = repo_root();
    let dockerfile = text_of(&root, "Dockerfile");
    let runtime = dockerfile
        .rsplit_once("\nFROM ")
        .map(|(_, tail)| tail)
        .expect("Dockerfile 里找不到运行段（最后一个 FROM）");
    for needed in ["RUSTUP_DIST_SERVER=", "RUSTUP_UPDATE_ROOT="] {
        assert!(
            runtime.contains(needed),
            "运行段没有 `{needed}`：刷新那一趟会去打官方源，撞上墙钟界之后退化成\
             「没刷成」，而判据仍旧是镜像里那一版 rustfmt"
        );
    }

    let pipeline = text_of(&root, "crates/cog-reflection/src/change_pipeline.rs");
    assert!(
        pipeline.contains(r#"["rustup", "check"]"#)
            && pipeline.contains("\"toolchain\",")
            && pipeline.contains("\"install\","),
        "判据里没有「问出当前 stable 是哪一版、按那个版本号装一份」这两趟：\
         镜像只在有变更落地时才重建，判据落后就形成环——判据旧 ⇒ 每笔红 ⇒ \
         没有落地 ⇒ 没有重建。就地更新那条路走不通（镜像的下层 rename 是 EXDEV），\
         所以这里认的必须是新起一份工具链目录的那条路"
    );
    for key in ["\"RUSTUP_DIST_SERVER\",", "\"RUSTUP_UPDATE_ROOT\","] {
        assert!(
            pipeline.contains(key),
            "验证进程的白名单里没有 {key}：子进程 `env_clear()` 之后读不到镜像源，\
             刷新在 CN 侧恒为失败"
        );
    }
}
