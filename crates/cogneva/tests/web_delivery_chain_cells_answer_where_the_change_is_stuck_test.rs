//! 链路那七格各答一问：我的改动卡在哪一步。每一格都必须由**对的那份读数**回答，
//! 而且要答「我的改动」，不只答「系统自己此刻是什么状态」。
//!
//! 这七问在顺畅的时候同答案，所以旧判据要等出事才显形。2026-11-10 实测三处：
//!
//! - 部署一格报绿而改动一行没上线：main 已从 `d436c9c3` 推进到 `88f5e0aa`，部署器
//!   因为没有窗口内的绿 rev 而 hold，现网镜像还停在 `main-18c1a02049b0`，而集群里
//!   4 / 4 个工作负载全都「已滚到位」。滚到位说的是「这一版的副本都起来了」，它答
//!   不了「这一版是不是 main 上那一版」；
//! - 编辑一格把一条 763938 秒前就停了的分支（`evolution-release`）说成「此刻在编辑
//!   的那棵树」——判据是「非 main 里最新那条」，不是「main 推进之后还在落的」；
//! - 构建一格一直说「启动以来还没有构建过」：演进进程的窗口随重启清零（13:54 重启后
//!   为空），而主线部署那个 Job 正是把现网那一版编出来的东西。
//!
//! 判据都在源码上：这一棵树没有前端测试运行器（`web/package.json` 里只有构建工具），
//! 能替这条做的门只能读源码。判据不读注释（`strip_comments`），改一句说明不该让它变红。
//! 名字与形状也不由前端发明：镜像标签前缀、主线 Job 前缀都从部署器源码里读出来再比。

use std::path::{Path, PathBuf};

use crate::common;
use crate::common::routes::region_between;

fn root() -> PathBuf {
    common::root::repo_root()
}

/// 读一份源码，去掉注释。注释不是界面：一句「这里比一下镜像标签」的说明不会让页面
/// 多出任何一格读数，按它判绿就是判据把自己的题目换掉了。
fn source(root: &Path, rel: &str) -> String {
    common::routes::strip_comments(&common::fsio::read(&root.join(rel)))
}

/// `needle` 之后第一对花括号之间的全文（含括号本身）。
///
/// 判据要问的是「这一格里有没有做这件事」，问整份文件等于问「这棵树里有没有」：
/// 别处的 `startsWith` 与别处的 `main.sha` 凑在一起也能判绿。
fn body_after(text: &str, needle: &str) -> String {
    let at = text
        .find(needle)
        .unwrap_or_else(|| panic!("源码里找不到 `{needle}`"));
    let open = at
        + text[at..]
            .find('{')
            .unwrap_or_else(|| panic!("`{needle}` 之后没有函数体"));
    let mut depth = 0i32;
    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return text[open..open + offset + 1].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("`{needle}` 的函数体没有闭合");
}

/// 两个针之间的文字。
///
/// 签名里带返回类型对象字面量的函数（`): { active: boolean; … }`）第一对花括号不是
/// 函数体，按括号配平会把判据截在类型上，于是「这段里没有那件事」被读成「没做」。
/// 这种函数用区间判。
/// 部署器源码里某个 `const NAME: &str = "…"` 的字符串值。
///
/// 名字的形状（镜像标签前缀、主线 Job 前缀）由部署器定，不由前端猜；前端为它写第二个
/// 字面量时，这里把两个值比一次——部署器改了前缀而页面没跟，就红在提交者自己的那一笔上。
fn deployer_const(root: &Path, name: &str) -> String {
    let deployer = source(root, "crates/cog-reflection/src/mainline_deployer.rs");
    let at = deployer
        .find(name)
        .unwrap_or_else(|| panic!("部署器不再声明 {name}：这条形状没有唯一出处了"));
    let rest = &deployer[at..];
    let open = rest
        .find('"')
        .unwrap_or_else(|| panic!("{name} 不再是字符串字面量"));
    let tail = &rest[open + 1..];
    let close = tail
        .find('"')
        .unwrap_or_else(|| panic!("{name} 的字面量没有闭合"));
    tail[..close].to_string()
}

/// 部署器写镜像标签时用的前缀。
fn tag_prefix(root: &Path) -> String {
    deployer_const(root, "MAIN_TAG_PREFIX")
}

const VIEW: &str = "web/src/views/DeliveryView.tsx";

/// 「此刻在编辑的那棵树」是服务端判的，页面只渲染。
///
/// 页面自己挑「非 main 里最新那条」会把一条一天前就停住的分支说成正在编辑的树
/// （2026-11-10 的 `evolution-release`：763938 秒前落笔，而 main 在它之后还推进过）。
/// 判据在服务端是「尖端比 main 的尖端更晚」，所以页面必须读 `editing` / `editing_others`
/// 这两个字段，并按它写归因——自己再排一次序就是第二个判据。
#[test]
fn the_edit_cell_shows_the_tree_the_source_line_calls_editing() {
    let root = root();
    let body = region_between(
        &source(&root, VIEW),
        "function editNode(",
        "function gateNode(",
    );
    assert!(
        body.contains("line.editing"),
        "`editNode` 不读服务端判好的 `editing`：页面自己挑一条分支，就会给死分支安上活的归因"
    );
    assert!(
        body.contains("editing_others"),
        "`editNode` 不说还有几条分支也在动：只给一个名字，读者会以为台上只有一棵树在动"
    );
    assert!(
        body.contains("owner: editing.name"),
        "`editNode` 的归因不是服务端判出的那条分支：正文写着它、归属列写着另一条，读者两边都不信"
    );
    assert!(
        body.contains("editing.subject"),
        "`editNode` 不写那条分支的尖端提交：一个名字答不了「它在改什么」"
    );
}

/// 「推到 main」这一格不能只报 main 走到哪，还要报在动的那条分支进去了没有。
///
/// 只报 main 的尖端时，改动还停在分支上，这一格照样报绿——与部署那一格旧的毛病同形：
/// 一格的结论答的是系统自己的状态，而读者问的是自己的改动。
#[test]
fn the_push_cell_says_when_the_moving_branch_has_not_landed() {
    let root = root();
    let body = region_between(
        &source(&root, VIEW),
        "function pushNode(",
        "function liveMainRev(",
    );
    assert!(
        body.contains("line.editing"),
        "`pushNode` 不看此刻在动的那条分支：它只报 main 在哪儿，答不了「我的改动进去了没有」"
    );
    assert!(
        body.contains("ahead_of_main") && body.contains("没进 main"),
        "`pushNode` 不把「在动的分支还差几笔」写出来：差一笔与已经推上去了在这一格上同形"
    );
    assert!(
        body.contains("contains_main === false"),
        "`pushNode` 不分「不含 main」与「含 main 但多几笔」：前者要先变基，后者快进就行，\
         读者要做的事不一样"
    );
    assert!(
        body.contains("state: behind ? 'waiting' : 'ok'"),
        "`pushNode` 在分支还没进 main 时报绿：这一格的状态把「推上去」这件事说成了已经做完"
    );
}

#[test]
fn the_deploy_cell_strips_the_deployers_own_tag_prefix_out_of_the_live_image() {
    let root = root();
    let prefix = tag_prefix(&root);
    let text = source(&root, VIEW);
    let body = body_after(&text, "function liveMainRev(");
    assert!(
        body.contains("images"),
        "`liveMainRev` 不再读工作负载的镜像列表：现网跑的是哪一版只能从镜像标签读出来"
    );
    let literal = format!("'{prefix}'");
    assert!(
        body.contains(&literal),
        "`liveMainRev` 里没有部署器写的标签前缀 {literal}：前端猜了一个形状，\
         前缀一改，这一格就开始把「读不出来」读成「没有现网版本」"
    );
}

/// 现网是哪一版，只在长驻的、会被滚动更新的工种身上问。
///
/// 2026-11-10 集群里同时有 `main-1968dbff1b45`、`main-a943172fc2bc` 两个 Job 的
/// 镜像在跑：主线部署自己就是一个 Job，它跑的恰恰是**要上线那一版**。把 Job 算进
/// 「现网」，新版本还没起来页面就说已经上线了——比不判还坏。
#[test]
fn the_live_rev_is_read_only_from_the_kinds_that_are_rolled() {
    let root = root();
    let body = body_after(&source(&root, VIEW), "function carriesTheLiveRev(");
    for kind in ["Deployment", "StatefulSet", "DaemonSet"] {
        assert!(
            body.contains(&format!("'{kind}'")),
            "`carriesTheLiveRev` 不再认 `{kind}`：这种工作负载跑的是不是 main 上那一版，\
             页面又没人问了"
        );
    }
    assert!(
        !body.contains("'Job'") && !body.contains("'Pod'"),
        "`carriesTheLiveRev` 把 Job 或裸 Pod 也算成了现网：Job 跑的是要上线那一版，\
         裸 Pod 不被滚动——两者都会让「已经上线」这句话在版本还没起来时说出口"
    );
    let deploy = body_after(&source(&root, VIEW), "function deployNode(");
    assert!(
        deploy.contains("rolling.filter(carriesTheLiveRev)"),
        "`deployNode` 直接扫了全部带副本的工作负载：Job 的镜像会被当成现网"
    );
}

#[test]
fn the_deploy_cell_compares_the_live_rev_against_the_main_tip() {
    let root = root();
    let text = source(&root, VIEW);
    let body = body_after(&text, "function deployNode(");
    assert!(
        body.contains("main.sha.startsWith("),
        "`deployNode` 没有拿 main 的 sha 去做前缀比较：滚到位的判词因此再一次变成\
         「副本都起来了」，而 main 上那一版有没有上线没人问"
    );
    assert!(
        body.contains("这一版还没上线"),
        "`deployNode` 里没有「这一版还没上线」这句判词：两版不一致时读者看不到差在哪"
    );
    assert!(
        body.contains("main.committed_secs_ago"),
        "`deployNode` 没有把「从何时起没上线」落在 main 那一次推进上：拿采集时刻算，\
         会报成一个刚开始的滞后"
    );
    // 判在先、报「都到位」在后：顺序反了，不一致的分支永远走不到——那正是这次
    // 要修的那个形状。
    let mismatch = text
        .find("这一版还没上线")
        .expect("`这一版还没上线` 不在源码里");
    let fallthrough = text
        .find("owner: allRolled ? undefined : stuckOwner")
        .expect("`deployNode` 最后的「都到位」返回不见了");
    assert!(
        mismatch < fallthrough,
        "「这一版还没上线」写在了「都到位」的返回之后：不一致时先返回的那个分支是\
         谁都滚到位，读者的结论又被读成绿"
    );
}

#[test]
fn the_deploy_cell_says_so_when_no_image_carries_a_main_tag() {
    let root = root();
    let text = source(&root, VIEW);
    let body = body_after(&text, "function deployNode(");
    assert!(
        body.contains("没有工作负载的镜像带 main 标签"),
        "`deployNode` 在比不了的时候什么也不说：一个 `registry:2` 与「现网就是 main \
         那一版」在这张表上同形"
    );
    assert!(
        body.contains("裸仓库里没有 main"),
        "`deployNode` 在裸仓库没有 main 时不说比不了：那一头缺席与「现网落后」在这张\
         表上同形"
    );
}

/// 「为什么还没上线」这一问必须由读数回答，而且必须由**对的那一行**回答。
///
/// 三行读数里 `_seconds` 在 hold 结束之后仍然非零（2026-11-02 实测 4802 秒而当时
/// 没有 hold），拿它判「扣着没有」会把一次过去的事读成正在发生。所以判状态只许用
/// `_active`，`_seconds` 只许用来写「这段红多久了」。
#[test]
fn the_deploy_cell_answers_why_with_the_row_that_means_active() {
    let root = root();
    let text = source(&root, VIEW);
    let hold = region_between(&text, "function ciHold(", "function deployNode(");
    for id in ["mainline_ci_hold_active", "mainline_ci_hold_seconds"] {
        assert!(
            hold.contains(&format!("'{id}'")),
            "`ciHold` 不读 `{id}`：部署器是不是在等 CI 绿，这一格又只能靠猜"
        );
    }
    assert!(
        hold.contains("active.value >= 1"),
        "`ciHold` 不拿 `_active` 的值判「扣着没有」：拿时长判会把一次早已结束的红\
         读成正在 hold，读者去查一个不存在的等待"
    );
    let readings = source(&root, "crates/cog-gateway/src/readings.rs");
    for id in ["mainline_ci_hold_active", "mainline_ci_hold_seconds"] {
        assert!(
            readings.contains(&format!("id: \"{id}\"")),
            "`ciHold` 读的读数行 `{id}` 在 readings.rs 里不存在：指到一个不存在的名字，\
             读者会去翻一块永远没有它的面板"
        );
    }
    let deploy = body_after(&text, "function deployNode(");
    assert!(
        deploy.contains("ciHold(observability)"),
        "`deployNode` 手里有观测读数却没有用它回答「为什么还没上线」"
    );
}

/// 链路上的镜像构建是主线部署那个 Job，不是演进进程那份重启即清零的窗口。
///
/// 2026-11-10 实测：演进窗口在两个 pod 上都是空的（进程 13:54 重启），而主线 Job
/// `cogneva-mainline-18c1a02049b0` 已经把现网跑着的那一版编出来了。只读窗口，这一格
/// 会一直说「启动以来还没有构建过」，读者据此以为镜像没编——而现网跑着的那一版正是
/// 它编的。
#[test]
fn the_build_cell_reads_the_mainline_job_that_builds_the_rev() {
    let root = root();
    let text = source(&root, VIEW);
    let prefix = deployer_const(&root, "MAINLINE_JOB_PREFIX");
    let finder = region_between(&text, "function mainlineJob(", "function buildNode(");
    assert!(
        finder.contains(&format!("'{prefix}'")),
        "`mainlineJob` 里没有部署器写的 Job 前缀 '{prefix}'：前端猜了一个名字，部署器一改\
         前缀，这一格就再也找不到主线构建"
    );
    assert!(
        finder.contains("kind === 'Job'"),
        "`mainlineJob` 不按工种筛：同名前缀的 Deployment 或裸 Pod 会被当成主线构建"
    );
    let build = body_after(&text, "function buildNode(");
    assert!(
        build.contains("mainlineJob(workloads.workloads)"),
        "`buildNode` 手里有工作负载却没有先问主线 Job：这一格又只能读那份重启即清零的窗口"
    );
    assert!(
        build.contains("集群里也没有主线部署 Job"),
        "两处读数都空时这一格不说明为什么空：「没有构建过」与「两处都读不到」在判词上\
         同形，而它们要做的事相反"
    );
}

#[test]
fn the_deploy_cell_is_handed_the_source_line_it_compares_against() {
    let root = root();
    let text = source(&root, VIEW);
    // `chainNodes` 的参数是解构出来的，第一对花括号不是函数体，所以这里按两段之间
    // 的文字判，不按括号配平判。
    let from = text
        .find("function chainNodes(")
        .expect("源码里找不到 `chainNodes`");
    let to = text
        .find("function editNode(")
        .expect("源码里找不到 `editNode`");
    assert!(
        text[from..to].contains("deployNode(workloads, line, observability)"),
        "`chainNodes` 没把源线与观测读数一起交给部署那一格：缺源线就比不了现网版本，\
         缺读数就答不了「为什么还没上线」"
    );
}
