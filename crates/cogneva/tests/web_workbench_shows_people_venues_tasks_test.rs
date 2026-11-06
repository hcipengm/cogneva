//! 工作台第一眼要看得见「人、场、任务」三样，而且三样都点得进去。
//!
//! 这三样一直是有的：`/api/v1/agents` 报名单，`/api/v1/venues` 报每个场地的
//! 三条读数，`/api/v1/tasks/status` 报任务计数。但它们只挂在左侧那列图标后面，
//! 而那一列**只画图标不写字**：22 格形状各异的方框，名字留在悬停提示里。新读者
//! 认不出哪一格是「员工」、哪一格是「场地」——认不出来的入口与不存在的入口，在
//! 屏幕上同形。
//!
//! 所以这里钉三件事，每一件都对应「读者看不见它」的一种具体形状：
//!
//! 1. 活动栏把名字写进元素体，不是只写在 `text=`（悬停提示）上。名字只存在于
//!    悬停提示里时，读者必须先猜中才能读到它。
//! 2. 工作台顶上有一条同时读这三份读数的带子，三格各自通向对应的那一面。缺一条
//!    读数，那一格就只剩一个数得靠切面去猜；缺一个去向，读者看见了也走不过去。
//! 3. 空名单与「还没读到」分开画。名单是心跳，30 秒不来就会被摘掉，所以空名单
//!    是**一个真实的状态**；把它画成转圈，读者看到的是一个永远转的圈，会以为
//!    界面坏了——而这正是「平台在线的智能体一个都看不到」被报上来的那一次的
//!    形状之一。
//!
//! 判据都在源码上：这一棵树没有前端测试运行器（`web/package.json` 里只有构建
//! 工具），能替这三条做的门只能读源码。判据不读注释（`strip_comments`），改一句
//! 说明不该让它变红。

use std::path::{Path, PathBuf};

use crate::common;

fn root() -> PathBuf {
    common::root::repo_root()
}

/// 读一份前端源码，去掉注释。注释不是界面：一句「这里画名字」的说明不会让读者
/// 看见任何名字，按它判绿就是判据把自己的题目换掉了。
fn source(root: &Path, rel: &str) -> String {
    common::routes::strip_comments(&common::fsio::read(&root.join(rel)))
}

/// 左侧那列必须把每一格的名字写出来。
///
/// 判据是「名字写在元素体里」：`text={item.label}` 那种写法把名字交给悬停提示，
/// 而提示在读到之前是不可见的。所以要求至少有一处 `{item.label}` 不是 `text=`
/// 的右值。三格的名字（员工／场地／任务）另各钉一次：名字本身被改掉，活动栏就
/// 不再指着那三面。
#[test]
fn the_activity_bar_writes_the_name_out_instead_of_hiding_it_in_a_tooltip() {
    let text = source(&root(), "web/src/shell/ActivityBar.tsx");
    for label in ["员工", "场地", "任务"] {
        assert!(
            text.contains(label),
            "活动栏的名单里少了「{label}」这一格：三面里的这一面没有入口"
        );
    }
    let mut visible = 0usize;
    let mut from = 0usize;
    while let Some(at) = text[from..].find("{item.label}") {
        let absolute = from + at;
        from = absolute + "{item.label}".len();
        // `text={item.label}` 是悬停提示；它前面紧挨着 `text=` 就不算写出来的名字。
        if !text[..absolute].ends_with("text=") {
            visible += 1;
        }
    }
    assert!(
        visible > 0,
        "活动栏只在悬停提示（`text={{item.label}}`）里写了名字，元素体里一个都没有：\
         图标-only 的一列，读者认不出哪一格是员工、场地、任务"
    );
}

/// 工作台顶上那条带子：三份读数都在，三格各自通得过去。
///
/// 读的是带子自己那一份源码，不是工作台：把三份读数拆到三个面里也行，但那样
/// 工作台顶上那一行就又回到了「什么都没有」。同时钉住工作台确实把它挂了出来——
/// 写好的带子没人渲染，读者一样看不见。
#[test]
fn the_workbench_carries_one_band_that_reads_people_venues_and_tasks() {
    let root = root();
    let view = source(&root, "web/src/views/WorkspaceView.tsx");
    assert!(
        view.contains("<WorkbenchBand"),
        "工作台没有挂上「人 · 场 · 任务」那条带子：三样读数回到各自的面里，\
         第一眼还是只剩对话"
    );
    let band = source(&root, "web/src/views/WorkbenchBand.tsx");
    for (call, what) in [
        ("listAgents", "在岗名单"),
        ("getVenues", "场地读数"),
        ("getTaskSummary", "任务总览"),
    ] {
        assert!(
            band.contains(call),
            "带子里没有读{what}（`{call}`）：那一格只能靠切面去猜"
        );
    }
    for to in ["people", "venues", "tasks"] {
        assert!(
            band.contains(&format!("to=\"{to}\"")),
            "带子里没有通向「{to}」这一面的格子：看见了也走不过去"
        );
    }
}

/// 空名单不许画成转圈。
///
/// 判据是先后次序：转圈只能出现在 `agentsLoaded ?` 之后——也就是说，只有在
/// 「还没读回来过」那一支里。名单读回来是空的，读者该看到的是「此刻没有在岗的
/// 员工」，不是一圈永远转不完的进度。
#[test]
fn an_empty_roster_says_so_instead_of_spinning_forever() {
    let text = source(&root(), "web/src/views/PeopleView.tsx");
    let gate = text.find("agentsLoaded ?").expect(
        "员工名单还分不清「读回来是空的」与「还没读回来」：空名单会画成转圈，\
         读者看到的是永远转的进度条",
    );
    let spinner = text
        .find("<Loading")
        .expect("员工名单连「还没读回来」的那一支都没有了");
    assert!(
        spinner > gate,
        "转圈画在「读回来是空的」那一支里（位置 {spinner}，判据在 {gate}）：\
         平台此刻真的没有在岗的人时，屏幕上只有一个永远转的圈"
    );
    assert!(
        text.contains("此刻没有在岗的员工"),
        "空名单那一支没有说出「此刻没有在岗的员工」：读者分不清「没人」与「界面坏了」"
    );
}

/// 回答里的壳要拆开再显示。
///
/// 这一族员工的回答是一个对象（`{"role":"employee","thought":"…"}`，没有结构的
/// 回答是 `{"result":"…"}`），把 `line.reply` 原样贴出来，读者看到的是一串花
/// 括号——那句话就在里面，却读不成一个回答。「它不回话」与「它的话在一个壳里」，
/// 在屏幕上长得一样。
#[test]
fn a_reply_wrapped_in_json_is_read_out_instead_of_pasted() {
    let text = source(&root(), "web/src/views/PeopleView.tsx");
    assert!(
        text.contains("{spoken(line.reply)}"),
        "会话把 `line.reply` 原样贴出来：回答包在 {{\"role\":…,\"thought\":…}} 里时，\
         读者看到的是花括号，而不是那句话"
    );
    // 没有结构的回答由内核收成 `{"result": …}`（`build_result` 不再问模型改写），
    // 拆壳的那张字段表要认得它，否则最常见的那条路反而漏在壳里。
    for field in ["'thought'", "'result'"] {
        assert!(
            text.contains(field),
            "`spoken` 认不出 {field} 这个字段：这一种回答会在屏幕上留成花括号，\
             而它正是最常见的那一种"
        );
    }
}

/// 这一面画的是帧流，不是快照。
///
/// 网上一直在说的那件事，屏幕上曾经一次也看不到：池里的执行者每次工具调用都发了
/// `tool_execution_start` / `tool_execution_end`，说话有 `message_start` /
/// `message_update` / `message_end`，网关按 `agent:<id>` 转到浏览器，而这一面只读
/// REST 会话行（一问一答）。一次跑了半分钟的活在屏幕上是空白，直到最后一句落库——
/// 「它在干活」与「它卡住了」在这块画布上同形。
///
/// 判据读源码：视图必须从帧流那一份 store 取数，把工具调用与它正在说的那句画出来，
/// 而且说过的话在收尾之后不许被丢回快照。
#[test]
fn the_conversation_pane_draws_the_frame_stream_not_only_the_transcript() {
    let text = source(&root(), "web/src/views/PeopleView.tsx");
    for needed in [
        "useStreamStore",
        "s.toolCalls",
        "s.messages",
        "ToolLine",
        "SaidLine",
    ] {
        assert!(
            text.contains(needed),
            "会话那一面没有从帧流取 `{needed}`：它调了什么、说到哪一句又只能等快照，\
             而一次运行里这两样在总线上本来都有"
        );
    }
    assert!(
        !text.contains("status !== 'done'"),
        "帧流里已经说完的那一句被丢掉了：它正是这一趟的回答，这一面画的就是总线上\
         说过什么，不该说完就换成快照里的那一份"
    );
}
