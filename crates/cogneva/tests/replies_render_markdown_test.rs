//! 回复的正文按 Markdown 画：列表、代码块、表格、diff，而不是一排 `**` 与两排 ```。
//!
//! 这条链很短，断在哪儿都很难看出来：
//!
//! 1. 正文直接铺进 DOM——一段贴回来的补丁要靠人自己对齐正负号；
//! 2. 渲染出来的 HTML 不过消毒——一段回复里可以带 `<img onerror=…>`，而这一面在
//!    一个已经登录的浏览器里跑（模型读过仓库、网页和别人发来的消息，间接提示注入
//!    就能到这一步）；
//! 3. diff 整块染一个颜色——读者分不出哪几行是新加的；
//! 4. 认不出来的语言硬套高亮——关键字被染错色，而原文本来是准的。
//!
//! 判据在源码上：这一棵树没有前端测试运行器（`web/package.json` 里只有构建工具），
//! 能替这几条做的门只能读源码。

use crate::common::routes::read_repo;

const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const VIEW: &str = "web/src/conversation/Markdown.tsx";
const RENDER: &str = "web/src/conversation/markdown.ts";
const TOOL_DIFF: &str = "web/src/conversation/toolDiff.ts";
const CSS: &str = "web/src/styles/index.css";
const PACKAGE: &str = "web/package.json";

/// 正文进的是那个渲染器，不是直接被铺进 DOM。
#[test]
fn the_reply_body_goes_through_the_renderer() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("<Markdown text={message.text}"),
        "{PANE} 不再把正文交给 Markdown 渲染器：回复又变回一排 `**` 与两排 ```"
    );
    assert!(
        !pane.contains("{message.text}\n"),
        "{PANE} 仍然把正文原文直接铺进 DOM"
    );

    let view = read_repo(VIEW);
    assert!(view.contains("renderMarkdown(text)"), "{VIEW} 不调用渲染器");
    assert!(
        view.contains("useMemo"),
        "{VIEW} 每渲染一次就重新解析一遍：花销、时刻一变就把整段正文重排"
    );
}

/// 渲染出来的 HTML 必须过消毒：这些字是模型写的，而模型读过外面。
#[test]
fn what_the_model_wrote_is_sanitized_before_it_reaches_the_page() {
    let render = read_repo(RENDER);
    assert!(
        render.contains("DOMPurify.sanitize"),
        "{RENDER} 把模型写的 HTML 原样放进页面：一段回复就能在这一面里跑脚本"
    );
    assert!(
        render.contains("FORBID_ATTR: ['style']"),
        "{RENDER} 不拦行内样式：模型写出来的一句 style 可以盖住整页的布局"
    );
    assert!(
        render.contains("ADD_ATTR: ['target']"),
        "{RENDER} 把链接的 target 也一起消毒掉了：点一个链接就把这一面（以及还没收尾的那一轮）换掉"
    );
}

/// diff 一行一个颜色，正负号在同一行上看得见。
#[test]
fn a_diff_is_coloured_line_by_line() {
    let render = read_repo(RENDER);
    assert!(
        render.contains("language === 'diff'"),
        "{RENDER} 不认 diff 这种围栏：补丁被当成普通代码画，正负号只有人自己看"
    );
    assert!(render.contains("'diff-add'"), "{RENDER} 不给新加的行上色");
    assert!(render.contains("'diff-del'"), "{RENDER} 不给删掉的行上色");
    assert!(
        render.contains("split('\\n')"),
        "{RENDER} 不按行切 diff：整块一个颜色，读者分不出哪几行是新加的"
    );

    let css = read_repo(CSS);
    assert!(
        css.contains(".diff-add"),
        "{CSS} 没有新加那一行的样式：记号写出来了，屏幕上还是一样的颜色"
    );
    assert!(css.contains(".diff-del"), "{CSS} 没有删掉那一行的样式");
}

/// 认不出来的语言按原文画；围起来的代码用等宽体。
#[test]
fn a_language_the_highlighter_does_not_know_is_drawn_as_written() {
    let render = read_repo(RENDER);
    assert!(
        render.contains("hljs.getLanguage(language)"),
        "{RENDER} 对认不出来的语言也套高亮：关键字会被染错色，而原文本来是准的"
    );
    assert!(
        render.contains("escapeHtml(code)"),
        "{RENDER} 不转义代码块里的原文：代码里的 `<` 会被浏览器当成一个标签"
    );

    let css = read_repo(CSS);
    assert!(
        css.contains(".md .md-code"),
        "{CSS} 没有围起来那段代码的样式：它与正文长得一样，读者认不出那是一段代码"
    );
    assert!(
        css.contains(".hljs-keyword"),
        "{CSS} 没有高亮记号的色板：高亮器写出来的类名没有颜色，等于没高亮"
    );
}

/// 三个库都登记在 `package.json` 里：干净检出的机器 `npm ci` 之后才编得出来。
#[test]
fn the_renderer_libraries_are_declared() {
    let package = read_repo(PACKAGE);
    for dependency in ["\"marked\"", "\"dompurify\"", "\"highlight.js\""] {
        assert!(
            package.contains(dependency),
            "{PACKAGE} 里没有 {dependency}：干净检出装不出这一份，构建红在一次找不到模块上"
        );
    }
}

/// 改动类工具在流里画的是行级 diff，不是两份 JSON 原文。
#[test]
fn an_edit_tool_shows_its_change_line_by_line() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("toolDiff(call)"),
        "{PANE} 不认改动类工具：`edit_file` 与 `apply_patch` 仍然只有入参与返回原文"
    );
    assert!(pane.contains("tool-diff"), "{PANE} 上没有那份 diff 的落点");

    let diff = read_repo(TOOL_DIFF);
    for tool in ["'edit_file'", "'apply_patch'", "'write_file'"] {
        assert!(
            diff.contains(tool),
            "{TOOL_DIFF} 不认 {tool}：它的改动只能靠人读 JSON 读出来"
        );
    }
    assert!(
        diff.contains("export function lineDiff"),
        "{TOOL_DIFF} 不逐行比对：`edit_file` 的「找到这段、换成这段」只能整段删整段加"
    );
    // `apply_patch` 的入参本来就是统一 diff：认它自己的正负号，不再自己算一遍。
    assert!(
        diff.contains("\"+++\"") || diff.contains("'+++'"),
        "{TOOL_DIFF} 不认补丁自己的文件头：`+++` 那一行会被画成一行新加的代码"
    );
    // `write_file` 的记录里没有改动前的文件，于是它画的是整份文件——这一句话必须在
    // 屏幕上，不然满屏的 `+` 读起来像这个文件原来不存在。
    assert!(
        diff.contains("整份写进去的文件"),
        "{TOOL_DIFF} 把整份内容画成 diff 而不说明：读者会以为这个文件是新建的"
    );
}
