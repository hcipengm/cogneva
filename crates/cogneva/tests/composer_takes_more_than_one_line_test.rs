//! 输入那一格能装下不止一行：回车发出去，Shift+Enter 换行。
//!
//! 一条 `<input type="text">` 从根上装不下一段多行的话——贴进来的换行会被浏览器
//! 吞掉，于是「贴一段报错、贴一份步骤」在发出去之前就已经不是原文了。这一面没有
//! 前端测试跑道（`web/package.json` 里只有构建工具），判据只能读源码。
//!
//! 三件事一起才叫「多行」：
//!
//! 1. 承载它的元素是 `<textarea>`，不是 `<input>`；
//! 2. 框随内容长高——固定一行高时，第二行藏在框里，光标跑到看不见的地方；
//! 3. 回车仍然发出去，Shift+Enter 才是换行；而**选词中的回车**（输入法）既不发也
//!    不换行——那是「确认这个词」，在它上面做任何一种动作都会打断一段中文。

use crate::common::routes::read_repo;

const PANE: &str = "web/src/conversation/ConversationPane.tsx";

#[test]
fn the_composer_is_a_textarea_not_a_one_line_input() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("<textarea"),
        "{PANE} 里没有 textarea：多行的一句话在发出去之前就被换行吞掉了"
    );
    // 选附件那一个 `<input type="file">` 不算：它不承载这一句话的字。文字那一格
    // 只要还是 `type="text"`，多行就没落地。
    assert!(
        !pane.contains("type=\"text\""),
        "{PANE} 里还有单行文本框：输入那一格仍然装不下一行以上的字"
    );
}

#[test]
fn the_box_grows_with_what_is_in_it() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("rows={1}"),
        "{PANE} 的草稿格没有从一行起头：一句短话也会留一大片空白"
    );
    assert!(
        pane.contains("scrollHeight"),
        "{PANE} 不问元素自己省出来的高度：写第三行时光标会跑到框外面"
    );
    // 长高挂在草稿本身上：改一个字才重量一次，别的时候不该碰布局。
    assert!(
        pane.contains("}, [draft]);"),
        "{PANE} 的长高没有跟着草稿走：要么一次都不量，要么每一帧都量"
    );
}

#[test]
fn enter_sends_and_shift_enter_writes_a_new_line() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("e.key !== 'Enter' || e.shiftKey"),
        "{PANE} 的回车不再是「非 Shift 才发」：Shift+Enter 会把半句话发出去"
    );
    // 输入法选词中的那一次回车不是这个键的语义，它属于正在选的那个词。
    assert!(
        pane.contains("e.nativeEvent.isComposing"),
        "{PANE} 不看输入法是否正在选词：打中文时按回车会把半句话送走"
    );
    assert!(
        pane.contains("e.nativeEvent.keyCode === 229"),
        "{PANE} 不认 keyCode 229 那条老路径：Safari/旧内核上选词中的回车照样发出去"
    );
    assert!(
        pane.contains("send();"),
        "{PANE} 的回车不再走那一个发点：一条消息可能从两条路出去"
    );
    assert_eq!(
        pane.matches("const send = useCallback(").count(),
        1,
        "{PANE} 的发点不止一个：回车与按钮各走各的，账上的顺序会分叉"
    );
}
