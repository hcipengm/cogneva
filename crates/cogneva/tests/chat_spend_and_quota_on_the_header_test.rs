//! 花销与配额在对话里看得见：三段各自会静默地断。
//!
//! 1. 服务端把会话合计放进 `message.usage` 帧，而界面不认那一格——屏幕上就只有
//!    这一轮的数，「这段聊下来多少钱」永远问不出来，且没有任何报错；
//! 2. 界面认它，却不在标题行上画——那一格等于不存在；
//! 3. 界面对「没配过上限」与「没读到」用了同一句话（或者干脆留白）：前者被读成
//!    「无限」、后者被读成「没花」，两者都与事实相反。
//!
//! WebUI 没有 JS 测试跑道，这一族门读源码（同 `todo_panel_shows_one_list_test`）。
//! 服务端那一半（帧里真有这些数、路由真在、拒绝真译成人话）由 `cog_gateway::chat`
//! 的单元用例与 `chat_quota_read_face` 钉住。
//!
//! 强度要说清：这里判的是**源码里那个函数怎么写的**，不是渲染出来的那一行字。
//! 铺开整份文件找一句话，等于只要求「这个词还在文件里某处」——把两句话画反了
//! 也能过。所以最要紧的那一条（「未配置」「读不到」「花了零」是三种画法）判的是
//! `quotaLine` 函数体里的分支，而不是整个文件。即便如此，它仍然看不见一次真实
//! 渲染，这一层靠人眼看界面。

use crate::common::routes::read_repo;

const EVENTS: &str = "web/src/types/events.ts";
const CLIENT: &str = "web/src/api/client.ts";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 取 `start` 开头那个函数的函数体：从这一行起，到下一个顶格 `}` 为止。
///
/// 有了它，「这句话在这个分支里」才判得出来；只判「这句话在这份文件里」的话，
/// 两个分支互换位置时门仍然是绿的。
fn function_body<'a>(source: &'a str, start: &str) -> &'a str {
    let from = source
        .find(start)
        .unwrap_or_else(|| panic!("源码里找不到 {start}"));
    let rest = &source[from..];
    let end = rest.find("\n}\n").unwrap_or(rest.len());
    &rest[..end]
}

/// 界面对 `message.usage` 里的 `session` 那一格是真解析的，不是丢掉。
#[test]
fn the_client_reads_the_conversation_total_off_the_usage_frame() {
    let events = read_repo(EVENTS);
    assert!(
        events.contains("session: sessionSpend(inner.session)"),
        "{EVENTS} 不再把帧里的会话合计读进来：界面上这一段的花销永远是空的，\
         而空着和「没花」看起来一样"
    );
    assert!(
        events.contains("function sessionSpend("),
        "{EVENTS} 少了把帧字段翻成界面形状的那一步：直接塞原对象进去，字段名的\
         拼写错误会一路走到屏幕上"
    );
    assert!(
        events.contains("readable: boolean"),
        "{EVENTS} 的形状里没有「读没读到」这一格：读不到账本与花了零会被画成同一句话"
    );
}

/// 配额读数走的是服务端那条与管理面同源的读口，不是前端自己算的。
#[test]
fn the_client_reads_the_quota_from_the_same_cell_the_proxy_judges() {
    let client = read_repo(CLIENT);
    assert!(
        client.contains("'/api/v1/chat/quota'"),
        "{CLIENT} 不再读这条路由：界面上的余量只能靠猜，或者根本不显示"
    );
    assert!(
        client.contains("effective?: ChatQuotaCell | null"),
        "{CLIENT} 的形状把「没人配过上限」和「配额是 0」合并了：前者是未配置，\
         不是用完了"
    );
    let pane = read_repo(PANE);
    assert!(
        pane.contains("getChatQuota()"),
        "{PANE} 不调用这条读口：会话头那一格永远没有数"
    );
}

/// 两格都画在会话标题行上：一段对话的属性就该在它的头上。
#[test]
fn the_header_shows_what_this_conversation_spent_and_what_is_left() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("{sessionSpendLine(sessionSpend)}"),
        "{PANE} 的标题行不再画会话花销：帧里有数也没人看见"
    );
    assert!(
        pane.contains("{quotaLine(quota, quotaUnreadable)}"),
        "{PANE} 的标题行不再画配额余量"
    );
    // 起算点必须是「最近一条带着会话合计的消息」：从第一条算会停在这一段最早
    // 那一轮的数上，比实际少，而且看起来完全正常。
    assert!(
        pane.contains("const sessionSpend = useMemo("),
        "{PANE} 少了从消息里取会话合计的那一步"
    );
}

/// 「未配置」「读不到」「花了零」是三句话，不许折成一句。
#[test]
fn an_unset_ceiling_and_an_unreadable_ledger_are_not_drawn_as_zero() {
    let pane = read_repo(PANE);
    let quota_line = function_body(&pane, "function quotaLine(");
    assert!(
        quota_line.contains("配额读不到"),
        "读不到那一支没有自己的话：它会与别的状态同形，读者只能把它读成零：{quota_line}"
    );
    assert!(
        quota_line.contains("配额未配置"),
        "没人配过上限那一支没有自己的话：{quota_line}"
    );
    assert!(
        quota_line.contains("(未配置上限)"),
        "花着钱但没人配过上限时不再明说「未配置」：剩下的只有「今日 N」，\
         读起来像还有余量：{quota_line}"
    );
    // 三句话必须落在三个不同的分支里：一句写在到达不了的地方，与没有这句一样。
    let unset_branch = quota_line
        .split("if (effective === null)")
        .nth(1)
        .expect("未配置那一支要先被判出来，不能等画完上限再补救");
    assert!(
        unset_branch.contains("配额未配置"),
        "「未配置」这句要真在 effective === null 那一支里：{unset_branch}"
    );

    let spend_line = function_body(&pane, "function sessionSpendLine(");
    assert!(
        spend_line.contains("会话花销读不到"),
        "会话花销读不到时没有自己的话：留白会被读成「这一段没花」：{spend_line}"
    );
    assert!(
        spend_line.contains("unpricedCalls"),
        "没定价的调用次数要在行里单列：藏在金额后面等于说金额是测量值，\
         而它只是下界：{spend_line}"
    );
}
