//! 这一轮交出来的活，在回答属主之前由另一次调用判过——判者只有读的手。
//!
//! 判据审要回答的是「这段终态在这棵树上立不立得住」，不是「这次运行成没成」。
//! 它必须是**另一次调用**（写终态与审终态的不是同一个），也必须是**只能读**的
//! 一次：把整张工具表交过去，那一次的手就能派活、改文件、发消息，「只判终态」
//! 这句话在结构上就不成立。判不成立的活当场撤回，判词随这一轮的交付物回给属主，
//! 而不是留给执行者去撞。
//!
//! 判据读源码：这一棵树没有一份能在单测里跑起真实判者模型的夹具，能替这条接线
//! 做的门只能读装配层写了什么。顺序也判：审必须排在回答之后、回答已在手上。

use crate::common;

fn source(rel: &str) -> String {
    let root = common::root::repo_root();
    common::routes::strip_comments(&common::fsio::read(&root.join(rel)))
}

#[test]
fn the_turn_reviews_what_it_filed_before_it_lands() {
    let text = source("crates/cogneva/src/conversation.rs");
    for (needle, why) in [
        (
            "EvaluatorActor::new(",
            "没有另一次调用判终态：写终态与审终态成了同一次",
        ),
        ("review_criteria(", "判据审没接上：不成立的终态也照进任务图"),
        (
            "tools.read_only()",
            "判者拿到了整张工具表：审的那一次的手也能落下东西",
        ),
        (
            "cog_core::roles::EVALUATOR",
            "判者用的不是审那个角色：账上分不出这是哪一次调用",
        ),
        (
            "criterion-review:",
            "判者没有自己的身份：两次调用在账上同形",
        ),
        (
            "cancel_task_with_reason(",
            "判不成立的活没有当场撤回：它留在了批里",
        ),
        (
            "\"intent_review\"",
            "判词没有随交付物回给属主：属主看不到为什么不成立",
        ),
    ] {
        assert!(text.contains(needle), "{why}（缺 {needle:?}）");
    }

    // 审排在回答之后：判词要随这一轮的答案一起回去，不能抢在答案之前。
    let answered = text
        .find("run_conversation(")
        .expect("这一轮没有跑内核：装配层改了形状");
    let reviewed = text.find("review_criteria(").expect("判据审没有接上");
    assert!(
        answered < reviewed,
        "判据审排在回答之前：判词赶不上这一轮的交付物"
    );
}
