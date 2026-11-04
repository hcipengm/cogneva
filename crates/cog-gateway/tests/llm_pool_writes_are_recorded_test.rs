//! 改上游池这件事要在平台自己的账上读得出来，而且**保存不许顺带删人**。
//!
//! 现场（2026-11-04）：一次「保存 LLM 配置」把池从两家换成一家，kimi k3 随之
//! 消失。事后问「谁删的」，平台的审计表是空的——`audit_logs` 一行没有；能反推
//! 的只有 Secret 的 `managedFields` 与网关指标序列，而前者记的是写入通道的名字
//! （一次原始 HTTP patch 落成 `unknown`），不是人。
//!
//! 根因不是"谁点了删除"，是那次保存本身：页面只认识"这一家"，服务端却按整池
//! 覆盖，于是保存一次 = 把池重写成页面这一份。属主的结论是：**除非运营者在
//! 页面上明确说删除，否则一律是新增或更新**。
//!
//! 判据（每一条都是没有这次改动就会红）：
//!   1. 保存成功后追加一行审计，动作名是 `llm.upstream_saved`；
//!   2. 这一行带着改前、改后、以及被摘掉的名字——只记「改后是什么」的账回答
//!      不了「谁删的」；
//!   3. 改前的那次读取在写入之前：写在后面读到的就是自己刚写的那一份，账上会
//!      记成一次什么都没变的保存；
//!   4. 保存那一支调的是"并入"（`keep_unnamed = true`、点名删除为空）；
//!   5. 删除是另一个动作、另一条路：只有它点名成员，并且写自己的动作名；
//!   6. 名字里只有 `base_url|model`，与网关指标同形，凭证不进账。

use std::path::PathBuf;

fn admin_source() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/llm_admin.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读不到 {}: {e}", path.display()))
}

/// 一个处理函数的函数体：从签名到它自己那一列顶格右花括号。
fn handler_body<'a>(source: &'a str, signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("找不到 {signature}：这条判据失去锚"));
    let rest = &source[start..];
    let end = rest
        .find("\n}\n")
        .expect("处理函数没有顶格的右花括号：函数体读不全");
    &rest[..end]
}

const SAVE: &str = "pub async fn llm_config_handler(";
const REMOVE: &str = "pub async fn remove_upstreams_handler(";

#[test]
fn a_saved_pool_leaves_a_row_saying_who_changed_what() {
    let source = admin_source();
    let body = handler_body(&source, SAVE);

    assert!(
        body.contains("llm.upstream_saved"),
        "保存池之后没有写审计行：下一次再问「谁改的池」仍然只能去翻 Secret 的 managedFields"
    );
    assert!(
        body.contains("AuditKind::Custom(\"llm_config\""),
        "审计行的类别不是 llm_config：读账的人要按类目找得到这一次写入"
    );
    assert!(
        body.contains("pool_change_detail("),
        "审计行没有带改前/改后/被摘掉的名字：只记改后的账回答不了「谁删的」"
    );
}

#[test]
fn the_before_pool_is_read_before_the_write_replaces_it() {
    let source = admin_source();
    let body = handler_body(&source, SAVE);

    let read = body
        .find("forwarded_llm_status(")
        .expect("没有读改前的池：账上写不出这一次摘掉了谁");
    let write = body
        .find("crate::secret_write::forward_pool_edit(")
        .expect("找不到写入点：这条判据失去锚");
    assert!(
        read < write,
        "改前的池是在写入之后读的：读到的就是自己刚写的那一份，账上会记成一次什么都没变的保存"
    );
}

/// 保存只并入：没点名的成员留在池里。
///
/// 这一条盯的是那一行调用本身。`keep_unnamed` 掉成 `false` 或删除名单里混进
/// 一个名字，编译照样过、页面照样回 200，只是别人的上游没了。
#[test]
fn a_save_never_removes_a_member() {
    let source = admin_source();
    let body = handler_body(&source, SAVE);

    assert!(
        body.contains("forward_pool_edit(fields, true, &[], true)"),
        "保存那一支应当是「并入且不删任何一家」（keep_unnamed = true、删除名单为空）：\
         页面只认识这一家，它的保存动作不能顺带把别人删掉"
    );
}

/// 删除是另一条路：点名成员、写自己的动作名。
#[test]
fn a_removal_is_a_separate_action_that_names_its_members() {
    let source = admin_source();
    let body = handler_body(&source, REMOVE);

    assert!(
        body.contains("llm.upstream_removed"),
        "删除池成员没有自己的动作名：账上分不出「保存了一次」与「删掉了一家」"
    );
    assert!(
        body.contains("forward_pool_edit(json!({}), false, &names, true)"),
        "删除那一支应当只交名字、不交条目（keep_unnamed = false）：两个意图混在一次\
         写入里，账上解释不成一句话"
    );
}

#[test]
fn the_audit_row_carries_names_and_never_keys() {
    let source = admin_source();
    let name_fn = source
        .find("fn pool_member_name(")
        .expect("没有把池条目压成名字的函数：审计行要么没有名字，要么只能抄整条（含 key）");
    let body = &source[name_fn..];
    let end = body.find("\n}\n").expect("名字函数没有顶格的右花括号");
    let body = &body[..end];

    assert!(
        body.contains("\"base_url\"") && body.contains("\"model\""),
        "名字不是由 base_url 与 model 拼成的：与网关指标的 upstream 标签对不上"
    );
    assert!(
        !body.contains("api_key"),
        "名字里读了 api_key：这一行要进审计链，凭证不许进账"
    );
}
