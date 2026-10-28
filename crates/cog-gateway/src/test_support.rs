//! 单测共用的样例：一份带着 workspace 范围的调用方身份。
//!
//! 它不是任何一处的题目，是每个「谁能看见什么」的单测都要先摆好的东西。原先
//! projects / tasks / memory / wiki 四处各写了一遍同一个 `Claims` 构造，两个就叫
//! `claims_with`、两个叫 `claims_with_workspaces`——字段一多（比如这里后来加的
//! `user_type`），改到哪一份、漏了哪一份，读起来全靠记性，而每一份都还是绿的。

/// 一份「测试用」的调用方身份，带 `workspaces` 声明的范围。
///
/// 空切片就是一份**没有声明范围**的身份：那不是「范围为空」，是「不按范围限制」，
/// 两者在视图里是相反的结果，所以调用点写 `&[]` 时读得出自己在测哪一半。
pub(crate) fn claims(workspaces: &[&str]) -> cog_core::Claims {
    cog_core::Claims {
        sub: "u1".into(),
        iss: "test".into(),
        aud: "test".into(),
        exp: 0,
        iat: 0,
        jti: "j".into(),
        preferred_username: "tester".into(),
        user_type: cog_core::UserType::Standard,
        workspace_ids: workspaces.iter().map(|w| w.to_string()).collect(),
        permissions: Vec::new(),
        roles: Vec::new(),
    }
}
