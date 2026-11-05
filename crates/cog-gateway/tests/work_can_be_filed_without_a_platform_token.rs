//! 提交口只有一道门：一个手里没有平台令牌的调用方，也走同一条流程。
//!
//! 现场：宿主上的会话与集群内的业务 Pod 都是零凭证的（红线），而它们都是这个平
//! 台的合法参与者——属主的一句话要在它们那里变成任务行。运营面上的
//! `POST /api/v1/tasks` 要 Admin 令牌，所以能写任务的只有浏览器与持有签发权限的
//! 人；其余调用方要么写不进去，要么只能自己去拼一行塞进任务图。
//!
//! 自己拼行的那条路看起来也「能用」：行写进去了，队列里看得见。但它绕开的正是
//! 提交口里那几条判据——`face_id` 与 `unit_id` 必须与面主一致、输入不许携带凭据
//! 形状的键、配额评估、以及最后经 `submit_goal_auto` 的分解注入。绕过之后，任务
//! 图里出现的是没有目标行、没有依赖编排、也没有任何一次归属校验的行，而「它进
//! 去了」这件事本身看起来是成功的。
//!
//! 所以这条面挂的是同一个处理函数，一个字的第二实现都没有。判据（每条都是没有
//! 这次改动就会红）：
//!   1. 零凭证的那一面挂着提交路径，而且挂的就是运营面那个处理函数；
//!   2. 那一面上没有鉴权层——挂上去再套 auth，等于这道门只对本来就进得来的人开；
//!   3. 这个处理函数在全文件里恰好挂两次：运营面一次、零凭证面一次。少一次是
//!      有一条腿没有入口，多一次是有人又造了一条写入路径。

use std::path::PathBuf;

fn lib_source() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读不到 {}: {e}", path.display()))
}

/// `create_internal_router` 的函数体。它的结束靠顶格的那个右花括号找，所以函数
/// 体里不能再出现顶格的 `}`。
fn internal_router_body(source: &str) -> &str {
    let start = source
        .find("pub fn create_internal_router")
        .expect("零凭证的那一面：create_internal_router");
    let end = source[start..]
        .find("\n}\n")
        .map(|offset| start + offset)
        .expect("create_internal_router 的函数体结束");
    &source[start..end]
}

#[test]
fn a_caller_without_a_token_files_work_through_the_same_handler() {
    let source = lib_source();
    let internal = internal_router_body(&source);

    assert!(
        internal.contains("\"/api/v1/tasks\""),
        "零凭证的那一面没有提交路径：宿主会话与集群内智能体就此没有了派活的入口，\
         而它们的队列会一直读作空的"
    );
    assert!(
        internal.contains("tasks::create_task_handler"),
        "那一面挂的不是运营面那个处理函数：归属校验、凭据形状拒收与配额评估都只\
         在另一扇门后面成立"
    );
}

#[test]
fn the_credential_free_face_stays_credential_free() {
    let source = lib_source();
    let internal = internal_router_body(&source);

    for layer in ["auth_layer", "require_role", "require_admin"] {
        assert!(
            !internal.contains(layer),
            "零凭证那一面加上了 {layer}：这条门本来是为手里没有令牌的调用方开的，\
             套上鉴权之后它只对本来就进得来的人开放"
        );
    }
}

#[test]
fn the_submission_handler_is_mounted_on_exactly_two_doors() {
    let source = lib_source();
    let mounts = source.matches("tasks::create_task_handler").count();
    assert_eq!(
        mounts, 2,
        "提交函数挂了 {mounts} 次：一次是运营面，一次是零凭证面。少一次说明有一条腿\
         没有入口，多一次说明又有人造了一条写入路径"
    );
}

/// 读账与派活挂在同一面上。
///
/// 只有提交口的那一面上，一个智能体唯一能数清的是自己派过的那几件；别人派的那
/// 几千件它看不见——2026-11-06 的现场正是这样：三个智能体分别被问到「账上现在有
/// 多少活」，三个人的回答都是「这个进程里没有读任务账的工具」，而当时开着 1684
/// 条 pending 与 762 条 scheduled。能派活、看不见账，规划就只剩猜。
///
/// 挂在同一面而不是运营面的理由与提交口逐字相同：跑在集群里的业务 Pod 本身零凭
/// 证，给它们看账再要一个令牌，等于为一个只读的读数面给每个容器发一个身份。
#[test]
fn the_ledger_read_is_on_the_same_credential_free_face() {
    let source = lib_source();
    let internal = internal_router_body(&source);

    assert!(
        internal.contains("/api/v1/tasks/ledger"),
        "零凭证的那一面没有读账路径：能派活的智能体看不见账，也就没有依据决定下一\
         件事做什么，而它看得见的那张 `/api/v1/tasks/list` 一次吐出整张表"
    );
    assert!(
        internal.contains("task_ledger::task_ledger_handler"),
        "那一面挂的不是这条读数自己的处理函数：换个处理函数挂上去，读出来的是别的\
         东西，而路由表上看不出区别"
    );
}

/// 工具拼出来的地址与那一面挂的路径是同一串。
///
/// 两处各写一遍时改一处另一处不红：工具会去问一个没人挂的地址，而它以一个 404
/// 回来——读起来像「账是空的」而不是「你问错了地方」。这正是这个文件开头那件事
/// 的读法：两份答案里短的那份看起来也完整。
#[test]
fn the_tool_and_the_door_name_the_same_path() {
    const PATH: &str = "/api/v1/tasks/ledger";
    assert!(
        internal_router_body(&lib_source()).contains(PATH),
        "零凭证面上挂的路径与工具问的路径不是同一串：{PATH}"
    );

    let tools_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../cog-agent/src/tools.rs");
    let tools = std::fs::read_to_string(&tools_path)
        .unwrap_or_else(|e| panic!("读不到 {}: {e}", tools_path.display()));
    let mentions = tools.matches(PATH).count();
    assert_eq!(
        mentions, 1,
        "工具层里写了 {mentions} 次 {PATH}：应当只有一处拼这个地址，两处就是两份答案"
    );
}
