//! principle: feedback_a_roster_entry_is_removed_by_identity_and_a_dead_one_is_never_handed_back
//!
//! 名字是可以换主人的——按名字从名册里摘，会把并发创建刚挂上的新对象一起摘掉。
//! 退役的空壳也不是「这个名字还活着」：它收得下消息、答不出一条，把它交回去与
//! 找不到它，在调用方那里是两件不同的事。
//!
//! 这条规则落在 `crates/cog-agent/src/runtime/manager.rs` 上，两半：
//!
//! 1. 收尾按**对象**摘。`finish_retirement` 用 `Arc::ptr_eq(&w.agent, agent)` 认的
//!    是它刚退掉的那一个，全程不读 `w.agent_id`：名字在收尾那一刻已经不是索引，
//!    并发创建可以在这个名字下挂上一个新的活对象，按名字摘会把它一起摘掉。被摘掉
//!    之后没有一条断言会红——退役这一次照样成功，收尾这一句照样执行，名册只是短了
//!    一格。两条退役入口（`retire_agent`、`retire_task_agents`）都必须走这一个收尾，
//!    否则同一条规则在两条路径上有两个答案。
//! 2. 交还按**活没活**判。`get_agent`、`list_workers`、`create_agent` 三处都先问
//!    `is_retired()`。唯一按名字摘的那一句在 `create_agent` 里，它摘的是上一次生命
//!    留下的空壳，所以只能排在「退役了没有」之后——排到前面，摘掉的就是并发创建
//!    刚挂上的活对象，而「同一个名字只对应一个活的对象」这条幂等也就反了过来。
//!
//! 为什么这里判的是文字，而不是再跑一遍行为：`agent_retirement_test.rs` 里
//! `a_late_retirement_cleanup_does_not_take_the_new_object_with_it` 量的正是同一件事，
//! 但它只分得出其中一种坏法。收尾时把名字从待退目标上带下来、再按名字摘，名册被清空，
//! 那条用例变红；而收尾时才去名册里**查**名字的写法，查不到已经隐身的那一个、也就不摘
//! 任何东西，行为上与按对象摘同形。那是失效而不是等价，可它留不下任何行为痕迹。所以
//! 这一面的判据长在文字上，与那条行为用例各管一半，谁也不替谁。
//!
//! 天花板：这一面解析源码，认的是 `fn <名字>(` 起头的函数体。改名、搬进宏或
//! `include!`、或者把收尾折回调用点，这里就找不到函数体——找不到时它 panic，而不是
//! 在空集上判「干净」。

use crate::common;

use common::routes::read_repo;
use common::rust_production::production_text;
use common::rust_text::rust_function_body;

/// 这套名册的缝：`GlobalAgentManager` 的创建、查找、列表与收尾都在这里。
const MANAGER: &str = "crates/cog-agent/src/runtime/manager.rs";

/// 这个文件的生产半边，注释行已折掉。
///
/// 读注释里写着 `w.agent_id` 的说法，会把「文档里提过这个名字」当成「代码用了这个名字」，
/// 而这条判据问的正是代码用没用它。
fn production() -> String {
    production_text(&read_repo(MANAGER))
}

/// 一个函数的生产半边函数体。
///
/// 读不到就 panic：函数搬走之后的「判据什么都没读到」与「树上真的干净」在判词上同形，
/// 而前者的绿是假的。
fn body(name: &str) -> String {
    rust_function_body(&production(), name).unwrap_or_else(|| {
        panic!(
            "{MANAGER} 里找不到 `fn {name}(`：这条规则的落点被搬走了，\
             判据在两堵墙之间读了个空"
        )
    })
}

/// 收尾摘的是它刚退掉的那一个对象，不是这个名字。
#[test]
fn the_cleanup_keys_on_the_object_it_retired_not_on_its_name() {
    let cleanup = body("finish_retirement");
    assert!(
        cleanup.contains("retain("),
        "收尾里没有 `retain(`：退役对象不被摘，名册越停越长，而管理器对每一个都持有强引用——\
         连接与注册表条目一条都不会自己断掉"
    );
    assert!(
        cleanup.contains("Arc::ptr_eq(&w.agent"),
        "收尾不是按对象摘的：名册里能被认出来的只有名字，而名字是可以换主人的"
    );
    assert!(
        !cleanup.contains("w.agent_id"),
        "收尾按名字摘了：并发创建刚在同一个名字下挂上的新对象会跟着一起被摘掉，\
         而退役这一次照样「成功」、名册照样变短，没有任何东西会红"
    );

    for entry in ["retire_agent", "retire_task_agents"] {
        assert!(
            body(entry).contains("finish_retirement("),
            "`{entry}` 没有走那个收尾：这条退役路径上的空壳留在名册里，另一条清得掉——\
             同一条规则在两条路上有两个答案，而两条路都算「退役成功」"
        );
    }
}

/// 名册的每一条出口都先问「这一个还活着吗」。
#[test]
fn no_roster_read_hands_a_retired_entry_back() {
    for reader in ["get_agent", "list_workers"] {
        assert!(
            body(reader).contains("is_retired()"),
            "`{reader}` 不过问对象退役了没有：它把上一次生命留下的空壳交回去，\
             调用方拿到的消费者收得下消息、答不出一条，而「找到它」与「没找到、另建一个」\
             是两条完全不同的路"
        );
    }

    let create = body("create_agent");
    assert!(
        create.contains("is_retired()"),
        "`create_agent` 不看名下那一个退役了没有：「同名就是同一个对象」于是把空壳当成\
         活着的那一个交回去，重跑接上的是一个再也不会回话的消费者"
    );
    let liveness = create
        .find("is_retired()")
        .expect("这一句上一条断言刚认过，找不到只是读法退化了");
    let replacement = create.find("w.agent_id !=").expect(
        "`create_agent` 里按名字摘掉空壳的那一句不见了：名字被上一次生命占着，\
         而这一次创建永远接不上去",
    );
    assert!(
        liveness < replacement,
        "按名字摘的那一句跑在「退役了没有」之前：摘掉的是并发创建刚挂上的活对象，\
         而不是上一次生命留下的空壳——名字换主人的那一刻，被赶走的是新来的那一个"
    );
}
