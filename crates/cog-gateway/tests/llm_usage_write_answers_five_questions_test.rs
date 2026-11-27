//! 代理那条记账路的五条目标态，各自被**反问一次**，而不是被声明一次。
//!
//! 五条是：无损、永久、无界、唯一正本、第一时间。它们说的是写入路径的属性，与落在
//! 哪里无关——换掉落点只换位置。声明写在文件头里没有意义：读的人不会去核对，而
//! 一次「顺手加了个 TTL」或「顺手加了个 LIMIT」正是这类属性悄悄失效的方式。
//!
//! 这里读的是**生产那一半的源码**，逐条问一句；每一条都能单独红。
//! 「第一时间」那一条由 `cog-llm` 的观测客户端用例钉着（那一行在把流交给下游之前
//! 写下），这里只钉代理这一侧的形态：写入与「丢了要报」在同一处，答复在那之后。

const SOURCE: &str = include_str!("../src/security_gateway.rs");

/// 生产那一半：测试模块里的同名调用不算数（那里的字面量与这里是同一份文本）。
fn production() -> &'static str {
    // 切点是**测试模块本身**，不是第一个 `#[cfg(test)]`：那个文件里有几处只给测试用
    // 的辅助函数也带这个属性，按第一个切会把一大段生产代码划进「测试半边」，
    // 于是门在真正该看的地方什么都没看（2026-11-24 复查发现）。
    SOURCE
        .split("\n#[cfg(test)]\nmod tests {")
        .next()
        .expect("这个文件没有生产的一半")
}

/// 无损：写不进去要重试，丢了要记成读数与告警——「可能丢」不许藏在成功路径里。
#[test]
fn nothing_is_lost_silently() {
    let code = production();
    assert!(
        code.contains("persist_with_retry("),
        "记账没有重试：一次抖动就把这一笔花销丢出所有窗口，而窗口是限额与告警的依据"
    );
    assert!(
        code.contains("LLM_USAGE_LANDING_DROPPED_TOTAL"),
        "写不进去时没有把它记成读数：丢了多少只有日志知道，读的人不知道"
    );
}

/// 永久与无界：这两条问的是同一张表上有没有「顺手加的」删除与截断。
#[test]
fn the_usage_rows_are_neither_deleted_nor_capped() {
    let code = production();
    let upper = code.to_uppercase();
    for forbidden in ["DELETE FROM", "TRUNCATE TABLE"] {
        assert!(
            !upper.contains(forbidden),
            "代理这道门里出现了 `{forbidden}`：用量行是花销的正本，删掉一条就是所有读数一起偏一点"
        );
    }
    // 读面可以带 `LIMIT`，写面不行：这里只问「这一笔写进去时有没有被截」。
    assert!(
        !code.contains("ORDER BY 1 LIMIT"),
        "写路径里出现了带序的截断，读起来像是给这张表加了容量上限"
    );
}

/// 唯一正本：一行调用只有一个构造函数，两次构造就是两处会各自漂移的字段表。
#[test]
fn one_row_has_one_constructor() {
    let code = production();
    let sites = code.matches("llm_call_from(&").count();
    assert_eq!(
        sites, 1,
        "`llm_call_from` 的生产调用点出现了 {sites} 处：同一笔调用被两处各拼一份，\
         字段表迟早对不上"
    );
}

/// 第一时间：行与事件在答复离开之前落地，而且行在前、事件在后。
///
/// 判的是顺序本身：先落行与事件，再让答复走。反过来的写照出来是「答复发了、事实还
/// 在队列里」，进程这时死掉，这一笔就永远不进任何窗口。
#[test]
fn the_fact_is_written_before_the_answer_leaves() {
    let code = production();
    let write = code
        .find("persist_with_retry(")
        .expect("这一侧没有记账写入");
    // 同一支里紧接着写事件流：行是投影，事件是正本，两者一起到。
    let event = code
        .find("state.pool_obs.llm_call_events")
        .expect("这一侧没有写事件流那一支");
    assert!(
        write < event,
        "事件流那一支在落行之前：同一笔调用的两份事实会在时序上分家"
    );
    // 两者之间的那一小段里不许有提前返回：先落地，再谈答复。
    let between = &code[write..event];
    assert!(
        !between.contains("return Ok("),
        "落行之后、写事件之前就提前返回了：这一支的答复先于事件离开"
    );
}
