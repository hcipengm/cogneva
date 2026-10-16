//! Gate: who may run the self-discovery loop is decided by a role, not by the
//! change-executor flag — and the change-executor flag keeps its own reader.
//!
//! The failure this exists for, measured on the live cluster 2026-10-16 (three
//! restarts inside the retention window, the lines paired by timestamp): the
//! process holding this loop and the process whose engine is built in memory
//! were the same one. `ReflectionEngine initialized in in-memory mode` at
//! 09:55:15 is followed one second later by `autonomous discovery loop ... not
//! armed`, while `production mode` at 09:52:34 is followed by `not the
//! self-discovery owner`. So the loop was gated on `executor_enabled` — the
//! flag that answers "who executes changes" — while its actual requirement is
//! "who holds a discovery engine". The two landed on different processes, and
//! the process that had the engine was refused by the flag, while the process
//! the flag admitted could never satisfy the third input.
//!
//! The shape of the fix is the part worth pinning, because the obvious repair
//! collapses two questions back into one: the flag must keep gating the signal
//! watcher (that one submits change-execution intents, so `executor_enabled` is
//! exactly its owner), and the loop must get its single-writer guarantee from
//! the arbiter the other seven roles already use. Both directions are asserted
//! here, plus the negative control that would catch "so just delete the flag".

use std::path::{Path, PathBuf};

const PLUGIN: &str = "crates/cog-reflection/src/plugin.rs";

/// The loop's entry point, as it appears in the plugin's production source.
const LOOP_FN: &str = "fn spawn_discovery_loop(";

/// The role this loop contends for, and the mechanism it contends through.
const ROLE: &str = "SELF_DISCOVERY_ROLE";
const ROLE_HOLD: &str = "RoleHold::start(";
const ROLE_ASK: &str = "hold.may_act()";

/// The flag that answers a different question, and the local that carries it.
const EXECUTOR_FLAG: &str = "self_evolution.executor_enabled";
const FLAG_LOCAL: &str = "owns_self_discovery";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cog-reflection")
        .to_path_buf()
}

fn plugin_source() -> String {
    let path = workspace_root().join(PLUGIN);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Everything up to the first test-only item: occurrences inside test modules
/// are fixtures, not the plugin's production wiring.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// The body of the function whose signature starts with `signature`, cut at the
/// next line that closes a top-level item.
///
/// Positional on purpose: a substring search over the whole file would call a
/// name in a comment a wiring, and this file's comments name every symbol the
/// assertions look for.
fn fn_body(source: &str, signature: &str) -> String {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("{signature} not found in the plugin source"));
    let rest = &source[start..];
    let end = rest.find("\n}\n").map(|i| i + 2).unwrap_or(rest.len());
    rest[..end].to_owned()
}

#[test]
fn the_loop_no_longer_takes_its_ownership_from_the_change_executor_flag() {
    let source = plugin_source();
    let body = fn_body(production_source(&source), LOOP_FN);

    // 形参里不许再有那个布尔：留着它就等于留着「谁执行变更」这道门。
    let signature = body
        .lines()
        .take_while(|l| !l.starts_with(") {"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !signature.contains("owns_discovery"),
        "spawn_discovery_loop 的形参里还有 owns_discovery（或它的同义形参）：这条环的属主\n\
         判据又回到配置开关上了。形参是：\n{signature}"
    );
    assert!(
        !body.contains("not the self-discovery owner"),
        "spawn_discovery_loop 里还在用「不归我」早退：那句话把「谁执行变更」当成「谁挂这条环」，\n\
         实测这两件事落在两个进程上（app 侧有真引擎、进化侧有提交权）。"
    );
}

#[test]
fn the_signal_watcher_still_takes_its_ownership_from_the_change_executor_flag() {
    // 负对照。上面那条一旦被读成「把这个开关删掉」，这一格会红：signal watcher
    // 提交的是**变更执行**意图，它的属主本来就该是 executor_enabled。
    let source = plugin_source();
    let production = production_source(&source);

    assert!(
        production.contains(&format!("let {FLAG_LOCAL} = ctx.config().{EXECUTOR_FLAG};")),
        "{FLAG_LOCAL} 不再取自 {EXECUTOR_FLAG}：换属主时把这个开关本身改掉了，\
         而 signal watcher 那一格读的就是它。"
    );

    let watcher = production
        .find("signal watcher: this process is not the change executor")
        .expect("signal watcher 的早退分支不在了——它是这一格的读者，改判据时要一起看");
    let before = &production[..watcher];
    let guard = before
        .rfind(FLAG_LOCAL)
        .expect("signal watcher 的早退分支不再读那个开关");
    assert!(
        before.len() - guard < 400,
        "signal watcher 早退分支前面 {:?} 的最远一次出现离它有 {} 字节：那个早退很可能已经\n\
         改读别的东西了。",
        FLAG_LOCAL,
        before.len() - guard
    );
}

#[test]
fn the_loop_holds_a_role_and_asks_it_every_round() {
    let source = plugin_source();
    let production = production_source(&source);
    let body = fn_body(production, LOOP_FN);

    assert!(
        production.contains(ROLE_HOLD),
        "全仓没有 {ROLE_HOLD}：这条环的至多一个持有者没有任何东西保证。"
    );

    // 角色的**名字**要出现在领角色的那次调用里，不能是别的角色借来的。
    let hold_at = body
        .find(ROLE_HOLD)
        .expect("spawn_discovery_loop 里没有领角色");
    let call = &body[hold_at..];
    let close = call.find(')').expect("RoleHold::start 调用没有收尾");
    assert!(
        call[..close].contains(ROLE),
        "领的不是 {ROLE}：这条环与 cog-github 的 issue_discovery 各领一个，借用会让两条环\n\
         互相把对方挡在门外。"
    );

    // 每轮现问，不是领一次就当作一直持有：丢角色之后照跑才是这条环真正的坏法
    // （`explored` 是进程内集合，两个进程各跑一遍就是同一条组合问两遍）。
    assert!(
        body.contains(ROLE_ASK),
        "循环体里没有 {ROLE_ASK}：角色只领不问，丢了角色之后这一轮照跑。"
    );
}

#[test]
fn a_held_round_is_readable_and_its_stretch_has_an_end() {
    let source = plugin_source();
    let body = fn_body(production_source(&source), LOOP_FN);

    for message in [
        "discovery round held off: the LLM pool gate is closed",
        "discovery round skipped: another process holds the self-discovery role",
    ] {
        let at = body
            .find(message)
            .unwrap_or_else(|| panic!("循环体里找不到这一句：{message}"));
        let before = &body[..at];
        // 取**最近**的那一个宏，不是「先试 info 再试 debug」：后者在 debug 更近时
        // 仍会挑中远处的 info，把一条真会红的判据变成恒真。
        let macro_name = ["info!(", "warn!(", "debug!("]
            .into_iter()
            .filter_map(|m| before.rfind(m).map(|i| (i, m)))
            .max()
            .map(|(_, m)| m)
            .expect("这一句不在任何 tracing 宏里");
        assert_eq!(
            macro_name, "info!(",
            "「{message}」印在 {macro_name} 上：这个平台的 Loki 不采集 debug（七天零行，\n\
             同窗口 info 几千行），写在 debug 上的跳过等于没有读数面。同族的 cog-github\n\
             那条环干同一件事用的是 info!。"
        );
    }

    // 仲裁者缺席时 may_act() 早退返回真且什么都不记（owner_lease.rs:201-204）⇒
    // 「没仲裁者」与「我持有」在读数上同形，所以持有那句必须把 broker 一起印。
    assert!(
        body.contains("broker = broker_present"),
        "持有/跳过那句没有印 broker 在不在：部署没发布仲裁者时 may_act() 恒真且不留痕，\n\
         读者会把「没人仲裁」读成「我持有」。"
    );

    // 状态类读数必须由结束那一轮写回 0，否则「连续被扣住到某轮为止」永远读不到尾。
    let resumed = body
        .find("discovery round resumed after being held off")
        .expect("没有「恢复」那一句：池恢复之后被扣住的计数不再出现，读者手上只剩最后一个非零值");
    assert!(
        body[resumed..].contains("held_rounds = 0"),
        "「恢复」那句没有把 held_rounds 写回 0。"
    );
}
