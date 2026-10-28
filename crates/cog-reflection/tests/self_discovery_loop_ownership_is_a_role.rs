//! principle: feedback_a_role_renewed_through_the_shutdown_of_its_work_is_a_seat_nobody_can_take
//! principle: feedback_a_verdict_grounded_in_a_state_of_the_code_goes_stale_wordlessly
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

/// 仓库根：一份算法，全仓共用（本文件按路径把那一份编进来）。
#[path = "../../cogneva/tests/common/root.rs"]
mod root;

use root::repo_root;

/// The production cut is the same one every other gate uses. Writing it out again
/// here is a second answer: this copy stops at the first `#[cfg(test)]` anywhere,
/// so one test-only item in the middle of a file silently drops every production
/// line after it -- and "nothing there" then reads like "nothing wrong".
#[path = "../../cogneva/tests/common/rust_production.rs"]
mod rust_production;

use rust_production::production_text_keeping_comments;

fn plugin_source() -> String {
    let path = repo_root().join(PLUGIN);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
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
    let body = fn_body(&production_text_keeping_comments(&source), LOOP_FN);

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
    let production = production_text_keeping_comments(&source);

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
    let production = production_text_keeping_comments(&source);
    let body = fn_body(&production, LOOP_FN);

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
    let body = fn_body(&production_text_keeping_comments(&source), LOOP_FN);

    for message in [
        "discovery round held off: the LLM pool gate is closed",
        "discovery round skipped: another process holds the self-discovery role",
        "discovery role released: the LLM pool gate is closed",
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

/// 池闸关着的那几轮，这条环**让出**角色——这是带外写者能顶上来的前提。
///
/// 老写法是「进循环之前领一次，之后一直攥着」：池不可用时这条环已经不动手了，
/// 但它照样续租，于是租约面上永远是「有人持着」。带外写者（宿主侧 claude code）
/// 顶上来靠的就是每轮读那个租约——读到的永远是「有人持着」，它就永远拿不到，
/// 而池不可用恰恰是**唯一**该它顶上的时候。
///
/// 位置关系而不是措辞，因为措辞会随改写漂移而这条要求不会：
///   1. 池闸那一问走在角色那一问**之前**——闸关着就连问都不问、更不续租；
///   2. 领角色发生在闸那一段**之后**（闸前领＝池关着也攥着，老写法）；
///   3. 闸那一段里把角色取走（`held.take()`）并把 owner 读数写成**非持有**
///      ——不写回的话 `cogneva_loop_owner_held` 上一次那个 1 会一直留着，
///      这条 gauge 会一直说「我持着」，直到池恢复（可能在几小时之后）。
#[test]
fn a_closed_pool_gate_releases_the_role_instead_of_holding_through_it() {
    let source = plugin_source();
    let body = fn_body(&production_text_keeping_comments(&source), LOOP_FN);

    let gate = body
        .find("pool_gate.llm_paused()")
        .expect("循环体里没有池闸那一问");
    let release = body
        .find("held.take()")
        .expect("闸那一段没有把角色取走：池关着时这条环照旧攥着租约，带外永远拿不到");
    let role_take = body
        .find(ROLE_HOLD)
        .expect("全仓没有 RoleHold::start：这条环的至多一个持有者没有任何东西保证");
    let role_ask = body.find(ROLE_ASK).expect("循环体里没有 hold.may_act()");

    assert!(
        gate < release && release < role_ask,
        "位置不对（池闸第 {gate} 字节、松手第 {release}、角色那一问第 {role_ask}）：\
         要的是「闸关着 → 松手 → 才轮到角色那一问」。"
    );
    assert!(
        gate < role_take,
        "领角色在池闸之前（闸第 {gate}、领角色第 {role_take} 字节）：这是「进循环前领一次、\
         之后一直攥着」的老写法，池关着时租约面上仍是「有人持着」。"
    );

    // 闸那一段（到它自己的 `continue` 为止）必须把 owner 读数写成**自己站开**。
    let branch = &body[gate..];
    let cont = branch
        .find("continue;")
        .expect("池闸那一段没有收尾的 continue");
    assert!(
        branch[..cont].contains("Ownership::Yielded"),
        "闸那一段没有把 owner 读数写成 `Yielded`：`cogneva_loop_owner_held` 会停在上一轮的 1 上\
         （这条 gauge 于是整段池不可用期间都说「我持着」），而写成 `Elsewhere` 则会让\
         `cogneva_loop_role_declared` 仍是 1 ⇒ 「已声明但没人持」那条 critical 规则在池关了\
         15 分钟之后报一条假红——那正是这条环按设计把角色让出去的时候。"
    );
}

/// 被扣住的那一轮**照样 beat**——这是「这一支被扣住」能从别处读数推出来的前提。
///
/// 判据写成**位置关系**而不是**当时的状态**：曾经的依据是「扣住时 beat 缺席」，
/// 而那条依据只对本函数当时的写法成立，函数一改，依据就作废而判词看着没变。
/// 「beat 在两道闸之前」是能用文本位置机检的形状，所以钉这个。
#[test]
fn the_rounds_beat_is_not_behind_either_gate() {
    let source = plugin_source();
    let body = fn_body(&production_text_keeping_comments(&source), LOOP_FN);

    let loop_at = body
        .find("loop {")
        .expect("spawn_discovery_loop 里没有循环");
    let beat = body.find("beat.beat()").expect("循环体里没有 beat.beat()");
    let next = body
        .find("interval.tick().await")
        .expect("循环里没有节拍等待");
    let hold = body.find(ROLE_ASK).expect("循环体里没有 hold.may_act()");
    let gate = body
        .find("pool_gate.llm_paused()")
        .expect("循环体里没有池闸那一问");

    // 一条链，不是两条断言：四个位置构成同一个形状（这一拍每轮走一次、且走在两道闸
    // 之前）。拆成两条时其中一条永远轮不到红——消融把 beat 挪到闸后，先断的那条
    // 是「在不在循环开头」，第二条就成了没被证过的半条。
    assert!(
        loop_at < beat && beat < next && next < hold && next < gate,
        "这一拍的位置不对（循环体起点第 {loop_at} 字节、beat 第 {beat}、节拍等待第 {next}、\n\
         may_act 第 {hold}、池闸第 {gate}）：要的是「每轮一拍，且走在两道闸之前」。\n\
         走到闸之后就变成——被扣住的那一轮不再 beat，于是「这条环被闸扣住」从别处的\n\
         读数推不出来：池级 gauge 只说闸关着，说不了这条环在不在跑，跳过那句就得\n\
         是唯一读数面。"
    );

    // 「位置在前」不等于「每轮都走」。把这一拍包进分支——`if held_rounds == 0 { beat.beat(); }`
    // ——时上面那四个偏移一个都没动、那条链照样绿，而这条测试真正要防的那件事已经破了：
    // 被扣住的那一轮不再 beat。（2026-10-16 只读复核实测；变异只跑在源码的文本复现上。）
    // 所以再钉一条**各自能被单独证伪**的：循环开头到这一拍之间没有条件词，这一拍无条件走。
    let between = &body[loop_at..beat];
    let guard = between
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .find(|t| ["if", "match", "while", "for"].contains(t));
    assert!(
        guard.is_none(),
        "循环开头到 beat.beat() 之间出现了条件词 `{}`：这一拍被包进了分支，被扣住的那一轮\n\
         就不再 beat，「这条环被闸扣住」于是从别处读数推不出来。这一拍要无条件走。",
        guard.unwrap_or("")
    );
}
