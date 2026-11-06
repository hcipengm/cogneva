//! Which ready queue a task belongs on.
//!
//! Almost every task hands its result back over the results stream, so any
//! worker in any deployment can run it: the ready queue is a competing-consumer
//! queue and the DAG state that arbitration needs lives in the shared store.
//!
//! Self-evolution change generation is the exception. It does not put the
//! generated change on the wire — it hands it to the in-process `ChangeSink`
//! fan-out, which writes the `.diff` into *this* process's
//! `self_evolution.change_dir`, and the cycle that scans that directory
//! (`pending_changes`) runs in the same process. A generation task executed by
//! any other worker therefore yields an artifact no reader exists for: the
//! writer writes one directory, the scanner reads another, and the change is
//! lost without anything failing.
//!
//! So placement is decided by who can read the artifact, not by task type
//! alone. [`ready_stream`] carries the tasks whose output travels over the bus;
//! [`process_local_ready_stream`] carries the ones whose output only their own
//! producer can read, and is read only by the deployment that owns the
//! executor role (`self_evolution.executor_enabled`). A queue with no reader is
//! the intended failure mode there: the task waits for the executor to come
//! back instead of being executed where nothing can consume what it produced.

use cog_core::Task;

/// The ready queue every worker deployment reads.
pub fn ready_stream(workspace_id: &str) -> String {
    format!("orchestrator:ready:{workspace_id}")
}

/// The ready queue for tasks whose artifact is only readable by the process
/// that produced it.
pub fn process_local_ready_stream(workspace_id: &str) -> String {
    format!("orchestrator:ready:{workspace_id}:local")
}

/// The ready queue for work that must run before the ordinary queue drains.
///
/// A stream is a first-in-first-out log and a task's place in it is fixed the
/// moment its message is appended: no priority in the graph can overtake a
/// message that is already ahead of it, and the ordinary queue can be
/// arbitrarily deep. Priority therefore has to be expressed as a queue and not
/// as a number: this stream is drained by a consumer whose budget is its own,
/// so a stop-the-line task runs while the ordinary queue is still full instead
/// of waiting for it to empty.
///
/// Read only by the deployment that owns the executor role, for the same reason
/// as [`process_local_ready_stream`]: the artifact of a change-generation task
/// is written into that process's change directory.
pub fn stop_the_line_ready_stream(workspace_id: &str) -> String {
    format!("orchestrator:ready:{workspace_id}:stop-the-line")
}

/// Whether this task names the unit that must run it.
///
/// A task with an owner is not offered to the shared queue. The queue is a
/// competing-consumer stream: the first worker to read an entry runs it, so
/// publishing an owned task there hands it to whoever is idle rather than to
/// the unit that was named, and the owner only finds out afterwards. It stays
/// `Pending` until its owner asks for it, which is the same state a task
/// waiting on a dependency sits in -- the ready scan offers it again on every
/// pass, so nothing has to remember that it was held.
///
/// The stop-the-line lane is the one exception, and it is named here rather
/// than in each caller because the three places that read ownership -- the
/// publisher, the consumer that finds an owned payload on a shared stream, and
/// the check that refuses a result from a unit other than the owner -- have to
/// answer the same way or the row is stranded in a new way at each. That lane
/// exists to run while everything else waits, and an owner is a routing
/// preference, not a lock: whichever deployment is free runs the row, and its
/// result is that deployment's to report. Measured 2026-11-05: the face
/// restaffing pass stamped the CI face's owner onto every open row of that
/// face, including the `-fix` row the deployer had just filed in the
/// stop-the-line lane; the publisher then held that row out of every queue for
/// ten hours while the dedicated lane sat drained, and the rollout stayed
/// stopped on the revision it was filed for.
pub fn carries_an_owner(task: &Task) -> bool {
    task.owner_unit.is_some() && !is_stop_the_line(task)
}

/// Whether this row travels in the lane that runs ahead of the ordinary queue.
///
/// One definition because the publisher's routing, the consumer's refusal and
/// the ownership gate above all have to agree on it: a row that one of them
/// reads as ordinary and another as stop-the-line is either held out of the
/// lane it was filed for or handed to a queue that cannot pay for it.
pub fn is_stop_the_line(task: &Task) -> bool {
    task.input
        .get(cog_core::INPUT_READY_LANE)
        .and_then(|lane| lane.as_str())
        == Some(cog_core::READY_LANE_STOP_THE_LINE)
}

/// Whether a task is work the dispatcher may start as a run.
///
/// Two things are excluded. A face's standing duty row carries the pair (face,
/// who is answerable for it) and is what the drift reading counts as the owner
/// being on the face; it has no body to run. Started as a run it burns its whole
/// budget, is failed on the degenerate-loop rule, and takes the face's only
/// evidence of staffing with it -- the face then reads idle and nothing
/// recreates the row. It stays executable so the owner's own session can still
/// take it from its queue; only the dispatcher is told to leave it alone.
///
/// The other is a row that does not [`states_what_done_means`]. Held is the safe
/// half of the two: the row stays pending and stays on the board, so a kind that
/// files one is visible as work that never starts rather than as work that
/// starts and can never be closed.
///
/// Being held is not a repair, though: a row filed without a criterion stays
/// that way until someone gives it one, and the dispatcher was for a while the
/// only reader that noticed. So the held rows are named where they are counted,
/// and the door that files rows refuses an injected row that states none. A
/// submission path that accepted such a row -- and dropped the criterion its
/// caller had stated -- is how two live rows sat here for a day.
pub fn is_dispatchable_work(task: &Task) -> bool {
    task.is_executable && !is_face_duty(task) && states_what_done_means(task)
}

/// Whether the dispatcher is leaving this row alone for want of a criterion.
///
/// The complement of [`is_dispatchable_work`] over the rows that have a body to
/// run, kept as its own name because the two are read by different callers for
/// different reasons: the dispatcher asks the first before it starts a row, and
/// the graph reading asks the second to say how much work is stuck behind it.
/// Writing the second as `!states_what_done_means(task)` alone would count face
/// duties and non-executable rows, which are left alone for reasons of their own.
pub fn held_for_want_of_a_criterion(task: &Task) -> bool {
    task.is_executable && !is_face_duty(task) && !states_what_done_means(task)
}

/// Whether the row says what would close it.
///
/// Two callers ask this: the dispatcher, before it starts a row, and the graph
/// reading that reports how many rows are held for want of it. The settle passes
/// ask the same question of a row they re-read -- as `acceptance.is_empty()` --
/// but the question is not quite this one: they correct a row rather than
/// dispatch it, so the face-duty half does not apply to them. A work package
/// nobody can close is not work -- it is a row that will complete on the summary
/// the executor writes about itself.
pub fn states_what_done_means(task: &Task) -> bool {
    !task.acceptance.is_empty()
}

/// Whether a row is a face's standing duty marker.
pub fn is_face_duty(task: &Task) -> bool {
    matches!(&task.task_type, cog_core::TaskType::Custom(kind)
        if kind == crate::governance_acts::FACE_DUTY_TASK_TYPE)
}

/// The queue a ready task is published to. One definition, because the
/// publisher and the consumer must answer the same question the same way.
pub fn ready_stream_for(task: &Task, workspace_id: &str) -> String {
    if is_stop_the_line(task) {
        stop_the_line_ready_stream(workspace_id)
    } else if task.is_self_evolution() {
        process_local_ready_stream(workspace_id)
    } else {
        ready_stream(workspace_id)
    }
}

/// The consumer group every deployment joins on a ready stream.
///
/// One per workspace, not per pod: the group is what survives a pod restart and
/// what lets a new pod resume from the last acknowledged entry instead of
/// re-reading the queue. Both the consumer that joins it and any reader that
/// asks the transport about its backlog have to name the same group, so the
/// name is derived here rather than written out at each call site.
pub fn ready_group(workspace_id: &str) -> String {
    format!("executor-loop-{workspace_id}")
}

/// How many more jobs the pool's window can pay for, before one is put on a
/// queue.
///
/// `None` is "no bound is derivable", and it is never zero: a missing
/// measurement read as a zero bound would stop dispatch for the whole length of
/// a rollout, which is a driver traded for a reading. Three cases return it --
/// nothing measured (`measured_upstreams == 0`), no job cost to divide by
/// (`reserve_tokens == 0`), and a measurement that covers fewer upstreams than
/// the pool has. The last one is the guard that keeps this from repeating the
/// refusal's old mistake: allowance and spend are summed over the upstreams
/// that reported a window, so a pool where only one member reports would be
/// sized by that member's ruler and the rest of the pool judged by it. A subset
/// says nothing about the pool, and `pool_size == 0` is "not reported" rather
/// than an empty pool.
///
/// `reserve_tokens` is one whole job, measured over finished jobs, so this is a
/// count of jobs and not an average call -- the gateway's own `blocked` test
/// compares the same two numbers, which makes one admitted job exactly one job
/// the gateway would not refuse on these same readings.
///
/// Spend past the allowance (a snapshot read after the window rolled) yields
/// zero rather than a wrapped-around huge number: saturated subtraction.
pub fn jobs_the_window_can_pay_for(
    pace: &cog_core::LlmWindowPace,
    pool_size: usize,
) -> Option<u64> {
    if pace.measured_upstreams == 0 || pace.reserve_tokens == 0 {
        return None;
    }
    if pool_size > 0 && pace.measured_upstreams < pool_size {
        return None;
    }
    Some(pace.allowance_tokens.saturating_sub(pace.spent_tokens) / pace.reserve_tokens)
}

/// How many pool members a pace measurement has to cover before it may bound
/// the queue.
///
/// Members whose last refusal was `entitlement` cannot come back by waiting:
/// the subscription, model, or account is not there, so the same refusal will
/// be there for as long as the configuration stands. Counting them makes the
/// measurement permanently partial -- two of the four upstreams in the pool
/// this was written against answer that way -- and a partial measurement is
/// deliberately `None`, which admits everything. That is the failure the bound
/// exists to prevent, reintroduced through the denominator.
///
/// A member that is merely out of quota still counts: its window rolls, so it
/// is part of what the pool will be able to pay for.
pub fn measurable_pool_size(status: &cog_core::LlmPoolStatus) -> usize {
    let permanently_out = status
        .upstream_evidence
        .iter()
        .filter(|entry| entry.refusal_kind == "entitlement")
        .count();
    status.pool_size.saturating_sub(permanently_out)
}

/// The pool's own bound, read off the whole status rather than the pace alone.
///
/// `unavailable` is a statement about the pool, not about one member: the
/// window's allowance keeps accruing while nothing serves, so a pool that is
/// down would otherwise admit work the moment the allowance crosses one job's
/// cost and hand the queue to a pool that cannot take it.
///
/// The degraded period has exactly these semantics, and they are what the
/// readings beside the count are for:
///
/// - **Zero admission, not a failure.** Every task that asks is held, whatever
///   the allowance pays for. Nothing is dropped, failed or retried into a
///   shorter deadline: a held task stays ready and asks again on the next pass,
///   so the cost of the outage is latency, not lost work.
/// - **The reason is the pool, not the budget.** [`hold_reason`] labels the
///   hold `pool_unavailable`; a reader that sees `window_exhausted` here would
///   be sent to tune an allowance that is not the constraint.
/// - **It has a clock.** `LlmPoolStatus::next_attempt_unix` is the earliest
///   instant the gateway will probe again, and it travels into the admission
///   reading as `retry_at_unix`. That is our retry cadence, not the upstream's
///   promise (`evidenced_recovery_unix` is that), so it is when the decision is
///   re-made, never when the window is claimed to reopen.
///
/// A status that was not read at all is `None` at the call site and admits
/// everything: an unanswered read happens during a rollout and is not an
/// outage.
pub fn jobs_the_window_can_pay_for_status(status: &cog_core::LlmPoolStatus) -> Option<u64> {
    if status.unavailable {
        return Some(0);
    }
    jobs_the_window_can_pay_for(&status.pace, measurable_pool_size(status))
}

/// The instant before which a ready-queue pass may be skipped outright.
///
/// A pool that declares itself unavailable names, in `next_attempt_unix`, the
/// earliest instant the gateway will probe it again. Until then nothing in a
/// pass can change: no reading it takes can flip the verdict, and no task it
/// finds is admissible. Running anyway is not free — it scans the whole ready
/// backlog and writes one refusal line per stream per cadence, so on a multi-hour
/// outage the log reads as a platform that never stops trying, and buries the one
/// instant that actually matters.
///
/// `None` runs the pass. A pool that is available, a snapshot that names no probe
/// instant (`0`), and an instant already in the past all run it: an unread clock
/// is not a closed window, the same way an unread pool is not an empty one.
pub fn publisher_backoff_until(
    unavailable: bool,
    retry_at_unix: i64,
    now_unix: i64,
) -> Option<i64> {
    (unavailable && retry_at_unix > now_unix).then_some(retry_at_unix)
}

/// Why a pass held work back, as the closed label the held counter carries.
///
/// The zeros arrive at the same place and mean different things: a spent window
/// is fixed by waiting, an unavailable pool is fixed by the upstreams coming
/// back, and a hold by the pass's own slice is fixed by nothing -- the next
/// pass takes the rest. Labelling any of them under another files an outage
/// under a budget and sends its reader to tune an allowance that is not the
/// constraint -- which is possible here precisely because the pool verdict
/// overrides an allowance that still pays for a job, and because a pass with no
/// reading at all has no window to blame.
pub fn hold_reason(pool_unavailable: bool, bound: PassBound) -> &'static str {
    match bound {
        PassBound::Ceiling => "pass_ceiling",
        PassBound::Window if pool_unavailable => "pool_unavailable",
        PassBound::Window => "window_exhausted",
    }
}

/// 计划预算这一道准入在读数里用的原因值。
///
/// 与 [`hold_reason`] 的三个值同一个 `reason` 轴：那一轴上的每个值都回答
/// 「哪个闸门说了零」。这道闸门的零不是池子给的，也不是这一拍的切片给的，
/// 是这个任务自己的计划基线给的——把它记进前两个里的任何一个，读它的人会去
/// 调池子的额度，而额度不是这里的约束。
pub const BUDGET_SPENT_REASON: &str = "budget_spent";

/// 队列还没被读走时，这一步准入在读数里用的原因值。
///
/// 与 [`hold_reason`] 的三个值和 [`BUDGET_SPENT_REASON`] 同一个 `reason` 轴：
/// 这道闸门的零既不是池子给的，也不是这一拍的切片给的，更不是任务自己的预算
/// 给的——是这条队列里已经躺着消费组还没拿走的条目。把它记进前三个里的任何
/// 一个，读它的人会去调额度/预算，而这两样都不是这里的约束。
///
/// 它必须和另外几个值分开记，因为它的代价形状不同：额度拦下的是「现在不能
/// 多花」，队列拦下的是「已经给出去还没人拿」。后者继续投只会把队列变长，
/// 而队列越长，每一条到达的时间越晚——这是实测出来的正反馈：2026-11-05
/// `default:local` 里 2,314 条任务占了 69,012 条消息（同一条最多 34 份），
/// 队列以约 86 条/分钟增长，而消费者同一时间在跑 2 条。
pub const QUEUE_STILL_UNREAD_REASON: &str = "queue_still_unread";

/// 门类变更在任务行上的身份：台账里由原则账（principle ledger）产出的那一路。
///
/// 认的是 `change_sources` 这个**结构字段**，不是目标文本里的某个词：同一批
/// 门类任务的句式会变（有的带标题、有的只有规则本身），而写进这个数组的来源
/// 是产出它们的那个 intake 自己的签名，改它等于改那一侧的合同。
pub const PRINCIPLE_LEDGER_SOURCE: &str = "principle_ledger";

/// 门类在账上那条身份的字面前缀。
///
/// 原则账写进账行的身份是 `principle: <规则名>`，而任务行上的
/// [`PRINCIPLE_LEDGER_SOURCE`] 是同一个 intake 写下的另一处签名。两处都要认，
/// 因为两个问题是在两个地方问的：任务行回答「这一条活是不是门类」，账行回答
/// 「这一类落下过多少」——后者没有任务行可读，门类的活跑完之后任务行已经不
/// 说明它落没落地。
pub const PRINCIPLE_LEDGER_PREFIX: &str = "principle:";

/// 这一类之间的先后在读数里用的原因值。
///
/// 与 [`hold_reason`] 的三个值、[`BUDGET_SPENT_REASON`]
/// 和 [`QUEUE_STILL_UNREAD_REASON`] 同一个 `reason` 轴：拦下这几条的既不是
/// 池子、也不是这一拍的切片、更不是任务自己的预算，而是**这一类在队列里的
/// 占比**。把它记进前几个里的任何一个，读它的人会去调额度，而额度不是这里的
/// 约束。它必须和 `queue_still_unread` 分开记：那个的修法是等消费者，这个的
/// 修法是让另一类先走。
pub const GATE_SHARE_REASON: &str = "gate_share";

/// This row is a change the principle ledger asked for.
///
/// 由 `change_sources` 判，理由见 [`PRINCIPLE_LEDGER_SOURCE`]：这是那个 intake
/// 写进任务行的签名，不是目标文本里的一句措辞。读不到这个字段的任务不是门类
/// 任务——一个没有数组的输入既不是「空来源」也不是「不属于任何一类」的证据，
/// 而把它算进门类会让每一个别的任务在读数里消失一个座位。
pub fn is_principle_ledger(task: &Task) -> bool {
    task.input
        .get("change_sources")
        .and_then(|sources| sources.as_array())
        .is_some_and(|sources| {
            sources
                .iter()
                .any(|source| source.as_str() == Some(PRINCIPLE_LEDGER_SOURCE))
        })
}

/// 门类在最近一次落地窗口里占了多少，量出来的那一对数。
///
/// 两个数一起走是因为占比的两个成分都要看得见：一条只有百分比的读数分不清
/// 「门类落地少」和「一共就没落下几笔」。`total` 是同一段时间里落到 `landed`
/// 的账行数，`gate` 是其中字面量以 `principle:` 开头的那几条。
///
/// 用账行的落地数，而不是任务的完成数，因为这两把尺子量的不是一件事：一条
/// 变更任务完成，等于它那一趟跑完了（可能是 retire、可能是被判定无需改动），
/// 而账行落到 `landed`，等于**它的检查在树上真的成立了**。队列里排的活值不
/// 值得让路，问的是后一件。2026-11-06 实测两者的回答差一个量级：
/// 同一个 24 小时里完成的门类变更任务占 81%，而落地的账行里门类只占 5%
/// （5/100；最近七天 50/630）。按完成数算这道闸门几乎不会生效，按落地数算
/// 它说的正是队列的形状：86% 的排队量（1,675/1,955）换来 5% 的落地。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GateShare {
    pub gate: u64,
    pub total: u64,
}

/// 少于这么多条落地样本时不给占比。
///
/// 这一道闸门判的是整整一类活的先后，几条样本算出来的占比是噪声；而噪声在
/// 这里不是中性值——它会把一类活按一个当天的偶然数排队。样本不够就是没有
/// 读数，没有读数不设界，与池子、预算两道闸门同一条规矩。
// cap: bound — 这是「噪声与读数的分界」，不是容量：样本下限只该由两类占比要多细才能分开来定
pub const MIN_GATE_SHARE_SAMPLES: u64 = 20;

impl GateShare {
    /// 千分比，样本不够时 `None`。
    ///
    /// 取整向下、后面按座位向上取整：先在这一步把「一点点」抹成零，再在座位
    /// 那一步把它抬成一个座位，等于让这道闸门在一类占比很小时也只是排得靠后
    /// 而不是不许走。
    pub fn permille(&self) -> Option<u64> {
        (self.total >= MIN_GATE_SHARE_SAMPLES).then(|| self.gate.saturating_mul(1000) / self.total)
    }
}

/// 这一拍最多让几条门类变更进队列。
///
/// 四条规矩，每一条都对应一种读错它的方式：
///
/// - **上限量出来的，不写死。** `share_permille` 是 [`GateShare::permille`] 的
///   回答，不是这里编的比例。写死一个 20% 的问题不在于它是多少，而在于它不
///   跟任何读数走：门类真的开始多落地时，写死的数会把座位继续扣着；它一落千
///   丈时，同一个数又放过它。`None`（样本不够、读不到账）一律不设界。
/// - **别人不要的座位门类可以全拿。** 这一刀判的是「谁先坐」，不是「谁不能
///   坐」：产品活少于座位时，让空位陪着门类一起等，是把省钱读成了停摆。
/// - **一个座位从来不降到零。** 占比小到取整为零时仍然放一条：门类每一拍都
///   在往前走一步，所以它是「排在后面」而不是「永久让路」——只会让给先问者
///   的仲裁会把永远被推迟的那一支饿死，而这一支是被判词拦下的活，不是摆设。
/// - **等过界插一次队。** `starving` 由调用方按「这一类多久没拿到过座位」判，
///   真到了那一步就多给一个，越过这道闸门一次。
pub fn gate_seats(
    jobs: u64,
    share_permille: Option<u64>,
    gate_ready: u64,
    other_ready: u64,
    starving: bool,
) -> u64 {
    let Some(permille) = share_permille else {
        return gate_ready;
    };
    // 这一拍一条都不许出时，门类也一条都不出：下面那个「至少一个座位」的
    // 地板说的是「别人占满时门类也要往前走一步」，不是「额度和队列都说了
    // 零时也要硬塞一条出去」——那一条会在截断那一步被丢掉，读数上却像是
    // 门类拿到了座位。
    if jobs == 0 {
        return 0;
    }
    let by_share = jobs.saturating_mul(permille).saturating_add(999) / 1000;
    let spare = jobs.saturating_sub(other_ready);
    let mut seats = by_share.max(spare);
    if starving {
        seats = seats.saturating_add(1);
    }
    if gate_ready > 0 {
        seats = seats.max(1);
    }
    seats.min(gate_ready)
}

/// 门类多久没拿到过座位就算「等过界」，下一拍插一次队。
///
/// 它量的是**这一类从何时起没被放行过**，不是一条任务从何时起在排队：
/// 2026-11-06 排队最久的那几条门类任务已经等了十几天，用它们的年龄当这一
/// 个判据，插队会从第一拍起就永远开着——那等于没有闸门。这个数由调用方
/// 拿一个进程内的时刻算，进程一滚就从此刻重新开始：那是「这个部署有多久
/// 没给过这一类座位」，重启之后没有人欠过任何一类。
// cap: bound — 这是插队的节流间隔，不是给某一类活的容量：调小会让插队变成常开，调大就是把等过界的类再饿半小时
pub const GATE_WAIT_BOUND_SECS: i64 = 30 * 60;

/// 把这一拍的批次排成「先别人、后门类」，并保证门类在 `jobs` 条里坐满 `seats` 个。
///
/// 门类的座位必须落在这一拍的额度**里面**：只顾着把它们排到后面，产品活一多，
/// 截断那一刀就会把门类整个切掉，座位形同虚设。所以别人的那一段只留
/// `jobs - seats` 条，门类坐在这一段之后——消费者按顺序读，于是产品活先走，
/// 门类跟在后面，而那几条**确实**在这一批里。
///
/// 超出门类座位、以及超出产品活那一段的条目都不进这一批：它们留在
/// `Pending`，下一拍再被扫到，和被额度拦下的活是同一种状态，也不需要任何人
/// 记住它们被拦过。
///
/// 两类各自保序：批里的顺序是就绪扫描给出的优先序（`priority` 降序），在类
/// 内部打乱它等于让同一类里等得最久的那条排到最后。
pub fn order_by_class<T>(
    entries: Vec<T>,
    is_gate: impl Fn(&T) -> bool,
    seats: u64,
    jobs: u64,
) -> Vec<T> {
    let mut gates = Vec::new();
    let mut others = Vec::new();
    for entry in entries {
        if is_gate(&entry) {
            gates.push(entry);
        } else {
            others.push(entry);
        }
    }
    gates.truncate(usize::try_from(seats).unwrap_or(usize::MAX));
    let room_for_others = jobs.saturating_sub(seats);
    others.truncate(usize::try_from(room_for_others).unwrap_or(usize::MAX));
    others.extend(gates);
    others
}

/// 这一拍怎么排：按类分先后，并回答门类拿到了几个座位、被拦下几条。
///
/// 座位数只说**这一刀分出去的**：没有读数时是零，哪怕这一批里躺着门类任务。
///
/// 把「数一数两类各有几条」「按占比算座位」「重排」这三步绑在一个函数里，是
/// 因为它们共用一个数——门口那几条既是被计数的集合，也是被重排的集合。分成
/// 三步由调用方串起来，调用方就有机会用这一批的条数配上一批的座位，而那两批
/// 之间只差一次就绪扫描。
pub fn class_the_batch<T>(
    entries: Vec<T>,
    is_gate: impl Fn(&T) -> bool,
    jobs: u64,
    share: Option<GateShare>,
    starving: bool,
) -> (Vec<T>, u64, u64) {
    let gate_ready = entries.iter().filter(|entry| is_gate(entry)).count() as u64;
    if gate_ready == 0 {
        return (entries, 0, 0);
    }
    // 没有读数时这一拍什么都不重排：把门类排到后面而手上没有它的占比，就等于
    // 让这两类按「谁先问」排队——那正是这道闸门要修的病，只是换了个方向。
    //
    // 报出去的座位是零，不是这一批里门类的条数：这一刀一个座位也没分，那几条
    // 排在哪里、能不能落进这一拍，是后面按优先序截断那一刀说了算。报成「全都
    // 坐下了」会让调用方误以为门类刚被放过一次，于是拿它去重置插队的时钟。
    let Some(permille) = share.and_then(|share| share.permille()) else {
        return (entries, 0, 0);
    };
    let other_ready = entries.len() as u64 - gate_ready;
    let seats = gate_seats(jobs, Some(permille), gate_ready, other_ready, starving);
    let held = gate_ready.saturating_sub(seats);
    (order_by_class(entries, is_gate, seats, jobs), seats, held)
}

/// 这条任务是否已经花掉了计划基线给它的 token。
///
/// 判据是「到线即停」，不是「明显超了再停」：预算是一个承诺过的数，多花一个
/// token 就是多花了；容忍度要有的话得有人先定出来，凭空写下的 10% 会让一条
/// 已经越界的任务继续跑，而越界这件事本身没有争议。
///
/// `budget == 0` 一律不拦。零预算是「这条基线没有给出 token 计划」，与
/// 「计划花零个 token」是两件事——后者会把每一次派发都拦下，而前者什么都不该拦。
pub fn past_its_budget(spent: u64, budget: u64) -> bool {
    budget > 0 && spent >= budget
}

/// How many of `ready` may go on the wire, given what the queue already owes.
///
/// `window_jobs` is the pool's own bound, or `None` when nothing measured one
/// -- and `None` admits everything. The subtraction is what keeps a pass from
/// re-admitting the same allowance while the previous batch is still queued;
/// saturating, because spend recorded after the window rolled can leave `owed`
/// above the allowance, and a wrapped-around count would admit the whole queue.
pub fn admitted(ready: u64, owed: u64, window_jobs: Option<u64>) -> u64 {
    match window_jobs {
        Some(window_jobs) => ready.min(window_jobs.saturating_sub(owed)),
        None => ready,
    }
}

/// Whether a project's board can take one more task, and which column says no.
///
/// A board column's limit is admission control, not a colour: when a project's
/// column is full, its ready work is not put on a queue at all -- the task stays
/// `Pending` and is offered again on the next pass, the same shape as a task
/// waiting on a dependency. "Full" means at or over, so a column configured at
/// four holds four and takes no fifth.
///
/// Publishing moves a task into `scheduled`, so `scheduled`'s limit bounds the
/// publish directly: the caller asks once per task, and the tasks already
/// admitted in the same pass count as occupancy, which is what keeps one pass
/// from filling a column past its limit. `running`'s limit is the upstream the
/// queue feeds -- pushing more work into a full `running` column does not make
/// it finish any faster, so it stops the publish rather than queueing behind it.
///
/// `occupancy` is called with a column name so the caller's snapshot is read
/// once per configured column, and an unconfigured board costs one `is_empty`.
pub fn column_at_limit(
    limits: &cog_core::BoardWipLimits,
    occupancy: impl Fn(&str) -> u64,
) -> Option<&'static str> {
    limits
        .iter()
        .find(|(column, limit)| occupancy(column) >= *limit as u64)
        .map(|(column, _)| column)
}

/// What one pass may put on a queue, and which gate produced that number.
///
/// `bound` is `window` when the pool's own allowance sized the batch and
/// `ceiling` when nothing measured the pool and the pass's slice did. The two
/// are different readings: a window that pays for nothing is repaired by
/// waiting, a ceiling is repaired by nothing at all -- the rest goes out on the
/// next pass -- and a reader told only "N held" would tune the first for the
/// second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassAdmission {
    pub jobs: u64,
    pub bound: PassBound,
}

/// Which gate sized one pass's batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassBound {
    /// The pool's window allowance.
    Window,
    /// The pass's own slice, because nothing measured the pool.
    Ceiling,
}

/// How many of `ready` one pass may put on the wire, when the pool may not
/// have measured anything.
///
/// [`admitted`] reads `None` as "admit everything", and for a single pass that
/// is what it means: the whole ready set goes out at once. That is affordable
/// only if the pool is there. The pass that reads no status is a rollout -- a
/// new process, before its first look at the pool -- which is also when the
/// transport is most likely to still be loading, and a batch that size is a
/// backlog handed to a stream whose writes may be refused one message at a
/// time, leaving the whole batch in `Scheduled` until the stall sweep re-arms
/// it.
///
/// So an unmeasured pass admits a slice instead. It does **not** admit nothing:
/// a publisher that stopped on a missing reading would be driving the platform
/// by the presence of a metric, and a pool source that is never attached would
/// then hold every task forever. The slice makes progress whenever the reading
/// is missing, at the ceiling's rate rather than the backlog's.
///
/// The owed count is deliberately not subtracted in this branch. `owed` is only
/// meaningful against a known allowance; subtracting it from a ceiling would
/// admit nothing as soon as the backlog exceeded one slice, which is the halt
/// this branch exists to avoid.
pub fn admission_for_pass(
    ready: u64,
    owed: u64,
    window_jobs: Option<u64>,
    ceiling: u64,
) -> PassAdmission {
    match window_jobs {
        Some(_) => PassAdmission {
            jobs: admitted(ready, owed, window_jobs),
            bound: PassBound::Window,
        },
        None => PassAdmission {
            jobs: ready.min(ceiling),
            bound: PassBound::Ceiling,
        },
    }
}

/// How one pass's window is shared out among the queues, in the order given.
///
/// The queues draw on one allowance instead of each being judged against the
/// whole of it. Judged separately, a pass over three queues may put three
/// windows' worth of work in flight while the reading that sized them says the
/// pool pays for one -- and that reading is the pool's answer, so the extra is
/// refused upstream rather than run.
///
/// Drawing in order is what makes the first queue a priority rather than a
/// second allowance: it takes what it needs, and what is left is what the next
/// one may dispatch. Only a queue with work ready and unsatisfied is affected
/// -- an empty high-priority queue takes nothing, so the ordinary queue gets
/// the whole window whenever nothing is waiting to preempt it. That is the
/// failure the equal split this replaced was written to avoid: a stalled
/// queue's backlog sizing a healthy queue's batch.
///
/// The ceiling branch is unchanged per queue: `owed` is only meaningful against
/// a known allowance, and subtracting it from a slice admits nothing as soon as
/// the backlog exceeds one slice.
pub fn admissions_by_lane(
    lanes: impl IntoIterator<Item = (u64, u64)>,
    window_jobs: Option<u64>,
    ceiling: u64,
) -> Vec<PassAdmission> {
    let lanes: Vec<(u64, u64)> = lanes.into_iter().collect();
    let mut remaining = window_jobs.map(|window| {
        let owed: u64 = lanes.iter().map(|(_, owed)| *owed).sum();
        window.saturating_sub(owed)
    });
    lanes
        .into_iter()
        .map(|(ready, _)| match remaining {
            Some(left) => {
                let jobs = ready.min(left);
                remaining = Some(left - jobs);
                PassAdmission {
                    jobs,
                    bound: PassBound::Window,
                }
            }
            None => PassAdmission {
                jobs: ready.min(ceiling),
                bound: PassBound::Ceiling,
            },
        })
        .collect()
}

/// Which queue this is, as the label an admission reading carries.
///
/// A closed set of three, derived rather than passed: a label value taken from a
/// caller is a label value that will one day hold a task id, and a series named
/// by a value that changes with the tick is a series per tick.
pub fn stream_kind(stream: &str, workspace_id: &str) -> &'static str {
    if stream == stop_the_line_ready_stream(workspace_id) {
        // 与写出这条通道的输入键同一个拼写：两处各写一遍时，改一处的那天
        // 另一个读者会去找一个再也不出现的东西，而「没找到」不报错。
        cog_core::READY_LANE_STOP_THE_LINE
    } else if stream == process_local_ready_stream(workspace_id) {
        "local"
    } else {
        "shared"
    }
}

/// The queues a deployment consumes. Every worker reads the shared one; only
/// the executor role reads the process-local one.
pub fn consumed_ready_streams(workspace_id: &str, owns_executor_role: bool) -> Vec<String> {
    let mut streams = vec![ready_stream(workspace_id)];
    if owns_executor_role {
        streams.push(process_local_ready_stream(workspace_id));
        streams.push(stop_the_line_ready_stream(workspace_id));
    }
    streams
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{Task, TaskType};

    fn task(id: &str, ty: TaskType, input: serde_json::Value) -> Task {
        Task::new(id, ty, input)
    }

    /// The duty exclusion has to be about the type and nothing else: the row
    /// is executable by design (the drift reading counts only executable rows),
    /// so a predicate that stopped at `is_executable` would let the dispatcher
    /// start it again.
    #[test]
    fn a_face_duty_is_never_dispatchable_but_ordinary_work_is() {
        let duty = task(
            "face-work-x",
            TaskType::Custom(crate::governance_acts::FACE_DUTY_TASK_TYPE.into()),
            serde_json::json!({}),
        );
        assert!(
            duty.is_executable,
            "the reading counts only executable work"
        );
        assert!(!is_dispatchable_work(&duty));

        let mut work = task("work-x", TaskType::DagNode, serde_json::json!({}));
        work.acceptance.push("门测试跑绿".into());
        assert!(
            is_dispatchable_work(&work),
            "the exclusion must not swallow ordinary work"
        );
    }

    #[test]
    fn the_two_queues_are_distinct_and_derived_from_the_workspace() {
        assert_eq!(ready_stream("ws"), "orchestrator:ready:ws");
        assert_eq!(
            process_local_ready_stream("ws"),
            "orchestrator:ready:ws:local"
        );
        assert_ne!(ready_stream("ws"), process_local_ready_stream("ws"));
        assert_eq!(
            stop_the_line_ready_stream("ws"),
            "orchestrator:ready:ws:stop-the-line"
        );
        assert_ne!(ready_stream("ws"), stop_the_line_ready_stream("ws"));
        assert_ne!(
            process_local_ready_stream("ws"),
            stop_the_line_ready_stream("ws")
        );
    }

    /// A task that says it may not wait goes to the lane nothing else uses.
    ///
    /// This is the whole mechanism behind "the red build is repaired first":
    /// the other queue is a first-in-first-out log, so the only way to run
    /// ahead of what is already in it is not to be in it. A task that keeps
    /// travelling on the ordinary queue waits behind every message already
    /// appended, however deep that queue has grown.
    #[test]
    fn a_task_that_may_not_wait_travels_in_its_own_lane() {
        let mut urgent = task(
            "t1",
            TaskType::Custom("ci_hold_fix".into()),
            serde_json::json!({"goal": "make CI green", "evolution_mode": "generate_change"}),
        );
        urgent.input[cog_core::INPUT_READY_LANE] =
            serde_json::json!(cog_core::READY_LANE_STOP_THE_LINE);
        assert_eq!(
            ready_stream_for(&urgent, "ws"),
            stop_the_line_ready_stream("ws"),
            "a stop-the-line task must not queue behind the ordinary generation backlog"
        );
        assert_eq!(
            stream_kind(&ready_stream_for(&urgent, "ws"), "ws"),
            "stop_the_line"
        );

        // The marker is read as a value, not as "any lane": a task naming some
        // other lane keeps the routing it had.
        let mut other = urgent.clone();
        other.input[cog_core::INPUT_READY_LANE] = serde_json::json!("ordinary-ish");
        assert_eq!(
            ready_stream_for(&other, "ws"),
            process_local_ready_stream("ws")
        );
    }

    /// Only the deployment that owns the executor role reads the lane: the
    /// change a stop-the-line repair produces is written into that process's
    /// change directory, exactly like an ordinary generation task's.
    #[test]
    fn only_the_executor_role_reads_the_stop_the_line_lane() {
        let worker = consumed_ready_streams("ws", false);
        assert_eq!(worker, vec![ready_stream("ws")]);
        let executor = consumed_ready_streams("ws", true);
        assert!(executor.contains(&stop_the_line_ready_stream("ws")));
        assert!(executor.contains(&process_local_ready_stream("ws")));
    }

    #[test]
    fn a_change_generation_task_goes_to_the_process_local_queue() {
        let generation = task(
            "t1",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it", "evolution_mode": "generate_change"}),
        );
        assert_eq!(
            ready_stream_for(&generation, "ws"),
            process_local_ready_stream("ws")
        );

        let typed = task(
            "t2",
            TaskType::Custom("self_evolution".into()),
            serde_json::json!({}),
        );
        assert_eq!(
            ready_stream_for(&typed, "ws"),
            process_local_ready_stream("ws")
        );
    }

    #[test]
    fn ordinary_tasks_stay_on_the_shared_queue() {
        let plain = task(
            "t3",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it"}),
        );
        assert_eq!(ready_stream_for(&plain, "ws"), ready_stream("ws"));

        let ported = task(
            "t4",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"evolution_mode": "baseline_port"}),
        );
        assert_eq!(ready_stream_for(&ported, "ws"), ready_stream("ws"));
    }

    #[test]
    fn a_task_naming_its_owner_is_not_offered_to_the_shared_queue() {
        let plain = task("t5", TaskType::DagNode, serde_json::json!({}));
        assert!(!carries_an_owner(&plain));

        let mut owned = task("t6", TaskType::DagNode, serde_json::json!({}));
        owned.owner_unit = Some("8b".into());
        assert!(
            carries_an_owner(&owned),
            "an owned task on the shared queue would be run by whoever read it \
             first, and its owner would only learn that afterwards"
        );
    }

    /// The lane that runs while everything else waits must not be held by an
    /// owner. Measured on 2026-11-05: the restaffing pass wrote the CI face's
    /// owner onto the deployer's just-filed `-fix` row, the publisher held it
    /// out of both queues, and the rollout stayed stopped for ten hours while
    /// the stop-the-line stream sat drained.
    #[test]
    fn an_owned_row_in_the_stop_the_line_lane_is_still_offered() {
        let mut owned = task(
            "t7",
            TaskType::Custom("ci_hold_fix".into()),
            serde_json::json!({ (cog_core::INPUT_READY_LANE): cog_core::READY_LANE_STOP_THE_LINE }),
        );
        owned.owner_unit = Some("f5".into());
        assert!(
            !carries_an_owner(&owned),
            "the lane outranks ownership: an owner is who the row is for, not a \
             lock on who may run it, and holding it here strands the row in a \
             lane nothing else can serve"
        );
        assert_eq!(
            ready_stream_for(&owned, "ws"),
            stop_the_line_ready_stream("ws")
        );

        // Everything else keeps the rule: the same owner, no lane, still held.
        let mut ordinary = task("t8", TaskType::DagNode, serde_json::json!({}));
        ordinary.owner_unit = Some("f5".into());
        assert!(carries_an_owner(&ordinary));
    }

    #[test]
    fn only_the_executor_role_reads_the_process_local_queue() {
        let worker = consumed_ready_streams("ws", false);
        assert_eq!(worker, vec![ready_stream("ws")]);
        assert!(
            !worker.contains(&process_local_ready_stream("ws")),
            "a deployment without the executor role must leave change-generation \
             tasks for the process that can read what they produce"
        );

        let executor = consumed_ready_streams("ws", true);
        assert_eq!(
            executor,
            vec![
                ready_stream("ws"),
                process_local_ready_stream("ws"),
                stop_the_line_ready_stream("ws"),
            ]
        );
    }

    fn pace(measured: usize, allowance: u64, spent: u64, reserve: u64) -> cog_core::LlmWindowPace {
        cog_core::LlmWindowPace {
            measured_upstreams: measured,
            allowance_tokens: allowance,
            spent_tokens: spent,
            reserve_tokens: reserve,
            reserve_population: 5,
            window_secs: 604_800,
            window_start_unix: 0,
        }
    }

    #[test]
    fn a_measured_pool_yields_the_jobs_its_remaining_allowance_pays_for() {
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(2, 5_000_000, 3_000_000, 500_000), 2),
            Some(4)
        );
        // Spent past the allowance is zero, not a wrapped-around huge count.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(2, 1_000_000, 2_000_000, 500_000), 2),
            Some(0)
        );
    }

    #[test]
    fn nothing_measured_is_no_bound_and_never_zero() {
        // A missing measurement read as a zero bound would stop dispatch for
        // as long as the reading is missing.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(0, 0, 0, 500_000), 2),
            None
        );
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(2, 5_000_000, 0, 0), 2),
            None
        );
    }

    #[test]
    fn admission_is_measured_against_what_the_queue_already_owes() {
        // Nothing measured: this function says everything ready may go out.
        // The publisher does not use that answer as it stands -- see
        // `admission_for_pass`, which puts the pass's slice on this branch.
        assert_eq!(admitted(10, 5, None), 10);
        // Ten ready, five already owed, a window that pays for eight: three.
        // Compared with zero instead, this would be eight every pass -- the
        // same allowance re-admitted while the previous batch is still queued.
        assert_eq!(admitted(10, 5, Some(8)), 3);
        // A queue that owes more than the window pays for admits nothing, and
        // the count does not wrap around.
        assert_eq!(admitted(10, 20, Some(8)), 0);
        // Fewer ready than the window could pay for: the queue is the bound.
        assert_eq!(admitted(2, 1, Some(100)), 2);
    }

    #[test]
    fn a_pass_that_measured_nothing_takes_a_slice_and_not_the_backlog() {
        // The case that made this a defect: a pod minutes old, no status read
        // yet, two thousand ready tasks. Unbounded, the whole backlog went out
        // on one tick and the transport -- still loading -- refused the writes,
        // leaving all of them in `Scheduled` for a stall window.
        let slice = admission_for_pass(2_024, 0, None, 200);
        assert_eq!(slice.jobs, 200);
        assert_eq!(slice.bound, PassBound::Ceiling);
        // A measured pass is not trimmed to the slice: the pool's own answer is
        // the bound, and a window that pays for more than one slice is a window
        // that should be used.
        let measured = admission_for_pass(2_024, 0, Some(500), 200);
        assert_eq!(measured.jobs, 500);
        assert_eq!(measured.bound, PassBound::Window);
        // The owed count is not subtracted from the ceiling: with a backlog
        // already in flight above one slice, subtracting would admit nothing,
        // which is the halt the ceiling branch exists to avoid.
        assert_eq!(admission_for_pass(2_024, 1_729, None, 200).jobs, 200);
        // And a ready set smaller than a slice is still just the ready set.
        assert_eq!(admission_for_pass(7, 0, None, 200).jobs, 7);
    }

    #[test]
    fn the_queues_draw_on_one_window_in_the_order_they_are_given() {
        // Three queues with work ready, a pool that pays for four jobs. Judged
        // one at a time against the whole window each would take four -- twelve
        // in flight on a reading that says four, and the twelve are what the
        // upstream refuses.
        let jobs = |lanes: [(u64, u64); 3], window| -> Vec<u64> {
            admissions_by_lane(lanes, window, 200)
                .into_iter()
                .map(|a| a.jobs)
                .collect()
        };
        assert_eq!(jobs([(4, 0), (4, 0), (4, 0)], Some(4)), vec![4, 0, 0]);

        // Work already in flight on any queue has been charged to the same
        // window: eight owed across the three queues, ten to pay for, two left
        // for the queue at the front of the order.
        assert_eq!(jobs([(4, 2), (4, 3), (4, 3)], Some(10)), vec![2, 0, 0]);

        // A queue with nothing ready takes nothing, whichever queue it is: the
        // ordinary work gets the whole window when nothing is waiting to
        // preempt it.
        assert_eq!(jobs([(0, 0), (9, 0), (0, 0)], Some(5)), vec![0, 5, 0]);
        assert_eq!(jobs([(4, 0), (0, 0), (0, 0)], Some(4)), vec![4, 0, 0]);

        // Nothing measured the pool: every queue takes a slice, as before. A
        // shared remainder here would admit nothing once the backlog passed one
        // slice, which is the halt the ceiling branch exists to avoid.
        let unmeasured = admissions_by_lane([(2_024, 0), (2_024, 1_729), (7, 0)], None, 200);
        assert_eq!(
            unmeasured.iter().map(|a| a.jobs).collect::<Vec<_>>(),
            vec![200, 200, 7]
        );
        assert!(unmeasured.iter().all(|a| a.bound == PassBound::Ceiling));
    }

    fn bound_of(window_jobs: Option<u64>) -> PassBound {
        admission_for_pass(1, 0, window_jobs, 200).bound
    }

    #[test]
    fn a_hold_names_the_slice_when_no_reading_decided_it() {
        // Nothing measured the pool: neither the window nor availability can be
        // what held the rest, and naming either would send the reader to repair
        // something that is not the constraint.
        assert_eq!(hold_reason(false, bound_of(None)), "pass_ceiling");
        assert_eq!(hold_reason(true, bound_of(None)), "pass_ceiling");
        assert_eq!(hold_reason(false, bound_of(Some(3))), "window_exhausted");
        assert_eq!(hold_reason(true, bound_of(Some(0))), "pool_unavailable");
    }

    fn status(
        pool_size: usize,
        measured: usize,
        unavailable: bool,
        kinds: &[&str],
    ) -> cog_core::LlmPoolStatus {
        cog_core::LlmPoolStatus {
            unavailable,
            pool_size,
            pace: pace(measured, 5_000_000, 3_000_000, 500_000),
            upstream_evidence: kinds
                .iter()
                .map(|kind| cog_core::LlmUpstreamEvidence {
                    refusal_kind: (*kind).into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn upstreams_that_can_never_come_back_do_not_make_the_measurement_partial() {
        // Two of four members answer `entitlement`: no window rolls them back,
        // so the measurement can cover the whole pool this reading can ever
        // cover. Counting them made this `None`, which admits everything.
        assert_eq!(
            jobs_the_window_can_pay_for_status(&status(
                4,
                2,
                false,
                &["quota_cycle", "entitlement", "quota_cycle", "entitlement"]
            )),
            Some(4)
        );
    }

    #[test]
    fn a_publisher_only_skips_a_pass_the_pool_itself_closed_with_a_clock() {
        // The pool is down and named when it will be probed again: every pass
        // before that instant would decide the same zero.
        assert_eq!(
            publisher_backoff_until(true, 1_000, 400),
            Some(1_000),
            "a closed pool with a future probe instant is a pass worth skipping"
        );
        // The instant has passed: the verdict may have changed, so ask again.
        assert_eq!(publisher_backoff_until(true, 1_000, 1_000), None);
        assert_eq!(publisher_backoff_until(true, 1_000, 2_000), None);
        // Down with no clock is not "sleep forever": an unread instant is not a
        // promise, and skipping on it would freeze the queue behind a zero.
        assert_eq!(publisher_backoff_until(true, 0, 2_000), None);
        // An available pool always runs the pass, whatever the clock says.
        assert_eq!(publisher_backoff_until(false, 1_000, 400), None);
    }

    #[test]
    fn a_hold_names_the_gate_that_produced_it_not_the_one_it_is_filed_under() {
        // Both zeros reach the same hold; only the label says which repair.
        assert_eq!(hold_reason(true, PassBound::Window), "pool_unavailable");
        assert_eq!(hold_reason(false, PassBound::Window), "window_exhausted");
        // The case that made this a defect: a pool that is down still has an
        // allowance that pays for jobs, so the window is not the constraint
        // while the bound is zero. Filing that under the window sends the
        // reader to tune a budget.
        let down = status(2, 2, true, &["quota_cycle", "quota_cycle"]);
        assert_eq!(jobs_the_window_can_pay_for_status(&down), Some(0));
        assert!(
            jobs_the_window_can_pay_for(&down.pace, measurable_pool_size(&down)).unwrap_or(0) > 0,
            "the fixture must keep paying for a job, or it cannot show the two reasons apart"
        );
        assert_eq!(
            hold_reason(down.unavailable, bound_of(Some(0))),
            "pool_unavailable"
        );
    }

    #[test]
    fn a_pool_that_is_down_pays_for_nothing_while_its_allowance_accrues() {
        // A window's allowance keeps growing whether or not anything serves.
        // Without the whole-pool statement, the moment it crossed one job's
        // cost this would start admitting work to a pool that cannot take it.
        assert_eq!(
            jobs_the_window_can_pay_for_status(&status(
                4,
                2,
                true,
                &["quota_cycle", "entitlement", "quota_cycle", "entitlement"]
            )),
            Some(0)
        );
    }

    #[test]
    fn a_measurement_that_covers_less_than_the_usable_pool_is_still_no_bound() {
        // One of the two members that can come back reported. That is a subset
        // of the usable pool, so it is still no bound -- the entitlement
        // exclusion must not turn a partial reading into a usable one.
        assert_eq!(
            jobs_the_window_can_pay_for_status(&status(
                4,
                1,
                false,
                &["quota_cycle", "entitlement", "quota_cycle", "entitlement"]
            )),
            None
        );
    }

    /// 没配上限的看板放行一切：一个「没人配上限」被读成「上限为零」的实现，
    /// 会在第一次没有配置的部署里停掉整块看板的派发。
    #[test]
    fn an_unconfigured_board_admits_everything() {
        let (limits, _) = cog_core::BoardWipLimits::from_spec("");
        assert_eq!(column_at_limit(&limits, |_| 1_000), None);
    }

    /// 满了就拦，并且点名是哪一列拦的——判词要能把人送到该去松的那一列。
    /// 边界是「到」不是「超」：上限 4 的第 5 条进不来，第 4 条进得来。
    #[test]
    fn a_full_column_names_itself_and_at_the_limit_is_full() {
        let (limits, _) = cog_core::BoardWipLimits::from_spec("scheduled=4");
        assert_eq!(
            column_at_limit(&limits, |c| if c == "scheduled" { 4 } else { 0 }),
            Some("scheduled")
        );
        assert_eq!(
            column_at_limit(&limits, |c| if c == "scheduled" { 3 } else { 0 }),
            None
        );
    }

    /// 别的列满了也是拦：往上游再压活不能让它变快。这里 `running` 满了而
    /// `scheduled` 还空着，若只看 scheduled 的余量，这一轮会照发一整批，
    /// 而它们只会堆在同一列后面。
    #[test]
    fn a_full_running_column_stops_the_queue_that_feeds_it() {
        let (limits, _) = cog_core::BoardWipLimits::from_spec("running=4");
        assert_eq!(
            column_at_limit(&limits, |c| if c == "running" { 4 } else { 0 }),
            Some("running")
        );
        assert_eq!(
            column_at_limit(&limits, |c| if c == "running" { 3 } else { 0 }),
            None,
            "有空的 running 列不该被凭空当成满"
        );
    }

    /// 到线即停：花到预算的那一条算花完了，不必再等它明显超出去。中间那一格
    /// 是这一族的形状陷阱——差一个 token 的两侧读起来几乎一样，判成放行就是
    /// 「预算说完的地方它还可以再跑一次」。
    #[test]
    fn a_task_that_spent_its_budget_is_past_it() {
        assert!(!past_its_budget(999, 1000), "没花完就还能派");
        assert!(past_its_budget(1000, 1000), "刚好花完也算花完");
        assert!(past_its_budget(1001, 1000), "超了当然算花完");
    }

    /// 零预算与「计划花零个 token」不是一件事：前者是没有计划，后者会把每一次
    /// 派发都拦下。基线表里 `budget_tokens` 的零值来自「分解没给估算」，所以它
    /// 只能读成不设界。
    #[test]
    fn a_plan_without_a_budget_holds_nothing() {
        assert!(!past_its_budget(0, 0));
        assert!(!past_its_budget(1_000_000, 0));
    }

    /// 一条不写「怎样算做完」的行不派发。
    ///
    /// 派发它得到的是一次会以执行器自己的总结收尾的运行：按状态取行的视图
    /// 分不出它和一条真修好的。留住的那一半是安全的一半——行仍 pending、仍在
    /// 看板上，于是它读作「从没开始」，而不是「开始了、永远关不掉」。
    #[test]
    fn a_row_that_does_not_say_what_done_means_is_not_dispatched() {
        let mut row = Task::new(
            "work-1",
            cog_core::TaskType::Custom("change_ledger".into()),
            serde_json::json!({"goal": "g"}),
        );
        assert!(row.is_executable);
        assert!(!is_dispatchable_work(&row), "没有判据的活不该被派发");
        assert!(!states_what_done_means(&row));

        row.acceptance.push("账上同一身份的账行落到 landed".into());
        assert!(states_what_done_means(&row));
        assert!(is_dispatchable_work(&row));
    }

    /// 判据是画在**派发**这一道上的，不是画在所有行上：不执行的承载行与面
    /// 值守行本来就不该被派发，缺判据不能让它们换一个理由被拒。
    ///
    /// 同一页的另一半是「被留住的」：它俩在有体可跑的那一族上互补。把那一半写成
    /// `!states_what_done_means` 一句话，面值守行与承载行就会一起被算进「因为没判据
    /// 而停着」，读数上的那一格从此永远有数。
    #[test]
    fn what_is_held_for_want_of_a_criterion_is_the_complement_over_work_that_runs() {
        let mut row = task(
            "work-2",
            TaskType::Custom("platform_ci_fix".into()),
            serde_json::json!({}),
        );
        assert!(held_for_want_of_a_criterion(&row));
        assert!(!is_dispatchable_work(&row));

        row.acceptance.push("分支的下一次运行判绿".into());
        assert!(!held_for_want_of_a_criterion(&row));
        assert!(is_dispatchable_work(&row));

        // 面值守行有体可跑、也是可执行的，但它不是「等判据」：派发器留它是因为它是
        // 值守行，判据补上也还是留。
        let mut duty = task(
            "face-work-y",
            TaskType::Custom(crate::governance_acts::FACE_DUTY_TASK_TYPE.into()),
            serde_json::json!({}),
        );
        assert!(!held_for_want_of_a_criterion(&duty));
        duty.acceptance.push("值班有人在".into());
        assert!(!held_for_want_of_a_criterion(&duty));

        let goal = Task::goal_row("goal-2", "ship it");
        assert!(!held_for_want_of_a_criterion(&goal));
    }
    #[test]
    fn a_row_with_no_body_to_run_is_excluded_for_its_own_reason() {
        let mut goal_row = Task::goal_row("goal-1", "ship it");
        goal_row
            .acceptance
            .push("目标由一段被放行的变更达成".into());
        assert!(!is_dispatchable_work(&goal_row));
        assert!(states_what_done_means(&goal_row));
    }

    #[test]
    fn a_measurement_covering_part_of_the_pool_is_no_bound() {
        // One member's window summed as if it were the pool's repeats the
        // refusal's old category error on the enqueue side.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(1, 5_000_000, 0, 500_000), 3),
            None
        );
        // `pool_size == 0` is "not reported", not a pool of no upstreams.
        assert_eq!(
            jobs_the_window_can_pay_for(&pace(1, 5_000_000, 0, 500_000), 0),
            Some(10)
        );
    }

    /// 门类是按那个 intake 自己的签名认的，不是按目标文本里的措辞。
    #[test]
    fn a_gate_is_the_row_the_principle_ledger_wrote() {
        let gate = task(
            "g",
            TaskType::Custom("change_ledger".into()),
            serde_json::json!({"goal": "Give the principle X the gate it is owed", "change_sources": ["principle_ledger"]}),
        );
        assert!(is_principle_ledger(&gate));

        let product = task(
            "p",
            TaskType::Custom("change_ledger".into()),
            serde_json::json!({"goal": "Give the principle X the gate it is owed", "change_sources": ["2026-09-03_deploy.md"]}),
        );
        assert!(
            !is_principle_ledger(&product),
            "同样一句话的目标，来源不同就不是门类：认的是签名不是措辞"
        );

        // 没有这个字段的行不是门类任务：读不到来源不等于它来自原则账。
        let plain = task(
            "x",
            TaskType::Custom("change_ledger".into()),
            serde_json::json!({"goal": "ship it"}),
        );
        assert!(!is_principle_ledger(&plain));
    }

    /// 占比是量出来的，样本不够就没有占比。
    #[test]
    fn a_share_needs_a_landing_sample_to_be_a_reading() {
        assert_eq!(
            GateShare {
                gate: 5,
                total: 100
            }
            .permille(),
            Some(50),
            "5/100 的落地占比是 5%"
        );
        assert_eq!(
            GateShare {
                gate: 5,
                total: MIN_GATE_SHARE_SAMPLES - 1
            }
            .permille(),
            None,
            "几条样本算出来的占比是噪声，不是读数"
        );
        assert_eq!(GateShare { gate: 0, total: 0 }.permille(), None);
    }

    /// 座位跟着实测占比走：86% 的排队量、5% 的落地占比，座位按后者算。
    ///
    /// 这条测的就是 2026-11-06 的现场：1,675 条门类排在一共 1,955 条里，
    /// 而落地的账行里门类只占 5%。
    #[test]
    fn gate_seats_follow_the_measured_share_not_the_queue_depth() {
        let share = GateShare {
            gate: 5,
            total: 100,
        }
        .permille();
        assert_eq!(
            gate_seats(200, share, 1_675, 280, false),
            10,
            "200 个座位 × 5% = 10 个"
        );
        // 占比小到乘出来不足一个座位时仍然给一个：门类每一拍都要往前走一步，
        // 否则「排在后面」就成了「永久让路」。
        let tiny = GateShare {
            gate: 1,
            total: 1_000,
        }
        .permille();
        assert_eq!(tiny, Some(1));
        assert_eq!(gate_seats(10, tiny, 1_675, 280, false), 1);
        // 没有读数就不设界（样本不够、账读不到）。
        assert_eq!(gate_seats(200, None, 1_675, 280, false), 1_675);
    }

    /// 别人不要的座位门类可以全拿：这一刀判先后，不判能不能走。
    #[test]
    fn spare_seats_are_not_kept_empty_to_hold_a_class_back() {
        let share = GateShare {
            gate: 5,
            total: 100,
        }
        .permille();
        // 产品活只有 3 条，座位 200 个：门类可以坐满剩下的。
        assert_eq!(gate_seats(200, share, 1_675, 3, false), 197);
        // 产品活比座位还多时，门类只剩它那一份。
        assert_eq!(gate_seats(200, share, 1_675, 900, false), 10);
        // 产品活和座位一样多时，门类的那一份是抢出来的，不是让出来的。
        assert_eq!(gate_seats(200, share, 1_675, 200, false), 10);
    }

    /// 等过界的一次插队要多给一个座位，而不是把闸门整个拆掉。
    #[test]
    fn starving_gate_class_jumps_the_queue_by_one_seat() {
        let share = GateShare {
            gate: 5,
            total: 100,
        }
        .permille();
        assert_eq!(
            gate_seats(200, share, 1_675, 900, true),
            11,
            "多一个座位，不是全放"
        );
        // 门类本来就没有就绪的活时，插队也变不出活来。
        assert_eq!(gate_seats(200, share, 0, 900, true), 0);
        // 这一拍一条都不许出时，门类也不硬塞：那一条会在截断那一步丢掉。
        assert_eq!(gate_seats(0, share, 1_675, 900, true), 0);
    }

    /// 重排之后先别人、后门类；超过座位的那几条这一拍不出去。
    #[test]
    fn the_batch_goes_out_others_first_and_gates_up_to_their_seats() {
        let batch = vec![
            ("g1", true),
            ("p1", false),
            ("g2", true),
            ("p2", false),
            ("g3", true),
        ];
        let ordered = order_by_class(batch, |(_, gate)| *gate, 2, 4);
        assert_eq!(
            ordered,
            vec![("p1", false), ("p2", false), ("g1", true), ("g2", true)],
            "产品活先走，门类按座位数给，超出的留在 Pending"
        );

        // 座位为零（没有门类就绪）时整批原样保留。
        let batch = vec![("p1", false), ("p2", false)];
        assert_eq!(order_by_class(batch, |(_, gate)| *gate, 0, 5).len(), 2);
    }

    /// 整条规则合成一处时，读数里能同时看见「给了几个座位」和「拦了几条」。
    #[test]
    fn classing_a_batch_reports_the_seats_it_gave_and_what_it_held() {
        let batch = vec!["g1", "p1", "g2", "p2", "g3", "g4"];
        let share = Some(GateShare {
            gate: 5,
            total: 100,
        });
        let (ordered, seats, held) =
            class_the_batch(batch, |entry| entry.starts_with('g'), 6, share, false);
        assert_eq!(seats, 4, "别人只占两条，剩下的座位门类全拿");
        assert_eq!(held, 0, "座位够时一条都不拦");
        assert_eq!(ordered.len(), 6);
        assert_eq!(
            &ordered[..2],
            &["p1", "p2"],
            "产品活先走，门类排在后面——但排在后面不等于让出这一批"
        );

        // 产品活占满座位时，门类只拿它那一份，多出来的这一拍不出去。
        let mut batch: Vec<String> = (0..20).map(|i| format!("p{i}")).collect();
        batch.extend(["g1", "g2", "g3"].map(String::from));
        let (ordered, seats, held) =
            class_the_batch(batch, |entry| entry.starts_with('g'), 10, share, false);
        assert_eq!((seats, held), (1, 2), "10 个座位 × 5% = 1 个给门类");
        assert_eq!(ordered.len(), 10, "这一拍就是十个座位，不多投");
        assert_eq!(
            ordered
                .iter()
                .filter(|entry| entry.starts_with('g'))
                .count(),
            1,
            "座位里那一条门类在后面，被拦住的两条留在 Pending"
        );
        assert_eq!(ordered[0], "p0", "产品活排在门类前面");
        assert_eq!(ordered[9], "g1", "门类坐的是这一批的最后一个座位");

        // 没有读数时这一拍不重排：只把门类排到后面而没有占比，等于让它们
        // 永久让路。
        let batch = vec!["g1", "p1", "g2"];
        let (ordered, seats, held) =
            class_the_batch(batch, |entry| entry.starts_with('g'), 10, None, false);
        assert_eq!(
            (seats, held),
            (0, 0),
            "没有读数时这一刀一个座位都没分：门类落不落进这一拍由后面按优先序截断那一刀说了算"
        );
        assert_eq!(ordered, vec!["g1", "p1", "g2"], "顺序原样保留");
    }

    /// 两类各自保序：类内部打乱会让同一类里等得最久的那条排到最后。
    #[test]
    fn ordering_keeps_each_class_in_its_own_priority_order() {
        let batch = vec![("g1", true), ("g2", true), ("p1", false), ("p2", false)];
        let ordered = order_by_class(batch, |(_, gate)| *gate, 1, 3);
        assert_eq!(
            ordered,
            vec![("p1", false), ("p2", false), ("g1", true)],
            "门类里先来的那条先走"
        );
    }
}
