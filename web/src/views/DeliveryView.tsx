import {
  getBuildGate,
  getBuildRuns,
  getSourceLine,
  getWorkloads,
  type BuildGateReport,
  type BuildRunsView,
  type SourceLineReport,
  type WorkloadView,
  type WorkloadsReport,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { BarList, Strip, type StripSegment } from '@/ui/plots';

/**
 * 构建与部署面：谁在跑第几版、滚动到哪一步；这轮构建过什么、每条怎么结束的。
 *
 * 上半页来自 apiserver 的五种工作负载对象——Deployment / StatefulSet / DaemonSet /
 * CronJob / Job，不是只有 Deployment。也只读 Deployment 时 postgres、redis（StatefulSet）、
 * 节点级的常驻组件（DaemonSet）、备份（CronJob）、每次主线部署（Job）都在集群里跑着而
 * 页面上没有，而空白读起来像「本来就没有」。每行带工种列，工种之间不能通用的那一列
 * 显式写「不适用」，不写 0，也不留空白。
 *
 * Deployment 的 revision 注解就是「这是第几次改动滚上来的」，条件里
 * `Progressing=False` 是滚动卡住而不是进行中。工作负载的名字就是平台自己的组件名，
 * 不另编一套漂亮的名字——运维要能把它和 `kubectl` 的输出对上。
 *
 * 下半页是另一种东西：它来自演进进程自己的内存窗口，重启清零，所以回答的是「这一轮」
 * 而不是历史。两半的出处不同，读数也各自带着出处——上半页的 0 是集群里真的没有,
 * 下半页的 0 可能是没人报。
 */
export function DeliveryView() {
  const workloads = useResource('工作负载', getWorkloads, 15_000);
  const report = workloads.data;
  const rows = report?.workloads ?? [];

  const builds = useResource('构建跑次', getBuildRuns, 20_000);
  // 槽位是宿主上的文件与内核锁，没有任何一条总线事件会在它变的时候说话：那是宿主上的
  // 进程在拿锁，不是集群里的一次写。所以它按秒级轮询，不是「有变化才读」——一份「谁
  // 占着」的读数晚半分钟就答不了「现在为什么不动」。
  const gate = useResource('构建槽', getBuildGate, 5_000);
  // 源线同样来自宿主上的文件（共享裸仓库），没有任何总线事件会在引用移动时说话。
  // 与槽同一档的理由：一份「推上去没有」的读数晚半分钟就答不了「现在为什么不动」。
  const line = useResource('源线', getSourceLine, 15_000);
  // 两半各有各的帧：工作负载来自集群对象，一次滚动的每一步都是一次写；构建跑次
  // 来自演进进程，一次跑完就落一条。两张表在同一屏上，一次触发两半一起重读——
  // 分两次读会让上半页先变、下半页还是上一轮。
  useKindTickedRefresh(
    [resourceKey('cluster'), resourceKey('evolution')],
    [workloads.refresh, builds.refresh]
  );
  const view = builds.data;
  const runs = view?.runs ?? [];
  // 表要一个稳定 key，而同一变更的两次尝试可能落在同一秒：带上行号，不靠时间戳
  // 自己去区分。
  const runRows = runs.map((run, index) => ({ ...run, key: `${run.change_id}-${run.at}-${index}` }));
  const built = runs.filter((r) => r.outcome === 'built');
  const notBuilt = runs.filter((r) => r.outcome !== 'built');

  // 「滚到位」只问那些真有目标副本的工种：CronJob 没有副本，把它算进分母会让这格
  // 永远差一个。新版本数缺席的工种（Job）只看就绪那一边，不当成零。
  const rolling = rows.filter((d) => d.desired != null);
  const rolled = rolling.filter(isRolled);
  const stuck = rows.filter((d) => d.conditions.some(conditionIsTrouble));
  const chain = chainNodes({
    line: line.data,
    lineError: line.error,
    gate: gate.data,
    gateError: gate.error,
    builds: view ?? null,
    workloads: report,
  });

  return (
    <ViewFrame
      title="构建与部署"
      meta={
        report
          ? `整个集群 ${report.namespaces.length} 个命名空间 · ${new Date(report.collected_at).toLocaleTimeString('zh-CN')}`
          : '按 15 秒重读'
      }
    >
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">链路</h2>
        <ChainPanel nodes={chain} />
        <BranchLineTable line={line.data} error={line.error} onRetry={line.refresh} />
      </section>

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">工作负载</h2>
        {workloads.error ? (
          <ReadError what="工作负载" error={workloads.error} onRetry={workloads.refresh} />
        ) : !report ? (
          <Loading what="工作负载" />
        ) : report.error ? (
          <p className="px-4 py-2 text-[11px] text-[var(--warn)]">{report.error}</p>
        ) : rows.length === 0 ? (
          <Empty>整个集群里没有读到工作负载。</Empty>
        ) : (
          <>
            <Readings from={workloads} source="GET /api/v1/cluster/deployments" caveat="「采集于」是服务端读 apiserver 的时刻；这份读数在两次轮询之间不会自己变。范围是整个集群的全部命名空间，不只是本进程那一个。Deployment / StatefulSet / DaemonSet / CronJob / Job 五种，加上没有控制器拥有的裸 Pod（评测环境就是这种），都在这一份里。">
            <div className="flex flex-wrap py-2">
              <Reading label="工作负载" value={rows.length} note={kindSummary(rows)} />
              <Reading
                label="已滚到位"
                value={rolled.length}
                tone={rolled.length === rolling.length ? 'ok' : 'warn'}
                note={`${rolling.length - rolled.length} 个还在滚`}
              />
              <Reading
                label="有异常条件"
                value={stuck.length}
                tone={stuck.length ? 'danger' : 'plain'}
              />
              <Reading label="采集于" value={<Moment iso={report.collected_at} />} />
            </div>
            </Readings>
            <Table
              columns={[
                { key: 'kind', title: '类型', width: '130px', render: (d) => (
                  <span className="mono text-[11px] text-[var(--muted)]">
                    {d.kind}
                    {d.phase ? ` · ${d.phase}` : ''}
                  </span>
                ) },
                // 跨命名空间读回来之后，`name` 单独一行已经不能指认是谁：两个
                // 命名空间各有一个 `default` Service 是常事。命名空间这一列不是
                // 装饰，它是这行唯一性的另一半。
                { key: 'namespace', title: '命名空间', width: '150px', render: (d) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{d.namespace || '—'}</span>
                ) },
                { key: 'name', title: '工作负载', width: '190px', render: (d) => (
                  <span className="mono text-[11px]">{d.name}</span>
                ) },
                { key: 'revision', title: '版本', width: '70px', render: (d) => (
                  <Cell value={d.revision} />
                ) },
                { key: 'replicas', title: '就绪 / 目标', width: '110px', render: (d) => (
                  <Replicas workload={d} />
                ) },
                { key: 'updated', title: '新版本', width: '80px', render: (d) => (
                  d.updated == null ? (
                    <Cell value={null} />
                  ) : (
                    <span className="mono text-[11px]" style={{ color: d.desired != null && d.updated < d.desired ? 'var(--warn)' : undefined }}>
                      {d.updated}
                    </span>
                  )
                ) },
                { key: 'available', title: '可用', width: '70px', render: (d) => (
                  <Cell value={d.available == null ? null : String(d.available)} />
                ) },
                { key: 'images', title: '镜像', width: '280px', render: (d) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{d.images.join(' · ') || '—'}</span>
                ) },
                { key: 'conditions', title: '条件', render: (d) => <Conditions workload={d} /> },
              ]}
              rows={rows}
              rowKey={(d) => `${d.namespace}/${d.kind}/${d.name}`}
            />
            {/* 「已经滚到位几个」是一句比例，而表里只有逐行的「3 / 3」。按各自的
                目标算成百分比画开，谁还差、差多少一眼看见。没有目标副本的工种
                （CronJob）不画：给它画一根 0% 的条是把「没有这回事」画成「没做到」。 */}
            <BarList
              label="每个工作负载滚到了几成"
              note="就绪副本 ÷ 目标副本（GET /api/v1/cluster/deployments）；红的是还没滚到位；没有目标副本的工种不画"
              max={100}
              rows={rolling.map((d) => ({
                id: `${d.namespace}/${d.kind}/${d.name}`,
                label: `${d.namespace}/${d.kind} ${d.name}`,
                value: (d.desired ?? 0) > 0 ? Math.round(((d.ready ?? 0) / (d.desired ?? 1)) * 100) : 0,
                note: `${d.ready ?? 0} / ${d.desired ?? 0}`,
                tone: (d.ready ?? 0) >= (d.desired ?? 0) ? undefined : ('warn' as const),
                detail: d.conditions.filter(conditionIsTrouble).map((c) => c.kind).join(' · ') || undefined,
              }))}
            />
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              版本号是 Deployment 上的 revision 注解，不是发布记录：平台自己的发布清单在
              变更面，两边对同一个 rev 用的是同一个字符串。别的工种没有滚动版本这回事，
              那一格写「不适用」而不是留空——空白读起来像「这一行没读到」。
            </p>
          </>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">宿主构建槽</h2>
        <BuildGatePanel
          report={gate.data}
          error={gate.error}
          onRetry={gate.refresh}
        />
      </section>

      <section>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">构建跑次</h2>
        {builds.error ? (
          <ReadError what="构建跑次" error={builds.error} onRetry={builds.refresh} />
        ) : !view ? (
          <Loading what="构建跑次" />
        ) : !view.attached ? (
          <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
            这个进程没有挂构建读数：0 行不是「没构建过」，是没人报。上面那张工作负载表
            还能读，因为那是 apiserver 自己说的话。
          </p>
        ) : runs.length === 0 ? (
          <Empty>挂了读数，但这个进程启动以来还没有构建过——下一次构建结束就会有一行。</Empty>
        ) : (
          <>
            <Readings
              from={builds}
              source="GET /api/v1/evolution/builds"
              caveat="窗口是演进进程内存里的，最多留最近若干次，重启清零：这一格回答的是「这一轮构建过什么」，不是历史。"
            >
              <div className="flex flex-wrap py-2">
                <Reading label="窗口里的跑次" value={runs.length} note={`最多 ${view.limit} 行`} />
                <Reading
                  label="成了"
                  value={built.length}
                  tone={built.length ? 'ok' : 'plain'}
                />
                <Reading
                  label="没成"
                  value={notBuilt.length}
                  tone={notBuilt.length ? 'warn' : 'plain'}
                  note="失败、超预算被杀、没跑起来的合计"
                />
                <Reading
                  label="涉及变更"
                  value={new Set(runs.map((r) => r.change_id)).size}
                  note="同一变更重试会占多行"
                />
              </div>
            </Readings>
            {/* 「成了几个、没成的是哪一种」在表里要一行一行数。四种结局各占一段，
                比例就是这一轮构建的成败；`timed_out` 与 `failed` 分开着色，因为前者
                是预算把它杀了、后者是它自己崩了。 */}
            <Strip
              label="窗口里这几次构建各是什么结局"
              note="按 outcome 分组数（GET /api/v1/evolution/builds）；窗口是进程内存里的，重启清零"
              segments={outcomeSegments(runs)}
            />
            <Table
              columns={[
                { key: 'at', title: '结束于', width: '150px', render: (r) => <Moment iso={r.at} /> },
                {
                  key: 'change',
                  title: '变更',
                  width: '220px',
                  render: (r) => <span className="mono text-[11px]">{r.change_id}</span>,
                },
                {
                  key: 'intent',
                  title: '入口',
                  width: '120px',
                  render: (r) => (
                    <span className="mono text-[11px] text-[var(--muted)]">{r.intent}</span>
                  ),
                },
                {
                  key: 'outcome',
                  title: '结局',
                  width: '100px',
                  render: (r) => (
                    <span className="mono text-[11px]" style={{ color: outcomeColor(r.outcome) }}>
                      {r.outcome}
                    </span>
                  ),
                },
                {
                  key: 'seconds',
                  title: '占用主机',
                  render: (r) => (
                    <span className="mono text-[11px]">
                      {r.seconds === undefined ? '没跑，没有耗时' : `${r.seconds.toFixed(1)} 秒`}
                    </span>
                  ),
                },
              ]}
              rows={runRows}
              rowKey={(r) => r.key}
            />
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              一条变更重试三次是三行：按变更去重会把重试藏起来，而重试次数正是决定下一次
              把构建花在哪里的那个数。耗时那一格 `unstarted` 留空而不是 0——没跑与跑了一瞬间
              是两件事。
            </p>
          </>
        )}
      </section>
    </ViewFrame>
  );
}

/**
 * 宿主构建槽：谁占着、谁排着、这台机器有多堵。
 *
 * 这一面存在的理由是一条具体的等待：一跑全量门、一条带内构建、另一个会话的推送同
 * 时压在一台机器上，而「同时几条构建」这条界只有进了槽的人才受约束。此前要看这些
 * 读数，只能在宿主上手工跑 `--who`；一份要人手动问的读数，在真正要它的时候永远不在。
 *
 * 三种结局分开画：`unwired` / `unread` 印服务端给的理由，绝不画成一张空表——空表
 * 读起来是「没有构建在跑」，而这两种情形的处置是去改挂载。
 */
function BuildGatePanel({
  report,
  error,
  onRetry,
}: {
  report: BuildGateReport | null;
  error: string | null;
  onRetry: () => void;
}) {
  if (error) return <ReadError what="构建槽" error={error} onRetry={onRetry} />;
  if (!report) return <Loading what="构建槽" />;
  if (report.state !== 'measured') {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--warn)]">
        {report.why ?? `读不到构建槽（${report.dir}）`}
      </p>
    );
  }
  const held = report.slots.filter((slot) => slot.held);
  const free = report.slots.length - held.length;
  const reservation = report.reservation;
  return (
    <>
      <Readings
        from={{ what: '宿主构建槽', at: report.generated_at }}
        source="GET /api/v1/delivery/build-gate"
        caveat="位子是不是被占着由内核的锁回答，不由记录文件在不在回答：一个被复制、被遗留的文件会让空位子读成占着。压力与负载是同一瞬间从 /proc 读的，与位子同拍，否则答不了「是构建压住了这台机器，还是别的东西」。"
      >
        <div className="flex flex-wrap py-2">
          <Reading
            label="界（同时几条）"
            value={report.bound ?? '读不到'}
            tone={report.bound == null ? 'warn' : 'plain'}
            note={
              report.bound_written_secs_ago == null
                ? '界是会动的读数，不是常量'
                : `${formatAge(report.bound_written_secs_ago)}前重算`
            }
          />
          <Reading
            label="位子"
            value={`${held.length} / ${report.slots.length}`}
            tone={free === 0 ? 'warn' : 'plain'}
            note={free === 0 ? '全占着：下一个构建要等' : `${free} 个空着`}
          />
          <Reading
            label="排队的声明"
            value={reservation ? (reservation.fresh ? reservation.who : `${reservation.who}（已过期）`) : '没人排'}
            tone={reservation && reservation.fresh ? 'warn' : 'plain'}
            note={
              reservation?.renewed_secs_ago == null
                ? `新鲜窗 ${reservation?.fresh_window_secs ?? '?'} 秒`
                : `${formatAge(reservation.renewed_secs_ago)}前刷新 · 新鲜窗 ${reservation.fresh_window_secs} 秒`
            }
          />
          <Reading label="负载" value={report.load?.load1 ?? '读不到'} note="1 分钟平均" />
          <Reading
            label="io 压力"
            value={pressureValue(report, 'io')}
            note="PSI some avg10（卡在盘上的时间占比）"
          />
        </div>
      </Readings>
      {report.slots.length === 0 ? (
        <Empty>这个槽目录里一个位子文件都没有：不是「都空着」，是界还没发布过。</Empty>
      ) : (
        <Table
          columns={[
            {
              key: 'name',
              title: '位子',
              width: '90px',
              render: (slot) => <span className="mono text-[11px]">{slot.name}</span>,
            },
            {
              key: 'held',
              title: '状态',
              width: '90px',
              render: (slot) => (
                <span style={{ color: slot.held ? 'var(--warn)' : 'var(--ok)' }}>
                  {slot.held ? '占着' : '空着'}
                </span>
              ),
            },
            {
              key: 'who',
              title: '谁',
              render: (slot) =>
                slot.holder ? (
                  <span className="mono text-[11px]">{slot.holder.what}</span>
                ) : slot.unrecorded ? (
                  <span className="text-[11px] text-[var(--muted)]">{slot.unrecorded}</span>
                ) : (
                  <span className="text-[11px] text-[var(--muted)]">—</span>
                ),
            },
            {
              key: 'held_for',
              title: '占了多久',
              width: '110px',
              render: (slot) => (
                <span className="mono text-[11px]">
                  {slot.holder?.held_secs == null ? '—' : formatAge(slot.holder.held_secs)}
                </span>
              ),
            },
            {
              key: 'pid',
              title: 'pid',
              width: '80px',
              render: (slot) => (
                <span className="mono text-[11px] text-[var(--muted)]">
                  {slot.holder?.pid ?? '—'}
                </span>
              ),
            },
          ]}
          rows={report.slots}
          rowKey={(slot) => slot.name}
        />
      )}
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        每 5 秒重读一次：槽位是宿主上的文件与内核锁，没有总线事件会在它变的时候说话。
        全量门与宿主上的构建都从这里拿位子；没拿到位子的那一格在门的读数里是「没判」，
        不是「红了」。
      </p>
    </>
  );
}

/** 一段秒数说成人话：读者要看的是量级，不是秒表。 */
function formatAge(secs: number): string {
  if (secs < 120) return `${Math.round(secs)} 秒`;
  if (secs < 7200) return `${Math.round(secs / 60)} 分钟`;
  return `${(secs / 3600).toFixed(1)} 小时`;
}

function pressureValue(report: BuildGateReport, resource: string): string {
  const line = report.pressure.find((p) => p.resource === resource);
  return line ? line.avg10 : '读不到';
}

/** 四种结局各几条。名字照抄线上的写法，图上与表里是同一串。 */
function outcomeSegments(runs: { outcome: string }[]): StripSegment[] {
  const counted = new Map<string, number>();
  for (const run of runs) counted.set(run.outcome, (counted.get(run.outcome) ?? 0) + 1);
  const tone = (outcome: string) =>
    outcome === 'built' ? 'ok' : outcome === 'timed_out' ? 'warn' : outcome === 'unstarted' ? 'muted' : 'danger';
  return [...counted.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([outcome, count]) => ({ label: outcome, value: count, tone: tone(outcome) }));
}

/** 行里那一个词的颜色：成了是绿的，被预算杀的是黄的，其余没成的是红的。 */
function outcomeColor(outcome: string): string {
  if (outcome === 'built') return 'var(--ok)';
  if (outcome === 'timed_out') return 'var(--warn)';
  if (outcome === 'unstarted') return 'var(--muted)';
  return 'var(--danger)';
}

/**
 * 一条条件是不是坏消息。服务端给的是 `ok = 状态是 True` 这个原样读数，方向和名字有关：
 * Deployment 的 `Progressing=True` 是正在滚（好事），Job 的 `Failed=True` 是失败，
 * 而 `Failed=False` 才是它该有的样子；Deployment 的 `ReplicaFailure=True` 同理。
 * 照 `!ok` 一律当坏消息，跑挂的 Job 会被画成绿点——和把 Succeeded 画成红点是同一个
 * 错误的两个方向。
 */
function conditionIsTrouble(condition: { kind: string; ok: boolean }): boolean {
  if (condition.kind === 'ReplicaFailure' || condition.kind.startsWith('Failed')) {
    return condition.ok;
  }
  return !condition.ok;
}

/** 工种各自几条。这一格存在的理由就是「列全了没有」：只有 Deployment 时它读 11，全列是三十几。 */
function kindSummary(rows: WorkloadView[]): string {
  const counted = new Map<string, number>();
  for (const row of rows) counted.set(row.kind, (counted.get(row.kind) ?? 0) + 1);
  return [...counted.entries()].map(([kind, count]) => `${kind} ${count}`).join(' · ');
}

/** 一格读数：有值写字，缺席写「不适用」。 */
function Cell({ value }: { value: string | null }) {
  if (value == null) {
    return <span className="text-[11px] text-[var(--muted)]">不适用</span>;
  }
  return <span className="mono text-[11px]">{value}</span>;
}

/**
 * 就绪 / 目标。CronJob 没有副本，它此刻的读数是「有几个 Job 在跑」，如实说这一句
 * 比硬凑成 `1 / 1` 有用。
 */
function Replicas({ workload }: { workload: WorkloadView }) {
  if (workload.desired == null) {
    if (workload.ready == null) return <Cell value={null} />;
    return <span className="mono text-[11px]">{workload.ready} 个 Job 在跑</span>;
  }
  const ready = workload.ready ?? 0;
  // 结束的裸 Pod（Succeeded / Failed）就绪数必然是 0，那不是「差一个副本」——
  // 涂成警告色等于把结算过的活读成正在坏的活。
  const ended = workload.phase === 'Succeeded' || workload.phase === 'Failed';
  return (
    <span className="mono text-[11px]" style={{ color: !ended && ready < workload.desired ? 'var(--warn)' : undefined }}>
      {ready} / {workload.desired}
    </span>
  );
}

/**
 * 一次改动从编辑到现网的五个环节，每格五句话：状态、结论、依据、归属、从何时起。
 *
 * 为什么不是七格：流程上「提交」与「推前自检」都在同一棵工作树里、由同一个人在同一段
 * 时间里做完，它们的读数与「编辑」那一格是同一份（分支尖端 + 槽位记录），拆成三格会
 * 让同一件事在页面上显示成三段互不相干的进度。合并的是**读数相同**的格子，不是环节
 * 本身——依据那一列写着每格读的是哪一个接口，合并之后也还看得见。
 *
 * 每一格读不到的处置都写在结论里，而不是画成一个 0：这份链路的用处就是让人一眼看出
 * 「卡在哪一环」，而一格空白与一格「读不到」在这件事上是两个完全不同的答案。
 */
interface ChainNode {
  id: string;
  stage: string;
  state: 'ok' | 'running' | 'waiting' | 'stuck' | 'unknown';
  verdict: string;
  evidence: string;
  owner?: string;
  sinceSecs?: number;
  /** 没有「从何时起」这个读数时，这里说明为什么没有。 */
  sinceNote?: string;
  /** 结论之外还要说的一句：例如「这一格不覆盖谁」。不写就会让它读成「没有」。 */
  aside?: string;
}

function chainNodes({
  line,
  lineError,
  gate,
  gateError,
  builds,
  workloads,
}: {
  line: SourceLineReport | null;
  lineError: string | null;
  gate: BuildGateReport | null;
  gateError: string | null;
  builds: BuildRunsView | null;
  workloads: WorkloadsReport | null;
}): ChainNode[] {
  return [
    editNode(line, lineError),
    gateNode(gate, gateError),
    buildNode(builds),
    pushNode(line, lineError),
    deployNode(workloads),
  ];
}

const SOURCE_LINE_EVIDENCE = 'GET /api/v1/delivery/source-line';

function editNode(line: SourceLineReport | null, error: string | null): ChainNode {
  const base = { id: 'edit', stage: '编辑', evidence: SOURCE_LINE_EVIDENCE };
  if (error) {
    return { ...base, state: 'unknown', verdict: `读不到源线：${error}`, sinceNote: '没有读数' };
  }
  if (!line) return { ...base, state: 'unknown', verdict: '还在读源线', sinceNote: '没有读数' };
  if (line.state !== 'measured') {
    return {
      ...base,
      state: 'unknown',
      verdict: line.why ?? '共享裸仓库读不到',
      sinceNote: '没有读数',
    };
  }
  // 最近动过的那条分支就是此刻有人在编辑的那一条。main 自己也在这张表里，但它是
  // 「推送」那一格的归属，拿它当「编辑」会把主线的最新一次合并读成有人正在改。
  const tips = line.branches
    .filter((branch) => branch.name !== 'main')
    .sort((a, b) => (a.committed_secs_ago ?? Infinity) - (b.committed_secs_ago ?? Infinity));
  const tip = tips[0] ?? line.branches[0];
  if (!tip) {
    return { ...base, state: 'unknown', verdict: '裸仓库里一条分支都没有', sinceNote: '没有读数' };
  }
  // 「还有几条分支最近有提交」是这一格的全部用处：属主要的不是一个名字，是
  // 「台上现在有几棵树在动」。一条与三条在结论那一句里必须读得出来。
  const others = tips.length > 1 ? `（另有 ${tips.length - 1} 条分支最近有提交）` : '';
  return {
    ...base,
    state: 'ok',
    verdict: `${tip.name} 的尖端：${tip.subject}${others}`,
    owner: tip.name,
    sinceSecs: tip.committed_secs_ago,
  };
}

function gateNode(gate: BuildGateReport | null, error: string | null): ChainNode {
  const base = { id: 'gate', stage: '编译与测', evidence: 'GET /api/v1/delivery/build-gate' };
  if (error) {
    return { ...base, state: 'unknown', verdict: `读不到构建槽：${error}`, sinceNote: '没有读数' };
  }
  if (!gate) return { ...base, state: 'unknown', verdict: '还在读构建槽', sinceNote: '没有读数' };
  if (gate.state !== 'measured') {
    return { ...base, state: 'unknown', verdict: gate.why ?? '构建槽读不到', sinceNote: '没有读数' };
  }
  if (gate.slots.length === 0) {
    return { ...base, state: 'unknown', verdict: '槽目录里一个位子文件都没有：界还没发布过', sinceNote: '没有读数' };
  }
  // 界之外的构建方：它不在槽位里是对的，但「不在槽位里」读不出「它有自己的界」。
  // 不写这一行，一个在 pod 里跑编译的执行器与「没人在编译」在这张表上同形。
  const aside =
    gate.outside_the_bound.length > 0
      ? `界之外：${gate.outside_the_bound.map((o) => `${o.who}（${o.why}）`).join('；')}`
      : undefined;
  const held = gate.slots.filter((slot) => slot.held);
  if (held.length === 0) {
    return {
      ...base,
      state: 'ok',
      verdict: `槽空着（${gate.slots.length} 个位子）：没有编译在跑`,
      sinceNote: '空着，没有起点',
      aside,
    };
  }
  const first = held[0];
  return {
    ...base,
    state: 'running',
    verdict: `${held.length} / ${gate.slots.length} 个位子在做编译`,
    owner: first.holder?.what ?? first.unrecorded ?? '有人占着但没留记录',
    sinceSecs: first.holder?.held_secs,
    sinceNote: first.holder?.held_secs == null ? '持有者没留下起始时刻' : undefined,
    aside,
  };
}

function buildNode(builds: BuildRunsView | null): ChainNode {
  const base = { id: 'build', stage: '镜像构建', evidence: 'GET /api/v1/evolution/builds' };
  if (!builds) return { ...base, state: 'unknown', verdict: '这个进程没挂构建读数', sinceNote: '没有读数' };
  if (!builds.attached) {
    return { ...base, state: 'unknown', verdict: '没挂构建读数：0 行不是「没构建过」，是没人报', sinceNote: '没有读数' };
  }
  if (builds.runs.length === 0) {
    return { ...base, state: 'unknown', verdict: '挂了读数，但启动以来还没有构建过', sinceNote: '没有读数' };
  }
  // 这个窗口按结束顺序留着，取 `at` 最新的那一条——同一秒的两次尝试也要有确定答案。
  const latest = builds.runs.reduce((newest, run) =>
    Date.parse(run.at) >= Date.parse(newest.at) ? run : newest
  );
  const ageSecs = Math.max(0, (Date.now() - Date.parse(latest.at)) / 1000);
  const seconds = latest.seconds == null ? '没跑，没有耗时' : `${latest.seconds.toFixed(1)} 秒`;
  return {
    ...base,
    state: latest.outcome === 'built' ? 'ok' : 'stuck',
    verdict: `${latest.outcome} · ${seconds}`,
    owner: latest.change_id,
    sinceSecs: Number.isFinite(ageSecs) ? ageSecs : undefined,
    sinceNote: Number.isFinite(ageSecs) ? undefined : '读不出这次构建的结束时刻',
  };
}

function pushNode(line: SourceLineReport | null, error: string | null): ChainNode {
  const base = { id: 'push', stage: '推到 main', evidence: SOURCE_LINE_EVIDENCE };
  if (error) {
    return { ...base, state: 'unknown', verdict: `读不到源线：${error}`, sinceNote: '没有读数' };
  }
  if (!line) return { ...base, state: 'unknown', verdict: '还在读源线', sinceNote: '没有读数' };
  if (line.state !== 'measured') {
    return { ...base, state: 'unknown', verdict: line.why ?? '共享裸仓库读不到', sinceNote: '没有读数' };
  }
  if (!line.main) {
    // 仓库读得到、主线缺席：这是一句「没有推上去过」，不是「读不动」。
    return { ...base, state: 'waiting', verdict: line.why ?? '裸仓库里没有 main 引用', sinceNote: '没有起点' };
  }
  return {
    ...base,
    state: 'ok',
    verdict: `main 在 ${line.main.sha.slice(0, 12)}：${line.main.subject}`,
    owner: 'main',
    sinceSecs: line.main.committed_secs_ago,
  };
}

function deployNode(workloads: WorkloadsReport | null): ChainNode {
  const base = { id: 'deploy', stage: '部署与现网', evidence: 'GET /api/v1/cluster/deployments' };
  if (!workloads) return { ...base, state: 'unknown', verdict: '还在读工作负载', sinceNote: '没有读数' };
  if (workloads.error) return { ...base, state: 'unknown', verdict: workloads.error, sinceNote: '没有读数' };
  const rolling = workloads.workloads.filter((w) => w.desired != null);
  // 判据与页面顶部「已滚到位」那一格是同一个函数：两个数字隔着半屏写不同的口径，
  // 读的人第一眼就会去查哪一边错了。
  const rolled = rolling.filter(isRolled);
  if (rolling.length === 0) {
    return { ...base, state: 'unknown', verdict: '集群里没有带副本的工作负载', sinceNote: '没有读数' };
  }
  const short = rolling.filter((w) => !isRolled(w));
  const allRolled = short.length === 0;
  const moment = chainMoment(allRolled ? rolling : short, allRolled);
  return {
    ...base,
    state: allRolled ? 'ok' : 'waiting',
    verdict: allRolled
      ? `${rolled.length} / ${rolling.length} 已滚到位`
      : `${rolled.length} / ${rolling.length} 已滚到位，差 ${short.length} 个`,
    owner: allRolled
      ? undefined
      : short
          .slice(0, 3)
          .map((w) => `${w.namespace}/${w.name}`)
          .join(' · ') + (short.length > 3 ? ` 等 ${short.length} 个` : ''),
    sinceSecs: moment.secs,
    sinceNote: moment.note,
  };
}

/**
 * 这一步从什么时候起是现在这个样子。
 *
 * 全都到位时取「最后一个到位的那一刻」——全体到位这句话从此刻起才成立；没到位时取
 * 「最早那一处卡住的那一刻」——至少从那时起就已经不齐了。两处都只用工作负载自己的
 * 条件时间，不拿采集时刻冒充：那会让一个停了三小时的滚动看起来刚开始。
 */
function chainMoment(
  workloads: WorkloadView[],
  allRolled: boolean
): { secs?: number; note?: string } {
  const moments = workloads
    .map(conditionMoment)
    .filter((at): at is number => at != null);
  if (moments.length === 0) {
    return { note: '这些工作负载的条件没带时间戳，滚动起点没有读数' };
  }
  const moment = allRolled ? Math.max(...moments) : Math.min(...moments);
  return { secs: Math.max(0, (Date.now() - moment) / 1000) };
}

/** 一个工作负载「当前状态」的起点，按语义顺序取第一条带时间的条件。 */
function conditionMoment(workload: WorkloadView): number | null {
  for (const kind of ['Available', 'Progressing', 'MinimumReplicasAvailable']) {
    const condition = workload.conditions.find((c) => c.kind === kind && c.last_transition_at);
    if (condition?.last_transition_at) {
      const at = Date.parse(condition.last_transition_at);
      if (Number.isFinite(at)) return at;
    }
  }
  return null;
}

/**
 * 一个工作负载算不算「滚到位」：就绪副本身数够，且新版本也起够了。
 *
 * 只问就绪会漏掉一种正在滚的形态——旧副本还全在、新副本一个都没起来，`ready` 仍是
 * 满的；那时 `updated` 小于目标才是真话。被页面顶部与链路那一格共用，是为了让同一份
 * 集群读数在两处说出同一个数。
 */
function isRolled(workload: WorkloadView): boolean {
  return (
    (workload.ready ?? 0) >= (workload.desired ?? 0) &&
    (workload.updated == null || workload.updated >= (workload.desired ?? 0))
  );
}

function chainColor(state: ChainNode['state']): string {
  if (state === 'ok') return 'var(--ok)';
  if (state === 'running') return 'var(--warn)';
  if (state === 'stuck') return 'var(--danger)';
  return 'var(--muted)';
}

function chainStateWord(state: ChainNode['state']): string {
  const words: Record<ChainNode['state'], string> = {
    ok: '在',
    running: '在跑',
    waiting: '没到位',
    stuck: '没成',
    unknown: '读不到',
  };
  return words[state];
}

function ChainPanel({ nodes }: { nodes: ChainNode[] }) {
  return (
    <>
      <Table
        columns={[
          {
            key: 'stage',
            title: '环节',
            width: '100px',
            render: (n) => <span className="text-[11px]">{n.stage}</span>,
          },
          {
            key: 'state',
            title: '状态',
            width: '80px',
            render: (n) => (
              <span className="text-[11px]" style={{ color: chainColor(n.state) }}>
                {chainStateWord(n.state)}
              </span>
            ),
          },
          {
            key: 'verdict',
            title: '结论',
            render: (n) => (
              <span className="text-[11px]">
                {n.verdict}
                {n.aside ? (
                  <span className="block text-[10px] text-[var(--muted)]">{n.aside}</span>
                ) : null}
              </span>
            ),
          },
          {
            key: 'owner',
            title: '归属',
            width: '220px',
            render: (n) => (
              <span className="mono text-[11px] text-[var(--muted)]">{n.owner ?? '—'}</span>
            ),
          },
          {
            key: 'since',
            title: '从何时起',
            width: '130px',
            render: (n) => (
              <span className="mono text-[11px] text-[var(--muted)]">
                {n.sinceSecs != null ? `${formatAge(n.sinceSecs)}前` : n.sinceNote ?? '没有读数'}
              </span>
            ),
          },
          {
            key: 'evidence',
            title: '依据',
            width: '260px',
            render: (n) => (
              <span className="mono text-[10px] text-[var(--muted)]">{n.evidence}</span>
            ),
          },
        ]}
        rows={nodes}
        rowKey={(n) => n.id}
      />
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        这条链按「一个字节从哪来」排，不按时间排：最上面是有人在改的那条分支，最下面
        是集群里真的在跑的副本。三处读数各自出自不同的地方，所以三处都可能读不到，而
        「读不到」写在状态列上，不画成 0——这条链唯一的用处就是看出一件事卡在哪一环，
        而一格空着与一格「没人报」在那件事上是两个完全不同的答案。
      </p>
    </>
  );
}

/**
 * 各条工作树分支相对主线的位置。
 *
 * 链路那一格只说「最近动过的是哪一条」；属主真正要看的全貌是「同时有几棵树在动、
 * 各自离 main 多远」。这张表就是那件事，数据与链路同一份读数，不另开接口。
 *
 * 「要变基」不是一个判断，是 `main` 不是这条分支的祖先——那是 git 自己回答的，
 * 页面照抄。缺席（`undefined`）读成「问不出来」，不读成「要变基」：后者会让人去做
 * 一件不必要的事。
 */
function BranchLineTable({
  line,
  error,
  onRetry,
}: {
  line: SourceLineReport | null;
  error: string | null;
  onRetry: () => void;
}) {
  if (error) return <ReadError what="源线" error={error} onRetry={onRetry} />;
  if (!line) return null;
  if (line.state !== 'measured') {
    return <p className="px-4 py-2 text-[11px] text-[var(--warn)]">{line.why ?? '源线读不到'}</p>;
  }
  const branches = line.branches
    .filter((branch) => branch.name !== 'main')
    .sort((a, b) => (a.committed_secs_ago ?? Infinity) - (b.committed_secs_ago ?? Infinity));
  if (branches.length === 0) {
    return <Empty>裸仓库里只有 main：没有别的工作树在它上面。</Empty>;
  }
  return (
    <Table
      columns={[
        {
          key: 'name',
          title: '工作树（分支）',
          width: '170px',
          render: (b) => <span className="mono text-[11px]">{b.name}</span>,
        },
        {
          key: 'tip',
          title: '尖端',
          render: (b) => (
            <span className="text-[11px]">
              <span className="mono text-[var(--muted)]">{b.sha.slice(0, 10)}</span> {b.subject}
            </span>
          ),
        },
        {
          key: 'ahead',
          title: '相对 main',
          width: '150px',
          render: (b) =>
            b.contains_main == null ? (
              <span className="text-[11px] text-[var(--muted)]">问不出来</span>
            ) : b.contains_main ? (
              <span className="mono text-[11px] text-[var(--ok)]">
                {b.ahead_of_main == null ? '含 main' : `+${b.ahead_of_main}`}
              </span>
            ) : (
              <span className="mono text-[11px] text-[var(--warn)]">要先变基（不含 main）</span>
            ),
        },
        {
          key: 'since',
          title: '尖端多久前落',
          width: '120px',
          render: (b) => (
            <span className="mono text-[11px] text-[var(--muted)]">
              {b.committed_secs_ago == null ? '—' : `${formatAge(b.committed_secs_ago)}前`}
            </span>
          ),
        },
      ]}
      rows={branches}
      rowKey={(b) => b.name}
    />
  );
}

/** 条件一条一行。哪一头是坏消息按名字判（见 conditionIsTrouble），不是「True 都好」。 */
function Conditions({ workload }: { workload: WorkloadView }) {
  if (workload.conditions.length === 0) {
    return <span className="text-[11px] text-[var(--muted)]">没有条件</span>;
  }
  return (
    <span className="flex flex-wrap gap-x-3 gap-y-0.5">
      {workload.conditions.map((condition) => (
        <span key={condition.kind} className="text-[11px]">
          <span
            className="mr-1 inline-block h-1.5 w-1.5 rounded-full align-middle"
            style={{ background: conditionIsTrouble(condition) ? 'var(--danger)' : 'var(--ok)' }}
          />
          <span className="mono">{condition.kind}</span>
          {conditionIsTrouble(condition) && condition.reason && (
            <span className="ml-1 text-[var(--danger)]">{condition.reason}</span>
          )}
        </span>
      ))}
    </span>
  );
}
