import {
  getBuildGate,
  getBuildRuns,
  getObservabilityReadings,
  getSourceLine,
  getWorkloads,
  type BuildGateReport,
  type BuildGateRun,
  type BuildGateStep,
  type BuildRunsView,
  type ObservabilityReading,
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
  // 部署器「此刻被 CI 扣住没有」只写在它自己的三行读数上，工作负载与源线都不知道。
  // 一次取回整张读数表（四十来条序列打 Prometheus），所以按 30 秒取，比槽慢一档：
  // 这一格答的是「为什么还没上线」，一份晚半分钟的答案仍然答对了那一问。
  const observability = useResource('观测读数', getObservabilityReadings, 30_000);
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
    observability: observability.data?.readings ?? null,
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
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          链路 · 编辑到部署 {chain.length} 步
        </h2>
        <StepLane nodes={chain} />
        <FlowSteps nodes={chain} />
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
      <GateRunPanel report={report} />
    </>
  );
}

/**
 * 推之前那道门最近跑过的几跑：每一格多少秒、哪一格红。
 *
 * 槽位那一面回答「现在谁在编译」；这一面回答「刚才那一推慢在哪」。两问不对等——一跑
 * 两小时的门跑完之后，槽位读数恢复成「空着」，从那张表上再也看不出钱花在哪一格。
 *
 * 记录由门自己写进同一个槽目录（`runs/`），所以这里是加读数，不是加通道：宿主进不了
 * 网关的 ingest，但槽目录本来就已经挂进来。
 */
function GateRunPanel({ report }: { report: BuildGateReport }) {
  if (report.runs_why) {
    return <p className="px-4 py-2 text-[11px] text-[var(--warn)]">{report.runs_why}</p>;
  }
  if (report.runs.length === 0) {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        槽目录里还没有门的跑次记录：这一面是后加的，下一次推之前跑门就会有一行。
        没有记录不是「门没跑过」——它只说明这一份记录还没被写过。
      </p>
    );
  }
  // 这一节是「整门」的读数：提交前与推之前那两段各自在链路那一格上，不混进这张表。
  const gateRuns = report.runs.filter((candidate) => candidate.stage === 'gate');
  const run = gateRuns[0];
  if (!run) {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        `runs/` 里有记录，但没有整门的那一份（只有提交前或推之前那两段）：这一节只读整门。
      </p>
    );
  }
  if (run.read_error) {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--warn)]">
        最近一跑（{run.tree}，{formatAge(run.started_secs_ago)}前）的记录读不动：{run.read_error}。
        这不是「这一跑一格都没记」——文件在，读不出内容。读的人与写的人不是同一个 uid：
        写记录的那一侧把权限钉成 0644，正是为了让这里读得到。
      </p>
    );
  }
  const steps = [...run.steps].sort((a, b) => b.seconds - a.seconds);
  const tone = run.red > 0 ? 'warn' : run.unjudged > 0 ? 'warn' : 'ok';
  return (
    <div className="border-t border-[var(--line)]">
      <Readings
        from={{ what: '推之前那道门', at: report.generated_at }}
        source="GET /api/v1/delivery/build-gate（槽目录 runs/）"
        caveat="合计是各格秒数之和，不是这一跑的墙钟时长：格子之间与构建槽排队的时间不在里面。"
      >
        <div className="flex flex-wrap py-2">
          <Reading
            label="这一跑"
            value={formatAge(run.started_secs_ago)}
            tone={tone}
            note={`${run.tree} · ${run.finished ? '已收尾' : `没收尾（最后一次落笔在 ${run.last_write_secs_ago == null ? '读不到' : `${formatAge(run.last_write_secs_ago)}前`}）`}`}
          />
          <Reading label="格数" value={run.steps.length} note="每格一步：编译、测试、每一条脚本判据" />
          <Reading
            label="红 / 没判"
            value={`${run.red} / ${run.unjudged}`}
            tone={run.red > 0 || run.unjudged > 0 ? 'warn' : 'plain'}
            note={run.red > 0 ? '红的那几格在下面表里' : run.unjudged > 0 ? '没进构建槽，不算验过' : '都判过了'}
          />
          <Reading label="合计" value={formatAge(run.total_secs)} note="各格秒数之和" />
        </div>
      </Readings>
      <Table
        columns={[
          {
            key: 'seconds',
            title: '秒',
            width: '80px',
            render: (step) => <span className="mono text-[11px]">{step.seconds}</span>,
          },
          {
            key: 'verdict',
            title: '结局',
            width: '90px',
            render: (step) => (
              <span
                className="mono text-[11px]"
                style={{
                  color:
                    step.verdict === 'RED'
                      ? 'var(--warn)'
                      : step.verdict === 'UNJUDGED'
                        ? 'var(--warn)'
                        : 'var(--ok)',
                }}
              >
                {step.verdict}
              </span>
            ),
          },
          { key: 'what', title: '这一步', render: (step) => step.what },
        ]}
        rows={steps}
        rowKey={(step) => `${step.what}-${step.seconds}`}
      />
      {gateRuns.length > 1 ? (
        <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
          另有一跑整门在盘上（{gateRuns[1].finished ? '上一次收尾的结果' : '开了头没走完'}，
          开跑在 {formatAge(gateRuns[1].started_secs_ago)}前）：一跑是「这一跑到哪儿了」，
          一跑是「上一跑怎么样」，两跑不能合读成一跑。
        </p>
      ) : null}
    </div>
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
 * 一次改动从编辑到现网的七个环节，每格五句话：状态、结论、依据、归属、从何时起。
 *
 * 「提交」与「推前自检」曾经是合在「编辑」那一格里的，理由是读数相同：三件事在同一棵
 * 工作树里、由同一个人在同一段时间里做完。真正的原因其实是当时没有逐阶段的记录——钩子
 * 只往终端上印。记录带上阶段之后，这两步各自有「这一笔被哪一格挡下、花了多少秒」的
 * 读数，拆开才是如实：一次秒级的推前拦截不该被两小时的整门盖住，也不该被编辑那一格
 * 盖住。依据那一列写着每格读的是哪一个接口。
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
  /**
   * 这一格这一跑实际走过的步。清单来自记录本身，不是这一页写死的：钩子可以增删、
   * 整门的格随判据变，页面对流程的增减不欠一次改动。
   */
  steps?: BuildGateStep[];
}

function chainNodes({
  line,
  lineError,
  gate,
  gateError,
  builds,
  workloads,
  observability,
}: {
  line: SourceLineReport | null;
  lineError: string | null;
  gate: BuildGateReport | null;
  gateError: string | null;
  builds: BuildRunsView | null;
  workloads: WorkloadsReport | null;
  observability: ObservabilityReading[] | null;
}): ChainNode[] {
  // 七步就是一条字节走到现网要经过的地方，顺序即发生顺序：编辑、提交前的钩子、整门、
  // 推之前的钩子、推到 main、镜像构建、部署。镜像构建曾经排在「推到 main」前面——那是
  // 这张表自己的顺序错误，读者照着读会把「还没推上去」当成「还没编完」。
  const nodes = [
    editNode(line, lineError),
    commitNode(gate, gateError),
    gateNode(gate, gateError),
    prePushNode(gate, gateError),
    pushNode(line, lineError),
    buildNode(builds, workloads),
    deployNode(workloads, line, observability),
  ];
  // 逐步记录只从记录里取，不为某一格编：写下 `precommit`/`gate`/`prepush` 的那一侧
  // 这一跑记了哪些格，页面就摊哪些格。整门那 196 格每 5 秒随记录重读，流程本身改了
  // （多一份钩子、少一条判据）页面下一次轮询就是新的，不需要改这一页。
  const recorded: Record<string, string> = { commit: 'precommit', gate: 'gate', prepush: 'prepush' };
  return nodes.map((node) => {
    const stage = recorded[node.id];
    if (!stage) return node;
    const run = lastRun(gate, stage);
    return run && !run.read_error ? { ...node, steps: run.steps } : node;
  });
}

/**
 * 交付链那三段共用同一份记录（`runs/`），按阶段取最近的一跑。
 *
 * 取 `runs[0]` 是错的：一段几秒的提交前钩子比两小时的整门更新，整门就再也轮不到
 * 显示出来。
 */
function lastRun(gate: BuildGateReport | null, stage: string): BuildGateRun | undefined {
  return gate?.runs.find((run) => run.stage === stage);
}

/** 三个钩子阶段共用的「读不到」判词：没有记录、读不动、接口本身读不到。 */
function hookRunBlocker(
  base: Pick<ChainNode, 'id' | 'stage' | 'evidence'>,
  run: BuildGateRun | undefined,
  gateError: string | null,
  missing: string
): ChainNode {
  if (gateError) {
    return { ...base, state: 'unknown', verdict: `读不到记录：${gateError}`, sinceNote: '没有读数' };
  }
  if (!run) {
    return { ...base, state: 'unknown', verdict: missing, sinceNote: '没有读数' };
  }
  if (run.read_error) {
    return {
      ...base,
      state: 'unknown',
      verdict: `记录读不动：${run.read_error}`,
      owner: run.tree,
      sinceSecs: run.started_secs_ago,
      sinceNote: '文件在，内容读不出来',
    };
  }
  // 调用方只在上面三种情况下进来；走到这一行说明调用条件被改了。
  return {
    ...base,
    state: 'unknown',
    verdict: '记录读得到了，却走到了「读不到」这一支',
    sinceNote: '没有读数',
  };
}

/**
 * 「提交」那一步：pre-commit 钩子放行还是挡下这一笔。
 *
 * 钩子退出码非零时提交不会发生，而那份记录正是唯一留下「被哪一格挡下」的地方——
 * 终端的滚动缓冲活不过下一次翻页。
 */
function commitNode(gate: BuildGateReport | null, gateError: string | null): ChainNode {
  const base = {
    id: 'commit',
    stage: '提交',
    evidence: 'GET /api/v1/delivery/build-gate（runs/ precommit）',
  };
  const run = lastRun(gate, 'precommit');
  if (!run || gateError || run.read_error) {
    return hookRunBlocker(base, run, gateError, '没有读数：pre-commit 钩子还没往这里写过记录');
  }
  const head = { ...base, owner: run.tree, sinceSecs: run.started_secs_ago };
  if (!run.finished) {
    return {
      ...head,
      state: 'running',
      verdict: `提交前的钩子在跑：已记 ${run.steps.length} 格`,
      sinceNote:
        run.last_write_secs_ago == null
          ? '最后一次落笔的时刻读不到'
          : '最后一格落笔到现在；它可能还在跑，也可能被杀了',
    };
  }
  const reds = run.steps.filter((step) => step.verdict === 'RED');
  if (reds.length > 0) {
    return {
      ...head,
      state: 'stuck',
      verdict: `这一笔提交被挡在 ${reds.map((step) => step.what).join('、')}`,
    };
  }
  return {
    ...head,
    state: 'ok',
    verdict: `提交前的 ${run.steps.length} 格都放行了，合计 ${formatAge(run.total_secs)}`,
  };
}

/**
 * 「推之前」那一步：pre-push 钩子放行还是挡下这一推。
 *
 * 与整门分开是因为问的不是一件事：整门问「这棵树的字节验过没有」（两小时），
 * pre-push 问「这一推带的是不是验过的那份字节」（秒级）。合成一格的话，一次秒级的
 * 拦截会被两小时的那一跑盖住。
 */
function prePushNode(gate: BuildGateReport | null, gateError: string | null): ChainNode {
  const base = {
    id: 'prepush',
    stage: '推之前',
    evidence: 'GET /api/v1/delivery/build-gate（runs/ prepush）',
  };
  const run = lastRun(gate, 'prepush');
  if (!run || gateError || run.read_error) {
    return hookRunBlocker(base, run, gateError, '没有读数：pre-push 钩子还没往这里写过记录');
  }
  const head = { ...base, owner: run.tree, sinceSecs: run.started_secs_ago };
  if (!run.finished) {
    return { ...head, state: 'running', verdict: `推之前的检查在跑：已记 ${run.steps.length} 格` };
  }
  const reds = run.steps.filter((step) => step.verdict === 'RED');
  if (reds.length > 0) {
    return {
      ...head,
      state: 'stuck',
      verdict: `这一推被挡在 ${reds.map((step) => step.what).join('、')}`,
    };
  }
  return {
    ...head,
    state: 'ok',
    verdict: `推之前的 ${run.steps.length} 问都过了，合计 ${formatAge(run.total_secs)}`,
  };
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
  // 「此刻在编辑的那棵树」由读数自己判定：尖端比 main 的尖端更晚的那几条分支。
  // 判据不是「非 main 里最新的那条」——那条可能是一天前分叉、此后一直没动的旧线，
  // 页面会因此给一条死分支安上一个活的归因。判定与依据在同一处（source_line.rs），
  // 这里只渲染。
  const editing = line.editing;
  if (!editing) {
    if (!line.main) {
      // 没有 main 就没有「上一次推进」这个参考点。这一句不能说成「推进之后没有
      // 分支再落笔」——那是拿一个不存在的参考点下的结论。
      return {
        ...base,
        state: 'unknown',
        verdict: line.why ?? '裸仓库里没有 main：没有可比的那一次推进',
        sinceNote: '没有读数',
      };
    }
    // 有推进、但推进之后无人落笔。
    const newest = line.branches
      .filter((branch) => branch.name !== 'main')
      .sort((a, b) => (a.committed_secs_ago ?? Infinity) - (b.committed_secs_ago ?? Infinity))[0];
    if (!newest) {
      return {
        ...base,
        state: 'ok',
        verdict: '共享裸仓库里没有工作树分支：此刻没有树在动',
        sinceNote: '没有起点',
      };
    }
    return {
      ...base,
      state: 'ok',
      verdict: `共享裸仓库里，main 最后一次推进之后没有分支再落笔（最新的一条是 ${newest.name}：${newest.subject}）`,
      owner: newest.name,
      sinceSecs: newest.committed_secs_ago,
    };
  }
  // 「还有几条分支也在动」是这一格的全部用处：属主要的不是一个名字，是
  // 「台上现在有几棵树在动」。一条与三条在结论那一句里必须读得出来。
  const others = line.editing_others > 0 ? `（另有 ${line.editing_others} 条分支也在动）` : '';
  return {
    ...base,
    state: 'ok',
    verdict: `${editing.name} 的尖端：${editing.subject}${others}`,
    owner: editing.name,
    sinceSecs: editing.committed_secs_ago,
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
  // 「槽顺着/占着」答的是「现在有没有人在编译」，不是「刚才那一推花了多少」。门把每格
  // 的秒数写进了同一个目录，所以这一格按记录说话：先看有没有一跑没收尾，再看上一跑
  // 红没红，最后才回落到「槽空着」。
  const run = lastRun(gate, 'gate');
  if (run?.read_error) {
    return {
      ...base,
      state: 'unknown',
      verdict: `最近一跑（${run.tree}）的记录读不动：${run.read_error}`,
      owner: run.tree,
      sinceSecs: run.started_secs_ago,
      sinceNote: '文件在，内容读不出来——不按绿也不按红判',
      aside,
    };
  }
  if (run && !run.finished) {
    const slowest = [...run.steps].sort((a, b) => b.seconds - a.seconds)[0];
    return {
      ...base,
      state: 'running',
      verdict: `一跑还没收尾：${run.steps.length} 格已记下${slowest ? `，最贵的一格 ${slowest.what}（${formatAge(slowest.seconds)}）` : ''}`,
      owner: run.tree,
      sinceSecs: run.last_write_secs_ago,
      sinceNote:
        run.last_write_secs_ago == null
          ? '最后一次落笔的时刻读不到'
          : '最后一格落笔到现在；它可能还在跑，也可能被杀了',
      aside,
    };
  }
  if (run && run.finished && run.red > 0) {
    const reds = run.steps.filter((step) => step.verdict === 'RED');
    return {
      ...base,
      state: 'stuck',
      verdict: `上一跑红了 ${run.red} 格：${reds.map((step) => step.what).join('、')}`,
      owner: run.tree,
      sinceSecs: run.started_secs_ago,
      sinceNote: '开跑到现在',
      aside,
    };
  }
  if (run && run.finished && run.unjudged > 0) {
    return {
      ...base,
      state: 'unknown',
      verdict: `上一跑有 ${run.unjudged} 格没进构建槽、没判：没判过的字节不算验过`,
      owner: run.tree,
      sinceSecs: run.started_secs_ago,
      sinceNote: '开跑到现在',
      aside,
    };
  }
  if (held.length === 0) {
    const lastRun = run
      ? `上一跑（${run.tree}）全绿：${run.steps.length} 格，合计 ${formatAge(run.total_secs)}，最贵一格 ${[...run.steps].sort((a, b) => b.seconds - a.seconds)[0]?.what ?? '读不到'}`
      : undefined;
    return {
      ...base,
      state: 'ok',
      verdict: lastRun ?? `槽空着（${gate.slots.length} 个位子）：没有编译在跑`,
      owner: run?.tree,
      sinceSecs: run?.started_secs_ago,
      sinceNote: run ? '上一跑开跑到现在' : '空着，没有起点',
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

/**
 * 主线部署的 Job：名字就是它要上线的那一版（`cogneva-mainline-<rev>`，前缀由部署器
 * 自己声明）。它做的是「从 main 的这一版编出镜像 → apply → 等 rollout」，所以链路上
 * 「镜像构建」这一格问的就是它。
 */
function mainlineJob(
  workloads: WorkloadView[]
): { rev: string; workload: WorkloadView; at: number | null } | null {
  const jobs = workloads
    .filter((w) => w.kind === 'Job' && w.name.startsWith('cogneva-mainline-'))
    .map((w) => ({
      rev: w.name.slice('cogneva-mainline-'.length),
      workload: w,
      at: conditionMoment(w),
    }));
  if (jobs.length === 0) return null;
  // 按条件时刻取最新；都没带时刻就取第一条并说明先后不可分（下面 sinceNote 交代）。
  jobs.sort((a, b) => (b.at ?? 0) - (a.at ?? 0));
  return jobs[0];
}

function buildNode(builds: BuildRunsView | null, workloads: WorkloadsReport | null): ChainNode {
  const base = { id: 'build', stage: '镜像构建', evidence: 'GET /api/v1/evolution/builds' };
  // 链路上的镜像构建是主线部署那个 Job。演进进程那份窗口记的是**变更级**构建，且随
  // 进程重启清零：2026-11-10 实测它 13:54 重启后一直为空，而同一时间主线 Job 已经把
  // 18c1a02049b0 编出来并滚上了现网。只读那份窗口，这一格就会一直说「启动以来还没
  // 有构建过」——读者据此以为镜像没编，而现网跑着的那一版正是它编的。
  const job = workloads && !workloads.error ? mainlineJob(workloads.workloads) : null;
  const windowAside =
    builds && builds.attached && builds.runs.length > 0
      ? `本轮变更构建 ${builds.runs.length} 次（演进进程窗口，重启清零）`
      : undefined;
  if (job) {
    const failed = job.workload.conditions.some((c) => c.kind === 'Failed' && c.ok);
    const done = job.workload.desired != null && isRolled(job.workload);
    const ageSecs = job.at == null ? undefined : Math.max(0, (Date.now() - job.at) / 1000);
    return {
      ...base,
      evidence: 'GET /api/v1/cluster/deployments（主线部署 Job）',
      state: failed ? 'stuck' : done ? 'ok' : 'running',
      verdict: `主线 Job 构建 ${job.rev.slice(0, 12)}：${failed ? '失败' : done ? '完成' : '在跑'}`,
      owner: `${job.workload.namespace}/${job.workload.name}`,
      sinceSecs: ageSecs,
      sinceNote: ageSecs == null ? '这个 Job 的条件没带时刻，先后分不出' : undefined,
      aside: windowAside,
    };
  }
  if (!builds) return { ...base, state: 'unknown', verdict: '这个进程没挂构建读数', sinceNote: '没有读数' };
  if (!builds.attached) {
    return { ...base, state: 'unknown', verdict: '没挂构建读数：0 行不是「没构建过」，是没人报', sinceNote: '没有读数' };
  }
  if (builds.runs.length === 0) {
    return {
      ...base,
      state: 'unknown',
      verdict: workloads && !workloads.error
        ? '变更构建窗口空着，集群里也没有主线部署 Job：两处都读不到构建'
        : '挂了读数，但启动以来还没有构建过',
      sinceNote: '没有读数',
    };
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
  // 「main 在 X」说的是主线走到哪了，答不了「我的改动进去了没有」。此刻在动的那条
  // 分支还差几笔没进 main，必须写在这一格上：只报 main 的尖端时，这一格在改动还停在
  // 分支上的时候照样报绿，与部署那一格旧的毛病是同一个形状。
  const moving = line.editing;
  const behind =
    moving == null
      ? null
      : moving.contains_main === false
        ? `在动的 ${moving.name} 不含 main：要先变基才推得上`
        : moving.ahead_of_main != null && moving.ahead_of_main > 0
          ? `在动的 ${moving.name} 还差 ${moving.ahead_of_main} 笔没进 main`
          : null;
  return {
    ...base,
    state: behind ? 'waiting' : 'ok',
    verdict: `main 在 ${line.main.sha.slice(0, 12)}：${line.main.subject}`,
    owner: 'main',
    sinceSecs: line.main.committed_secs_ago,
    aside: behind ?? undefined,
  };
}

/**
 * 这个工作负载的镜像跑的是主线哪一版。
 *
 * 标签是部署器自己写的（`MAIN_TAG_PREFIX` + rev，见
 * `crates/cog-reflection/src/mainline_deployer.rs`），所以标签里的那段 sha 是「镜像
 * 从哪个提交编出来的」唯一的读法。Deployment 自己的 `revision` 答不了这一问：那是
 * 「第几次滚动」，升一版加一，与 main 到哪无关。
 *
 * 读不出这个形状就返回 null——`registry:2`、`postgres:16-alpine` 这些不是从 main
 * 编出来的，不该被算进「现网跑的是哪一版」。
 */
function liveMainRev(workload: WorkloadView): string | null {
  for (const image of workload.images) {
    // 标签是最后一个 `/` 之后的那个冒号后面的事；`localhost:30500/x` 里的冒号是
    // 端口，不是标签。
    const colon = image.lastIndexOf(':');
    if (colon <= image.lastIndexOf('/')) continue;
    const tag = image.slice(colon + 1);
    if (tag.startsWith('main-') && tag.length > 'main-'.length) return tag.slice('main-'.length);
  }
  return null;
}

/**
 * 哪些工作负载的镜像代表「现网跑的是哪一版」。
 *
 * 只有长驻、且会被滚动更新的那三种。Job 不算：主线部署本身就是一个 Job，它跑的
 * 镜像恰恰是**要上线的那一版**——拿它当现网，新版本还没起来页面就说「已经上线
 * 了」；2026-11-10 实测集群里同时躺着 `main-1968dbff1b45` 与 `main-a943172fc2bc`
 * 两个 Job 的镜像，混进来就会读成「同一批工作负载跑着三版 main」。裸 Pod 也不算：
 * 它不被滚动，那一行读数属于某一次执行，不属于现网持续状态。
 */
function carriesTheLiveRev(workload: WorkloadView): boolean {
  return (
    workload.kind === 'Deployment' ||
    workload.kind === 'StatefulSet' ||
    workload.kind === 'DaemonSet'
  );
}

/**
 * 部署器此刻是不是被 CI 扣住，扣了多久。
 *
 * 三行读数各有分工，混用会读错：`_active` 答「扣着没有」，`_seconds` 答「这段红持续
 * 了多久」——后者在 hold 结束之后仍然非零（2026-11-02 实测 4802 秒而 hold 早已结束），
 * 拿它判「扣着没有」会把一次过去的事读成正在发生。读不到就返回 null，不读成 0。
 */
function ciHold(
  readings: ObservabilityReading[] | null
): { active: boolean; seconds: number | null } | null {
  if (!readings) return null;
  const active = readings.find((r) => r.id === 'mainline_ci_hold_active');
  if (!active || active.value == null) return null;
  const seconds = readings.find((r) => r.id === 'mainline_ci_hold_seconds');
  return { active: active.value >= 1, seconds: seconds?.value ?? null };
}

function deployNode(
  workloads: WorkloadsReport | null,
  line: SourceLineReport | null,
  observability: ObservabilityReading[] | null
): ChainNode {
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
  const rolloutVerdict = allRolled
    ? `${rolled.length} / ${rolling.length} 已滚到位`
    : `${rolled.length} / ${rolling.length} 已滚到位，差 ${short.length} 个`;
  const stuckOwner =
    short
      .slice(0, 3)
      .map((w) => `${w.namespace}/${w.name}`)
      .join(' · ') + (short.length > 3 ? ` 等 ${short.length} 个` : '');

  // 「滚到位」只说明这一版的副本都起来了，不说明它就是 main 上那一版：main 在上一次
  // 部署之后又推进过时，集群会一直报「N / N 已滚到位」，而新提交一行代码都没上线。
  // 这一格答的是「我的改动上线没有」，所以必须比镜像标签里的 sha。
  const live: { rev: string; names: string[] }[] = [];
  for (const workload of rolling.filter(carriesTheLiveRev)) {
    const rev = liveMainRev(workload);
    if (!rev) continue;
    const name = `${workload.namespace}/${workload.name}`;
    const known = live.find((entry) => entry.rev === rev);
    if (known) known.names.push(name);
    else live.push({ rev, names: [name] });
  }
  const main = line?.state === 'measured' ? line.main ?? null : null;
  if (live.length > 1) {
    return {
      ...base,
      state: 'waiting',
      verdict: `同一批工作负载跑着 ${live.length} 版 main：${live.map((entry) => entry.rev.slice(0, 12)).join(' 与 ')}（滚动中）`,
      owner: live
        .flatMap((entry) => entry.names)
        .slice(0, 3)
        .join(' · '),
      sinceSecs: moment.secs,
      sinceNote: moment.note,
      aside: rolloutVerdict,
    };
  }
  if (live.length === 1 && main && !main.sha.startsWith(live[0].rev)) {
    // 「为什么还没上线」这一问的答案不在工作负载里：部署器 hold 的原因只写在它自己
    // 的那几行读数上。读到了就说读到的那句，读不到就指到那两行，不替它猜。
    const hold = ciHold(observability);
    const why =
      hold == null
        ? '为什么还没上线看观测读数的 mainline_ci_hold_active 与 mainline_ci_hold_seconds'
        : hold.active
          ? `部署器被 CI 扣住${hold.seconds != null ? `，这段红已经 ${formatAge(hold.seconds)}` : ''}：主线在等一个绿 rev`
          : '部署器没有被 CI 扣住：这一版在排队，或者部署器根本没在跑';
    return {
      ...base,
      state: 'waiting',
      verdict: `现网跑的是 ${live[0].rev.slice(0, 12)}，main 已经是 ${main.sha.slice(0, 12)}：这一版还没上线`,
      owner: live[0].names.slice(0, 3).join(' · '),
      // 「从何时起没上线」的起点是 main 那一次推进，不是这次采集：main 停下之后
      // 无论等多久，这个差都不会变。
      sinceSecs: main.committed_secs_ago,
      sinceNote: 'main 上一次推进到现在',
      aside: `${rolloutVerdict}——滚的是上一版；${why}`,
    };
  }
  return {
    ...base,
    state: allRolled ? 'ok' : 'waiting',
    verdict: rolloutVerdict,
    owner: allRolled ? undefined : stuckOwner,
    sinceSecs: moment.secs,
    sinceNote: moment.note,
    // 比不了就直说比不了：没有带 main 标签的镜像时，「都到位了」只是说滚动这一半
    // 结束了，它答不了「上线的是不是 main 那一版」；裸仓库里没有 main 时，连可比
    // 的那一头都没有。
    aside:
      live.length === 0
        ? '没有工作负载的镜像带 main 标签：这一格只报了滚动，没报现网是哪一版'
        : main
          ? undefined
          : '裸仓库里没有 main：现网是哪一版没有可比的那一头',
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

/**
 * 全局那一行：七步各一格，颜色是结局，下面一行是这一格的一句话。
 *
 * 表在下面把每一格的读数摊开；这里先回答「卡在哪一步」——七格里哪一格不是绿的，
 * 一眼就该看出来，不必逐行读结论。
 */
function StepLane({ nodes }: { nodes: ChainNode[] }) {
  return (
    <div className="flex flex-wrap gap-[2px] px-4 py-2">
      {nodes.map((node, index) => (
        <div
          key={node.id}
          className="min-w-[110px] flex-1 rounded-[4px] border border-[var(--line)] px-2 py-1"
          style={{ borderTop: `3px solid ${chainColor(node.state)}` }}
        >
          <div className="text-[10px] text-[var(--muted)]">
            {index + 1} · {node.stage}
            {/* 步数是这一跑记录里的格数，不是常量：钩子增删、判据增减，这里跟着变。 */}
            {node.steps?.length ? ` · ${node.steps.length} 步` : ''}
          </div>
          <div className="text-[11px]" style={{ color: chainColor(node.state) }}>
            {chainStateWord(node.state)}
          </div>
          <div className="truncate text-[10px] text-[var(--muted)]">{node.verdict}</div>
        </div>
      ))}
    </div>
  );
}

interface FlowStepRow {
  key: string;
  order: number;
  stage: string;
  what: string;
  seconds: number | null;
  verdict: string;
}

/** 把每一格的逐步记录按环节顺序摊平；没有逐步记录的环节占一行，说清它只到环节一级。 */
function flowStepRows(nodes: ChainNode[]): FlowStepRow[] {
  const rows: FlowStepRow[] = [];
  for (const node of nodes) {
    const steps = node.steps ?? [];
    if (steps.length === 0) {
      rows.push({
        key: `${node.id}-stage`,
        order: rows.length + 1,
        stage: node.stage,
        what: `这一格只到环节一级：${node.verdict}`,
        seconds: null,
        verdict: '',
      });
      continue;
    }
    for (const [index, step] of steps.entries()) {
      rows.push({
        key: `${node.id}-${index}-${step.what}`,
        order: rows.length + 1,
        stage: node.stage,
        what: step.what,
        seconds: step.seconds,
        verdict: step.verdict,
      });
    }
  }
  return rows;
}

/**
 * 端到端逐步摊开：从编辑到部署，这一轮具体走过哪些步、每步多少秒、结局是什么。
 *
 * 步骤清单不是这一页写死的。写下 `precommit` / `gate` / `prepush` 的那一侧这一跑记了
 * 哪些格，这里就铺哪些格——提交前的钩子可以增删、整门的格随判据变，流程改了页面
 * 不该跟着改一次；轮询（槽那份每 5 秒）把新的一跑取回来，这张表下一个周期就是新的。
 * 没有逐步记录的三格（编辑、推到 main、镜像构建与部署）各占一行并写明它只到环节
 * 一级，而不是让它们从流程里消失。
 */
function FlowSteps({ nodes }: { nodes: ChainNode[] }) {
  const rows = flowStepRows(nodes);
  const verdictColor = (verdict: string) =>
    verdict === 'RED' ? 'var(--danger)' : verdict === 'UNJUDGED' ? 'var(--warn)' : 'var(--ok)';
  return (
    <div className="max-h-[320px] overflow-auto">
      <Table
        columns={[
          {
            key: 'order',
            title: '步',
            width: '48px',
            render: (r) => <span className="mono text-[10px] text-[var(--muted)]">{r.order}</span>,
          },
          {
            key: 'stage',
            title: '环节',
            width: '90px',
            render: (r) => <span className="text-[11px] text-[var(--muted)]">{r.stage}</span>,
          },
          {
            key: 'what',
            title: '步骤',
            render: (r) => <span className="text-[11px]">{r.what}</span>,
          },
          {
            key: 'seconds',
            title: '秒',
            width: '70px',
            render: (r) => (
              <span className="mono text-[11px] text-[var(--paper-dim)]">
                {r.seconds == null ? '—' : r.seconds}
              </span>
            ),
          },
          {
            key: 'verdict',
            title: '结局',
            width: '90px',
            render: (r) =>
              r.verdict ? (
                <span className="mono text-[11px]" style={{ color: verdictColor(r.verdict) }}>
                  {r.verdict}
                </span>
              ) : (
                <span className="text-[10px] text-[var(--muted)]">环节级</span>
              ),
          },
        ]}
        rows={rows}
        rowKey={(r) => r.key}
      />
    </div>
  );
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
        这条链按发生顺序排，不按读数新鲜度排：最上面是有人在改的那条分支，最下面
        是集群里真的在跑的副本。七格各有各的读数出处（源线、三份钩子记录、构建跑次、
        工作负载），所以每一格都可能读不到，而「读不到」写在状态列上，不画成 0——
        这条链唯一的用处就是看出一件事卡在哪一环，而一格空着与一格「没人报」在那件
        事上是两个完全不同的答案。
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
