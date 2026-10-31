import { getBuildRuns, getWorkloads, type WorkloadView } from '@/api/client';
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
  const rolled = rolling.filter(
    (d) =>
      (d.ready ?? 0) >= (d.desired ?? 0) && (d.updated == null || d.updated >= (d.desired ?? 0))
  );
  const stuck = rows.filter((d) => d.conditions.some(conditionIsTrouble));

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
