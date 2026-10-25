import { getBuildRuns, getDeployments, type DeploymentView } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';

/**
 * 构建与部署面：谁在跑第几版、滚动到哪一步。
 *
 * 数据只来自 apiserver 的 Deployment 对象：revision 注解就是「这是第几次改动滚上来
 * 的」，条件里 `Progressing=False` 是滚动卡住而不是进行中。工作负载的名字就是平台
 * 自己的组件名，不另编一套漂亮的名字——运维要能把它和 `kubectl` 的输出对上。
 */
export function DeliveryView() {
  const deployments = useResource('部署版本', getDeployments, 15_000);
  const report = deployments.data;
  const rows = report?.deployments ?? [];

  const builds = useResource('构建跑次', getBuildRuns, 20_000);
  const view = builds.data;
  const runs = view?.runs ?? [];
  // 表要一个稳定 key，而同一变更的两次尝试可能落在同一秒：带上行号，不靠时间戳
  // 自己去区分。
  const runRows = runs.map((run, index) => ({ ...run, key: `${run.change_id}-${run.at}-${index}` }));
  const built = runs.filter((r) => r.outcome === 'built');
  const notBuilt = runs.filter((r) => r.outcome !== 'built');

  const rolled = rows.filter((d) => d.ready_replicas >= d.desired_replicas && d.updated_replicas >= d.desired_replicas);
  const stuck = rows.filter((d) => !d.conditions.every((c) => c.ok));

  return (
    <ViewFrame
      title="构建与部署"
      meta={report ? `命名空间 ${report.namespace || '（不在集群里）'} · ${new Date(report.collected_at).toLocaleTimeString('zh-CN')}` : '按 15 秒重读'}
    >
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">部署版本</h2>
        {deployments.error ? (
          <ReadError what="部署版本" error={deployments.error} onRetry={deployments.refresh} />
        ) : !report ? (
          <Loading what="部署版本" />
        ) : report.error ? (
          <p className="px-4 py-2 text-[11px] text-[var(--warn)]">{report.error}</p>
        ) : rows.length === 0 ? (
          <Empty>这个命名空间里没有 Deployment。</Empty>
        ) : (
          <>
            <Readings from={deployments} source="GET /api/v1/cluster/deployments" caveat="「采集于」是服务端读 apiserver 的时刻；这份读数在两次轮询之间不会自己变。">
            <div className="flex flex-wrap py-2">
              <Reading label="工作负载" value={rows.length} />
              <Reading
                label="已滚到位"
                value={rolled.length}
                tone={rolled.length === rows.length ? 'ok' : 'warn'}
                note={`${rows.length - rolled.length} 个还在滚`}
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
                { key: 'name', title: '工作负载', width: '190px', render: (d) => (
                  <span className="mono text-[11px]">{d.name}</span>
                ) },
                { key: 'revision', title: '版本', width: '70px', render: (d) => (
                  <span className="mono text-[11px]">{d.revision ?? '—'}</span>
                ) },
                { key: 'replicas', title: '就绪 / 目标', width: '110px', render: (d) => (
                  <span className="mono text-[11px]" style={{ color: d.ready_replicas < d.desired_replicas ? 'var(--warn)' : undefined }}>
                    {d.ready_replicas} / {d.desired_replicas}
                  </span>
                ) },
                { key: 'updated', title: '新版本', width: '80px', render: (d) => (
                  <span className="mono text-[11px]" style={{ color: d.updated_replicas < d.desired_replicas ? 'var(--warn)' : undefined }}>
                    {d.updated_replicas}
                  </span>
                ) },
                { key: 'available', title: '可用', width: '70px', render: (d) => (
                  <span className="mono text-[11px]">{d.available_replicas}</span>
                ) },
                { key: 'images', title: '镜像', width: '280px', render: (d) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{d.images.join(' · ') || '—'}</span>
                ) },
                { key: 'conditions', title: '条件', render: (d) => <Conditions deployment={d} /> },
              ]}
              rows={rows}
              rowKey={(d) => d.name}
            />
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              版本号是 Deployment 上的 revision 注解，不是发布记录：平台自己的发布清单在
              变更面，两边对同一个 rev 用的是同一个字符串。
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
            这个进程没有挂构建读数：0 行不是「没构建过」，是没人报。上面那张部署版本表
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

/** 行里那一个词的颜色：成了是绿的，被预算杀的是黄的，其余没成的是红的。 */
function outcomeColor(outcome: string): string {
  if (outcome === 'built') return 'var(--ok)';
  if (outcome === 'timed_out') return 'var(--warn)';
  if (outcome === 'unstarted') return 'var(--muted)';
  return 'var(--danger)';
}

/** 条件一条一行。部署的条件与 Pod 不同向：这里 True 都是好消息。 */
function Conditions({ deployment }: { deployment: DeploymentView }) {
  if (deployment.conditions.length === 0) {
    return <span className="text-[11px] text-[var(--muted)]">没有条件</span>;
  }
  return (
    <span className="flex flex-wrap gap-x-3 gap-y-0.5">
      {deployment.conditions.map((condition) => (
        <span key={condition.kind} className="text-[11px]">
          <span
            className="mr-1 inline-block h-1.5 w-1.5 rounded-full align-middle"
            style={{ background: condition.ok ? 'var(--ok)' : 'var(--danger)' }}
          />
          <span className="mono">{condition.kind}</span>
          {!condition.ok && condition.reason && (
            <span className="ml-1 text-[var(--danger)]">{condition.reason}</span>
          )}
        </span>
      ))}
    </span>
  );
}
