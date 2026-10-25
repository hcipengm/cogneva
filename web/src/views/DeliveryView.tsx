import { getDeployments, type DeploymentView } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, Moment, ReadError, Reading, Table, ViewFrame } from '@/ui/primitives';

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
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">构建与门禁</h2>
        <Empty>
          构建运行与门禁读数还没有服务端来源：这一面先只画部署版本，不画一张没有数据的图。
          等门禁结果落表之后，这里接的是那批行，不是另起一套心跳。
        </Empty>
      </section>
    </ViewFrame>
  );
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
