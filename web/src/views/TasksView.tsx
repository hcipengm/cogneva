import { useState } from 'react';
import { getTaskSummary, listTasks, type TaskRow } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, Moment, ReadError, Reading, Status, Table, ViewFrame } from '@/ui/primitives';

/**
 * 任务面：现在有什么在跑、跑成了什么、卡在哪一步。
 *
 * 先读一次汇总，再按状态过滤列表——汇总里的数是服务端算的，列表是同一个端点
 * 的另一面，两者用同一个过滤词才不会出现「上面写着 12 个失败，下面一条没有」。
 */
const FILTERS = ['全部', 'running', 'ready', 'completed', 'failed'] as const;

export function TasksView() {
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>('全部');
  const summary = useResource('任务汇总', getTaskSummary, 15_000);
  const rows = useResource(
    '任务列表',
    () => listTasks(filter === '全部' ? { limit: 300 } : { status: filter, limit: 300 }),
    15_000
  );

  const tasks = rows.data ?? [];
  const counts = summary.data;

  return (
    <ViewFrame
      title="任务"
      meta={counts ? `共 ${counts.total} · 运行 ${counts.running} · 就绪 ${counts.ready} · 失败 ${counts.failed}` : '按 15 秒重读'}
      actions={
        <div className="flex gap-1">
          {FILTERS.map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className="border px-2 py-1 text-[11px]"
              style={{
                borderColor: filter === f ? 'var(--brass-dim)' : 'var(--line)',
                color: filter === f ? 'var(--brass)' : 'var(--paper-dim)',
              }}
            >
              {f}
            </button>
          ))}
        </div>
      }
    >
      {counts && (
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="全部" value={counts.total} />
          <Reading label="运行中" value={counts.running} tone="ok" />
          <Reading label="就绪待领" value={counts.ready} tone={counts.ready > 0 ? 'warn' : 'plain'} />
          <Reading label="已完成" value={counts.completed} />
          <Reading label="失败" value={counts.failed} tone={counts.failed > 0 ? 'danger' : 'plain'} />
        </div>
      )}
      {rows.error ? (
        <ReadError what="任务列表" error={rows.error} onRetry={rows.refresh} />
      ) : !rows.data && rows.loading ? (
        <Loading what="任务列表" />
      ) : tasks.length === 0 ? (
        <Empty>这个过滤条件下没有任务。</Empty>
      ) : (
        <Table<TaskRow>
          columns={[
            { key: 'id', title: 'id', width: '150px', render: (t) => (
              <span className="mono text-[11px] text-[var(--paper-dim)]">{t.id.slice(0, 18)}</span>
            ) },
            { key: 'type', title: '类型', width: '130px', render: (t) => t.task_type },
            { key: 'status', title: '状态', width: '110px', render: (t) => <Status value={t.status} /> },
            { key: 'owner', title: '归属', width: '150px', render: (t) => (
              <span className="mono text-[11px] text-[var(--muted)]">{t.owner_unit ?? t.agent_id ?? '—'}</span>
            ) },
            { key: 'goal', title: '目标', width: '120px', render: (t) => (
              <span className="mono text-[11px] text-[var(--muted)]">{t.goal_id?.slice(0, 12) ?? '—'}</span>
            ) },
            { key: 'error', title: '结局', render: (t) =>
              t.error ? (
                <span className="text-[11px] text-[var(--danger)]">{t.error}</span>
              ) : (
                <span className="text-[11px] text-[var(--muted)]">—</span>
              ) },
            { key: 'updated', title: '更新', width: '100px', render: (t) => <Moment iso={t.updated_at} /> },
          ]}
          rows={tasks}
          rowKey={(t) => t.id}
        />
      )}
    </ViewFrame>
  );
}
