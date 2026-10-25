import { useState } from 'react';
import { getTrace, listTraces, type AgentTrace, type TraceMeta } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, Table } from '@/ui/primitives';

/**
 * trace 板：列出盘上真的有的执行轨迹，点开一条看它的形状。
 *
 * 这一面不做逐帧回放——拖回任意一帧是时间轴那一面的事（它把每一帧还原成当时的样子）。
 * 这里回答的是「有哪些轨迹、每条多大、由哪些帧组成」：选一条，给出帧数、字节、
 * 时间跨度与帧类型分布。两处各做一套回放，同一帧就会被说成两件事。
 */
export function TracesBoard() {
  const setActivity = useShell((s) => s.setActivity);
  const list = useResource('执行轨迹', () => listTraces(50), 60_000);
  const [selected, setSelected] = useState<string | null>(null);
  const trace = useResource(
    '轨迹内容',
    () => getTrace(selected as string),
    300_000,
    selected != null
  );

  if (list.error) {
    return <ReadError what="执行轨迹" error={list.error} onRetry={list.refresh} />;
  }
  if (!list.data) return <Loading what="执行轨迹" />;

  const traces = list.data;
  if (traces.length === 0) {
    return (
      <Empty>
        轨迹索引是空的。这一面按 50 条上限读最近的那些；空列表是「索引里没有」，
        不是「读失败」（读失败会连同错误一起显示）。
      </Empty>
    );
  }

  const shown: TraceMeta | undefined = traces.find((t) => t.trace_id === selected);

  return (
    <>
      <div className="flex items-center gap-3 px-4 py-2 text-[11px] text-[var(--muted)]">
        <span>最近 {traces.length} 条（按创建时间倒序）</span>
        <Button onClick={list.refresh}>重读</Button>
        <div className="ml-auto">
          <Button tone="accent" onClick={() => setActivity('timeline')}>
            去时间轴逐帧回放
          </Button>
        </div>
      </div>
      <Table
        columns={[
          { key: 'id', title: '轨迹', width: '220px', render: (t) => (
            <button
              type="button"
              onClick={() => setSelected(t.trace_id)}
              className="mono text-[11px] text-left underline decoration-dotted decoration-transparent underline-offset-4 transition-colors hover:text-[var(--paper)] hover:decoration-[var(--muted)]"
              style={{ color: t.trace_id === selected ? 'var(--brass)' : 'var(--paper-dim)' }}
            >
              {t.trace_id}
            </button>
          ) },
          { key: 'agent', title: '智能体', width: '160px', render: (t) => (
            <span className="mono text-[11px] text-[var(--muted)]">{t.agent_id}</span>
          ) },
          { key: 'task', title: '任务', width: '160px', render: (t) => (
            <span className="mono text-[11px] text-[var(--muted)]">{t.task_id}</span>
          ) },
          { key: 'frames', title: '帧', width: '80px', render: (t) => (
            <span className="mono text-[11px]">{t.event_count}</span>
          ) },
          { key: 'tier', title: '档', width: '80px', render: (t) => (
            <span className="text-[11px] text-[var(--muted)]">{t.tier}</span>
          ) },
          { key: 'at', title: '创建', width: '110px', render: (t) => <Moment iso={t.created_at} /> },
        ]}
        rows={traces}
        rowKey={(t) => t.trace_id}
      />
      {shown && (
        <TraceShape
          meta={shown}
          trace={trace.data && trace.data.trace_id === shown.trace_id ? trace.data : null}
          error={trace.error}
          loading={trace.loading}
        />
      )}
    </>
  );
}

/** 一条轨迹的形状：由哪些帧组成、跨多长、有没有失败的帧。 */
function TraceShape({
  meta,
  trace,
  error,
  loading,
}: {
  meta: TraceMeta;
  trace: AgentTrace | null;
  error: string | null;
  loading: boolean;
}) {
  if (error) return <ReadError what={`轨迹 ${meta.trace_id}`} error={error} />;
  if (!trace) {
    return loading ? <Loading what="轨迹内容" /> : <Empty>选一条轨迹看它的形状。</Empty>;
  }

  const counts = new Map<string, number>();
  for (const frame of trace.events) {
    counts.set(frame.type, (counts.get(frame.type) ?? 0) + 1);
  }
  const types = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  const first = trace.events[0]?.timestamp;
  const last = trace.events[trace.events.length - 1]?.timestamp;
  const span =
    first && last
      ? `${Math.max(0, Math.round((new Date(last).getTime() - new Date(first).getTime()) / 1000))}s`
      : '—';

  return (
    <section className="border-t border-[var(--line)]">
      <Readings
        from={{ what: `轨迹 ${trace.trace_id}`, at: trace.created_at }}
        source={`GET /api/v1/traces/${trace.trace_id}`}
        caveat="帧按写入顺序排列；时间跨度是首帧与末帧的差，不是这条任务跑了多久。"
      >
        <div className="flex flex-wrap py-2">
          <Reading label="帧" value={trace.event_count.toLocaleString()} note={`这一次读回 ${trace.events.length} 条`} />
          <Reading label="字节" value={trace.byte_size.toLocaleString()} />
          <Reading label="跨度" value={span} />
          <Reading label="帧类型" value={types.length} />
        </div>
      </Readings>
      <div className="flex flex-wrap gap-x-4 gap-y-1 px-4 pb-3">
        {types.map(([type, count]) => (
          <span key={type} className="mono text-[11px] text-[var(--paper-dim)]">
            {type}
            <span className="ml-1 text-[var(--muted)]">×{count}</span>
          </span>
        ))}
      </div>
      {trace.events.length < trace.event_count && (
        <p className="px-4 pb-3 text-[11px] text-[var(--warn)]">
          这条轨迹说有 {trace.event_count} 帧，这次只读回 {trace.events.length} 帧——上面那份分布只是读回来的那部分。
        </p>
      )}
    </section>
  );
}
