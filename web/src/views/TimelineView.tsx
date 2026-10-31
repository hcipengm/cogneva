import { useMemo, useState } from 'react';
import { getTrace, listTraces, type AgentTrace, type TraceMeta, type WireFrame } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { describeEvent, type ActivityEntry } from '@/store/streamStore';
import type { AgentEvent } from '@/types/events';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { BarList, Strip, type StripSegment } from '@/ui/plots';
import type { ExplainOrigin } from '@/ui/explain';

import { Note } from '@/ui/Note';
/**
 * 线上的一帧 → 活动流认得的形状。
 *
 * 总线上的帧是 `{id, type, timestamp, payload}`，轨迹里存的是变体字段平铺的同一批
 * 数据：没有 `id`，也没有 `payload` 这一层。这里把两份形状对齐，只是为了让轨迹与
 * 实时流用同一张「这一帧在做什么」的表——两处各写一份的话，同一帧在时间轴上和活动
 * 流里会被说成两件事。
 */
function toActivity(frame: WireFrame, index: number): ActivityEntry {
  const { type, timestamp, ...rest } = frame;
  return describeEvent({
    id: `${index}`,
    type: type as AgentEvent['type'],
    timestamp,
    payload: rest,
  });
}

function clock(iso: string): string {
  const at = new Date(iso);
  return Number.isNaN(at.getTime()) ? iso : at.toLocaleTimeString('zh-CN', { hour12: false });
}

/**
 * 时间轴：把一份执行轨迹拖回任意一帧。
 *
 * 拖的不是动画，是那次执行里真的记下来的帧——每一格都对应盘上的一条，拖到哪儿就
 * 是它当时的样子。分不出「当时什么都没发生」与「这一格没记下来」的地方，这里不
 * 画点，因为画上去的那一格就是在替数据说话。
 */
function TraceScrubber({ trace, origin }: { trace: AgentTrace; origin: ExplainOrigin }) {
  const frames = trace.events;
  const [cursor, setCursor] = useState(frames.length > 0 ? frames.length - 1 : 0);

  const rows = useMemo(
    () => frames.slice(0, cursor + 1).map((frame, index) => toActivity(frame, index)),
    [frames, cursor]
  );
  const current = rows[rows.length - 1];
  const failedSoFar = rows.filter((row) => row.failed).length;
  // 一趟执行的「形状」是它由哪几种帧组成：只有 message_update 的一趟是在说话，
  // 一堆 tool_call 的一趟在干活。逐行读能看出顺序，看不出构成。
  const byType = new Map<string, number>();
  for (const row of rows) byType.set(row.type, (byType.get(row.type) ?? 0) + 1);
  const first = frames[0]?.timestamp;
  const now = frames[Math.min(cursor, frames.length - 1)]?.timestamp;

  if (frames.length === 0) {
    return (
      <Empty>
        这份轨迹里没有帧。它可能只记下了元数据：事件数为 {trace.event_count}，而正文是空的——
        这不是「当时什么都没发生」。
      </Empty>
    );
  }

  return (
    <div>
      <div className="flex items-baseline gap-3 px-4 py-2">
        <span className="mono text-[11px] text-[var(--paper-dim)]">{trace.trace_id}</span>
        <span className="text-[11px] text-[var(--muted)]">
          {trace.agent_id || '—'} · 任务 {trace.task_id || '—'} · {trace.event_count} 帧 ·{' '}
          {trace.byte_size} 字节
        </span>
        <button
          onClick={() => setCursor(frames.length - 1)}
          className="ml-auto text-[11px] text-[var(--muted)] hover:text-[var(--paper)]"
        >
          回到最后一帧
        </button>
      </div>
      <Readings from={origin} source="GET /api/v1/traces/{id} 读回来的那一份轨迹" caveat="这条读数是本地拖动状态：位置与「当时」随游标变，不代表平台此刻的样子。">
      <div className="flex flex-wrap py-2">
        <Reading label="拖到的位置" value={`${cursor + 1} / ${frames.length}`} />
        <Reading label="当时" value={now ? clock(now) : '—'} note={first ? `起点 ${clock(first)}` : undefined} />
        <Reading
          label="到此刻为止的失败帧"
          value={failedSoFar}
          tone={failedSoFar > 0 ? 'warn' : 'plain'}
        />
        <Reading label="校验和" value={<span className="text-[11px]">{trace.checksum ? '在' : '缺'}</span>} note={trace.version} />
      </div>
      </Readings>
      <div className="px-4 pb-2">
        <input
          type="range"
          min={0}
          max={frames.length - 1}
          value={Math.min(cursor, frames.length - 1)}
          onChange={(e) => setCursor(Number(e.target.value))}
          className="w-full accent-[var(--brass)]"
          aria-label="拖动到某一帧"
        />
        <div className="flex justify-between text-[11px] text-[var(--muted)]">
          <span className="mono">{first ? clock(first) : '—'}</span>
          <span className="mono">{clock(frames[frames.length - 1].timestamp)}</span>
        </div>
      </div>
      {current && (
        <div className="mx-4 mb-2 border border-[var(--line)] p-2">
          <div className="text-[11px] text-[var(--muted)]">这一帧</div>
          <div className="mt-1 flex items-baseline gap-2 text-[12px]">
            <span className="mono text-[11px] text-[var(--muted)]">{clock(current.at)}</span>
            <span className={current.failed ? 'text-[var(--danger)]' : 'text-[var(--paper)]'}>
              {current.summary}
            </span>
            <span className="mono text-[11px] text-[var(--muted)]">{current.type}</span>
          </div>
          {current.detail && (
            <pre className="mono mt-1 max-h-32 overflow-auto text-[11px] whitespace-pre-wrap text-[var(--paper-dim)]">
              {current.detail}
            </pre>
          )}
        </div>
      )}
      <div className="border-t border-[var(--line)]">
        <Strip
          label="拖到的这一格为止，这一趟由哪几种帧组成"
          note="只数拖到的位置之前那些帧；颜色不表示好坏，这段是构成不是判决"
          segments={[...byType.entries()]
            .sort((a, b) => b[1] - a[1])
            .map(([type, count]): StripSegment => ({ label: type, value: count, tone: 'magnitude' }))}
        />
      </div>
      <div className="max-h-72 overflow-auto border-t border-[var(--line)]">
        <ol className="px-4 py-2">
          {rows.map((row, index) => (
            <li
              key={`${row.id}-${index}`}
              className={`flex items-baseline gap-2 py-0.5 text-[11px] ${
                index === rows.length - 1 ? 'text-[var(--paper)]' : 'text-[var(--muted)]'
              }`}
            >
              <span className="mono w-20 shrink-0">{clock(row.at)}</span>
              <Note className="mono w-40 shrink-0 truncate" text={row.type}>
                {row.type}
              </Note>
              <span className={row.failed ? 'text-[var(--danger)]' : ''}>{row.summary}</span>
            </li>
          ))}
        </ol>
      </div>
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        这一列帧就是盘上那一份的全部：没有补帧、没有跳过。拖到某一格看到的是「到这里为止发生过什么」，
        不是平台此刻的状态。
      </p>
    </div>
  );
}

/**
 * 时间轴面：谁跑过一趟，那一趟里一步一步做了什么。
 *
 * 与活动流不是同一份东西：活动流是这一台浏览器连上之后收到的帧，刷新就没了；轨迹是
 * 采集器边跑边落盘的，重启、换一台机器都还在。要回答「昨天那次为什么失败」，只有后者答得了。
 */
export function TimelineView() {
  const traces = useResource('执行轨迹', () => listTraces(50), 60_000);
  const [picked, setPicked] = useState<TraceMeta | null>(null);
  // 没选的时候不读：这条读面按名字重开，选中的轨迹名一变就重新读它那一份。
  const trace = useResource<AgentTrace | null>(
    picked ? `轨迹 ${picked.trace_id}` : '轨迹（未选）',
    async () => (picked ? getTrace(picked.trace_id) : null),
    300_000,
    picked !== null
  );

  // 一份轨迹是一次运行跑完之后落盘的：总线上那次运行结束的帧就是它的「到货通知」。
  // 不订 `usage` 也不订 `alerts`——它们启动不了任何一次新的轨迹。
  useKindTickedRefresh(
    ['agents', 'tasks', resourceKey('traces')],
    traces.refresh
  );

  const rows = traces.data ?? [];

  return (
    <ViewFrame
      title="时间轴"
      meta={traces.data ? `${rows.length} 份轨迹 · 按 60 秒重读` : '采集器落盘的执行轨迹 · 按 60 秒重读'}
    >
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">执行轨迹</h2>
        {traces.error ? (
          <ReadError what="执行轨迹" error={traces.error} onRetry={traces.refresh} />
        ) : !traces.data && traces.loading ? (
          <Loading what="执行轨迹" />
        ) : rows.length === 0 ? (
          <Empty>
            没有读到任何轨迹。读不到与「没跑过」是两件事：这一屏能画出这一段，说明读是通的，
            空着只有一种解释——采集器还没把哪一次执行落进轨迹存储。
          </Empty>
        ) : (
          <Table
            columns={[
              { key: 'id', title: '轨迹', width: '230px', render: (t) => (
                <Note
                  as="button"
                  onClick={() => setPicked(picked?.trace_id === t.trace_id ? null : t)}
                  className="mono block w-full truncate text-left text-[11px] text-[var(--paper-dim)] underline-offset-2 hover:underline"
                  text={t.trace_id}
                >
                  {t.trace_id}
                </Note>
              ) },
              { key: 'agent', title: '智能体', width: '150px', render: (t) => (
                <span className="mono truncate text-[11px]">{t.agent_id || '—'}</span>
              ) },
              { key: 'task', title: '任务', width: '150px', render: (t) => (
                <span className="mono truncate text-[11px]">{t.task_id || '—'}</span>
              ) },
              { key: 'frames', title: '帧', width: '70px', render: (t) => (
                <span className="mono text-[11px]">{t.event_count}</span>
              ) },
              { key: 'tier', title: '层', width: '70px', render: (t) => (
                <span className="mono text-[11px]">{t.tier}</span>
              ) },
              { key: 'at', title: '采集于', width: '110px', render: (t) => <Moment iso={t.created_at} /> },
            ]}
            rows={rows}
            rowKey={(t) => t.trace_id}
          />
        )}
        {rows.length > 0 && (
          <BarList
            label="每份轨迹记下了多少帧"
            note="帧数来自列表这一页（GET /api/v1/traces）；帧多的那一趟不一定跑得久，但一定更值得点开"
            rows={rows
              .map((t) => ({ id: t.trace_id, label: t.trace_id, value: t.event_count }))
              .sort((a, b) => b.value - a.value)}
          />
        )}
      </section>
      {picked && (
        <section className="border-b border-[var(--line)]">
          {trace.error ? (
            <ReadError what="轨迹" error={trace.error} onRetry={trace.refresh} />
          ) : !trace.data && trace.loading ? (
            <Loading what="轨迹" />
          ) : trace.data ? (
            <TraceScrubber trace={trace.data} origin={trace} />
          ) : null}
        </section>
      )}
      <p className="px-4 py-3 text-[11px] text-[var(--muted)]">
        这一面读的是采集器落盘的轨迹（存储层可能是文件也可能是 Redis），不是浏览器收到的流：
        列表最多 50 份，点开的那一份整份读回来再拖。翻更早的：轨迹会按层下移，冷层的读法不在这里。
      </p>
    </ViewFrame>
  );
}
