import { useMemo } from 'react';
import { listEvents } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { describeEvent } from '@/store/streamStore';
import { normalizeAgentEvent } from '@/transport/websocket';
import type { AgentEvent } from '@/types/events';
import { Empty, Loading, ReadError, Reading, Readings, Table } from '@/ui/primitives';

/**
 * 事件回放板：把历史事件查询接上一个读者。
 *
 * `/api/v1/events` 一直在，但界面上从没有谁读它——通路有了、消费者没有，等于没有。
 *
 * 这一面读的是**有界的**历史：服务端把窗口钉在最近 24 小时、一次最多 1000 条。
 * 所以读满 1000 条时这里必须说「窗口被截断了」，而不是把它当作「一共就这些」。
 * 归一走的是实时流同一个 `normalizeAgentEvent`，两条路不会把同一帧说成两件事。
 */
export function EventsBoard() {
  const page = useResource('历史事件', () => listEvents(), 60_000);

  // 服务端给的是平铺的变体（没有 id 也没有 payload 层），索引当兜底 id。
  const events = useMemo<AgentEvent[]>(() => {
    const raw = page.data ?? [];
    const out: AgentEvent[] = [];
    raw.forEach((item, index) => {
      const event = normalizeAgentEvent(item, `replay-${index}`);
      if (event) out.push(event);
    });
    return out;
  }, [page.data]);

  const dropped = (page.data?.length ?? 0) - events.length;
  const truncated = (page.data?.length ?? 0) >= 1000;

  if (page.error) {
    return <ReadError what="历史事件" error={page.error} onRetry={page.refresh} />;
  }
  if (!page.data) return <Loading what="历史事件" />;
  if (events.length === 0) {
    return (
      <Empty>
        最近 24 小时的窗口里一条事件都没有。这是这一面的窗口说的，不是「从来没发生过」——
        更早的事件要换一段时间去读，而这一面现在还换不了。
      </Empty>
    );
  }

  return (
    <>
      <Readings
        from={page}
        source="GET /api/v1/events（窗口：最近 24 小时，最多 1000 条）"
        caveat={
          truncated
            ? '读满了 1000 条：窗口被截断了，这里的条数不是这段时间的总数。'
            : '窗口是最近 24 小时；条数没有触到 1000 条的上限。'
        }
      >
        <div className="flex flex-wrap py-2">
          <Reading label="读回事件" value={events.length.toLocaleString()} />
          <Reading
            label="窗口"
            value="最近 24 小时"
            note={truncated ? '读满了上限，被截断' : '没触到上限'}
          />
          {dropped > 0 && (
            <Reading
              label="认不出的帧"
              value={dropped}
              tone="warn"
              note="服务端给了、界面的名字表里没有，所以没画出来"
            />
          )}
        </div>
      </Readings>
      <Table
        columns={[
          { key: 'when', title: '时刻', width: '110px', render: (e) => (
            <span className="mono text-[11px] text-[var(--paper-dim)]">
              {new Date(e.timestamp).toLocaleTimeString('zh-CN', { hour12: false })}
            </span>
          ) },
          { key: 'type', title: '类型', width: '190px', render: (e) => (
            <span className="mono text-[11px]">{e.type}</span>
          ) },
          { key: 'what', title: '这一帧在做什么', render: (e) => {
            const activity = describeEvent(e);
            return (
              <span className="text-[11px]" style={{ color: activity.failed ? 'var(--danger)' : 'var(--paper-dim)' }}>
                {activity.summary}
              </span>
            );
          } },
        ]}
        rows={events}
        rowKey={(e) => e.id}
      />
    </>
  );
}
