import { useCallback, useEffect, useState } from 'react';
import { listAuditEvents, type AuditEvent } from '@/api/client';
import { useStreamDrivenRefresh } from '@/hooks/useStreamDrivenRefresh';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { BarList } from '@/ui/plots';

/**
 * 审计面：归档表里都留了什么。
 *
 * 它回答的是事后的问题——「那一刻总线上到底来过什么」「是谁触发的」。这一页只
 * 读，没有编辑、没有删除：一张能被这一页改写的审计表不算审计表。额度那一类读数
 * 在设置里（它不属于事后追查），不在这里再拉一遍。
 *
 * 时间窗与条数都由读者选：默认最近一小时、最近 100 条。这个上限是给读者的，
 * 不是给库的——不带窗口的「全部」在归档表大起来之后会变成一次全表拉取，所以
 * 「全部」也带一个条数上限，并在页头写出取到了几条。
 */
const LIMITS = [50, 100, 500] as const;
const WINDOWS: { label: string; hours: number | null }[] = [
  { label: '最近 1 小时', hours: 1 },
  { label: '最近 24 小时', hours: 24 },
  { label: '最近 7 天', hours: 24 * 7 },
  { label: '不限时间', hours: null },
];

function sinceIso(hours: number | null): string | undefined {
  if (hours === null) return undefined;
  return new Date(Date.now() - hours * 3600_000).toISOString();
}

/** 这一次查询到底是什么，写成一行给人看：端点加上真的发出去的那几个参数。 */
function sourceOf(query: { agent_id?: string; trigger_type?: string; since?: string; limit: number }): string {
  const params = new URLSearchParams();
  if (query.agent_id) params.set('agent_id', query.agent_id);
  if (query.trigger_type) params.set('trigger_type', query.trigger_type);
  if (query.since) params.set('since', query.since);
  params.set('limit', String(query.limit));
  return `GET /api/v1/audit/events?${params.toString()}`;
}

function summarise(payload: unknown): string {
  if (payload === null || payload === undefined) return '—';
  if (typeof payload === 'string') return payload;
  try {
    const text = JSON.stringify(payload);
    return text.length > 160 ? `${text.slice(0, 160)}…` : text;
  } catch {
    return String(payload);
  }
}

export function AuditView() {
  const [events, setEvents] = useState<AuditEvent[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [agentId, setAgentId] = useState('');
  const [triggerType, setTriggerType] = useState('');
  const [windowIndex, setWindowIndex] = useState(1);
  const [limit, setLimit] = useState<number>(100);
  const [open, setOpen] = useState<number | null>(null);
  /** 最近一次读成功的时刻。 */
  const [at, setAt] = useState<string | null>(null);
  /**
   * 屏幕上这几行是哪一次查询带回来的。
   *
   * 与输入框分开存：改了筛选却没按「重读」时，框里是新条件、屏上是旧结果。拿框里
   * 的值去说这批行的来路，会把另一份读数的出处贴在这一份上。
   */
  const [applied, setApplied] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    const query = {
      agent_id: agentId.trim() || undefined,
      trigger_type: triggerType.trim() || undefined,
      since: sinceIso(WINDOWS[windowIndex].hours),
      limit,
    };
    try {
      const rows = await listAuditEvents(query);
      setEvents(rows);
      setError(null);
      setAt(new Date().toISOString());
      setApplied(sourceOf(query));
    } catch (e) {
      setError((e as { message?: string }).message ?? '读取失败');
      setEvents(null);
    } finally {
      setLoading(false);
    }
  }, [agentId, triggerType, windowIndex, limit]);

  useEffect(() => {
    void load();
  }, [load]);

  // 这一面答的是「那一刻总线上到底来过什么」，订的是**全部**帧：审计表里多出来的
  // 一行可能来自任何一种变体，按种类挑就等于先把可能漏掉的那几种排除了。全局那
  // 一条信号正好是这个意思。
  useStreamDrivenRefresh(load);

  const rows = events ?? [];
  const triggers = Array.from(new Set(rows.map((row) => row.trigger_type))).sort();
  // 「这一段里哪种事在反复发生」是这一面最常被问的一句，而触发类型在表里是一列
  // 名字：五十行里同一个名字出现十五次，数不出来。只数这一次查询带回来的行。
  const byTrigger = new Map<string, number>();
  for (const row of rows) byTrigger.set(row.trigger_type, (byTrigger.get(row.trigger_type) ?? 0) + 1);

  return (
    <ViewFrame
      title="审计"
      meta={
        events
          ? `${WINDOWS[windowIndex].label} · ${rows.length} 条${rows.length >= limit ? '（到条数上限，可能有更早的）' : ''}`
          : error
            ? '读不到'
            : '读取中'
      }
      actions={
        <Button onClick={() => void load()} disabled={loading}>
          {loading ? '读取中…' : '重读'}
        </Button>
      }
    >
      <Readings
        from={{ what: '审计归档', at }}
        source={applied ?? 'GET /api/v1/audit/events'}
        caveat="这几格只数这一次查询带回来的行：窗口与条数是筛选条件，换一个窗口就是另一批行，不是「更多」。到条数上限时更早的行没读进来。"
      >
      <div className="flex flex-wrap border-b border-[var(--line)] py-2">
        <Reading label="这一段留档" value={rows.length} />
        <Reading label="触发类型" value={triggers.length} note={triggers.slice(0, 3).join(' · ')} />
        <Reading label="带智能体的" value={rows.filter((row) => row.agent_id).length} />
        <Reading label="带任务的" value={rows.filter((row) => row.task_id).length} />
      </div>
      </Readings>

      {rows.length > 0 && (
        <div className="border-b border-[var(--line)]">
          <BarList
            label="这一段里每种触发各留了几条"
            note="只数这一次查询带回来的行，窗口与条数上限之外的不在里面"
            rows={[...byTrigger.entries()]
              .sort((a, b) => b[1] - a[1])
              .map(([trigger, count]) => ({ id: trigger, label: trigger, value: count }))}
          />
        </div>
      )}

      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--line)] px-4 py-2">
        <input
          value={triggerType}
          onChange={(event) => setTriggerType(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') void load();
          }}
          placeholder="触发类型（例如 task.completed）"
          className="mono w-56 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] outline-none focus:border-[var(--brass)]"
          style={{ color: 'var(--paper)' }}
        />
        <input
          value={agentId}
          onChange={(event) => setAgentId(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') void load();
          }}
          placeholder="智能体 ID"
          className="mono w-48 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] outline-none focus:border-[var(--brass)]"
          style={{ color: 'var(--paper)' }}
        />
        <select
          value={windowIndex}
          onChange={(event) => setWindowIndex(Number(event.target.value))}
          className="border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] outline-none"
          style={{ color: 'var(--paper)' }}
        >
          {WINDOWS.map((window, index) => (
            <option key={window.label} value={index}>
              {window.label}
            </option>
          ))}
        </select>
        <select
          value={limit}
          onChange={(event) => setLimit(Number(event.target.value))}
          className="border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] outline-none"
          style={{ color: 'var(--paper)' }}
        >
          {LIMITS.map((value) => (
            <option key={value} value={value}>
              最多 {value} 条
            </option>
          ))}
        </select>
        <span className="text-[11px] text-[var(--muted)]">
          这张表只追加：这一页只读，删不掉也改不了
        </span>
      </div>

      {error ? (
        <ReadError what="审计归档" error={error} onRetry={() => void load()} />
      ) : !events && loading ? (
        <Loading what="审计归档" />
      ) : rows.length === 0 ? (
        <Empty>这一段里没有留档事件。没有事件不等于没有动作——只有 hook 事件进这张表。</Empty>
      ) : (
        <Table<AuditEvent>
          columns={[
            { key: 'at', title: '时间', width: '150px', render: (row) => <Moment iso={row.timestamp} /> },
            {
              key: 'trigger',
              title: '触发',
              width: '200px',
              render: (row) => <span className="mono text-[11px]">{row.trigger_type}</span>,
            },
            {
              key: 'agent',
              title: '智能体',
              width: '150px',
              render: (row) => (
                <span className="mono text-[11px] text-[var(--muted)]">{row.agent_id ?? '—'}</span>
              ),
            },
            {
              key: 'task',
              title: '任务',
              width: '150px',
              render: (row) => (
                <span className="mono text-[11px] text-[var(--muted)]">{row.task_id ?? '—'}</span>
              ),
            },
            {
              key: 'dedup',
              title: '去重键',
              width: '140px',
              render: (row) => (
                <span className="mono text-[11px] text-[var(--muted)]">{row.dedup_key ?? '—'}</span>
              ),
            },
            {
              key: 'payload',
              title: '内容',
              render: (row) => (
                <button
                  type="button"
                  onClick={() => setOpen((current) => (current === row.id ? null : row.id))}
                  className="mono w-full truncate text-left text-[11px] text-[var(--paper-dim)] transition-colors hover:text-[var(--paper)]"
                  title="点一下展开整段内容"
                >
                  {open === row.id ? '' : summarise(row.payload)}
                </button>
              ),
            },
          ]}
          rows={rows}
          rowKey={(row) => String(row.id)}
        />
      )}

      {open !== null && (
        <pre className="mono m-0 overflow-auto border-t border-[var(--line)] bg-[var(--ink-900)] px-4 py-2 text-[11px] text-[var(--paper-dim)]">
          {JSON.stringify(rows.find((row) => row.id === open)?.payload ?? null, null, 2)}
        </pre>
      )}
    </ViewFrame>
  );
}
