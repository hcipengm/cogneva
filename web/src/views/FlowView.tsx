import { useMemo } from 'react';
import { getProcessOverview, type GoalGroup, type ProcessTask } from '@/api/client';
import { FlowGraph } from '@/views/FlowGraph';
import { Strip } from '@/ui/plots';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { frameIsNewer, liveIndex, useStreamStore } from '@/store/streamStore';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';

/**
 * 流程面：一次意图从进来、被分解、被调度、被认领到收口，现在停在哪一段。
 *
 * 与任务面不是同一件事：任务面是对象图（谁等谁、谁负责），这一面是时间轴（哪一段
 * 正在跑、积压在哪）。两者读同一批任务行，所以一处红两处红。
 *
 * 「评测」与「落地」不在这张图上：它们的数据源在评测面与变更面，硬塞进这条时间轴
 * 会让一段没有读数的格子看起来像「这一段没活干」。
 */
export function FlowView() {
  const processes = useResource('流程总览', getProcessOverview, 10_000);
  // 这一段一段的计数是从任务行折出来的：任务换态、执行者换态、有人认领，都会让
  // 某一段的格数变。三件事都在总线上，所以不必等 10 秒那一拍。
  useKindTickedRefresh(
    ['tasks', 'agents', 'comms', resourceKey('processes')],
    processes.refresh
  );

  const overview = processes.data;

  const tasks = overview?.goals.flatMap((goal) => goal.tasks) ?? [];

  // 帧补在快照之后：总览带自己的采集时刻，晚于它的帧比整份快照新。分段计数用
  // 补过的状态，否则一条刚被领走的任务在这一面上仍算「就绪待领」，下一面（任务
  // 面）却已经算「执行中」——同一批行两个口径。
  const liveTasks = useStreamStore((s) => s.liveTasks);
  const live = useMemo(() => liveIndex(liveTasks), [liveTasks]);
  const statusOf = (t: ProcessTask): string => {
    const frame = live[t.id];
    return frame && frameIsNewer(frame, overview?.timestamp ?? '') ? frame.status : t.status;
  };
  const fromFrame = tasks.filter((t) => statusOf(t) !== t.status).length;

  const planned = tasks.length;
  const blocked = tasks.filter((t) => t.blocked_by.length > 0 && isOpen(statusOf(t))).length;
  const ready = tasks.filter((t) => t.blocked_by.length === 0 && isOpen(statusOf(t))).length;
  const running = tasks.filter((t) => statusOf(t).toLowerCase() === 'running').length;
  const completed = tasks.filter((t) => statusOf(t).toLowerCase() === 'completed').length;
  const failed = tasks.filter((t) => statusOf(t).toLowerCase() === 'failed').length;

  return (
    <ViewFrame
      title="流程"
      meta={overview ? `${overview.goals.length} 个意图 · ${new Date(overview.timestamp).toLocaleTimeString('zh-CN')}` : '按 10 秒重读'}
    >
      {processes.error ? (
        <ReadError what="流程总览" error={processes.error} onRetry={processes.refresh} />
      ) : !overview ? (
        <Loading what="流程总览" />
      ) : (
        <>
          <section className="border-b border-[var(--line)]">
            <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              一段活走到哪了 · 每一个数都是一行任务
            </h2>
            <Readings
              from={processes}
              source="GET /api/v1/processes：每一段由 goals[].tasks[] 的状态算出来"
              caveat="分段计数用的是这份快照加上比它新的帧；快照的采集时刻在标题上。"
            >
            <div className="flex flex-wrap items-stretch py-2">
              <Reading label="意图（goal）" value={overview.goals.length} />
              <Arrow />
              <Reading label="分解出的原子任务" value={planned} />
              <Arrow />
              <Reading
                label="被依赖挡住"
                value={blocked}
                tone={blocked > 0 ? 'warn' : 'plain'}
                note="等前面的任务完成"
              />
              <Arrow />
              <Reading label="就绪待领" value={ready} tone={ready > 0 ? 'warn' : 'plain'} />
              <Arrow />
              <Reading label="执行中" value={running} tone="ok" />
              <Arrow />
              <Reading label="已收口" value={completed} />
              <Reading label="失败" value={failed} tone={failed > 0 ? 'danger' : 'plain'} />
              <Reading
                label="帧里更新过"
                value={fromFrame}
                caveat="只数比这份快照新的那些帧；它数的是差，不是总量。"
                frames={liveTasks.slice(0, 20).map((t) => ({
                  at: t.at,
                  type: 'task_status_change',
                  summary: `任务 ${t.taskId} → ${t.status}`,
                }))}
                tone={fromFrame > 0 ? 'warn' : 'plain'}
                note="比这份快照新"
              />
              {overview.dlq_len > 0 && (
                <Reading
                  label="死信队列"
                  value={overview.dlq_len}
                  tone="danger"
                  note="重试用尽，没人接"
                />
              )}
            </div>
            </Readings>
            {/* 上面那一排小格是一个漏斗，而漏斗本来是一条：段与段的差就是这一段还
                卡着多少活。分成几段排开，卡在哪儿是第一眼的事。 */}
            {planned > 0 && (
              <Strip
                label="这批原子任务各卡在哪一段"
                note="每一段由这一批任务的状态算（与上面那几格同一份读数）；已收口那一段不是「好」，是「走完了」"
                segments={[
                  { label: '被依赖挡住', value: blocked, tone: 'warn' },
                  { label: '就绪待领', value: ready, tone: 'warn' },
                  { label: '执行中', value: running, tone: 'ok' },
                  { label: '已收口', value: completed, tone: 'muted' },
                  { label: '失败', value: failed, tone: 'danger' },
                ]}
              />
            )}
            <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">
              评测与落地不在这条线上：它们的读数在评测面与变更面。把它们画进这一段，
              会让「没有来源」看起来像「这一段没活」。
            </p>
          </section>

          <section className="border-b border-[var(--line)]">
            <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              流程图 · 谁等谁，现在走到哪一列
            </h2>
            {overview.goals.length === 0 ? (
              <Empty>现在没有目标在跑，所以没有可画的流程。</Empty>
            ) : (
              <FlowGraph goals={overview.goals} statusOf={statusOf} />
            )}
            <p className="px-4 pb-2 text-[10px] text-[var(--muted)]">
              一列是这个任务在依赖链上的深度：它等的那几条里最长的那条再加一。在跑的那一列，
              箭头会往前走、节点会亮；停住的地方完全静止——形状本身就是「卡在哪儿」。
            </p>
          </section>

          <section className="border-b border-[var(--line)]">
            <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              按意图展开 · 分解成了哪些原子任务
            </h2>
            {overview.goals.length === 0 ? (
              <Empty>现在没有目标在跑。这一面读的是任务图，不是一张心跳图。</Empty>
            ) : (
              overview.goals.map((goal) => (
                <GoalBlock key={goal.goal_id} goal={goal} statusOf={statusOf} />
              ))
            )}
          </section>

          <section className="border-b border-[var(--line)]">
            <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              协作 · 小队进度
            </h2>
            {overview.crews.length === 0 ? (
              <Empty>没有小队读数：这一步由协作层上报，没有上报时这里是空的，不是「没有小队」。</Empty>
            ) : (
              <Table
                columns={[
                  { key: 'crew', title: '小队', width: '200px', render: (c) => (
                    <span className="mono text-[11px]">{c.crew_id}</span>
                  ) },
                  { key: 'members', title: '成员', width: '80px', render: (c) => (
                    <span className="mono text-[11px] text-[var(--muted)]">{c.agent_ids.length}</span>
                  ) },
                  { key: 'progress', title: '进度', width: '200px', render: (c) => (
                    <Meter percent={c.progress_pct} label={`${c.completed_tasks}/${c.total_tasks}`} />
                  ) },
                  { key: 'failed', title: '失败', width: '80px', render: (c) => (
                    <span className="mono text-[11px]" style={{ color: c.failed_tasks ? 'var(--danger)' : 'var(--muted)' }}>
                      {c.failed_tasks}
                    </span>
                  ) },
                  { key: 'retry', title: '重试', width: '80px', render: (c) => (
                    <span className="mono text-[11px] text-[var(--muted)]">{c.crew_retry_count}</span>
                  ) },
                  { key: 'updated', title: '更新', render: (c) => <Moment iso={c.updated_at} /> },
                ]}
                rows={overview.crews}
                rowKey={(c) => c.crew_id}
              />
            )}
          </section>

          <section>
            <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              责任人 · 谁在岗、谁卡住、谁在跑什么
            </h2>
            {overview.agents.length === 0 ? (
              <Empty>没有智能体读数。</Empty>
            ) : (
              <Table
                columns={[
                  { key: 'agent', title: '智能体', width: '200px', render: (a) => (
                    <span className="mono text-[11px]">{a.agent_id}</span>
                  ) },
                  { key: 'role', title: '角色', width: '120px', render: (a) => a.role },
                  { key: 'health', title: '在岗', width: '100px', render: (a) => (
                    <span
                      className="text-[11px]"
                      style={{ color: healthColor(a.health) }}
                    >
                      {a.health}
                    </span>
                  ) },
                  { key: 'running', title: '在跑', width: '80px', render: (a) => (
                    <span className="mono text-[11px]">{a.running_tasks}</span>
                  ) },
                  { key: 'load', title: '负载', width: '140px', render: (a) => (
                    <Meter percent={a.load_score == null ? null : a.load_score * 100} label={a.load_score == null ? '' : undefined} />
                  ) },
                  { key: 'beat', title: '最后心跳', width: '130px', render: (a) => <Moment iso={a.last_heartbeat} /> },
                  { key: 'caps', title: '能力', render: (a) => (
                    <span className="text-[11px] text-[var(--muted)]">{a.capabilities.join(' · ') || '—'}</span>
                  ) },
                ]}
                rows={overview.agents}
                rowKey={(a) => a.agent_id}
              />
            )}
          </section>
        </>
      )}
    </ViewFrame>
  );
}

/** 一个意图：进度、计数，以及它分解出的那批原子任务。 */
function GoalBlock({
  goal,
  statusOf,
}: {
  goal: GoalGroup;
  statusOf: (task: ProcessTask) => string;
}) {
  return (
    <div className="border-t border-[var(--line)] first:border-t-0">
      <div className="flex flex-wrap items-center gap-3 px-4 py-1.5">
        <span className="mono text-[11px] text-[var(--paper-dim)]">{goal.goal_id}</span>
        <Meter percent={goal.progress_pct} label={`${goal.completed}/${goal.total}`} />
        <span className="text-[11px] text-[var(--muted)]">
          运行 {goal.running} · 待办 {goal.pending} · 失败 {goal.failed}
        </span>
      </div>
      <Table
        columns={[
          { key: 'id', title: '任务', width: '200px', render: (t) => (
            <span className="mono text-[11px]">{t.id.slice(0, 22)}</span>
          ) },
          { key: 'type', title: '类型', width: '120px', render: (t) => t.task_type },
          { key: 'status', title: '状态', width: '120px', render: (t) => {
            const live = statusOf(t);
            return (
              <span className="inline-flex items-baseline gap-1.5">
                <span className="text-[11px]" style={{ color: statusColor(live) }}>{live}</span>
                {live !== t.status && (
                  <span className="mono text-[10px] text-[var(--brass)]" title="比这份快照新">帧</span>
                )}
              </span>
            );
          } },
          { key: 'agent', title: '执行者', width: '150px', render: (t) => (
            <span className="mono text-[11px] text-[var(--muted)]">{t.agent_id ?? '未认领'}</span>
          ) },
          { key: 'blocked', title: '等谁', width: '160px', render: (t) => (
            <span className="text-[11px]" style={{ color: t.blocked_by.length ? 'var(--warn)' : 'var(--muted)' }}>
              {t.blocked_by.length ? t.blocked_by.map((id) => id.slice(0, 10)).join(' · ') : '—'}
            </span>
          ) },
          { key: 'retry', title: '重试', width: '70px', render: (t) => (
            <span className="mono text-[11px] text-[var(--muted)]">{t.retry_count}/{t.max_retries}</span>
          ) },
          { key: 'error', title: '结局', render: (t) => (
            <span className="text-[11px]" style={{ color: t.error ? 'var(--danger)' : 'var(--muted)' }}>
              {t.error ?? '—'}
            </span>
          ) },
        ]}
        rows={goal.tasks.slice(0, 12)}
        rowKey={(t) => t.id}
      />
      {goal.tasks.length > 12 && (
        <p className="px-4 py-1 text-[10px] text-[var(--muted)]">
          只列了 12 条，还有 {goal.tasks.length - 12} 条——全量在任务面按这个 goal 过滤。
        </p>
      )}
    </div>
  );
}

function Arrow() {
  return <span className="self-center px-1 text-[var(--muted)]">→</span>;
}

/** 只画有数的进度条：没有百分比就不画，空格比一条假进度诚实。 */
function Meter({ percent, label }: { percent: number | null; label?: string }) {
  const value = percent == null ? null : Math.max(0, Math.min(100, Math.round(percent)));
  return (
    <span className="flex items-center gap-2">
      <span className="relative h-1.5 w-24 overflow-hidden bg-[var(--ink-700)]">
        {value != null && (
          <span
            className="absolute inset-y-0 left-0"
            style={{
              width: `${value}%`,
              background: value >= 100 ? 'var(--ok)' : 'var(--brass)',
            }}
          />
        )}
      </span>
      <span className="mono text-[10px] text-[var(--muted)]">
        {value == null ? '无读数' : label ? `${label} · ${value}%` : `${value}%`}
      </span>
    </span>
  );
}

/** 还没走完的状态。判定按状态词，不按「不是 completed」——失败的任务也不是待办。 */
function isOpen(status: string): boolean {
  const value = status.toLowerCase();
  return value !== 'completed' && value !== 'failed';
}

function healthColor(health: string): string {
  switch (health) {
    case 'healthy':
      return 'var(--ok)';
    case 'dead':
      return 'var(--danger)';
    case 'suspect':
    case 'stuck':
      return 'var(--warn)';
    default:
      return 'var(--muted)';
  }
}

function statusColor(status: string): string {
  switch (status.toLowerCase()) {
    case 'running':
      return 'var(--ok)';
    case 'failed':
      return 'var(--danger)';
    case 'completed':
      return 'var(--paper-dim)';
    default:
      return 'var(--warn)';
  }
}
