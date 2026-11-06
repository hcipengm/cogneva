/*
 * 工作台顶上那一条「人 · 场 · 任务」。
 *
 * 工作台的正文是几段对话，一眼看到的是「正在说什么」；而这三样说的是「这个平台
 * 此刻长什么样」——谁在岗、活在哪个场地、手上有多少件。它们原来只躺在左侧那列
 * 图标后面，而图标没有字：认不出来的入口等于没有入口。
 *
 * 每一格只读「有多少」和「头几个叫什么」，点一下就走到那一面看全文。三份读数
 * 各自带读失败与没读到过，不把失败画成空名单——空名单读起来是「一个人都没有」。
 *
 * 画面：这条带子本身就是画面——三格并排，每格一个数加一行名字，读者一眼看到
 * 平台的规模，点进去才看明细；它不再另画一张分布图。
 *
 * 三份读数都订帧：有人上岗、有任务动了，这一条立刻重读，而不是等 15 秒的兜底
 * 定时器。只留定时器的话，把间隔调成十分钟，帧到了屏幕上也不动。
 */
import { getTaskSummary, getVenues, listAgents } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { Icon, type IconName } from '@/shell/icons';
import { useShell, type ActivityId } from '@/shell/store';
import { Note } from '@/ui/Note';
import { resourceKey } from '@/store/frameTick';

/** 名单里最多念几个名字。再多也读不完，剩下的交给「员工」那一面。 */
const NAMES_SHOWN = 3;

function joined(parts: string[]): string {
  if (parts.length === 0) return '';
  const head = parts.slice(0, NAMES_SHOWN).join(' · ');
  return parts.length > NAMES_SHOWN ? `${head} 等 ${parts.length} 个` : head;
}

function Cell({
  title,
  count,
  unit,
  line,
  icon,
  to,
}: {
  title: string;
  count: string;
  unit: string;
  line: string;
  icon: IconName;
  to: ActivityId;
}) {
  const setActivity = useShell((s) => s.setActivity);
  return (
    <button
      type="button"
      onClick={() => setActivity(to)}
      className="group flex min-w-0 flex-1 flex-col items-start gap-0.5 px-3 py-1.5 text-left transition-colors hover:bg-[var(--ink-700)]"
    >
      <span className="flex w-full items-baseline gap-2 truncate">
        <Icon name={icon} size={13} />
        <span className="text-[11px] text-[var(--muted)]">{title}</span>
        <span className="mono text-[13px]">{count}</span>
        <span className="text-[11px] text-[var(--muted)]">{unit}</span>
        <span className="ml-auto text-[11px] text-[var(--muted)] group-hover:text-[var(--brass)]">
          打开 ›
        </span>
      </span>
      <Note className="w-full truncate text-[11px] text-[var(--paper-dim)]" text={line}>
        {line}
      </Note>
    </button>
  );
}

export function WorkbenchBand() {
  const agents = useResource('在岗名单', listAgents, 15_000);
  const venues = useResource('场地读数', getVenues, 30_000);
  const tasks = useResource('任务总览', getTaskSummary, 15_000);
  // 订的键就是这一条盯的那几件事：有人上岗（`agents`）、有任务动（`tasks`）。
  // 场地那两条读数跟着任务走——一场活挂到哪个场地，就写在任务的读数里。
  useKindTickedRefresh(['agents', resourceKey('agents')], agents.refresh);
  useKindTickedRefresh(['tasks', resourceKey('tasks')], [venues.refresh, tasks.refresh]);

  /** 一份读数的第二行：失败说失败，没读到过说在读，读到了才说内容。 */
  const secondLine = (r: { data: unknown; error: string | null }, ready: string) =>
    r.error ? `读不到：${r.error}` : r.data === null ? '读取中…' : ready;

  const roster = agents.data ?? [];
  const venueRows = venues.data?.venues ?? [];
  const taskCounts = tasks.data;
  /** 还没读到过时写一个破折号：0 是一个读数，「不知道」不是。 */
  const count = (read: boolean, n: number) => (read ? String(n) : '—');

  return (
    <div
      className="flex shrink-0 gap-px border-y border-[var(--line)] bg-[var(--line)]"
      role="group"
      aria-label="人 场 任务"
    >
      <Cell
        title="人"
        count={count(agents.data !== null, roster.length)}
        unit="位在岗"
        icon="agents"
        to="people"
        line={
          roster.length === 0
            ? secondLine(agents, '此刻没有在岗的员工——名单是心跳，30 秒不来就会被摘掉')
            : secondLine(agents, joined(roster.map((a) => `${a.display_name}（${a.role}）`)))
        }
      />
      <Cell
        title="场"
        count={count(venues.data !== null, venueRows.length)}
        unit="个场地"
        icon="workspace"
        to="venues"
        line={
          venueRows.length === 0
            ? secondLine(venues, '还没有活落到任何场地上')
            : secondLine(
                venues,
                // 场地只有几个，全念出来；按待做的多寡排，截断时先丢的是闲的那几个。
                [...venueRows]
                  .sort((a, b) => b.open - a.open)
                  .map((v) => `${v.venue} 待做 ${v.open}`)
                  .join(' · ')
              )
        }
      />
      <Cell
        title="任务"
        count={count(taskCounts !== null, taskCounts?.total ?? 0)}
        unit="件"
        icon="tasks"
        to="tasks"
        line={secondLine(
          tasks,
          `排队 ${taskCounts?.ready ?? 0} · 在跑 ${taskCounts?.running ?? 0} · 完成 ${
            taskCounts?.completed ?? 0
          } · 失败 ${taskCounts?.failed ?? 0}`
        )}
      />
    </div>
  );
}
