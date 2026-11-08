/*
 * 工作台：四格与一条状态灯。
 *
 * 四格答四个问题：花了多少（投入）、换回什么（产出）、会干什么（能力）、为谁做
 * （项目）。任务不单独占格——每个任务都恰好落在一个项目桶里，项目是容器、任务是
 * 明细；两格并列时印的是同一批数的两种切法，读的人会在两处看到「在跑 3」却看不出
 * 它们是同一件事。
 *
 * **版式意图在数据里。** 每格带 `priority` 与三档 `span`，这一层只做「格 → 网格
 * 位置」的确定性映射，不跑「找最短列」那种由客户端事后发明版式的事——版式一旦在
 * 这里被发明，后端说要放大的那一格就放不大，而没人看得出是谁改的。
 *
 * **三态**：`measured` 印值；`not_wired` 与 `unread` 印破折号并说原因。0 是一个
 * 读数，把「这一条还没接上」印成 0，读者会以为量过了——两者要的动作相反。
 *
 * 画面：这一面本身就是画面——四格是一张仪表盘，一格一个主数加几行分项。四格的
 * 单位各不相同（钱、条、项、个），画成条形只会把同一个数摊成更占地方的字。
 */
import {
  getWorkbench,
  type WorkbenchCell,
  type WorkbenchCellId,
  type WorkbenchLamp,
  type WorkbenchPage,
  type WorkbenchReading,
  type WorkbenchRow,
  type WorkbenchSpan,
  type WorkbenchValue,
} from '@/api/client';
import { useBreakpoint, type Breakpoint } from '@/hooks/useBreakpoint';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { Icon, type IconName } from '@/shell/icons';
import { useShell, type ActivityId } from '@/shell/store';
import { Button } from '@/ui/primitives';
import { Note } from '@/ui/Note';
import { resourceKey } from '@/store/frameTick';

/**
 * 每一格点进去落在哪一面由数据里的 `href` 说，图标反过来是界面自己的事：它是
 * 这一格的标记，不是数据。
 */
const CELL_ICONS: Record<WorkbenchCellId, IconName> = {
  invest: 'token',
  output: 'changes',
  capability: 'governance',
  project: 'projects',
};

/** 三个断点各用哪一档跨度。断点是界面的，跨度数是数据的。 */
function spanColumns(span: WorkbenchSpan, tier: Breakpoint): string {
  return `span ${span[tier]}`;
}

function symbolOf(currency: string): string {
  if (currency === 'CNY') return '¥';
  if (currency === 'USD') return '$';
  return `${currency} `;
}

/** 微元折成人读的数：钱少时留两位，多了就不留——一格宽只有这么点。 */
function amount(micros: number): string {
  const v = micros / 1_000_000;
  if (v >= 10_000) return `${(v / 1_000).toFixed(1)}k`;
  if (v >= 100) return v.toFixed(0);
  return v.toFixed(2);
}

/** 一个数怎么写。单位在这一层只说「小时」——其余的名词由行的标签带着。 */
function valueText(value: WorkbenchValue): string {
  if (value.kind === 'money') {
    const lines = value.totals.by_currency.map(
      (line) => `${symbolOf(line.currency)}${amount(line.cost_micros)}`
    );
    // 一笔算过价的都没有时，币种一行也没有。印 0 而不编一个币种符号：这一窗
    // 的钱确实是零，但说它是「¥0.00」就替读者假定了一个币种。
    return lines.length > 0 ? lines.join(' · ') : '0';
  }
  if (value.unit === 'hours') return `${value.n} 小时`;
  return String(value.n);
}

/** 一份读数怎么写：量到了写值，没量到写破折号。 */
function readingText(reading: WorkbenchReading): string {
  return reading.state === 'measured' && reading.value ? valueText(reading.value) : '—';
}

/** 量到了但带限定语时，那句话要跟在值旁边，不能藏在别处。 */
function qualifierOf(reading: WorkbenchReading): string | null {
  if (!reading.because) return null;
  return reading.because;
}

function ReadingLine({ reading, strong }: { reading: WorkbenchReading; strong?: boolean }) {
  const text = readingText(reading);
  const because = qualifierOf(reading);
  const unread = reading.state !== 'measured';
  return (
    <span className="flex min-w-0 flex-col">
      <span
        className={`mono ${strong ? 'text-[22px] leading-tight' : 'text-[12px]'} ${
          unread ? 'text-[var(--muted)]' : ''
        }`}
      >
        {text}
      </span>
      {because && (
        <Note className="truncate text-[11px] text-[var(--paper-dim)]" text={because}>
          {because}
        </Note>
      )}
    </span>
  );
}

function RowLine({ row }: { row: WorkbenchRow }) {
  return (
    <div className="flex items-baseline justify-between gap-2">
      <span className={`shrink-0 text-[11px] ${row.warn ? 'text-[var(--warn)]' : 'text-[var(--muted)]'}`}>
        {row.label}
      </span>
      <ReadingLine reading={row.reading} />
    </div>
  );
}

function CellCard({ cell, tier }: { cell: WorkbenchCell; tier: Breakpoint }) {
  const setActivity = useShell((s) => s.setActivity);
  return (
    <button
      type="button"
      onClick={() => setActivity(cell.href as ActivityId)}
      style={{ gridColumn: spanColumns(cell.span, tier) }}
      className="group flex min-w-0 flex-col items-start gap-2 border border-[var(--line)] bg-[var(--ink-800)] p-3 text-left transition-colors hover:border-[var(--brass)]"
    >
      <span className="flex w-full items-center gap-2">
        <Icon name={CELL_ICONS[cell.id]} size={13} />
        <span className="text-[12px] text-[var(--paper)]">{cell.title}</span>
        <span className="ml-auto text-[11px] text-[var(--muted)] group-hover:text-[var(--brass)]">
          打开 ›
        </span>
      </span>
      <ReadingLine reading={cell.main} strong />
      <span className="flex w-full flex-col gap-1">
        {cell.rows.map((row) => (
          <RowLine key={row.label} row={row} />
        ))}
      </span>
    </button>
  );
}

/**
 * 灯亮不亮由数决定：量到了、且不为零，才亮。没量到的灯不亮——它亮起来会说
 * 「这里有 N 件事等你」，而那句话正是它没法说的。
 */
function measuredCount(reading: WorkbenchReading): number | null {
  if (reading.state !== 'measured' || !reading.value) return null;
  return reading.value.kind === 'number' ? reading.value.n : null;
}

/** 状态灯的一段：读的是「该处理了没有」，所以点得进去。 */
function LampSegment({ lamp }: { lamp: WorkbenchLamp }) {
  const setActivity = useShell((s) => s.setActivity);
  const n = measuredCount(lamp.reading);
  const lit = n !== null && n > 0;
  return (
    <Note
      as="button"
      type="button"
      onClick={() => setActivity(lamp.href as ActivityId)}
      text={lamp.reading.because}
      className={`shrink-0 px-1 text-[11px] transition-colors hover:text-[var(--brass)] ${
        lit ? 'text-[var(--warn)]' : 'text-[var(--muted)]'
      }`}
    >
      {lamp.label} {readingText(lamp.reading)}
    </Note>
  );
}

function Lamps({ lamps }: { lamps: WorkbenchLamp[] }) {
  return (
    <div className="flex flex-wrap items-center gap-2 border-y border-[var(--line)] bg-[var(--ink-800)] px-3 py-1">
      {lamps.map((lamp, index) => (
        <span key={lamp.id} className="flex items-center gap-2">
          {index > 0 && <span className="text-[11px] text-[var(--muted)]">·</span>}
          <LampSegment lamp={lamp} />
        </span>
      ))}
    </div>
  );
}

function Board({ page }: { page: WorkbenchPage }) {
  // 顺序是数据说的（`priority`），不是数组下标说的：这一层排一次序，后端调次第
  // 就跟着动，不用改界面。
  const cells = [...page.cells].sort((a, b) => a.priority - b.priority);
  const tier = useBreakpoint();
  return (
    <div className="flex flex-col gap-2 p-3">
      <Lamps lamps={page.lamps} />
      <div className="grid grid-cols-12 gap-2" role="group" aria-label="工作台四格">
        {cells.map((cell) => (
          <CellCard key={cell.id} cell={cell} tier={tier} />
        ))}
      </div>
      <span className="text-[11px] text-[var(--muted)]">
        {page.generated_at} 这一刻的四格 · 投入与产出对账的窗口 {page.window_hours} 小时
      </span>
    </div>
  );
}

export function WorkbenchView() {
  const workbench = useResource('工作台', getWorkbench, 30_000);
  // 四格全都落在任务图上：任务换态、变更落定都会让某一格变。订这两类帧，剩下的
  // 靠兜底定时器——花销与插件清单慢，跟着别人的节拍重读只是白读。
  useKindTickedRefresh(['tasks', 'changes', resourceKey('tasks')], workbench.refresh);

  return (
    <div className="flex h-full flex-col overflow-auto">
      <header className="flex shrink-0 items-baseline gap-3 px-4 py-2">
        <h1 className="text-[15px] font-medium tracking-tight">工作台</h1>
        <span className="text-[11px] text-[var(--muted)]">
          投入 · 产出 · 能力 · 项目 —— 会话在员工页
        </span>
        <div className="ml-auto flex items-center gap-2">
          <Button onClick={workbench.refresh}>重读</Button>
        </div>
      </header>

      {workbench.error ? (
        <div className="p-4 text-[12px] text-[var(--danger)]">
          读不到工作台：{workbench.error}
        </div>
      ) : workbench.data === null ? (
        <div className="p-4 text-[12px] text-[var(--muted)]">读取中…</div>
      ) : (
        <Board page={workbench.data} />
      )}
    </div>
  );
}
