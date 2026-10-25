import { createContext, useContext, type ReactNode } from 'react';
import {
  openExplain,
  type ExplainFrame,
  type ExplainOrigin,
  type ExplainSubject,
} from '@/ui/explain';

/**
 * 视图里共用的几块形状。放一处，是为了让「读失败」「没有数据」「正在读」在这
 * 一面里**长得一样**：三件事在运维眼里完全不同，混成一片空白是这类界面最常见
 * 的谎。
 */

export function ViewFrame({
  title,
  meta,
  actions,
  children,
}: {
  title: string;
  meta?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="flex h-full flex-col overflow-hidden">
      <header className="flex items-baseline gap-3 border-b border-[var(--line)] px-4 py-2">
        <h1 className="text-[15px] font-medium tracking-tight">{title}</h1>
        {meta && <span className="text-[11px] text-[var(--muted)]">{meta}</span>}
        <div className="ml-auto flex items-center gap-2">{actions}</div>
      </header>
      <div className="min-h-0 flex-1 overflow-auto">{children}</div>
    </div>
  );
}

export function Button({
  children,
  onClick,
  disabled,
  tone = 'plain',
}: {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  tone?: 'plain' | 'accent';
}) {
  const base =
    'px-2 py-1 text-[11px] border transition-colors disabled:opacity-40 disabled:cursor-not-allowed';
  const palette =
    tone === 'accent'
      ? 'border-[var(--brass-dim)] text-[var(--brass)] hover:bg-[var(--ink-700)]'
      : 'border-[var(--line)] text-[var(--paper-dim)] hover:text-[var(--paper)] hover:border-[var(--line-strong)]';
  return (
    <button type="button" className={`${base} ${palette}`} onClick={onClick} disabled={disabled}>
      {children}
    </button>
  );
}

/**
 * 同一格里的一块读数各自的来路来自同一份读数时，写一次就够。
 *
 * 包一层上下文而不是给每一格加参数：一块读数的来路是它上面那份数据带的，不是
 * 这一格自己知道的；逐格抄一遍，抄漏的那格就会安静地没有来路。
 */
const ReadingOrigin = createContext<{
  origin: ExplainOrigin;
  source: string;
  caveat?: string;
} | null>(null);

export function Readings({
  from,
  source,
  caveat,
  children,
}: {
  from: ExplainOrigin;
  source: string;
  /** 这一组数共同的边界（「进程启动算起」「截断前」）；各格也可以自己盖过它。 */
  caveat?: string;
  children: ReactNode;
}) {
  return (
    <ReadingOrigin.Provider value={{ origin: from, source, caveat }}>
      {children}
    </ReadingOrigin.Provider>
  );
}

/** 画出来的值（图形、组件）不是一串数，抽屉里就直说，不去猜它的文本。 */
function shown(value: ReactNode): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number') return String(value);
  return '（这一格是画出来的，不是一串数）';
}

/**
 * 一块读数：上方是值，下方是它是什么。值比标签大，但都在这两行之内。
 *
 * 带得出来路的数字可以点开：读的是什么、什么时候算的、由哪几帧折出来。没有来路
 * 的数字**不做成按钮**——一格点开却只能说「不知道」的控件，比不点更坏。
 */
export function Reading({
  label,
  value,
  tone,
  note,
  from,
  source,
  frames,
  caveat,
}: {
  label: string;
  value: ReactNode;
  tone?: 'ok' | 'warn' | 'danger' | 'plain';
  note?: string;
  /** 这一格自己的读数来路；不给就跟随外层 `Readings`。 */
  from?: ExplainOrigin;
  source?: string;
  /** 这个数由哪几帧算出来的。轮询来的聚合数不传，而不是传个空数组冒充「没有帧」。 */
  frames?: ExplainFrame[];
  caveat?: string;
}) {
  const color =
    tone === 'ok'
      ? 'var(--ok)'
      : tone === 'warn'
        ? 'var(--warn)'
        : tone === 'danger'
          ? 'var(--danger)'
          : 'var(--paper)';
  const group = useContext(ReadingOrigin);
  const origin = from ?? group?.origin ?? null;
  const src = source ?? group?.source ?? origin?.what ?? null;
  const subject: ExplainSubject | null =
    origin || src
      ? {
          label,
          value: shown(value),
          source: src ?? '没有记下来源',
          computedAt: origin?.at ?? null,
          frames,
          caveat: caveat ?? group?.caveat,
        }
      : null;
  return (
    <div className="border-l border-[var(--line)] py-1 pl-3 pr-4">
      <div className="mono text-[17px] leading-tight" style={{ color }}>
        {subject ? (
          <button
            type="button"
            onClick={() => openExplain(subject)}
            title="这个数由什么算出来"
            className="cursor-pointer text-left underline decoration-dotted decoration-transparent underline-offset-4 transition-colors hover:decoration-[var(--muted)]"
          >
            {value}
          </button>
        ) : (
          value
        )}
      </div>
      <div className="text-[11px] text-[var(--muted)]">{label}</div>
      {note && <div className="text-[11px] text-[var(--muted)] opacity-80">{note}</div>}
    </div>
  );
}

/** 读失败：说清是哪一面、错在哪，并给一个重试的落点。 */
export function ReadError({ what, error, onRetry }: { what: string; error: string; onRetry?: () => void }) {
  return (
    <div className="m-4 border border-[color-mix(in_srgb,var(--danger)_45%,transparent)] bg-[color-mix(in_srgb,var(--danger)_8%,transparent)] p-3">
      <div className="text-[12px] text-[var(--danger)]">读不到{what}</div>
      <div className="mono mt-1 text-[11px] text-[var(--paper-dim)]">{error}</div>
      {onRetry && (
        <div className="mt-2">
          <Button onClick={onRetry}>重试</Button>
        </div>
      )}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="px-4 py-6 text-[12px] text-[var(--muted)]">{children}</div>;
}

export function Loading({ what }: { what: string }) {
  return <div className="px-4 py-6 text-[12px] text-[var(--muted)]">正在读{what}…</div>;
}

const TONES: Record<string, string> = {
  firing: 'var(--danger)',
  active: 'var(--live)',
  healthy: 'var(--ok)',
  up: 'var(--ok)',
  running: 'var(--live)',
  ready: 'var(--ok)',
  completed: 'var(--muted)',
  resolved: 'var(--muted)',
  degraded: 'var(--warn)',
  suspect: 'var(--warn)',
  down: 'var(--danger)',
  failed: 'var(--danger)',
  dead: 'var(--danger)',
  unknown: 'var(--muted)',
};

/** 状态词：颜色之外保持文字本身，读的人不必靠色卡猜。 */
export function Status({ value }: { value: string | null | undefined }) {
  if (!value) return <span className="text-[var(--muted)]">—</span>;
  const color = TONES[value.toLowerCase()] ?? 'var(--paper-dim)';
  return (
    <span className="mono text-[11px]" style={{ color }}>
      <span className="dot" style={{ background: color }} />
      {value}
    </span>
  );
}

/** 一张通用的表：列定义与取行分开，读失败与空表由调用方说清。 */
export interface Column<T> {
  key: string;
  title: string;
  width?: string;
  render: (row: T) => ReactNode;
}

export function Table<T>({
  columns,
  rows,
  rowKey,
}: {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
}) {
  return (
    <table className="grid-table">
      <thead>
        <tr>
          {columns.map((c) => (
            <th key={c.key} style={c.width ? { width: c.width } : undefined}>
              {c.title}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={rowKey(row)}>
            {columns.map((c) => (
              <td key={c.key}>{c.render(row)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/**
 * 一个时刻。绝对时间带日期，相对时间放在旁边：只看「3 分钟前」看不出是哪一天
 * 的事，只看绝对时刻又要心算多久之前。
 */
export function Moment({ iso }: { iso: string | null | undefined }) {
  if (!iso) return <span className="text-[var(--muted)]">—</span>;
  const then = new Date(iso);
  if (Number.isNaN(then.getTime())) return <span className="mono text-[11px]">{iso}</span>;
  const seconds = Math.round((Date.now() - then.getTime()) / 1000);
  const relative =
    Math.abs(seconds) < 60
      ? `${seconds} 秒前`
      : Math.abs(seconds) < 3600
        ? `${Math.round(seconds / 60)} 分钟前`
        : Math.abs(seconds) < 86_400
          ? `${Math.round(seconds / 3600)} 小时前`
          : `${Math.round(seconds / 86_400)} 天前`;
  return (
    <span className="mono text-[11px] text-[var(--paper-dim)]" title={then.toLocaleString('zh-CN')}>
      {relative}
    </span>
  );
}
