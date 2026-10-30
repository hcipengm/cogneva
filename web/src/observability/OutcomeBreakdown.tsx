import type { OutcomeLine, TokenOutcomes } from '@/api/client';

/**
 * 同一段窗口里，每一次调用是怎么结束的。
 *
 * 上面几块读的都是「花了多少」。当那个数是 0 的时候——上游熔断、配额用尽、
 * 配速挡住——花销面给不出任何区分：池子空着和池子还活着，在每一个花销读数里
 * 都是同一个 0。这一块读的是逐笔账本的结局列，所以「一次都没成功，全被池级
 * 熔断挡回来」这句能直接读出来，而不是靠人去猜。
 *
 * 分母是这段窗口里的全部尝试，包括根本没发出去的那些：这正是它要回答的问题。
 */
export function OutcomeBreakdown({ data }: { data: TokenOutcomes }) {
  const lines = [...data.outcomes].sort((a, b) => b.calls - a.calls);
  const total = lines.reduce((sum, line) => sum + line.calls, 0);
  if (total === 0) {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        这段窗口里账本上一行都没有：不是「全都成功」，是一次调用都没尝试过。
        账本读的是逐笔记录，所以这个空是真的空。
      </p>
    );
  }
  const ok = lines.filter((line) => line.result === 'ok').reduce((sum, line) => sum + line.calls, 0);

  return (
    <div className="flex flex-col gap-1 px-4 py-3" style={{ userSelect: 'text' }}>
      <div className="flex items-baseline gap-3 text-[11px]">
        <span className="text-[var(--paper-dim)]">
          这次读的区间是 {stamp(data.since)} – {stamp(data.until)}
        </span>
        <span className="mono text-[var(--muted)]">
          共 {total.toLocaleString()} 次尝试，成功 {ok.toLocaleString()} 次
        </span>
      </div>
      <div className="flex flex-col gap-1">
        {lines.map((line) => {
          const read = describe(line);
          return (
            <div key={`${line.result}|${line.refusal_kind}`} className="flex items-baseline gap-3 text-[11px]">
              <span className="mono w-40 shrink-0" style={{ color: read.tone }}>
                {read.label}
              </span>
              <span className="min-w-0 flex-1 truncate text-[var(--muted)]">{read.hint}</span>
              <span className="mono w-24 shrink-0 text-right text-[var(--paper-dim)]">
                {line.calls.toLocaleString()} 次
              </span>
              <span className="mono w-14 shrink-0 text-right text-[var(--muted)]">{share(line.calls, total)}</span>
              <span className="h-1.5 w-28 shrink-0 overflow-hidden bg-[var(--ink-700)]" aria-hidden>
                <span
                  className="block h-full"
                  style={{
                    width: `${(line.calls / total) * 100}%`,
                    background: read.bar,
                  }}
                />
              </span>
            </div>
          );
        })}
      </div>
      <p className="text-[11px] text-[var(--muted)]">
        账本里的一行就是一次尝试，「拒绝」也算：被熔断挡回来的请求没有发出、没有花 token，
        但它确实被尝试过，所以它在这里、而不在花销里。两种「零花销」——没人用、和每次都被挡回来——
        只有从这个分布上才分得开。
      </p>
    </div>
  );
}

/** 一个结局怎么读给人听。 */
function describe(line: OutcomeLine): { label: string; hint: string; tone: string; bar: string } {
  const kind = refusalKind(line.refusal_kind);
  switch (line.result) {
    case 'ok':
      return {
        label: '成功',
        hint: '走完并拿到了回应',
        tone: 'var(--ok)',
        bar: 'var(--ok)',
      };
    case 'error':
      return {
        label: '上游报错',
        hint: kind ?? '送到了一个上游，但它没能把这次调用做完',
        tone: 'var(--danger)',
        bar: 'var(--danger)',
      };
    case 'refused_by_circuit':
      return {
        label: '池级熔断',
        hint: kind
          ? `一个上游都没试：熔断挡在前面（${kind}）`
          : '一个上游都没试：网关的熔断先一步挡住了全部请求',
        tone: 'var(--danger)',
        bar: 'var(--danger)',
      };
    case 'refused_by_pace':
      return {
        label: '配速拒绝',
        hint: kind ?? '没发出去：配额窗口的配速器把这次调用押后了',
        tone: 'var(--warn)',
        bar: 'var(--warn)',
      };
    default:
      return {
        label: line.result,
        hint: kind ?? '账本里出现了界面不认识的结局词',
        tone: 'var(--paper-dim)',
        bar: 'var(--line)',
      };
  }
}

/** 上游报错/拒绝的细分成因；空串是「没有更细的说法」，不是「原因不明」。 */
function refusalKind(kind: string): string | null {
  if (!kind) return null;
  switch (kind) {
    case 'quota_cycle':
      return '上游的配额周期用尽——窗口走完会自己好';
    case 'entitlement':
      return '上游说这个账号没有使用权——等不会让它好';
    case 'unclassified':
      return '上游拒绝了，但正文里读不出成因';
    default:
      return `上游给的成因是「${kind}」，界面没有登记过这个词`;
  }
}

function share(part: number, whole: number): string {
  const value = (part / whole) * 100;
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)}%`;
}

function stamp(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}
