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
      <Metering data={data} ok={ok} />
    </div>
  );
}

/**
 * 成功的那些调用，有多少真的量到了数。
 *
 * 这一块回答的是上面所有 token 合计的成色。一次 `ok` 的调用如果没带回用量帧，
 * 落库时就是三个 0——和一次真的没花钱的调用在账本上是同一行，合计算出来的
 * 窗口也一模一样。只有这一格分得开「上游说零」与「上游没开口」，所以它不能并
 * 进「成功 N 次」里：那个 N 里混着测到的钱和没测到的钱。
 */
function Metering({ data, ok }: { data: TokenOutcomes; ok: number }) {
  if (ok === 0) return null;
  // 旧网关不返回这一格：那时整页还是一份旧前端，但一旦有人把前端与网关分开部署，
  // 少一格不该让整面白屏——空数组在这里的意思正是「一条读数都没有」。
  const lines = [...(data.metering ?? [])].sort((a, b) => b.calls - a.calls);
  const metered = lines.reduce((sum, line) => sum + line.calls, 0);
  if (metered === 0) {
    return (
      <p className="text-[11px] text-[var(--warn)]">
        这 {ok.toLocaleString()} 次成功调用没有一条带着计量结果：账本在写入时还没有这一列，
        所以它们的 token 是记成 0 的「没量到」，不是量到的 0。这一段的花销合计只能当下界读。
      </p>
    );
  }
  const withNumbers = lines
    .filter((line) => line.outcome === 'read')
    .reduce((sum, line) => sum + line.calls, 0);
  const blind = metered - withNumbers;
  const missing = ok - metered;
  return (
    <div className="mt-2 flex flex-col gap-1 border-t border-[var(--line)] pt-2">
      <div className="flex items-baseline gap-3 text-[11px]">
        <span className="text-[var(--paper-dim)]">成功调用里量到数的份额</span>
        <span className="mono text-[var(--muted)]">
          {withNumbers.toLocaleString()} / {metered.toLocaleString()} 次 · {share(withNumbers, metered)}
        </span>
        {blind > 0 && (
          <span className="mono text-[var(--warn)]">
            另有 {blind.toLocaleString()} 次没拿到数——那几次的 token 在合计里算作 0
          </span>
        )}
      </div>
      {lines.map((line) => {
        const read = meterCell(line.outcome);
        return (
          <div key={line.outcome || 'unrecorded'} className="flex items-baseline gap-3 text-[11px]">
            <span className="mono w-40 shrink-0" style={{ color: read.tone }}>
              {read.label}
            </span>
            <span className="min-w-0 flex-1 truncate text-[var(--muted)]">{read.hint}</span>
            <span className="mono w-24 shrink-0 text-right text-[var(--paper-dim)]">
              {line.calls.toLocaleString()} 次
            </span>
            <span className="mono w-14 shrink-0 text-right text-[var(--muted)]">
              {share(line.calls, metered)}
            </span>
          </div>
        );
      })}
      {missing > 0 && (
        <p className="text-[11px] text-[var(--muted)]">
          还有 {missing.toLocaleString()} 次成功调用不在上面这几格里：它们的行写于计量列存在之前，
          没有读数可归，既不算量到，也不替它猜一格。
        </p>
      )}
      <p className="text-[11px] text-[var(--muted)]">
        只有「上游报了用量」那几次的 token 是上游自己说的数；其余各格的 token 是记成 0 的
        「没量到」，两者在上面的合计里长得一样，在这一块里分得开。
      </p>
    </div>
  );
}

/** 一格计量读数怎么读给人听。 */
function meterCell(outcome: string): { label: string; hint: string; tone: string } {
  switch (outcome) {
    case 'read':
      return { label: '上游报了用量', hint: '这一格的 token 是上游自己说的数', tone: 'var(--ok)' };
    case 'absent':
      return {
        label: '上游没报',
        hint: '问了用量、上游没答：这次调用的 token 记成 0，实际不是 0',
        tone: 'var(--warn)',
      };
    case 'not_asked':
      return {
        label: '我们没问',
        hint: '请求里没要用量帧，上游本来就不用报——修法在我们自己的请求这一侧',
        tone: 'var(--warn)',
      };
    case 'interrupted':
      return {
        label: '响应没走完',
        hint: '流断在半路，没读到用量；这是调用故障，不是上游不肯说',
        tone: 'var(--danger)',
      };
    case 'abandoned':
      return {
        label: '调用方提前断开',
        hint: '我们自己没把正文读完，用量帧还没到；两边都没法归咎',
        tone: 'var(--muted)',
      };
    default:
      return {
        label: outcome || '没有计量结果',
        hint: '账本里出现了界面不认识的计量词',
        tone: 'var(--paper-dim)',
      };
  }
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
    case 'refused_by_budget':
      return {
        label: '任务预算用完',
        hint: kind ?? '没发出去：这件工作自己那份当日预算已经用完，等它过午夜重置',
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
