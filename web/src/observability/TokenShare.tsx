import type { ActorUsage } from '@/api/client';

/**
 * 谁在吃 token：一条按执行者分段的横条。
 *
 * 表里每一行都对，但一屏扫下来回答不了「是不是有一个执行者在吃掉大半预算」。
 * 这张图只回答这一个问题，所以只按「输入 + 输出」合计排序，不把它拆成两张图：
 * 拆开之后读者要在两张图之间对照「同一个执行者在哪」，而这正是这张图要省掉的。
 *
 * 比例的分母是用量本身，不是任何配额：这个平台没有「预算上限」这个数，
 * 画一条假的 100% 线会让人以为超了会有事发生。
 *
 * **零 token 的行照样列出。** 一条有调用、有失败、却没有 token 的行不是噪声，
 * 它说的是「这个执行者在试，但一次都没拿到回应」——上游熔断的时候整段窗口就是
 * 这样。把它从这里滤掉，剩下的图只会显示「这段没花多少」，而真实情况是
 * 「这段根本没跑起来」。条上它没有宽度（本来就 0），但清单里有它的位置。
 */
export function TokenShare({ actors }: { actors: ActorUsage[] }) {
  const rows = actors
    .map((actor) => ({
      key: `${actor.actor}|${actor.origin}|${actor.upstream}|${actor.api_style}`,
      label: actor.actor,
      origin: actor.origin,
      upstream: actor.upstream,
      style: actor.api_style,
      input: actor.tokens_input,
      output: actor.tokens_output,
      calls: actor.calls,
      failed: actor.failed_calls,
      total: actor.tokens_input + actor.tokens_output,
    }))
    .sort(
      (a, b) =>
        b.total - a.total || b.calls - a.calls || a.key.localeCompare(b.key)
    );

  if (rows.length === 0) {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        这段覆盖里一行花销都没有。
      </p>
    );
  }

  const spenders = rows.filter((row) => row.total > 0);
  const total = spenders.reduce((sum, row) => sum + row.total, 0);
  const calls = rows.reduce((sum, row) => sum + row.calls, 0);
  const failed = rows.reduce((sum, row) => sum + row.failed, 0);

  return (
    <div className="flex flex-col gap-1.5 px-4 py-3">
      {total > 0 ? (
        <div className="flex h-2 w-full overflow-hidden bg-[var(--ink-700)]" aria-hidden>
          {spenders.map((row, index) => (
            <span
              key={row.key}
              style={{
                width: `${(row.total / total) * 100}%`,
                background: index % 2 === 0 ? 'var(--brass)' : 'color-mix(in srgb, var(--brass) 55%, var(--ink-600))',
              }}
            />
          ))}
        </div>
      ) : (
        // 一条都没有宽度：不存在「谁占了大部分」。空着一条底也不是零花销的图，
        // 所以干脆不画条，把话写在上面。
        <p className="text-[11px]" style={{ color: 'var(--danger)' }}>
          这段有 {calls.toLocaleString()} 次调用、0 token，其中 {failed.toLocaleString()} 次失败：
          不是「没人用模型」，是每次调用都在拿到回应前就结束了。下面按执行者列出它们各自试了多少次。
        </p>
      )}
      <div className="flex flex-col gap-1">
        {rows.map((row, index) => (
          <div key={row.key} className="flex items-baseline gap-2 text-[11px]">
            <span
              aria-hidden
              className="h-1.5 w-1.5 shrink-0"
              style={{
                background:
                  row.total === 0
                    ? 'var(--line)'
                    : index % 2 === 0
                      ? 'var(--brass)'
                      : 'color-mix(in srgb, var(--brass) 55%, var(--ink-600))',
              }}
            />
            <span
              className="mono min-w-0 flex-1 truncate text-[var(--paper-dim)]"
              title={`${row.label} · ${row.origin} · ${row.upstream} · ${row.style}`}
            >
              {row.label}
              <span className="text-[var(--muted)]"> · {row.origin}</span>
            </span>
            <span className="mono shrink-0 text-[var(--paper)]">
              {row.total > 0 ? percent(row.total, total) : '—'}
            </span>
            <span className="mono w-20 shrink-0 text-right text-[var(--muted)]">
              {compact(row.input)} in
            </span>
            <span className="mono w-20 shrink-0 text-right text-[var(--muted)]">
              {compact(row.output)} out
            </span>
            <span className="mono w-16 shrink-0 text-right text-[var(--muted)]">{row.calls} 次</span>
            {row.failed > 0 && (
              <span className="mono w-14 shrink-0 text-right" style={{ color: 'var(--danger)' }}>
                失败 {row.failed}
              </span>
            )}
          </div>
        ))}
      </div>
      {spenders.length < rows.length && (
        <p className="text-[11px] text-[var(--muted)]">
          {rows.length - spenders.length} 行是零 token 的：它们有调用、没拿到回应，所以不进那条分摊条，
          但留在上面这份清单里——把一条试过却失败的调用从「谁在花」里删掉，读起来就像那段时间根本没人调模型。
        </p>
      )}
    </div>
  );
}

function percent(part: number, whole: number): string {
  const value = (part / whole) * 100;
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)}%`;
}

/** 大数用万 / 亿，不把一长串数字塞进小格子——读者要的是量级，不是个位。 */
function compact(value: number): string {
  if (value >= 100_000_000) return `${(value / 100_000_000).toFixed(1)}亿`;
  if (value >= 10_000) return `${(value / 10_000).toFixed(1)}万`;
  return value.toLocaleString();
}
