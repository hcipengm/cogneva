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
 */
export function TokenShare({ actors }: { actors: ActorUsage[] }) {
  const rows = actors
    .map((actor) => ({
      key: `${actor.actor}|${actor.origin}|${actor.upstream}|${actor.api_style}`,
      label: actor.actor,
      origin: actor.origin,
      input: actor.tokens_input,
      output: actor.tokens_output,
      calls: actor.calls,
      failed: actor.failed_calls,
      total: actor.tokens_input + actor.tokens_output,
    }))
    .filter((row) => row.total > 0)
    .sort((a, b) => b.total - a.total);

  const total = rows.reduce((sum, row) => sum + row.total, 0);
  if (rows.length === 0 || total === 0) {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        窗口里没有可分摊的 token：所有执行者都是零。
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-1.5 px-4 py-3">
      <div className="flex h-2 w-full overflow-hidden bg-[var(--ink-700)]" aria-hidden>
        {rows.map((row, index) => (
          <span
            key={row.key}
            title={`${row.label}：占 ${percent(row.total, total)}`}
            style={{
              width: `${(row.total / total) * 100}%`,
              background: index % 2 === 0 ? 'var(--brass)' : 'color-mix(in srgb, var(--brass) 55%, var(--ink-600))',
            }}
          />
        ))}
      </div>
      <div className="flex flex-col gap-1">
        {rows.map((row, index) => (
          <div key={row.key} className="flex items-baseline gap-2 text-[11px]">
            <span
              aria-hidden
              className="h-1.5 w-1.5 shrink-0"
              style={{
                background: index % 2 === 0 ? 'var(--brass)' : 'color-mix(in srgb, var(--brass) 55%, var(--ink-600))',
              }}
            />
            <span className="mono min-w-0 flex-1 truncate text-[var(--paper-dim)]" title={`${row.label} · ${row.origin}`}>
              {row.label}
              <span className="text-[var(--muted)]"> · {row.origin}</span>
            </span>
            <span className="mono shrink-0 text-[var(--paper)]">{percent(row.total, total)}</span>
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
