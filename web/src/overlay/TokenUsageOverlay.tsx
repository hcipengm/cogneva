import { useState } from 'react';
import { useTokenUsage } from '@/hooks/useTokenUsage';
import type { ActorUsage } from '@/api/client';

/**
 * 尾窗取值（小时）。服务端另有上限并自行截断、且回的是**它答出的**窗口，
 * 所以这里只是取值的入口，不承担「窗口对不对」的判断——那是 `covered_*` 的事。
 */
const WINDOWS = [6, 24, 72, 168];

function n(x: number): string {
  return x.toLocaleString('zh-CN');
}

function stamp(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function Row({ line }: { line: ActorUsage }) {
  return (
    <tr className="border-t border-slate-800/60">
      <td className="py-1.5 pr-3 text-slate-200">{line.actor}</td>
      <td className="py-1.5 pr-3 text-slate-400">{line.origin}</td>
      <td className="py-1.5 pr-3 text-slate-400">{line.upstream}</td>
      <td className="py-1.5 pr-3 text-slate-400">{line.api_style}</td>
      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">{n(line.calls)}</td>
      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">
        {line.failed_calls > 0 ? (
          <span className="text-amber-400">{n(line.failed_calls)}</span>
        ) : (
          n(line.failed_calls)
        )}
      </td>
      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">{n(line.tokens_input)}</td>
      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">{n(line.tokens_output)}</td>
      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">{n(line.tokens_cached)}</td>
      <td className="py-1.5 text-right tabular-nums text-slate-400">{n(line.avg_latency_ms)} ms</td>
    </tr>
  );
}

export function TokenUsageOverlay({ onClose }: { onClose: () => void }) {
  const [hours, setHours] = useState(24);
  const { usage, loading, error, refresh } = useTokenUsage(hours, true);

  const lines = usage?.actors ?? [];
  const total = (pick: (l: ActorUsage) => number) =>
    lines.reduce((sum, l) => sum + pick(l), 0);
  const covered =
    usage?.covered_since && usage.covered_until
      ? `${stamp(usage.covered_since)} → ${stamp(usage.covered_until)}`
      : '';

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 p-6 backdrop-blur-sm">
      <div className="max-h-[90vh] w-full max-w-4xl overflow-y-auto rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl">
        <div className="mb-1 flex items-start justify-between">
          <h1 className="text-2xl font-semibold text-white">令牌花销</h1>
          <div className="flex items-center gap-2">
            <select
              value={hours}
              onChange={(e) => setHours(Number(e.target.value))}
              className="rounded-lg border border-slate-700 bg-slate-950/60 px-2 py-1 text-xs text-slate-300"
            >
              {WINDOWS.map((h) => (
                <option key={h} value={h}>
                  过去 {h} 小时
                </option>
              ))}
            </select>
            <button
              onClick={refresh}
              className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              刷新
            </button>
            <button
              onClick={onClose}
              className="rounded-lg px-2 py-1 text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              ✕
            </button>
          </div>
        </div>

        <p className="mb-6 text-sm text-slate-400">
          按调用方 × 来源环境 × 上游 × 协议汇总，取自耐久折叠表而不是逐笔明细。
        </p>

        {error && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
            {error}
            <div className="mt-1 text-xs text-red-300/80">
              这一格没有读数——不是「这期间没有花销」。
            </div>
          </div>
        )}

        {loading && !usage && <p className="text-sm text-slate-400">读取中…</p>}

        {usage && !covered && (
          <div className="mb-4 rounded-lg bg-amber-500/10 p-3 text-sm text-amber-300">
            请求的 {hours} 小时里没有任何一行折叠窗口——折叠每整点落一行，最近的窗口
            通常还没折。这是「没读到」，不是「没花销」。
          </div>
        )}

        {usage && lines.length > 0 && (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500">
                <th className="pb-2 pr-3 text-left font-medium">调用方</th>
                <th className="pb-2 pr-3 text-left font-medium">环境</th>
                <th className="pb-2 pr-3 text-left font-medium">上游</th>
                <th className="pb-2 pr-3 text-left font-medium">协议</th>
                <th className="pb-2 pr-3 text-right font-medium">调用</th>
                <th className="pb-2 pr-3 text-right font-medium">失败</th>
                <th className="pb-2 pr-3 text-right font-medium">输入</th>
                <th className="pb-2 pr-3 text-right font-medium">输出</th>
                <th className="pb-2 pr-3 text-right font-medium">缓存</th>
                <th className="pb-2 text-right font-medium">平均耗时</th>
              </tr>
            </thead>
            <tbody>
              {lines.map((line) => (
                <Row
                  key={`${line.actor}\u0000${line.origin}\u0000${line.upstream}\u0000${line.api_style}`}
                  line={line}
                />
              ))}
              <tr className="border-t border-slate-700 text-slate-200">
                <td className="py-2 pr-3 font-medium">合计</td>
                <td className="py-2 pr-3" />
                <td className="py-2 pr-3" />
                <td className="py-2 pr-3" />
                <td className="py-2 pr-3 text-right tabular-nums">{n(total((l) => l.calls))}</td>
                <td className="py-2 pr-3 text-right tabular-nums">
                  {n(total((l) => l.failed_calls))}
                </td>
                <td className="py-2 pr-3 text-right tabular-nums">
                  {n(total((l) => l.tokens_input))}
                </td>
                <td className="py-2 pr-3 text-right tabular-nums">
                  {n(total((l) => l.tokens_output))}
                </td>
                <td className="py-2 pr-3 text-right tabular-nums">
                  {n(total((l) => l.tokens_cached))}
                </td>
                <td className="py-2 text-right text-slate-500">不跨协议合成</td>
              </tr>
            </tbody>
          </table>
        )}

        {usage && lines.length === 0 && covered && (
          <p className="text-sm text-slate-400">
            这段窗口（{covered}）里没有一行花销。
          </p>
        )}

        {covered && (
          <p className="mt-4 text-xs text-slate-500">
            折叠覆盖到 {covered}。输入与输出的含义逐协议不同（OpenAI 兼容上游的输入已含缓存），
            所以缓存命中率只在同一协议的行内可比。
          </p>
        )}
      </div>
    </div>
  );
}
