import { getEvolutionMetrics } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Loading, ReadError, Reading, Readings } from '@/ui/primitives';

/**
 * 指标板：平台自己登记的那几个累计计数。
 *
 * 这四个数是演化进程内存里的累计值，重启清零——它们的来路是「平台自己加的数」，
 * 不是时间序列库。所以这一面只给数、不画曲线：没有历史采样，画出来的线是编的，
 * 而一条编出来的线比一格诚实的数更让人当真。
 */
export function MetricsBoard() {
  const metrics = useResource('演化指标', getEvolutionMetrics, 30_000);
  const m = metrics.data;

  if (metrics.error) {
    return <ReadError what="演化指标" error={metrics.error} onRetry={metrics.refresh} />;
  }
  if (!m) return <Loading what="演化指标" />;

  const failureRate =
    m.changes_applied + m.changes_failed > 0
      ? m.changes_failed / (m.changes_applied + m.changes_failed)
      : null;

  return (
    <>
      <Readings
        from={metrics}
        source="GET /api/v1/evolution/metrics"
        caveat="累计值来自演化进程内存，重启清零；它不是「历史上发生过多少」的存量。"
      >
        <div className="flex flex-wrap py-2">
          <Reading label="事件总数" value={m.events_total.toLocaleString()} />
          <Reading
            label="失败事件"
            value={m.events_failed.toLocaleString()}
            tone={m.events_failed > 0 ? 'warn' : 'plain'}
            note="失败事件不是失败的任务：一次重试成功前的那几次也记在这里"
          />
          <Reading label="变更落地" value={m.changes_applied.toLocaleString()} tone="ok" />
          <Reading
            label="变更失败"
            value={m.changes_failed.toLocaleString()}
            tone={m.changes_failed > 0 ? 'danger' : 'plain'}
          />
          <Reading
            label="变更失败率"
            value={failureRate == null ? '还没落过地' : `${(failureRate * 100).toFixed(1)}%`}
            note="两个数都为零时不给 0%，那会被读成「一次都没失败过」"
          />
        </div>
      </Readings>
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        想看一条曲线要先有采样：这一面按 30 秒重读一次，但只有最近一次的值被留下来，
        前后两次的差不是「这段时间新增了多少」。构建跑次的来路在变更面。
      </p>
    </>
  );
}
