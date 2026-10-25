import { type ObservabilityReading, getObservabilityReadings } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { deepLink } from '@/observability/deepLink';
import { Loading, ReadError, Table } from '@/ui/primitives';

/**
 * 读数板：六条会在决策点上被要的数，每条带判词、来源与去深挖的链接。
 *
 * 三件事在这里刻意不做：
 *
 * 1. 不把六条合成一个健康分——盘快满了和入口在返 5xx 是两种处置，加权之后读者
 *    还得再拆一次。
 * 2. 不把「没有读数」画成 0。`unknown` 是「这条序列一条样本都没有」或「Prometheus
 *    问不到」，与真实的 0 是两回事：一个是有东西坏了，一个是系统很安静。
 * 3. 不自己画曲线。这里回答「现在要不要动手」，往下挖去 Grafana——所以每条读数
 *    都带它那块面板的 uid，同时把「为什么这条点不过去」写出来。
 */

const VERDICT_TONE: Record<string, string> = {
  ok: 'var(--ok)',
  warning: 'var(--warn)',
  critical: 'var(--danger)',
  unknown: 'var(--muted)',
};

const VERDICT_WORD: Record<string, string> = {
  ok: '正常',
  warning: '告警',
  critical: '严重',
  unknown: '无读数',
};

function Verdict({ verdict }: { verdict: string }) {
  const color = VERDICT_TONE[verdict] ?? 'var(--paper-dim)';
  return (
    <span className="mono text-[11px] whitespace-nowrap" style={{ color }}>
      <span className="dot" style={{ background: color }} />
      {/* 判词之外再给中文：颜色不是唯一的身份。 */}
      {verdict} · {VERDICT_WORD[verdict] ?? verdict}
    </span>
  );
}

function value(r: ObservabilityReading): string {
  if (r.value == null) return '—';
  if (r.unit === '%') return `${(r.value * 100).toFixed(1)}%`;
  if (r.unit === '/s') return `${r.value.toFixed(3)}/s`;
  if (Number.isInteger(r.value)) return `${r.value} ${r.unit}`;
  return `${r.value.toFixed(1)} ${r.unit}`;
}

export function ReadingsBoard() {
  const readings = useResource('观测读数', getObservabilityReadings, 15_000);
  const data = readings.data;

  if (readings.error) {
    return <ReadError what="观测读数" error={readings.error} onRetry={readings.refresh} />;
  }
  if (!data) return <Loading what="观测读数" />;

  return (
    <>
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        六条读数，取数自 <span className="mono">{data.prometheus}</span> · 每条注明序列与产出方；
        「无读数」是没有样本或问不到，不是 0。
      </p>
      <Table
        columns={[
          {
            key: 'title',
            title: '读数',
            width: '180px',
            render: (r) => (
              <div>
                <div className="text-[12px]">{r.title}</div>
                <div className="mono text-[10px] text-[var(--muted)]">{r.id}</div>
              </div>
            ),
          },
          {
            key: 'value',
            title: '当前值',
            width: '110px',
            render: (r) => <span className="mono text-[12px]">{value(r)}</span>,
          },
          {
            key: 'verdict',
            title: '判词',
            width: '140px',
            render: (r) => <Verdict verdict={r.verdict} />,
          },
          {
            key: 'source',
            title: '来源',
            render: (r) => (
              <div className="text-[11px]">
                <div className="mono text-[var(--paper-dim)]">{r.series}</div>
                <div className="text-[var(--muted)]">产出方：{r.produced_by}</div>
                {r.unavailable_because && (
                  <div className="text-[var(--warn)]">{r.unavailable_because}</div>
                )}
              </div>
            ),
          },
          {
            key: 'link',
            title: '深挖',
            width: '150px',
            render: (r) => {
              const link = deepLink(r.dashboard_uid);
              if (link.href) {
                return (
                  <a
                    href={link.href}
                    target="_blank"
                    rel="noreferrer"
                    className="text-[11px] underline decoration-dotted"
                    style={{ color: 'var(--brass)' }}
                  >
                    在 Grafana 打开 {r.dashboard_uid}
                  </a>
                );
              }
              // 点不过去也要说出理由：不说的话，读者会以为是漏做了。
              return <span className="text-[10px] text-[var(--muted)]">{link.why}</span>;
            },
          },
        ]}
        rows={data.readings}
        rowKey={(r) => r.id}
      />
    </>
  );
}
