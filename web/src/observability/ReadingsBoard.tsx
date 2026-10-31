import { type ObservabilityReading, getObservabilityReadings } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { deepLink } from '@/observability/deepLink';
import { Loading, ReadError, Table } from '@/ui/primitives';

/**
 * 读数板：会在决策点上被要的数，每条带判词、来源与去深挖的链接，按组分段。
 *
 * 三件事在这里刻意不做：
 *
 * 1. 不把几条合成一个健康分——盘快满了和入口在返 5xx 是两种处置，加权之后读者
 *    还得再拆一次。
 * 2. 不把「没有读数」画成 0。`unknown` 是「这条序列一条样本都没有」或「Prometheus
 *    问不到」，与真实的 0 是两回事：一个是有东西坏了，一个是系统很安静。
 * 3. 不自己画曲线。这里回答「现在要不要动手」，往下挖去 Grafana——所以每条读数
 *    都带它那块面板的 uid，同时把「为什么这条点不过去」写出来。
 *
 * 分段回答的是「该在什么时候读它」：平台自己的交付链停着时，基础设施那一段可能
 * 全是正常——2026-10-31 主线停了 2.7 小时，而这一页的每一行都读作「正常」。
 */

/** 判词的键与 `ObservabilityReading['verdict']` 是同一组词：两张表都按它取行。 */
type VerdictWord = ObservabilityReading['verdict'];

const VERDICT_TONE: Record<VerdictWord, string> = {
  ok: 'var(--ok)',
  warning: 'var(--warn)',
  critical: 'var(--danger)',
  unknown: 'var(--muted)',
};

const VERDICT_WORD: Record<VerdictWord, string> = {
  ok: '正常',
  warning: '告警',
  critical: '严重',
  unknown: '无读数',
};

function Verdict({ verdict }: { verdict: VerdictWord }) {
  const color = VERDICT_TONE[verdict];
  return (
    <span className="mono text-[11px] whitespace-nowrap" style={{ color }}>
      <span className="dot" style={{ background: color }} />
      {/* 判词之外再给中文：颜色不是唯一的身份。 */}
      {verdict} · {VERDICT_WORD[verdict]}
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

/**
 * 每一段的标题与它回答的问题。按组的出现顺序画，不按这里列的顺序：一组读数
 * 全被撤掉时，屏上不该留一个空标题说「这里本来有东西」。
 */
const GROUP_LABEL: Record<string, { title: string; what: string }> = {
  delivery: { title: '平台自身的交付链', what: '代码能不能进 · 活能不能干 · 结果能不能走' },
  infra: { title: '基础设施', what: '盘、库、向量库、入口、可达性' },
};

/** 不认识的组照原样单列一段，并进别的一段等于把它藏在错的那一分钟里。 */
function groupLabel(group: string) {
  return GROUP_LABEL[group] ?? { title: group, what: '未登记的组' };
}

export function ReadingsBoard() {
  const readings = useResource('观测读数', getObservabilityReadings, 15_000);
  const data = readings.data;

  if (readings.error) {
    return <ReadError what="观测读数" error={readings.error} onRetry={readings.refresh} />;
  }
  if (!data) return <Loading what="观测读数" />;

  // 组按数据里出现的顺序收集，条数也是数出来的：把「六条」写进标题的那一版，
  // 加了三条读数之后标题就开始说谎，而它说错的地方正是读者用来判断全不全的那个数。
  const groups: string[] = [];
  for (const r of data.readings) {
    if (!groups.includes(r.group)) groups.push(r.group);
  }

  return (
    <>
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        {data.readings.length} 条读数（{groups.map((g) => groupLabel(g).title).join(' · ')}），
        取数自 <span className="mono">{data.prometheus}</span> · 每条注明序列与产出方；
        「无读数」是没有样本或问不到，不是 0。
      </p>
      {groups.map((group) => (
        <section key={group}>
          <h3 className="flex items-baseline gap-2 px-4 pt-3 pb-1 text-[12px]">
            <span>{groupLabel(group).title}</span>
            <span className="text-[10px] font-normal text-[var(--muted)]">
              {groupLabel(group).what}
            </span>
          </h3>
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
            rows={data.readings.filter((r) => r.group === group)}
            rowKey={(r) => r.id}
          />
        </section>
      ))}
    </>
  );
}
