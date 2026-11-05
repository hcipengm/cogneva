import { getVenues, type VenueReading, type VenueVerdict } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Empty, Loading, Moment, ReadError, Reading, ViewFrame } from '@/ui/primitives';
import { Note } from '@/ui/Note';
import { BarList } from '@/ui/plots';

/**
 * 场地这一维：同一批任务按「在哪儿做」分开看。
 *
 * 场地不是员工，也不是面。员工是「谁在做」，面是组织上的责任归属，场地是这件
 * 活落在哪一栏。同一个员工今天在 memory 这一栏、明天在 wiki 那一栏，人是同一个；
 * 所以这里按场地分组，绝不按人分。
 *
 * 每一栏只报三条：**待做**（还没到终态的活）、**判据**（最近一条到过终态的任务
 * 成没成）、**写回量**（一天内交回的答案字节数）。场地不花 token，所以这里没有
 * 花销那一格——花销按员工 id 记，在花销榜上。
 */

function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}

/** 判据那一格：通过、没通过、还是从来没跑完过。 */
function verdictCell(latest: VenueVerdict | undefined) {
  if (!latest) return { value: '还没有跑完过一件', tone: 'plain' as const, note: '这一栏今天还没到过终态' };
  if (latest.outcome === 'completed') {
    return { value: '通过', tone: 'ok' as const, note: `${latest.task_id} · 最近一次到终态是完成` };
  }
  if (latest.outcome === 'failed') {
    return { value: '没通过', tone: 'danger' as const, note: `${latest.task_id} · 最近一条失败了` };
  }
  return {
    value: '没判成',
    tone: 'warn' as const,
    note: `${latest.task_id} · 最近一条是 ${latest.outcome}，不是完成`,
  };
}

function VenueColumn({ row, windowSecs }: { row: VenueReading; windowSecs: number }) {
  const verdict = verdictCell(row.latest);
  return (
    <section className="flex min-h-0 flex-col border-r border-[var(--line)] last:border-r-0">
      <header className="flex items-baseline justify-between border-b border-[var(--line)] px-3 py-2">
        <span className="text-[13px] text-[var(--paper)]">{row.venue}</span>
        <span className="mono text-[10px] text-[var(--muted)]">{row.tasks} 件活</span>
      </header>
      <div className="min-h-0 flex-1 overflow-y-auto p-3">
        <Reading
          label="待做任务数"
          value={row.open}
          tone={row.open > 0 ? 'warn' : 'plain'}
          note={row.open > 0 ? '还没到终态的活' : '没有等着的活'}
        />
        <Reading
          label="最近一条任务的判据"
          value={verdict.value}
          tone={verdict.tone}
          note={verdict.note}
        />
        <Reading
          label="写回量"
          value={bytes(row.written_back_bytes)}
          note={`近 ${Math.round(windowSecs / 3600)} 小时交回的答案字节数`}
        />
        {row.latest && (
          <p className="mt-2 text-[10px] text-[var(--muted)]">
            最近一次终态 <Moment iso={row.latest.at} />
          </p>
        )}
      </div>
    </section>
  );
}

export function VenuesView() {
  const venues = useResource('场地读数', getVenues, 20_000);
  useKindTickedRefresh(['tasks', resourceKey('tasks')], venues.refresh);

  const data = venues.data;

  return (
    <ViewFrame
      title="场地"
      meta={
        data
          ? `${data.venues.length} 个场地 · 读的是同一条任务图，按「在哪儿做」分组 · 每 ${venues.pollMs / 1000} 秒重读`
          : '同一条任务图，按「在哪儿做」分组'
      }
      actions={
        <button
          onClick={venues.refresh}
          className="text-[11px] text-[var(--muted)] hover:text-[var(--paper)]"
        >
          重读
        </button>
      }
    >
      {venues.error ? (
        <ReadError what="场地读数" error={venues.error} onRetry={venues.refresh} />
      ) : !data && venues.loading ? (
        <Loading what="场地读数" />
      ) : !data || data.venues.length === 0 ? (
        <Empty>
          还没有落在任何场地上的任务。场地是任务行上的标签：memory 抽取、wiki 蒸馏、
          评测题、进化活儿各自会给自己的任务打上那一栏的名字，标上了才会出现在这里。
        </Empty>
      ) : (
        <div className="flex min-h-0 flex-1 flex-col">
          {/* 三栏读数答「这一栏现在怎么样」，这一幅答「几栏摆在一起谁积压」——
              两件事分开画：前者要原值与判据，后者要形状。 */}
          <BarList
            label="各场地还没做完的活"
            note="同一条任务图按场地标签折叠，没到终态的活都算待做；长度按这一屏最多的那一栏归一"
            rows={data.venues.map((row) => ({
              label: row.venue,
              value: row.open,
              note: `${row.open} 件`,
              tone: row.open > 0 ? 'warn' : 'muted',
              detail: row.latest
                ? `最近一条到终态的任务是 ${row.latest.outcome}`
                : '这一栏还没跑完过任何一件',
            }))}
          />
          <div className="flex min-h-0 flex-1 border-t border-[var(--line)]">
            {data.venues.map((row) => (
              <div key={row.venue} className="flex min-h-0 flex-1 flex-col">
                <VenueColumn row={row} windowSecs={data.write_back_window_secs} />
              </div>
            ))}
          </div>
        </div>
      )}
      <Note text="场地与花销为什么不在同一张榜上">
        场地不花 token，也不在花销榜上：花销按员工 id 记，谁花的记在谁头上。一个场地
        今天由谁来做是活的属性，明天换一个人做，这一栏仍然是同一个场地。
      </Note>
    </ViewFrame>
  );
}
