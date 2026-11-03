import { getProgress, type OwnerProgress, type Presence } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { Strip } from '@/ui/plots';
import type { Column } from '@/ui/primitives';

/**
 * 在岗那一格。
 *
 * 「读不到」与「不在」刻意分开画：前者要去看那条在岗账，后者要派人接手。压成
 * 同一格的时候，一次读不动的在岗账会把所有责任人画成走光了。
 */
function PresenceCell({ presence }: { presence: Presence }) {
  const text = presence === 'present' ? '在' : presence === 'absent' ? '不在' : '读不到';
  const color =
    presence === 'present' ? 'var(--ok, #4a8)' : presence === 'absent' ? 'var(--danger, #c55)' : 'var(--muted)';
  return (
    <span className="mono text-[11px]" style={{ color }}>
      <span className="dot" style={{ background: color }} />
      {text}
    </span>
  );
}

const OWNER_COLUMNS: Column<OwnerProgress>[] = [
  {
    key: 'unit',
    title: '责任人',
    width: '32%',
    render: (row) => <span className="mono text-[11px]">{row.unit}</span>,
  },
  {
    key: 'presence',
    title: '在岗',
    width: '10%',
    render: (row) => <PresenceCell presence={row.presence} />,
  },
  { key: 'open', title: '未结', width: '10%', render: (row) => <span className="mono">{row.open}</span> },
  { key: 'completed', title: '完成', width: '10%', render: (row) => <span className="mono">{row.completed}</span> },
  { key: 'failed', title: '失败', width: '10%', render: (row) => <span className="mono">{row.failed}</span> },
  { key: 'cancelled', title: '取消', width: '10%', render: (row) => <span className="mono">{row.cancelled}</span> },
  {
    key: 'oldest',
    title: '最老未结',
    render: (row) => <Moment iso={row.oldest_open_at ?? null} />,
  },
];

/**
 * 进度面：三本账在同一个时刻的一份读数。
 *
 * 三本账各有各的读面（任务、变更、治理），但问「进展到哪儿了」的人只能把三个响应
 * 拼起来，而三次请求各有各的时刻。这一页读的是同一时刻的那一份。
 *
 * 数字按「谁该动手」排，不按「一共多少」排：任务那一段先给出没主的与被人放回队里
 * 的两批，因为它们只有在这里才看得见——按状态数的话，它们与排得好好的活逐字同形。
 */
export function ProgressView() {
  const progress = useResource('三本账进度', getProgress, 15_000);
  // 三本账的写者各在各的频道上，任务换态又是第四件。三件都订上，一次触发一份重读：
  // 只刷其中一本，同一屏上就会出现三本账各自的时刻。
  useKindTickedRefresh(
    ['tasks', 'projects', resourceKey('governance'), resourceKey('evolution')],
    progress.refresh
  );

  const page = progress.data;
  const tasks = page?.tasks;

  return (
    <ViewFrame
      title="进度"
      meta={
        page
          ? `这一拍 @ ${new Date(page.generated_at).toLocaleTimeString('zh-CN')}`
          : '按 15 秒重读'
      }
    >
      {progress.error ? (
        <ReadError what="三本账进度" error={progress.error} onRetry={progress.refresh} />
      ) : !page || !tasks ? (
        <Loading what="三本账进度" />
      ) : (
        <div className="min-h-0 flex-1 overflow-y-auto">
          <Readings from={progress} source="GET /api/v1/governance/progress（同一个时刻取三本账）">
            <div className="flex flex-wrap gap-x-8 gap-y-2 border-b border-[var(--line)] px-4 py-2">
              <Reading
                label="未结的活"
                value={tasks.open}
                tone={tasks.open > 0 ? 'warn' : 'plain'}
                note={`共 ${tasks.rows} 行，其中 ${tasks.open} 行还开着`}
              />
              <Reading
                label="没人认领"
                value={tasks.unowned_open}
                tone={tasks.unowned_open > 0 ? 'danger' : 'ok'}
                note="开着的活里没有 owner_unit 的那些。它们不在任何一个责任人名下，所以也不会出现在下面的表里。"
              />
              <Reading
                label="没闭的目标"
                value={tasks.unowned_goals}
                tone={tasks.unowned_goals > 0 ? 'warn' : 'plain'}
                note="开着、没有负责人、本来也不该被谁领的行：目标占位行。它们不是没人认领的活，处置也不同。"
              />
              <Reading
                label="被放回队里"
                value={tasks.returned_to_line}
                tone={tasks.returned_to_line > 0 ? 'danger' : 'ok'}
                note="状态还是 pending、行上却带着错：出过错没有结束，是被放回了队里。按状态看，它们与排得好好的活一样。"
              />
              <Reading
                label="责任人"
                value={tasks.owners.length}
                note="名下还有未结活的 unit 数。活交完的 unit 不列在这里。"
              />
              <Reading
                label="在岗缺席"
                value={tasks.owners.filter((owner) => owner.presence === 'absent').length}
                tone={tasks.owners.some((owner) => owner.presence === 'absent') ? 'danger' : 'ok'}
                note="名下有未结活、在岗账里却查无此人。这一格不为零时，「责任到人」只在账面上成立。"
              />
            </div>
          </Readings>

          <div className="border-b border-[var(--line)]">
            <Strip
              label="任务图里这些活各处在哪个状态"
              note="按这次读回来的同一份行数；黄的那几段是还要人动手的活"
              segments={tasks.by_state.map((cell) => ({
                label: cell.state,
                value: cell.rows,
                tone: cell.open && cell.rows > 0 ? ('warn' as const) : ('magnitude' as const),
              }))}
            />
          </div>

          <section className="border-b border-[var(--line)]">
            <h2 className="px-4 py-2 text-[12px] text-[var(--muted)]">
              谁名下还有什么 · 在岗一列问的是在岗账，不是这一页的推断
            </h2>
            {tasks.owners.length === 0 ? (
              <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
                没有一条开着的活写着负责人。这不是「没人欠东西」：上面那一格「没人认领」
                是同一个数。
              </p>
            ) : (
              <Table columns={OWNER_COLUMNS} rows={tasks.owners} rowKey={(row) => row.unit} />
            )}
          </section>

          <Readings from={progress} source="GET /api/v1/governance/progress 的变更与原则两段">
            <div className="flex flex-wrap gap-x-8 gap-y-2 border-b border-[var(--line)] px-4 py-2">
              {page.changes ? (
                <>
                  <Reading
                    label="变更欠账"
                    value={page.changes.outstanding}
                    tone={page.changes.outstanding > 0 ? 'warn' : 'ok'}
                    note={`共 ${page.changes.rows} 行，已落地或已退役 ${page.changes.settled} 行`}
                  />
                  <Reading label="变更总行数" value={page.changes.rows} note="变更账上全部身份，含已结的" />
                </>
              ) : (
                <Reading
                  label="变更欠账"
                  value="没有接上"
                  tone="warn"
                  note="这个部署没有发布变更账。一串零会被读成「没有欠账」，与「这一本没接上」是两件事。"
                />
              )}
              {page.principles ? (
                <>
                  <Reading
                    label="有门的原则"
                    value={page.principles.enforced}
                    note="数的是「有没有人声明过一道门」，不是「规则有没有被遵守」。"
                  />
                  <Reading
                    label="还欠门"
                    value={page.principles.unenforced}
                    tone={page.principles.unenforced > 0 ? 'warn' : 'ok'}
                  />
                  <Reading
                    label="从未判读"
                    value={page.principles.unmeasured}
                    note="还没有任何一次判读落过地的条数。"
                  />
                </>
              ) : (
                <Reading
                  label="原则台账"
                  value="没有接上"
                  tone="warn"
                  note="这个部署没有发布原则台账。"
                />
              )}
            </div>
          </Readings>

          {tasks.rows === 0 && (
            <Empty>任务图里一行都没有。这一页读的是同一批行，空着说明图是空的，不是读数失败。</Empty>
          )}
        </div>
      )}
    </ViewFrame>
  );
}
