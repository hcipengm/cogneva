/*
 * 产出：这一窗口里回来了什么。
 *
 * 产出不是变更。变更面记的是「谁提了什么、走到哪一步」，那是一条过程账；这一面记的是
 * 离开系统的东西：结束的活、构建出来的产物、带着提交落地的变更。一条变更可以走完全程
 * 而没有产出，一件产出也可以不经过变更账——所以这一面的主数不读变更账的结算数。
 *
 * 第三层是唯一一处借变更账的地方，借的也不是结算数：那是漏斗里「落定带了提交」的那一
 * 栏，与同一个漏斗里「落定」那一栏不是一回事——后者大多是「文档要的，树本来就满足」，
 * 一条提交都没送出去。
 *
 * 完成不等于交付：任务走到 Completed 只是说这次执行结束了，`result` 才是交回来的东西。
 * 带结果与不带结果分两栏印，明细里也一样；把两者合成一个数，读的人会以为每一条完成都
 * 交回了东西。
 *
 * 三态照旧：量到了印数，没接上印破折号并说清缺哪一层，读失败说清为什么。0 是一个读数，
 * 把「这一层没接」印成 0，读的人会以为量过了。
 *
 * 画面：项目那一段是一组横条——这一窗活集中在哪几个项目桶里，一眼看出交付的集中度。
 * 没有读数的层不进这一幅：一根零长度的条读作「这个项目这一窗没交东西」，而它真正要说
 * 的可能是「这一拍没量过」。
 */
import {
  getOutputs,
  type OutputDelivery,
  type OutputLayer,
  type OutputProject,
  type OutputWork,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { BarList, type BarRow } from '@/ui/plots';
import { Empty, Loading, Moment, ReadError, Reading, Table, ViewFrame } from '@/ui/primitives';

/** 一层的三态：量到了印它自己的值，另两态印破折号与理由。 */
function show<T>(layer: OutputLayer<T>, value: (v: T) => string, note: (v: T) => string) {
  if (layer.state === 'measured') {
    return { value: value(layer.value), tone: 'plain' as const, note: note(layer.value) };
  }
  return { value: '—', tone: 'warn' as const, note: layer.because };
}

export function OutputView() {
  const inv = useResource('产出', getOutputs, 30_000);
  useKindTickedRefresh([resourceKey('output')], inv.refresh);

  const data = inv.data;
  const work = data?.work;
  const builds = data?.builds;
  const commits = data?.commits;

  // 项目横条只在「活」这一层量到时才画：那一层的项目归属来自任务图，别的层没有。
  const projectRows: BarRow[] = (data?.by_project ?? []).map((p: OutputProject) => ({
    id: p.project,
    label: p.project,
    value: p.completed,
    note:
      p.completed === p.with_result
        ? `${p.completed} 条，都带结果`
        : `${p.completed} 条，其中 ${p.with_result} 条带结果`,
    // 悬停多一句：这条的长度是按什么数的。带上带结果的比例，因为它决定这条里
    // 有多少是真的交付。
    detail: `${p.project}：完成 ${p.completed} 条，其中 ${p.with_result} 条带结果`,
  }));

  const workShown = work
    ? show<OutputWork>(
        work,
        (v) => String(v.completed),
        (v) => `带结果 ${v.with_result} · 失败 ${v.failed}` + (v.note ? ` · ${v.note}` : ''),
      )
    : null;
  const buildsShown = builds
    ? show(builds, (v) => String(v.built), (v) => `这一窗记下 ${v.total} 次跑次` + (v.note ? ` · ${v.note}` : ''))
    : null;
  const commitsShown = commits
    ? show(
        commits,
        (v) => String(v.delivered),
        // 天数与「按落定时刻分天」放在值旁边：那个数覆盖的是几天、按哪个时刻切的，
        // 决定它能不能与旁边两栏放在同一张屏上比。
        (v) => `最近 ${v.days} 天 · 按落定时刻分天 · 数是变更条数，不是提交条数`
      )
    : null;

  return (
    <ViewFrame
      title="产出"
      meta={
        data
          ? `最近 ${data.window_hours} 小时 · ${
              data.recent.length === 0
                ? '这一窗没有结束的活'
                : `${data.recent.length} 条明细${data.recent.length < (work?.state === 'measured' ? work.value.completed : 0) ? '（只列最近的）' : ''}`
            } · 按 30 秒重读`
          : '这一窗口里回来了什么 · 按 30 秒重读'
      }
    >
      <section className="border-b border-[var(--line)]">
        <p className="px-4 pb-2 text-[11px] leading-relaxed text-[var(--muted)]">
          产出是离开系统、别人拿得到的东西：结束的活、构建出来的产物、带着提交落地的变更。
          它不读变更账的结算数——变更记的是过程，一条变更走完全程也可以什么都没交出来。
          完成也不等于交付：只有带回 result 的完成才算交回了东西。
        </p>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          三层
        </h2>
        {inv.error ? (
          <ReadError what="产出" error={inv.error} onRetry={inv.refresh} />
        ) : !data && inv.loading ? (
          <Loading what="产出" />
        ) : (
          <div className="flex flex-wrap gap-6 px-4 pb-3">
            <Reading
              label="结束的活"
              value={workShown?.value ?? '—'}
              tone={workShown?.tone ?? 'warn'}
              note={workShown?.note ?? '这一拍没有读任务图'}
              source="GET /api/v1/output · work"
            />
            <Reading
              label="构建产物"
              value={buildsShown?.value ?? '—'}
              tone={buildsShown?.tone ?? 'warn'}
              note={buildsShown?.note ?? '这一拍没有读构建跑次'}
              source="GET /api/v1/output · builds"
            />
            <Reading
              label="带提交落地的变更"
              value={commitsShown?.value ?? '—'}
              tone={commitsShown?.tone ?? 'warn'}
              note={commitsShown?.note ?? '这一拍没有这一层的读数'}
              source="GET /api/v1/output · commits"
            />
          </div>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <BarList
          label="这一窗的活落在哪些项目里"
          note={
            '来源：GET /api/v1/output · by_project · 一条一行，长度按本组最大值归一' +
            (work?.state === 'measured' ? '' : '；这一层这一拍没有读数，所以这一幅没有长度可画')
          }
          empty={<span>这一窗没有结束的活：这一幅没有长度可画，不等于系统什么都没做。</span>}
          rows={projectRows}
        />
      </section>

      <section className="min-h-0 flex-1 overflow-auto">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          最近的交付
        </h2>
        {data && data.recent.length === 0 ? (
          work?.state === 'measured' ? (
            <Empty>这一窗没有结束的活。这一格里没有东西可列——不是没读到，是真的没有。</Empty>
          ) : (
            <Empty>
              任务图这一拍没读到（{work?.state === 'unread' ? work.because : '没有接上'}）：
              明细空是因为没量到，不是因为没有。
            </Empty>
          )
        ) : data ? (
          <Table
            columns={[
              {
                key: 'finished_at',
                title: '结束于',
                width: '110px',
                render: (d: OutputDelivery) => (
                  <span className="text-[11px]">
                    <Moment iso={d.finished_at} />
                  </span>
                ),
              },
              {
                key: 'project',
                title: '项目',
                width: '160px',
                render: (d) => <span className="mono text-[11px]">{d.project}</span>,
              },
              {
                key: 'worker',
                title: '谁做的',
                width: '150px',
                render: (d) => (
                  <span className="text-[11px]">{d.worker || '没有署名'}</span>
                ),
              },
              {
                key: 'result',
                title: '交回了什么',
                render: (d) =>
                  d.carried_result ? (
                    <span className="text-[11px] text-[var(--muted)]">
                      {d.result_excerpt || '（结果为空）'}
                    </span>
                  ) : (
                    // 完成但没有 result 的行是一条记账，不是交付。两者在表上要分开，
                    // 否则「这一窗交了多少」会被这类行撑大。
                    <span className="text-[11px] text-[var(--warn,var(--muted))]">
                      没有结果：只记了一次结束
                    </span>
                  ),
              },
            ]}
            rows={data.recent}
            rowKey={(d) => d.id}
          />
        ) : null}
      </section>
    </ViewFrame>
  );
}
