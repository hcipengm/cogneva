/*
 * 能力：这个平台此刻真的做得到的动作。
 *
 * 这一面答的是「会什么」，不是「装了什么」。一项能力是一件能被点单的动作，所以这张
 * 表只列动作——插件、技能文档、分工表都不在这里，它们是能力的来源或者影子，列进来
 * 会让一个声明读起来像一次执行。
 *
 * 同名不同源是两行：删掉一个来源并不会让这项能力消失，按名字合并的表会说它消失了。
 *
 * 三态照旧：量到了印数，没接上印破折号并说清缺哪一层，读失败说清为什么。0 是一个
 * 读数，把「这一层没接」印成 0，读的人会以为量过了。
 *
 * 有名字不等于能调。插件清单里声明的工具今天没有任何执行侧登记它，所以每一项都带
 * 「调用点」：`callable` 点了会真的跑，`declared_only` 点了落空。整张表按可调数排，
 * 只有声明的那几项自己说出来——混在一起读，这张表就退化成「装了什么」。
 *
 * 「谁能点它」另占一栏，读的是分工表（每个角色声明的 tools），不是名册自报——名册
 * 那一栏写的是角色名，而运行时收窄一轮用的是分工表。没有声明 tools 的角色是「不设
 * 边界，拿整张注册表」，单独一句话说清：不点破的话，那一栏的空格会被读成「谁都不许」。
 *
 * 三要件各占一栏：名字、调用点、边界（要什么参数）。第四栏是「点过几次」——它不属
 * 于三要件，答的是另一问：这些动作里哪些真的在被人用。数据只覆盖这一台进程，重启清
 * 零；只有内建那一层有计数器，其余层印「这一层没计数」而不是 0。
 *
 * 画面：来源那一段是一组横条——每一层有几项真的能调，一眼看出供给集中在哪一层。没
 * 量到的层不进这一幅：一根零长度的条读作「这一层供了零项」，而它真正要说的是「没量
 * 过」。
 */
import {
  getCapabilities,
  type CapabilityRunsIn,
  type CapabilitySource,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Note } from '@/ui/Note';
import { BarList, type BarRow } from '@/ui/plots';
import { Empty, Loading, ReadError, Reading, Table, ViewFrame } from '@/ui/primitives';

const SOURCE_LABEL: Record<CapabilitySource, string> = {
  builtin: '内建',
  plugin: '插件',
  mcp: 'MCP',
};

const RUNS_IN_LABEL: Record<CapabilityRunsIn, string> = {
  native: '进程内',
  wasm: 'WASM 沙箱',
  shell: '远端执行器',
};

const SOURCE_NOTE: Record<CapabilitySource, string> = {
  builtin: '编译进这个进程、由 cog-agent 发布的工具',
  plugin: '磁盘清单声明的工具',
  mcp: '远端 MCP 对端暴露的工具',
};

export function CapabilitiesView() {
  const inv = useResource('能力清单', getCapabilities, 30_000);
  useKindTickedRefresh([resourceKey('能力清单')], inv.refresh);

  const data = inv.data;
  const sources = data?.sources ?? [];
  const unwired = sources.filter(([, r]) => r.state !== 'measured').length;
  // 「有名字」与「能调」是两个数：插件清单里的工具今天只有名字，点了会落空。这一面
  // 两个都印，主数才是「会做什么」。
  const callable = data?.entries.filter((e) => e.reach === 'callable').length ?? 0;
  const declaredOnly = (data?.entries.length ?? 0) - callable;
  // 分工表里没有声明 tools 的角色：它们拿整张注册表，所以每一项都能点。`null` 是这张
  // 表没读到——那时「没有这种角色」无从说起。
  const unboundedRoles = data?.unbounded_roles ?? null;

  // 有长度的只有量到的那几层。没接上、读失败的那几层不进这一幅——画成一根零长度
  // 的条，读者看到的是「这一层供了零项」，而真话是「这一层没量过」。它们去哪了，
  // 写在下面那行注里。
  const barRows: BarRow[] = [];
  const silent: string[] = [];
  for (const [source, reading] of sources) {
    if (reading.state === 'measured') {
      barRows.push({
        id: source,
        label: SOURCE_LABEL[source],
        value: reading.callable,
        note:
          reading.callable === reading.entries
            ? `${reading.callable} 项，都能调`
            : `${reading.callable} 项能调，另 ${reading.entries - reading.callable} 项只有声明`,
      });
    } else {
      silent.push(`${SOURCE_LABEL[source]}（${reading.because}）`);
    }
  }

  return (
    <ViewFrame
      title="能力"
      meta={
        data
          ? `${callable} 项能调${
              declaredOnly > 0 ? ` · ${declaredOnly} 项只有声明` : ''
            } · ${
              data.declaring_units === null
                ? '名册这一拍没读到'
                : `${data.declaring_units} 个单位自报了能力`
            } · 按 30 秒重读`
          : '这个系统此刻做得到的动作 · 按 30 秒重读'
      }
    >
      <section className="border-b border-[var(--line)]">
        <p className="px-4 pb-2 text-[11px] leading-relaxed text-[var(--muted)]">
          一项能力是一件能被点单的动作：有名字（别人用它点单），有调用点（这个名字走到一处
          真实执行），有边界（要什么参数、做砸了怎么算）。插件清单、技能文档、执行手表是它的
          来源或影子，本身不算能力——把「装了几个插件」读成「会几件事」，是把供给当成了能力。
        </p>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">来源</h2>
        {inv.error ? (
          <ReadError what="能力清单" error={inv.error} onRetry={inv.refresh} />
        ) : !data && inv.loading ? (
          <Loading what="能力清单" />
        ) : sources.length === 0 ? (
          <Empty>这份清单没有任何来源读数：不是「一项能力都没有」，是这三层都没接上。</Empty>
        ) : (
          <div className="flex flex-wrap gap-6 px-4 pb-3">
            {sources.map(([source, reading]) => (
              <Reading
                key={source}
                label={SOURCE_LABEL[source]}
                value={
                  reading.state === 'measured' ? String(reading.callable) : '—'
                }
                tone={reading.state === 'measured' ? 'plain' : 'warn'}
                note={
                  reading.state === 'measured'
                    ? SOURCE_NOTE[source] +
                      (reading.callable === reading.entries
                        ? ''
                        : `；这一层声明了 ${reading.entries} 项，其中 ${reading.callable} 项有调用点`)
                    : reading.because
                }
                source={`/api/v1/capabilities · ${source}`}
              />
            ))}
          </div>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <BarList
          label="每一层各供了多少项动作"
          note={
            '来源：GET /api/v1/capabilities · 一条一行，长度按本组最大值归一'
            + (silent.length > 0 ? `；没有读数的层不画长度：${silent.join('、')}` : '')
          }
          empty={<span>三层都没有报读数：这一幅没有长度可画，不等于零项能力。</span>}
          rows={barRows}
        />
      </section>

      <section className="min-h-0 flex-1 overflow-auto">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">动作</h2>
        <p className="px-4 pb-2 text-[11px] leading-relaxed text-[var(--muted)]">
          「点过几次」只数这一台进程里的调用，进程重启清零：跨 Pod 的调用不经过这里
          的登记表，那两个数要分开读。
        </p>
        {unboundedRoles && unboundedRoles.length > 0 ? (
          // 空表 `tools` 不是「什么都不能调」，是「不设边界，拿整张注册表」——
          // 不点破的话，「谁能点它」那一栏的空格会被读成「谁都不许」，而那恰好
          // 把最宽的角色读成了最窄的。
          <p className="px-4 pb-2 text-[11px] leading-relaxed text-[var(--warn,var(--muted))]">
            {unboundedRoles.join('、')} 没有声明 tools：它们拿整张注册表，所以每一项都能点。
            「谁能点它」那一栏空着，不等于谁都不许。
          </p>
        ) : null}
        {data && data.declaring_units !== null && data.declaring_units > 0 && callable + declaredOnly > 0 && data.entries.every((e) => e.holders.length === 0) ? (
          // 名册这一拍确实有单位自报了能力，而没有一个名字对得上动作名：这不是「没人
          // 会做」，是两边说的不是同一组词——名册自报的是角色（planner、generator），
          // 这一栏要的是动作名（read_file）。不点破的话，读者会去查一个不存在的接线
          // 问题，而真正要接的是「谁可以调它」这条来自角色分工表的读数。
          <p className="px-4 pb-2 text-[11px] leading-relaxed text-[var(--warn,var(--muted))]">
            {data.declaring_units} 个单位自报了能力，却没有一条与动作名对上：名册自报的是
            角色，不是动作名。「谁可以调它」要看角色分工表（SkillConfig 的 tools），这一栏
            今天没人填。
          </p>
        ) : null}
        {data && data.entries.length === 0 ? (
          // 「空表」有两种，要采取的动作相反：三层都读到了而一项没有（这个系统真
          // 的不会做什么），与还有层没接上（表空是因为没量全）。一句话说成前者，
          // 读者会去查一个不存在的接线问题。
          <Empty>
            {unwired > 0
              ? `一层动作都没有，而 ${unwired} 层还没报出读数：表空是因为没量全，先看上面那一段。`
              : '三层来源都读到了，但一项动作都没有。'}
          </Empty>
        ) : data ? (
          <Table
            columns={[
              {
                key: 'name',
                title: '名字',
                width: '240px',
                render: (e) => <span className="mono text-[11px]">{e.name}</span>,
              },
              {
                key: 'source',
                title: '来源',
                width: '90px',
                render: (e) => <span className="text-[11px]">{SOURCE_LABEL[e.source]}</span>,
              },
              {
                key: 'reach',
                title: '调用点',
                width: '120px',
                // 名字在清册上有，与按这个名字点单能跑到，是两件事。只有声明的那一行
                // 必须自己说出来：混在「能调」里读，整张表就成了「装了什么」。
                render: (e) =>
                  e.reach === 'callable' ? (
                    <span className="text-[11px]">在</span>
                  ) : (
                    <span className="text-[11px] text-[var(--warn,var(--muted))]">
                      只声明，点了落空
                    </span>
                  ),
              },
              {
                key: 'allowed_roles',
                title: '谁能点它',
                width: '150px',
                // 这一栏从分工表读，不从名册自报读：名册写的是角色名，而运行时
                // 收窄一轮用的是分工表里的 `tools`。`null` 是分工表没读到，不是
                // 「谁都不许」——后者印「没有角色点名它」。
                render: (e) =>
                  e.allowed_roles === null ? (
                    <span className="text-[11px] text-[var(--warn,var(--muted))]">
                      分工表没读到
                    </span>
                  ) : e.allowed_roles.length > 0 ? (
                    <span className="text-[11px]">{e.allowed_roles.join('、')}</span>
                  ) : unboundedRoles && unboundedRoles.length > 0 ? (
                    <Note
                      className="text-[11px] text-[var(--muted)]"
                      text={`不设边界的角色（${unboundedRoles.join('、')}）能点每一项`}
                    >
                      只有不设边界的角色
                    </Note>
                  ) : (
                    <span className="text-[11px] text-[var(--muted)]">没有角色点名它</span>
                  ),
              },
              {
                key: 'boundary',
                title: '要什么参数',
                width: '170px',
                // 边界是三要件之一：没有参数表，这一项点了也不知道该带什么。`null`
                // 是这一层根本没给 schema，不是「不用参数」——后者印「不要参数」。
                render: (e) =>
                  e.boundary === null ? (
                    <span className="text-[11px] text-[var(--warn,var(--muted))]">
                      清单没给参数表
                    </span>
                  ) : (
                    <Note
                      className="text-[11px]"
                      text={
                        e.boundary.required.length > 0
                          ? `必填：${e.boundary.required.join('、')}`
                          : '没有必填参数'
                      }
                    >
                      {e.boundary.params === 0
                        ? '不要参数'
                        : e.boundary.required.length === 0
                          ? `${e.boundary.params} 个，都可选`
                          : `${e.boundary.params} 个，${e.boundary.required.join('、')} 必填`}
                    </Note>
                  ),
              },
              {
                key: 'runs_in',
                title: '跑在哪',
                width: '110px',
                // 边界的另一半：做砸了长什么样取决于它在哪跑。`null` 是这一层没报
                // 实现方式，不是「不在任何地方跑」。
                render: (e) =>
                  e.runs_in === null ? (
                    <span className="text-[11px] text-[var(--muted)]">这一层没报</span>
                  ) : (
                    <Note
                      className="text-[11px]"
                      text={
                        e.runs_in === 'native'
                          ? '进程内闭包：失败是一次调用返回错误'
                          : e.runs_in === 'wasm'
                            ? 'WASM 沙箱：失败是超时或被上限掐掉'
                            : '远端执行器：失败是不再应答的对端'
                      }
                    >
                      {RUNS_IN_LABEL[e.runs_in]}
                    </Note>
                  ),
              },
              {
                key: 'calls',
                title: '点过几次',
                width: '110px',
                // 这一栏答「有没有人在点它」。`null` 是这一层没有计数器：印 0 会让
                // 人去查「为什么这个工具没人用」，而该修的是「这里根本没人记」。
                // 数只覆盖这一台进程，重启清零——写在表头注里，不在这格里重复。
                render: (e) =>
                  e.calls === null ? (
                    <span className="text-[11px] text-[var(--muted)]">这一层没计数</span>
                  ) : (
                    <span
                      className={`mono text-[11px] ${e.calls === 0 ? 'text-[var(--muted)]' : ''}`}
                    >
                      {e.calls}
                    </span>
                  ),
              },
              {
                key: 'description',
                title: '做什么',
                render: (e) => (
                  <span className="text-[11px] text-[var(--muted)]">{e.description || '—'}</span>
                ),
              },
              {
                key: 'holders',
                title: '自报持有它的单位',
                width: '240px',
                render: (e) =>
                  data.declaring_units === null ? (
                    // 名册没读到这一拍，「没有单位自报」这句话无从说起：它与「零个单位
                    // 自报」在屏幕上同形，而一个要接线、一个要查读失败。
                    <span className="text-[11px] text-[var(--muted)]">
                      {data.roster_caveat
                        ? `名册没读到：${data.roster_caveat}`
                        : '名册没读到，这一栏无从判断'}
                    </span>
                  ) : e.holders.length === 0 ? (
                    // 空集合不是「没有单位能用它」，是「没有单位这么自报」——名册上的
                    // 能力标签与实际能点单的动作之间差着一次改名。
                    <span className="text-[11px] text-[var(--muted)]">没有单位自报</span>
                  ) : (
                    <span className="text-[11px]">
                      {e.holders.map((h) => `${h.unit}${h.live ? '' : '（不在岗）'}`).join(' · ')}
                    </span>
                  ),
              },
            ]}
            rows={data.entries}
            rowKey={(e) => `${e.source}:${e.name}`}
          />
        ) : null}
      </section>
    </ViewFrame>
  );
}
