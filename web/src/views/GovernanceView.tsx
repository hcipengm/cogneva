import { getGovernanceDrift, getGovernanceFaces, getGovernancePrinciples } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Status, Table, ViewFrame } from '@/ui/primitives';
import { BarList, Strip, type StripSegment, type Tone } from '@/ui/plots';

/**
 * 治理面：哪些面有人在守、哪些面已经过期、原则语料被判成了什么。
 *
 * 「谁在守」取的是声明与实测两说：`owner_source` 写明这一格的所有者是声明的还是
 * 量出来的——两者不一致时，界面要能看出分歧出在哪一侧，而不是给一个折中的名字。
 */
export function GovernanceView() {
  const faces = useResource('面的清单', getGovernanceFaces, 60_000);
  const drift = useResource('面的漂移', getGovernanceDrift, 60_000);
  const principles = useResource('原则语料', getGovernancePrinciples, 120_000);
  // 「谁在守」这件事由别处的写改动：认领、交还、名册订正都落在 governance 前缀下。
  // 派活与收尾改动的是任务面，而这一面的漂移读的正是任务账。三份一起重读。
  useKindTickedRefresh(
    ['tasks', resourceKey('governance')],
    [faces.refresh, drift.refresh, principles.refresh]
  );

  const census = faces.data?.census;
  const readings = drift.data?.readings ?? [];
  const driftByFace = new Map(readings.map((r) => [r.face_id, r]));

  // 判词各几个：散在表的一列里，读者数不出「十三个面里几个红」，而这一面要回答的
  // 恰恰是这一句。unread 单独成一段，不并进 on_target——那是把「没人读」画成「没事」。
  const byState = new Map<string, StripSegment>();
  for (const reading of readings) {
    const seen = byState.get(reading.state);
    if (seen) {
      seen.value += 1;
    } else {
      byState.set(reading.state, { label: reading.state, value: 1, tone: stateTone(reading.state) });
    }
  }

  // 规则按名字收拢：一个面红在哪条上是逐行的，看不出这条规则是不是整片都在犯。
  const byRule = new Map<string, { label: string; value: number; tone: Tone }>();
  for (const reading of readings) {
    for (const [rule, tone] of [
      ...reading.red_rules.map((r) => [r, 'danger'] as const),
      ...reading.yellow_rules.map((r) => [r, 'warn'] as const),
    ]) {
      const seen = byRule.get(rule);
      if (seen) {
        seen.value += 1;
      } else {
        byRule.set(rule, { label: rule, value: 1, tone });
      }
    }
  }

  return (
    <ViewFrame
      title="治理"
      meta={census ? `${census.faces} 个面 · ${census.cross} 条横切 · 无人守 ${census.unstaffed} · 过期 ${census.expired}` : '按 60 秒重读'}
    >
      {census && (
        <Readings from={faces} source="GET /api/v1/governance/faces 的 census 一栏">
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="面" value={census.faces} />
          <Reading label="横切线" value={census.cross} />
          <Reading label="无人守" value={census.unstaffed} tone={census.unstaffed ? 'warn' : 'ok'} />
          <Reading label="守约过期" value={census.expired} tone={census.expired ? 'danger' : 'ok'} />
          {principles.data && (
            <>
              <Reading
                label="原则已强制"
                from={principles}
                source="GET /api/v1/governance/principles 的 census 一栏"
                caveat="三个数是语料自己数的：强制、未强制、从未判读，相加是语料条数，不是面数。"
                value={principles.data.census.enforced}
                tone="ok"
              />
              <Reading label="未强制" from={principles} source="GET /api/v1/governance/principles 的 census 一栏" value={principles.data.census.unenforced} />
              <Reading
                label="从未判读"
                from={principles}
                source="GET /api/v1/governance/principles 的 census 一栏"
                value={principles.data.census.unmeasured}
                tone={principles.data.census.unmeasured ? 'warn' : 'plain'}
              />
            </>
          )}
        </div>
        </Readings>
      )}
      {readings.length > 0 && (
        <div className="border-b border-[var(--line)]">
          <Strip
            label={`这 ${readings.length} 个面现在各判成什么`}
            note="判词来自 GET /api/v1/governance/drift；「没读到」是这一轮没人读这一面，不是它没问题"
            segments={[...byState.values()].sort((a, b) => b.value - a.value)}
          />
          <BarList
            label="哪条规则在最多的面上成立"
            note="同一个面上同一条规则只记一次；红规则挡变更，黄规则只报告"
            rows={[...byRule.values()].sort((a, b) => b.value - a.value)}
            empty="这一轮没有任何规则成立"
          />
        </div>
      )}
      {faces.error ? (
        <ReadError what="面的清单" error={faces.error} onRetry={faces.refresh} />
      ) : !faces.data && faces.loading ? (
        <Loading what="面的清单" />
      ) : !faces.data || faces.data.faces.length === 0 ? (
        <Empty>这个部署还没有登记任何面。</Empty>
      ) : (
        <Table
          columns={[
            { key: 'id', title: '面', width: '230px', render: (f) => (
              <span className="mono text-[11px]">{f.face_id}</span>
            ) },
            { key: 'name', title: '名字', width: '150px', render: (f) => f.spec.display_name },
            { key: 'kind', title: '类型', width: '80px', render: (f) => (
              <span className="text-[11px] text-[var(--muted)]">{f.spec.kind}</span>
            ) },
            { key: 'owner', title: '所有者', width: '160px', render: (f) => {
              const reading = driftByFace.get(f.face_id);
              const owner = reading?.owner ?? f.owner_unit;
              const source = reading?.owner_source;
              return (
                <span className="text-[11px]">
                  <span className="mono">{owner ?? '—'}</span>
                  {source && <span className="ml-1 text-[var(--muted)]">（{source === 'declared' ? '声明' : '实测'}）</span>}
                </span>
              );
            } },
            { key: 'state', title: '判词', width: '120px', render: (f) => {
              const reading = driftByFace.get(f.face_id);
              return reading ? <Status value={reading.state} /> : <span className="text-[11px] text-[var(--muted)]">没有实测</span>;
            } },
            { key: 'why', title: '依据', width: '240px', render: (f) => {
              const reading = driftByFace.get(f.face_id);
              if (!reading) return <span className="text-[11px] text-[var(--muted)]">—</span>;
              const rules = [...reading.red_rules, ...reading.yellow_rules];
              return (
                <span className="text-[11px] text-[var(--muted)]" title={reading.detail ?? ''}>
                  {reading.cause ? `${reading.cause} · ` : ''}
                  {reading.detail || rules.join(', ')}
                </span>
              );
            } },
            { key: 'vacancy', title: '空位到', width: '110px', render: (f) => <Moment iso={f.spec.vacancy_until} /> },
            { key: 'updated', title: '更新', width: '100px', render: (f) => <Moment iso={f.updated_at} /> },
          ]}
          rows={faces.data.faces}
          rowKey={(f) => f.face_id}
        />
      )}

      {principles.data && (
        <section className="border-t border-[var(--line)]">
          <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
            原则语料
          </h2>
          <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">
            检查档位：形状 {principles.data.check_kinds.shape} · 树 {principles.data.check_kinds.tree} · 现场{' '}
            {principles.data.check_kinds.live} · 人判 {principles.data.check_kinds.judgement} · 未声明{' '}
            {principles.data.check_kinds.undeclared}。「人判」是本来就不该有机器的门，
            「未声明」是还没人说过该怎么查——两件事，别加在一起。
          </p>
          <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">
            被拒过的原则 {principles.data.refused_principles} 条，累计拒绝{' '}
            {principles.data.refusals_total} 次。下表是次数最多的几条，两个总数是全集口径。
          </p>
          {principles.data.refused.length > 0 && (
            <Table
              columns={[
                { key: 'slug', title: '原则', width: '260px', render: (r) => (
                  <span className="text-[12px]" title={r.slug}>
                    {r.lead ?? r.slug}
                  </span>
                ) },
                { key: 'state', title: '判读', width: '100px', render: (r) => (
                  <span className="text-[11px]" style={{ color: stateColor(r.state) }}>
                    {r.state ?? '从未判读'}
                  </span>
                ) },
                { key: 'where', title: '会在哪儿红', width: '230px', render: (r) => (
                  <span className="mono text-[10px] text-[var(--muted)]" title={r.where_red ?? ''}>
                    {r.where_red ?? '还没人说过'}
                  </span>
                ) },
                { key: 'gate', title: '门', width: '200px', render: (r) => (
                  <span className="mono text-[10px] text-[var(--muted)]">
                    {[r.host_gate, r.platform_gate].filter(Boolean).join(' · ') || '—'}
                  </span>
                ) },
                { key: 'carrier', title: '载体', width: '230px', render: (r) => (
                  <span className="mono text-[10px] text-[var(--muted)]" title={r.carrier_path ?? ''}>
                    {r.carrier_path ?? '—'}
                  </span>
                ) },
                { key: 'n', title: '拒绝', width: '70px', render: (r) => (
                  <span className="mono text-[11px]">{r.refusals}</span>
                ) },
                { key: 'at', title: '最近一次', width: '110px', render: (r) => <Moment iso={r.last_refused_at} /> },
              ]}
              rows={principles.data.refused}
              rowKey={(r) => r.slug}
            />
          )}
        </section>
      )}
    </ViewFrame>
  );
}

/**
 * 判词在分布条上的色，与表里 `Status` 用的是同一套判据：四条红规则占红，三条黄
 * 规则占黄，`on_target` 是绿的，`unread` 是灰的——灰不是「好」，是「没人读」。
 */
function stateTone(state: string): Tone {
  switch (state) {
    case 'on_target':
      return 'ok';
    case 'owner_idle':
    case 'witness_is_owner':
    case 'deputy_overlap':
      return 'warn';
    case 'unread':
      return 'muted';
    default:
      return 'danger';
  }
}

/** 判读状态的颜色：已强制是好的，未强制要人看，退役只是历史。 */
function stateColor(state: string | null | undefined): string {
  switch (state) {
    case 'enforced':
      return 'var(--ok)';
    case 'unenforced':
      return 'var(--warn)';
    default:
      return 'var(--muted)';
  }
}
