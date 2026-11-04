import { useCallback, useEffect, useState } from 'react';
import {
  clearQuotaLimits,
  getQuotaLimits,
  getTokenSpenders,
  QUOTA_SCOPES,
  setQuotaLimits,
  type QuotaLimitsReading,
  type QuotaScopeName,
  type TokenSpenders,
} from '@/api/client';
import { Note } from '@/ui/Note';
import { Button, Reading, Readings } from '@/ui/primitives';
import { Meter } from '@/ui/plots';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';

/**
 * 日上限的管理面：给某一格（人 / 工作空间 / 团队 / 组织 / 全局）写下
 * 一对 soft / hard，或把覆盖删掉、让它回到默认值。
 *
 * 这一面只写声明，不写计数：计数由安全网关按账本投影（今天人与工作空间两格有写者）。
 * 两个读法必须同时给出，因为它们答的不是同一个问题：
 * 声明（`limits`）答「这一格配过没有」，生效（`effective`）答「检查会怎么判」。
 * 没有覆盖时生效值就是默认值——只显示它会让人以为默认值是自己配的，于是
 * 「清除」这个按钮看起来永远没有效果。
 *
 * 每次写和删之后都读回服务端的那一份，而不是把输入框里的数当成结果：服务端会把
 * soft 压到 hard 以内，回显自己算出来的数就等于替它圆谎。
 */
export function QuotaLimitsPanel() {
  const [scope, setScope] = useState<QuotaScopeName>('user');
  const [targetId, setTargetId] = useState('');
  const [soft, setSoft] = useState('');
  const [hard, setHard] = useState('');
  const [reading, setReading] = useState<QuotaLimitsReading | null>(null);
  const [readAt, setReadAt] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  /** 这一级今天谁在花。只有人与工作空间两级有这条路，其余三级是 `null`。 */
  const [spenders, setSpenders] = useState<TokenSpenders | null>(null);
  const [spendersAt, setSpendersAt] = useState<string | null>(null);
  const [spendersError, setSpendersError] = useState<string | null>(null);

  const spendable = scope === 'user' || scope === 'ws';
  const loadSpenders = useCallback(() => {
    if (scope !== 'user' && scope !== 'ws') {
      setSpenders(null);
      setSpendersError(null);
      return;
    }
    void getTokenSpenders(scope).then(
      (next) => {
        setSpenders(next);
        setSpendersAt(new Date().toISOString());
        setSpendersError(null);
      },
      (e) => {
        // 读不出来与「今天没人花」不是一回事：后者是空数组，前者只能说读不到。
        setSpenders(null);
        setSpendersError(describe(e));
      }
    );
  }, [scope]);

  useEffect(() => {
    loadSpenders();
  }, [loadSpenders]);

  const target = targetId.trim();
  const canAct = target.length > 0 && !busy;
  /** 屏幕上此刻的那一对：有覆盖就是声明的那一对，没有就是默认值（生效值）。 */
  const shownPair = reading ? (reading.limits ?? reading.effective) : null;

  const run = async (action: () => Promise<QuotaLimitsReading>) => {
    setBusy(true);
    setError(null);
    try {
      const next = await action();
      setReading(next);
      setReadAt(new Date().toISOString());
      // 读回之后把输入框对齐到声明值：没有覆盖时用生效值起步，改哪一格改哪一格。
      const seed = next.limits ?? next.effective;
      setSoft(String(seed.soft_limit));
      setHard(String(seed.hard_limit));
    } catch (e) {
      setReading(null);
      setError(describe(e));
    } finally {
      setBusy(false);
    }
  };

  // 这一面读的快照也会在**别人**写的时候变：一次成功的 PUT/DELETE 落在
  // `/api/v1/quota/...` 下，网关按前缀在总线上落一帧（`resource:quota`），这里订它。
  // 帧到了只重读、只更新读数与读的时刻，不碰输入框：用户手里那一对数可能正改到一半，
  // 用服务端的值盖上去就是边打字边被改。也不再对齐输入框——对齐只在读回自己那次写的
  // 时候做（`run`），那一次用户是看着结果按下去的。
  useKindTickedRefresh([resourceKey('quota')], () => {
    // 排行与单格读数一起重读：额度帧既可能来自别人给某一行写了上限，也可能只是
    // 投影推进了这一格的计数。排行读失败时不抹掉屏幕上已有的那一份。
    loadSpenders();
    if (!reading || reading.scope !== scope || reading.target_id !== target) return;
    void getQuotaLimits(reading.scope, reading.target_id).then(
      (next) => {
        setReading(next);
        setReadAt(new Date().toISOString());
      },
      () => {
        // 一次帧触发的读失败不抹掉屏幕上已有的读数：它还标着上一次读成的时刻。
      }
    );
  });

  const write = () => {
    const softLimit = Number(soft);
    const hardLimit = Number(hard);
    if (!Number.isFinite(softLimit) || !Number.isFinite(hardLimit)) {
      setError('soft 与 hard 都得是数字；空着不是「不改」，是没写全');
      return;
    }
    if (softLimit < 0 || hardLimit < 0) {
      setError('上限是 token 数，不能为负');
      return;
    }
    void run(() =>
      setQuotaLimits(scope, target, {
        soft_limit: Math.floor(softLimit),
        hard_limit: Math.floor(hardLimit),
      })
    );
  };

  return (
    <section className="border-b border-[var(--line)]">
      <header className="flex flex-wrap items-center gap-3 px-4 py-2">
        <h2 className="text-[12px]">日上限</h2>
        <span className="text-[11px] text-[var(--muted)]">
          一格一对；不给某一格配，它就跟着默认值走
        </span>
        <div className="ml-auto flex items-center gap-2">
          <select
            value={scope}
            onChange={(e) => {
              setScope(e.target.value as QuotaScopeName);
              setReading(null);
            }}
            className="mono px-2 py-0.5 text-[11px]"
            style={{ background: 'var(--paper)', color: 'var(--ink-1000)', border: '1px solid var(--line)' }}
          >
            {QUOTA_SCOPES.map((name) => (
              <option key={name} value={name}>
                {SCOPE_LABELS[name]}
              </option>
            ))}
          </select>
          <input
            value={targetId}
            onChange={(e) => setTargetId(e.target.value)}
            placeholder="这一格的 id"
            className="mono px-2 py-0.5 text-[11px]"
            style={{ background: 'var(--paper)', color: 'var(--ink-1000)', border: '1px solid var(--line)' }}
          />
          <Button onClick={() => void run(() => getQuotaLimits(scope, target))} disabled={!canAct}>
            读
          </Button>
        </div>
      </header>

      {error && (
        <p className="px-4 pb-2 text-[11px]" style={{ color: 'var(--danger)' }}>
          {error}
        </p>
      )}

      {/* 今天谁在花：写入面收的是一个 id，而运维在看见这一列之前不知道该填谁。
          窗口是 UTC 日，与额度判定数的那个日同一个——读出来的排行就是「谁快被拒了」。
          点一行把那格的 id 填进输入框并直接读，从「看见」到「改」不用手抄 id。 */}
      {spendable && (
        <div className="px-4 pb-2">
          <div className="flex flex-wrap items-baseline gap-2 py-1">
            <span className="text-[11px] text-[var(--muted)]">
              今天（UTC 日）在这一级花钱的格子，按花销降序
            </span>
            {spenders && (
              <span className="mono ml-auto text-[11px] text-[var(--muted)]">
                合计 {spenders.total.toLocaleString()} token
                {spendersAt ? ` · 读于 ${new Date(spendersAt).toLocaleTimeString()}` : ''}
              </span>
            )}
          </div>
          {spendersError && (
            <p className="text-[11px]" style={{ color: 'var(--danger)' }}>
              排行读不到：{spendersError}
            </p>
          )}
          {spenders && spenders.spenders.length === 0 && (
            <p className="text-[11px] text-[var(--muted)]">
              今天这一级没有花销，也没有被上限拒掉的调用。
            </p>
          )}
          {spenders && spenders.spenders.length > 0 && (
            <>
              <table className="mono w-full text-[11px]">
                <thead>
                  <tr className="text-left text-[var(--muted)]">
                    <th className="py-0.5 pr-3 font-normal">格子</th>
                    <th className="py-0.5 pr-3 text-right font-normal">今天已花</th>
                    <th className="py-0.5 pr-3 text-right font-normal">今天被拒</th>
                    <th className="py-0.5 pr-3 text-right font-normal">声明的 hard</th>
                    <th className="py-0.5 text-right font-normal">剩余</th>
                  </tr>
                </thead>
                <tbody>
                  {spenders.spenders.map((row) => (
                    <tr key={row.id}>
                      <td className="py-0.5 pr-3">
                        <Note
                          as="button"
                          type="button"
                          className="underline-offset-2 hover:underline"
                          text="把这一格填进上面的输入框并读出来"
                          onClick={() => {
                            setTargetId(row.id);
                            void run(() => getQuotaLimits(scope, row.id));
                          }}
                        >
                          {row.id}
                        </Note>
                      </td>
                      <td className="py-0.5 pr-3 text-right">{row.tokens.toLocaleString()}</td>
                      {/* 花销为零、拒绝数不为零的一行，正是这一面存在的理由：那个人
                          今天每一发都被自己那格上限挡下了，花销排行里只有他一个零。 */}
                      <td
                        className="py-0.5 pr-3 text-right"
                        style={row.refusals > 0 ? { color: 'var(--danger)' } : undefined}
                      >
                        {row.refusals === 0 ? '—' : row.refusals.toLocaleString()}
                      </td>
                      <td className="py-0.5 pr-3 text-right">
                        {row.hard_limit === null ? '—' : row.hard_limit.toLocaleString()}
                      </td>
                      <td className="py-0.5 text-right">
                        {row.remaining === null ? '—' : row.remaining.toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="py-1 text-[11px] text-[var(--muted)]">
                {spenders.ceilings_readable
                  ? '「—」是这一格没人声明过上限，它跟着默认值走；「剩余」按账本当场算，判定读的是每 30 秒覆盖一次的格子，两者可能差最近这几十秒的花销。'
                  : '这个部署没有上限库，读不到任何一格声明过的上限；这一列的空值是「读不到」，不是「没人配过」。'}
                {' '}
                「今天被拒」只数被这一格自己的上限挡下的调用：花销为零而这一列不为零，就是今天每一发都被挡了；任务预算用尽是另一堵墙，不记在这里。
              </p>
            </>
          )}
        </div>
      )}

      {reading ? (
        <Readings
          from={{ what: `${SCOPE_LABELS[scope]} · ${reading.target_id}`, at: readAt }}
          source={`GET /api/v1/quota/limits/${reading.scope}/${reading.target_id}`}
          caveat="「声明」是有人为这一格写过的那一对，「生效」是检查此刻真正会用的那一对——没有覆盖时后者就是默认值。"
        >
          {/* 这一格配的是一对什么数：soft 落在 hard 之内的哪里，中间那截就是 soft
              拦下调用之前预留的余量。用量那条只在「人」与「工作空间」两格画——五级里
              只有它们有投影写者（安全网关按账本把这一格的真实花销写回来），其余三级
              至今没有，把它们画成一根零长度的条就是拿「没量过」冒充「一条都没有」。 */}
          {shownPair !== null && shownPair.hard_limit > 0 && (
            <Meter
              label={reading.limits ? '声明：soft 落在 hard 之内的哪里' : '生效：soft 落在 hard 之内的哪里'}
              value={shownPair.soft_limit}
              max={shownPair.hard_limit}
              note={`${shownPair.soft_limit.toLocaleString()} / ${shownPair.hard_limit.toLocaleString()}`}
            />
          )}
          {reading.enforced && reading.effective.hard_limit > 0 && (
            <Meter
              label="这个人今天已花 / 生效的 hard"
              value={reading.used_today}
              max={reading.effective.hard_limit}
              note={`${reading.used_today.toLocaleString()} / ${reading.effective.hard_limit.toLocaleString()}`}
            />
          )}
          <div className="flex flex-wrap py-2">
            <Reading
              label="声明"
              value={
                reading.limits
                  ? `${reading.limits.soft_limit.toLocaleString()} / ${reading.limits.hard_limit.toLocaleString()}`
                  : '无覆盖'
              }
              note={reading.limits ? 'soft / hard' : '跟着默认值走'}
            />
            <Reading
              label="生效"
              value={`${reading.effective.soft_limit.toLocaleString()} / ${reading.effective.hard_limit.toLocaleString()}`}
              note="soft / hard"
            />
            <Reading label="今天已记" value={reading.used_today.toLocaleString()} />
            <Reading label="剩余" value={reading.remaining.toLocaleString()} />
            <Reading label="这一格" value={<span className="mono text-[11px]">{reading.target_id}</span>} />
            <Reading
              label="执法"
              value={reading.enforced ? '有' : '没有'}
              note={reading.enforced ? '越过之后会拒下一发' : '这一级写不进去'}
            />
          </div>
        </Readings>
      ) : (
        <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">
          选层级、填 id、点「读」，这一格现在声明了什么、检查会怎么判，两个数会分开列出来。
        </p>
      )}

      <div className="flex flex-wrap items-center gap-2 px-4 pb-3">
        <label className="text-[11px] text-[var(--muted)]">soft</label>
        <input
          value={soft}
          onChange={(e) => setSoft(e.target.value)}
          inputMode="numeric"
          className="mono w-24 px-2 py-0.5 text-[11px]"
          style={{ background: 'var(--paper)', color: 'var(--ink-1000)', border: '1px solid var(--line)' }}
        />
        <label className="text-[11px] text-[var(--muted)]">hard</label>
        <input
          value={hard}
          onChange={(e) => setHard(e.target.value)}
          inputMode="numeric"
          className="mono w-24 px-2 py-0.5 text-[11px]"
          style={{ background: 'var(--paper)', color: 'var(--ink-1000)', border: '1px solid var(--line)' }}
        />
        <Button onClick={write} disabled={!canAct || (reading !== null && !reading.enforced)}>
          写入
        </Button>
        <Button onClick={() => void run(() => clearQuotaLimits(scope, target))} disabled={!canAct}>
          清除覆盖
        </Button>
        <span className="text-[11px] text-[var(--muted)]">
          soft 高于 hard 时服务端压到 hard，回显的是它算出来的那一对
        </span>
      </div>

      <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
        「今天已记」是账本的投影：安全网关按「调用方声明的属主，否则是任务提交人」把花销记到
        人那一格，按任务行上的 workspace_id 记到工作空间那一格。越过某一格声明过的 hard 之后，
        下一发会被拒（402，结果词 refused_by_owner_budget，正文的 scope 说明越线的是人或租户，
        观测页的结局分布里单列一行）。没配过的格子不会被默认值拦下，默认值只决定读出来是多少。
        团队 / 组织 / 全局三级今天没有任何一处能把一次调用归到它们名下，写在那里的上限永远
        不会被读到，所以服务端拒写、这一面也给出「没有执法」。逐笔花销的真账在观测页的 Token 面。
      </p>
    </section>
  );
}

const SCOPE_LABELS: Record<QuotaScopeName, string> = {
  user: '个人',
  ws: '工作空间',
  team: '团队',
  org: '组织',
  global: '全局',
};

function describe(e: unknown): string {
  if (typeof e === 'object' && e !== null && 'message' in e) {
    return String((e as { message: unknown }).message);
  }
  return String(e);
}
