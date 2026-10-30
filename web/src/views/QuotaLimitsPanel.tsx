import { useState } from 'react';
import {
  clearQuotaLimits,
  getQuotaLimits,
  QUOTA_SCOPES,
  setQuotaLimits,
  type QuotaLimitsReading,
  type QuotaScopeName,
} from '@/api/client';
import { Button, Reading, Readings } from '@/ui/primitives';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';

/**
 * 日上限的管理面：给某一格（人 / 工作空间 / 团队 / 组织 / 全局）写下
 * 一对 soft / hard，或把覆盖删掉、让它回到默认值。
 *
 * 这一面只写声明，不碰计数。两个读法必须同时给出，因为它们答的不是同一个问题：
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

  const target = targetId.trim();
  const canAct = target.length > 0 && !busy;

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

      {reading ? (
        <Readings
          from={{ what: `${SCOPE_LABELS[scope]} · ${reading.target_id}`, at: readAt }}
          source={`GET /api/v1/quota/limits/${reading.scope}/${reading.target_id}`}
          caveat="「声明」是有人为这一格写过的那一对，「生效」是检查此刻真正会用的那一对——没有覆盖时后者就是默认值。"
        >
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
        <Button onClick={write} disabled={!canAct}>
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
        这是一张声明表：写成什么样，下一节的刻度就是什么样。它现在量不到用量——本部署没有任何一方
        往这套计数器里记数（拦调用的那层已经撤掉），所以「今天已记」读作 0 只说明没人记，不说明没人用。
        花销的真账在观测页的 Token 面，那一侧由网关逐调用写。
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
