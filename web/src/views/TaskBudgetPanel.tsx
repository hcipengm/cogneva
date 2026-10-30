import { useCallback, useEffect, useState } from 'react';
import {
  clearTaskBudget,
  getTaskBudget,
  listTaskBudgets,
  setTaskBudget,
  type TaskBudgetReading,
} from '@/api/client';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Button, Empty, Table } from '@/ui/primitives';
import { Meter, type Tone } from '@/ui/plots';

/**
 * 按任务的当日上限：给一件具体的工作写一对 soft / hard，用完网关就拒它，UTC 午夜置零。
 *
 * 与上面那张五级声明表是两件事。那五级互相包含，检查时逐级走级联；任务是工作单位，
 * 不属于任何一级，所以它自成一张表——把任务塞进那张级联表里，「这一格还能花多少」
 * 与「这件工作还能花多少」就会共用一条读法，两种关系被读成一种。
 *
 * 没有声明过预算的任务**没有上限**，不是「上限为零」：所以这里读不到的那一行说的是
 * 「没人管它」，而放行与否在这份面板之外。
 *
 * 表给「是哪几条、上限多少」，下面那组 Meter 给「谁快用完了」——每条的量尺是它自己的
 * hard，因为任务之间的上限本来就不同。
 */
export function TaskBudgetPanel() {
  const [roster, setRoster] = useState<TaskBudgetReading[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [taskId, setTaskId] = useState('');
  const [soft, setSoft] = useState('');
  const [hard, setHard] = useState('');
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const r = await listTaskBudgets();
      setRoster(r.tasks);
      setError(null);
    } catch (e) {
      // 名册读不出来时不把表清空成「一条都没有」：空表读作「没人给任何任务
      // 配过上限」，而真相是这一拍读不到。
      setError(describe(e));
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // 这张表也会在**别人**写的时候变：一次成功的写落在 `/api/v1/admin/...` 下，网关按
  // 前缀在总线上落一帧（`resource:admin`），这里订它。没有这一订，两条命令行的
  // `task-budget set` 之后这张表要等到读者自己按「重读」才动——屏幕停在上一拍，而
  // 它读的是服务端真实的读数，看起来像预算没写进去。
  //
  // 帧只触发重读，不碰输入框：用户可能正把一对上限改到一半。写完之后的对齐在 `run`
  // 里做，那一次用户是看着结果按下去的。
  useKindTickedRefresh([resourceKey('admin')], () => {
    void refresh();
  });

  const target = taskId.trim();

  const run = async (action: () => Promise<TaskBudgetReading>, done: string) => {
    setBusy(true);
    setNote(null);
    try {
      const next = await action();
      setNote(`${done}：${next.task_id}`);
      // 写完之后用服务端读回来的那一份对齐输入框，不用自己手里的数：把输入当成
      // 结果，就等于替服务端圆谎。
      if (next.budget) {
        setSoft(String(next.budget.soft_limit));
        setHard(String(next.budget.hard_limit));
      }
      await refresh();
    } catch (e) {
      setNote(describe(e));
    } finally {
      setBusy(false);
    }
  };

  const write = () => {
    const softLimit = Number(soft);
    const hardLimit = Number(hard);
    if (!Number.isFinite(softLimit) || !Number.isFinite(hardLimit)) {
      setNote('soft 与 hard 都得是数字：只给一半会让另一半沿用旧值，所以两个都要写');
      return;
    }
    if (softLimit < 0 || hardLimit < 0) {
      setNote('上限是 token 数，不能为负');
      return;
    }
    void run(
      () =>
        setTaskBudget(target, {
          soft_limit: Math.floor(softLimit),
          hard_limit: Math.floor(hardLimit),
        }),
      '已写入'
    );
  };

  const canAct = target.length > 0 && !busy;

  return (
    <section className="border-b border-[var(--line)]">
      <header className="flex flex-wrap items-center gap-3 px-4 py-2">
        <h2 className="text-[12px]">按任务的当日上限</h2>
        <span className="text-[11px] text-[var(--muted)]">
          一件工作自己的一份额度；没声明的任务没有上限，不是零
        </span>
        <div className="ml-auto">
          <Button onClick={() => void refresh()} disabled={busy}>
            重读
          </Button>
        </div>
      </header>

      {(note || error) && (
        <p
          className="px-4 pb-2 text-[11px]"
          style={{ color: error ? 'var(--danger)' : 'var(--muted)' }}
        >
          {error ?? note}
        </p>
      )}

      {roster === null ? (
        <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">正在读名册…</p>
      ) : roster.length === 0 ? (
        <Empty>还没有任何任务声明过上限。填一个任务名与一对上限，它才开始被管。</Empty>
      ) : (
        <Table
          columns={[
            {
              key: 'task',
              title: '任务',
              render: (r) => <span className="mono text-[11px]">{r.task_id}</span>,
            },
            {
              key: 'limits',
              title: '上限 soft / hard',
              width: '200px',
              render: (r) =>
                r.budget ? (
                  <span className="mono text-[11px]">
                    {r.budget.soft_limit.toLocaleString()} /{' '}
                    {r.budget.hard_limit.toLocaleString()}
                  </span>
                ) : (
                  <span className="text-[11px] text-[var(--warn)]">读不出来</span>
                ),
            },
            {
              key: 'spent',
              title: '今天已花',
              width: '140px',
              render: (r) =>
                r.spent_today === null ? (
                  <span className="text-[11px] text-[var(--warn)]">读不出来</span>
                ) : (
                  <span className="mono text-[11px]">{r.spent_today.toLocaleString()}</span>
                ),
            },
            {
              key: 'remaining',
              title: '剩余',
              width: '140px',
              render: (r) =>
                r.budget && r.spent_today !== null ? (
                  <span className="mono text-[11px]">
                    {Math.max(0, r.budget.hard_limit - r.spent_today).toLocaleString()}
                  </span>
                ) : (
                  <span className="text-[11px] text-[var(--muted)]">—</span>
                ),
            },
          ]}
          rows={roster}
          rowKey={(r) => r.task_id}
        />
      )}

      {roster !== null && roster.length > 0 && (
        <div className="pb-1">
          <p className="px-4 pt-1 text-[11px] text-[var(--paper-dim)]">
            今天各件任务用掉它自己那份硬上限的多少
          </p>
          <p className="px-4 pb-1 text-[11px] text-[var(--muted)]">
            每条的量尺是它自己的 hard：任务之间的上限本来就不同，并排按同一把尺子归一，
            会让「只花了十个」与「快花完了」看起来一样长。
          </p>
          {roster.map((r) => {
            // 上限与花销任缺一样就画不出「离满还差多少」：那种情形交给 Meter 画斜纹
            // 并写明原因，而不是拿一个 0 顶上去——空条读作「还没花」。
            const gap = meterProps(r);
            return (
              <Meter
                key={r.task_id}
                label={r.task_id}
                value={gap.value}
                max={gap.max}
                note={gap.note}
                unmeasurable={gap.unmeasurable}
                tone={gap.tone}
              />
            );
          })}
        </div>
      )}

      {roster?.some((r) => r.read_error) && (
        <p className="px-4 py-2 text-[11px]" style={{ color: 'var(--warn)' }}>
          有名册项这一拍读不出来（{roster.find((r) => r.read_error)?.read_error}
          ）。它仍在名册上：丢掉一行会让「这件工作没有预算」与「这一拍读不到」同形。
        </p>
      )}

      <div className="flex flex-wrap items-center gap-2 px-4 py-3">
        <input
          value={taskId}
          onChange={(e) => setTaskId(e.target.value)}
          placeholder="任务 id"
          className="mono px-2 py-0.5 text-[11px]"
          style={{
            background: 'var(--paper)',
            color: 'var(--ink-1000)',
            border: '1px solid var(--line)',
          }}
        />
        <label className="text-[11px] text-[var(--muted)]">soft</label>
        <input
          value={soft}
          onChange={(e) => setSoft(e.target.value)}
          inputMode="numeric"
          className="mono w-24 px-2 py-0.5 text-[11px]"
          style={{
            background: 'var(--paper)',
            color: 'var(--ink-1000)',
            border: '1px solid var(--line)',
          }}
        />
        <label className="text-[11px] text-[var(--muted)]">hard</label>
        <input
          value={hard}
          onChange={(e) => setHard(e.target.value)}
          inputMode="numeric"
          className="mono w-24 px-2 py-0.5 text-[11px]"
          style={{
            background: 'var(--paper)',
            color: 'var(--ink-1000)',
            border: '1px solid var(--line)',
          }}
        />
        <Button onClick={write} disabled={!canAct}>
          写入
        </Button>
        <Button
          onClick={() => void run(() => getTaskBudget(target), '当前')}
          disabled={!canAct}
        >
          读这一件
        </Button>
        <Button
          onClick={() => void run(() => clearTaskBudget(target), '已撤销')}
          disabled={!canAct}
        >
          撤销
        </Button>
      </div>

      <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
        撤销之后这件工作回到「没人管」——不是「上限为零」，所以它又会畅通无阻。今天的计数不清零：
        改上限不是给这件工作发一笔新额度，要让花销从头算得换一个任务 id。这一面的计数由网关逐个
        LLM 调用记，所以它是真的用量，不是声明。
      </p>
    </section>
  );
}

/**
 * 一行名册对应的那一格「离满还差多少」。
 *
 * 三种读不全的情形都不许折算成数字：没上限、没花销读数、上限恰好是 0，各自有各自的
 * 意思，只有第一种才是「畅通无阻」。
 */
function meterProps(r: TaskBudgetReading): {
  value: number | null;
  max: number;
  note?: string;
  unmeasurable?: string;
  tone: Tone;
} {
  if (!r.budget) {
    return {
      value: null,
      max: 0,
      unmeasurable: '这一拍读不到它声明的上限，画不出离满还差多少',
      tone: 'muted',
    };
  }
  const hard = r.budget.hard_limit;
  if (hard <= 0) {
    return {
      value: null,
      max: 0,
      unmeasurable: '硬上限是 0：这件工作的每一次调用都会被拒，图上没有可画的一段',
      tone: 'muted',
    };
  }
  if (r.spent_today === null) {
    return {
      value: null,
      max: hard,
      unmeasurable: '这一拍读不到它今天的花销，画不出离满还差多少',
      tone: 'muted',
    };
  }
  const spent = r.spent_today;
  return {
    value: spent,
    max: hard,
    note: `${spent.toLocaleString()} / ${hard.toLocaleString()}`,
    // 越过软上限是「该收尾了」的状态，用保留色；没越过就是纯幅度。
    tone: spent >= hard ? 'danger' : spent >= r.budget.soft_limit ? 'warn' : 'magnitude',
  };
}

function describe(e: unknown): string {
  if (typeof e === 'object' && e !== null && 'message' in e) {
    return String((e as { message: unknown }).message);
  }
  return String(e);
}
