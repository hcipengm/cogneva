import { useState, type ReactNode } from 'react';
import {
  createProject,
  createProjectTask,
  downloadProjectCsv,
  getProjectAudit,
  moveTask,
  saveTaskPlan,
  updateTask,
  type BoardCard,
  type BoardColumnId,
  type ProjectRow,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Button, Empty, Loading, Moment, ReadError } from '@/ui/primitives';

/**
 * 项目面的写入口。
 *
 * 读面能把一棵树、一块看板、一张甘特、一份挣值画出来，而客户看到的第一件事
 * 往往是「这棵树拆错了」。写入口与读面放在同一屏上，是因为它们在产品里是同一
 * 件事：看见偏差的那一刻正是要改的那一刻。分成两个界面，改的人要先去别处找到
 * 同一个对象，再回来核对改对了没有。
 *
 * 每一个写都自带拒绝的读法。服务端的拒绝带一个稳定的 `code`（跨项目父节点、
 * 未知父节点、成环、列满、没有验收标准、租户说不清），界面按 `code` 分支，不按
 * 那句话——句子会改，码是契约。
 */

/** 把一次拒绝读成「码 ＋ 人话」。读不出 JSON 时原样给出，不吞掉。 */
export function refusalText(e: unknown): string {
  const raw = (e as { message?: string }).message;
  if (!raw) return '请求失败';
  try {
    const parsed = JSON.parse(raw) as { code?: string; message?: string; error?: string };
    const human = parsed.message ?? parsed.error ?? raw;
    return parsed.code ? `${human}（${parsed.code}）` : human;
  } catch {
    return raw;
  }
}

function Field({
  label,
  children,
  hint,
}: {
  label: string;
  children: ReactNode;
  hint?: string;
}) {
  return (
    <label className="flex flex-col gap-0.5">
      <span className="text-[10px] text-[var(--muted)]">{label}</span>
      {children}
      {hint && <span className="text-[10px] text-[var(--muted)]">{hint}</span>}
    </label>
  );
}

const INPUT_CLASS =
  'mono w-full border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] outline-none focus:border-[var(--brass)]';

/**
 * 新建项目。
 *
 * 建项目不再等于「提交一次 goal 再等分解」：那条路要 50–90 秒，还会在树里长出
 * 十几条别人写的活。建项目是范围管理的第一步，它本身不该顺带产生工作。
 */
export function NewProjectForm({ onCreated }: { onCreated: () => void }) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [workspace, setWorkspace] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    const trimmed = name.trim();
    if (!trimmed) return;
    setBusy(true);
    setError(null);
    try {
      await createProject({
        name: trimmed,
        workspace_id: workspace.trim() || undefined,
      });
      setName('');
      setWorkspace('');
      setOpen(false);
      onCreated();
    } catch (e) {
      setError(refusalText(e));
    } finally {
      setBusy(false);
    }
  };

  if (!open) {
    return <Button onClick={() => setOpen(true)}>＋ 新建项目</Button>;
  }

  return (
    <div className="flex flex-wrap items-end gap-2 border border-[var(--line)] p-2">
      <Field label="项目名">
        <input
          autoFocus
          value={name}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') void submit();
            if (e.key === 'Escape') setOpen(false);
          }}
          placeholder="这个名字就是根任务在图上的标题"
          className={INPUT_CLASS}
          style={{ color: 'var(--paper)' }}
        />
      </Field>
      <Field
        label="归属租户"
        hint="留空时：你只属于一个 workspace 就继承它，属于多个就必须写清楚"
      >
        <input
          value={workspace}
          onChange={(e) => setWorkspace(e.target.value)}
          placeholder="workspace id（可留空）"
          className={INPUT_CLASS}
          style={{ color: 'var(--paper)' }}
        />
      </Field>
      <Button tone="accent" onClick={() => void submit()} disabled={busy || !name.trim()}>
        {busy ? '建…' : '建'}
      </Button>
      <Button onClick={() => setOpen(false)} disabled={busy}>
        取消
      </Button>
      {error && (
        <p className="mono w-full text-[11px]" style={{ color: 'var(--danger)' }}>
          {error}
        </p>
      )}
    </div>
  );
}

/**
 * 在项目里建一条活，可以指定挂在谁下面。
 *
 * 验收标准是一个多行框，一行一条：没有它，这条活的「做完」没有判据，计划基线
 * 也写不上去——两件事在服务端是同一道判据的上下游。
 */
export function NewTaskForm({
  project,
  parentTaskId,
  parentTitle,
  onCreated,
  onCancel,
}: {
  project: ProjectRow;
  /** 不给就是项目的顶层。 */
  parentTaskId?: string;
  parentTitle?: string;
  onCreated: () => void;
  onCancel?: () => void;
}) {
  const [title, setTitle] = useState('');
  const [acceptance, setAcceptance] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    const trimmed = title.trim();
    if (!trimmed) return;
    setBusy(true);
    setError(null);
    try {
      await createProjectTask(project.id, {
        title: trimmed,
        parent_task_id: parentTaskId ?? null,
        acceptance: acceptance
          .split('\n')
          .map((s) => s.trim())
          .filter(Boolean),
      });
      setTitle('');
      setAcceptance('');
      onCreated();
      onCancel?.();
    } catch (e) {
      setError(refusalText(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="border border-[var(--line)] p-2">
      <div className="mb-1 text-[10px] text-[var(--muted)]">
        {parentTaskId
          ? `挂在「${parentTitle ?? parentTaskId}」下面`
          : `作为「${project.id}」下的顶层活`}
      </div>
      <Field label="标题">
        <input
          autoFocus
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') void submit();
            if (e.key === 'Escape') onCancel?.();
          }}
          placeholder="这条活叫什么"
          className={INPUT_CLASS}
          style={{ color: 'var(--paper)' }}
        />
      </Field>
      <div className="mt-1">
        <Field label="验收标准" hint="一行一条。留空也能建，但这条活之后写不了计划基线。">
          <textarea
            value={acceptance}
            onChange={(e) => setAcceptance(e.target.value)}
            rows={2}
            placeholder="怎么算做完"
            className={`${INPUT_CLASS} resize-none`}
            style={{ color: 'var(--paper)' }}
          />
        </Field>
      </div>
      <div className="mt-1 flex items-center gap-2">
        <Button tone="accent" onClick={() => void submit()} disabled={busy || !title.trim()}>
          {busy ? '建…' : '建这条活'}
        </Button>
        {onCancel && (
          <Button onClick={onCancel} disabled={busy}>
            取消
          </Button>
        )}
      </div>
      {error && (
        <p className="mono mt-1 text-[11px]" style={{ color: 'var(--danger)' }}>
          {error}
        </p>
      )}
    </div>
  );
}

/**
 * 把一条活的父节点改掉，或者提到顶层。
 *
 * 三态在这里是一条选择：不改、提到顶层、挂到某个 id 下。做成一个输入框而不是
 * 一个下拉，是因为可选的父节点常常不在这一屏上（树很大，只画了前几百行）；
 * 强行给一个下拉，等于把「能挂到哪儿」限制在「这一屏里有什么」。
 */
export function ReparentControl({
  taskId,
  parentTaskId,
  onDone,
}: {
  taskId: string;
  parentTaskId: string | null;
  onDone: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [target, setTarget] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const apply = async (next: string | null) => {
    setBusy(true);
    setError(null);
    try {
      await updateTask(taskId, { parent_task_id: next });
      setOpen(false);
      setTarget('');
      onDone();
    } catch (e) {
      setError(refusalText(e));
    } finally {
      setBusy(false);
    }
  };

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="mono text-[10px] text-[var(--muted)] hover:text-[var(--paper)]"
        title={parentTaskId ? `现在的父节点是 ${parentTaskId}` : '现在挂在顶层'}
      >
        改归属
      </button>
    );
  }

  return (
    <span className="flex items-center gap-1">
      <input
        autoFocus
        value={target}
        onChange={(e) => setTarget(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter') void apply(target.trim() || null);
          if (e.key === 'Escape') setOpen(false);
        }}
        placeholder="新父任务 id（留空＝提到顶层）"
        className="mono w-52 border border-[var(--line)] bg-[var(--ink-900)] px-1 py-0.5 text-[10px] outline-none focus:border-[var(--brass)]"
        style={{ color: 'var(--paper)' }}
      />
      <Button onClick={() => void apply(target.trim() || null)} disabled={busy}>
        改
      </Button>
      <Button onClick={() => setOpen(false)} disabled={busy}>
        取消
      </Button>
      {error && (
        <span className="mono text-[10px]" style={{ color: 'var(--danger)' }}>
          {error}
        </span>
      )}
    </span>
  );
}

/**
 * 看板卡片上能去的下一列。
 *
 * 与状态机的准入是同一条：待办→排期、排期→执行、执行→完成/失败、任意→取消、
 * 失败→待办重试。把六列全列出来，读者点下去的多数是拒绝，而拒绝的句子还要
 * 教他一次「只能挪到隔壁」。
 */
export function nextColumns(status: string): BoardColumnId[] {
  switch (status) {
    case 'pending':
      return ['scheduled', 'cancelled'];
    case 'scheduled':
      return ['running', 'cancelled'];
    case 'running':
      return ['completed', 'failed', 'cancelled'];
    case 'failed':
      return ['pending', 'cancelled'];
    default:
      return [];
  }
}

const COLUMN_LABEL: Record<string, string> = {
  pending: '待办',
  scheduled: '排期',
  running: '执行',
  completed: '完成',
  failed: '失败',
  cancelled: '取消',
};

/** 挪一张卡。只有合法的下一列出现，非法的一列都不画。 */
export function MoveCardControl({
  card,
  onDone,
}: {
  card: BoardCard;
  onDone: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const targets = nextColumns(card.status);

  const move = async (to: BoardColumnId) => {
    setBusy(true);
    setError(null);
    try {
      await moveTask(card.id, to);
      onDone();
    } catch (e) {
      setError(refusalText(e));
    } finally {
      setBusy(false);
    }
  };

  if (targets.length === 0) {
    return <span className="text-[10px] text-[var(--muted)]">这一列是终态</span>;
  }

  return (
    <span className="flex flex-wrap items-center gap-1">
      {targets.map((to) => (
        <button
          key={to}
          type="button"
          disabled={busy}
          onClick={() => void move(to)}
          className="mono border border-[var(--line)] px-1 text-[10px] text-[var(--paper-dim)] hover:border-[var(--brass-dim)] hover:text-[var(--brass)] disabled:opacity-40"
          title={`挪到「${COLUMN_LABEL[to] ?? to}」`}
        >
          → {COLUMN_LABEL[to] ?? to}
        </button>
      ))}
      {error && (
        <span className="mono text-[10px]" style={{ color: 'var(--danger)' }}>
          {error}
        </span>
      )}
    </span>
  );
}

/**
 * 写一条计划基线：起止时间与打算花多少 token。
 *
 * 三个格子都不许留空着当 0：一份没有预算的基线在挣值里贡献一个恒为零的计划
 * 价值，读面分不出「还没排期」与「排了但没给钱」。所以预算这一格要么填一个数，
 * 要么不带——不带时服务端沿用上一版基线，连上一版都没有就被拒。
 */
export function PlanForm({
  taskId,
  onDone,
  onCancel,
}: {
  taskId: string;
  onDone: () => void;
  onCancel: () => void;
}) {
  const [start, setStart] = useState('');
  const [end, setEnd] = useState('');
  const [budget, setBudget] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      await saveTaskPlan(taskId, {
        planned_start: start ? new Date(start).toISOString() : null,
        planned_end: end ? new Date(end).toISOString() : null,
        budget_tokens: budget.trim() ? Number(budget.trim()) : null,
      });
      onDone();
      onCancel();
    } catch (e) {
      setError(refusalText(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="border border-[var(--line)] p-2">
      <div className="mb-1 mono text-[10px] text-[var(--muted)]">给自己排个期：{taskId}</div>
      <div className="flex flex-wrap items-end gap-2">
        <Field label="计划开始">
          <input
            type="datetime-local"
            value={start}
            onChange={(e) => setStart(e.target.value)}
            className={INPUT_CLASS}
            style={{ color: 'var(--paper)' }}
          />
        </Field>
        <Field label="计划结束">
          <input
            type="datetime-local"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
            className={INPUT_CLASS}
            style={{ color: 'var(--paper)' }}
          />
        </Field>
        <Field label="预算 token" hint="留空＝沿用上一版基线">
          <input
            value={budget}
            onChange={(e) => setBudget(e.target.value)}
            inputMode="numeric"
            placeholder="例如 200000"
            className={INPUT_CLASS}
            style={{ color: 'var(--paper)' }}
          />
        </Field>
        <Button tone="accent" onClick={() => void submit()} disabled={busy}>
          {busy ? '写…' : '写基线'}
        </Button>
        <Button onClick={onCancel} disabled={busy}>
          取消
        </Button>
      </div>
      {error && (
        <p className="mono mt-1 text-[11px]" style={{ color: 'var(--danger)' }}>
          {error}
        </p>
      )}
    </div>
  );
}

/** 每行一个「写基线」的开合按钮。 */
export function PlanButton({ taskId, onDone }: { taskId: string; onDone: () => void }) {
  const [open, setOpen] = useState(false);
  if (open) {
    return <PlanForm taskId={taskId} onDone={onDone} onCancel={() => setOpen(false)} />;
  }
  return (
    <button
      type="button"
      onClick={() => setOpen(true)}
      className="mono text-[10px] text-[var(--muted)] hover:text-[var(--paper)]"
    >
      排期
    </button>
  );
}

/** 导出当前视图。四个视图共用一列按钮，导出的列与读面同源。 */
export function ExportCsvButton({
  projectId,
  view,
}: {
  projectId: string;
  view: 'wbs' | 'board' | 'gantt' | 'evm';
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = async () => {
    setBusy(true);
    setError(null);
    try {
      await downloadProjectCsv(projectId, view);
    } catch (e) {
      setError(refusalText(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <span className="flex items-center gap-1">
      <Button onClick={() => void run()} disabled={busy}>
        {busy ? '导出中…' : '导出 CSV'}
      </Button>
      {error && (
        <span className="mono text-[10px]" style={{ color: 'var(--danger)' }}>
          {error}
        </span>
      )}
    </span>
  );
}

/**
 * 这个项目的改动时间线。
 *
 * 审计面的成品不是一张有 DDL 的表，是「谁在什么时候把哪张卡改成了什么」这句话
 * 有人答得上来。所以这一屏读的是项目的审计行，不是一个全局的事件流——客户问的
 * 永远是「我这个项目改过什么」。
 */
export function AuditTimeline({ projectId }: { projectId: string }) {
  const [limit, setLimit] = useState(50);
  const audit = useResource(
    `改动时间线（${projectId}）`,
    () => getProjectAudit(projectId, limit),
    30_000
  );
  // 时间线是一份快照，而它记的正是别处刚发生的事——不订帧的话，一个人在自己这一屏
  // 改完一张卡，切到时间线看到的是三十秒前的那一版，于是他会以为那一改没写进去。
  // 订的是项目面的那一族：任务状态帧改的是树，REST 侧的写落在 projects/tasks 前缀下，
  // 两者都该让这一份重读。
  useKindTickedRefresh(
    [resourceKey('projects'), resourceKey('tasks')],
    audit.refresh
  );

  if (audit.error) {
    return <ReadError what="改动时间线" error={audit.error} onRetry={audit.refresh} />;
  }
  if (!audit.data && audit.loading) return <Loading what="改动时间线" />;
  const rows = audit.data?.rows ?? [];

  if (rows.length === 0) {
    return (
      <Empty>
        这个项目还没有留下任何改动。新建项目、建任务、挪卡、改归属、写基线都会在这里留一行。
      </Empty>
    );
  }

  return (
    <>
      <div className="flex items-center gap-2 border-b border-[var(--line)] px-4 py-1.5 text-[11px]">
        <span className="text-[var(--muted)]">最近 {rows.length} 条</span>
        <Button onClick={() => setLimit((n) => Math.min(500, n + 100))} disabled={limit >= 500}>
          再多读 100 条
        </Button>
        <Button onClick={audit.refresh}>重读</Button>
      </div>
      <div className="overflow-auto">
        {rows.map((r) => (
          <div key={r.id} className="border-b border-[var(--line)] px-4 py-1">
            <div className="flex items-baseline gap-2">
              <span className="mono w-40 shrink-0 text-[10px] text-[var(--paper-dim)]">
                {r.action}
              </span>
              <span className="mono min-w-0 flex-1 truncate text-[11px]" title={r.resource_id}>
                {r.resource_type} · {r.resource_id}
              </span>
              <span className="mono w-32 shrink-0 text-[10px] text-[var(--muted)]">{r.actor}</span>
              <span className="w-36 shrink-0 text-right text-[10px] text-[var(--muted)]">
                <Moment iso={r.created_at} />
              </span>
            </div>
            {(r.before !== null || r.after !== null) && (
              <div className="mono mt-0.5 truncate text-[10px] text-[var(--muted)]">
                改前 {compact(r.before)} → 改后 {compact(r.after)}
              </div>
            )}
          </div>
        ))}
      </div>
    </>
  );
}

/** 一段改动压成一行。太长的原样截断——点开原文不是这一屏的事。 */
function compact(v: unknown): string {
  if (v === null || v === undefined) return '（空）';
  const text = typeof v === 'string' ? v : JSON.stringify(v);
  return text.length > 80 ? `${text.slice(0, 80)}…` : text;
}
