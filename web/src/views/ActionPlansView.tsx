import { useState } from 'react';
import {
  createActionPlan,
  deleteActionPlan,
  getActionPlan,
  listActionPlans,
  type ActionPlanTaskView,
  type StoredActionPlanView,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, Moment, ReadError, Table, ViewFrame } from '@/ui/primitives';
import { FormRow, TextArea, TextInput } from '@/ui/forms';
import { Note } from '@/ui/Note';

/**
 * 动作计划面：目标分解出来的那张图——节点、依赖边、以及六条 DAG 规则算出的校验。
 *
 * 此前这条接口只在 API 上。这里补上阅读、提交与删除三条入口。校验摘要（入口点、
 * 出点、关键路径、连通分量）是**服务端算的**，界面不自己数：同一份图两处各算一次，
 * 两处迟早会对不上，而读者无法知道该信哪一个。
 *
 * 提交时有两条约定写在输入旁边，因为服务端不会替人猜：
 * - `edges` 是有向边 {@code [["a","b"]]}，含义是 b 等 a；
 * - `tasks` 里的每个节点要有唯一 `id`，边里出现没定义过的 id 会被 DAG 校验拒绝
 *   （400，原文落在输出格里）。
 *
 * 画面：这一面是一张目标图——节点表、边表、以及服务端算出的关键路径与连通分量。
 * 同一张图用两处布局各画一遍，位置没有含义，读者要读的是「依赖成不成环、关键路径
 * 多长」这类结论，所以这里按边与校验摘要排成表，而不是画一张每次都不一样的图。
 */

function edgeText(edge: [string, string]): string {
  return `${edge[0]} → ${edge[1]}`;
}

export function ActionPlansView() {
  const page = useResource('动作计划', listActionPlans, 30_000);
  const appendOutput = useShell((s) => s.appendOutput);
  const [selected, setSelected] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);

  const [goal, setGoal] = useState('');
  const [tasksJson, setTasksJson] = useState('[]');
  const [edgesJson, setEdgesJson] = useState('[]');
  const [skillsJson, setSkillsJson] = useState('[]');

  const plans = page.data ?? [];
  const chosen = plans.find((p) => p.id === selected) ?? plans[0] ?? null;
  const detail = useResource<StoredActionPlanView | null>(
    `动作计划 ${chosen?.id ?? ''}`,
    () => (chosen ? getActionPlan(chosen.id) : Promise.resolve(null)),
    30_000,
    Boolean(chosen)
  );
  // 计划的提交、删除都落在 `/api/v1/action-plan` 这一前缀下，一次成功的写就报一次
  // 这个键：列表与选中那份的详情一起重读，免得刚提交的图停在上一份。
  useKindTickedRefresh([resourceKey('action-plan')], [page.refresh, detail.refresh]);

  const submit = async () => {
    if (!goal.trim()) {
      appendOutput('动作计划没有提交：goal 不能空——它是这张图要回答的问题。');
      return;
    }
    let tasks: unknown[];
    let edges: [string, string][];
    let skills: unknown[];
    try {
      tasks = JSON.parse(tasksJson || '[]') as unknown[];
      skills = JSON.parse(skillsJson || '[]') as unknown[];
      const parsed = JSON.parse(edgesJson || '[]') as unknown;
      if (!Array.isArray(parsed) || parsed.some((e) => !Array.isArray(e) || e.length !== 2)) {
        appendOutput('动作计划没有提交：edges 要是 [[\"a\",\"b\"], …] 这样的二元数组。');
        return;
      }
      edges = parsed as [string, string][];
    } catch (e) {
      appendOutput(`动作计划没有提交：JSON 解析失败（${(e as Error).message}）`);
      return;
    }

    setBusy(true);
    try {
      const res = await createActionPlan({ goal: goal.trim(), tasks, skills, edges });
      appendOutput(`已提交动作计划 ${res.id}（${tasks.length} 个节点、${edges.length} 条边）`);
      setGoal('');
      setTasksJson('[]');
      setEdgesJson('[]');
      setSkillsJson('[]');
      setOpen(false);
      setSelected(res.id);
      page.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`提交动作计划失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (plan: StoredActionPlanView) => {
    if (!window.confirm(`删除动作计划 ${plan.id}？它分解出的节点与边会一起消失。`)) return;
    try {
      await deleteActionPlan(plan.id);
      appendOutput(`已删除动作计划 ${plan.id}`);
      if (selected === plan.id) setSelected(null);
      page.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`删除动作计划 ${plan.id} 失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    }
  };

  return (
    <ViewFrame
      title="动作计划"
      meta={
        page.data
          ? `${plans.length} 份计划 · 有共享存储时跨重启保留`
          : page.error
            ? '读不到'
            : '读取中'
      }
      actions={<Button tone="accent" onClick={() => setOpen((v) => !v)}>{open ? '收起' : '提交一份计划'}</Button>}
    >
      {open && (
        <section className="border-b border-[var(--line)] px-4 py-2">
          <FormRow label="目标" hint="这张图要回答的那句话。空的目标会被服务端拒绝。">
            <TextInput value={goal} onChange={setGoal} mono={false} width="100%" placeholder="把变更与可观测性接起来" />
          </FormRow>
          <FormRow
            label="节点"
            hint='JSON 数组，每项至少 {"id":"a","name":"先做什么"}；可选 skill_id / estimated_tokens / blocked_by。'
          >
            <TextArea value={tasksJson} onChange={setTasksJson} rows={5} />
          </FormRow>
          <FormRow label="边" hint='[["a","b"], …]：b 等 a。出现没定义的节点会被 DAG 校验拒绝。'>
            <TextArea value={edgesJson} onChange={setEdgesJson} rows={3} />
          </FormRow>
          <FormRow label="技能" hint="可选。每项 {id,name,description,tools}；不提交也能建图。">
            <TextArea value={skillsJson} onChange={setSkillsJson} rows={3} />
          </FormRow>
          <div className="flex items-center gap-2 pt-1">
            <Button tone="accent" onClick={submit} disabled={busy}>
              {busy ? '提交中…' : '提交'}
            </Button>
          </div>
        </section>
      )}

      {page.error ? (
        <ReadError what="动作计划" error={page.error} onRetry={page.refresh} />
      ) : !page.data ? (
        <Loading what="动作计划" />
      ) : plans.length === 0 ? (
        <Empty>还没有人提交过动作计划。计划不是任务图的另一份副本——它只在这里被提交、被删除。</Empty>
      ) : (
        <div className="flex h-full min-h-0">
          <aside className="w-72 shrink-0 overflow-y-auto border-r border-[var(--line)] p-2">
            {plans.map((plan) => {
              const active = plan.id === chosen?.id;
              return (
                <button
                  key={plan.id}
                  type="button"
                  onClick={() => setSelected(plan.id)}
                  className={`mb-1 block w-full px-2 py-1.5 text-left transition-colors ${
                    active
                      ? 'bg-[var(--ink-700)] text-[var(--paper)]'
                      : 'text-[var(--paper-dim)] hover:bg-[var(--ink-800)]'
                  }`}
                >
                  <div className="line-clamp-2 text-[12px]">{plan.plan.goal}</div>
                  <div className="mono text-[10px] text-[var(--muted)]">
                    {plan.plan.tasks.length} 节点 · {plan.id.slice(0, 8)}
                  </div>
                </button>
              );
            })}
          </aside>

          <section className="flex min-h-0 flex-1 flex-col overflow-auto">
            {detail.error ? (
              <ReadError what="这份计划" error={detail.error} onRetry={detail.refresh} />
            ) : !detail.data ? (
              <Loading what="计划详情" />
            ) : (
              <PlanDetail plan={detail.data} onDelete={() => void remove(detail.data as StoredActionPlanView)} />
            )}
          </section>
        </div>
      )}
    </ViewFrame>
  );
}

function PlanDetail({ plan, onDelete }: { plan: StoredActionPlanView; onDelete: () => void }) {
  const validation = plan.validation;
  return (
    <>
      <header className="shrink-0 border-b border-[var(--line)] px-4 py-2">
        <div className="flex items-start gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-[14px]">{plan.plan.goal}</h2>
            <div className="mono mt-0.5 text-[10px] text-[var(--muted)]">
              {plan.id} · 建于 <Moment iso={plan.created_at} />
            </div>
          </div>
          <Button onClick={onDelete}>删除</Button>
        </div>
        <div className="mt-2 flex flex-wrap gap-4 text-[11px] text-[var(--paper-dim)]">
          <span>{plan.plan.tasks.length} 个节点</span>
          <span>{plan.plan.edges.length} 条边</span>
          <span>{plan.plan.skills.length} 项技能</span>
          {validation ? (
            <span>
              入口 {validation.entry_nodes.length} · 出点 {validation.exit_nodes.length} · 关键路径{' '}
              {validation.critical_path_len} · 连通分量 {validation.components}
            </span>
          ) : (
            <span className="text-[var(--muted)]">服务端没有留下校验摘要</span>
          )}
        </div>
        {validation && validation.entry_nodes.length > 0 && (
          <div className="mt-1 text-[10px] text-[var(--muted)]">
            入口：<span className="mono">{validation.entry_nodes.join('、')}</span>
            {' · '}
            出点：<span className="mono">{validation.exit_nodes.join('、')}</span>
          </div>
        )}
      </header>

      <div className="min-h-0 flex-1 overflow-auto">
        {plan.plan.tasks.length === 0 ? (
          <Empty>这份计划没有节点。空图是合法的（校验直接通过），但它没有可执行的东西。</Empty>
        ) : (
          <Table
            columns={[
              { key: 'id', title: '节点', width: '110px', render: (t: ActionPlanTaskView) => <span className="mono text-[11px]">{t.id}</span> },
              { key: 'name', title: '做什么', render: (t: ActionPlanTaskView) => t.name },
              { key: 'skill', title: '技能', width: '110px', render: (t: ActionPlanTaskView) => t.skill_id ?? '—' },
              {
                key: 'blocked',
                title: '等谁',
                render: (t: ActionPlanTaskView) =>
                  t.blocked_by && t.blocked_by.length > 0 ? (
                    <span className="mono text-[11px]">{t.blocked_by.join('、')}</span>
                  ) : (
                    '不等'
                  ),
              },
              {
                key: 'tokens',
                title: '预估 token',
                width: '100px',
                render: (t: ActionPlanTaskView) =>
                  t.estimated_tokens ? t.estimated_tokens.toLocaleString('zh-CN') : '—',
              },
              {
                key: 'gap',
                title: '缺口',
                width: '70px',
                render: (t: ActionPlanTaskView) =>
                  t.skill_gap ? <Note text="分解时点名了这个节点需要一个平台还没有的技能">缺</Note> : '—',
              },
            ]}
            rows={plan.plan.tasks}
            rowKey={(t) => t.id}
          />
        )}
        {plan.plan.edges.length > 0 && (
          <div className="border-t border-[var(--line)] px-4 py-2 text-[11px] text-[var(--paper-dim)]">
            边：<span className="mono">{plan.plan.edges.map(edgeText).join('；')}</span>
          </div>
        )}
      </div>
    </>
  );
}
