import { useState } from 'react';
import { useAgentConversation } from '@/hooks/useAgentConversation';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { hireEmployee, type AgentConversationLine } from '@/api/client';
import { useLiveAgentStore } from '@/store/liveAgentStore';
import { Button, Empty, Loading, ReadError, ViewFrame } from '@/ui/primitives';
import { Note } from '@/ui/Note';

/**
 * 人这一维：名册上的每一个员工，和他自己的对话框。
 *
 * 这里不按活分人。今天做 memory 抽取、明天做 wiki 维护的是同一个员工——场地
 * 是活挂着的标签，不是长在身份上的职位。所以这一面的左栏只有人：名字、id、
 * 现在处在生命周期的哪个状态；右栏是和他一个人说过的话。
 *
 * 名单读注册表（现在能叫到谁），状态读总线折出来的帧（`agent_start` /
 * `state_change` / `agent_end` / `agent_error`），两样都不是这里自己编的。
 *
 * 说话走 `POST /api/v1/agents/{id}/messages`：话进它的收件箱，回答从同一条路
 * 回来。属主与员工是平的——这条路由在操作者权限组，不在管理员组。
 *
 * 画面：这一面本身就是画面——左边是一排人，右边是和他们一来一往的对话正文。
 * 每一行话的形状（谁说的、送达没有、回答是什么）就在版式里，不另画一张分布图。
 */

/** 每种结局一句话，键取自端点类型本身：没配话的结局会把英文原词贴在灰字里。 */
const OUTCOME_LABEL: Record<AgentConversationLine['outcome'], { text: string; color: string }> = {
  queued: { text: '已送达，等它自己回', color: 'var(--muted)' },
  replied: { text: '已回', color: 'var(--ok)' },
  timed_out: { text: '没在等待窗口内回答', color: 'var(--warn)' },
  refused: { text: '没送到', color: 'var(--danger)' },
  failed: { text: '发送或回收答的通道断了', color: 'var(--danger)' },
};

/**
 * 员工回话的形状不总是纯文本。
 *
 * 平台这一族员工把回答包在一个对象里（`{"role":"employee","thought":"…"}`），
 * 原样贴出来读者看到的是一串花括号，而那句话就在里面——「不会说话」与「话说在
 * 一个壳里」在屏幕上是同一种样子。认得出这几种壳就把里面的句子取出来；认不出
 * 就原样贴，宁可露出一个不认识的形状，也不替它编一句。
 */
function spoken(reply: string): string {
  const trimmed = reply.trim();
  if (!trimmed.startsWith('{')) return reply;
  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    for (const field of ['text', 'content', 'message', 'answer', 'thought']) {
      const value = parsed[field];
      if (typeof value === 'string' && value.trim()) return value;
    }
  } catch {
    // 不是 JSON：那它本来就是一句人话。
  }
  return reply;
}

function stamp(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function Line({ line }: { line: AgentConversationLine }) {
  const outcome = OUTCOME_LABEL[line.outcome];
  return (
    <div className="border-t border-[var(--line)] py-2 first:border-t-0">
      <div className="flex flex-wrap items-baseline gap-2 text-[11px]">
        <span className="text-[var(--paper)]">{line.from_unit}</span>
        <span className="text-[var(--muted)]">→ {line.to_unit}</span>
        <span className="text-[var(--muted)]">{line.kind}</span>
        <span className="text-[var(--muted)]">{stamp(line.created_at)}</span>
        <span style={{ color: outcome.color }}>{outcome.text}</span>
      </div>
      <div className="mt-1 whitespace-pre-wrap text-[12px] text-[var(--paper)]">{line.body}</div>
      {line.reply && (
        <div className="mt-1 whitespace-pre-wrap border-l-2 border-[var(--ok)]/40 pl-2 text-[12px] text-[var(--paper-dim)]">
          {spoken(line.reply)}
        </div>
      )}
      {line.outcome_detail && (
        <div className="mt-1 text-[11px]" style={{ color: 'var(--warn)' }}>
          {line.outcome_detail}
        </div>
      )}
    </div>
  );
}

export function PeopleView() {
  const {
    agents,
    agentsError,
    agentsLoaded,
    selected,
    select,
    lines,
    loading,
    error,
    sending,
    sendError,
    send,
    refreshAgents,
    refreshConversation,
  } = useAgentConversation(true);
  // 名单与这一屏的会话一起交给「执行者这一面变了」的帧：有人上岗、换态、走了，
  // 或者有人往某个员工的收件箱里写过一句，立刻重读，不等下一拍 15 秒。两份读数
  // 同一次触发一起读——分两次读，左栏已经多了一个人、右栏还对着旧的那份会话。
  useKindTickedRefresh(['agents', resourceKey('agents')], [
    refreshAgents,
    refreshConversation,
  ]);
  const [draft, setDraft] = useState('');
  const [hiring, setHiring] = useState(false);
  const [hireError, setHireError] = useState<string | null>(null);
  const liveRows = useLiveAgentStore((s) => s.rows);

  const chosen = agents.find((a) => a.agent_id === selected) ?? null;

  const submit = async () => {
    const text = draft;
    setDraft('');
    await send(text);
  };

  const hire = async () => {
    setHiring(true);
    setHireError(null);
    try {
      await hireEmployee();
      refreshAgents();
    } catch (e) {
      setHireError(e instanceof Error ? e.message : String(e));
    } finally {
      setHiring(false);
    }
  };

  return (
    <ViewFrame
      title="员工"
      meta={`${agents.length} 个在岗 · 每人一个对话框 · 名单按 15 秒重读`}
      actions={
        <>
          <Button onClick={hire} disabled={hiring} tone="accent">
            {hiring ? '招人中…' : '招一个员工'}
          </Button>
          <Button onClick={refreshAgents}>重读名单</Button>
        </>
      }
    >
      {hireError && (
        <div className="mx-3 mb-1 border border-[var(--danger)]/40 px-2 py-1 text-[11px]" style={{ color: 'var(--danger)' }}>
          没招成：{hireError}
        </div>
      )}
      {agentsError ? (
        <ReadError what="在岗名单" error={agentsError} onRetry={refreshAgents} />
      ) : agents.length === 0 ? (
        // 读回来是空的，就是真的没人：名单是心跳，30 秒不来就会被摘掉。这与
        // 「还没开口」是两件事，画成同一个转圈会让空名单读成界面坏了。
        agentsLoaded ? (
          <Empty>
            此刻没有在岗的员工。名单来自心跳（30 秒不来就会被摘掉），平台正在运行的员工会在几十秒内出现；下面的「重读名单」可以立刻再问一次。
          </Empty>
        ) : (
          <Loading what="在岗名单" />
        )
      ) : (
        <div className="flex h-full min-h-0">
          <aside className="w-72 shrink-0 overflow-y-auto border-r border-[var(--line)] p-2">
            {agents.map((agent) => {
              const live = liveRows[agent.agent_id];
              const active = selected === agent.agent_id;
              return (
                <button
                  key={agent.agent_id}
                  type="button"
                  onClick={() => select(agent.agent_id)}
                  className={`mb-1 block w-full px-2 py-1.5 text-left transition-colors ${
                    active
                      ? 'bg-[var(--ink-700)] text-[var(--paper)]'
                      : 'text-[var(--paper-dim)] hover:bg-[var(--ink-800)]'
                  }`}
                >
                  <div className="flex items-baseline justify-between gap-2">
                    <span className="text-[12px]">{agent.display_name}</span>
                    {live?.state ? (
                      <Note
                        className="mono text-[10px]"
                        text={`这一格来自 ${live.stateFrame} 帧（${live.stateAt}），不是这次轮询`}
                      >
                        {live.state}
                      </Note>
                    ) : null}
                  </div>
                  <div className="mono text-[10px] text-[var(--muted)]">agent:{agent.agent_id}</div>
                  <div className="text-[10px] text-[var(--muted)]">
                    {agent.role} · {agent.workspace_id}
                  </div>
                </button>
              );
            })}
          </aside>

          <section className="flex min-h-0 flex-1 flex-col">
            <div className="flex shrink-0 items-center justify-between border-b border-[var(--line)] px-3 py-1.5 text-[11px] text-[var(--muted)]">
              <span>
                {chosen ? `与 ${chosen.display_name} 的会话（agent:${chosen.agent_id}）` : '先选一个员工'}
                {loading && ' · 读取中…'}
              </span>
              <Button onClick={refreshConversation}>刷新会话</Button>
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto px-3 py-2">
              {error && <ReadError what="这段会话" error={error} onRetry={refreshConversation} />}
              {!error && selected && lines.length === 0 && !loading && (
                <Empty>还没有说过话。下面写一句，就是和它的第一句。</Empty>
              )}
              {lines.map((line) => (
                <Line key={line.id} line={line} />
              ))}
            </div>

            {sendError && (
              <div className="mx-3 mb-1 border border-[var(--danger)]/40 px-2 py-1 text-[11px]" style={{ color: 'var(--danger)' }}>
                {sendError}
              </div>
            )}
            <div className="flex shrink-0 gap-2 border-t border-[var(--line)] p-2">
              <textarea
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    void submit();
                  }
                }}
                rows={2}
                disabled={!selected || sending}
                placeholder={selected ? '对它说一句，Enter 发送，Shift+Enter 换行' : '先选一个员工'}
                className="flex-1 resize-none border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[12px] outline-none focus:border-[var(--brass)] disabled:opacity-50"
              />
              <Button onClick={submit} disabled={!selected || sending || !draft.trim()} tone="accent">
                {sending ? '发送中…' : '发送'}
              </Button>
            </div>
            <p className="px-3 pb-2 text-[10px] text-[var(--muted)]">
              发出去就答「已送达」，不占着这一次请求等它：它回话可能比一条请求的寿命长。回答到了会自己补进这一行，
              不用刷新——这一面读的是会话，不是一次请求的结果。
            </p>
          </section>
        </div>
      )}
    </ViewFrame>
  );
}
