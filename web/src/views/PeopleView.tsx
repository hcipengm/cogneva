import { useMemo, useState, type ReactNode } from 'react';
import { useAgentConversation } from '@/hooks/useAgentConversation';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { hireEmployee, type AgentConversationLine } from '@/api/client';
import { useLiveAgentStore } from '@/store/liveAgentStore';
import { useStreamStore, type Message as StreamedMessage, type ToolCallRecord } from '@/store/streamStore';
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
 * 这一面画的是**总线上发生了什么**。它说的话、调的工具、跑到哪一步，全部来自
 * 帧流（`agent:*`）；会话那一份 REST 只补两样帧上不带的：页面打开之前的历史，
 * 和一次投递的结局（送达没有）。两边都画同一句话就是同一句话说两遍，所以窗口里
 * 已经有帧流回答的那一行，不再贴它自己那份 `reply`。
 *
 * 画面：这一面本身就是画面——左边是一排人，右边是和他们一来一往的对话正文，
 * 中间夹着它这一趟真正做过的事。每一行话的形状（谁说的、送达没有、回答是什么）
 * 就在版式里，不另画一张分布图。
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
    for (const field of ['text', 'content', 'message', 'answer', 'reply', 'thought', 'result']) {
      const value = parsed[field];
      if (typeof value === 'string' && value.trim()) return value;
    }
  } catch {
    // 不是 JSON：那它本来就是一句人话。
  }
  return reply;
}

/** 一行参数或返回留多长。一行放不下整份文件，截断处明写它被截了。 */
const TOOL_PREVIEW = 400;

function clip(text: string): string {
  return text.length > TOOL_PREVIEW ? `${text.slice(0, TOOL_PREVIEW)}…` : text;
}

/**
 * 它调过的一次工具，按 Claude Code 那种一行一次的样子画。
 *
 * 「在跑」「跑完了」「失败了」三种状态分开画：正在跑的那一行是读者唯一的进度，
 * 把它画成结束会让一次还没回来的调用看着像空转。
 */
function ToolLine({ call }: { call: ToolCallRecord }) {
  const state =
    call.status === 'running'
      ? { text: '在跑', color: 'var(--brass)' }
      : call.status === 'error'
        ? { text: '失败', color: 'var(--danger)' }
        : { text: '返回', color: 'var(--ok)' };
  return (
    <div className="border-t border-[var(--line)] py-2">
      <div className="flex flex-wrap items-baseline gap-2 text-[11px]">
        <span className="mono" style={{ color: 'var(--brass)' }}>
          {call.name}
        </span>
        <span style={{ color: state.color }}>{state.text}</span>
        <span className="text-[var(--muted)]">{stamp(call.startedAt)}</span>
      </div>
      {call.args && (
        <div className="mono mt-1 whitespace-pre-wrap break-all text-[11px] text-[var(--paper-dim)]">
          {clip(call.args)}
        </div>
      )}
      {(call.result || call.progress) && (
        <div className="mono mt-1 whitespace-pre-wrap break-all border-l-2 border-[var(--line)] pl-2 text-[11px] text-[var(--muted)]">
          {clip(call.result || call.progress)}
        </div>
      )}
    </div>
  );
}

/** 它正在说的那句：流到一半也在画，否则整趟运行在屏幕上只有一个转圈。 */
function SaidLine({ message }: { message: StreamedMessage }) {
  return (
    <div className="border-t border-[var(--line)] py-2">
      <div className="flex flex-wrap items-baseline gap-2 text-[11px]">
        <span className="text-[var(--paper-dim)]">{message.agent ?? '执行者'}</span>
        <span className="text-[var(--muted)]">
          {message.status === 'streaming' ? '正在说…' : message.status === 'error' ? '说到一半断了' : '说的'}
        </span>
      </div>
      <div className="mt-1 whitespace-pre-wrap text-[12px] text-[var(--paper-dim)]">{message.text}</div>
    </div>
  );
}

function stamp(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function Line({ line, showReply }: { line: AgentConversationLine; showReply: boolean }) {
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
      {showReply && line.reply && (
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
  // 它这一趟到底做了什么：工具调用从总线帧流来，按帧里的 `agent_id` 认领。
  // 会话那一面只有「问了一句、答了一句」，中间的每一步都在这里。
  const toolCalls = useStreamStore((s) => s.toolCalls);
  const streamed = useStreamStore((s) => s.messages);
  const calls = useMemo(
    () => toolCalls.filter((call) => call.agent === selected),
    [toolCalls, selected]
  );
  // 它说的每一句都从帧流来，包括已经说完的那一句：这一面画的就是「总线上说了
  // 什么」，不是「十五秒前快照里存了什么」。会话行只为两件事存在——没赶上这一趟
  // 的历史（页面打开之前说过的话），以及一次投递的结局（送达没有，帧上不带）。
  const said = useMemo(
    () => streamed.filter((message) => message.agent === selected && message.text.trim()),
    [streamed, selected]
  );
  // 说与做并成一条时间线：先问的那一句，然后它调了什么、边说边做了什么，按各自的
  // 时刻排。只画问与答的话，一次跑了半分钟的活看起来像一次卡住。
  const timeline = useMemo(() => {
    const at = (iso: string) => {
      const parsed = Date.parse(iso);
      return Number.isFinite(parsed) ? parsed : 0;
    };
    // 流式消息的 id 形如 `<agent>:<iso 时刻>`：那一刻是这句开始流的时候。
    // 按 agent 前缀切而不是按冒号切：agent id 自己可能带冒号（`employee:xxxx`），
    // 从第一个冒号切出来的是半截 id，`Date.parse` 会把它读成 0，于是每一句都被
    // 排到时间线的最前面。
    const saidAt = said.map((message) => {
      const prefix = message.agent ? `${message.agent}:` : '';
      const when = prefix && message.id.startsWith(prefix)
        ? message.id.slice(prefix.length)
        : message.id;
      return at(when);
    });
    const sorted = [...lines].sort((a, b) => at(a.created_at) - at(b.created_at));
    // 一行问话的「回答窗口」是它自己的时刻到下一行的时刻：落在窗口里的帧流就是
    // 这一行的回答。窗口里有话，说明这句回答已经在屏幕上以流的方式出现过，会话行
    // 再贴一次就是同一句话出现两遍；窗口里没有（历史行、或者这一趟它一个字都没说
    // 只调了工具），那一行的 `reply` 才是屏幕上唯一的回答，必须画。
    const answeredByFrames = sorted.map((line, i) => {
      const start = at(line.created_at);
      const end = i + 1 < sorted.length ? at(sorted[i + 1].created_at) : Number.POSITIVE_INFINITY;
      return saidAt.some((t) => t >= start && t < end);
    });
    const items: { sort: number; node: ReactNode }[] = [
      ...sorted.map((line, i) => ({
        sort: at(line.created_at),
        node: <Line key={line.id} line={line} showReply={!answeredByFrames[i]} />,
      })),
      ...calls.map((call) => ({ sort: at(call.startedAt), node: <ToolLine key={call.id} call={call} /> })),
      ...said.map((message, i) => ({
        sort: saidAt[i],
        node: <SaidLine key={message.id} message={message} />,
      })),
    ];
    return items.sort((a, b) => a.sort - b.sort);
  }, [lines, calls, said]);

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
              {!error && selected && lines.length === 0 && calls.length === 0 && said.length === 0 && !loading && (
                <Empty>还没有说过话。下面写一句，就是和它的第一句。</Empty>
              )}
              {timeline.map((entry) => entry.node)}
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
