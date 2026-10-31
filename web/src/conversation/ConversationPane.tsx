import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { callsForGoal, shortGoal, type GoalLive } from '@/conversation/dispatch';
import type { StreamClient } from '@/hooks/useEventStream';
import { sessionKind, useChatStore, type ChatMessage, type ChatSession } from '@/store/chatStore';
import { useStreamStore, type ToolCallRecord } from '@/store/streamStore';
import { Note } from '@/ui/Note';

/**
 * 一段对话的窗口。
 *
 * 画成回合而不是气泡：气泡把每一句圈成一张卡片，一段会话于是变成一串互不相干
 * 的胶囊，读者要在里面找「上一句是谁说的、这一轮到哪儿了」。这里用左侧一条细线
 * 贯穿一轮、角色用等宽体标在行首，句子就是句子本身。正在流的那一轮线是亮的，
 * 流完就暗下去——一轮的边界因此是看得见的，而不是靠气泡的边缘去猜。
 */

function stamp(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
}

function Turn({ message }: { message: ChatMessage }) {
  const [showThinking, setShowThinking] = useState(false);
  const streaming = message.status === 'streaming' || message.status === 'sending';
  const who = message.role === 'user' ? '你' : '执行者';

  return (
    <article className="group relative pl-4">
      <span
        aria-hidden
        className="chat-turn-rule absolute left-0 top-1 bottom-1 w-px"
        data-streaming={streaming}
      />
      <header className="mb-1 flex items-baseline gap-2">
        <span
          className="mono text-[11px]"
          style={{ color: message.role === 'user' ? 'var(--brass)' : 'var(--live)' }}
        >
          {who}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">{stamp(message.at)}</span>
        {message.status === 'sending' && (
          <span className="text-[10px] text-[var(--muted)]">已发出，等待回声</span>
        )}
        {message.failure && (
          <span className="text-[10px]" style={{ color: 'var(--warn)' }}>
            {message.failure}
          </span>
        )}
      </header>

      {message.thinking && (
        <div className="mb-1">
          <button
            type="button"
            onClick={() => setShowThinking((v) => !v)}
            className="text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--paper-dim)]"
          >
            {showThinking ? '收起思考' : '思考过程'}
          </button>
          {showThinking && (
            <p className="mt-1 whitespace-pre-wrap border-l border-[var(--line)] pl-2 text-[11px] leading-[1.6] text-[var(--muted)]">
              {message.thinking}
            </p>
          )}
        </div>
      )}

      <p
        className="whitespace-pre-wrap break-words text-[13px] leading-[1.7]"
        style={{ color: message.role === 'user' ? 'var(--paper)' : 'var(--paper-dim)' }}
      >
        {message.text}
        {streaming && <span className="chat-caret" aria-hidden />}
      </p>
    </article>
  );
}

/**
 * 一次工具调用在对话流里的样子。
 *
 * 不是一行日志：它是这一段对话里真的发生过的一件事，要能展开看它传了什么、回了
 * 什么。收起来时只留一行——工具名、哪个执行者、成没成；展开时给原文。默认收起是
 * 因为一次运行能调几十次，全展开就把回复挤到屏幕外面去了。
 */
function ToolTurn({ call }: { call: ToolCallRecord }) {
  const [open, setOpen] = useState(false);
  const tone =
    call.status === 'error' ? 'var(--danger)' : call.status === 'running' ? 'var(--brass)' : 'var(--ok)';
  const label = call.status === 'error' ? '失败' : call.status === 'running' ? '进行中' : '返回';
  return (
    <article className="group relative pl-4">
      <span aria-hidden className="absolute left-0 top-1 bottom-1 w-px" style={{ background: 'var(--line)' }} />
      <Note
        as="button"
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-baseline gap-2 text-left"
        text="展开这次调用的入参与返回"
      >
        <span className="mono text-[11px]" style={{ color: 'var(--paper-dim)' }}>
          工具
        </span>
        <span className="mono text-[11px]" style={{ color: 'var(--paper)' }}>
          {call.name || '未命名'}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">
          {call.agent ? call.agent.slice(0, 8) : '未知执行者'}
        </span>
        <span className="mono text-[10px]" style={{ color: tone }}>
          {label}
        </span>
        <span className="mono ml-auto text-[10px] text-[var(--muted)]">{stamp(call.startedAt)}</span>
      </Note>
      {open && (
        <div className="mt-1 flex flex-col gap-1 border-l border-[var(--line)] pl-2">
          <div>
            <div className="text-[10px] uppercase tracking-[0.12em] text-[var(--muted)]">入参</div>
            <pre className="mono max-h-48 overflow-auto whitespace-pre-wrap break-all text-[11px] text-[var(--paper-dim)]">
              {call.args || '（没有入参）'}
            </pre>
          </div>
          {call.progress && (
            <div>
              <div className="text-[10px] uppercase tracking-[0.12em] text-[var(--muted)]">中途输出</div>
              <pre className="mono max-h-48 overflow-auto whitespace-pre-wrap break-all text-[11px] text-[var(--muted)]">
                {call.progress}
              </pre>
            </div>
          )}
          <div>
            <div className="text-[10px] uppercase tracking-[0.12em] text-[var(--muted)]">返回</div>
            <pre
              className="mono max-h-64 overflow-auto whitespace-pre-wrap break-all text-[11px]"
              style={{ color: call.status === 'error' ? 'var(--danger)' : 'var(--paper-dim)' }}
            >
              {call.result || (call.status === 'running' ? '还在跑' : '（空）')}
            </pre>
          </div>
        </div>
      )}
    </article>
  );
}

/**
 * 一个执行者说的话。
 *
 * 与「你」和「这段对话的回复」分开画：它是目标下面的某一个人在汇报，不是平台在
 * 回答你。混成一个样子，读者会以为这些字是说给自己听的，于是把「第 3 轮用了
 * read_file」当成一次正式答复。
 */
function AgentTurn({ turn }: { turn: { agent: string; text: string; at: number } }) {
  const [open, setOpen] = useState(false);
  const long = turn.text.length > 400;
  const shown = long && !open ? `${turn.text.slice(0, 400)}…` : turn.text;
  return (
    <article className="group relative pl-4">
      <span
        aria-hidden
        className="absolute left-0 top-1 bottom-1 w-px"
        style={{ background: 'var(--line)' }}
      />
      <header className="mb-1 flex items-baseline gap-2">
        <span className="mono text-[11px]" style={{ color: 'var(--live)' }}>
          执行者 {turn.agent.slice(0, 8)}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">
          {new Date(turn.at).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}
        </span>
        {long && (
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            className="text-[10px] text-[var(--muted)] transition-colors hover:text-[var(--paper-dim)]"
          >
            {open ? '收起' : `全文（${turn.text.length} 字）`}
          </button>
        )}
      </header>
      <p className="whitespace-pre-wrap break-words text-[12px] leading-[1.7] text-[var(--paper-dim)]">
        {shown}
      </p>
    </article>
  );
}

/**
 * 这段对话是哪种：关掉就断的一问一答，还是已经交给守护接着做的活。
 *
 * 判的是它有没有把目标交出去，不是用户勾了什么：勾选是意愿，屏幕上要说的是这件事
 * 现在有没有人接着做。
 */
function KindBadge({ session, live }: { session: ChatSession; live: GoalLive[] }) {
  const kind = sessionKind(session);
  if (kind === 'chat') {
    return (
      <Note
        className="shrink-0 border border-[var(--line)] px-1.5 py-0.5 text-[10px] text-[var(--muted)]"
        text="一问一答：回复只走当前这条连接。关掉这一段，这一轮就没有下文了——说过的话仍然记在历史里。"
      >
        可下线
      </Note>
    );
  }
  const running = live.reduce((sum, goal) => sum + goal.running, 0);
  const total = live.reduce((sum, goal) => sum + goal.total, 0);
  const done = live.reduce((sum, goal) => sum + goal.completed, 0);
  const failed = live.reduce((sum, goal) => sum + goal.failed, 0);
  return (
    <Note
      className="flex shrink-0 items-baseline gap-1.5 border px-1.5 py-0.5 text-[10px]"
      style={{ borderColor: 'var(--ok)', color: 'var(--ok)' }}
      text={
        '已交给编排器：任务由分解、调度与监督接管，关掉这一段它继续跑，回来接得上。' +
        (total > 0 ? `这道目标下 ${total} 个任务，${done} 个收口、${running} 个在跑。` : '还在分解，子任务尚未落地。')
      }
    >
      <span>常驻 · 守护</span>
      {total > 0 && (
        <span className="mono text-[var(--muted)]">
          {done}/{total}
          {running > 0 ? ` · 跑 ${running}` : ''}
          {failed > 0 ? ` · 失败 ${failed}` : ''}
        </span>
      )}
    </Note>
  );
}

export interface ConversationPaneProps {
  sessionId: string;
  /**
   * 这一段对话的叫法。由工作台统一分配——同时开着哪几段决定谁叫什么，
   * 单独一格算不出「和邻居不重名」这件事。
   */
  name: string;
  active: boolean;
  onFocus: () => void;
  onClose: () => void;
  clientRef: React.RefObject<StreamClient | null>;
  /** 只剩一段时不给关：关掉最后一段对话，面上就没有落点了。 */
  closable: boolean;
  /**
   * 去不去读回历史。演示模式没有后端，读一个不存在的接口只会让每一段对话的
   * 顶上挂一条「历史没读到」——那是把「没有这个功能」和「这个功能坏了」画成
   * 同一个样子。
   */
  historyEnabled?: boolean;
  /**
   * 平台这一拍的流程总览折成的目标表（按 `goal_id`）。
   *
   * 这不是给对话面凑一列数字：它决定这一段对话是不是「常驻」，也决定工具调用该
   * 归到谁名下。没有它的时候两件事都只能猜，而猜出来的「守护中」比不显示更坏。
   */
  goalLive?: Record<string, GoalLive>;
}

export function ConversationPane({
  sessionId,
  name,
  active,
  onFocus,
  onClose,
  clientRef,
  closable,
  historyEnabled = true,
  goalLive,
}: ConversationPaneProps) {
  const session = useChatStore((s) => s.sessions[sessionId]);
  const appendUserTurn = useChatStore((s) => s.appendUserTurn);
  const hydrate = useChatStore((s) => s.hydrate);
  const [draft, setDraft] = useState('');
  const scrollerRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);
  /** 这一段交出去的目标，按总览折出来的现状。总览里还没有的那几道如实缺席。 */
  const live = useMemo(
    () => (session?.goals ?? []).map((goal) => goalLive?.[goal.id]).filter(Boolean) as GoalLive[],
    [goalLive, session?.goals]
  );
  const toolCalls = useStreamStore((s) => s.toolCalls);
  const agentMessages = useStreamStore((s) => s.messages);
  /**
   * 这一段自己的执行者调过的工具，按时间接回对话里。
   *
   * 一道目标一个人接：先交出去的那道从自己的时刻起算，后交出去的各自从自己的时刻
   * 起算——同一段对话交过两次活时，第二次不该把第一次的调用也算成自己的。
   */
  const tools = useMemo(() => {
    const goals = session?.goals ?? [];
    const collected: ToolCallRecord[] = [];
    for (const goal of goals) {
      for (const call of callsForGoal(toolCalls, goalLive?.[goal.id], goal.at)) {
        if (!collected.some((seen) => seen.id === call.id)) collected.push(call);
      }
    }
    return collected.sort((a, b) => Date.parse(a.startedAt) - Date.parse(b.startedAt));
  }, [goalLive, session?.goals, toolCalls]);
  /**
   * 这道目标名下的执行者自己说的话。
   *
   * 与上面的工具调用同一条归属规则：按「这道活派给过谁」加「什么时候交出去的」
   * 一起判。它们是执行者的正文，不是这段对话的回复——混成一个样子，读者会以为
   * 平台在直接回答他。所以单独一支、标着谁说的。
   */
  const agentTurns = useMemo(() => {
    const goals = session?.goals ?? [];
    const out: { id: string; agent: string; text: string; at: number }[] = [];
    for (const goal of goals) {
      const goalState = goalLive?.[goal.id];
      if (!goalState || goalState.agents.length === 0) continue;
      const agents = new Set(goalState.agents);
      const from = Date.parse(goal.at);
      for (const message of agentMessages) {
        if (!message.agent || !agents.has(message.agent)) continue;
        // 消息 id 形如 `<agent>:<iso 时刻>`：那一刻是这条消息开始流的时候。
        const at = Date.parse(message.id.split(':').slice(1).join(':'));
        if (Number.isFinite(from) && Number.isFinite(at) && at < from) continue;
        if (out.some((seen) => seen.id === message.id)) continue;
        out.push({ id: message.id, agent: message.agent, text: message.text, at });
      }
    }
    return out;
  }, [agentMessages, goalLive, session?.goals]);
  /** 说与做并成一条时间线。时刻读不出来的排在最前——宁可前也不能藏。 */
  const timeline = useMemo(() => {
    type Item =
      | { kind: 'tool'; at: number; call: ToolCallRecord }
      | { kind: 'agent'; at: number; agent: { id: string; agent: string; text: string; at: number } }
      | { kind: 'message'; at: number; message: ChatMessage };
    const items: Item[] = (session?.messages ?? []).map((message) => ({
      kind: 'message' as const,
      at: Date.parse(message.at),
      message,
    }));
    for (const call of tools) {
      items.push({ kind: 'tool' as const, at: Date.parse(call.startedAt), call });
    }
    for (const turn of agentTurns) {
      items.push({ kind: 'agent' as const, at: turn.at, agent: turn });
    }
    const rank = (at: number) => (Number.isFinite(at) ? at : 0);
    return items.sort((a, b) => rank(a.at) - rank(b.at));
  }, [session?.messages, tools, agentTurns]);

  // 打开就先把历史读回来。读过的不再读：重读会把刚流出来的这一段冲掉。
  useEffect(() => {
    if (historyEnabled) void hydrate(sessionId);
  }, [hydrate, historyEnabled, sessionId]);

  // 只在读者本来就贴着底的时候跟着滚。正在往回翻的时候被拽回底部，是对话界面
  // 最容易被骂的一个动作。
  useEffect(() => {
    const el = scrollerRef.current;
    if (!el || !stickRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [session?.messages, tools.length]);

  const onScroll = useCallback(() => {
    const el = scrollerRef.current;
    if (!el) return;
    stickRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
  }, []);

  const send = useCallback(() => {
    const text = draft.trim();
    if (!text) return;
    setDraft('');
    // 先入账，再出门。顺序反过来时，网络慢一拍、或者这个部署的回声路径断了，
    // 用户看到的就是「我说话了，但这里什么都没发生」。
    appendUserTurn(sessionId, text);
    stickRef.current = true;

    const client = clientRef.current;
    if (!client) return;
    if ('simulateUserMessage' in client) {
      client.setSession?.(sessionId);
      client.simulateUserMessage(text);
    } else {
      client.send({ type: 'chat_message', session_id: sessionId, content: text });
    }
  }, [appendUserTurn, clientRef, draft, sessionId]);

  if (!session) return null;

  const streaming = Boolean(session.streaming);

  return (
    <section
      onMouseDown={onFocus}
      className="relative flex min-h-0 min-w-0 flex-1 flex-col"
      style={{
        borderLeft: active ? '1px solid var(--line-strong)' : '1px solid transparent',
      }}
      aria-label={`对话 ${name}`}
    >
      <header className="flex shrink-0 items-center gap-2 px-4 pt-3 pb-2">
        <span
          aria-hidden
          className="h-1.5 w-1.5 rounded-full"
          style={{
            background: streaming ? 'var(--brass)' : 'var(--muted)',
            boxShadow: streaming ? '0 0 8px var(--brass)' : 'none',
          }}
        />
        {/*
          三行都写「新的对话」时没法指认谁是谁。名字由工作台统一分给这一段，所以
          同时开着的几段不会重名；后面那截才是这次聊的内容。
        */}
        <span className="mono shrink-0 text-[12px] font-medium" style={{ color: 'var(--brass)' }}>
          {name}
        </span>
        <Note as="h2" className="truncate text-[12px] text-[var(--paper-dim)]" text={session.id}>
          {session.title}
        </Note>
        <KindBadge session={session} live={live} />
        {session.goals.length > 0 && (
          <Note
            className="mono shrink-0 text-[10px] text-[var(--muted)]"
            text={`目标 ${session.goals.map((goal) => goal.id).join('、')}`}
          >
            目标 {session.goals.map((goal) => shortGoal(goal.id)).join(' ')}
          </Note>
        )}
        <span className="ml-auto flex items-center gap-3">
          {session.hydrating && (
            <span className="text-[10px] text-[var(--muted)]">正在读回历史…</span>
          )}
          {session.hydrateError && (
            <span className="text-[10px]" style={{ color: 'var(--warn)' }}>
              历史没读到：{session.hydrateError}
            </span>
          )}
          {closable && (
            <button
              type="button"
              onClick={onClose}
              className="text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--danger)]"
              aria-label="关掉这一段对话"
            >
              关闭
            </button>
          )}
        </span>
      </header>

      <div
        ref={scrollerRef}
        onScroll={onScroll}
        className="min-h-0 flex-1 overflow-y-auto px-4 pb-3"
      >
        {session.messages.length === 0 && !session.hydrating && (
          <p className="max-w-[46ch] text-[12px] leading-[1.7] text-[var(--muted)]">
            还没有说过话。说一句你要它做的事——它会先回一句话，然后把这件事拆成任务，
            每一步都会回到这一屏。
          </p>
        )}
        {/*
          对话与执行混在一条时间线上：一次运行里「说了什么」和「做了什么」交替
          发生，分成两栏读的人要自己在两列里对时刻。工具那一行收起来只占一行，
          展开才是原文——几十次调用全展开会把回复挤到屏幕外。
        */}
        <div className="flex flex-col gap-4">
          {timeline.map((item) =>
            item.kind === 'tool' ? (
              <ToolTurn key={`tool-${item.call.id}`} call={item.call} />
            ) : item.kind === 'agent' ? (
              <AgentTurn key={`agent-${item.agent.id}`} turn={item.agent} />
            ) : (
              <Turn key={item.message.id} message={item.message} />
            )
          )}
        </div>
      </div>

      <div className="shrink-0 px-4 pb-4 pt-1">
        {/*
          一条发问的行，不是一个输入框。
          没有边框、没有底色、没有「请输入…」的空白盒：这几样合起来是表单的样子，
          读者会以为在填一格东西。这里是一句话的起头——一个提示符，一句还没说出口
          的话，右边是「怎么送出去」。分隔靠上方那一条发丝线，焦点在它上面时才亮。
        */}
        <div
          className="flex items-stretch gap-2 border-t pt-2 transition-colors"
          style={{ borderColor: draft ? 'var(--line-strong)' : 'var(--line)' }}
        >
          <span className="mono flex items-center pl-0.5 text-[12px] text-[var(--brass)]">›</span>
          <input
            type="text"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onFocus={onFocus}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                send();
              }
            }}
            placeholder="要它做什么…"
            aria-label="说一句话"
            className="min-w-0 flex-1 bg-transparent px-0.5 py-1 text-[13px] text-[var(--paper)] outline-none placeholder:text-[var(--muted)]"
          />
          <button
            type="button"
            onClick={send}
            disabled={!draft.trim()}
            className="mono px-2 text-[10px] transition-opacity disabled:cursor-not-allowed disabled:opacity-0"
            style={{ color: 'var(--brass)' }}
          >
            ↵ 发送
          </button>
        </div>
      </div>
    </section>
  );
}
