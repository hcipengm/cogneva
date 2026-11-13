import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { callsForGoal, shortGoal, type GoalLive } from '@/conversation/dispatch';
import { Markdown } from '@/conversation/Markdown';
import { TodoPanel, todoListsIn } from '@/conversation/TodoPanel';
import { toolDiff } from '@/conversation/toolDiff';
import type { StreamClient } from '@/hooks/useEventStream';
import { sessionKind, useChatStore, type ChatMessage, type ChatSession } from '@/store/chatStore';
import { useStreamStore, type ToolCallRecord } from '@/store/streamStore';
import {
  DEFAULT_PERMISSION_MODE,
  usePermissionStore,
  type PermissionAsk,
} from '@/store/permissionStore';
import { useShell } from '@/shell/store';
import type { TurnUsage } from '@/types/events';
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

/**
 * 账上一条变更的叫法。身份是 `kind:scope:literal` 三段，正文里那段才是人认得的；
 * 整串放在悬停提示里——点进去靠的是整串，读名字靠的是这一段。
 */
function shortChange(id: string): string {
  const parts = id.split(':');
  const literal = parts.length >= 3 ? parts.slice(2).join(':') : id;
  return literal.length > 14 ? `${literal.slice(0, 14)}…` : literal;
}

/** 一万以上按「3.2 万」读，以下的整数带千分位。 */
function compactCount(n: number): string {
  if (n >= 10000) return `${(n / 10000).toFixed(1)} 万`;
  return n.toLocaleString('zh-CN');
}

/**
 * 一轮花销读成一行：轮次、进出 token、时长、模型、费用。
 *
 * 读不到的那几项不写：费用只有上游声明了价目才有数，模型名只有上游报了才有，
 * 把它们写成 0 或「未知」就是拿一个没读到的值冒充一个读到的值。
 */
function usageLine(usage: TurnUsage): string {
  const parts: string[] = [];
  if (usage.rounds !== null) parts.push(`${usage.rounds} 轮`);
  if (usage.inputTokens !== null || usage.outputTokens !== null) {
    const input = usage.inputTokens === null ? '—' : compactCount(usage.inputTokens);
    const output = usage.outputTokens === null ? '—' : compactCount(usage.outputTokens);
    parts.push(`输入 ${input} · 输出 ${output}`);
  }
  if (usage.durationMs !== null) parts.push(`${(usage.durationMs / 1000).toFixed(1)} 秒`);
  if (usage.model) parts.push(usage.model);
  if (usage.costUsd !== null) parts.push(`$${usage.costUsd.toFixed(4)}`);
  return parts.join(' · ');
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

      {/*
        正文按 Markdown 画：回复里带的是列表、代码块和 diff，按纯文本铺出来，
        读者要自己对齐正负号、自己认哪一段是代码。两边的颜色仍然分开——「你」说
        的是纸色，回复略暗一档。
      */}
      <div style={{ color: message.role === 'user' ? 'var(--paper)' : 'var(--paper-dim)' }}>
        <Markdown text={message.text} streaming={streaming} />
      </div>

      {/*
        这一轮花了多少。只有服务端真的量到了才有一行：读回的历史与被叫停的那
        一轮没有这一格，那时屏幕上什么也不写，而不是写一排零。
      */}
      {message.role === 'assistant' && message.usage && (
        <p className="mono mt-1 text-[10px] text-[var(--muted)]">{usageLine(message.usage)}</p>
      )}
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
  /** 改动类工具折成的那份 diff；别的调用是 `null`（见 `toolDiff`）。 */
  const diff = useMemo(() => toolDiff(call), [call]);
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
      {/*
        改动类工具直接在流里画那一份 diff，不用先展开：这一行最要紧的信息是「改了
        哪几行」，而它在入参与返回那两份 JSON 里只有自己找。展开仍然是原文——diff
        是给眼睛的，原文是给核对的。
      */}
      {diff && (
        <div className="tool-diff mt-1 border-l border-[var(--line)] pl-2">
          <div className="mono flex flex-wrap items-baseline gap-2 text-[10px] text-[var(--muted)]">
            {diff.path && (
              <span className="break-all" style={{ color: 'var(--paper-dim)' }}>
                {diff.path}
              </span>
            )}
            <span>{diff.label}</span>
            <span style={{ color: 'var(--ok)' }}>+{diff.added}</span>
            <span style={{ color: 'var(--danger)' }}>−{diff.removed}</span>
            {call.status === 'running' && <span>调用还没回来</span>}
          </div>
          <pre className="mono mt-1 max-h-64 overflow-auto text-[11px] leading-[1.5]">
            {diff.lines.map((line, index) => (
              <span key={index} className={`diff-${line.kind}`}>
                {line.kind === 'add' ? '+' : line.kind === 'del' ? '−' : ' '}
                {line.text || ' '}
                {'\n'}
              </span>
            ))}
          </pre>
        </div>
      )}
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
  /** 把账上的某一行打开。这一面只知道身份，落到哪一行由账那一面决定。 */
  const focusRecord = useShell((s) => s.focusRecord);
  const [draft, setDraft] = useState('');
  /**
   * 等着这一段答的「要不要动手」。
   *
   * 按会话筛，不按连接筛：同一段对话可以有几问同时在等（用户连发两句），而它们
   * 各自按请求号回去，抢不得。这一段已经停掉的那一轮不会有问留下——服务端在连接
   * 与那一轮一起消失时自己拒绝。
   */
  const allPermissionAsks = usePermissionStore((s) => s.asks);
  const permissionAsks = useMemo(
    () => allPermissionAsks.filter((ask) => ask.sessionId === sessionId),
    [allPermissionAsks, sessionId]
  );
  const permissionMode = usePermissionStore((s) => s.modes[sessionId] ?? DEFAULT_PERMISSION_MODE);
  /**
   * 这个人留下来的规则。`null` 是「还没读到」，不是「一条也没有」——两句话在
   * 这一面写出来就是两句不同的字，见下面那块规则面板。
   */
  const permissionRules = usePermissionStore((s) => s.rules);
  const [rulesOpen, setRulesOpen] = useState(false);
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
    // 这一段自己的员工做的事，先接回来。
    //
    // 下面那一支认的是「这道活派给过谁」，只覆盖交出去以后由执行者接着干的部分。
    // 而一段对话在自己手里的时候（还没交出任何目标，或者这一问本来就是一句话的
    // 活）它是自己在调工具的——那些帧带着它的 `agent_id` 落在同一条总线上，却
    // 一条都没有进过这一面：屏幕上只有一问一答，看起来像它一个工具都没用。
    // 身份由服务端的 `agent.identity` 帧带进来，前端算不出这个 id（名字里带进程
    // 指纹），所以没有那一帧时这里什么也接不回来，而不是接错。
    const own = session?.agentId;
    if (own) {
      for (const call of toolCalls) {
        if (call.agent !== own) continue;
        if (!collected.some((seen) => seen.id === call.id)) collected.push(call);
      }
    }
    for (const goal of goals) {
      for (const call of callsForGoal(toolCalls, goalLive?.[goal.id], goal.at)) {
        if (!collected.some((seen) => seen.id === call.id)) collected.push(call);
      }
    }
    return collected.sort((a, b) => Date.parse(a.startedAt) - Date.parse(b.startedAt));
  }, [goalLive, session?.agentId, session?.goals, toolCalls]);
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
  /**
   * 这一段手上那张待办单，按写者分开，一位一张。
   *
   * 模型每一步都把整张单子重写一遍，所以多次 `todo` 调用是同一张单子的几个版本。
   * 折成一块面板之后，下面时间线里那些调用本身不再各占一行——画五遍同一份清单，
   * 读者还得自己找哪份最新。
   */
  const todoLists = useMemo(() => todoListsIn(tools), [tools]);
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
      // 待办不进时间线：它的现况画在下面那块面板里，一次调用一行原文只会重复。
      // 认不出来的（没有结果、也没有可读入参）那一次仍然照常出现，见 TodoPanel。
      if (call.name === 'todo') continue;
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

  /**
   * 停下这一段上正在跑的那一轮。
   *
   * 界面上不做本地拦截：停了没有，由服务端那一帧说了算（`message.cancelled`
   * 之后紧跟收尾帧）。本地先把光标收掉的话，一次没送到的停止会看起来像停下了，
   * 而模型那边还在说——屏幕和账从此不是一件事。
   */
  const stop = useCallback(() => {
    clientRef.current?.send({ type: 'chat_cancel', session_id: sessionId });
  }, [clientRef, sessionId]);

  const streaming = Boolean(session?.streaming);

  /**
   * 会话头那一格上下文占用：最近一条带着用量的消息，它最后一次调用读进去的
   * 提示词长度。没有它就是没读到过（刷新读回的历史不带花销），那一格不出现。
   */
  const contextTokens = useMemo(() => {
    const messages = session?.messages ?? [];
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const seen = messages[i].usage?.contextTokens;
      if (seen !== null && seen !== undefined) return seen;
    }
    return null;
  }, [session?.messages]);

  /**
   * Esc 与「■ 停止」走同一条路：只在有轮在跑时挂上，停的是这一段上所有在跑的轮。
   *
   * 不避让输入框——Esc 在这里没有第二种含义，而「手还在键盘上、想让它停下」正是
   * 这个键存在的理由。监听挂在 window 上而不是这一面的元素上：焦点多半就在那个
   * 输入框里，元素级监听会在刚说完一句、还没点别处的时候收不到。
   */
  useEffect(() => {
    if (!streaming) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      event.preventDefault();
      stop();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [streaming, stop]);

  if (!session) return null;

  /**
   * 答一次「要不要动手」。
   *
   * 账上的那一行先摘掉再发：答案没送到时服务端仍然是拒绝（连接消失那一头自己
   * 拒绝），所以摘掉不会让一次执行悄悄发生。反过来留着它，屏幕上就一直挂着一个
   * 已经不存在的问。
   */
  const answerPermission = (ask: PermissionAsk, allow: boolean, remember = false) => {
    clientRef.current?.send({
      type: 'permission_response',
      request_id: ask.id,
      allow,
      remember,
    });
    usePermissionStore.getState().answered(ask.id);
  };

  /**
   * 规则面板：第一次打开时向服务端要一份名单，之后每一次撤销的回执都是一整份新名单。
   *
   * 不在挂载时就问：一个人可能从来不开这块面板，而每一次连上来都问一次名单，是把
   * 一件没人看的东西算进每一次连接的开销。
   */
  const toggleRules = () => {
    const next = !rulesOpen;
    setRulesOpen(next);
    if (next) clientRef.current?.send({ type: 'permission_rules', session_id: sessionId });
  };

  /** 撤掉一条。回执是撤完之后的整份名单，所以这里不自己改本地那一份。 */
  const revokeRule = (id: string) => {
    clientRef.current?.send({
      type: 'permission_rule_revoke',
      session_id: sessionId,
      rule_id: id,
    });
  };

  /** 换这一段对话的模式。名字是闭集里的那一格，服务端不认识时按更严的一格处理。 */
  const changePermissionMode = (next: string) => {
    usePermissionStore.getState().setMode(sessionId, next);
    clientRef.current?.send({ type: 'permission_mode', session_id: sessionId, mode: next });
  };

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
        {/*
          「Ada」是这一面的叫法，员工名册上那个人叫的是另一个名字（由会话 id 折
          出来）。两个名字之间没有一行字连起来，读的人会以为是两个人——把总线上的
          那个名字写在这里，名册上按它找得到同一段对话。
        */}
        {session.agentId && (
          <Note
            className="mono max-w-[42%] shrink-0 truncate text-[10px] text-[var(--muted)]"
            text={`这一段对话在总线上叫 ${session.agentId}；员工名册上按这个名字找得到同一个人`}
          >
            {session.agentId}
          </Note>
        )}
        <Note as="h2" className="truncate text-[12px] text-[var(--paper-dim)]" text={session.id}>
          {session.title}
        </Note>
        <KindBadge session={session} live={live} />
        {contextTokens !== null && (
          <Note
            className="mono shrink-0 text-[10px] text-[var(--muted)]"
            text="这一轮结束时模型读进去的提示词长度：这段对话现在背着多重的上下文"
          >
            上下文 {compactCount(contextTokens)}
          </Note>
        )}
        {/*
          这一段处在哪一格模式：问两句还是一句不问，是这一面唯一一处能改的东西。
          放在标题行上，因为它是这一段对话的属性，不是某一条消息的。
        */}
        <select
          value={permissionMode}
          onChange={(e) => changePermissionMode(e.target.value)}
          aria-label="权限模式"
          className="mono ml-auto shrink-0 bg-transparent text-[10px] outline-none"
          style={{ color: 'var(--muted)' }}
        >
          <option value="plan">plan · 只读</option>
          <option value="default">default · 动手前问</option>
          <option value="acceptEdits">acceptEdits · 编辑不问</option>
          <option value="bypass">bypass · 全不问</option>
        </select>
        {/*
          留下来的规则：人点过「总是」的那些。入口只有一行字，因为它是设置，不是这一面
          的主体；列表本身体现在下面那块面板里，撤销就在同一行上。
        */}
        <Note
          as="button"
          type="button"
          onClick={toggleRules}
          className="mono shrink-0 px-1 text-[10px] transition-opacity hover:opacity-70"
          style={{ color: rulesOpen ? 'var(--brass)' : 'var(--muted)' }}
          text="看这个人留过哪些「总是允许 / 总是拒绝」，并撤掉其中一条"
        >
          规则
        </Note>
        {/*
          目标与变更都是账上的行，所以每一个都能点进去。这里只是入口：这一面
          不知道那一行在账的第几页，所以它把身份交出去，由账那一面落位。
        */}
        {session.goals.length > 0 && (
          <span className="flex shrink-0 items-baseline gap-1 text-[10px] text-[var(--muted)]">
            目标
            {session.goals.map((goal) => (
              <Note
                as="button"
                key={goal.id}
                type="button"
                onClick={() => focusRecord('goal', goal.id)}
                text={`在项目面打开目标 ${goal.id}`}
                className="mono text-[var(--brass)] transition-opacity hover:opacity-70"
              >
                {shortGoal(goal.id)}
              </Note>
            ))}
          </span>
        )}
        {session.changes.length > 0 && (
          <span className="flex min-w-0 shrink items-baseline gap-1 text-[10px] text-[var(--muted)]">
            变更
            {session.changes.slice(-3).map((change) => (
              <Note
                as="button"
                key={change.id}
                type="button"
                onClick={() => focusRecord('change', change.id)}
                text={`在变更账上打开 ${change.id}`}
                className="mono max-w-[12ch] truncate text-[var(--brass)] transition-opacity hover:opacity-70"
              >
                {shortChange(change.id)}
              </Note>
            ))}
          </span>
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

      {/*
        这张单子贴在输入框上方，不跟着历史滚走：它回答的是「这件事走到哪了」，
        是现况，不是某一次执行的记录。两位写者就两块，谁的活谁的单。
      */}
      {todoLists.length > 0 && (
        <div className="shrink-0 px-4 pb-1">
          {todoLists.map((list) => (
            <TodoPanel key={list.agent ?? 'unattributed'} list={list} />
          ))}
        </div>
      )}

      {rulesOpen && (
        <div className="shrink-0 px-4 pb-1">
          <div className="mb-1 flex items-baseline gap-2">
            <span className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
              留下来的规则
            </span>
            <Note
              as="button"
              type="button"
              onClick={() =>
                clientRef.current?.send({ type: 'permission_rules', session_id: sessionId })
              }
              className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
              text="重新从服务端读一份名单"
            >
              刷新
            </Note>
            <Note
              as="button"
              type="button"
              onClick={toggleRules}
              className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
              text="收起这块面板"
            >
              收起
            </Note>
          </div>
          {/*
            「还在读」与「一条也没有」分开写：前者是这一次还没拿到名单（连接刚起、
            或者服务端这一侧还没接上），后者是这个人真的干净。合起来写，一次失败的
            读取会被读成一次干净的家底。
          */}
          {permissionRules === null ? (
            <p className="mono text-[10px] text-[var(--muted)]">还在读这份名单…</p>
          ) : permissionRules.length === 0 ? (
            <p className="mono text-[10px] text-[var(--muted)]">这个人名下还没有留下过规则。</p>
          ) : (
            <ul className="flex flex-col gap-1">
              {permissionRules.map((rule) => (
                <li key={rule.id} className="flex items-baseline gap-2 text-[11px]">
                  <span
                    className="mono"
                    style={{ color: rule.allow ? 'var(--brass)' : 'var(--danger)' }}
                  >
                    {rule.allow ? '允许' : '拒绝'}
                  </span>
                  <span className="mono">{rule.tool}</span>
                  {rule.subject && (
                    <span className="mono break-all text-[var(--paper-dim)]">{rule.subject}</span>
                  )}
                  <span className="mono text-[10px] text-[var(--muted)]">
                    {rule.scope === 'owner' ? '这个人' : '这一段'}
                  </span>
                  <Note
                    as="button"
                    type="button"
                    onClick={() => revokeRule(rule.id)}
                    className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
                    text="撤掉这条规则；从此同类的调用会重新问你"
                  >
                    撤销
                  </Note>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
      {permissionAsks.length > 0 && (
        <div className="shrink-0 px-4 pb-1">
          {permissionAsks.map((ask) => (
            <div
              key={ask.id}
              className="mb-2 border-t pt-2"
              style={{ borderColor: 'var(--line-strong)' }}
            >
              <div className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
                {ask.plan ? '计划待批准' : `要动手：${ask.tool}`}（{ask.mode} 模式）
              </div>
              {/*
                计划正文不截：这一份要在点「批准并执行」之前被读完，截到几行就等于
                让人闭眼点头。摘要那种一行字只在「要动手」时出现。
              */}
              {ask.plan ? (
                <div
                  className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap break-words border-l-2 pl-2 text-[12px] text-[var(--paper)]"
                  style={{ borderColor: 'var(--line-strong)' }}
                >
                  {ask.plan}
                </div>
              ) : (
                <pre className="mt-1 max-h-24 overflow-auto whitespace-pre-wrap break-all text-[12px] text-[var(--paper)]">
                  {ask.summary}
                </pre>
              )}
              <div className="mt-2 flex gap-3">
                {/*
                  计划只有两个答案：批准，或者让它继续改。批准不是「允许这一次」——
                  它换的是这一轮的模式，之后每一步动手照旧按模式与规则问人。
                */}
                {ask.plan ? (
                  <>
                    <Note
                      as="button"
                      type="button"
                      onClick={() => answerPermission(ask, true)}
                      className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                      style={{ color: 'var(--brass)' }}
                      text="批准这份计划：这一轮从此按 default 模式执行，要动手的每一步仍然按模式与规则问你"
                    >
                      批准并执行
                    </Note>
                    <Note
                      as="button"
                      type="button"
                      onClick={() => {
                        clientRef.current?.send({
                          type: 'permission_response',
                          request_id: ask.id,
                          allow: false,
                          reason: '计划没有批准：继续只读调研，把计划改好再来请我批准。',
                        });
                        usePermissionStore.getState().answered(ask.id);
                      }}
                      className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                      style={{ color: 'var(--muted)' }}
                      text="不执行这份计划；模型会收到这句话，继续在只读模式下改计划"
                    >
                      继续改
                    </Note>
                  </>
                ) : (
                  <>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, true)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--brass)' }}
                  text="允许这一次执行；下一次同类调用仍然会问"
                >
                  允许
                </Note>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, false)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--danger)' }}
                  text="拒绝这一次，并把理由交回模型；同一个调用再发一遍会得到同一个答案"
                >
                  拒绝
                </Note>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, true, true)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--brass)' }}
                  text="允许这一次，并把这个工具碰这个东西的调用记成一条放行规则：下次同类调用不再问，在规则面板里可以撤销"
                >
                  总是允许
                </Note>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, false, true)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--danger)' }}
                  text="拒绝这一次，并把这个工具碰这个东西的调用记成一条拒绝规则：下次同类调用直接拒绝不再问，在规则面板里可以撤销"
                >
                  总是拒绝
                </Note>
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
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
          {streaming && (
            <Note
              as="button"
              type="button"
              onClick={stop}
              className="mono shrink-0 px-2 text-[10px] transition-opacity hover:opacity-70"
              style={{ color: 'var(--danger)' }}
              text="停下这一段上正在跑的这一轮；说过的字留着，这一轮记为已中止"
            >
              ■ 停止
            </Note>
          )}
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
