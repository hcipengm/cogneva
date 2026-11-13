/**
 * 事件的名字以服务端为准。
 *
 * 服务端把 `AgentEvent` 以变体自己的 snake_case 名字直接写在 `type` 上
 * （`tool_execution_start`、`state_change`……）。这里曾经记的是一套界面自己
 * 编的名字（`tool_call.start`），于是客户端只认三种名字、其余全部静默丢弃——
 * 事件一直在总线上流，界面却只看得见自己编的那几个。名字多写一个都对不上，
 * 所以这张表和 Rust 侧的 `event_type_name` 一一对应。
 */
export type EventType =
  | 'agent_start'
  | 'agent_end'
  | 'turn_start'
  | 'turn_end'
  | 'message_start'
  | 'message_update'
  | 'message_end'
  | 'tool_execution_start'
  | 'tool_execution_update'
  | 'tool_execution_end'
  | 'state_change'
  | 'task_status_change'
  | 'self_review'
  | 'react_step_start'
  | 'react_step_end'
  | 'agent_error'
  | 'resource_alert'
  | 'heartbeat'
  | 'checkpoint_saved'
  | 'resource_changed';

const KNOWN_TYPES: ReadonlySet<string> = new Set<EventType>([
  'agent_start',
  'agent_end',
  'turn_start',
  'turn_end',
  'message_start',
  'message_update',
  'message_end',
  'tool_execution_start',
  'tool_execution_update',
  'tool_execution_end',
  'state_change',
  'task_status_change',
  'self_review',
  'react_step_start',
  'react_step_end',
  'agent_error',
  'resource_alert',
  'heartbeat',
  'checkpoint_saved',
  'resource_changed',
]);

export function isEventType(type: unknown): type is EventType {
  return typeof type === 'string' && KNOWN_TYPES.has(type);
}

/**
 * 一条事件。`payload` 是事件本身：服务端把变体的字段平铺在 `type` 旁边，
 * 没有第二层信封。
 */
export interface AgentEvent {
  id: string;
  type: EventType;
  timestamp: string;
  payload: Record<string, unknown>;
}

export interface ToolExecutionStartFields {
  agent_id: string;
  tool_call_id: string;
  tool_name: string;
  args: unknown;
}

export interface ToolExecutionEndFields {
  agent_id: string;
  tool_call_id: string;
  result: unknown;
  is_error: boolean;
}

export interface StateChangeFields {
  agent_id: string;
  from: string;
  to: string;
}

export interface TaskStatusChangeFields {
  task_id: string;
  status: string;
  agent_id?: string | null;
}

export interface AgentErrorFields {
  agent_id: string;
  error_code: string;
  details: string;
  severity?: string;
}

export interface SelfReviewFields {
  agent_id: string;
  status: string;
  score: number;
  summary?: string | null;
  critique?: string | null;
}

export interface ReActStepEndFields {
  agent_id: string;
  iteration: number;
  thought: string;
  tool_calls: unknown[];
  observations: unknown[];
}

export interface ResourceAlertFields {
  agent_id: string;
  metric: string;
  threshold: number;
  current: number;
}

export interface MessageStartFields {
  agent_id: string;
  message?: { id?: string; role?: string };
}

export interface MessageEndFields {
  agent_id: string;
  message?: { id?: string; role?: string; content?: unknown };
}

/**
 * 聊天流自己的帧族。名字里带点，和上面那套 agent 事件不是一张表。
 *
 * 服务端按 `message.start` / `message.text_delta` / `message.end` 发一回合的
 * 每一段（`crates/cog-gateway/src/chat.rs` 的 `envelope`），外层仍然是
 * `agent_event` 信封，内层才是这一族。把它们塞进 `KNOWN_TYPES` 是不对的：
 * 那张表与 Rust 侧 `event_type_name` 一一对应，而聊天帧不是 `AgentEvent`
 * 的变体。它们曾经因此被整族丢掉——回复一直在总线上，屏幕上一条也没有。
 */
export type ChatFrameKind =
  | 'start'
  | 'text_delta'
  | 'end'
  | 'cancelled'
  | 'thinking_start'
  | 'thinking_delta'
  | 'thinking_end'
  | 'goal'
  | 'change'
  | 'identity'
  | 'usage'
  | 'compaction';

/** 帧名 → 这一帧在做什么。不在这张表里的名字不是聊天帧。 */
const CHAT_FRAME_KINDS: Record<string, ChatFrameKind> = {
  'message.start': 'start',
  'message.text_delta': 'text_delta',
  'message.end': 'end',
  'message.thinking_start': 'thinking_start',
  'message.thinking_delta': 'thinking_delta',
  'message.thinking_end': 'thinking_end',
  // 这一轮被受理成了一道目标：后面由任务与监督接管，不再是「一句话的问答」。
  'message.goal': 'goal',
  // 这一轮翻开了账上的某一条变更。点进去要落到变更账上的这一行，所以它不是
  // 正文里的几个字，而是一帧带着身份的证据。
  'message.change': 'change',
  // 用户按了停止，这一轮没有跑完。它排在 `message.end` 之前：收尾帧负责把光标
  // 收掉，而「是被停的」这件事只有这一帧说得出来。
  'message.cancelled': 'cancelled',
  // 说这句话的是谁：这一轮在总线上的名字。名字由服务端按会话折出（带进程指纹），
  // 前端算不出同一个，所以它跟着帧来。没有它，这位员工自己调的工具就归不到这段
  // 对话下面——帧上只有 `agent_id`，而这里不知道它等于谁。
  'agent.identity': 'identity',
  // 这一轮花了多少：token、轮次、时长、上下文占用、哪一路模型答的、算了多少钱。
  // 数字是循环量出来的那一份，网关原样搬来。它排在 `message.end` 之前——收尾帧
  // 一到，这一条就定稿了，还没读到的花销再也没有落点。
  'message.usage': 'usage',
  // 这一轮把一段早期上下文挤出了提示词：模型下一轮看不见它了。没有裁剪时
  // 服务端根本不发这一帧，所以它的出现本身就是「这一段对话开始装不下了」。
  'message.compaction': 'compaction',
};

/**
 * 一句话是谁说的。
 *
 * 只有这两个词。写者一共两处，都只写这两个：聊天套接字的 `message.start` 帧
 * （`crates/cog-gateway/src/chat.rs` 的 `emit_message_start`，三处调用分别传
 * `"user"` 与 `"assistant"`），以及落盘的对话行 `ChatTurnRole`（`user` /
 * `assistant`，库里的 check 约束也是这两个）。
 *
 * 曾经这里还有一个 `system`：`cog_core::Message::role()` 会返回它，但那条路是
 * 模型请求里的消息角色，不经过这两个写者；聊天面上它一格都到不了，于是「系统」
 * 那个分支从写下来那天起就没亮过。一个没有写者的词与一个拼错的词在界面上是同
 * 一种表现：永远不匹配。所以这里不摆它，真出现了第三个角色，写者那一侧先红。
 */
export type ChatRole = 'user' | 'assistant';

export interface ChatFrame {
  id: string;
  /** 这段对话的身份。多段对话各有一条流，帧必须带着它自己的那一条。 */
  sessionId: string;
  /** 一条消息的身份。同一条消息的第一帧是 `start`，之后按它聚合。 */
  messageId: string;
  kind: ChatFrameKind;
  /** 只有 `start` 带着角色；其余帧从这条消息已有的角色继承。 */
  role: ChatRole | null;
  /** 只有 `*_delta` 带着增量。 */
  delta: string | null;
  /** 只有 `goal` 帧带着：这句话被受理成的那道目标。 */
  goalId: string | null;
  /** 只有 `change` 帧带着：这一轮翻开的是变更账上的哪一条（`kind:scope:literal`）。 */
  changeId: string | null;
  /** 只有 `identity` 帧带着：这一轮在总线上叫什么（`employee:<指纹>` 形式）。 */
  agentId: string | null;
  /** 只有 `usage` 帧带着：这一轮的花销。 */
  usage: TurnUsage | null;
  /** 只有 `compaction` 帧带着：这一轮从提示词里挤掉了什么。 */
  compaction: ContextCompaction | null;
  /**
   * 只有超过窗口的 `start` 带着：这一句的原文落成了哪个文件、有多长。
   *
   * 服务端对长消息只回一段开头，原文在执行器那边成了一份文件；界面据此把这一条
   * 画成附件（带全文长度），而不是把开头当成这句话的全部。
   */
  attachment: ChatAttachment | null;
  timestamp: string;
}

/** 一条长消息落成的文件。`path` 是执行器上的路径，界面只拿来显示与回溯。 */
export interface ChatAttachment {
  path: string;
  /** 原文的字符数——界面上写的是这个数，不是开头那一段的长度。读不到就是 `null`。 */
  chars: number | null;
}

/** 帧里的附件格。没有路径就不认它：一个打不开的附件只能让人以为全文在那里。 */
function chatAttachment(value: unknown): ChatAttachment | null {
  if (!value || typeof value !== 'object') return null;
  const raw = value as { path?: unknown; chars?: unknown };
  if (typeof raw.path !== 'string' || !raw.path.trim()) return null;
  return { path: raw.path, chars: frameCount(raw.chars) };
}

/**
 * 一轮对话花掉的量，读的是服务端量出来的那一份。
 *
 * 除了模型名与费用，其余都是计数，缺了就发 `null`——这一层不替服务端补零：
 * 屏幕上写「输入 0 tokens」与写「输入没读到」是两句不同的话。
 */
export interface TurnUsage {
  inputTokens: number | null;
  outputTokens: number | null;
  totalTokens: number | null;
  /** 这一轮里模型开口了几次（一次工具调用之后接着说，算下一次）。 */
  rounds: number | null;
  /** 这一轮结束时上下文背了多少：最后一次调用带进去的提示词长度。 */
  contextTokens: number | null;
  durationMs: number | null;
  /** 答话的那一路模型。上游没报名字时 `null`，不是「未知模型」这个名字。 */
  model: string | null;
  /** 按声明价目算出来的美元数。上游没声明价目时 `null`：没算，不是免费。 */
  costUsd: number | null;
  /**
   * 这一段会话到目前为止的合计，账本按会话那一列折叠出来的。
   *
   * `readable: false` 时其余几格都没有——「没读到」不许画成「花了零」。
   */
  session: SessionSpend | null;
}

/** 一段会话到目前为止花了多少，钱按币种分列（价目本换过，不折汇率）。 */
export interface SessionSpend {
  readable: boolean;
  /** 读不到的原因（`readable: false` 时有）。 */
  reason: string | null;
  calls: number | null;
  inputTokens: number | null;
  outputTokens: number | null;
  cachedTokens: number | null;
  money: { currency: string; costMicros: number; pricedCalls: number }[];
  /** 到过上游但没定价的调用次数：它们的钱不在 `money` 里。 */
  unpricedCalls: number | null;
}

/**
 * 这一轮从提示词里挤掉了什么。
 *
 * 计数是循环量出来的那一份；两段开头是**原文的开头**，不是模型写的摘要——
 * 读者据此认出被挤掉的是哪一轮、留下的尾巴从哪一句开始，而不必相信一段转述。
 */
export interface ContextCompaction {
  /** 被挤出提示词的消息条数。 */
  droppedMessages: number | null;
  /** 这些消息按估算占了多少 token。 */
  freedTokens: number | null;
  /** 裁剪之后提示词里还剩多少条。 */
  keptMessages: number | null;
  /** 还在的那一段从哪一条开始（`role: 开头…`）。 */
  keptFrom: string | null;
  /** 被挤掉的第一条的开头（`role: 开头…`）。 */
  droppedHead: string | null;
  /** 这一次裁剪的界：窗口预算与低水位。 */
  budgetTokens: number | null;
  targetTokens: number | null;
}

/**
 * 帧里「这一段会话花了多少」那一格。
 *
 * 形状不对（旧服务端没有这一格、或者字段缺了）返回 `null`：没有这一格与
 * 「这一段花了零」是两句话，后者要由服务端明说 `calls: 0`。
 */
function sessionSpend(value: unknown): SessionSpend | null {
  if (!value || typeof value !== 'object') return null;
  const raw = value as Record<string, unknown>;
  if (typeof raw.readable !== 'boolean') return null;
  const money = Array.isArray(raw.money)
    ? raw.money
        .map((line) => {
          if (!line || typeof line !== 'object') return null;
          const cell = line as Record<string, unknown>;
          const currency = typeof cell.currency === 'string' ? cell.currency : null;
          const costMicros = frameCount(cell.cost_micros);
          const pricedCalls = frameCount(cell.priced_calls);
          if (currency === null || costMicros === null || pricedCalls === null) return null;
          return { currency, costMicros, pricedCalls };
        })
        .filter((line): line is { currency: string; costMicros: number; pricedCalls: number } =>
          line !== null
        )
    : [];
  return {
    readable: raw.readable,
    reason: typeof raw.reason === 'string' ? raw.reason : null,
    calls: frameCount(raw.calls),
    inputTokens: frameCount(raw.input_tokens),
    outputTokens: frameCount(raw.output_tokens),
    cachedTokens: frameCount(raw.cached_tokens),
    money,
    unpricedCalls: frameCount(raw.unpriced_calls),
  };
}

/** 帧里的数字转成界面认得的数：不是有限数就是 `null`，不猜 0。 */
function frameCount(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

/**
 * 一段聊天帧 → 界面认得的形状。认不出返回 `null`。
 *
 * `sessionId` 从信封上取而不是从帧体里取：帧体里没有它，而一段没有会话身份
 * 的消息落不进任何一段对话——它会被并到默认会话里，读者看到自己的话出现在
 * 别人的窗口里。
 */
export function normalizeChatFrame(
  payload: Record<string, unknown>,
  sessionId: string | undefined,
  fallbackId: string
): ChatFrame | null {
  const rawType = payload.type;
  if (typeof rawType !== 'string') return null;
  const kind = CHAT_FRAME_KINDS[rawType];
  if (!kind) return null;

  const inner = (payload.payload as Record<string, unknown> | undefined) ?? payload;
  const messageId = typeof inner.message_id === 'string' ? inner.message_id : null;
  if (!messageId) return null;

  const role = inner.role;
  return {
    id: (payload.id as string | undefined) ?? fallbackId,
    sessionId: sessionId && sessionId.trim() ? sessionId : 'default',
    messageId,
    kind,
    role: role === 'user' || role === 'assistant' ? role : null,
    delta: typeof inner.delta === 'string' ? inner.delta : null,
    goalId: typeof inner.goal_id === 'string' ? inner.goal_id : null,
    changeId: typeof inner.change_id === 'string' ? inner.change_id : null,
    agentId: typeof inner.agent_id === 'string' ? inner.agent_id : null,
    attachment: chatAttachment(inner.attachment),
    usage:
      kind === 'usage'
        ? {
            inputTokens: frameCount(inner.input_tokens),
            outputTokens: frameCount(inner.output_tokens),
            totalTokens: frameCount(inner.total_tokens),
            rounds: frameCount(inner.rounds),
            contextTokens: frameCount(inner.context_tokens),
            durationMs: frameCount(inner.duration_ms),
            model: typeof inner.model === 'string' ? inner.model : null,
            costUsd: frameCount(inner.cost_usd),
            session: sessionSpend(inner.session),
          }
        : null,
    compaction:
      kind === 'compaction'
        ? {
            droppedMessages: frameCount(inner.dropped_messages),
            freedTokens: frameCount(inner.freed_tokens),
            keptMessages: frameCount(inner.kept_messages),
            keptFrom: typeof inner.kept_from === 'string' ? inner.kept_from : null,
            droppedHead: typeof inner.dropped_head === 'string' ? inner.dropped_head : null,
            budgetTokens: frameCount(inner.budget_tokens),
            targetTokens: frameCount(inner.target_tokens),
          }
        : null,
    timestamp:
      (payload.timestamp as string | undefined) ?? new Date().toISOString(),
  };
}
