import { create } from 'zustand';
import { getSessionMessages } from '@/api/client';
import type { ChatFrame, ChatRole, TurnUsage } from '@/types/events';

export type { ChatRole };

/**
 * 对话面的账。
 *
 * 一段对话有两个来源：自己刚发出去的那一句，和总线上流回来的每一帧。两边都要
 * 落在同一份账上，否则会出现两种大家都见过的坏画面——敲了回车屏幕上什么都没有，
 * 或者刷新之后那段对话凭空消失。所以这里的规则只有两条：
 *
 * - **发出去就先入账**。不等服务端回显。没有本地这一笔，网络慢一秒、或者那个
 *   部署的回声路径断了，用户看到的就是「我说话了，但这里什么都没发生」。
 * - **服务端的回声按身份接上，不另起一条**。回声到达时把本地那一条换成服务端的
 *   消息 id，后面的增量才接得上；不接就是把同一句话显示两遍。
 */

/**
 * 一条消息的四个状态。
 *
 * `sending` 与 `streaming` 分开：前者是「说出去，还没有人开始答」，后者是
 * 「有东西正在往外流」。合成一个，界面上就看不出「对面没反应」与「对面正在想」。
 */
export type ChatStatus = 'sending' | 'streaming' | 'done' | 'failed';

export interface ChatMessage {
  id: string;
  role: ChatRole;
  text: string;
  /** 模型的思考流。与正文分开存：它不是回答，不能混进正文里。 */
  thinking: string;
  status: ChatStatus;
  /** 这一轮没答上来的原因。有值即 `failed`（或 `done` 但内容为空）。 */
  failure: string | null;
  /**
   * 这一轮花掉的量，由服务端的 `usage` 帧带进来。
   *
   * `null` 不是「花了零」：读回的历史里没有这一格（服务端只把正文落了库），
   * 失败与被叫停的那一轮也没有。界面上因此不画这一行，而不是画一排零。
   */
  usage: TurnUsage | null;
  at: string;
}

export interface ChatSession {
  id: string;
  title: string;
  messages: ChatMessage[];
  /** 历史读过没有。读过就不再读——重读会把刚流的这一段冲掉。 */
  hydrated: boolean;
  hydrating: boolean;
  hydrateError: string | null;
  /** 正在流的那一条。 */
  streaming: string | null;
  /**
   * 这段对话受理成的目标，带交出时间。空表示它到现在还只是「一问一答」——关掉
   * 窗口，这一轮就没有下文了。非空表示它有活交给了编排器：后面的分解、调度与
   * 重试由监督接管，关掉窗口它继续跑，回来接得上。
   *
   * 交出的时刻要留着：执行者的工具调用帧只带 `agent_id`，要把它们归到这道目标
   * 下面，除了「谁」还得知道「从什么时候起」。
   */
  goals: GoalRef[];
  /**
   * 这一段的员工在总线上叫什么。由服务端在第一帧之前的 `agent.identity` 帧带进来。
   *
   * 它是「这一面画的是谁」的钥匙：工具调用帧只有 `agent_id`，没有它，这段对话
   * 自己的执行过程（调了哪个工具、跑了几次）一条也接不回来，屏幕上就只剩下一问
   * 一答。空表示还不知道——这一轮还没开始，或者服务端比这一页老。
   */
  agentId: string | null;
  /**
   * 这一段对话翻开过的变更，带翻开的时间。
   *
   * 与目标同一类东西：它们是这段对话在账上真读过的那几行，而正文里提到的「那条
   * 变更」只是一句话。点得到账上的那一行，靠的是这串身份。
   */
  changes: ChangeRef[];
}

export interface GoalRef {
  id: string;
  at: string;
}

/** 账上的一条变更：`kind:scope:literal` 三段身份。 */
export interface ChangeRef {
  id: string;
  at: string;
}

/**
 * 一段对话的两种身份，读的是它自己有没有把活交出去。
 *
 * 不按「用户勾了常驻吗」判：勾选只是意愿，而界面要说的是这件事**现在**有没有人
 * 接着做。目标是平台里唯一那种「关掉浏览器还在跑」的东西，所以有目标就是常驻，
 * 没有就是可下线。
 */
export type SessionKind = 'chat' | 'dispatch';

export function sessionKind(session: Pick<ChatSession, 'goals'>): SessionKind {
  return session.goals.length > 0 ? 'dispatch' : 'chat';
}

interface ChatState {
  sessions: Record<string, ChatSession>;
  /** 拨号条上的顺序，最新的在前。 */
  order: string[];
  active: string;

  createSession: (title?: string) => string;
  /**
   * 打开一段已经存在的会话（名字来自员工名单）。
   *
   * 名单上的人是服务端按会话 id 算出来的，所以「接着跟他聊」就是把那段会话放回
   * 工作台——不是新建一段同名的新对话。这段会话的本地消息可能还没读过：`hydrate`
   * 会按同一个 id 从服务端把历史读回来。
   */
  openSession: (id: string) => void;
  setActive: (id: string) => void;
  closeSession: (id: string) => void;
  renameSession: (id: string, title: string) => void;
  /** 本地先入账。返回本地那一条的 id。 */
  appendUserTurn: (sessionId: string, text: string) => string;
  ingestFrame: (frame: ChatFrame) => void;
  /** 从服务端读回历史。已读过时直接返回。 */
  hydrate: (sessionId: string) => Promise<void>;
}

/**
 * 已经发出去、还没等到服务端回声的本地消息 id，按先后排。
 *
 * 放在 store 外面：它是「还欠着几次回声」的记账，不是界面要画的东西，进去只会
 * 让每次回声都触发一轮与它无关的重渲染。
 */
const pendingEcho = new Map<string, string[]>();

function localId(): string {
  return `local-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function titleFrom(text: string): string {
  const line = text.replace(/\s+/g, ' ').trim();
  return line.length > 18 ? `${line.slice(0, 18)}…` : line || '新的对话';
}

/**
 * 默认那段对话的身份。与服务端此前的默认值保持一致：改一个字母，之前存下的
 * 对话就再也读不回来了，而它们看起来只是「不见了」。
 */
const FIRST = 'default';

function emptySession(id: string, title = '新的对话'): ChatSession {
  return {
    id,
    title,
    messages: [],
    hydrated: false,
    hydrating: false,
    hydrateError: null,
    streaming: null,
    goals: [],
    agentId: null,
    changes: [],
  };
}

/**
 * 开着哪几段对话，记在这一台机器的这个浏览器里。
 *
 * 与左栏宽度、页签顺序同一类：它回答的是「我上次开着什么」，不是平台的事。
 * 不记的话，刷新一次多段对话就全回到默认那一段——而「同时开几段、来回切」正是
 * 这个界面存在的理由之一。
 */
const DOCK_STORAGE_KEY = 'cogneva.chatSessions';

interface DockEntry {
  id: string;
  title: string;
}

function bootDock(): { sessions: Record<string, ChatSession>; order: string[]; active: string } {
  let entries: DockEntry[] = [{ id: FIRST, title: '新的对话' }];
  try {
    const raw = window.localStorage.getItem(DOCK_STORAGE_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : null;
    if (Array.isArray(parsed)) {
      const read = parsed
        .filter(
          (entry): entry is DockEntry =>
            Boolean(entry) &&
            typeof (entry as DockEntry).id === 'string' &&
            (entry as DockEntry).id.length > 0 &&
            typeof (entry as DockEntry).title === 'string'
        )
        .map((entry) => ({ id: entry.id, title: entry.title }));
      if (read.length > 0) entries = read;
    }
  } catch {
    // 隐私模式下面 localStorage 会抛：这一段照样能开新对话，只是刷新后回到默认。
  }
  const sessions: Record<string, ChatSession> = {};
  const order: string[] = [];
  for (const entry of entries) {
    if (sessions[entry.id]) continue;
    sessions[entry.id] = emptySession(entry.id, entry.title);
    order.push(entry.id);
  }
  return { sessions, order, active: order[0] };
}

/** 把现在的顺序与标题写回去。写不进去只影响下一次打开，不影响这一屏。 */
function saveDock(state: Pick<ChatState, 'sessions' | 'order'>): void {
  try {
    const entries = state.order
      .filter((id) => state.sessions[id])
      .map((id) => ({ id, title: state.sessions[id].title }));
    window.localStorage.setItem(DOCK_STORAGE_KEY, JSON.stringify(entries));
  } catch {
    // 同上。
  }
}

/** 改一段会话的一份副本再放回去。所有写入都走它，免得某处漏改一个字段。 */
function withSession(
  state: ChatState,
  id: string,
  patch: (session: ChatSession) => ChatSession
): Partial<ChatState> {
  const session = state.sessions[id];
  if (!session) return {};
  return { sessions: { ...state.sessions, [id]: patch(session) } };
}

/** 改一条消息的一份副本。消息不在时什么都不做——迟到的帧不该把列表撑开。 */
function withMessage(
  session: ChatSession,
  messageId: string,
  patch: (message: ChatMessage) => ChatMessage
): ChatSession {
  let touched = false;
  const messages = session.messages.map((message) => {
    if (message.id !== messageId) return message;
    touched = true;
    return patch(message);
  });
  return touched ? { ...session, messages } : session;
}

const boot = bootDock();

export const useChatStore = create<ChatState>((set, get) => ({
  sessions: boot.sessions,
  order: boot.order,
  active: boot.active,

  createSession: (title) => {
    const id = `conv-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
    set((state) => ({
      sessions: {
        ...state.sessions,
        [id]: { ...emptySession(id), title: title ?? `对话 ${state.order.length + 1}` },
      },
      order: [id, ...state.order],
      active: id,
    }));
    saveDock(get());
    return id;
  },

  openSession: (id) => {
    set((state) => {
      if (state.sessions[id]) return { active: id };
      return {
        sessions: { ...state.sessions, [id]: { ...emptySession(id), title: '从名单打开' } },
        order: [id, ...state.order],
        active: id,
      };
    });
    saveDock(get());
  },

  setActive: (id) =>
    set((state) => (state.sessions[id] ? { active: id } : state)),

  closeSession: (id) => {
    set((state) => {
      pendingEcho.delete(id);
      const order = state.order.filter((other) => other !== id);
      const { [id]: _dropped, ...sessions } = state.sessions;
      // 最后一段不关：对话面空着的时候没有落点，而「关掉全部对话」不是任何人
      // 想要的结果。关掉当前那段时焦点移到相邻的那一段。
      if (order.length === 0) return state;
      return {
        sessions,
        order,
        active: state.active === id ? order[0] : state.active,
      };
    });
    saveDock(get());
  },

  renameSession: (id, title) => {
    set((state) => withSession(state, id, (session) => ({ ...session, title })));
    saveDock(get());
  },

  appendUserTurn: (sessionId, text) => {
    const id = localId();
    const at = new Date().toISOString();
    set((state) => {
      const base = state.sessions[sessionId] ?? emptySession(sessionId);
      const pending = pendingEcho.get(sessionId) ?? [];
      pending.push(id);
      pendingEcho.set(sessionId, pending);
      const firstTurn = !base.messages.some((m) => m.role === 'user');
      return {
        sessions: {
          ...state.sessions,
          [sessionId]: {
            ...base,
            title: firstTurn ? titleFrom(text) : base.title,
            messages: [
              ...base.messages,
              {
                id,
                role: 'user',
                text,
                thinking: '',
                status: 'sending',
                failure: null,
                usage: null,
                at,
              },
            ],
          },
        },
        order: state.sessions[sessionId]
          ? state.order
          : [sessionId, ...state.order],
      };
    });
    return id;
  },

  ingestFrame: (frame) => {
    const known = Boolean(get().sessions[frame.sessionId]);
    set((state) => {
      const base = state.sessions[frame.sessionId] ?? emptySession(frame.sessionId);
      let session = base;

      switch (frame.kind) {
        case 'start': {
          const pending = pendingEcho.get(frame.sessionId) ?? [];
          const awaited = frame.role === 'user' ? pending.shift() : undefined;
          if (awaited) {
            // 服务端把用户那一句原样回声回来了：把本地那一条换成它的身份，
            // 不新开一条。不换就是同一句话在屏幕上出现两遍，而消息 id 一变，
            // 接下来那几帧增量也就没有落点了。
            //
            // 正文也要清空，而且只能由这里清：服务端紧接着把这句话当**增量**再发
            // 一遍（`start` → `text_delta(content)` → `end`），那是这句话在屏幕上
            // 唯一的来源。留着本地那份抄本，增量一拼上去就是同一句连着写两遍
            // ——没有分隔、没有气泡边界，读起来像用户把话打重了（实测 2026-11-04，
            // 属主在 Live 面板里为此提了两次）。本地那份本来就是猜的，服务端的字节
            // 才是这句话。
            pendingEcho.set(frame.sessionId, pending);
            session = withMessage(session, awaited, (message) => ({
              ...message,
              id: frame.messageId,
              text: '',
              status: 'streaming',
            }));
          } else {
            session = {
              ...session,
              messages: [
                ...session.messages,
                {
                  id: frame.messageId,
                  role: frame.role ?? 'assistant',
                  text: '',
                  thinking: '',
                  status: 'streaming',
                  failure: null,
                  usage: null,
                  at: frame.timestamp,
                },
              ],
            };
          }
          if ((frame.role ?? 'assistant') === 'assistant') {
            session = { ...session, streaming: frame.messageId };
          }
          break;
        }

        case 'text_delta': {
          if (!frame.delta) break;
          const delta = frame.delta;
          if (!session.messages.some((m) => m.id === frame.messageId)) {
            // 先到的增量：占位一条，别让这段话丢掉。
            session = {
              ...session,
              messages: [
                ...session.messages,
                {
                  id: frame.messageId,
                  role: 'assistant',
                  text: '',
                  thinking: '',
                  status: 'streaming',
                  failure: null,
                  usage: null,
                  at: frame.timestamp,
                },
              ],
            };
          }
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            text: message.text + delta,
            status: 'streaming',
          }));
          break;
        }

        case 'cancelled': {
          // 被停掉的那一轮：它没有失败原因（模型没坏），也不该被画成答完了。
          // 原因只写一次——收尾帧紧跟着来，再来一次会把这句话盖成别的原因。
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            status: 'failed',
            failure: message.failure ?? '已中止（这一轮没有跑完）',
          }));
          if (session.streaming === frame.messageId) {
            session = { ...session, streaming: null };
          }
          break;
        }

        case 'end': {
          session = withMessage(session, frame.messageId, (message) => {
            // 已经有原因的（被停掉、上游失败）不再改写成别的：收尾帧只负责收掉
            // 这一轮，它不知道这一轮为什么停，而屏幕上的原因已经写对了。
            if (message.failure) return { ...message, status: 'failed' };
            const spoke = Boolean(message.text.trim());
            return {
              ...message,
              status: spoke ? 'done' : 'failed',
              failure: spoke ? null : '这一轮没有说出任何内容',
            };
          });
          if (session.streaming === frame.messageId) {
            session = { ...session, streaming: null };
          }
          break;
        }

        case 'thinking_start':
          session = withMessage(session, frame.messageId, (message) => message);
          break;

        case 'thinking_delta': {
          if (!frame.delta) break;
          const delta = frame.delta;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            thinking: message.thinking + delta,
          }));
          break;
        }

        case 'thinking_end':
          break;

        case 'identity': {
          if (!frame.agentId || frame.agentId === session.agentId) break;
          session = { ...session, agentId: frame.agentId };
          break;
        }

        case 'usage': {
          // 记在说这句话的那一条上，不另开一行：这一轮花了多少是这条消息的
          // 属性，不是一句新的话。帧排在 `end` 之前，所以它到的时候这一条还
          // 在流里，`withMessage` 找得到。
          if (!frame.usage) break;
          const usage = frame.usage;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            usage,
          }));
          break;
        }

        case 'goal': {
          if (!frame.goalId) break;
          if (!session.goals.some((goal) => goal.id === frame.goalId)) {
            session = {
              ...session,
              goals: [...session.goals, { id: frame.goalId, at: frame.timestamp }],
            };
          }
          break;
        }

        case 'change': {
          if (!frame.changeId) break;
          if (!session.changes.some((change) => change.id === frame.changeId)) {
            session = {
              ...session,
              changes: [...session.changes, { id: frame.changeId, at: frame.timestamp }],
            };
          }
          break;
        }
      }

      return {
        sessions: { ...state.sessions, [frame.sessionId]: session },
        order: state.sessions[frame.sessionId]
          ? state.order
          : [frame.sessionId, ...state.order],
      };
    });
    // 帧带来了一段新的对话：它现在开在拨号条上，刷新之后也应该还在。
    if (!known) saveDock(get());
  },

  hydrate: async (sessionId) => {
    const existing = get().sessions[sessionId];
    if (!existing || existing.hydrated || existing.hydrating) return;

    set((state) =>
      withSession(state, sessionId, (session) => ({ ...session, hydrating: true }))
    );

    try {
      const lines = await getSessionMessages(sessionId);
      set((state) =>
        withSession(state, sessionId, (session) => {
          const remote: ChatMessage[] = lines.map((line) => ({
            id: line.id,
            role: line.role,
            text: line.content,
            thinking: '',
            status: line.failure ? 'failed' : 'done',
            failure: line.failure,
            // 落库的只有正文：这一次读回来的花销没有存放处，于是空着。
            usage: null,
            at: line.created_at,
          }));
          // 读回的历史排在前，本地还没落定的排在后面。已经落定、且历史里
          // 也有的本地那一条要去掉：同一句话不该出现两遍。
          const settled = new Set(remote.map((m) => `${m.role}\u0000${m.text}`));
          const live = session.messages.filter(
            (m) => m.status !== 'done' || !settled.has(`${m.role}\u0000${m.text}`)
          );
          // 交出去的目标从记录里读回来，不从内存里读：刷新之后，这段对话是
          // 「能关掉的问答」还是「还在服务器上跑的目标」，答案只有历史里有。
          // 本地已经收到的那些排后面去重，同一道目标不重复记。
          const goals = [...session.goals];
          for (const line of lines) {
            if (!line.goal_id) continue;
            if (goals.some((goal) => goal.id === line.goal_id)) continue;
            goals.push({ id: line.goal_id, at: line.created_at });
          }
          return {
            ...session,
            goals,
            messages: [...remote, ...live],
            hydrated: true,
            hydrating: false,
            hydrateError: null,
          };
        })
      );
    } catch (error) {
      const message =
        (error as { message?: string })?.message ?? '读不到这段对话的记录';
      set((state) =>
        withSession(state, sessionId, (session) => ({
          ...session,
          hydrating: false,
          // 读失败不标 hydrated：下一次打开还要再试。把失败当成「读过且为空」，
          // 这段对话就永远读不回来了。
          hydrateError: message,
        }))
      );
    }
  },
}));
