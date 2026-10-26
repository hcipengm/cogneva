import { create } from 'zustand';
import { getSessionMessages } from '@/api/client';
import type { ChatFrame } from '@/types/events';

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

export type ChatRole = 'user' | 'assistant' | 'system';

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
}

export interface GoalRef {
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

function emptySession(id: string): ChatSession {
  return {
    id,
    title: '新的对话',
    messages: [],
    hydrated: false,
    hydrating: false,
    hydrateError: null,
    streaming: null,
    goals: [],
  };
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

export const useChatStore = create<ChatState>((set, get) => ({
  sessions: { [FIRST]: emptySession(FIRST) },
  order: [FIRST],
  active: FIRST,

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
    return id;
  },

  setActive: (id) =>
    set((state) => (state.sessions[id] ? { active: id } : state)),

  closeSession: (id) =>
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
    }),

  renameSession: (id, title) =>
    set((state) => withSession(state, id, (session) => ({ ...session, title }))),

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
              { id, role: 'user', text, thinking: '', status: 'sending', failure: null, at },
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

  ingestFrame: (frame) =>
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
            pendingEcho.set(frame.sessionId, pending);
            session = withMessage(session, awaited, (message) => ({
              ...message,
              id: frame.messageId,
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

        case 'end': {
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            status: message.text.trim() ? 'done' : 'failed',
            failure: message.text.trim() ? message.failure : '这一轮没有说出任何内容',
          }));
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
      }

      return {
        sessions: { ...state.sessions, [frame.sessionId]: session },
        order: state.sessions[frame.sessionId]
          ? state.order
          : [frame.sessionId, ...state.order],
      };
    }),

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
            at: line.created_at,
          }));
          // 读回的历史排在前，本地还没落定的排在后面。已经落定、且历史里
          // 也有的本地那一条要去掉：同一句话不该出现两遍。
          const settled = new Set(remote.map((m) => `${m.role}\u0000${m.text}`));
          const live = session.messages.filter(
            (m) => m.status !== 'done' || !settled.has(`${m.role}\u0000${m.text}`)
          );
          return {
            ...session,
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
