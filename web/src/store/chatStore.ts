import { create } from 'zustand';
import { getSessionMessages, listChatSessions } from '@/api/client';
import { useStreamStore } from '@/store/streamStore';
import type { ChatSessionSummary } from '@/api/client';
import type {
  BackgroundCard,
  ChatAttachment,
  ChatFrame,
  ChatRole,
  ContextCompaction,
  ColleagueMessage,
  HookReading,
  LoadedMemory,
  McpListing,
  TurnTaskRow,
  TurnUsage,
} from '@/types/events';
import { NOTICE_MESSAGE_ID_PREFIX } from '@/types/events';

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

/**
 * 重连时服务端交回的一条消息：这一段现在说到哪儿了。
 *
 * 它来自 `chat_resume` 那一帧，语义是**替换**：正文与思考按这里的样子整段换掉，
 * 之后到的增量接着往上写。所以「重连两次也不会把同一句话叠两遍」是这条语义
 * 直接给的，而不是靠客户端去记每一帧有没有见过。
 */
export interface ResumedMessage {
  messageId: string;
  role: ChatRole;
  text: string;
  thinking: string;
  /** 落成文件的那条长消息（`message.start` 上带的那一格）。 */
  attachment: ChatAttachment | null;
  /** 这一句已经收尾。没收尾的就是「它还在说」。 */
  ended: boolean;
}

/**
 * 一段对话能选的那几只模型，以及为什么一只都没有。
 *
 * `reason` 与空清单同时出现才对得上：清单非空时它讲的是「只读到一半」的原因，可以
 * 不显示；清单为空时它就是界面上要画的那句话——「平台还没配上游」与「安全网关说不
 * 出模型」是两句不同的话，只有前一句在告诉人去哪儿修。
 */
export interface ModelCatalogue {
  models: string[];
  /** 平台说「有东西被配了」。空清单 + `false` = 还没配；空清单 + `true` = 配了但读不出名字。 */
  configured: boolean;
  reason: string | null;
}

/**
 * 这一段对话记下的一个检查点：哪一轮**结束**时的树。
 *
 * `turn === 0` 是会话开头——第一轮之前没有「上一轮结束」，退回到第一轮之前靠的
 * 就是它。
 */
export interface CheckpointRow {
  key: string;
  turn: number;
  at: string;
}

/**
 * 一次退回会碰到的一个文件。
 *
 * `change` 说的是**退回要做什么**：`added` 是现在有、快照里没有，退回就是删掉；
 * `removed` 反过来，退回要写回来。
 */
export interface CheckpointFile {
  path: string;
  change: string;
}

/** 一次退回的结局：问到的那一份、会碰的文件、以及树到底动没动。 */
export interface RewindReport {
  turn: number;
  target: number;
  applied: boolean;
  files: CheckpointFile[];
  truncated: boolean;
}

/**
 * 一段对话的检查点读面。
 *
 * `reason` 有值时 `rows` 与 `rewind` 都不可信：那是「做不了」的一句话，不是
 * 「没有」。两句话分开存，界面上才分得清该再跑一轮还是去换一个部署。
 */
export interface CheckpointState {
  rows: CheckpointRow[];
  /** 更早的还有几份没列出来。 */
  hidden: number;
  /** 最近一次问到会改什么 / 退成什么样；`null` = 还没问过。 */
  rewind: RewindReport | null;
  reason: string | null;
}

/** 一条边界：哪个维度、这一层管没管住、读数是什么。 */
export interface BoundaryLine {
  dimension: string;
  /** `enforced` / `declared` / `none` / `unknown` 四个词，见传输层的注释。 */
  state: string;
  detail: string;
}

/**
 * 这一段对话里的命令跑在什么样的环境里。
 *
 * `reason` 有值时 `boundary` 不可信：那是「这个部署答不出来」的一句话，不是一份
 * 空边界。把它画成「什么都不限制」，人会把「没人回答」当成「没有边界」。
 */
export interface SandboxBoundaryState {
  boundary: {
    kind: string;
    isolated: boolean;
    workdir: string | null;
    lines: BoundaryLine[];
  } | null;
  reason: string | null;
}

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
  /**
   * 这一轮从提示词里挤掉了什么，由服务端的 `compaction` 帧带进来。
   *
   * `null` 是「这一轮没有裁剪」，不是「裁了 0 条」：服务端没有裁剪就不发这一帧，
   * 界面因此不画那一行。读回的历史里也没有这一格——压缩是这一轮发生的事，
   * 不是这条消息的属性。
   */
  compaction: ContextCompaction | null;
  /**
   * 这一轮读进了哪几句属主约定，由服务端的 `memory` 帧带进来。
   *
   * `null` 是「这一轮没有这一帧」——平台没接读面，或者这一轮在发帧之前就结束了；
   * 界面上因此不画这一行，而不是写「加载了 0 条」。
   */
  memory: LoadedMemory[] | null;
  /** 这一轮的外部 MCP 服务，由服务端的 `mcp` 帧带进来；`null` 是没挂外部服务。 */
  mcp: McpListing | null;
  /**
   * 这一轮配了哪些钩子、哪几条跑过，由服务端的 `hooks` 帧带进来。
   *
   * `null` 是「这一轮没有这一帧」：平台没接钩子表。配了但一条都没跑过时它是
   * 一份有 `configured`、`ran` 为空的读数——那正好是「配了、没生效」的样子。
   */
  hooks: HookReading | null;
  /**
   * 这一轮发给同事的每一条消息，由服务端的 `colleague` 帧带进来（一条一帧）。
   *
   * `null` 是「这一轮没跟同事说过话」，不是「平台不做这件事」：能发消息的部署
   * 在真发过时才发这一帧。数组按到达顺序累计。
   */
  colleagues: ColleagueMessage[] | null;
  /**
   * 这一轮挂上共享任务图的活，由服务端的 `tasks` 帧带进来（一帧带整份列表）。
   *
   * `null` 是「这一轮没派活」，不是「平台没有任务图」：真派过活才发这一帧。
   * 帧里只有派活那一刻的事实（哪一行、什么活、派给谁）；状态由面板拿这些 id
   * 去任务图那同一张表里读回，所以这里不存状态。
   */
  tasks: TurnTaskRow[] | null;
  /**
   * 这一条消息上的后台命令卡片，由服务端的 `background` 帧带进来。
   *
   * `null` 是「这条消息上没有后台命令」。同一张卡片先到的是「在跑」，跑完时
   * 被结局那一版盖掉——不是两张卡片：一条命令只有一个句柄，也就只有一张卡。
   */
  background: BackgroundCard | null;
  /**
   * 这一句的原文落成的文件。只有超过窗口的那些带它：服务端把原文逐字写进执行器
   * 的一个文件，界面上这一条因此画成附件（带全文长度），而不是把开头当成全部。
   *
   * 读回的历史里没有这一格（记录只存正文），界面按正文长度把它画成同一类附件。
   */
  attachment: ChatAttachment | null;
  /**
   * 随这一句发上去的附件（图片 / PDF / 音频 / 视频）。
   *
   * 只存认人的那几格：名字、类型、字节数。数据本身（base64）只在发出之前活一
   * 小会儿，账上不留——一屏几十张图的内存不该由「刷新之后还认得它」来换。
   * 记录里存着名字与类型，所以刷新之后这一行还在；字节数没跟着回来，那一格
   * 是 `null`，界面上不写大小，而不是写一个零。
   */
  files: ChatFile[];
  at: string;
}

/** 一条附件的身份：够在消息上认出来、说得清多大，不带数据本身。 */
export interface ChatFile {
  name: string;
  mimeType: string;
  /** 读回的历史里没有这一格：记录只存名字与类型。 */
  bytes: number | null;
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
   * 这一段对话现在选的那只模型。`null` 是「没选过」——走上游配置的那一只，不是
   * 一只被这一页替人挑好的模型。选择存在服务端、按「这个人 + 这一段」记，所以
   * 同一段对话换一台机器打开，看到的还是它。
   */
  model: string | null;
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
  /**
   * 服务端那份会话名单，最近说话的在前。
   *
   * 与 `sessions` 不是同一份账：`sessions` 是这一页开着的那几段（存在浏览器
   * 里），这份是这个人在服务端有过的全部会话。换一台机器、清掉浏览器存储之后，
   * 找回来的路只有这一份。
   */
  history: ChatSessionSummary[];
  historyLoading: boolean;
  /** 读名单失败的原因；读成功时是 `null`。空名单与读失败不合成一句。 */
  historyError: string | null;
  /**
   * 平台上配着的那几只模型，与「为什么一只都没有」。
   *
   * 部署级的一份，不是每段对话一份：清单来自平台已经配置好的那几处，同一台部署上
   * 各段对话看到的是同一张表。逐段记会在两段对话之间出现两份可能不一致的清单。
   *
   * `null` 是「还没问过」——不是「一只都没有」。两者画成同一句，界面上就会在连上
   * 之前先说「平台没配模型」。
   */
  modelCatalogue: ModelCatalogue | null;
  /**
   * 检查点，按会话分。`undefined` 是「还没问过」——与一张空清单（问到了，
   * 一份也没有）不是同一件事。
   */
  checkpoints: Record<string, CheckpointState>;
  /**
   * 命令跑在什么样的环境里，按会话分。`undefined` 是「还没问过」——与一份读不到
   * 的边界（问到了，答不出来）不是同一件事。
   */
  boundaries: Record<string, SandboxBoundaryState>;

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
  appendUserTurn: (sessionId: string, text: string, files?: ChatFile[]) => string;
  ingestFrame: (frame: ChatFrame) => void;
  /**
   * 重连之后的续流：把这一段现在说到哪儿了接上。
   *
   * 断线时那一轮没有停，字也没有丢——它们记在这一段的服务端快照里。这一帧到达
   * 时按消息整段替换正文与思考，之后到的增量接着往上写；所以这一趟既补上了断线
   * 期间说的字，也不会把重连之前已经画出来的字再写一遍。
   */
  applyResume: (sessionId: string, messages: ResumedMessage[]) => void;
  /**
   * 服务端答回来的「这一段用哪只模型、还能选哪些」。
   *
   * 一次往返办两件事，所以两件事一起落：`model` 是这一段现在的那一只（`null` =
   * 没选过），`catalogue` 是能选的那一份（空清单带一句为什么——「还没配」与
   * 「配了但没模型」都要说得出来，而一只默认型号会让下一轮失败）。
   */
  applyModel: (sessionId: string, model: string | null, catalogue: ModelCatalogue) => void;
  /**
   * 这一段对话的检查点：清单，或者一次退回的结局。
   *
   * 不走消息流：它是这一段的状态，不是这一轮说出来的话。按会话存，因为退回
   * 就是把**这一段**的树带回某一刻——两段对话的检查点混在一格里，点错一行
   * 改的就是另一段对话的树。
   */
  applyCheckpoints: (sessionId: string, checkpoint: CheckpointState) => void;
  /** 断开时清掉：上一条连接读到的不该留在屏幕上冒充现状。 */
  forgetCheckpoints: () => void;
  /** 服务端答回来的边界读数。它是一份部署读数，按会话存，换一段就问一次。 */
  applyBoundary: (sessionId: string, boundary: SandboxBoundaryState) => void;
  /** 断开时清掉：上一条连接读到的边界不该留在屏幕上冒充现状。 */
  forgetBoundaries: () => void;
  /** 从服务端读回历史。已读过时直接返回。 */
  hydrate: (sessionId: string) => Promise<void>;
  /** 读服务端那份会话名单，可带一个搜索词。 */
  listSessions: (search?: string) => Promise<void>;
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
export const FIRST_SESSION = 'default';

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
    model: null,
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
  let entries: DockEntry[] = [{ id: FIRST_SESSION, title: '新的对话' }];
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
  history: [],
  historyLoading: false,
  historyError: null,
  modelCatalogue: null,
  /**
   * 检查点按会话分。`undefined` 是「还没问过」——与 `{rows: []}`（问到了，
   * 一份也没有）不是同一件事，画成同一句会让人在连上之前就以为退不回去。
   */
  checkpoints: {},
  boundaries: {},

  /**
   * 读名单。空结果与读失败分开：空名单是「你确实没有别的会话」，读失败是
   * 「这一页不知道你有没有」，把后者画成前者，人就会以为自己丢了东西。
   */
  listSessions: async (search) => {
    set({ historyLoading: true, historyError: null });
    try {
      const items = await listChatSessions(search);
      set({ history: items, historyLoading: false });
    } catch (error) {
      set({
        history: [],
        historyLoading: false,
        historyError:
          (error as { message?: string })?.message ?? '读不到会话名单',
      });
    }
  },

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

  appendUserTurn: (sessionId, text, files = []) => {
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
                compaction: null,
                memory: null,
                mcp: null,
                hooks: null,
                colleagues: null,
                tasks: null,
                background: null,
                attachment: null,
                files,
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
              attachment: frame.attachment,
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
                  compaction: null,
                  memory: null,
                  mcp: null,
                  hooks: null,
                  colleagues: null,
                  tasks: null,
                  background: null,
                  attachment: frame.attachment,
                  files: [],
                  at: frame.timestamp,
                },
              ],
            };
          }
          // 平台自己说的那条通知（后台命令跑完）同样是一段 assistant 正文，但它
          // 不在任何一轮里：它不收这一轮的光标，收尾时也就不清别人的。否则一条
          // 后台命令恰好在回答流到一半时跑完，屏幕上的光标会跟着它提前熄灭。
          if (
            (frame.role ?? 'assistant') === 'assistant' &&
            !frame.messageId.startsWith(NOTICE_MESSAGE_ID_PREFIX)
          ) {
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
                  compaction: null,
                  memory: null,
                  mcp: null,
                  hooks: null,
                  colleagues: null,
                  tasks: null,
                  background: null,
                  attachment: null,
                  files: [],
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

        case 'compaction': {
          // 记在说这句话的那一条上，与花销同一种落点：挤掉的是这一轮模型手上
          // 少看到的那一段，是这条回答的背景，不是一句新的话。
          if (!frame.compaction) break;
          const compaction = frame.compaction;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            compaction,
          }));
          break;
        }

        case 'memory': {
          // 与花销同一处落点：它是这条回答的前提，不是一句新的话。
          if (!frame.memory) break;
          const memory = frame.memory;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            memory,
          }));
          break;
        }

        case 'mcp': {
          if (!frame.mcp) break;
          const mcp = frame.mcp;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            mcp,
          }));
          break;
        }

        case 'hooks': {
          // 与花销同一处落点：它说的是这条回答是在什么规则下说出来的。
          if (!frame.hooks) break;
          const hooks = frame.hooks;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            hooks,
          }));
          break;
        }

        case 'colleague': {
          // 一条消息一帧，同一条回答下按到达顺序累计：一次发好几条时，最后那条
          // 不能把前面的盖掉。
          if (!frame.colleague) break;
          const colleague = frame.colleague;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            colleagues: [...(message.colleagues ?? []), colleague],
          }));
          break;
        }

        case 'tasks': {
          // 一帧就是整份列表：它说的是「这一轮挂上了哪几行」，不按行累计。
          if (!frame.tasks) break;
          const tasks = frame.tasks;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            tasks,
          }));
          break;
        }

        case 'background': {
          // 同一张卡片先「在跑」后「结局」：按句柄找现有的那张，再整张换掉。
          // message_id 是卡片挂在哪一条消息上，不同命令各有各的一条。
          if (!frame.background) break;
          session = withMessage(session, frame.messageId, (message) => ({
            ...message,
            background: frame.background,
          }));
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

  applyResume: (sessionId, messages) => {
    const known = Boolean(get().sessions[sessionId]);
    set((state) => {
      let session = state.sessions[sessionId] ?? emptySession(sessionId);
      for (const line of messages) {
        // 用户那一句永远是说完的：它不是「还在流」的那一条。
        const status: ChatStatus =
          line.ended || line.role === 'user' ? 'done' : 'streaming';
        if (session.messages.some((message) => message.id === line.messageId)) {
          session = withMessage(session, line.messageId, (message) => ({
            ...message,
            // 整段替换：这就是续流的语义。
            text: line.text,
            thinking: line.thinking,
            // 已经判成失败的那一条不被快照改写：失败原因是那一轮的事实，
            // 而快照只说「说到哪儿了」。
            status: message.status === 'failed' ? 'failed' : status,
            attachment: line.attachment ?? message.attachment,
          }));
        } else {
          session = {
            ...session,
            messages: [
              ...session.messages,
              {
                id: line.messageId,
                role: line.role,
                text: line.text,
                thinking: line.thinking,
                status,
                failure: null,
                usage: null,
                compaction: null,
                memory: null,
                mcp: null,
                hooks: null,
                colleagues: null,
                tasks: null,
                background: null,
                attachment: line.attachment,
                files: [],
                at: new Date().toISOString(),
              },
            ],
          };
        }
      }
      // 「正在流」只有一条：快照里没收尾的那一句。一条都没有时，还停在流上的
      // 那些是断了的那一头留下的残影——那一轮已经不在跑了，不该永远转圈。
      const inFlight = messages.filter(
        (line) => !line.ended && line.role !== 'user'
      );
      const streaming = inFlight.length > 0 ? inFlight[inFlight.length - 1].messageId : null;
      session =
        streaming === null
          ? {
              ...session,
              streaming: null,
              messages: session.messages.map((message) =>
                message.status === 'streaming' || message.status === 'sending'
                  ? { ...message, status: 'done' as ChatStatus }
                  : message
              ),
            }
          : { ...session, streaming };
      return {
        sessions: { ...state.sessions, [sessionId]: session },
        order: known ? state.order : [sessionId, ...state.order],
      };
    });
    if (!known) saveDock(get());
  },

  applyModel: (sessionId, model, catalogue) => {
    set((state) => ({
      modelCatalogue: catalogue,
      ...withSession(state, sessionId, (session) => ({ ...session, model })),
    }));
  },

  applyCheckpoints: (sessionId, checkpoint) => {
    set((state) => ({
      checkpoints: { ...state.checkpoints, [sessionId]: checkpoint },
    }));
  },

  forgetCheckpoints: () => {
    set({ checkpoints: {} });
  },

  applyBoundary: (sessionId, boundary) => {
    set((state) => ({
      boundaries: { ...state.boundaries, [sessionId]: boundary },
    }));
  },

  forgetBoundaries: () => {
    set({ boundaries: {} });
  },

  hydrate: async (sessionId) => {
    const existing = get().sessions[sessionId];
    if (!existing || existing.hydrated || existing.hydrating) return;

    set((state) =>
      withSession(state, sessionId, (session) => ({ ...session, hydrating: true }))
    );

    try {
      const lines = await getSessionMessages(sessionId);
      // 这一轮调过的工具放回同一本账：待办面板是从这本账里折出来的，而实时
      // 帧在页面关掉那一刻就断了。不放回去，重开之后这段对话只剩一问一答，
      // 它刚才做过的那些事一件都看不见。
      //
      // 每一笔都带着会话号：实时帧按 `agent_id` 归人，而记录读回来时我们正好
      // 知道自己在读哪一段——回答者没被记下的老轮也能靠这一格放回正确的位置。
      //
      // 放在 `set` 外面：往另一本账里写是这一趟读回来的副作用，不是这一段
      // 状态的一部分，塞进 updater 会让「算新状态」这件事顺带改了别处。
      const replay = useStreamStore.getState().noteToolCall;
      const rememberedAgent =
        lines.map((line) => line.agent_id).find((id) => typeof id === 'string' && id) ?? null;
      for (const line of lines) {
        for (const call of line.tool_calls ?? []) {
          replay({
            id: call.tool_call_id,
            agent: line.agent_id ?? rememberedAgent ?? null,
            session: sessionId,
            name: call.tool_name,
            args: call.args || undefined,
            result: call.result ?? undefined,
            status: call.status,
            at: call.finished_at ?? call.started_at,
            startedAt: call.started_at,
          });
        }
      }
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
            // 记录里不存这一格：压缩是那一轮发生的事，不是这条消息的属性。
            compaction: null,
            // 读回的历史里也没有这一格：约定是那一轮开头读的，不是这条消息的属性。
            memory: null,
            // 外部服务那一格同理：它说的是那一轮开头问了什么，不是这句话的属性。
            mcp: null,
            hooks: null,
            // 读回的历史里也没有这一格：同事消息属于那一轮，不在记录的列里。
            colleagues: null,
            tasks: null,
            background: null,
            // 记录里没有附件那一格；长消息由界面按正文长度画成同一种附件。
            attachment: null,
            // 记录里有名字与类型：刷新之后仍认得出这句话说过一张图。字节数
            // 只在发出去那一刻量过，这里没有，于是留空而不是记成 0。
            files: (line.attachments ?? []).map((file, index) => ({
              name: file.name ?? `附件 ${index + 1}`,
              mimeType: file.mime_type,
              bytes: null,
            })),
            at: line.created_at,
          }));
          // 读回的历史排在前，本地还没落定的排在后面。已经落定、且历史里
          // 也有的本地那一条要去掉：同一句话不该出现两遍。
          const settled = new Set(remote.map((m) => `${m.role}\u0000${m.text}`));
          const live = session.messages.filter(
            (m) => m.status !== 'done' || !settled.has(`${m.role}\u0000${m.text}`)
          );
          // 回答者从记录里补回来：`agent.identity` 帧只在连接里出现过一次，
          // 页面重开时它不会重发。少了这一格，下面那段「谁答的」与工具调用
          // 的归属都认不出来。
          const agentId = session.agentId ?? rememberedAgent;
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
            agentId,
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
