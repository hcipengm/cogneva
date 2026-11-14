import { useEffect, useRef } from 'react';
import {
  EventStreamClient,
  type ChatCheckpointWire,
  type ChatModelWire,
  type ChatSandboxBoundaryWire,
  type ResumedMessageWire,
} from '@/transport/websocket';
import { MockEventStreamClient } from '@/transport/mockStream';
import {
  describeEvent,
  useStreamStore,
  type FrameSink,
} from '@/store/streamStore';
import { chatAttachment, type ChatRole } from '@/types/events';
import { FIRST_SESSION, useChatStore } from '@/store/chatStore';
import { agentFrame } from '@/store/liveAgents';
import { alertFrame } from '@/store/liveAlerts';
import { commsFrame } from '@/store/liveComms';
import { taskStatusFrame } from '@/store/liveProjects';
import { useLiveAgentStore } from '@/store/liveAgentStore';
import { useLiveAlertStore } from '@/store/liveAlertStore';
import { useLiveCommsStore } from '@/store/liveCommsStore';
import { useLiveProjectStore } from '@/store/liveProjectStore';
import { useLivePulseStore } from '@/store/livePulseStore';
import { useFrameTickStore } from '@/store/frameTickStore';
import { resourceKey } from '@/store/frameTick';
import type { AgentEvent } from '@/types/events';
import type { NotificationItem } from '@/api/client';
import { usePermissionStore } from '@/store/permissionStore';
import { useMemoryStore } from '@/store/memoryStore';

/**
 * 一条消息里的纯文本。
 *
 * 服务端的 `Message::content` 是内容块数组（文本、思考、工具调用、图片混在
 * 一起），画布只画说得出口的那部分——把工具调用的 JSON 当正文铺开，读的人
 * 会以为那是 agent 说的话。
 */
function messageText(message: unknown): string {
  if (typeof message === 'string') return message;
  const content = (message as { content?: unknown })?.content;
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .map((block) => {
      const b = block as { type?: string; text?: string };
      return b.type === 'text' ? b.text ?? '' : '';
    })
    .filter(Boolean)
    .join('\n');
}

export type StreamClient = EventStreamClient | MockEventStreamClient;

export interface UseEventStreamOptions {
  url?: string;
  channels?: string[];
  mock?: boolean;
  /** 服务端推来的通知；调用方每次渲染换一个新函数也不会重连（走 ref） */
  onNotification?: (notification: NotificationItem) => void;
}

/**
 * 一条增量事件里真正要追加的那段文字。
 *
 * 只有 `text_delta` 是正文。`thinking_delta` 是模型的内部草稿，把它铺进画布
 * 会让读的人分不清哪些是会落进回答的；工具调用的参数是 JSON 片段，拼起来才
 * 有意义，单看每一片是噪声。
 */
function extractDelta(assistantEvent: unknown): string | null {
  const e = assistantEvent as { type?: string; delta?: string } | null;
  if (!e) return null;
  return e.type === 'text_delta' && typeof e.delta === 'string' ? e.delta : null;
}

export function useEventStream(options: UseEventStreamOptions = {}): {
  clientRef: React.RefObject<StreamClient | null>;
} {
  const clientRef = useRef<StreamClient | null>(null);
  const notificationHandler = useRef(options.onNotification);
  notificationHandler.current = options.onNotification;
  // 频道列表按内容比，不按数组身份比。调用方写的是字面量 `['agent:*', …]`，
  // 每次渲染都是一个新数组；把它放进依赖里，任何一次重渲染（告警轮询每十
  // 几秒一次）都会断开重连，实时视图于是每隔一会儿就空一段。
  const channelKey = (options.channels ?? []).join(',');
  const startMessage = useStreamStore((state) => state.startMessage);
  const appendDelta = useStreamStore((state) => state.appendDelta);
  const endMessage = useStreamStore((state) => state.endMessage);
  const finishMessage = useStreamStore((state) => state.finishMessage);
  const noteTaskStatus = useStreamStore((state) => state.noteTaskStatus);
  const noteToolCall = useStreamStore((state) => state.noteToolCall);
  const setConnectionStatus = useStreamStore(
    (state) => state.setConnectionStatus
  );
  const recordActivity = useStreamStore((state) => state.recordActivity);
  const noteFrame = useStreamStore((state) => state.noteFrame);
  const noteUnrecorded = useStreamStore((state) => state.noteUnrecorded);
  const ingestChatFrame = useChatStore((state) => state.ingestFrame);
  const applyChatResume = useChatStore((state) => state.applyResume);
  const applyModel = useChatStore((state) => state.applyModel);
  const applyCheckpoints = useChatStore((state) => state.applyCheckpoints);
  const applyBoundary = useChatStore((state) => state.applyBoundary);
  const noteAgentFrame = useLiveAgentStore((state) => state.noteFrame);
  const noteAlertFrame = useLiveAlertStore((state) => state.noteFrame);
  const noteCommsFrame = useLiveCommsStore((state) => state.noteFrame);
  const noteProjectTaskFrame = useLiveProjectStore((state) => state.noteFrame);
  const noteArrival = useLivePulseStore((state) => state.noteArrival);
  const noteFaceTick = useFrameTickStore((state) => state.note);
  /** 每个 agent 正在流的那一条消息（agent_id → 画布上的消息 id）。 */
  const streaming = useRef(new Map<string, string>());

  useEffect(() => {
    // 未登录时 url 为空，不要建连——new WebSocket('') 会解析到页面根路径
    if (!options.mock && !options.url) {
      return;
    }

    const handleEvent = (event: AgentEvent) => {
      const p = event.payload as Record<string, unknown>;
      const agentId = typeof p.agent_id === 'string' ? p.agent_id : null;

      // 「这一路还活着吗」先记一笔，再谈这一帧说了什么。
      //
      // 记的是**到达**（`Date.now()`），不是帧里的 `timestamp`：重连重放一串旧帧
      // 时，按帧里那个时间算，状态条会把「刚刚在动」读成「半小时没动」。位置也必须
      // 在下面那些提前返回之前——心跳与正文增量按构造不入活动流，可它们正是「平台
      // 在干活」最密的那两种帧，漏掉它们，忙碌与停摆就都是「一帧没有」。
      noteArrival(Date.now());

      // 这一帧有没有真的改动折叠出来的态。由每个分支在确实写完之后自己置位：
      // 时间轴上那条「态」徽记是读者判断「这帧改了东西吗」的唯一依据，估着置位
      // 就变成了替这一帧编一个它没做过的改动。
      let changedState = false;

      // 这一帧改动了**哪几个读快照的面**。
      //
      // 与 `changedState` 分开：那个还包含画布上正在流的那条消息、一次工具调用的
      // 开始与结束——它们改动的是逐帧渲染的面，不该让集群概况跟着每两秒重读一次。
      // 这里只在「平台的状态真的变了」时记名（执行者换态、告警越线、交接推进、任务
      // 换状态、项目面那四条边），也就是 REST 快照会算得不一样的那几种。
      //
      // 记的是**面**不是布尔：一个布尔只能告诉所有人「有东西变了」，于是配额、花销、
      // 设置这些与任务无关的面也被 `task_status_change` 拖着每两秒重读一次——那不是
      // 事件驱动，是换了个说法的轮询，还把服务端一起拖进去。每一种面只订自己的名字。
      const touched: string[] = [];

      // REST 侧一次成功的写：改的是哪个前缀下的东西，就让订了那个前缀的面重读。
      //
      // 编排器那些变体只覆盖得到智能体与任务；设置、文件、存储、变更流水这些面在
      // 总线上原本一帧都没有，屏幕要动就只能靠轮询。这一族帧说的是「`domain` 下的
      // 读数过期了」——它不带内容，也不该带：内容仍然由服务端现算。
      if (event.type === 'resource_changed') {
        const domain = typeof p.domain === 'string' ? p.domain : '';
        if (domain) touched.push(resourceKey(domain));
      }

      // 执行者这一面：状态帧改状态、心跳只推「还在」的那一刻（两件不同的事，
      // 见 store/liveAgents.ts）。名单面每 15 秒才重读一次快照，没有这一折，
      // 一个刚起来的执行者要等下一拍才出现在「在岗名单」上。
      const liveAgent = agentFrame(event);
      if (liveAgent) {
        noteAgentFrame(liveAgent);
        // 心跳按构造不入账（下面会提前返回），它的「改了态」不落在时间轴上，
        // 所以这里只给会落点的那几类置位。
        if (event.type !== 'heartbeat' && liveAgent.state !== null) changedState = true;
        if (liveAgent.state !== null) touched.push('agents');
      }

      // 花销这一面：一轮话、一次运行结束，就是又花掉了一笔。额度与令牌曲线那几面
      // 与任务状态无关，订的是这两帧——拿 `task_status_change` 去拖它们，拖出来的
      // 是一份每两秒重读一次的轮询。
      if (event.type === 'turn_end' || event.type === 'agent_end') touched.push('usage');

      // 告警这一面：活跃告警表每 30 秒才重读一次，而 `resource_alert` 一直在总线上。
      // 折进来的是「表之外刚越的那一次」（见 store/liveAlerts.ts），它改的是读者
      // 「现在要不要动手」的判断，所以这一帧算改了态。
      const liveAlert = alertFrame(event);
      if (liveAlert) {
        noteAlertFrame(liveAlert);
        changedState = true;
        touched.push('alerts');
      }

      // 通信这一面：交接图每 15 秒才重读一次，而让图长出边的那些结局一直在总线上。
      // 折进来的是「图上马上要多出边」的那几个任务（见 store/liveComms.ts），读者
      // 据此知道这条链刚往前走了，不必等下一拍。
      const liveComms = commsFrame(event);
      if (liveComms) {
        noteCommsFrame(liveComms);
        changedState = true;
        touched.push('comms');
      }

      // 项目这一面：范围、看板、甘特、挣值四张屏都是每 15 秒重读一次的快照，而
      // `task_status_change` 一直在总线上。折进来的是「这一面该重读了」的那几条
      // （见 store/liveProjects.ts），帧只是触发器，四张屏的数仍然由服务端重算——
      // 拿帧去改服务端的数，就是两份账写成一本。
      const liveProjectTask = taskStatusFrame(event);
      if (liveProjectTask) {
        noteProjectTaskFrame(liveProjectTask);
        changedState = true;
        touched.push('projects');
      }

      switch (event.type) {
        case 'message_start': {
          if (!agentId) break;
          const id = `${agentId}:${event.timestamp}`;
          const role = ((p.message as { role?: string } | undefined)?.role ??
            'assistant') as ChatRole;
          streaming.current.set(agentId, id);
          startMessage(id, agentId, role);
          changedState = true;
          break;
        }
        case 'message_update': {
          if (!agentId) break;
          const delta = extractDelta(p.assistant_event);
          const id = streaming.current.get(agentId);
          if (id && delta) {
            appendDelta(id, delta);
            changedState = true;
          }
          break;
        }
        case 'message_end': {
          if (!agentId) break;
          const id = streaming.current.get(agentId);
          // 用整条消息补一次正文：用户发的与工具回的不走增量，只收尾的话
          // 画布上那一条是空的。
          if (id) {
            finishMessage(id, messageText(p.message));
            changedState = true;
          }
          streaming.current.delete(agentId);
          break;
        }
        case 'agent_end': {
          // 运行结束时把还没收尾的流关掉：少了这一句，画布上最后一条会永远
          // 转着「生成中」，而平台其实早就停了。
          if (agentId) {
            const id = streaming.current.get(agentId);
            if (id) {
              endMessage(id);
              changedState = true;
            }
            streaming.current.delete(agentId);
          }
          break;
        }
        case 'tool_execution_start': {
          const id = typeof p.tool_call_id === 'string' ? p.tool_call_id : null;
          if (!id) break;
          noteToolCall({
            id,
            agent: agentId,
            name: typeof p.tool_name === 'string' ? p.tool_name : '',
            args: p.args,
            status: 'running',
            at: event.timestamp,
          });
          changedState = true;
          break;
        }
        case 'tool_execution_update': {
          const id = typeof p.tool_call_id === 'string' ? p.tool_call_id : null;
          if (!id) break;
          // 线上这一族把中途输出放在 `partial_result` 里（`ToolExecutionUpdate`），
          // 也只有这一处；早先还按 `chunk`/`delta`/`output` 找过，三个名字没有一个
          // 存在过——一次长工具在面板上于是只剩「输出中」三个字，没有输出。
          //
          // 名字与参数只在 `tool_execution_start` 上，这一帧和结束帧都不带：这里不传
          // `name`，同一次调用（同 id）在开始那一帧已经记下了。传一个读不到的字段，
          // 记下来的永远是空串，而那一格看上去像「这个工具没有名字」。
          const chunk = p.partial_result;
          noteToolCall({
            id,
            agent: agentId,
            progress: typeof chunk === 'string' ? chunk : JSON.stringify(chunk ?? '') ?? '',
            status: 'running',
            at: event.timestamp,
          });
          break;
        }
        case 'tool_execution_end': {
          const id = typeof p.tool_call_id === 'string' ? p.tool_call_id : null;
          if (!id) break;
          noteToolCall({
            id,
            agent: agentId,
            result: p.result,
            status: p.is_error ? 'error' : 'ok',
            at: event.timestamp,
          });
          changedState = true;
          break;
        }
        case 'task_status_change': {
          const taskId = typeof p.task_id === 'string' ? p.task_id : null;
          const status = typeof p.status === 'string' ? p.status : '';
          // `hook_event` 借这个变体过总线，它不是任务在换状态；把它当状态
          // 写进进度条，画布上就会显示一个从没发生过的迁移。
          if (!taskId || status === 'hook_event') break;
          noteTaskStatus({ taskId, status, at: event.timestamp });
          changedState = true;
          touched.push('tasks');
          break;
        }
        default:
          break;
      }

      // 两个按构造高频的变体：`message_update` 每个 token 一帧、`heartbeat` 每个
      // agent 每条心跳一帧。逐条入账，一次回话就能把帧流与时间轴一起冲干净，真正
      // 要紧的工具调用被挤出去。它们不是消失了：按类型计数，面板上写着各自多少条。
      if (event.type === 'message_update' || event.type === 'heartbeat') {
        noteUnrecorded(event.type);
        return;
      }

      // 逐条的事先进帧流，再在时间轴上落一个点，点上记着它到底改过哪几处。
      // 这一行同时是「不认识的变体也留一行」的地方：认不出不等于没发生。
      const entry = describeEvent(event);
      const fed: FrameSink[] = ['stream', 'timeline'];
      if (changedState) fed.push('state');
      recordActivity(entry);
      noteFrame({ ...entry, fed });

      // 读快照的那几面在这里被告知「你订的那件事变了」，它们据此立刻重读一次。
      //
      // 位置在这三种提前返回之后：心跳与正文增量按构造高频（每个 token 一帧），
      // 让它们触发重读，等于把轮询换成一种更密的轮询，而它们改的是画布上正在流
      // 的那一条消息，那一面本来就是逐帧渲染的。
      for (const kind of touched) noteFaceTick(kind);
    };

    /**
     * 每一次连上（含重连）第一件事：把「我正在看的那一段」重新接上。
     *
     * 不接的话，断线时那一轮还在说，而这一页再也收不到它的下文——人看到的
     * 是「网一抖，回答停在那儿了」。接上之后服务端先回一份快照，那一句接着写。
     */
    const resumeOpenSession = () => {
      const active = useChatStore.getState().active;
      if (active) clientRef.current?.send({ type: 'chat_resume', session_id: active });
    };

    const ingestChatResume = (sessionId: string | undefined, messages: ResumedMessageWire[]) => {
      const session = sessionId || FIRST_SESSION;
      applyChatResume(
        session,
        messages.flatMap((line) =>
          line.message_id && typeof line.text === 'string'
            ? [
                {
                  messageId: line.message_id,
                  role: (line.role === 'user' ? 'user' : 'assistant') as ChatRole,
                  text: line.text,
                  thinking: typeof line.thinking === 'string' ? line.thinking : '',
                  attachment: chatAttachment(line.attachment),
                  ended: line.ended === true,
                },
              ]
            : []
        )
      );
    };

    /**
     * 这一段用哪只模型、还能选哪些。一次往返办两件事，所以两件事一起落：换完之后
     * 手上立刻要有「现在用的是哪只」与「还能选哪些」。
     */
    const ingestChatModel = (wire: ChatModelWire) => {
      const session = wire.session_id || FIRST_SESSION;
      applyModel(session, wire.model ?? null, {
        models: wire.models ?? [],
        configured: wire.configured === true,
        reason: wire.reason ?? null,
      });
    };

    /**
     * 这一段对话的检查点：清单，或者一次退回的结局。按会话落，与模型选择同一个
     * 理由——它是这一段的状态，不是这一轮说出来的话。
     */
    const ingestChatCheckpoint = (wire: ChatCheckpointWire) => {
      const session = wire.session_id || FIRST_SESSION;
      applyCheckpoints(session, {
        rows: (wire.rows ?? []).flatMap((row) =>
          typeof row.turn === 'number'
            ? [{ key: row.key ?? '', turn: row.turn, at: row.at ?? '' }]
            : []
        ),
        hidden: wire.hidden ?? 0,
        rewind: wire.rewind
          ? {
              turn: wire.rewind.turn ?? 0,
              target: wire.rewind.target ?? 0,
              applied: wire.rewind.applied === true,
              files: (wire.rewind.files ?? []).flatMap((file) =>
                file.path ? [{ path: file.path, change: file.change ?? 'modified' }] : []
              ),
              truncated: wire.rewind.truncated === true,
            }
          : null,
        reason: wire.reason ?? null,
      });
    };

    /**
     * 命令跑在什么样的环境里。按会话落，与检查点同一个理由——它是部署的读数，
     * 不是这一轮说出来的话。认不出形状的那一份按「读不到」落：把一行边界读丢了
     * 比整份不画更糟，因为读丢的恰好可能是那个「不设边界」的词。
     */
    const ingestChatSandboxBoundary = (wire: ChatSandboxBoundaryWire) => {
      const session = wire.session_id || FIRST_SESSION;
      const boundary = wire.boundary;
      applyBoundary(session, {
        boundary: boundary
          ? {
              kind: boundary.kind ?? '',
              isolated: boundary.isolated === true,
              workdir: boundary.workdir ?? null,
              lines: (boundary.lines ?? []).flatMap((line) =>
                line.dimension && line.state
                  ? [{ dimension: line.dimension, state: line.state, detail: line.detail ?? '' }]
                  : []
              ),
            }
          : null,
        reason: wire.reason ?? null,
      });
    };

    const channels = channelKey ? channelKey.split(',') : [];
    const client = options.mock
      ? new MockEventStreamClient({
          onEvent: handleEvent,
          onChatFrame: ingestChatFrame,
          onChatSandboxBoundary: ingestChatSandboxBoundary,
          onStatusChange: (status) => {
            setConnectionStatus(status);
            // 演示流也会断：接上时同样要把这一段续上。
            if (status === 'open') resumeOpenSession();
          },
        })
      : new EventStreamClient({
          url: options.url ?? '',
          channels,
          onEvent: handleEvent,
          onChatFrame: ingestChatFrame,
          onNotification: (notification) =>
            notificationHandler.current?.(notification),
          // 问句直接进那一页的账：它不是一轮里的某一帧，而是一个正等在那里的
          // 调用，丢进事件流只会被当成不认识的事件计数一次。
          onPermissionRequest: (wire) => {
            const id = wire.request_id;
            if (!id) return;
            usePermissionStore.getState().ask({
              id,
              sessionId: wire.session_id ?? 'default',
              tool: wire.tool ?? '',
              summary: wire.summary ?? '',
              mode: wire.mode ?? 'default',
              plan: wire.plan,
              diff: wire.diff ?? null,
            });
          },
          // 规则名单是设置，不进聊天流。少一格（没有号或没有工具名）的那一条不落：
          // 没有号就撤不掉，画出来只是一个撤不掉的按钮。
          onPermissionRules: (rules) => {
            const known = rules.flatMap((rule) =>
              rule.id && rule.tool
                ? [
                    {
                      id: rule.id,
                      tool: rule.tool,
                      subject: rule.subject,
                      allow: rule.allow === true,
                      scope: rule.scope ?? 'session',
                    },
                  ]
                : []
            );
            usePermissionStore.getState().setRules(known);
          },
          // 约定名单同理：少一格（没有号或没有原文）的那一条不落——没有号就撤
          // 不掉，没有原文就画不出一句人写的话。
          onChatResume: (resume) => ingestChatResume(resume.session_id, resume.messages ?? []),
          onChatModel: ingestChatModel,
          onChatCheckpoint: ingestChatCheckpoint,
          onChatSandboxBoundary: ingestChatSandboxBoundary,
          onOpen: resumeOpenSession,
          onOwnerMemory: (entries) => {
            const known = entries.flatMap((entry) =>
              entry.id && entry.text ? [{ id: entry.id, text: entry.text }] : []
            );
            useMemoryStore.getState().setEntries(known);
          },
          onStatusChange: (status) => {
            setConnectionStatus(status);
            // 断了就把名单清回「还没读到」：上一条连接读到的规则不该留在屏幕上
            // 冒充现状，而重连之后那一次请求会把真的名单拿回来。
            if (status !== 'open') {
              usePermissionStore.getState().forgetRules();
              useMemoryStore.getState().clearEntries();
              // 检查点同理：上一条连接读到的那几份不该留在屏幕上冒充现状。
              useChatStore.getState().forgetCheckpoints();
              // 边界同理，而且更甚：它是「命令跑在哪里」的读数，断开之后没人能
              // 保证执行器还是那一副。
              useChatStore.getState().forgetBoundaries();
            }
          },
          onError: (error) => {
            console.error('WebSocket error:', error);
          },
        });

    clientRef.current = client;
    client.connect();

    // 换到另一段对话时，把这一段的现场接上。连上的那一刻接的是当时正在看的那一段；
    // 之后人再切走、切回来，那一段可能还在说——不接的话，切回来看到的是切换前的
    // 旧字，而回答已经往后写了。切走时不必退订：广播一直留着这一段的帧，再一次
    // `chat_resume` 就是一份新快照，重复接同一条不会多出字。
    let watching = useChatStore.getState().active;
    const stopWatching = useChatStore.subscribe((state) => {
      if (state.active === watching) return;
      watching = state.active;
      resumeOpenSession();
    });

    return () => {
      stopWatching();
      client.disconnect();
      clientRef.current = null;
    };
  }, [
    options.mock,
    options.url,
    channelKey,
    startMessage,
    appendDelta,
    endMessage,
    finishMessage,
    noteTaskStatus,
    setConnectionStatus,
    recordActivity,
    noteFrame,
    noteUnrecorded,
    ingestChatFrame,
    applyChatResume,
    applyModel,
    noteAgentFrame,
    noteAlertFrame,
    noteCommsFrame,
    noteProjectTaskFrame,
    noteArrival,
    noteFaceTick,
  ]);

  return { clientRef };
}
