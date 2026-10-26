import { useEffect, useRef } from 'react';
import { EventStreamClient } from '@/transport/websocket';
import { MockEventStreamClient } from '@/transport/mockStream';
import {
  describeEvent,
  useStreamStore,
  type FrameSink,
  type MessageRole,
} from '@/store/streamStore';
import { useChatStore } from '@/store/chatStore';
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
import type { AgentEvent } from '@/types/events';
import type { NotificationItem } from '@/api/client';

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
  const setConnectionStatus = useStreamStore(
    (state) => state.setConnectionStatus
  );
  const recordActivity = useStreamStore((state) => state.recordActivity);
  const noteFrame = useStreamStore((state) => state.noteFrame);
  const noteUnrecorded = useStreamStore((state) => state.noteUnrecorded);
  const ingestChatFrame = useChatStore((state) => state.ingestFrame);
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

      // 这一帧有没有改动**读快照的那几面**（任务、流程、项目、观测）。
      //
      // 与 `changedState` 分开：那个还包含画布上正在流的那条消息、一次工具调用的
      // 开始与结束——它们改动的是逐帧渲染的面，不该让集群概况跟着每两秒重读一次。
      // 这里只在「平台的状态真的变了」时置位（执行者换态、告警越线、交接推进、任务
      // 换状态、项目面那四条边），也就是 REST 快照会算得不一样的那几种。
      let changedPolledFace = false;

      // 执行者这一面：状态帧改状态、心跳只推「还在」的那一刻（两件不同的事，
      // 见 store/liveAgents.ts）。名单面每 15 秒才重读一次快照，没有这一折，
      // 一个刚起来的执行者要等下一拍才出现在「在岗名单」上。
      const liveAgent = agentFrame(event);
      if (liveAgent) {
        noteAgentFrame(liveAgent);
        // 心跳按构造不入账（下面会提前返回），它的「改了态」不落在时间轴上，
        // 所以这里只给会落点的那几类置位。
        if (event.type !== 'heartbeat' && liveAgent.state !== null) changedState = true;
        if (liveAgent.state !== null) changedPolledFace = true;
      }

      // 告警这一面：活跃告警表每 30 秒才重读一次，而 `resource_alert` 一直在总线上。
      // 折进来的是「表之外刚越的那一次」（见 store/liveAlerts.ts），它改的是读者
      // 「现在要不要动手」的判断，所以这一帧算改了态。
      const liveAlert = alertFrame(event);
      if (liveAlert) {
        noteAlertFrame(liveAlert);
        changedState = true;
        changedPolledFace = true;
      }

      // 通信这一面：交接图每 15 秒才重读一次，而让图长出边的那些结局一直在总线上。
      // 折进来的是「图上马上要多出边」的那几个任务（见 store/liveComms.ts），读者
      // 据此知道这条链刚往前走了，不必等下一拍。
      const liveComms = commsFrame(event);
      if (liveComms) {
        noteCommsFrame(liveComms);
        changedState = true;
        changedPolledFace = true;
      }

      // 项目这一面：范围、看板、甘特、挣值四张屏都是每 15 秒重读一次的快照，而
      // `task_status_change` 一直在总线上。折进来的是「这一面该重读了」的那几条
      // （见 store/liveProjects.ts），帧只是触发器，四张屏的数仍然由服务端重算——
      // 拿帧去改服务端的数，就是两份账写成一本。
      const liveProjectTask = taskStatusFrame(event);
      if (liveProjectTask) {
        noteProjectTaskFrame(liveProjectTask);
        changedState = true;
        changedPolledFace = true;
      }

      switch (event.type) {
        case 'message_start': {
          if (!agentId) break;
          const id = `${agentId}:${event.timestamp}`;
          const role = ((p.message as { role?: string } | undefined)?.role ??
            'assistant') as MessageRole;
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
        case 'task_status_change': {
          const taskId = typeof p.task_id === 'string' ? p.task_id : null;
          const status = typeof p.status === 'string' ? p.status : '';
          // `hook_event` 借这个变体过总线，它不是任务在换状态；把它当状态
          // 写进进度条，画布上就会显示一个从没发生过的迁移。
          if (!taskId || status === 'hook_event') break;
          noteTaskStatus({ taskId, status, at: event.timestamp });
          changedState = true;
          changedPolledFace = true;
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

      // 读快照的那几面在这里被告知「有东西变了」，它们据此立刻重读一次。
      //
      // 位置在这三种提前返回之后：心跳与正文增量按构造高频（每个 token 一帧），
      // 让它们触发重读，等于把轮询换成一种更密的轮询，而它们改的是画布上正在流
      // 的那一条消息，那一面本来就是逐帧渲染的。
      if (changedPolledFace) noteFaceTick();
    };

    const channels = channelKey ? channelKey.split(',') : [];
    const client = options.mock
      ? new MockEventStreamClient({
          onEvent: handleEvent,
          onChatFrame: ingestChatFrame,
          onStatusChange: (status) => setConnectionStatus(status),
        })
      : new EventStreamClient({
          url: options.url ?? '',
          channels,
          onEvent: handleEvent,
          onChatFrame: ingestChatFrame,
          onNotification: (notification) =>
            notificationHandler.current?.(notification),
          onStatusChange: (status) => setConnectionStatus(status),
          onError: (error) => {
            console.error('WebSocket error:', error);
          },
        });

    clientRef.current = client;
    client.connect();

    return () => {
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
    noteAgentFrame,
    noteAlertFrame,
    noteCommsFrame,
    noteProjectTaskFrame,
    noteArrival,
    noteFaceTick,
  ]);

  return { clientRef };
}
