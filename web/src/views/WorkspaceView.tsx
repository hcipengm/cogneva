import { useCallback, useEffect, useMemo } from 'react';
import { getProcessOverview } from '@/api/client';
import { CanvasStage } from '@/canvas/CanvasStage';
import { ConversationPane } from '@/conversation/ConversationPane';
import { goalLiveIndex } from '@/conversation/dispatch';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { useServices } from '@/shell/context';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { useChatStore } from '@/store/chatStore';
import { useStreamStore } from '@/store/streamStore';
import { Button } from '@/ui/primitives';
import { USE_MOCK } from '@/runtime';

/** 同时并排的对话上限。再多每一段都窄到读不成句子，而切走再切回来并不丢内容。 */
const MAX_PANES = 3;

/**
 * 工作台：几段对话并排，底是一层安静的主题色。
 *
 * 以前这一面是一张画布：画着栅格、气泡对话和两个写死的可视化面板。三样东西
 * 看起来都在讲集群里发生的事，实际上没有一样来自集群。现在底只负责「安静」，
 * 内容交给真正的读数：每一段对话是一列，各自吃自己的聊天帧。
 *
 * 多段并存不是「多开几个页签」的另一种说法：一段对话与一个智能体的上下文是一
 * 对一的，两条任务线并排跑的时候，人要看的是两边同时在说什么，而不是来回切。
 *
 * 画面：几段并排的对话本身就是这一面的画面——每一列都按自己那条流在长。这一面
 * 没有可画的量：它显示的是话，不是话的统计。
 */
export function WorkspaceView() {
  const { clientRef } = useServices();
  const connectionStatus = useStreamStore((s) => s.connectionStatus);
  const activity = useStreamStore((s) => s.activity);
  const setPanelTab = useShell((s) => s.setPanelTab);

  // 每段对话交出去的目标现在做到哪儿了。对话面自己画不出「交给谁了」——那部分
  // 在任务图上，所以这一段总览由这一层读一次、折成一张表给所有窗格共用。
  const processes = useResource('流程总览', getProcessOverview, 10_000, !USE_MOCK);
  // 任务换态、执行者换态、流程面自己报了一次改写，都会让这道目标的进度变。总览
  // 每 10 秒才重读一次；不订这几帧，「跑 1」要等下一拍才变成「跑 0」。
  useKindTickedRefresh(['tasks', 'agents', resourceKey('processes')], processes.refresh);
  const goalLive = useMemo(() => goalLiveIndex(processes.data), [processes.data]);

  const order = useChatStore((s) => s.order);
  const active = useChatStore((s) => s.active);
  const setActive = useChatStore((s) => s.setActive);
  const createSession = useChatStore((s) => s.createSession);
  const closeSession = useChatStore((s) => s.closeSession);

  // 工作台打开的那一刻至少有一段对话。空着的对话面没有落点，读者第一眼看到的
  // 会是一个只有背景的面，而「这里能说话」这件事就藏起来了。
  useEffect(() => {
    if (order.length === 0) createSession();
  }, [order.length, createSession]);

  const openAnother = useCallback(() => {
    if (order.length >= MAX_PANES) {
      setActive(order[0]);
      return;
    }
    createSession();
  }, [createSession, order, setActive]);

  return (
    <div className="relative flex h-full flex-col overflow-hidden">
      <header className="flex shrink-0 items-baseline gap-3 px-4 py-2">
        <h1 className="text-[15px] font-medium tracking-tight">工作台</h1>
        <span className="text-[11px] text-[var(--muted)]">
          {connectionStatus === 'open'
            ? `实时流已连接 · 本次会话已记 ${activity.length} 条总线事件`
            : `实时流${connectionStatus}：对话的回复不会到达，下面的读数仍在轮询`}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <Button onClick={() => setPanelTab('logs')}>总线日志</Button>
          <Button onClick={openAnother} tone="accent" disabled={order.length >= MAX_PANES}>
            + 新对话
          </Button>
        </div>
      </header>

      <div className="relative min-h-0 flex-1">
        <CanvasStage />
        <div className="relative flex h-full min-h-0">
          {order.map((id) => (
            <ConversationPane
              key={id}
              sessionId={id}
              active={id === active}
              onFocus={() => setActive(id)}
              onClose={() => closeSession(id)}
              closable={order.length > 1}
              historyEnabled={!USE_MOCK}
              clientRef={clientRef}
              goalLive={goalLive}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
