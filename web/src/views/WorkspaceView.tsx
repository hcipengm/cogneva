import { useCallback, useEffect } from 'react';
import { CanvasStage } from '@/canvas/CanvasStage';
import { ConversationPane } from '@/conversation/ConversationPane';
import { useServices } from '@/shell/context';
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
 */
export function WorkspaceView() {
  const { clientRef } = useServices();
  const connectionStatus = useStreamStore((s) => s.connectionStatus);
  const activity = useStreamStore((s) => s.activity);
  const setPanelTab = useShell((s) => s.setPanelTab);

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
            />
          ))}
        </div>
      </div>
    </div>
  );
}
