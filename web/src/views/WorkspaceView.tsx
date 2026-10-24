import { CanvasStage } from '@/canvas/CanvasStage';
import { InputOverlay } from '@/overlay/InputOverlay';
import { useServices } from '@/shell/context';
import { useShell } from '@/shell/store';
import { useStreamStore } from '@/store/streamStore';
import { Button } from '@/ui/primitives';

/**
 * 工作台：实时画布 + 发话框。
 *
 * 画布画的是总线上到达的帧，不是任务状态的投影；下面那一行小字把这件事
 * 说出来，否则「画布空着」会被读成「平台没有在干活」。
 */
export function WorkspaceView() {
  const { clientRef, overlays } = useServices();
  const { openConversations } = overlays;
  const connectionStatus = useStreamStore((s) => s.connectionStatus);
  const messages = useStreamStore((s) => s.messages);
  const taskProgress = useStreamStore((s) => s.taskProgress);
  const openPanel = useShell((s) => s.setPanelTab);

  const streaming = messages.filter((m) => m.status === 'streaming').length;

  return (
    <div className="relative flex h-full flex-col overflow-hidden">
      <header className="flex shrink-0 items-baseline gap-3 border-b border-[var(--line)] px-4 py-2">
        <h1 className="text-[15px] font-medium tracking-tight">工作台</h1>
        <span className="text-[11px] text-[var(--muted)]">
          {connectionStatus === 'open'
            ? `实时流已连接 · 窗口内 ${messages.length} 条消息${streaming ? `（${streaming} 条正在流）` : ''}`
            : `实时流${connectionStatus}：画布与日志都不会更新，但各面的读数仍在轮询`}
        </span>
        <div className="ml-auto flex items-center gap-2">
          {taskProgress && (
            <span className="mono text-[11px] text-[var(--paper-dim)]">
              任务 {taskProgress.taskId.slice(0, 8)} · {taskProgress.step} · {taskProgress.progress}%
            </span>
          )}
          <Button onClick={() => openPanel('logs')}>总线日志</Button>
          <Button onClick={openConversations} tone="accent">
            与智能体对话
          </Button>
        </div>
      </header>

      <div className="relative min-h-0 flex-1">
        <CanvasStage />
        <InputOverlay clientRef={clientRef} />
      </div>
    </div>
  );
}
