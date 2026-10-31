/*
 * 工作台：几段对话并排，底是一层安静的主题色。
 *
 * 画面：这一面本身就是画面——每一格是一段正在进行的对话，读者要看的形状就是
 * 「谁在说什么、说到哪儿了」；把它再折成一张图，只是把对话压成计数。
 */
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { getProcessOverview } from '@/api/client';
import { CanvasStage } from '@/canvas/CanvasStage';
import { conversationNames } from '@/conversation/callsign';
import { ConversationPane } from '@/conversation/ConversationPane';
import { goalLiveIndex } from '@/conversation/dispatch';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { useServices } from '@/shell/context';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { useChatStore } from '@/store/chatStore';
import { useStreamStore } from '@/store/streamStore';
import { Icon } from '@/shell/icons';
import { Button } from '@/ui/primitives';
import { USE_MOCK } from '@/runtime';

import { Note } from '@/ui/Note';
/**
 * 一段对话最小要多少像素才读得成句子，以及一格最少要多高才看得见一轮问答。
 * 这两个数是「排几列几行」的尺子，不是硬下限：窗口小到放不下时，格子会一起
 * 缩，而不是把某一段挤出屏幕。
 */
const MIN_PANE_WIDTH = 340;
const MIN_PANE_HEIGHT = 220;

/** 量一个容器现在有多宽多高。窗口改尺寸、侧栏被拖、底部面板开合都要重新量。 */
function useBoxSize<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  // 第一帧按窗口大小估一个，量完再换成真实值：初值给 0 的话，第一帧会按零宽
  // 排成一行十列或一列十行，等到量完再跳一次——那一下是看得见的。
  const [size, setSize] = useState(() => ({
    width: window.innerWidth,
    height: window.innerHeight,
  }));
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const measure = () => {
      const rect = el.getBoundingClientRect();
      setSize((prev) =>
        Math.abs(prev.width - rect.width) < 1 && Math.abs(prev.height - rect.height) < 1
          ? prev
          : { width: rect.width, height: rect.height }
      );
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, []);
  return { ref, size };
}

/**
 * 几段对话该排成几列几行。
 *
 * 列先按宽度定上限，再把段数均分到各行：十段对话在宽屏上是 4×3，不是竖着的
 * 一条线（1×10），也不是把一个拆到第二行只剩一格。高度不设固定值——行数变了，
 * 每一格的高度跟着变，这是「自适应宽和高」里「高也自适应」那一半。
 */
export function gridShape(count: number, width: number, height: number) {
  if (count <= 1) return { cols: 1, rows: 1 };
  const byWidth = Math.max(1, Math.floor(width / MIN_PANE_WIDTH));
  // 高度也给一个上界，否则窄而高的窗口会排成一条竖线。
  const byHeight = Math.max(1, Math.floor(height / MIN_PANE_HEIGHT));
  const maxCols = Math.max(1, Math.min(byWidth, count));
  let cols = Math.min(count, Math.max(maxCols, 1));
  let rows = Math.ceil(count / cols);
  // 行数超过高度放得下的行数时，宁可多一列：多一列只是窄一点，多一行是把
  // 每一格压到看不清。
  while (rows > byHeight && cols < count) {
    cols += 1;
    rows = Math.ceil(count / cols);
  }
  return { cols, rows };
}

/**
 * 工作台：几段对话并排，底是一层安静的主题色。
 *
 * 画面：这一面本身就是画面——每一格是一段正在进行的对话，读出它的形状不需要
 * 再画一张图；把它折成图表反而是把「谁在说什么」压成计数。
 *
 * 以前这一面是一张画布：画着栅格、气泡对话和两个写死的可视化面板。三样东西
 * 看起来都在讲集群里发生的事，实际上没有一样来自集群。现在底只负责「安静」，
 * 内容交给真正的读数：每一段对话是一列，各自吃自己的聊天帧。
 *
 * 多段并存不是「多开几个页签」的另一种说法：一段对话与一个智能体的上下文是一
 * 对一的，两条任务线并排跑的时候，人要看的是两边同时在说什么，而不是来回切。
 * 但段数多了以后并排会窄到读不成句子，所以两种排法都留着，读者自己选：
 * 「分栏」是编辑器式的并排，「标签页」是一次全屏看一段。
 */
export function WorkspaceView() {
  const { clientRef } = useServices();
  const connectionStatus = useStreamStore((s) => s.connectionStatus);
  const activity = useStreamStore((s) => s.activity);
  const setPanelTab = useShell((s) => s.setPanelTab);
  const mode = useShell((s) => s.workspaceMode);
  const setMode = useShell((s) => s.setWorkspaceMode);

  // 每段对话交出去的目标现在做到哪儿了。对话面自己画不出「交给谁了」——那部分
  // 在任务图上，所以这一段总览由这一层读一次、折成一张表给所有窗格共用。
  const processes = useResource('流程总览', getProcessOverview, 10_000, !USE_MOCK);
  // 任务换态、执行者换态、流程面自己报了一次改写，都会让这道目标的进度变。总览
  // 每 10 秒才重读一次；不订这几帧，「跑 1」要等下一拍才变成「跑 0」。
  useKindTickedRefresh(['tasks', 'agents', resourceKey('processes')], processes.refresh);
  const goalLive = useMemo(() => goalLiveIndex(processes.data), [processes.data]);

  const order = useChatStore((s) => s.order);
  const sessions = useChatStore((s) => s.sessions);
  const active = useChatStore((s) => s.active);
  const setActive = useChatStore((s) => s.setActive);
  const createSession = useChatStore((s) => s.createSession);
  const closeSession = useChatStore((s) => s.closeSession);

  // 工作台打开的那一刻至少有一段对话。空着的对话面没有落点，读者第一眼看到的
  // 会是一个只有背景的面，而「这里能说话」这件事就藏起来了。
  useEffect(() => {
    if (order.length === 0) createSession();
  }, [order.length, createSession]);

  // 关掉当前这一段时落点交给还在的第一个，这一步由 store 做：把 `active` 留在
  // 一个不存在的 id 上，标签页模式会整面空白，看起来像「刚才那段对话丢了」。

  const { ref: boxRef, size } = useBoxSize<HTMLDivElement>();
  const shape = useMemo(
    () => gridShape(order.length, size.width, size.height),
    [order.length, size.height, size.width]
  );

  // 名字由这一层统一分配：同时开着哪几段决定谁叫什么，单独一格算不出来。
  const names = useMemo(() => conversationNames(order), [order]);
  const nameOf = (id: string) => names.get(id) ?? id;

  const tabMode = mode === 'tabs';
  // 标签页模式只画当前这一段；没有当前（刚关掉最后一段又还没建新的）时退到第一段。
  const currentId = active && order.includes(active) ? active : order[0];
  const shown: string[] = tabMode && currentId ? [currentId] : order;

  return (
    <div className="relative flex h-full flex-col overflow-hidden">
      <header className="flex shrink-0 items-baseline gap-3 px-4 py-2">
        <h1 className="text-[15px] font-medium tracking-tight">工作台</h1>
        <span className="text-[11px] text-[var(--muted)]">
          {connectionStatus === 'open'
            ? `实时流已连接 · 本次会话已记 ${activity.length} 条总线事件 · ${order.length} 段对话`
            : `实时流${connectionStatus}：对话的回复不会到达，下面的读数仍在轮询`}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <div className="flex items-center" role="group" aria-label="工作台排法">
            {(
              [
                ['columns', '分栏', 'columns'],
                ['tabs', '标签页', 'tabs'],
              ] as const
            ).map(([id, label, icon]) => (
              <Note
                as="button"
                key={id}
                type="button"
                onClick={() => setMode(id)}
                aria-pressed={mode === id}
                text={
                  id === 'columns'
                    ? '分栏：几段对话并排，宽高按段数与窗口大小自适应'
                    : '标签页：一次全屏看一段，上面一排标签切换'
                }
                className="flex h-6 items-center gap-1.5 border px-2 text-[11px] transition-colors"
                style={{
                  borderColor: mode === id ? 'var(--brass)' : 'var(--line)',
                  color: mode === id ? 'var(--brass)' : 'var(--muted)',
                }}
              >
                <Icon name={icon} size={12} />
                {label}
              </Note>
            ))}
          </div>
          <Button onClick={() => setPanelTab('logs')}>总线日志</Button>
          <Button onClick={() => createSession()} tone="accent">
            + 新对话
          </Button>
        </div>
      </header>

      <div className="relative min-h-0 flex-1">
        <CanvasStage />
        <div ref={boxRef} className="relative flex h-full min-h-0 flex-col">
          {tabMode && (
            // 标签条：一段一格，关掉的那一格连关闭键一起消失，不留空位。
            <div
              className="flex shrink-0 items-stretch gap-px overflow-x-auto border-b border-[var(--line)]"
              role="tablist"
              aria-label="对话标签页"
            >
              {order.map((id) => {
                const current = shown[0] === id;
                return (
                  <div
                    key={id}
                    className="group flex shrink-0 items-center border-r border-[var(--line)]"
                    style={{ background: current ? 'var(--ink-700)' : 'transparent' }}
                  >
                    <Note
                      as="button"
                      type="button"
                      role="tab"
                      aria-selected={current}
                      onClick={() => setActive(id)}
                      className="flex h-8 items-center gap-2 px-3 text-[11px] transition-colors"
                      style={{ color: current ? 'var(--paper)' : 'var(--muted)' }}
                      text={sessions[id]?.title ?? id}
                    >
                      <span className="mono">{nameOf(id)}</span>
                      <span className="max-w-[18ch] truncate text-[var(--paper-dim)]">
                        {sessions[id]?.title ?? ''}
                      </span>
                    </Note>
                    {order.length > 1 && (
                      <button
                        type="button"
                        onClick={() => closeSession(id)}
                        aria-label={`关掉 ${nameOf(id)}`}
                        className="mr-1 flex h-5 w-5 items-center justify-center text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--danger)]"
                      >
                        ×
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {tabMode ? (
            <div className="relative flex min-h-0 flex-1">
              {shown.map((id) => (
                <ConversationPane
                  key={id}
                  sessionId={id}
                  name={nameOf(id)}
                  active
                  onFocus={() => setActive(id)}
                  onClose={() => closeSession(id)}
                  closable={order.length > 1}
                  historyEnabled={!USE_MOCK}
                  clientRef={clientRef}
                  goalLive={goalLive}
                />
              ))}
            </div>
          ) : (
            <div
              className="grid min-h-0 flex-1 gap-px"
              style={{
                gridTemplateColumns: `repeat(${shape.cols}, minmax(0, 1fr))`,
                gridTemplateRows: `repeat(${shape.rows}, minmax(0, 1fr))`,
              }}
            >
              {order.map((id) => (
                <ConversationPane
                  key={id}
                  sessionId={id}
                  name={nameOf(id)}
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
          )}
        </div>
      </div>
    </div>
  );
}
