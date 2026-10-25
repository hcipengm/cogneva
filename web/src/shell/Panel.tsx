import { useCallback, useEffect, useRef, useState } from 'react';
import { useServices } from '@/shell/context';
import { deriveProblems } from '@/shell/problems';
import { Icon } from '@/shell/icons';
import { useShell, type PanelTab } from '@/shell/store';
import { useStreamStore } from '@/store/streamStore';

/**
 * 底部面板：问题、输出、日志、原文。
 *
 * 四格回答四个不同的问题——「有什么需要人处理」「我刚才点的那个动作结果如何」
 * 「总线上真的在跑什么」「智能体正在说什么」。合成一格会让最要紧的那类被最多的
 * 那类淹没。
 */
const TABS: { key: PanelTab; label: string }[] = [
  { key: 'problems', label: '问题' },
  { key: 'output', label: '输出' },
  { key: 'logs', label: '日志' },
  { key: 'text', label: '原文' },
];

const MIN_HEIGHT = 120;
const MAX_HEIGHT = 520;

export function Panel() {
  const open = useShell((s) => s.panelOpen);
  const tab = useShell((s) => s.panelTab);
  const setPanelTab = useShell((s) => s.setPanelTab);
  const togglePanel = useShell((s) => s.togglePanel);
  const output = useShell((s) => s.output);
  const setActivity = useShell((s) => s.setActivity);
  const { overview, alerts, llm } = useServices();
  const logs = useStreamStore((s) => s.activity);
  const messages = useStreamStore((s) => s.messages);

  const problems = deriveProblems({
    overview: { data: overview.data, error: overview.error },
    alerts: { data: alerts.alerts, error: alerts.error },
    llm: { status: llm.status },
  });

  const [height, setHeight] = useState(200);
  const dragging = useRef<{ startY: number; startHeight: number } | null>(null);

  const onPointerDown = useCallback(
    (event: React.PointerEvent) => {
      dragging.current = { startY: event.clientY, startHeight: height };
      (event.target as HTMLElement).setPointerCapture(event.pointerId);
    },
    [height]
  );

  useEffect(() => {
    if (!open) return;
    const onMove = (event: PointerEvent) => {
      const drag = dragging.current;
      if (!drag) return;
      const next = drag.startHeight + (drag.startY - event.clientY);
      setHeight(Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, next)));
    };
    const onUp = () => {
      dragging.current = null;
    };
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
    return () => {
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
    };
  }, [open]);

  if (!open) return null;

  const counts: Record<PanelTab, number | null> = {
    problems: problems.length,
    output: output.length,
    logs: logs.length,
    text: messages.length,
  };

  return (
    <section
      className="flex shrink-0 flex-col border-t border-[var(--line)] bg-[var(--ink-800)]"
      style={{ height }}
      aria-label="底部面板"
    >
      <div
        onPointerDown={onPointerDown}
        className="h-[3px] shrink-0 cursor-row-resize"
        style={{ background: 'var(--line)' }}
      />
      <header className="flex h-8 shrink-0 items-center border-b border-[var(--line)] px-1">
        {TABS.map((t) => {
          const active = tab === t.key;
          const count = counts[t.key];
          return (
            <button
              key={t.key}
              type="button"
              onClick={() => setPanelTab(t.key)}
              className="flex h-full items-center gap-1.5 px-3 text-[11px] uppercase tracking-[0.1em]"
              style={{
                color: active ? 'var(--paper)' : 'var(--muted)',
                borderBottom: active ? '1px solid var(--brass)' : '1px solid transparent',
              }}
            >
              {t.label}
              {count !== null && (
                <span
                  className="mono text-[10px]"
                  style={{
                    color:
                      t.key === 'problems' && count > 0 ? 'var(--danger)' : 'var(--muted)',
                  }}
                >
                  {count}
                </span>
              )}
            </button>
          );
        })}
        <button
          type="button"
          onClick={togglePanel}
          title="收起面板（⌘J）"
          className="ml-auto mr-1 flex h-6 w-6 items-center justify-center text-[var(--muted)] hover:text-[var(--paper)]"
        >
          <Icon name="close" size={12} />
        </button>
      </header>

      <div className="min-h-0 flex-1 overflow-auto">
        {tab === 'problems' && (
          problems.length === 0 ? (
            <p className="px-3 py-3 text-[11px] text-[var(--muted)]">
              没有需要人处理的问题。这一格读的是告警、集群判词与模型配置，不是「一切都验过」。
            </p>
          ) : (
            <ul>
              {problems.map((p) => (
                <li key={p.id}>
                  <button
                    type="button"
                    onClick={() => setActivity(p.goto)}
                    className="flex w-full items-baseline gap-3 border-b border-[var(--line)] px-3 py-1 text-left text-[11px] hover:bg-[var(--ink-700)]"
                  >
                    <span
                      className="mono w-14 shrink-0"
                      style={{ color: p.severity === 'error' ? 'var(--danger)' : 'var(--warn)' }}
                    >
                      {p.severity}
                    </span>
                    <span className="mono w-36 shrink-0 truncate text-[var(--muted)]">{p.source}</span>
                    <span className="min-w-0 flex-1 truncate text-[var(--paper-dim)]">{p.message}</span>
                  </button>
                </li>
              ))}
            </ul>
          )
        )}

        {tab === 'output' && (
          output.length === 0 ? (
            <p className="px-3 py-3 text-[11px] text-[var(--muted)]">
              还没有写过东西。命令的结果与确认告警的结局都落在这里。
            </p>
          ) : (
            <ol className="mono px-3 py-1 text-[11px] leading-relaxed">
              {output.map((line, index) => (
                <li key={`${index}-${line}`} className="text-[var(--paper-dim)]">
                  <span className="mr-2 select-none text-[var(--muted)]">
                    {String(index + 1).padStart(3, '0')}
                  </span>
                  {line}
                </li>
              ))}
            </ol>
          )
        )}

        {tab === 'logs' && (
          logs.length === 0 ? (
            <p className="px-3 py-3 text-[11px] text-[var(--muted)]">
              窗口内还没有总线事件。它只记连上之后到达的帧，不是平台启动以来的全量日志。
            </p>
          ) : (
            <ul>
              {logs.slice(0, 200).map((entry) => (
                <li
                  key={entry.id}
                  className="flex items-baseline gap-3 border-b border-[color-mix(in_srgb,var(--line)_55%,transparent)] px-3 py-1 text-[11px]"
                >
                  <span className="mono w-16 shrink-0 text-[var(--muted)]">
                    {new Date(entry.at).toLocaleTimeString('zh-CN', { hour12: false })}
                  </span>
                  <span className="mono w-32 shrink-0 truncate text-[var(--live)]" title={entry.agent ?? ''}>
                    {entry.agent ?? '—'}
                  </span>
                  <span
                    className="w-56 shrink-0 truncate"
                    style={{ color: entry.failed ? 'var(--danger)' : 'var(--paper-dim)' }}
                  >
                    {entry.summary}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-[var(--muted)]" title={entry.detail ?? ''}>
                    {entry.detail ?? ''}
                  </span>
                </li>
              ))}
            </ul>
          )
        )}

        {tab === 'text' && (
          messages.length === 0 ? (
            <p className="px-3 py-3 text-[11px] text-[var(--muted)]">
              窗口内还没有消息帧。这里记的是智能体运行推回来的原文（`message_start` /
              `message_update` / `message_end`），与工作台上那几列会话对话是两条通路：
              会话走聊天帧，这一格走 agent 事件。
            </p>
          ) : (
            <ol className="px-3 py-1 text-[11px] leading-relaxed">
              {messages.map((m) => (
                <li key={m.id} className="border-b border-[color-mix(in_srgb,var(--line)_55%,transparent)] py-1">
                  <div className="flex items-baseline gap-3">
                    <span className="mono w-32 shrink-0 truncate text-[var(--live)]" title={m.agent ?? ''}>
                      {m.agent ?? '—'}
                    </span>
                    <span className="mono w-16 shrink-0 text-[var(--muted)]">{m.role}</span>
                    <span
                      className="mono w-16 shrink-0"
                      style={{ color: m.status === 'streaming' ? 'var(--brass)' : 'var(--muted)' }}
                    >
                      {m.status === 'streaming' ? '生成中' : m.status === 'error' ? '出错' : '完成'}
                    </span>
                    <span className="min-w-0 flex-1 whitespace-pre-wrap break-words text-[var(--paper-dim)]">
                      {m.text || '（还没有正文）'}
                    </span>
                  </div>
                </li>
              ))}
            </ol>
          )
        )}
      </div>
    </section>
  );
}
