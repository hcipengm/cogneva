import { useEffect, useMemo, useRef, useState } from 'react';
import { useServices } from '@/shell/context';
import { Icon } from '@/shell/icons';
import { useShell, VIEW_TITLES, type ActivityId } from '@/shell/store';
import { useTheme } from '@/shell/theme';
import { THEMES } from '@/styles/themes';

/**
 * 命令面板：一个输入框，两种问法。
 *
 * ⌘K 问「能做什么」，⌘P 问「去哪一页」。合成一个列表看似省事，但两个动作的
 * 意图不同：找命令的人会打出动词，找页的人会打出页面名，混在一起时前者被
 * 后者挤下去。
 */
interface Command {
  id: string;
  group: '前往' | '动作' | '主题';
  title: string;
  hint?: string;
  run: () => void;
}

/** 子序列匹配：查询的每个字符按顺序出现即可，「任合任务」能命中「任务列表」。 */
function matches(text: string, query: string): boolean {
  if (!query) return true;
  const haystack = text.toLowerCase();
  let position = 0;
  for (const char of query.toLowerCase()) {
    const found = haystack.indexOf(char, position);
    if (found < 0) return false;
    position = found + 1;
  }
  return true;
}

export function Palette() {
  const mode = useShell((s) => s.palette);
  const setPalette = useShell((s) => s.setPalette);
  const setActivity = useShell((s) => s.setActivity);
  const toggleSidebar = useShell((s) => s.toggleSidebar);
  const togglePanel = useShell((s) => s.togglePanel);
  const setPanelTab = useShell((s) => s.setPanelTab);
  const appendOutput = useShell((s) => s.appendOutput);
  const clearOutput = useShell((s) => s.clearOutput);
  const { alerts, notifications, overlays, signOut } = useServices();
  const { openConversations, openLlmSetup } = overlays;
  const theme = useTheme((s) => s.theme);
  const setTheme = useTheme((s) => s.setTheme);

  const [query, setQuery] = useState('');
  const [index, setIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (mode === 'closed') return;
    setQuery('');
    setIndex(0);
    // 浮层刚挂上时输入框还没焦点；下一帧再抓，否则打出来的第一个字落空。
    const frame = requestAnimationFrame(() => inputRef.current?.focus());
    return () => cancelAnimationFrame(frame);
  }, [mode]);

  useEffect(() => {
    setIndex(0);
  }, [query]);

  const commands = useMemo<Command[]>(() => {
    const views = (Object.keys(VIEW_TITLES) as ActivityId[]).map((id) => ({
      id: `view:${id}`,
      group: '前往' as const,
      title: VIEW_TITLES[id],
      hint: id,
      run: () => setActivity(id),
    }));

    const actions: Command[] = [
      {
        id: 'action:conversations',
        group: '动作',
        title: '与智能体对话',
        hint: '打开在岗名单与彼此的会话',
        run: openConversations,
      },
      {
        id: 'action:llm',
        group: '动作',
        title: '配置模型上游',
        hint: '服务端会做双协议探测',
        run: openLlmSetup,
      },
      {
        id: 'action:alerts-refresh',
        group: '动作',
        title: '重读告警',
        run: () => {
          alerts.refresh();
          appendOutput('已请求重读告警');
        },
      },
      {
        id: 'action:notifications-read-all',
        group: '动作',
        title: '把通知全部标为已读',
        run: () => {
          void notifications.markAllRead();
          appendOutput('已请求把通知全部标为已读');
        },
      },
      {
        id: 'action:sidebar',
        group: '动作',
        title: '显示 / 隐藏侧栏',
        hint: '⌘B',
        run: toggleSidebar,
      },
      {
        id: 'action:panel',
        group: '动作',
        title: '显示 / 隐藏底部面板',
        hint: '⌘J',
        run: togglePanel,
      },
      {
        id: 'action:panel-output',
        group: '动作',
        title: '打开输出格',
        run: () => setPanelTab('output'),
      },
      { id: 'action:clear-output', group: '动作', title: '清空输出格', run: clearOutput },
      { id: 'action:signout', group: '动作', title: '退出登录', run: signOut },
    ];

    // 每个主题是一条命令，而不是「切到下一个」——找颜色的人记得住名字，记不住
    // 自己现在停在第几个；一条命令一个终点，按名字搜得到。
    const themes: Command[] = THEMES.map((entry) => ({
      id: `theme:${entry.id}`,
      group: '主题' as const,
      title: `${entry.name}${entry.id === theme ? '（当前）' : ''}`,
      hint: entry.note,
      run: () => setTheme(entry.id),
    }));

    return [...views, ...actions, ...themes];
  }, [
    setActivity,
    openConversations,
    openLlmSetup,
    alerts,
    notifications,
    appendOutput,
    toggleSidebar,
    togglePanel,
    setPanelTab,
    clearOutput,
    signOut,
    theme,
    setTheme,
  ]);

  const visible = useMemo(
    () => commands.filter((c) => matches(`${c.title} ${c.hint ?? ''}`, query)),
    [commands, query]
  );

  if (mode === 'closed') return null;

  const close = () => setPalette('closed');
  const runAt = (position: number) => {
    const command = visible[position];
    if (!command) return;
    command.run();
    close();
  };

  const onKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      close();
    } else if (event.key === 'ArrowDown' || (event.key === 'n' && event.ctrlKey)) {
      event.preventDefault();
      setIndex((i) => Math.min(visible.length - 1, i + 1));
    } else if (event.key === 'ArrowUp' || (event.key === 'p' && event.ctrlKey)) {
      event.preventDefault();
      setIndex((i) => Math.max(0, i - 1));
    } else if (event.key === 'Enter') {
      event.preventDefault();
      runAt(index);
    }
  };

  return (
    <div
      className="absolute inset-0 z-40 flex items-start justify-center bg-[color-mix(in_srgb,var(--ink-1000)_55%,transparent)] pt-[8vh]"
      onMouseDown={close}
    >
      <div
        className="w-[min(620px,92vw)] border border-[var(--line-strong)] bg-[var(--ink-800)] shadow-2xl"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-[var(--line)] px-3 py-2">
          <Icon name="search" size={14} className="text-[var(--muted)]" />
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder={mode === 'commands' ? '输入命令…' : '打开哪一页…'}
            className="mono flex-1 bg-transparent text-[13px] text-[var(--paper)] outline-none placeholder:text-[var(--muted)]"
            aria-label="命令面板"
          />
          <span className="text-[10px] text-[var(--muted)]">esc 关闭 · ↑↓ 选择 · ↵ 执行</span>
        </div>
        <div className="max-h-[52vh] overflow-y-auto">
          {visible.length === 0 && (
            <p className="px-3 py-3 text-[12px] text-[var(--muted)]">没有匹配的命令。</p>
          )}
          {groupOf(visible).map(([group, items]) => (
            <div key={group}>
              <div className="px-3 pb-1 pt-2 text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
                {group}
              </div>
              {items.map(({ command, position }) => (
                <button
                  key={command.id}
                  type="button"
                  onMouseMove={() => setIndex(position)}
                  onClick={() => runAt(position)}
                  className="flex w-full items-baseline gap-3 px-3 py-1.5 text-left text-[12px]"
                  style={{
                    background: position === index ? 'var(--ink-600)' : 'transparent',
                    color: position === index ? 'var(--paper)' : 'var(--paper-dim)',
                  }}
                >
                  <span className="min-w-0 flex-1 truncate">{command.title}</span>
                  {command.hint && (
                    <span className="shrink-0 text-[10px] text-[var(--muted)]">{command.hint}</span>
                  )}
                </button>
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

/** 按组切一遍，同时留住在整个列表里的位置，回车执行的就是高亮那一行。 */
function groupOf(commands: Command[]): [string, { command: Command; position: number }[]][] {
  const groups = new Map<string, { command: Command; position: number }[]>();
  commands.forEach((command, position) => {
    const bucket = groups.get(command.group) ?? [];
    bucket.push({ command, position });
    groups.set(command.group, bucket);
  });
  return Array.from(groups.entries());
}
