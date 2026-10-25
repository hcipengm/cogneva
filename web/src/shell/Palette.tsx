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
  group: '前往' | '动作' | '主题' | '帮助';
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
  const seed = useShell((s) => s.paletteSeed);
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
    setQuery(seed);
    setIndex(0);
    // 浮层刚挂上时输入框还没焦点；下一帧再抓，否则打出来的第一个字落空。
    const frame = requestAnimationFrame(() => inputRef.current?.focus());
    return () => cancelAnimationFrame(frame);
  }, [mode, seed]);

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
      // 标题里带上游名字：认这套主题的人搜的是「Neon Lagoon」，不是我们起的
      // 中文名；只放中文名时，搜上游名字一条都匹配不到。
      title: `${entry.name} · ${entry.origin}${entry.id === theme ? '（当前）' : ''}`,
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

  /**
   * 帮助不是一段说明，是一张「问一句 → 去一处」的表。
   *
   * 每一条都以人真正会问的那句话开头，落到一个真的能打开的地方。写成「点击左侧
   * 观测图标进入观测面板」那种句子，界面一改版它就开始说假话；写成去处，它随
   * 界面一起对。
   */
  const help = useMemo<Command[]>(() => {
    const go = (id: ActivityId, title: string, hint: string): Command => ({
      id: `help:${id}`,
      group: '帮助',
      title,
      hint,
      run: () => setActivity(id),
    });
    return [
      {
        id: 'help:llm',
        group: '帮助',
        title: '模型上游在哪配？接哪一个模型、密钥放哪',
        hint: '只在安全网关里配',
        run: openLlmSetup,
      },
      go('observability', '集群现在什么状态？机器、负载、资源', VIEW_TITLES.observability),
      go('agents', '谁在替我干活？在岗的智能体与它们的上下文', VIEW_TITLES.agents),
      go('tasks', '这件事排到哪一步了？', VIEW_TITLES.tasks),
      go('comms', '谁把活交给了谁？断在哪一环？', VIEW_TITLES.comms),
      go('changes', '我提的改动是怎么进到集群里的？', VIEW_TITLES.changes),
      go('governance', '哪条规则把我挡下来了？', VIEW_TITLES.governance),
      go('audit', '这一屏的数字能追到源头吗？', VIEW_TITLES.audit),
      go('alerts', '现在有什么在报警？', VIEW_TITLES.alerts),
      go('inbox', '有没有人找我？', VIEW_TITLES.inbox),
      go('timeline', '那一次到底发生了什么？倒回去看一帧一帧', VIEW_TITLES.timeline),
      go('eval', '这东西测得出来吗？跑过几次、结论是什么？', VIEW_TITLES.eval),
      go('storage', '平台自己存了些什么？占多大？', VIEW_TITLES.storage),
      go('files', '代码和文档在哪看、在哪改？', VIEW_TITLES.files),
      go('settings', '账号、主题、模型入口', VIEW_TITLES.settings),
    ];
  }, [openLlmSetup, setActivity]);

  const source = mode === 'help' ? help : commands;
  const visible = useMemo(
    () => source.filter((c) => matches(`${c.title} ${c.hint ?? ''}`, query)),
    [source, query]
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
            placeholder={
              mode === 'commands'
                ? '输入命令…'
                : mode === 'help'
                  ? '你想找什么…'
                  : '打开哪一页…'
            }
            className="mono flex-1 bg-transparent text-[13px] text-[var(--paper)] outline-none placeholder:text-[var(--muted)]"
            aria-label={mode === 'help' ? '帮助' : '命令面板'}
          />
          <span className="text-[10px] text-[var(--muted)]">
            {mode === 'help'
              ? '⌘K 命令 · ⌘P 打开页 · ⌘B 侧栏 · ⌘J 面板'
              : 'esc 关闭 · ↑↓ 选择 · ↵ 执行'}
          </span>
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
