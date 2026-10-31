import { Icon } from '@/shell/icons';
import { useShell } from '@/shell/store';

import { Note } from '@/ui/Note';
/**
 * 编辑器上方的页签。
 *
 * 同一面只开一页：页的身份是面的键，不是每次点开的动作。开两份「任务」会让
 * 两份读数各自轮询、各自过滤，最后谁也不是权威。
 */
export function Tabs() {
  const tabs = useShell((s) => s.tabs);
  const activeTabId = useShell((s) => s.activeTabId);
  const activateTab = useShell((s) => s.activateTab);
  const closeTab = useShell((s) => s.closeTab);
  const setPalette = useShell((s) => s.setPalette);

  return (
    <div className="flex h-9 shrink-0 items-stretch border-b border-[var(--line)] bg-[var(--ink-800)]">
      <div className="flex min-w-0 flex-1 overflow-x-auto">
        {tabs.map((tab) => {
          const active = tab.id === activeTabId;
          return (
            <div
              key={tab.id}
              className="group relative flex min-w-0 max-w-[220px] items-center border-r border-[var(--line)]"
              style={{ background: active ? 'var(--ink-900)' : 'transparent' }}
            >
              {active && (
                <span className="absolute inset-x-0 top-0 h-[1.5px]" style={{ background: 'var(--brass)' }} />
              )}
              <Note
                as="button"
                type="button"
                onClick={() => activateTab(tab.id)}
                text={tab.hint ? `${tab.title} — ${tab.hint}` : tab.title}
                className="flex min-w-0 flex-1 items-center py-0 pl-3 pr-1 text-left text-[12px]"
                style={{ color: active ? 'var(--paper)' : 'var(--paper-dim)' }}
              >
                <span className="truncate">{tab.title}</span>
              </Note>
              <Note
                as="button"
                type="button"
                onClick={() => closeTab(tab.id)}
                disabled={tabs.length === 1}
                text={tabs.length === 1 ? '最后一页不关' : '关闭'}
                className="mr-1 flex h-4 w-4 items-center justify-center opacity-0 transition-opacity hover:bg-[var(--ink-600)] group-hover:opacity-100 disabled:opacity-0"
                style={{ color: 'var(--muted)' }}
              >
                <Icon name="close" size={11} />
              </Note>
            </div>
          );
        })}
      </div>
      <Note
        as="button"
        type="button"
        onClick={() => setPalette('open')}
        text="打开一页（⌘P）"
        className="flex w-9 shrink-0 items-center justify-center border-l border-[var(--line)] text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
      >
        <Icon name="plus" size={14} />
      </Note>
    </div>
  );
}
