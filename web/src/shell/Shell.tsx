import { useEffect } from 'react';
import { ActivityBar } from '@/shell/ActivityBar';
import { ExplainDrawer } from '@/shell/ExplainDrawer';
import { useServices } from '@/shell/context';
import { Palette } from '@/shell/Palette';
import { Panel } from '@/shell/Panel';
import { SideBar } from '@/shell/SideBar';
import { StatusBar } from '@/shell/StatusBar';
import { Tabs } from '@/shell/Tabs';
import { TitleBar } from '@/shell/TitleBar';
import { useShell } from '@/shell/store';
import { AgentsOverlay } from '@/overlay/AgentsOverlay';
import { LlmSetupOverlay } from '@/overlay/LlmSetupOverlay';
import { SearchFallbackOverlay } from '@/overlay/SearchFallbackOverlay';
import { ContributionOverlay } from '@/overlay/ContributionOverlay';
import { ViewFor } from '@/views/registry';

/**
 * 工作台外壳：顶栏、活动栏、侧栏、页签、编辑器、底部面板、状态条。
 *
 * 布局照编辑器来，不照后台模板来：一屏里同时看得见「有哪几面」「这一面是什么」
 * 「刚才发生了什么」，不用在页面之间跳。每一格的读失败都画在它自己身上，所以
 * 布局本身不承担任何状态。
 */
export function Shell() {
  const tabs = useShell((s) => s.tabs);
  const activeTabId = useShell((s) => s.activeTabId);
  const palette = useShell((s) => s.palette);
  const setPalette = useShell((s) => s.setPalette);
  const toggleSidebar = useShell((s) => s.toggleSidebar);
  const togglePanel = useShell((s) => s.togglePanel);
  const { llm, overlays } = useServices();

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const meta = event.metaKey || event.ctrlKey;
      const key = event.key.toLowerCase();
      if (meta && key === 'k') {
        event.preventDefault();
        setPalette(palette === 'commands' ? 'closed' : 'commands');
      } else if (meta && key === 'p') {
        event.preventDefault();
        setPalette(palette === 'open' ? 'closed' : 'open');
      } else if (meta && key === 'b') {
        event.preventDefault();
        toggleSidebar();
      } else if (meta && key === 'j') {
        event.preventDefault();
        togglePanel();
      } else if (event.key === 'Escape' && palette !== 'closed') {
        setPalette('closed');
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [palette, setPalette, toggleSidebar, togglePanel]);

  const llmReady = !!llm.status?.configured;
  const showLlmWizard = !!llm.status && (!llmReady || overlays.llmSetup);
  const active = tabs.find((tab) => tab.id === activeTabId) ?? tabs[0];

  return (
    <div className="shell-root relative flex h-full w-full flex-col overflow-hidden">
      <TitleBar />
      <div className="flex min-h-0 flex-1">
        <ActivityBar />
        <SideBar />
        <main className="flex min-w-0 flex-1 flex-col">
          <Tabs />
          {/* 只挂当前这一页：藏起来的页仍在轮询，而画布的尺寸在 `display:none`
              里是 0，切回来时不会自己再量一次——那样会得到一个空白的画布，
              看起来像「没有事件」。 */}
          <div className="relative min-h-0 flex-1 overflow-hidden">
            <ViewFor view={active.view} />
          </div>
          <Panel />
        </main>
      </div>
      <StatusBar />
      <Palette />
      {/* 抽屉挂在最外层：任何一面里的任何一个带虚线的数都从这里打开，不各面自带一份。 */}
      <ExplainDrawer />

      {overlays.conversations && <AgentsOverlay onClose={overlays.closeConversations} />}

      {overlays.webSearch && (
        <SearchFallbackOverlay onClose={overlays.closeWebSearch} />
      )}

      {overlays.credentials && (
        <ContributionOverlay onClose={overlays.closeCredentials} />
      )}

      {showLlmWizard && (
        <LlmSetupOverlay
          initial={llm.status?.backends?.[0]}
          mandatory={!llmReady}
          onConfigured={() => {
            overlays.closeLlmSetup();
            llm.refresh();
          }}
          onClose={llmReady ? overlays.closeLlmSetup : undefined}
        />
      )}
    </div>
  );
}
