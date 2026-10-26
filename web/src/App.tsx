import { useCallback, useMemo, useState } from 'react';
import { getClusterOverview } from '@/api/client';
import { useActiveAlerts } from '@/hooks/useActiveAlerts';
import { useAuth } from '@/hooks/useAuth';
import { useEventStream } from '@/hooks/useEventStream';
import { useLlmStatus } from '@/hooks/useLlmStatus';
import { useNotifications } from '@/hooks/useNotifications';
import { useResource } from '@/hooks/useResource';
import { resourceKey } from '@/store/frameTick';
import { USE_MOCK } from '@/runtime';
import { AuthOverlay } from '@/overlay/AuthOverlay';
import { ShellServicesProvider, type ShellServices } from '@/shell/context';
import { Shell } from '@/shell/Shell';

/**
 * 入口：先判身份，再把跨面共用的读数装进工作台。
 *
 * 共用读数放在这一层而不是各面自己读：状态条、侧栏、问题面板、收件箱要看到
 * 同一个数，否则同一秒里两处会给出两个不同的答案，而读者还不知道该信谁。
 */
export default function App() {
  const auth = useAuth();
  const signedIn = USE_MOCK || auth.isAuthenticated;

  const wsUrl = auth.token
    ? `${
        import.meta.env.VITE_WS_URL ??
        `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}`
      }/ws?token=${auth.token}`
    : '';

  // 通知中心先于事件流建：推送到达时要直接落进它的列表，顺序反过来只能缓存
  // 或轮询，两种都是「看得见但要晚一拍」。
  const notifications = useNotifications(signedIn && !USE_MOCK);

  const { clientRef } = useEventStream({
    mock: USE_MOCK,
    url: wsUrl,
    // 任务状态不走 `agent:<id>` 面，它按 `task:<id>` 分频道。只订 agent 面时
    // 界面上一件事都看不见任务在动，而总线上它一直在动。
    channels: ['agent:*', 'task:*', 'hooks'],
    onNotification: notifications.receive,
  });

  const llm = useLlmStatus(signedIn && !USE_MOCK);
  const alerts = useActiveAlerts(signedIn && !USE_MOCK, 30_000);
  const overview = useResource(
    '集群概况',
    getClusterOverview,
    30_000,
    signedIn && !USE_MOCK,
    // 状态条上那几个数是全平台共用的，订的面也宽：任务、执行者、告警都算数。
    ['tasks', 'agents', 'alerts', resourceKey('cluster')]
  );

  const [conversations, setConversations] = useState(false);
  const [llmSetup, setLlmSetup] = useState(false);
  const [webSearch, setWebSearch] = useState(false);
  const [credentials, setCredentials] = useState(false);

  const openConversations = useCallback(() => setConversations(true), []);
  const closeConversations = useCallback(() => setConversations(false), []);
  const openLlmSetup = useCallback(() => setLlmSetup(true), []);
  const closeLlmSetup = useCallback(() => setLlmSetup(false), []);
  const openWebSearch = useCallback(() => setWebSearch(true), []);
  const closeWebSearch = useCallback(() => setWebSearch(false), []);
  const openCredentials = useCallback(() => setCredentials(true), []);
  const closeCredentials = useCallback(() => setCredentials(false), []);

  const services = useMemo<ShellServices>(
    () => ({
      username: auth.username,
      clientRef,
      overview,
      alerts,
      notifications,
      llm,
      signOut: auth.signOut,
      overlays: {
        conversations,
        llmSetup,
        webSearch,
        credentials,
        openConversations,
        closeConversations,
        openLlmSetup,
        closeLlmSetup,
        openWebSearch,
        closeWebSearch,
        openCredentials,
        closeCredentials,
      },
    }),
    [
      auth.username,
      auth.signOut,
      clientRef,
      overview,
      alerts,
      notifications,
      llm,
      conversations,
      llmSetup,
      webSearch,
      credentials,
      openConversations,
      closeConversations,
      openLlmSetup,
      closeLlmSetup,
      openWebSearch,
      closeWebSearch,
      openCredentials,
      closeCredentials,
    ]
  );

  if (!signedIn) {
    return (
      <div className="relative h-screen w-screen overflow-hidden bg-[var(--ink-1000)]">
        <AuthOverlay
          onLogin={auth.signIn}
          onPlatformToken={auth.signInPlatformToken}
          error={auth.error}
          isLoading={auth.isLoading}
        />
      </div>
    );
  }

  return (
    <ShellServicesProvider services={services}>
      <Shell />
    </ShellServicesProvider>
  );
}
