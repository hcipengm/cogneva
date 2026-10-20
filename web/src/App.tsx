import { useState } from 'react';
import { CanvasStage } from './canvas/CanvasStage';
import { InputOverlay } from './overlay/InputOverlay';
import { AuthOverlay } from './overlay/AuthOverlay';
import { LlmSetupOverlay } from './overlay/LlmSetupOverlay';
import { TokenUsageOverlay } from './overlay/TokenUsageOverlay';
import { ActiveAlertsOverlay } from './overlay/ActiveAlertsOverlay';
import { useActiveAlerts } from './hooks/useActiveAlerts';
import { useEventStream } from './hooks/useEventStream';
import { useAuth } from './hooks/useAuth';
import { useLlmStatus } from './hooks/useLlmStatus';

const USE_MOCK = import.meta.env.VITE_USE_MOCK === 'true';

export default function App() {
  const { token, isAuthenticated, isLoading, error, signIn, signInPlatformToken, signOut } =
    useAuth();

  const wsUrl = token
    ? `${
        import.meta.env.VITE_WS_URL ??
        `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}`
      }/ws?token=${token}`
    : '';

  const { clientRef } = useEventStream({
    mock: USE_MOCK,
    url: wsUrl,
    channels: ['agent:*', 'hooks'],
  });

  const {
    status: llmStatus,
    loading: llmLoading,
    refresh: refreshLlmStatus,
  } = useLlmStatus(!USE_MOCK && isAuthenticated);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [usageOpen, setUsageOpen] = useState(false);
  const [alertsOpen, setAlertsOpen] = useState(false);
  // 轮询放在 App 层：面板关着的时候它的计数也在更新，否则「有人看才亮」
  // 就还是面板里的一个数。
  const {
    alerts: activeAlerts,
    loading: alertsLoading,
    error: alertsError,
    refresh: refreshAlerts,
  } = useActiveAlerts(!USE_MOCK && isAuthenticated);
  const firingCount = activeAlerts?.count ?? 0;

  const llmReady = !!llmStatus?.configured;
  const showLlmWizard =
    !USE_MOCK && isAuthenticated && !llmLoading && !llmReady;

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-slate-950 text-slate-100">
      <CanvasStage />
      <InputOverlay clientRef={clientRef} />

      {!USE_MOCK && !isAuthenticated && (
        <AuthOverlay
          onLogin={signIn}
          onPlatformToken={signInPlatformToken}
          error={error}
          isLoading={isLoading}
        />
      )}

      {!USE_MOCK && isAuthenticated && (showLlmWizard || settingsOpen) && (
        <LlmSetupOverlay
          initial={llmStatus?.backends?.[0]}
          mandatory={!llmReady}
          onConfigured={() => {
            setSettingsOpen(false);
            refreshLlmStatus();
          }}
          onClose={llmReady ? () => setSettingsOpen(false) : undefined}
        />
      )}

      {!USE_MOCK && isAuthenticated && usageOpen && (
        <TokenUsageOverlay onClose={() => setUsageOpen(false)} />
      )}

      {!USE_MOCK && isAuthenticated && alertsOpen && (
        <ActiveAlertsOverlay
          alerts={activeAlerts}
          loading={alertsLoading}
          error={alertsError}
          refresh={refreshAlerts}
          onClose={() => setAlertsOpen(false)}
        />
      )}

      {!USE_MOCK && isAuthenticated && (
        <div className="absolute right-4 top-12 z-10 flex gap-2">
          <button
            onClick={() => setUsageOpen(true)}
            className="rounded-lg border border-slate-700/60 bg-slate-900/80 px-3 py-1.5 text-xs text-slate-300 backdrop-blur-sm transition hover:bg-slate-800"
          >
            用量
          </button>
          <button
            onClick={() => setAlertsOpen(true)}
            className={`rounded-lg border px-3 py-1.5 text-xs backdrop-blur-sm transition ${
              alertsError
                ? 'border-amber-500/60 bg-amber-500/10 text-amber-300 hover:bg-amber-500/20'
                : firingCount > 0
                  ? 'border-red-500/60 bg-red-500/10 text-red-300 hover:bg-red-500/20'
                  : 'border-slate-700/60 bg-slate-900/80 text-slate-300 hover:bg-slate-800'
            }`}
            title={
              alertsError
                ? '告警读数没读到——不是没有告警'
                : '当前 firing 的告警'
            }
          >
            {alertsError ? '告警 ?' : `告警 ${firingCount}`}
          </button>
          {llmReady && (
            <button
              onClick={() => setSettingsOpen(true)}
              className="rounded-lg border border-slate-700/60 bg-slate-900/80 px-3 py-1.5 text-xs text-slate-300 backdrop-blur-sm transition hover:bg-slate-800"
            >
              LLM 设置
            </button>
          )}
          <button
            onClick={signOut}
            className="rounded-lg border border-slate-700/60 bg-slate-900/80 px-3 py-1.5 text-xs text-slate-300 backdrop-blur-sm transition hover:bg-slate-800"
          >
            退出登录
          </button>
        </div>
      )}
    </div>
  );
}
