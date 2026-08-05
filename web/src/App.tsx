import { useState } from 'react';
import { CanvasStage } from './canvas/CanvasStage';
import { InputOverlay } from './overlay/InputOverlay';
import { AuthOverlay } from './overlay/AuthOverlay';
import { LlmSetupOverlay } from './overlay/LlmSetupOverlay';
import { useEventStream } from './hooks/useEventStream';
import { useAuth } from './hooks/useAuth';
import { useLlmStatus } from './hooks/useLlmStatus';

const USE_MOCK = import.meta.env.VITE_USE_MOCK === 'true';

export default function App() {
  const { token, isAuthenticated, isLoading, error, signIn, signOut } =
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

  const llmReady = !!llmStatus?.configured;
  const showLlmWizard =
    !USE_MOCK && isAuthenticated && !llmLoading && !llmReady;

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-slate-950 text-slate-100">
      <CanvasStage />
      <InputOverlay clientRef={clientRef} />

      {!USE_MOCK && !isAuthenticated && (
        <AuthOverlay onLogin={signIn} error={error} isLoading={isLoading} />
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

      {!USE_MOCK && isAuthenticated && (
        <div className="absolute right-4 top-12 z-10 flex gap-2">
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
