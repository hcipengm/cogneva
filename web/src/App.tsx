import { CanvasStage } from './canvas/CanvasStage';
import { InputOverlay } from './overlay/InputOverlay';
import { AuthOverlay } from './overlay/AuthOverlay';
import { useEventStream } from './hooks/useEventStream';
import { useAuth } from './hooks/useAuth';

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

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-slate-950 text-slate-100">
      <CanvasStage />
      <InputOverlay clientRef={clientRef} />

      {!USE_MOCK && !isAuthenticated && (
        <AuthOverlay onLogin={signIn} error={error} isLoading={isLoading} />
      )}

      {!USE_MOCK && isAuthenticated && (
        <button
          onClick={signOut}
          className="absolute right-4 top-12 z-10 rounded-lg border border-slate-700/60 bg-slate-900/80 px-3 py-1.5 text-xs text-slate-300 backdrop-blur-sm transition hover:bg-slate-800"
        >
          退出登录
        </button>
      )}
    </div>
  );
}
