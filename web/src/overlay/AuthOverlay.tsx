import { useState } from 'react';
import {
  platformOAuthStart,
  platformOAuthExchange,
  preferredPlatform,
  type PlatformProvider,
} from '@/api/client';

interface AuthOverlayProps {
  onLogin: (username: string, password: string) => void;
  onPlatformToken: (provider: PlatformProvider, token: string) => Promise<void>;
  error: string | null;
  isLoading: boolean;
}

const PLATFORM_LABEL: Record<PlatformProvider, string> = {
  gitee: 'Gitee',
  github: 'GitHub',
};

export function AuthOverlay({
  onLogin,
  onPlatformToken,
  error,
  isLoading,
}: AuthOverlayProps) {
  const recommended = preferredPlatform();
  const other: PlatformProvider = recommended === 'gitee' ? 'github' : 'gitee';
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('admin');
  const [showOther, setShowOther] = useState(false);
  const [busyProvider, setBusyProvider] = useState<PlatformProvider | null>(null);
  // Paste-URL fallback for installs the platform cannot redirect back to.
  const [pending, setPending] = useState<{
    provider: PlatformProvider;
    state: string;
  } | null>(null);
  const [pasted, setPasted] = useState('');
  const [oauthError, setOauthError] = useState<string | null>(null);
  const [pat, setPat] = useState('');
  const [patProvider, setPatProvider] = useState<PlatformProvider>(recommended);
  const [patError, setPatError] = useState<string | null>(null);

  const startOAuth = async (provider: PlatformProvider) => {
    setOauthError(null);
    setBusyProvider(provider);
    try {
      const { authorize_url, state } = await platformOAuthStart(provider);
      window.open(authorize_url, '_blank', 'noopener');
      setPending({ provider, state });
    } catch (e) {
      setOauthError((e as { message?: string }).message ?? 'OAuth 发起失败');
    } finally {
      setBusyProvider(null);
    }
  };

  const finishOAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!pending || !pasted.trim()) return;
    setOauthError(null);
    setBusyProvider(pending.provider);
    try {
      const value = pasted.trim();
      const res = await platformOAuthExchange(
        pending.provider,
        pending.state,
        undefined,
        value
      );
      localStorage.setItem('cogneva_token', res.token);
      window.location.reload();
    } catch (err) {
      setOauthError(
        (err as { message?: string }).message ?? '授权码交换失败，请重试'
      );
      setBusyProvider(null);
    }
  };

  const submitPat = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!pat.trim()) return;
    setPatError(null);
    try {
      await onPlatformToken(patProvider, pat.trim());
    } catch (err) {
      setPatError((err as { message?: string }).message ?? '令牌校验失败');
    }
  };

  const submitPassword = (e: React.FormEvent) => {
    e.preventDefault();
    if (!username.trim() || !password.trim()) return;
    onLogin(username.trim(), password.trim());
  };

  const card = (provider: PlatformProvider) => (
    <button
      key={provider}
      type="button"
      disabled={isLoading || busyProvider !== null}
      onClick={() => startOAuth(provider)}
      className={`flex-1 rounded-xl border px-4 py-5 text-left transition disabled:cursor-not-allowed disabled:opacity-50 ${
        provider === recommended
          ? 'border-emerald-500/60 bg-emerald-500/10 hover:bg-emerald-500/20'
          : 'border-slate-700 bg-slate-800/60 hover:bg-slate-800'
      }`}
    >
      <div className="flex items-center gap-2">
        <span className="text-base font-semibold text-white">
          {PLATFORM_LABEL[provider]}
        </span>
        {provider === recommended && (
          <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-xs text-emerald-300">
            推荐
          </span>
        )}
      </div>
      <div className="mt-1 text-xs text-slate-400">一键授权登录</div>
    </button>
  );

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 backdrop-blur-sm">
      <div className="w-full max-w-lg rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl">
        <h1 className="mb-2 text-xl font-semibold text-white">
          连上账号，让你的 Cogneva 加入一个共同进化的社群
        </h1>
        <p className="mb-6 text-sm leading-6 text-slate-400">
          你的 Cogneva 每天都在变聪明。连上账号后，它学会的新本领会分享给所有人；
          同样，全世界其他 Cogneva 学会的本领，也会流回你的系统。一个人走得快，一群人走得远。
          你的代码和密钥永远只存在你自己的机器里，分享的只有「学会的本领」。
        </p>

        {error && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
            {error}
          </div>
        )}

        <div className="flex gap-3">
          {card(recommended)}
          {card(other)}
        </div>
        <p className="mt-3 text-xs text-slate-500">
          还没有账号？点进去顺手注册一个，免费。
        </p>

        {pending && (
          <form
            onSubmit={finishOAuth}
            className="mt-4 rounded-xl border border-slate-700 bg-slate-800/60 p-4"
          >
            <p className="text-sm text-slate-300">
              已在新标签页打开 {PLATFORM_LABEL[pending.provider]} 授权页。
              授权完成后若页面自动关闭即已登录；若浏览器停在无法访问的地址，
              把地址栏完整网址粘贴到这里：
            </p>
            <input
              type="text"
              value={pasted}
              onChange={(e) => setPasted(e.target.value)}
              placeholder="粘贴授权后地址栏的完整网址或授权码"
              className="mt-3 w-full rounded-xl border border-slate-700 bg-slate-800 px-4 py-2.5 text-sm text-white outline-none transition focus:border-emerald-500"
              autoFocus
            />
            {oauthError && (
              <div className="mt-2 text-xs text-red-400">{oauthError}</div>
            )}
            <button
              type="submit"
              disabled={!pasted.trim() || busyProvider !== null}
              className="mt-3 w-full rounded-xl bg-emerald-600 py-2.5 text-sm font-medium text-white transition hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {busyProvider ? '完成授权中…' : '完成登录'}
            </button>
          </form>
        )}

        <p className="mt-4 text-xs leading-5 text-slate-500">
          授权令牌只写入你自己集群的安全网关，不会上传给任何第三方。你可随时一键断开。
        </p>

        <div className="mt-5 border-t border-slate-800 pt-4">
          <button
            type="button"
            onClick={() => setShowOther((v) => !v)}
            className="text-xs text-slate-400 transition hover:text-slate-200"
          >
            {showOther ? '收起其他方式 ▴' : '其他方式（访问令牌 / 密码，离线场景）▾'}
          </button>

          {showOther && (
            <div className="mt-4 space-y-5">
              <form onSubmit={submitPat}>
                <div className="mb-2 flex gap-2">
                  {(['gitee', 'github'] as const).map((p) => (
                    <button
                      key={p}
                      type="button"
                      onClick={() => setPatProvider(p)}
                      className={`rounded-lg px-3 py-1 text-xs transition ${
                        patProvider === p
                          ? 'bg-slate-700 text-white'
                          : 'bg-slate-800 text-slate-400 hover:text-slate-200'
                      }`}
                    >
                      {PLATFORM_LABEL[p]}
                    </button>
                  ))}
                </div>
                <input
                  type="password"
                  value={pat}
                  onChange={(e) => setPat(e.target.value)}
                  placeholder={`粘贴 ${PLATFORM_LABEL[patProvider]} 访问令牌（PAT）`}
                  className="w-full rounded-xl border border-slate-700 bg-slate-800 px-4 py-2.5 text-sm text-white outline-none transition focus:border-blue-500"
                />
                {patError && (
                  <div className="mt-2 text-xs text-red-400">{patError}</div>
                )}
                <button
                  type="submit"
                  disabled={!pat.trim() || isLoading}
                  className="mt-2 w-full rounded-xl bg-slate-700 py-2.5 text-sm font-medium text-white transition hover:bg-slate-600 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  用令牌登录
                </button>
              </form>

              <form onSubmit={submitPassword} className="space-y-3">
                <div className="text-xs text-slate-500">
                  本地管理员密码（演示 / 离线环境）
                </div>
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="w-full rounded-xl border border-slate-700 bg-slate-800 px-4 py-2.5 text-sm text-white outline-none transition focus:border-blue-500"
                  placeholder="用户名"
                />
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full rounded-xl border border-slate-700 bg-slate-800 px-4 py-2.5 text-sm text-white outline-none transition focus:border-blue-500"
                  placeholder="密码"
                />
                <button
                  type="submit"
                  disabled={isLoading || !username.trim() || !password.trim()}
                  className="w-full rounded-xl bg-slate-700 py-2.5 text-sm font-medium text-white transition hover:bg-slate-600 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {isLoading ? '登录中…' : '密码登录'}
                </button>
              </form>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
