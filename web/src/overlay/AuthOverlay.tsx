import { useState } from 'react';

interface AuthOverlayProps {
  onLogin: (username: string, password: string) => void;
  error: string | null;
  isLoading: boolean;
}

export function AuthOverlay({ onLogin, error, isLoading }: AuthOverlayProps) {
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('admin');

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!username.trim() || !password.trim()) return;
    onLogin(username.trim(), password.trim());
  };

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 backdrop-blur-sm">
      <form
        onSubmit={submit}
        className="w-full max-w-md rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl"
      >
        <h1 className="mb-2 text-2xl font-semibold text-white">Cogneva</h1>
        <p className="mb-6 text-sm text-slate-400">登录以连接事件流网关</p>

        {error && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
            {error}
          </div>
        )}

        <div className="space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-300">
              用户名
            </label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full rounded-xl border border-slate-700 bg-slate-800 px-4 py-3 text-white outline-none ring-blue-500 transition focus:border-blue-500 focus:ring-2"
              placeholder="admin"
              autoFocus
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-300">
              密码
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full rounded-xl border border-slate-700 bg-slate-800 px-4 py-3 text-white outline-none ring-blue-500 transition focus:border-blue-500 focus:ring-2"
              placeholder="••••••••"
            />
          </div>
          <button
            type="submit"
            disabled={isLoading || !username.trim() || !password.trim()}
            className="w-full rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 py-3 font-medium text-white shadow-lg transition hover:from-blue-500 hover:to-indigo-500 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {isLoading ? '登录中...' : '登录'}
          </button>
        </div>
      </form>
    </div>
  );
}
