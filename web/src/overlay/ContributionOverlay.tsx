import { useCallback, useEffect, useState } from 'react';
import {
  connectContribution,
  disconnectContribution,
  exchangeGiteeOAuth,
  getContributionStatus,
  pollGithubDevice,
  startGiteeOAuth,
  startGithubDevice,
  type ContributionStatus,
  type DeviceStart,
  type GiteeOAuthStart,
} from '@/api/client';

/**
 * 代码平台与凭据：GitHub / Gitee 的令牌，以及网关的 SSH 身份。
 *
 * 三条路都能到同一个终点，页面按「平台自己让人怎么授权」排序：GitHub 走设备码
 * （显示一串码，人去浏览器里输入），Gitee 走授权码（打开授权页，把跳回来的地址
 * 粘回来），两条路都不可用时才退回粘贴 PAT。三条路都在网关侧换取并校验令牌，
 * 这一页从头到尾拿不到令牌本身，只拿得到「连上了、账号是谁、SSH 公钥传上去没有」。
 *
 * SSH 单独一块，因为它有自己的状态词：`ready` 是「密钥在、登记也确认了」，
 * `pending` 是「密钥在、但还没确认登记成功」——两件事的下一步不一样，合成一句
 * 「已配置」会让人对着不生效的身份继续推送。
 */

type Phase = 'loading' | 'idle' | 'device' | 'gitee' | 'pat';

function explain(err: unknown): string {
  const raw = (err as { message?: string }).message ?? String(err);
  try {
    const parsed = JSON.parse(raw) as { message?: string };
    return parsed.message ?? raw;
  } catch {
    return raw;
  }
}

const STATE_LABEL: Record<string, string> = {
  ready: '已就绪',
  pending: '未确认',
  absent: '没有密钥',
  unknown: '读不到',
};

export function ContributionOverlay({ onClose }: { onClose: () => void }) {
  const [status, setStatus] = useState<ContributionStatus | null>(null);
  const [phase, setPhase] = useState<Phase>('loading');
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [device, setDevice] = useState<DeviceStart | null>(null);
  const [gitee, setGitee] = useState<GiteeOAuthStart | null>(null);
  const [pasted, setPasted] = useState('');
  const [patProvider, setPatProvider] = useState<'github' | 'gitee'>('github');
  const [pat, setPat] = useState('');
  const [copied, setCopied] = useState(false);

  const load = useCallback(async () => {
    try {
      const s = await getContributionStatus();
      setStatus(s);
    } catch (err) {
      setError(explain(err));
    } finally {
      setPhase((p) => (p === 'loading' ? 'idle' : p));
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // 设备码要轮询到人点完为止：间隔由平台给，`slow_down` 时按它给的更慢的间隔退。
  // 停下只有三种：连上了、平台给了判词、组件被关掉——最后一种靠 cancelled 收尾。
  useEffect(() => {
    if (phase !== 'device' || !device) return;
    let cancelled = false;
    let timer: number | undefined;
    let wait = Math.max(5, device.interval) * 1000;
    const tick = async () => {
      try {
        const r = await pollGithubDevice(device.client_id, device.device_code);
        if (cancelled) return;
        if (r.status === 'connected') {
          setMessage(r.message ?? '已连接');
          setDevice(null);
          setPhase('idle');
          void load();
          return;
        }
        if (r.status === 'error') {
          setError(r.message);
          setDevice(null);
          setPhase('idle');
          return;
        }
        if (r.status === 'slow_down') wait = Math.max(wait, r.interval * 1000);
      } catch (err) {
        if (cancelled) return;
        setError(explain(err));
        setDevice(null);
        setPhase('idle');
        return;
      }
      timer = window.setTimeout(() => void tick(), wait);
    };
    timer = window.setTimeout(() => void tick(), wait);
    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
    };
  }, [phase, device, load]);

  const startGithub = async () => {
    setError(null);
    setMessage(null);
    setPhase('loading');
    try {
      const d = await startGithubDevice();
      setDevice(d);
      setPhase('device');
    } catch (err) {
      setError(explain(err));
      setPhase('idle');
    }
  };

  const startGitee = async () => {
    setError(null);
    setMessage(null);
    setPhase('loading');
    try {
      const g = await startGiteeOAuth(window.location.origin);
      setGitee(g);
      // 回调落在公开落地页上，它唯一能把 code 送回来的路是 `window.opener`：
      // 带 `noopener` 就等于把这条路封死，那一页只剩「把码显示出来让人复制」。
      // 同一个落地页在登录入口那边一直是自动回送的，两个入口走的是同一条链。
      window.open(g.authorize_url, 'cogneva-oauth', 'width=720,height=820');
      setPhase('gitee');
    } catch (err) {
      setError(explain(err));
      setPhase('idle');
    }
  };

  const completeGitee = useCallback(
    async (raw: string) => {
      if (!gitee) return;
      setError(null);
      setPhase('loading');
      try {
        const r = await exchangeGiteeOAuth(gitee.state, raw.trim());
        setMessage(r.message);
        setGitee(null);
        setPasted('');
        setPhase('idle');
        void load();
      } catch (err) {
        setError(explain(err));
        setPhase('gitee');
      }
    },
    [gitee, load]
  );

  // 落地页把 code `postMessage` 回打开它的窗口。它的 origin 不是我们的（那是一张
  // 公开页），所以凭证是发起时写进 state 的那串：服务端换码时按一次性消费它。
  // 少了这一支，用户点完授权还得多做一个复制粘贴——而弹窗打开时已经带着 opener。
  useEffect(() => {
    if (!gitee) return;
    const onMessage = (e: MessageEvent) => {
      const data = (e.data ?? {}) as { type?: string; code?: string; state?: string };
      if (data.type !== 'cogneva-gitee-oauth') return;
      if (!data.code || data.state !== gitee.state) return;
      void completeGitee(data.code);
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [gitee, completeGitee]);

  const finishGitee = async () => {
    await completeGitee(pasted);
  };

  const submitPat = async () => {
    setError(null);
    setPhase('loading');
    try {
      const r = await connectContribution(patProvider, pat.trim());
      setMessage(r.message);
      setPat('');
      setPhase('idle');
      void load();
    } catch (err) {
      setError(explain(err));
      setPhase('pat');
    }
  };

  const disconnect = async () => {
    if (!window.confirm('断开后令牌会从网关删除，已暂存的变更留在本机。继续？')) return;
    setError(null);
    setPhase('loading');
    try {
      const r = await disconnectContribution();
      setMessage(r.message);
      setPhase('idle');
      void load();
    } catch (err) {
      setError(explain(err));
      setPhase('idle');
    }
  };

  const copyKey = async () => {
    const key = status?.git_identity.public_key;
    if (!key) return;
    try {
      await navigator.clipboard.writeText(key);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      setError('浏览器拒绝了剪贴板写入；请手工选中公钥复制');
    }
  };

  const identity = status?.git_identity;
  const busy = phase === 'loading';
  const inputCls =
    'w-full rounded-xl border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-white outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-500';
  const smallBtn =
    'rounded-lg border border-slate-700 px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200 disabled:opacity-40';

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 p-4 backdrop-blur-sm">
      <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl">
        <div className="mb-1 flex items-start justify-between">
          <h1 className="text-2xl font-semibold text-white">代码平台与凭据</h1>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg px-2 py-1 text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
          >
            ✕
          </button>
        </div>
        <p className="mb-6 text-sm text-slate-400">
          令牌只进安全网关，主应用与沙盒一律不带凭证；这一页从头到尾读不到令牌本身，
          只看得到「连上了没有、账号是谁、SSH 公钥登记到了哪一步」。
        </p>

        {error && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">{error}</div>
        )}
        {message && (
          <div className="mb-4 rounded-lg bg-emerald-500/10 p-3 text-sm text-emerald-300">
            {message}
          </div>
        )}

        <section className="mb-6 rounded-xl border border-slate-700/60 bg-slate-950/40 p-4">
          <div className="mb-2 flex items-center justify-between">
            <h2 className="text-sm font-medium text-slate-300">代码平台</h2>
            <span className="text-xs text-slate-400">
              {status
                ? status.configured
                  ? `已连接 ${status.provider}${status.github_app_configured ? ' · 另配了 GitHub App' : ''}`
                  : '还没连接'
                : '读取中…'}
            </span>
          </div>

          {phase === 'device' && device ? (
            <div className="space-y-3">
              <p className="text-xs text-slate-400">
                在打开的页面里输入这串码，完成后本页会自动继续（每 {Math.max(5, device.interval)} 秒查一次）：
              </p>
              <div className="flex items-center gap-3">
                <code className="rounded-lg bg-slate-800 px-4 py-2 text-xl tracking-[0.3em] text-white">
                  {device.user_code}
                </code>
                <a
                  href={device.verification_uri}
                  target="_blank"
                  rel="noreferrer"
                  className="text-xs text-blue-400 underline"
                >
                  打开 {device.verification_uri}
                </a>
              </div>
              <p className="text-xs text-slate-500">
                这串码 {Math.round(device.expires_in / 60)} 分钟内有效。
              </p>
              <button
                type="button"
                onClick={() => {
                  setDevice(null);
                  setPhase('idle');
                }}
                className={smallBtn}
              >
                取消
              </button>
            </div>
          ) : phase === 'gitee' && gitee ? (
            <div className="space-y-3">
              <p className="text-xs text-slate-400">
                授权页已在另一个标签打开。授权后浏览器会跳到本网关的回调地址——把那一整条网址粘回来
                （粘授权码也行）：
              </p>
              <a
                href={gitee.authorize_url}
                target="_blank"
                rel="noreferrer"
                className="block break-all text-xs text-blue-400 underline"
              >
                {gitee.authorize_url}
              </a>
              <input
                type="text"
                value={pasted}
                onChange={(e) => setPasted(e.target.value)}
                placeholder={`${gitee.redirect_uri}?code=… 或直接粘 code`}
                className={inputCls}
                autoComplete="off"
              />
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => void finishGitee()}
                  disabled={!pasted.trim() || busy}
                  className={`${smallBtn} border-blue-600 bg-blue-600/20 px-3 py-1.5 text-blue-300 hover:bg-blue-600/30`}
                >
                  完成授权
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setGitee(null);
                    setPasted('');
                    setPhase('idle');
                  }}
                  className={smallBtn}
                >
                  取消
                </button>
              </div>
            </div>
          ) : phase === 'pat' ? (
            <div className="space-y-3">
              {/* 平台是一个选择，不是两个并列的按钮：按钮并排时看着像"两格输入"，
                  贴令牌的人会把 Gitee 的贴进 GitHub 那一格，而写入成功、连接失败的
                  读数与「令牌无效」同形。设置页上那一行早就是下拉了。 */}
              <select
                value={patProvider}
                onChange={(e) =>
                  setPatProvider(e.target.value === 'gitee' ? 'gitee' : 'github')
                }
                className={inputCls}
                aria-label="令牌属于哪个平台"
              >
                <option value="github">GitHub</option>
                <option value="gitee">Gitee</option>
              </select>
              <input
                type="password"
                value={pat}
                onChange={(e) => setPat(e.target.value)}
                placeholder="粘贴访问令牌（PAT）；它只发往网关，不会被读回"
                className={inputCls}
                autoComplete="off"
              />
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => void submitPat()}
                  disabled={!pat.trim() || busy}
                  className={`${smallBtn} border-blue-600 bg-blue-600/20 px-3 py-1.5 text-blue-300 hover:bg-blue-600/30`}
                >
                  连接
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setPat('');
                    setPhase('idle');
                  }}
                  className={smallBtn}
                >
                  取消
                </button>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => void startGithub()}
                  disabled={busy || !status?.device_flow_available}
                  title={
                    status && !status.device_flow_available
                      ? '网关没配 GitHub OAuth App 的 client_id，这条路不可用；可以用下面的粘贴令牌'
                      : undefined
                  }
                  className={smallBtn}
                >
                  用 GitHub 登录（设备码）
                </button>
                <button
                  type="button"
                  onClick={() => void startGitee()}
                  disabled={busy || !status?.gitee_oauth_available}
                  title={
                    status && !status.gitee_oauth_available
                      ? '网关没配齐 Gitee 应用的 client_id / client_secret，这条路不可用；可以用下面的粘贴令牌'
                      : undefined
                  }
                  className={smallBtn}
                >
                  用 Gitee 授权
                </button>
                <button
                  type="button"
                  onClick={() => setPhase('pat')}
                  disabled={busy}
                  className={smallBtn}
                >
                  粘贴令牌
                </button>
                {status?.configured && (
                  <button
                    type="button"
                    onClick={() => void disconnect()}
                    disabled={busy}
                    className={`${smallBtn} ml-auto border-red-800 text-red-400 hover:bg-red-950/40`}
                  >
                    断开连接
                  </button>
                )}
              </div>
              <p className="text-xs text-slate-500">
                两条授权通道能不能用，读的是网关自己的登记：GitHub 设备码
                {status ? (status.device_flow_available ? '可用' : '不可用（缺 client_id）') : '…'}
                ；Gitee 授权码
                {status ? (status.gitee_oauth_available ? '可用' : '不可用（缺 client_secret）') : '…'}
                。两条都不可用时，粘贴令牌是唯一的路。
              </p>
              {status && (
                <p className="text-xs text-slate-500">
                  待提交变更 {status.pending_count} 条 · 回流策略 {status.policy}
                </p>
              )}
            </div>
          )}
        </section>

        <section className="mb-6 rounded-xl border border-slate-700/60 bg-slate-950/40 p-4">
          <div className="mb-2 flex items-center justify-between">
            <h2 className="text-sm font-medium text-slate-300">SSH 身份</h2>
            <span className="text-xs text-slate-400">
              {identity ? `${STATE_LABEL[identity.state] ?? identity.state}` : '读取中…'}
            </span>
          </div>
          {identity?.public_key ? (
            <>
              <p className="mb-2 text-xs text-slate-400">
                公钥是可公开的材料，私钥留在网关。连接时它会自动上传到账户；上传失败不影响
                通过 API 与代理推送，只是 SSH 这条腿没通——判据是账户的公钥列表里有没有下面这个指纹。
              </p>
              <textarea
                readOnly
                value={identity.public_key}
                rows={3}
                className="w-full resize-none rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 font-mono text-[11px] text-slate-300"
              />
              <div className="mt-2 flex items-center gap-3">
                <button type="button" onClick={() => void copyKey()} className={smallBtn}>
                  {copied ? '已复制' : '复制公钥'}
                </button>
                {identity.fingerprint && (
                  <span className="font-mono text-[11px] text-slate-500">
                    {identity.fingerprint}
                  </span>
                )}
              </div>
            </>
          ) : (
            <p className="text-xs text-slate-500">
              {identity?.state === 'unknown'
                ? '这个进程读不到网关的 Secret，身份状态是未知，不是「没配」。'
                : '还没有密钥。连上代码平台时会自动生成一对，并把公钥登记到账户。'}
            </p>
          )}
          {identity?.note && <p className="mt-2 text-xs text-amber-400">{identity.note}</p>}
        </section>

        <button
          type="button"
          onClick={onClose}
          className="w-full rounded-xl bg-slate-800 py-3 text-sm text-slate-300 transition hover:bg-slate-700"
        >
          关闭
        </button>
      </div>
    </div>
  );
}
