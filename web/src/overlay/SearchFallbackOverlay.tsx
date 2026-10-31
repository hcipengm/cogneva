import { useCallback, useEffect, useState } from 'react';
import {
  getSearchFallback,
  saveSearchFallback,
  type SearchFallbackEntryPayload,
  type SearchFallbackStatus,
  type SearchUpstream,
} from '@/api/client';

import { Note } from '@/ui/Note';
/**
 * 上网检索与抓取的设置：本机两层之后还有谁可以问。
 *
 * 顺序就是优先级，而且这个顺序不是这一页定的——它是三层结构的固定部分：
 * 自建 SearXNG 检索、自建 crawl4ai 抓取先答，答不上才轮到这一页配的池；
 * 池也全答不上，才用**现网正在用的那条 LLM** 自带的上网工具。所以页面分三段，
 * 前两段只读，第三段才是这里要填的。
 *
 * 凭证从不回显：读回来的条目只带「有没有 key」，输入框留空就是保持原值。
 * 这不是省事——把 key 画回页面就等于它在浏览器、日志与截图里各多一份副本，
 * 而副本在下一次轮换后变成 401，读起来像「这家挂了」。
 */

interface Preset {
  shape: string;
  label: string;
  base_url: string;
  /** 这一家能不能抓取；不能的只做检索，那一条不该让人以为它接抓取 */
  capability: string;
  note: string;
}

const PRESETS: Preset[] = [
  {
    shape: 'exa',
    label: 'Exa',
    base_url: 'https://api.exa.ai',
    capability: 'both',
    note: '免费额度约每月 2,500 次检索；抓取另计',
  },
  {
    shape: 'tavily',
    label: 'Tavily',
    base_url: 'https://api.tavily.com',
    capability: 'both',
    note: '免费额度每月 1,000 次',
  },
  {
    shape: 'qianfan',
    label: '百度千帆',
    base_url: 'https://qianfan.baidubce.com/v2',
    capability: 'search',
    note: '只做检索，没有抓取面',
  },
];

interface Row {
  /** React 列表键，与提交里的 `id` 无关：改名不该让整行重挂 */
  key: number;
  id: string;
  shape: string;
  baseUrl: string;
  /** 用户这次新填的 key；空串 = 没填 */
  apiKey: string;
  /** 服务器说这一条已经有 key */
  hasKey: boolean;
  /** 用户明确要求清掉已存的 key */
  clearKey: boolean;
  capability: string;
}

type Phase = 'loading' | 'form' | 'saving' | 'waiting';

const POLL_INTERVAL_MS = 5000;
const POLL_TIMEOUT_MS = 2 * 60 * 1000;

function presetOf(shape: string): Preset | undefined {
  return PRESETS.find((p) => p.shape === shape);
}

/**
 * 一份池的身份：顺序、各家、能力、有没有 key。
 *
 * 判「已生效」只能比这个，不能比条数——只改一个 key 或只挪一下顺序时条数不变，
 * 比条数会在第一拍就报「已生效」，而那时新 Pod 还没起来。
 */
function signature(entries: {
  id: string;
  shape: string;
  base_url: string;
  capability: string;
  has_key: boolean;
}[]): string {
  return entries
    .map((e) => `${e.id}|${e.shape}|${e.base_url}|${e.capability}|${e.has_key ? 1 : 0}`)
    .join(',');
}

/** 保存成功后应当读回来的那份池。 */
function intendedSignature(rows: Row[]): string {
  return signature(
    rows.map((row) => ({
      id: row.id.trim() || row.shape,
      shape: row.shape,
      base_url: row.baseUrl.trim(),
      capability: row.capability,
      has_key: row.clearKey ? false : row.apiKey.trim() ? true : row.hasKey,
    }))
  );
}

function rowFrom(entry: SearchUpstream, key: number): Row {
  return {
    key,
    id: entry.id,
    shape: entry.shape,
    baseUrl: entry.base_url,
    apiKey: '',
    hasKey: entry.has_key,
    clearKey: false,
    capability: entry.capability,
  };
}

export function SearchFallbackOverlay({ onClose }: { onClose: () => void }) {
  const [status, setStatus] = useState<SearchFallbackStatus | null>(null);
  const [rows, setRows] = useState<Row[]>([]);
  const [phase, setPhase] = useState<Phase>('loading');
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [nextKey, setNextKey] = useState(1);
  /** 等生效时比对的那份读数；`null` = 没有在等 */
  const [pending, setPending] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const s = await getSearchFallback();
      setStatus(s);
      const loaded = s.upstreams.map((entry, i) => rowFrom(entry, i));
      setRows(loaded);
      setNextKey(loaded.length);
      setPhase('form');
      if (s.read_error) setError(s.read_error);
    } catch (err) {
      setError((err as { message?: string }).message ?? '读取配置失败');
      setPhase('form');
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // 保存之后网关要滚动重启才读到新池：判据是「新 Pod 读回来的池变了」，
  // 不是保存请求返回 200——env 引用 Secret 条目的部署里，改内容本身不触发重启。
  useEffect(() => {
    if (phase !== 'waiting' || pending === null) return;
    const deadline = Date.now() + POLL_TIMEOUT_MS;
    const timer = setInterval(async () => {
      try {
        const s = await getSearchFallback();
        if (signature(s.upstreams) === pending) {
          clearInterval(timer);
          setStatus(s);
          setPending(null);
          setMessage('已生效');
          setPhase('form');
          void load();
          return;
        }
      } catch {
        // 网关正在滚动，继续等
      }
      if (Date.now() > deadline) {
        clearInterval(timer);
        setPending(null);
        setMessage('已保存，但新配置还没读回来；一分钟后可关闭本页重开确认');
        setPhase('form');
      }
    }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [phase, pending, load]);

  const update = (key: number, patch: Partial<Row>) => {
    setRows((prev) => prev.map((r) => (r.key === key ? { ...r, ...patch } : r)));
  };

  const addRow = (shape: string) => {
    const preset = presetOf(shape);
    const row: Row = {
      key: nextKey,
      id: shape,
      shape,
      baseUrl: preset?.base_url ?? '',
      apiKey: '',
      hasKey: false,
      clearKey: false,
      capability: preset?.capability ?? 'search',
    };
    setNextKey((n) => n + 1);
    setRows((prev) => [...prev, row]);
  };

  const move = (key: number, delta: number) => {
    setRows((prev) => {
      const at = prev.findIndex((r) => r.key === key);
      const to = at + delta;
      if (at < 0 || to < 0 || to >= prev.length) return prev;
      const next = [...prev];
      const [item] = next.splice(at, 1);
      next.splice(to, 0, item);
      return next;
    });
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setMessage(null);
    const payload: SearchFallbackEntryPayload[] = rows.map((row) => ({
      id: row.id.trim() || row.shape,
      shape: row.shape,
      base_url: row.baseUrl.trim(),
      // 三态：明确清除 -> 空串；这次没填 -> 省略（后端保留原来那把）；
      // 填了 -> 新值。
      api_key: row.clearKey ? '' : row.apiKey.trim() ? row.apiKey.trim() : undefined,
      capability: row.capability,
    }));
    setPhase('saving');
    try {
      const result = await saveSearchFallback(payload);
      setMessage(result.message);
      setPending(intendedSignature(rows));
      setPhase('waiting');
    } catch (err) {
      const raw = (err as { message?: string }).message ?? '保存失败';
      let shown = raw;
      try {
        const parsed = JSON.parse(raw) as { message?: string };
        shown = parsed.message ?? raw;
      } catch {
        // 非 JSON 错误体，原样展示
      }
      setError(shown);
      setPhase('form');
    }
  };

  const inputCls =
    'w-full rounded-xl border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-white outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-500';
  const smallBtn =
    'rounded-lg border border-slate-700 px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200 disabled:opacity-40';

  const fromLlm = status?.from_llm ?? [];

  /**
   * 能点出来的那几家，由服务器说了算。
   *
   * `PRESETS` 只是这几家的展示信息（名字、默认地址、能力、说明），**不是**「能配哪
   * 几种形状」的声明——那张表在服务端，写面收什么由它决定。在这里按自己那份名单
   * 画按钮的话，两边会各自自报成功一次：页面上点得出来、保存被拒；或者服务端加了
   * 一种形状、页面上永远点不到。读回来的 `shapes` 有就用它，没有（老网关或读取
   * 失败）才退回本地那三家，免得页面整个空掉。
   */
  const offered = (
    status?.shapes?.length ? status.shapes : PRESETS.map((p) => p.shape)
  ).map(
    (shape) =>
      presetOf(shape) ?? { shape, label: shape, base_url: '', capability: 'search', note: '' }
  );

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 backdrop-blur-sm">
      <form
        onSubmit={submit}
        className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl"
      >
        <div className="mb-1 flex items-start justify-between">
          <h1 className="text-2xl font-semibold text-white">上网检索与抓取</h1>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg px-2 py-1 text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
          >
            ✕
          </button>
        </div>
        <p className="mb-6 text-sm text-slate-400">
          问网页的顺序是固定的：先用本机自建的两层，再按这里的顺序问第三方，全都答不上
          才用现网正在使用的 LLM 自带的上网工具。这里的空白表示「这一层就是没有」，
          不是「还没配好」。
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
          <h2 className="mb-2 text-sm font-medium text-slate-300">本机自建（最先问）</h2>
          <ul className="space-y-1 text-xs text-slate-400">
            <li>检索：SearXNG，集群内自建，不需要账号</li>
            <li>抓取：crawl4ai，集群内自建，只读公网地址</li>
          </ul>
        </section>

        <section className="mb-6">
          <div className="mb-2 flex items-center justify-between">
            <h2 className="text-sm font-medium text-slate-300">第三方后备（按顺序问）</h2>
            <div className="flex gap-2">
              {offered.map((p) => (
                <Note
                  as="button"
                  key={p.shape}
                  type="button"
                  onClick={() => addRow(p.shape)}
                  className={smallBtn}
                  text={p.note}
                >
                  + {p.label}
                </Note>
              ))}
            </div>
          </div>

          {phase === 'loading' ? (
            <p className="text-sm text-slate-500">读取中…</p>
          ) : rows.length === 0 ? (
            <p className="rounded-xl border border-dashed border-slate-700 p-4 text-sm text-slate-500">
              还没有第三方后备。上面每加一家就是一层：免费额度用完时会按这里的顺序问下一家。
            </p>
          ) : (
            <ol className="space-y-3">
              {rows.map((row, i) => {
                const preset = presetOf(row.shape);
                return (
                  <li
                    key={row.key}
                    className="rounded-xl border border-slate-700/60 bg-slate-950/40 p-4"
                  >
                    <div className="mb-3 flex items-center justify-between">
                      <span className="text-sm font-medium text-white">
                        {i + 1}. {preset?.label ?? row.shape}
                        {row.hasKey && (
                          <span className="ml-2 rounded bg-emerald-500/10 px-1.5 py-0.5 text-xs text-emerald-400">
                            已有密钥
                          </span>
                        )}
                        {!row.hasKey && (
                          <span className="ml-2 rounded bg-amber-500/10 px-1.5 py-0.5 text-xs text-amber-400">
                            未配密钥
                          </span>
                        )}
                      </span>
                      <span className="flex gap-1">
                        <button
                          type="button"
                          onClick={() => move(row.key, -1)}
                          disabled={i === 0}
                          className={smallBtn}
                        >
                          上移
                        </button>
                        <button
                          type="button"
                          onClick={() => move(row.key, 1)}
                          disabled={i === rows.length - 1}
                          className={smallBtn}
                        >
                          下移
                        </button>
                        <button
                          type="button"
                          onClick={() =>
                            setRows((prev) => prev.filter((r) => r.key !== row.key))
                          }
                          className={smallBtn}
                        >
                          移除
                        </button>
                      </span>
                    </div>

                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <label className="block">
                        <span className="mb-1 block text-xs text-slate-400">名字（source 里显示）</span>
                        <input
                          type="text"
                          value={row.id}
                          onChange={(e) => update(row.key, { id: e.target.value })}
                          className={inputCls}
                          placeholder={row.shape}
                        />
                      </label>
                      <label className="block">
                        <span className="mb-1 block text-xs text-slate-400">地址</span>
                        <input
                          type="text"
                          value={row.baseUrl}
                          onChange={(e) => update(row.key, { baseUrl: e.target.value })}
                          className={inputCls}
                          required
                        />
                      </label>
                      <label className="block">
                        <span className="mb-1 block text-xs text-slate-400">API Key</span>
                        <input
                          type="password"
                          value={row.apiKey}
                          onChange={(e) =>
                            update(row.key, { apiKey: e.target.value, clearKey: false })
                          }
                          className={inputCls}
                          placeholder={row.hasKey ? '已配置；留空保持不变' : '留空 = 不接凭证'}
                          autoComplete="off"
                        />
                      </label>
                      <label className="block">
                        <span className="mb-1 block text-xs text-slate-400">用来做什么</span>
                        <select
                          value={row.capability}
                          onChange={(e) => update(row.key, { capability: e.target.value })}
                          className={inputCls}
                        >
                          <option value="search">只检索</option>
                          <option value="fetch">只抓取</option>
                          <option value="both">检索与抓取</option>
                        </select>
                      </label>
                    </div>
                    {row.hasKey && (
                      <label className="mt-3 flex items-center gap-2 text-xs text-slate-400">
                        <input
                          type="checkbox"
                          checked={row.clearKey}
                          onChange={(e) => update(row.key, { clearKey: e.target.checked, apiKey: '' })}
                        />
                        清除已存的密钥（改成不带凭证）
                      </label>
                    )}
                    {preset?.note && (
                      <p className="mt-2 text-xs text-slate-500">{preset.note}</p>
                    )}
                  </li>
                );
              })}
            </ol>
          )}
        </section>

        <section className="mb-6 rounded-xl border border-slate-700/60 bg-slate-950/40 p-4">
          <h2 className="mb-2 text-sm font-medium text-slate-300">
            现网在用的 LLM（最后问）
          </h2>
          {fromLlm.length === 0 ? (
            <p className="text-xs text-slate-500">
              当前 LLM 上游没有自带的上网工具面，这一层是空的。换成带有上网工具的
              模型（DeepSeek、Kimi 的服务端都提供）后，这一层自动出现。注意包月订阅型
              的入口（例如 Kimi 的 coding 主机）只有对话面，独立检索接口不在它上面、
              也不接受那把订阅密钥——这种情况这一层会一直是空的，只能换成平台密钥。
            </p>
          ) : (
            <ul className="space-y-1 text-xs text-slate-400">
              {fromLlm.map((entry) => (
                <li key={entry.id}>
                  {entry.id} · {entry.capability === 'both' ? '检索与抓取' : entry.capability}
                  <span className="ml-2 text-slate-500">{entry.base_url}</span>
                </li>
              ))}
              <li className="pt-1 text-slate-500">
                这一层用的就是上面模型服务那把 key，跟着它一起轮换，所以不在这里配。
              </li>
            </ul>
          )}
        </section>

        <button
          type="submit"
          disabled={phase === 'saving' || phase === 'waiting' || phase === 'loading'}
          className="w-full rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 py-3 font-medium text-white shadow-lg transition hover:from-blue-500 hover:to-indigo-500 disabled:cursor-not-allowed disabled:opacity-50"
        >
          {phase === 'saving'
            ? '保存中...'
            : phase === 'waiting'
              ? '等待生效...'
              : '保存并生效'}
        </button>
      </form>
    </div>
  );
}
