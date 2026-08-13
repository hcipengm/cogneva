import { useEffect, useRef, useState } from 'react';
import {
  getLlmStatus,
  saveLlmConfig,
  type LlmBackend,
} from '@/api/client';

interface Preset {
  label: string;
  base_url: string;
  model: string;
  /** 协议面首猜：保存时后端用真 key 做双协议实证探测，猜错会被纠正 */
  api_style: string;
}

const PRESETS: Preset[] = [
  { label: '自定义', base_url: '', model: '', api_style: 'openai' },
  {
    label: 'Kimi（月之暗面）',
    base_url: 'https://api.kimi.com/coding/v1',
    model: 'kimi-for-coding',
    api_style: 'anthropic',
  },
  {
    label: 'DeepSeek',
    base_url: 'https://api.deepseek.com/v1',
    model: 'deepseek-chat',
    api_style: 'openai',
  },
  {
    label: 'OpenAI',
    base_url: 'https://api.openai.com/v1',
    model: 'gpt-4o',
    api_style: 'openai',
  },
  {
    label: 'Anthropic',
    base_url: 'https://api.anthropic.com',
    model: 'claude-sonnet-4-6',
    api_style: 'anthropic',
  },
  {
    label: 'Qwen（通义千问）',
    base_url: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    model: 'qwen3-max',
    api_style: 'openai',
  },
  {
    label: '智谱 GLM',
    base_url: 'https://open.bigmodel.cn/api/paas/v4',
    model: 'glm-4.6',
    api_style: 'openai',
  },
  {
    label: 'MiniMax',
    base_url: 'https://api.minimaxi.com/v1',
    model: 'MiniMax-M2',
    api_style: 'openai',
  },
  {
    label: '豆包（火山方舟）',
    base_url: 'https://ark.cn-beijing.volces.com/api/v3',
    model: 'doubao-seed-2.0-code',
    api_style: 'openai',
  },
  {
    label: 'Ollama（本地）',
    base_url: 'http://host.docker.internal:11434/v1',
    model: 'qwen2.5-coder',
    api_style: 'openai',
  },
];

interface LlmSetupOverlayProps {
  initial?: LlmBackend;
  /** 首启向导模式：未配置完成前不允许关闭 */
  mandatory: boolean;
  onConfigured: () => void;
  onClose?: () => void;
}

type Phase = 'form' | 'saving' | 'waiting';

const POLL_INTERVAL_MS = 5000;
const POLL_TIMEOUT_MS = 3 * 60 * 1000;

export function LlmSetupOverlay({
  initial,
  mandatory,
  onConfigured,
  onClose,
}: LlmSetupOverlayProps) {
  const [presetIdx, setPresetIdx] = useState(0);
  const [baseUrl, setBaseUrl] = useState(initial?.base_url ?? '');
  const [model, setModel] = useState(initial?.model ?? '');
  const [styleHint, setStyleHint] = useState('openai');
  const [apiKey, setApiKey] = useState('');
  const [phase, setPhase] = useState<Phase>('form');
  const [error, setError] = useState<string | null>(null);
  const [verifyFailed, setVerifyFailed] = useState(false);
  const [timedOut, setTimedOut] = useState(false);
  const deadlineRef = useRef(0);

  // 保存成功后轮询等待滚动重启完成（期间本 Pod 可能短暂不可达，属正常）
  useEffect(() => {
    if (phase !== 'waiting') return;
    deadlineRef.current = Date.now() + POLL_TIMEOUT_MS;
    const timer = setInterval(async () => {
      try {
        const s = await getLlmStatus();
        if (s.configured) {
          clearInterval(timer);
          onConfigured();
          return;
        }
      } catch {
        // Pod 重启中，继续等
      }
      if (Date.now() > deadlineRef.current) {
        clearInterval(timer);
        setTimedOut(true);
      }
    }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [phase, onConfigured]);

  const applyPreset = (idx: number) => {
    setPresetIdx(idx);
    const p = PRESETS[idx];
    setStyleHint(p.api_style);
    if (p.base_url) {
      setBaseUrl(p.base_url);
      setModel(p.model);
    }
  };

  const doSave = async (skipVerify: boolean) => {
    await saveLlmConfig({
      base_url: baseUrl.trim(),
      model: model.trim(),
      api_key: apiKey.trim(),
      api_style: styleHint,
      skip_verify: skipVerify,
    });
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setVerifyFailed(false);
    setPhase('saving');
    try {
      await doSave(false);
      setPhase('waiting');
    } catch (err) {
      const apiErr = err as { status?: number; message?: string };
      let code = '';
      let msg = apiErr.message ?? '保存失败';
      try {
        const parsed = JSON.parse(msg) as { error?: string; message?: string };
        code = parsed.error ?? '';
        msg = parsed.message ?? msg;
      } catch {
        // 非 JSON 错误体，原样展示
      }
      setError(msg);
      setVerifyFailed(code === 'llm_verify_failed');
      setPhase('form');
    }
  };

  const forceSave = async () => {
    setError(null);
    setVerifyFailed(false);
    setPhase('saving');
    try {
      await doSave(true);
      setPhase('waiting');
    } catch (err) {
      const msg = (err as { message?: string }).message ?? '保存失败';
      setError(msg);
      setPhase('form');
    }
  };

  const recheck = async () => {
    setTimedOut(false);
    try {
      const s = await getLlmStatus();
      if (s.configured) {
        onConfigured();
        return;
      }
    } catch {
      // ignore
    }
    setPhase('waiting');
  };

  const inputCls =
    'w-full rounded-xl border border-slate-700 bg-slate-800 px-4 py-3 text-white outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-500';

  if (phase === 'waiting') {
    return (
      <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 backdrop-blur-sm">
        <div className="w-full max-w-md rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 text-center shadow-2xl">
          {timedOut ? (
            <>
              <h1 className="mb-2 text-xl font-semibold text-white">
                仍在等待生效
              </h1>
              <p className="mb-6 text-sm text-slate-400">
                配置已保存，但系统重启耗时超出预期。请稍后重新检测。
              </p>
              <button
                onClick={recheck}
                className="w-full rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 py-3 font-medium text-white transition hover:from-blue-500 hover:to-indigo-500"
              >
                重新检测
              </button>
            </>
          ) : (
            <>
              <div className="mx-auto mb-4 h-10 w-10 animate-spin rounded-full border-2 border-slate-600 border-t-blue-500" />
              <h1 className="mb-2 text-xl font-semibold text-white">
                配置已保存，正在生效
              </h1>
              <p className="text-sm text-slate-400">
                系统正在滚动重启相关组件，期间页面可能短暂断开，属正常现象。
              </p>
            </>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 backdrop-blur-sm">
      <form
        onSubmit={submit}
        className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl"
      >
        <div className="mb-1 flex items-start justify-between">
          <h1 className="text-2xl font-semibold text-white">LLM 设置</h1>
          {!mandatory && onClose && (
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg px-2 py-1 text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              ✕
            </button>
          )}
        </div>
        <p className="mb-6 text-sm text-slate-400">
          {mandatory
            ? '系统尚未连接 LLM，自治与进化能力不可用。请填写模型服务信息完成接入。'
            : '修改后系统将滚动重启相关组件，约一分钟后生效。'}
        </p>

        {error && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
            {error}
          </div>
        )}

        <div className="space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-300">
              服务商预设
            </label>
            <select
              value={presetIdx}
              onChange={(e) => applyPreset(Number(e.target.value))}
              className={inputCls}
            >
              {PRESETS.map((p, i) => (
                <option key={p.label} value={i}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-300">
              Base URL
            </label>
            <input
              type="text"
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
              className={inputCls}
              placeholder="https://api.example.com/v1"
              required
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-300">
              模型
            </label>
            <input
              type="text"
              value={model}
              onChange={(e) => setModel(e.target.value)}
              className={inputCls}
              placeholder="例如 kimi-for-coding"
              required
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-300">
              API Key
            </label>
            <input
              type="password"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              className={inputCls}
              placeholder="sk-..."
              required
              autoFocus={mandatory}
            />
            <p className="mt-1 text-xs text-slate-500">
              密钥只写入集群 Secret，不会回显到界面。API 风格保存时自动探测。
            </p>
          </div>
          <button
            type="submit"
            disabled={
              phase === 'saving' ||
              !baseUrl.trim() ||
              !model.trim() ||
              !apiKey.trim()
            }
            className="w-full rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 py-3 font-medium text-white shadow-lg transition hover:from-blue-500 hover:to-indigo-500 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {phase === 'saving' ? '保存中...' : '保存并生效'}
          </button>
          {verifyFailed && (
            <button
              type="button"
              onClick={forceSave}
              className="w-full rounded-xl border border-amber-600/50 py-3 font-medium text-amber-400 transition hover:bg-amber-600/10"
            >
              仍然保存（跳过连通验证）
            </button>
          )}
        </div>
      </form>
    </div>
  );
}
