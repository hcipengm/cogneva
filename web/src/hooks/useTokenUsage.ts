import { useCallback, useEffect, useState } from 'react';
import { getTokenUsage, type TokenUsage } from '@/api/client';

/**
 * 读一次按 actor 的令牌花销。
 *
 * 与其它读面不同，失败**不留旧值也不吞掉**：这个读面答不出与答「零花销」
 * 长得一样（网关未配置读侧时回 500，未折叠的区间回空列表），把错误藏起来
 * 就会把「没读到」显示成「没花钱」。
 */
export function useTokenUsage(hours: number, enabled: boolean) {
  const [usage, setUsage] = useState<TokenUsage | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setUsage(await getTokenUsage(hours));
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setUsage(null);
      setError(err?.message?.trim() || `读不到花销（HTTP ${err?.status ?? '?'}）`);
    } finally {
      setLoading(false);
    }
  }, [hours]);

  useEffect(() => {
    if (enabled) {
      refresh();
    }
  }, [enabled, refresh]);

  return { usage, loading, error, refresh };
}
