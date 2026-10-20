import { useCallback, useEffect, useState } from 'react';
import { getActiveAlerts, type ActiveAlerts } from '@/api/client';

/**
 * 读一次活跃告警。
 *
 * 失败与「没有告警」必须分开：网关在告警源未配置时回 503，读失败时列表
 * 只剩进程内那一半。把错误吞掉，面板会显示成「一切正常」，而它恰恰是
 * 出问题时才有人打开的那一面。
 */
export function useActiveAlerts(enabled: boolean, pollMs = 30_000) {
  const [alerts, setAlerts] = useState<ActiveAlerts | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setAlerts(await getActiveAlerts());
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setAlerts(null);
      setError(err?.message?.trim() || `读不到告警（HTTP ${err?.status ?? '?'}）`);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!enabled) return;
    refresh();
    const timer = window.setInterval(refresh, pollMs);
    return () => window.clearInterval(timer);
  }, [enabled, refresh, pollMs]);

  return { alerts, loading, error, refresh };
}
