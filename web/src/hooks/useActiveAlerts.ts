import { useCallback, useEffect, useState } from 'react';
import { acknowledgeAlert, getActiveAlerts, type ActiveAlerts } from '@/api/client';

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
  const [ackError, setAckError] = useState<string | null>(null);
  const [pendingAck, setPendingAck] = useState<Record<string, true>>({});

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

  /**
   * 确认一条告警。
   *
   * 不先自己填一个「已确认」：那会把谁确认的写成一个界面编出来的值。等服务端
   * 回来再写，写的才是库里的那一条。确认中只标 pending，那一行显示「确认中…」。
   *
   * 404 单独说：告警在列表与点击之间清掉了，服务端不写、也不该写——下次 firing
   * 复用同一行，旧标记会被读成「有人已经看过这一次」。
   */
  const acknowledge = useCallback(async (id: string) => {
    setPendingAck((p) => ({ ...p, [id]: true }));
    setAckError(null);
    try {
      const recorded = await acknowledgeAlert(id);
      setAlerts((prev) =>
        prev
          ? {
              ...prev,
              alerts: prev.alerts.map((a) =>
                a.source === 'durable' && a.id === id
                  ? {
                      ...a,
                      acknowledged_at: recorded.acknowledged_at,
                      acknowledged_by: recorded.acknowledged_by,
                    }
                  : a,
              ),
            }
          : prev,
      );
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setAckError(
        err?.status === 404
          ? '这条已经不在 firing 了（可能刚清掉），刷新一下列表。'
          : `确认没写进去（HTTP ${err?.status ?? '?'}）——它仍然是未确认。`,
      );
    } finally {
      setPendingAck((p) => {
        const next = { ...p };
        delete next[id];
        return next;
      });
    }
  }, []);

  useEffect(() => {
    if (!enabled) return;
    refresh();
    const timer = window.setInterval(refresh, pollMs);
    return () => window.clearInterval(timer);
  }, [enabled, refresh, pollMs]);

  return { alerts, loading, error, ackError, pendingAck, acknowledge, refresh };
}
