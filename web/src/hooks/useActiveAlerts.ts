import { useCallback, useEffect, useState } from 'react';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { acknowledgeAlert, getActiveAlerts, type ActiveAlerts } from '@/api/client';

/** 一次确认的结局。界面要把它写进输出格，而不是只留一个可能看不见的横幅。 */
export interface AckOutcome {
  ok: boolean;
  message?: string;
}

/**
 * 读一次活跃告警。
 *
 * 失败与「没有告警」必须分开：网关在告警源未配置时回 503，读失败时列表
 * 只剩进程内那一半。把错误吞掉，面板会显示成「一切正常」，而它恰恰是
 * 出问题时才有人打开的那一面。
 */
export function useActiveAlerts(enabled: boolean, pollMs = 30_000) {
  const [alerts, setAlerts] = useState<ActiveAlerts | null>(null);
  /** 最近一次读成功的时刻。界面上的数因此能说清「这是什么时候的那个数」。 */
  const [at, setAt] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [ackError, setAckError] = useState<string | null>(null);
  const [pendingAck, setPendingAck] = useState<Record<string, true>>({});

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setAlerts(await getActiveAlerts());
      setAt(new Date().toISOString());
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
  const acknowledge = useCallback(async (id: string): Promise<AckOutcome> => {
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
      return { ok: true };
    } catch (e) {
      const err = e as { status?: number; message?: string };
      const message =
        err?.status === 404
          ? '这条已经不在 firing 了（可能刚清掉），刷新一下列表。'
          : `确认没写进去（HTTP ${err?.status ?? '?'}）——它仍然是未确认。`;
      setAckError(message);
      return { ok: false, message };
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

  // 越线是总线上的一帧（`resource_alert`），确认是 alerts 前缀下的一次写。两种都
  // 让这张表该重读——只靠 30 秒那一拍，越线之后面板要等半分钟才红。
  useKindTickedRefresh(['alerts', resourceKey('alerts')], refresh);

  return {
    alerts,
    loading,
    error,
    ackError,
    pendingAck,
    acknowledge,
    refresh,
    /** 这一份读数是谁给的：读的是哪个端点。 */
    what: '活跃告警',
    /** 最近一次读成功的时刻；没成功过就是 null，那几格不点开。 */
    at,
  };
}
