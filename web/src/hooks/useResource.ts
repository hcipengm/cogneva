import { useCallback, useEffect, useRef, useState } from 'react';

/** 一次定时读取的当前读数。三件事分开给：有数据、读失败、还没读到过。 */
export interface Resource<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refresh: () => void;
  /**
   * 这份读数叫什么、最近一次读回的**本地时刻**、按多少毫秒重读。
   *
   * 数字的来路由读数自己带出来：界面上一格「17」读者要知道的是「几点的 17」。
   * 读失败时 `at` 不清空——它说的是最后一次真的读回来的那一刻，清空会让失败
   * 看起来像「从没读过」。
   */
  what: string;
  at: string | null;
  pollMs: number;
}

/**
 * 读一份远端数据，按固定间隔重读。
 *
 * 返回值把「还没有过第一份数据」与「读失败」分开：前者是加载中，后者要连同错误
 * 一起显示——把失败画成空列表，读者看到的会是「一切正常」。
 */
export function useResource<T>(
  what: string,
  load: () => Promise<T>,
  pollMs = 30_000,
  enabled = true
): Resource<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [at, setAt] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  // 回调放在 ref 里：调用方常写成内联箭头函数，进依赖会让每次渲染都重开定时器。
  const loadRef = useRef(load);
  loadRef.current = load;

  const refresh = useCallback(() => setRevision((r) => r + 1), []);

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;

    const run = async () => {
      setLoading(true);
      try {
        const value = await loadRef.current();
        if (!cancelled) {
          setData(value);
          setError(null);
          setAt(new Date().toISOString());
        }
      } catch (e) {
        const err = e as { status?: number; message?: string };
        if (!cancelled) {
          setError(err?.message?.trim() || `读不到${what}（HTTP ${err?.status ?? '?'}）`);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void run();
    const timer = window.setInterval(() => void run(), pollMs);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [pollMs, enabled, what, revision]);

  return { data, error, loading, refresh, what, at, pollMs };
}
