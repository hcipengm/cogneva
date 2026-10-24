import { useCallback, useEffect, useRef, useState } from 'react';

/** 一次定时读取的当前读数。三件事分开给：有数据、读失败、还没读到过。 */
export interface Resource<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refresh: () => void;
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

  return { data, error, loading, refresh };
}
