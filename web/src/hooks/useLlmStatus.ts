import { useCallback, useEffect, useState } from 'react';
import { getLlmStatus, type LlmStatus } from '@/api/client';

export function useLlmStatus(enabled: boolean) {
  const [status, setStatus] = useState<LlmStatus | null>(null);
  /** 最近一次读成功的时刻；读失败时保持旧值，这一格就不动。 */
  const [at, setAt] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const s = await getLlmStatus();
      setStatus(s);
      setAt(new Date().toISOString());
    } catch {
      // 网络闪断（如保存配置后本 Pod 正在滚动重启）不致命，保持旧状态
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (enabled) {
      setLoading(true);
      refresh();
    }
  }, [enabled, refresh]);

  return {
    status,
    loading,
    refresh,
    /** 这一份读数是谁给的：网关问上游得到的那一份。 */
    what: '模型状态',
    /** 最近一次读成功的时刻；没成功过就是 null。 */
    at,
  };
}
