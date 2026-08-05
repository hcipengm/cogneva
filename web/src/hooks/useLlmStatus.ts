import { useCallback, useEffect, useState } from 'react';
import { getLlmStatus, type LlmStatus } from '@/api/client';

export function useLlmStatus(enabled: boolean) {
  const [status, setStatus] = useState<LlmStatus | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const s = await getLlmStatus();
      setStatus(s);
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

  return { status, loading, refresh };
}
