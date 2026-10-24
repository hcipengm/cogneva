import { createContext, useContext, type ReactNode, type RefObject } from 'react';
import type { ClusterOverview } from '@/api/client';
import type { StreamClient } from '@/hooks/useEventStream';
import type { Resource } from '@/hooks/useResource';
import type { useActiveAlerts } from '@/hooks/useActiveAlerts';
import type { useLlmStatus } from '@/hooks/useLlmStatus';
import type { useNotifications } from '@/hooks/useNotifications';

/**
 * 工作台里跨面共用的那几份读数。
 *
 * 状态条、侧栏、问题面板、收件箱读的是同一批端点。每个面各自轮询一遍，
 * 同一秒里会问同一个端点四次，而四份读数还会在某一刻互相不一致——读者
 * 看到状态条说「0 条告警」、告警页说「1 条」，只能信一个。
 */
export interface ShellServices {
  username: string | null;
  /** 实时流客户端。只有工作台那一面需要发话，其余面只读。 */
  clientRef: RefObject<StreamClient | null>;
  overview: Resource<ClusterOverview>;
  alerts: ReturnType<typeof useActiveAlerts>;
  notifications: ReturnType<typeof useNotifications>;
  llm: ReturnType<typeof useLlmStatus>;
  overlays: ShellOverlays;
  signOut: () => void;
}

/**
 * 两个浮层的开关。
 *
 * 浮层本身由 Shell 画，开关放在 App：命令面板要能开它，而命令面板住在 Shell
 * 里——开关若是 Shell 自己的局部状态，命令面板就够不着它。
 */
export interface ShellOverlays {
  conversations: boolean;
  llmSetup: boolean;
  openConversations: () => void;
  closeConversations: () => void;
  openLlmSetup: () => void;
  closeLlmSetup: () => void;
}

const ShellServicesContext = createContext<ShellServices | null>(null);

export function ShellServicesProvider({
  services,
  children,
}: {
  services: ShellServices;
  children: ReactNode;
}) {
  return (
    <ShellServicesContext.Provider value={services}>{children}</ShellServicesContext.Provider>
  );
}

export function useServices(): ShellServices {
  const services = useContext(ShellServicesContext);
  if (!services) throw new Error('工作台组件挂在了 ShellServicesProvider 之外');
  return services;
}
