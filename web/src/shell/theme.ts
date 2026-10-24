import { create } from 'zustand';
import { THEME_STORAGE_KEY, applyTheme, bootTheme, themeById } from '@/styles/themes';

/**
 * 当前配色主题。
 *
 * 主题是这台浏览器里的选择，不是部署级的配置：同一个人白天用亮色、夜里用深色，
 * 另一个人用糖果色，彼此不该互相覆盖。所以它只落 `localStorage`，不进服务端，
 * 也不进集群。
 */
interface ThemeState {
  theme: string;
  setTheme: (id: string) => void;
}

export const useTheme = create<ThemeState>((set) => ({
  theme: bootTheme().id,
  setTheme: (id) => {
    const applied = applyTheme(id);
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, applied.id);
    } catch {
      // 存不下就只对这一屏生效：选中的颜色仍然看得见，只是下次要重选。
    }
    set({ theme: applied.id });
  },
}));

/** 给命令面板用：按注册表顺序换到下一个，绕一圈回到第一个。 */
export function nextThemeId(current: string, ids: string[]): string {
  const index = ids.indexOf(themeById(current).id);
  return ids[(index + 1) % ids.length];
}
