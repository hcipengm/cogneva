/*
 * 三档断点：窄 / 中 / 宽。
 *
 * 断点是界面的，跨度数是数据的。这一层只回答「现在在哪一档」，每格横跨几列仍由
 * 后端在数据里声明（`span.narrow` / `mid` / `wide`），这样同一份读数在窄屏与宽屏
 * 上都是数据要的那个版式。
 *
 * 为什么不写成 CSS 自定义属性再交给媒体查询：全站的 `--x` 是配色令牌，每套主题
 * 必须逐个填满（`styles/themes.ts`）；把布局数字混进那组令牌，等于要求十八套配色
 * 各声明一遍网格跨度，缺一套就在那一套里悄悄继承上一个主题的数字。
 */
import { useEffect, useState } from 'react';

export type Breakpoint = 'narrow' | 'mid' | 'wide';

/** 断点的界只有这一处；与设计稿的 768 / 1280 同数。 */
const MID_QUERY = '(min-width: 768px)';
const WIDE_QUERY = '(min-width: 1280px)';

function reading(): Breakpoint {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return 'wide';
  }
  if (window.matchMedia(WIDE_QUERY).matches) return 'wide';
  if (window.matchMedia(MID_QUERY).matches) return 'mid';
  return 'narrow';
}

export function useBreakpoint(): Breakpoint {
  const [tier, setTier] = useState<Breakpoint>(reading);
  useEffect(() => {
    const queries = [MID_QUERY, WIDE_QUERY].map((query) => window.matchMedia(query));
    const update = () => setTier(reading());
    for (const query of queries) query.addEventListener('change', update);
    return () => {
      for (const query of queries) query.removeEventListener('change', update);
    };
  }, []);
  return tier;
}
