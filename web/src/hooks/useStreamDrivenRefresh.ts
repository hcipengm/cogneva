import { useEffect, useRef } from 'react';
import { planRefresh } from '@/store/frameTick';
import { useFrameTickStore } from '@/store/frameTickStore';

/**
 * 「总线上有东西变了就重读这一面」——把读快照的那几面接到帧上的唯一入口。
 *
 * 传进来的是读数的 `refresh`，可以一次传几个：一屏上挂着的读数通常要一起重读，
 * 分两次读会让画面先变一半再变另一半。
 *
 * 这里只决定**什么时候读**，不碰读回来的数：帧里没有基线、没有预算，拿帧去改
 * 服务端算出来的数就是两份账写成一本。定时轮询照旧留着当兜底——连接断了、帧丢了，
 * 读数仍然会靠它追上来，只是慢一拍。
 */
export function useStreamDrivenRefresh(
  refresh: (() => void) | Array<() => void>
): void {
  const tick = useFrameTickStore((s) => s.tick);
  // 回调放 ref 里：调用方多半写成内联数组，进依赖会让每一帧都重排一次定时器。
  const fns = useRef(refresh);
  fns.current = refresh;
  /** 上一次**真的读出去**的时刻。 */
  const lastRef = useRef<number | null>(null);
  /** 已经有一发排在路上了（定时器 id）；有它才数得清「合并」到底合了多少。 */
  const pending = useRef<number | null>(null);

  useEffect(
    () => () => {
      if (pending.current !== null) {
        window.clearTimeout(pending.current);
        pending.current = null;
      }
    },
    []
  );

  useEffect(() => {
    if (tick === 0) return;
    const now = Date.now();
    const plan = planRefresh(now, lastRef.current, pending.current !== null);
    if (plan.action === 'now') {
      lastRef.current = now;
      for (const fn of toList(fns.current)) fn();
      return;
    }
    if (plan.action !== 'wait') return;
    // 这一发**不**登记清理函数：中途再来的帧不该把它往后推——推下去就回到「帧
    // 永不停、定时器永不到点」的那种饿死形状，忙的时候这一面反而最旧。组件卸载
    // 由上面那一支收。
    pending.current = window.setTimeout(() => {
      pending.current = null;
      lastRef.current = Date.now();
      for (const fn of toList(fns.current)) fn();
    }, plan.waitMs);
  }, [tick]);
}

function toList(refresh: (() => void) | Array<() => void>): Array<() => void> {
  return Array.isArray(refresh) ? refresh : [refresh];
}
