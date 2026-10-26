import { useEffect, useRef } from 'react';
import { planRefresh } from '@/store/frameTick';
import { useFrameTickStore } from '@/store/frameTickStore';

/**
 * 「信号来了就重读这一面」——把读快照的那几面接上帧的唯一调度。
 *
 * 信号是「有东西变了」的一个计数或版本号：全局那条（`useStreamDrivenRefresh`）由
 * 总线置位，项目面那条（`projectRevision`）只数属于这个项目的帧。**什么时候读**由
 * 这里决定，读回来的是什么这里一概不碰：帧里没有基线、没有预算，拿帧去改服务端
 * 算出来的数就是两份账写成一本。定时轮询照旧留着当兜底——连接断了、帧丢了，读数
 * 仍然会靠它追上来，只是慢一拍。
 *
 * 传进来的 `refresh` 可以一次给几个：一屏上挂着的读数通常要一起重读，分两次读会
 * 让画面先变一半再变另一半。
 */
export function useFrameDrivenRefresh(
  signal: number | string | null | undefined,
  refresh: (() => void) | Array<() => void>
): void {
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
    if (!signal) return;
    const now = Date.now();
    const plan = planRefresh(now, lastRef.current, pending.current !== null);
    if (plan.action === 'now') {
      lastRef.current = now;
      for (const fn of toList(fns.current)) fn();
      return;
    }
    if (plan.action !== 'wait') return;
    // 这一发**不**登记清理函数：中途再来的信号不该把它往后推——推下去就回到「信号
    // 永不停、定时器永不到点」的那种饿死形状，忙的时候这一面反而最旧。组件卸载
    // 由上面那一支收。
    pending.current = window.setTimeout(() => {
      pending.current = null;
      lastRef.current = Date.now();
      for (const fn of toList(fns.current)) fn();
    }, plan.waitMs);
  }, [signal]);
}

/**
 * 总线说「平台上变了」就重读这一面。
 *
 * 这是全局那条信号：任何一面都可能有东西变了，各自重读自己的那几份。
 */
export function useStreamDrivenRefresh(
  refresh: (() => void) | Array<() => void>
): void {
  const tick = useFrameTickStore((s) => s.tick);
  useFrameDrivenRefresh(tick, refresh);
}

function toList(refresh: (() => void) | Array<() => void>): Array<() => void> {
  return Array.isArray(refresh) ? refresh : [refresh];
}
