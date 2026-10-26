import { useEffect, useRef } from 'react';
import { planRefresh, ticksFor } from '@/store/frameTick';
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
 * 这是全局那条信号：任何一面都可能有东西变了，各自重读自己的那几份。精确到种类
 * 的那一版是 `useKindTickedRefresh`——面越多，把「与我无关的帧」挡在外面越要紧。
 */
export function useStreamDrivenRefresh(
  refresh: (() => void) | Array<() => void>
): void {
  const tick = useFrameTickStore((s) => s.tick);
  useFrameDrivenRefresh(tick, refresh);
}

/**
 * 只在**自己认得的那几种帧**到了的时候才重读。
 *
 * 键就是这一面订的那件事：总线原生帧用变体名（`task_status_change`），REST 侧
 * 一次成功的写用 `resource:<前缀>`（`resourceKey('settings')`）。不订的帧对它等于
 * 没发生——这正是「屏幕在动」与「屏幕在被别人的轮询拖着动」的区别。
 *
 * 订阅本身写在调用点，不写在 store 里：哪个面盯着哪几个前缀，是那一面自己的事，
 * 收在一张全局表里改一处会悄悄改掉另外十面。
 */
export function useKindTickedRefresh(
  keys: readonly string[],
  refresh: (() => void) | Array<() => void>
): void {
  // 键是调用点写死的字符串字面量，但写成内联数组时每次渲染都是新引用；拼成字符串
  // 当信号，值没变就不会重排定时器。
  const tick = useFrameTickStore((s) => ticksFor(s.byKind, keys));
  useFrameDrivenRefresh(tick, refresh);
}

function toList(refresh: (() => void) | Array<() => void>): Array<() => void> {
  return Array.isArray(refresh) ? refresh : [refresh];
}
