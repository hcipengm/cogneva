import { useSyncExternalStore } from 'react';

/**
 * 一个数字的来路。
 *
 * 界面上的数字有两种病：一种是没有来路（读者只能信），另一种是有来路却不说
 * （读者把它当成平台此刻的状态）。这个结构就是把每个数的三件事带上——它读的是
 * 什么、什么时候算出来的、由哪几帧折出来的——点开就能看见，不必去翻代码。
 */
export interface ExplainFrame {
  at: string;
  type: string;
  summary: string;
}

export interface ExplainSubject {
  /** 这一格读的是什么（「交接」「原始来源」「在岗副本」）。 */
  label: string;
  /** 屏幕上那个数。画出来的值（图形、节点）不是一串数，这里就说清它是什么。 */
  value: string;
  /** 这个数从哪儿来：哪个端点、哪一列、哪一本账。 */
  source: string;
  /** 什么时候算的：服务端读回的时刻，或帧到达的时刻。没有就是没有，不补一个。 */
  computedAt: string | null;
  /** 由哪几帧算出来的。轮询来的聚合数没有帧，`undefined` 与「没有帧」要分清。 */
  frames?: ExplainFrame[];
  /** 这一格的已知边界，例如「进程启动算起」「截断前的总数」。 */
  caveat?: string;
}

let current: ExplainSubject | null = null;
const listeners = new Set<() => void>();

function emit() {
  for (const listener of listeners) listener();
}

export function openExplain(subject: ExplainSubject) {
  current = subject;
  emit();
}

export function closeExplain() {
  if (current === null) return;
  current = null;
  emit();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** 现在被点开的那个数。抽屉自己订阅；打开/关闭都不进全局 store。 */
export function useExplainSubject(): ExplainSubject | null {
  return useSyncExternalStore(
    subscribe,
    () => current,
    () => current
  );
}

/** 一格的来路别名：读数是 `useResource` 的返回值，这里只要它的三个字段。 */
export interface ExplainOrigin {
  what: string;
  at: string | null;
}
