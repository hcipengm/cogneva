import { useEffect, useRef } from 'react';
import { useShell } from './store';

/**
 * 把「对话里点了一道目标 / 一条变更」这件事落到账上。
 *
 * 请求由对话面发出（它只知道身份），落点由账那一面决定（它才知道这一行在不在
 * 当前这一页上）。`onFocus` 认领到这一行时返回 true，请求在这里被清掉；返回
 * false 表示「我还读不到这一行」——账还在读、或者这一行在这一页之外。那时请求
 * 留着，`onFocus` 的身份变了下一次渲染还会再来一次：一次点击不该因为读数慢半拍
 * 就悄悄丢掉。
 *
 * `onFocus` 走 ref 而不是依赖：它每次渲染都是一个新的闭包（它读的是这一拍的
 * 行），把它写进依赖里，这个 effect 会随每一次渲染重跑，而它要等的是「落点变了」
 * 或「账读了新的一页」这两件事。
 */
export function useRecordFocus(
  kind: 'goal' | 'change',
  onFocus: (id: string) => boolean
): void {
  const focus = useShell((s) => s.focus);
  const setFocus = useShell((s) => s.setFocus);
  const onFocusRef = useRef(onFocus);
  onFocusRef.current = onFocus;

  useEffect(() => {
    if (!focus || focus.kind !== kind) return;
    if (onFocusRef.current(focus.id)) setFocus(null);
  }, [focus, kind, setFocus]);
}
