import { useEffect, useState } from 'react';

/**
 * 每隔 `intervalMs` 返回一个新的「现在」。
 *
 * 有的读数只在「时间过去了」的时候才该变，例如状态条上「最近一帧在 3 分前」——
 * 帧不来的时候没有事件可以触发重渲染，那一格会永远停在「刚刚」，而它正是用来
 * 说「已经很久没有帧了」的。这类读数得有一个自己的钟。
 */
export function useNow(intervalMs = 5_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(timer);
  }, [intervalMs]);
  return now;
}
