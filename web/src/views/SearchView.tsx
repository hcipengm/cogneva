import { useState } from 'react';
import { searchKnowledge, searchPlatform } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Button, Empty, Loading, ReadError, ViewFrame } from '@/ui/primitives';

/**
 * 搜索面：一次查询分别问「知识库」与「整个平台」。
 *
 * 两条检索接口此前只有 API：能力在、能查，人却找不到入口。两个来源分开列，因为
 * 它们的召回与失败方式不同——混成一个列表会把「知识库里没有」和「平台没搜到」说成同一件事。
 * 画面：这一面的结果是检索命中，一次查询一份；把它们画成图会掩盖召回本身的样子（顺序、相关度、来源），而那正是读者要判断的东西。
 *
 */
export function SearchView() {
  const [text, setText] = useState('');
  const [submitted, setSubmitted] = useState<{ q: string; scope: 'knowledge' | 'platform' } | null>(null);
  const result = useResource(
    submitted ? `${submitted.scope}:${submitted.q}` : '未查询',
    async () =>
      submitted
        ? submitted.scope === 'knowledge'
          ? searchKnowledge(submitted.q)
          : searchPlatform(submitted.q)
        : null,
    0,
  );

  return (
    <ViewFrame title="搜索" meta="知识库检索与平台检索，两条接口的入口">
      <div className="flex items-center gap-2 px-1 py-2">
        <input
          className="rounded border px-2 py-1 text-[12px]"
          style={{ borderColor: 'var(--line)', background: 'transparent' }}
          placeholder="查什么"
          value={text}
          onChange={(e) => setText(e.target.value)}
        />
        <Button onClick={() => setSubmitted({ q: text, scope: 'knowledge' })}>查知识库</Button>
        <Button onClick={() => setSubmitted({ q: text, scope: 'platform' })}>查平台</Button>
      </div>
      {submitted === null ? (
        <Empty>还没有查询。选一个来源再查——两个来源的召回不一样，合起来列会把差别藏掉。</Empty>
      ) : result.error ? (
        <ReadError what="检索" error={result.error} onRetry={result.refresh} />
      ) : !result.data ? (
        <Loading what="检索" />
      ) : (
        <pre className="whitespace-pre-wrap text-[12px]">{JSON.stringify(result.data, null, 2)}</pre>
      )}
    </ViewFrame>
  );
}
