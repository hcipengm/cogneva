import { useMemo } from 'react';
import { renderMarkdown } from '@/conversation/markdown';

/**
 * 一段回复正文的画布。
 *
 * 单独一个组件，是为了让「什么时候重新解析」只有一处：正文每来一个 token 就
 * 变一次，把解析放进 `useMemo` 里，其它字段（花销、时刻）的变化就不会连带把一整
 * 段 Markdown 重排一遍。
 *
 * 正在流的那个落点由样式表画在最后一块的末尾（`.md[data-streaming]` 的
 * `::after`），不在这里多插一个空元素：插在块后面，它会掉到下一行的行首，
 * 而那正好不是「字长到哪儿了」。
 */
export function Markdown({ text, streaming = false }: { text: string; streaming?: boolean }) {
  const html = useMemo(() => renderMarkdown(text), [text]);
  return (
    <div
      className="md break-words text-[13px] leading-[1.7]"
      data-streaming={streaming || undefined}
      // 上面那条路已经过消毒（`renderMarkdown`），这里只负责放进来。
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
