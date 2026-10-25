import { Fragment, type ReactNode } from 'react';

/**
 * 一份 TeX 源文件的阅读视图。
 *
 * 排不出真 PDF——那要一整套字体与排版引擎。这里做的是「读懂它写了什么」：分节、
 * 列表、强调、公式原样等宽显示，导言区折起来。这样打开一份论文源，读者能顺着
 * 标题读下去；而要看真正的排版结果，页面上有「源码」一栏，两条路都留着。
 *
 * 命令一律按字面显示而不是丢掉：`\cite{...}`、`\ref{...}` 这类东西恰恰是这份
 * 文档的骨架，删掉它们会让人以为自己读到了全文。
 */

interface TexBlock {
  kind: 'heading' | 'paragraph' | 'item' | 'display';
  level?: number;
  text: string;
  ordered?: boolean;
}

export function Tex({ source }: { source: string }) {
  const { preamble, body } = splitPreamble(source);
  return (
    <div className="flex flex-col gap-3 pb-16 pt-4">
      {preamble && (
        <details className="border border-[var(--line)] px-3 py-2">
          <summary className="cursor-pointer text-[11px] text-[var(--muted)]">
            导言区（{preamble.split('\n').length} 行，排版用的设置，不是正文）
          </summary>
          <pre className="mono mt-2 overflow-x-auto text-[11px] leading-[1.6] text-[var(--muted)]">
            {preamble}
          </pre>
        </details>
      )}
      <div className="flex flex-col gap-2">
        {parseBody(body).map((block, index) => (
          <BlockView key={index} block={block} />
        ))}
      </div>
    </div>
  );
}

function splitPreamble(source: string): { preamble: string; body: string } {
  const at = source.indexOf('\\begin{document}');
  if (at < 0) return { preamble: '', body: source };
  const end = source.lastIndexOf('\\end{document}');
  const after = end > at ? source.slice(at + '\\begin{document}'.length, end) : source.slice(at);
  return { preamble: source.slice(0, at).trim(), body: after };
}

function parseBody(source: string): TexBlock[] {
  const blocks: TexBlock[] = [];
  // 命令与正文交替出现，逐个切出来比按行猜更稳：一句里就可能有一个 \textbf。
  const pattern = /\\(section|subsection|subsubsection|paragraph)\*?\{([^}]*)\}|\\(begin|end)\{(itemize|enumerate)\}|\\item\b|\\\[([\s\S]*?)\\\]|\$\$([\s\S]*?)\$\$|\n\s*\n/g;
  let cursor = 0;
  let current: TexBlock | null = null;
  let inList = false;
  let ordered = false;
  let match: RegExpExecArray | null;

  const flush = () => {
    if (current && current.text.trim()) blocks.push(current);
    current = null;
  };

  while ((match = pattern.exec(source)) !== null) {
    const text = source.slice(cursor, match.index);
    if (text.trim()) {
      if (inList) {
        if (!current) current = { kind: 'item', text: '', ordered };
        current.text += text;
      } else {
        if (!current) current = { kind: 'paragraph', text: '' };
        current.text += text;
      }
    }
    cursor = match.index + match[0].length;

    const [, section, title, mode, environment, display, displayMath] = match;
    if (section) {
      flush();
      const level = section === 'section' ? 1 : section === 'subsection' ? 2 : section === 'subsubsection' ? 3 : 4;
      blocks.push({ kind: 'heading', level, text: title ?? '' });
    } else if (mode === 'begin' && (environment === 'itemize' || environment === 'enumerate')) {
      flush();
      inList = true;
      ordered = environment === 'enumerate';
    } else if (mode === 'end') {
      flush();
      inList = false;
    } else if (match[0] === '\\item') {
      flush();
      current = { kind: 'item', text: '', ordered };
    } else if (display !== undefined || displayMath !== undefined) {
      flush();
      blocks.push({ kind: 'display', text: (display ?? displayMath ?? '').trim() });
    } else {
      // 段落分隔：结束当前块，列表里则结束当前一项。
      flush();
      if (inList) current = { kind: 'item', text: '', ordered };
    }
  }

  if (cursor < source.length) {
    const rest = source.slice(cursor);
    if (rest.trim()) {
      if (current) current.text += rest;
      else blocks.push({ kind: inList ? 'item' : 'paragraph', text: rest, ordered });
    }
  }
  flush();

  return blocks;
}

const SECTION_SIZE = ['15px', '13.5px', '13px', '12.5px'];

function BlockView({ block }: { block: TexBlock }) {
  if (block.kind === 'heading') {
    return (
      <h3
        className="mt-3 font-medium tracking-tight text-[var(--paper)]"
        style={{ fontSize: SECTION_SIZE[(block.level ?? 1) - 1] ?? '12.5px' }}
      >
        {inlineTex(block.text)}
      </h3>
    );
  }
  if (block.kind === 'display') {
    return (
      <pre
        className="mono overflow-x-auto border-l-2 px-3 py-1 text-[12px] leading-[1.6] text-[var(--paper-dim)]"
        style={{ borderColor: 'var(--line-strong)' }}
      >
        {block.text}
      </pre>
    );
  }
  if (block.kind === 'item') {
    return (
      <p className="ml-4 flex gap-2 text-[12.5px] leading-[1.75] text-[var(--paper-dim)]">
        <span className="mono shrink-0 text-[var(--muted)]">{block.ordered ? '1.' : '·'}</span>
        <span>{inlineTex(block.text)}</span>
      </p>
    );
  }
  return (
    <p className="text-[12.5px] leading-[1.75] text-[var(--paper-dim)]">{inlineTex(block.text)}</p>
  );
}

/** 行内：`\textbf{…}` 加粗、`\emph{…}` 斜体、`$…$` 与其余命令按等宽原样显示。 */
function inlineTex(text: string): ReactNode[] {
  const cleaned = text.replace(/\s+/g, ' ');
  const nodes: ReactNode[] = [];
  const pattern = /\\textbf\{([^}]*)\}|\\emph\{([^}]*)\}|\\texttt\{([^}]*)\}|\$([^$]+)\$|(\\[a-zA-Z]+)/g;
  let cursor = 0;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(cleaned)) !== null) {
    if (match.index > cursor) nodes.push(cleaned.slice(cursor, match.index));
    const [token, bold, emph, mono, math, command] = match;
    if (bold !== undefined) {
      nodes.push(<span key={nodes.length} className="font-medium text-[var(--paper)]">{bold}</span>);
    } else if (emph !== undefined) {
      nodes.push(<span key={nodes.length} style={{ fontStyle: 'italic' }}>{emph}</span>);
    } else if (mono !== undefined) {
      nodes.push(<code key={nodes.length} className="mono text-[11.5px] text-[var(--paper)]">{mono}</code>);
    } else if (math !== undefined) {
      nodes.push(<code key={nodes.length} className="mono text-[11.5px] text-[var(--paper-dim)]">{math}</code>);
    } else {
      nodes.push(<code key={nodes.length} className="mono text-[11.5px] text-[var(--muted)]">{command}</code>);
    }
    cursor = match.index + token.length;
  }

  if (cursor < cleaned.length) nodes.push(cleaned.slice(cursor));
  return nodes.map((node, index) => <Fragment key={index}>{node}</Fragment>);
}
