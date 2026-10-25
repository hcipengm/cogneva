import { Fragment, type ReactNode } from 'react';

/**
 * 一个够用的 Markdown 渲染器。
 *
 * 不引 marked / remark：这个控制台要读的文档是仓库里的说明与记录，用到的是
 * 标题、列表、代码、表格、引用、行内强调这几样。引一个大解析器换来的能力，多半
 * 是「嵌 HTML 执行脚本」这类我们并不想在这个面上开的口子——这里所有内容都当文本
 * 处理，链接一律新窗口打开、不外链执行。
 *
 * 认不出来的一律原样显示：文档里出现一段没被渲染的源码，读者立刻知道这是渲染器
 * 的边界；而悄悄丢掉，读者会以为原文就写到那儿。
 */

interface Block {
  kind: 'heading' | 'paragraph' | 'fence' | 'list' | 'quote' | 'table' | 'rule';
  level?: number;
  language?: string;
  lines: string[];
}

export function Markdown({ source }: { source: string }) {
  const blocks = parseBlocks(source);
  return (
    <div className="md-body flex flex-col gap-3 pb-16 pt-4">
      {blocks.map((block, index) => (
        <BlockView key={index} block={block} />
      ))}
    </div>
  );
}

function parseBlocks(source: string): Block[] {
  const lines = source.replace(/\r\n?/g, '\n').split('\n');
  const blocks: Block[] = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];

    if (!line.trim()) {
      index += 1;
      continue;
    }

    const fence = line.match(/^\s*(```|~~~)\s*([\w+-]*)\s*$/);
    if (fence) {
      const body: string[] = [];
      index += 1;
      while (index < lines.length && !new RegExp(`^\\s*${fence[1]}\\s*$`).test(lines[index])) {
        body.push(lines[index]);
        index += 1;
      }
      index += 1;
      blocks.push({ kind: 'fence', language: fence[2] || undefined, lines: body });
      continue;
    }

    const heading = line.match(/^\s*(#{1,6})\s+(.*)$/);
    if (heading) {
      blocks.push({ kind: 'heading', level: heading[1].length, lines: [heading[2]] });
      index += 1;
      continue;
    }

    if (/^\s*([-*_])(?:\s*\1){2,}\s*$/.test(line)) {
      blocks.push({ kind: 'rule', lines: [] });
      index += 1;
      continue;
    }

    if (/^\s*>/.test(line)) {
      const body: string[] = [];
      while (index < lines.length && /^\s*>/.test(lines[index])) {
        body.push(lines[index].replace(/^\s*>\s?/, ''));
        index += 1;
      }
      blocks.push({ kind: 'quote', lines: body });
      continue;
    }

    if (/^\s*\|/.test(line) && /^\s*\|?[\s:|-]+\|[\s:|-]*$/.test(lines[index + 1] ?? '')) {
      const body: string[] = [];
      body.push(lines[index]);
      index += 2; // 表头 + 分隔行
      while (index < lines.length && /^\s*\|/.test(lines[index])) {
        body.push(lines[index]);
        index += 1;
      }
      blocks.push({ kind: 'table', lines: body });
      continue;
    }

    if (/^\s*([-*+]|\d+[.)])\s+/.test(line)) {
      const body: string[] = [];
      while (index < lines.length && (/^\s*([-*+]|\d+[.)])\s+/.test(lines[index]) || /^\s{2,}\S/.test(lines[index]))) {
        body.push(lines[index]);
        index += 1;
      }
      blocks.push({ kind: 'list', lines: body });
      continue;
    }

    const body: string[] = [];
    while (
      index < lines.length &&
      lines[index].trim() &&
      !/^\s*(#{1,6}\s|```|~~~|>|\||([-*+]|\d+[.)])\s)/.test(lines[index])
    ) {
      body.push(lines[index]);
      index += 1;
    }
    if (body.length === 0) {
      body.push(lines[index]);
      index += 1;
    }
    blocks.push({ kind: 'paragraph', lines: body });
  }

  return blocks;
}

const HEADING_SIZE = ['15px', '14px', '13px', '13px', '13px', '13px'];

function BlockView({ block }: { block: Block }) {
  switch (block.kind) {
    case 'heading': {
      const level = block.level ?? 1;
      return (
        <h3
          className="font-medium tracking-tight text-[var(--paper)]"
          style={{
            fontSize: HEADING_SIZE[level - 1] ?? '13px',
            marginTop: level <= 2 ? '10px' : '4px',
          }}
        >
          {inline(block.lines[0] ?? '')}
        </h3>
      );
    }
    case 'fence':
      return (
        <pre className="mono overflow-x-auto border border-[var(--line)] bg-[var(--ink-900)] px-3 py-2 text-[11.5px] leading-[1.65] text-[var(--paper-dim)]">
          <code>{block.lines.join('\n')}</code>
        </pre>
      );
    case 'quote':
      return (
        <blockquote className="border-l-2 pl-3 text-[12.5px] leading-[1.7] text-[var(--muted)]" style={{ borderColor: 'var(--line-strong)' }}>
          {block.lines.map((line, index) => (
            <p key={index}>{inline(line)}</p>
          ))}
        </blockquote>
      );
    case 'rule':
      return <hr className="border-0 border-t border-[var(--line)]" />;
    case 'list':
      return <ListBlock lines={block.lines} />;
    case 'table':
      return <TableBlock lines={block.lines} />;
    default:
      return (
        <p className="text-[12.5px] leading-[1.75] text-[var(--paper-dim)]">
          {block.lines.map((line, index) => (
            <Fragment key={index}>
              {index > 0 && <br />}
              {inline(line)}
            </Fragment>
          ))}
        </p>
      );
  }
}

function ListBlock({ lines }: { lines: string[] }) {
  const items: { ordered: boolean; text: string }[] = [];
  for (const line of lines) {
    const match = line.match(/^\s*([-*+]|\d+[.)])\s+(.*)$/);
    if (match) {
      items.push({ ordered: /\d/.test(match[1]), text: match[2] });
    } else if (items.length > 0) {
      items[items.length - 1].text += ` ${line.trim()}`;
    }
  }
  const ordered = items.some((item) => item.ordered);
  const Tag = ordered ? 'ol' : 'ul';
  return (
    <Tag className={`ml-4 flex list-outside flex-col gap-1 text-[12.5px] leading-[1.7] text-[var(--paper-dim)] ${ordered ? 'list-decimal' : 'list-disc'}`}>
      {items.map((item, index) => (
        <li key={index} className="pl-1 marker:text-[var(--muted)]">
          {inline(item.text)}
        </li>
      ))}
    </Tag>
  );
}

function TableBlock({ lines }: { lines: string[] }) {
  const rows = lines.map((line) =>
    line
      .replace(/^\s*\|/, '')
      .replace(/\|\s*$/, '')
      .split('|')
      .map((cell) => cell.trim())
  );
  const [head, ...body] = rows;
  return (
    <div className="overflow-x-auto border border-[var(--line)]">
      <table className="w-full border-collapse text-[12px]">
        <thead>
          <tr>
            {head.map((cell, index) => (
              <th
                key={index}
                className="border-b border-[var(--line)] px-2 py-1 text-left font-medium text-[var(--paper)]"
              >
                {inline(cell)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {body.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {row.map((cell, cellIndex) => (
                <td
                  key={cellIndex}
                  className="border-b border-[var(--line)] px-2 py-1 align-top text-[var(--paper-dim)]"
                >
                  {inline(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** 行内：先认代码，再认链接与强调。代码优先，否则反引号里的星号会被当成强调。 */
export function inline(text: string): ReactNode[] {
  const nodes: ReactNode[] = [];
  const pattern = /(`[^`]+`)|(\[[^\]]+\]\([^)\s]+\))|(\*\*[^*]+\*\*)|(__[^_]+__)|(\*[^*]+\*)|(_[^_]+_)/g;
  let cursor = 0;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(text)) !== null) {
    if (match.index > cursor) nodes.push(text.slice(cursor, match.index));
    const token = match[0];
    if (token.startsWith('`')) {
      nodes.push(
        <code
          key={nodes.length}
          className="mono border border-[var(--line)] bg-[var(--ink-900)] px-[3px] py-[1px] text-[11.5px] text-[var(--paper)]"
        >
          {token.slice(1, -1)}
        </code>
      );
    } else if (token.startsWith('[')) {
      const link = token.match(/^\[([^\]]+)\]\(([^)\s]+)\)$/);
      const label = link?.[1] ?? token;
      const href = link?.[2] ?? '';
      const external = /^https?:/i.test(href);
      nodes.push(
        external ? (
          <a
            key={nodes.length}
            href={href}
            target="_blank"
            rel="noreferrer noopener"
            className="underline decoration-dotted underline-offset-2"
            style={{ color: 'var(--brass)' }}
          >
            {label}
          </a>
        ) : (
          // 站内相对路径在这里没有可走的路由：原样显示路径，读者能看出它指向哪。
          <span key={nodes.length} className="mono text-[11.5px] text-[var(--muted)]" title="相对链接，不在这里跳转">
            {label}（{href}）
          </span>
        )
      );
    } else {
      const strong = token.startsWith('**') || token.startsWith('__');
      const inner = strong ? token.slice(2, -2) : token.slice(1, -1);
      nodes.push(
        <span
          key={nodes.length}
          className={strong ? 'font-medium text-[var(--paper)]' : ''}
          style={strong ? undefined : { fontStyle: 'italic' }}
        >
          {inner}
        </span>
      );
    }
    cursor = match.index + token.length;
  }

  if (cursor < text.length) nodes.push(text.slice(cursor));
  return nodes;
}
