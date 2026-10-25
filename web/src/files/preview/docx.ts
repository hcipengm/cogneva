import { findEntry, textEntry, unzip } from '@/files/preview/zip';

/**
 * 从 docx 里取出能读的那部分：段落、标题、列表、粗体斜体。
 *
 * 不追求版式还原——分页、页边距、浮动图片、域代码在屏幕上都另有答案。这里只
 * 保住「读者打开文档是想读里面写了什么」这件事，其余宁可说清楚不支持，也不画
 * 一个看着像文档、实际缺了半页的仿制品。
 */

export interface Run {
  text: string;
  bold: boolean;
  italic: boolean;
  code: boolean;
}

export interface Block {
  kind: 'heading' | 'paragraph' | 'listItem';
  /** 标题的层级（1–6）；列表项恒为 1。 */
  level: number;
  runs: Run[];
}

export interface DocxDocument {
  blocks: Block[];
  /** 文档里出现了什么这里读不出来的东西，用来在页脚说实话。 */
  unsupported: string[];
}

export async function readDocx(buffer: ArrayBuffer): Promise<DocxDocument> {
  const files = await unzip(buffer);
  const main = findEntry(files, (name) => name === 'word/document.xml');
  if (!main) throw new Error('容器里没有 word/document.xml，这不是一个 docx 文档');
  const xml = textEntry(files, main);
  const document = new DOMParser().parseFromString(xml, 'application/xml');
  if (document.querySelector('parsererror')) throw new Error('word/document.xml 不是合法 XML');

  const unsupported = new Set<string>();
  if (findEntry(files, (name) => /^word\/media\//.test(name))) unsupported.add('图片');

  const blocks: Block[] = [];
  const body = document.documentElement;
  for (const node of Array.from(body.children)) {
    collect(node, blocks, unsupported);
  }
  // 有的导出器把正文包在 w:body 里，有的直接挂在根下；两种都在 paragraphsOf 里兜住了。
  return { blocks, unsupported: Array.from(unsupported) };
}

function collect(node: Element, blocks: Block[], unsupported: Set<string>): void {
  const name = localName(node);
  if (name === 'p') {
    blocks.push(paragraphOf(node));
    return;
  }
  if (name === 'tbl') {
    // 表格的单元格里也是段落，按文档顺序读出来不会丢字，丢的是列的对齐关系。
    unsupported.add('表格的列与合并单元格');
  }
  if (name === 'txbxContent') {
    unsupported.add('文本框');
  }
  for (const child of Array.from(node.children)) collect(child, blocks, unsupported);
}

function paragraphOf(paragraph: Element): Block {
  const style = childNamed(paragraph, 'pPr');
  const styleId = style ? childNamed(style, 'pStyle')?.getAttribute('w:val') ?? null : null;
  const outline = style ? childNamed(style, 'outlineLvl')?.getAttribute('w:val') ?? null : null;
  const numbered = style ? childNamed(style, 'numPr') != null : false;

  const runs: Run[] = [];
  for (const run of descendants(paragraph, 'r')) {
    const properties = childNamed(run, 'rPr');
    const bold = properties ? childNamed(properties, 'b') != null : false;
    const italic = properties ? childNamed(properties, 'i') != null : false;
    const code = properties
      ? childNamed(properties, 'rFonts')?.getAttribute('w:ascii') === 'Consolas'
      : false;
    let text = '';
    for (const piece of Array.from(run.children)) {
      const pieceName = localName(piece);
      if (pieceName === 't') text += piece.textContent ?? '';
      else if (pieceName === 'tab') text += '\t';
      else if (pieceName === 'br' || pieceName === 'cr') text += '\n';
      else if (pieceName === 'drawing' || pieceName === 'pict') text += '';
    }
    if (text) runs.push({ text, bold, italic, code });
  }

  const match = styleId?.match(/^(?:Heading|heading)\s*(\d)$/);
  const headingLevel = match ? Number(match[1]) : outline ? Number(outline) + 1 : 0;
  if (headingLevel >= 1 && headingLevel <= 6) {
    return { kind: 'heading', level: headingLevel, runs };
  }
  return { kind: numbered ? 'listItem' : 'paragraph', level: 1, runs };
}

function descendants(root: Element, name: string): Element[] {
  const out: Element[] = [];
  const walk = (node: Element) => {
    for (const child of Array.from(node.children)) {
      if (localName(child) === name) out.push(child);
      else walk(child);
    }
  };
  walk(root);
  return out;
}

function childNamed(root: Element, name: string): Element | null {
  for (const child of Array.from(root.children)) {
    if (localName(child) === name) return child;
  }
  return null;
}

/**
 * 前缀 `w:` 只是这一份 XML 采用的约定，不是规范：按冒号后面那段认元素，
 * 换个前缀（或者干脆没有前缀）也读得出来。
 */
function localName(element: Element): string {
  const prefixed = element.tagName;
  const colon = prefixed.indexOf(':');
  return colon >= 0 ? prefixed.slice(colon + 1) : prefixed;
}
