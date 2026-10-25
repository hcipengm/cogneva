import { findEntry, textEntry, unzip } from '@/files/preview/zip';

/**
 * 从 xlsx 里取出表格：每张表的名字、行、列、单元格里的字。
 *
 * 公式只显示它的缓存值——打开一份账，读者要的是那一格现在是多少，不是它怎么
 * 算出来的；要公式的场合在这里看见的值本来也不足以判断对错。
 *
 * 上限是硬写的：一张十几万行的表塞进 DOM，会把整个控制台拖死。超出的部分不进
 * 页面，并在页脚说明截断——先让人看见「这里还有多少」比无声地画一部分强。
 */

export const MAX_ROWS = 400;
export const MAX_COLUMNS = 40;

export interface Cell {
  text: string;
  /** `n` 数字、`s` 文本、`b` 布尔、`e` 错误值。 */
  kind: 'n' | 's' | 'b' | 'e';
}

export interface Sheet {
  name: string;
  rows: Cell[][];
  /** 这份工作表的实际尺寸（截断前）。 */
  rowTotal: number;
  columnTotal: number;
}

export async function readXlsx(buffer: ArrayBuffer): Promise<Sheet[]> {
  const files = await unzip(buffer);
  const shared = readSharedStrings(files);
  const names = readSheetNames(files);
  const sheets: Sheet[] = [];

  const paths = Array.from(files.keys())
    .filter((name) => /^xl\/worksheets\/[^/]+\.xml$/.test(name))
    .sort(compareSheetPaths);

  for (let index = 0; index < paths.length; index += 1) {
    const xml = textEntry(files, paths[index]);
    const document = new DOMParser().parseFromString(xml, 'application/xml');
    if (document.querySelector('parsererror')) {
      throw new Error(`${paths[index]} 不是合法 XML`);
    }
    sheets.push({
      name: names[index] ?? paths[index].replace(/^xl\/worksheets\//, '').replace(/\.xml$/, ''),
      ...readSheet(document, shared),
    });
  }

  if (sheets.length === 0) throw new Error('容器里没有工作表，这不是一个 xlsx 文档');
  return sheets;
}

function readSharedStrings(files: Map<string, Uint8Array>): string[] {
  const path = findEntry(files, (name) => name === 'xl/sharedStrings.xml');
  if (!path) return [];
  const document = new DOMParser().parseFromString(textEntry(files, path), 'application/xml');
  const out: string[] = [];
  // `si` 里可能是单个 `t`，也可能是几段带格式的 `r/t`；两处都要按顺序接起来，
  // 否则富文本单元格会只剩第一段。
  for (const item of Array.from(document.getElementsByTagName('si'))) {
    let text = '';
    for (const piece of Array.from(item.getElementsByTagName('t'))) text += piece.textContent ?? '';
    out.push(text);
  }
  return out;
}

function readSheetNames(files: Map<string, Uint8Array>): string[] {
  const path = findEntry(files, (name) => name === 'xl/workbook.xml');
  if (!path) return [];
  const document = new DOMParser().parseFromString(textEntry(files, path), 'application/xml');
  return Array.from(document.getElementsByTagName('sheet'))
    .map((sheet) => sheet.getAttribute('name') ?? '')
    .filter(Boolean);
}

function readSheet(document: Document, shared: string[]): Omit<Sheet, 'name'> {
  const rows: Cell[][] = [];
  let rowTotal = 0;
  let columnTotal = 0;
  let nextRow = 0;

  for (const row of Array.from(document.getElementsByTagName('row'))) {
    // 行的 `r` 是 1 起的行号。跳过空行会让编号对不上，所以宁可补空行。
    const declared = Number(row.getAttribute('r'));
    const at = Number.isFinite(declared) && declared > 0 ? declared - 1 : nextRow;
    rowTotal = Math.max(rowTotal, at + 1);
    nextRow = at + 1;
    if (at >= MAX_ROWS) continue;

    const cells: Cell[] = [];
    let nextColumn = 0;
    for (const cell of Array.from(row.getElementsByTagName('c'))) {
      const reference = cell.getAttribute('r');
      const column = reference ? columnOf(reference) : nextColumn;
      nextColumn = column + 1;
      columnTotal = Math.max(columnTotal, column + 1);
      if (column >= MAX_COLUMNS) continue;
      while (cells.length < column) cells.push({ text: '', kind: 's' });
      cells[column] = valueOf(cell, shared);
    }
    while (rows.length < at) rows.push([]);
    rows[at] = cells;
  }

  return { rows, rowTotal, columnTotal };
}

function valueOf(cell: Element, shared: string[]): Cell {
  const type = cell.getAttribute('t');
  if (type === 'inlineStr') {
    let text = '';
    for (const piece of Array.from(cell.getElementsByTagName('t'))) text += piece.textContent ?? '';
    return { text, kind: 's' };
  }
  const value = cell.getElementsByTagName('v')[0]?.textContent ?? '';
  if (type === 's') {
    const index = Number(value);
    return { text: shared[index] ?? '', kind: 's' };
  }
  if (type === 'b') return { text: value === '1' ? '真' : '假', kind: 'b' };
  if (type === 'e') return { text: value, kind: 'e' };
  return { text: value, kind: 'n' };
}

/** `B12` → 1。列号是 26 进制，从 A 起算。 */
export function columnOf(reference: string): number {
  const letters = reference.replace(/[^A-Z]/gi, '').toUpperCase();
  let column = 0;
  for (const letter of letters) column = column * 26 + (letter.charCodeAt(0) - 64);
  return Math.max(0, column - 1);
}

export function columnName(index: number): string {
  let name = '';
  let rest = index + 1;
  while (rest > 0) {
    const digit = (rest - 1) % 26;
    name = String.fromCharCode(65 + digit) + name;
    rest = Math.floor((rest - 1) / 26);
  }
  return name;
}

function compareSheetPaths(left: string, right: string): number {
  const a = Number(left.match(/(\d+)\.xml$/)?.[1] ?? 0);
  const b = Number(right.match(/(\d+)\.xml$/)?.[1] ?? 0);
  return a - b;
}
