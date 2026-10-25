/** 打开一个文件时，先按后缀决定用什么去读它。 */
export type PreviewKind = 'markdown' | 'html' | 'tex' | 'docx' | 'xlsx' | 'text';

const KIND_BY_EXTENSION: Record<string, PreviewKind> = {
  md: 'markdown',
  markdown: 'markdown',
  mdx: 'markdown',
  html: 'html',
  htm: 'html',
  tex: 'tex',
  latex: 'tex',
  docx: 'docx',
  docm: 'docx',
  xlsx: 'xlsx',
  xlsm: 'xlsx',
};

export function previewKindOf(path: string): PreviewKind {
  const extension = path.split('.').pop()?.toLowerCase() ?? '';
  return KIND_BY_EXTENSION[extension] ?? 'text';
}

/** 这两种要按字节读，不能当成文本读进来。 */
export function needsBytes(kind: PreviewKind): boolean {
  return kind === 'docx' || kind === 'xlsx';
}

export const KIND_LABEL: Record<PreviewKind, string> = {
  markdown: 'Markdown',
  html: 'HTML',
  tex: 'TeX',
  docx: 'Word 文档',
  xlsx: '表格',
  text: '源码',
};
