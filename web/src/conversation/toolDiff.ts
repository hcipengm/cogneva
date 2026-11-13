import type { ToolCallRecord } from '@/store/streamStore';

/**
 * 改动类工具在对话流里该怎么画。
 *
 * `edit_file` 的入参是「找到这段、换成这段」，`apply_patch` 的入参本来就是一份
 * 统一 diff，`write_file` 的入参是整个文件的内容。三者按「入参与返回原文」画出来
 * 都一样：一坨 JSON，读者要在里面自己找哪几行变了、哪个文件变了。这里把每一次都
 * 折成一份行级 diff。
 *
 * 一件事是硬的：**没有旧内容就不画 diff**。`write_file` 的记录里没有改动前的文件，
 * 于是它画的是「这次写进去的整个文件」，并写明这一点——把整份内容画成满屏的
 * `+` 行，读起来像这个文件原来不存在。
 */

export type DiffLineKind = 'same' | 'add' | 'del' | 'meta';

export interface DiffLine {
  kind: DiffLineKind;
  text: string;
}

export interface ToolDiff {
  /** 动的哪个文件；补丁里可能不止一个，那时是空。 */
  path: string | null;
  /** 这份东西是什么：一份行级 diff，还是整份被写进去的文件。 */
  label: string;
  lines: DiffLine[];
  added: number;
  removed: number;
}

/**
 * 逐个比对的规模上限（行）。
 *
 * 逐行比对是 O(旧行数 × 新行数)：一次改一整份大文件的调用会让这一面卡住几秒，
 * 而它只是画一个预览。超过这个规模就退回「整段删、整段加」——那仍然是准的，
 * 只是不再指出哪些行没动。
 */
export const DIFF_LINE_LIMIT = 400;

function parseArgs(call: ToolCallRecord): Record<string, unknown> | null {
  if (!call.args) return null;
  try {
    const parsed = JSON.parse(call.args);
    return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function text(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

/**
 * 两份文本的逐行差异。
 *
 * 先削掉公共的头尾——一次编辑通常只动中间几行，削完剩下的两块才小到可以逐行比。
 * 剩下的部分走一遍最长公共子序列；超过 [`DIFF_LINE_LIMIT`] 就整段删、整段加。
 */
export function lineDiff(before: string, after: string): DiffLine[] {
  const a = before.split('\n');
  const b = after.split('\n');
  let head = 0;
  while (head < a.length && head < b.length && a[head] === b[head]) head += 1;
  let tail = 0;
  while (
    tail < a.length - head &&
    tail < b.length - head &&
    a[a.length - 1 - tail] === b[b.length - 1 - tail]
  ) {
    tail += 1;
  }
  const oldMiddle = a.slice(head, a.length - tail);
  const newMiddle = b.slice(head, b.length - tail);

  const out: DiffLine[] = [];
  for (const line of a.slice(0, head)) out.push({ kind: 'same', text: line });

  if (oldMiddle.length > DIFF_LINE_LIMIT || newMiddle.length > DIFF_LINE_LIMIT) {
    for (const line of oldMiddle) out.push({ kind: 'del', text: line });
    for (const line of newMiddle) out.push({ kind: 'add', text: line });
  } else {
    // 最长公共子序列：`table[i][j]` 是 oldMiddle[i..] 与 newMiddle[j..] 能对齐的
    // 行数。两个块都小（上面那个上限管着），这张表也就小。
    const rows = oldMiddle.length;
    const cols = newMiddle.length;
    const table: number[][] = Array.from({ length: rows + 1 }, () => new Array<number>(cols + 1).fill(0));
    for (let i = rows - 1; i >= 0; i -= 1) {
      for (let j = cols - 1; j >= 0; j -= 1) {
        table[i][j] =
          oldMiddle[i] === newMiddle[j]
            ? table[i + 1][j + 1] + 1
            : Math.max(table[i + 1][j], table[i][j + 1]);
      }
    }
    let i = 0;
    let j = 0;
    while (i < rows && j < cols) {
      if (oldMiddle[i] === newMiddle[j]) {
        out.push({ kind: 'same', text: oldMiddle[i] });
        i += 1;
        j += 1;
      } else if (table[i + 1][j] >= table[i][j + 1]) {
        out.push({ kind: 'del', text: oldMiddle[i] });
        i += 1;
      } else {
        out.push({ kind: 'add', text: newMiddle[j] });
        j += 1;
      }
    }
    for (; i < rows; i += 1) out.push({ kind: 'del', text: oldMiddle[i] });
    for (; j < cols; j += 1) out.push({ kind: 'add', text: newMiddle[j] });
  }

  for (const line of a.slice(a.length - tail)) out.push({ kind: 'same', text: line });
  return out;
}

/**
 * 一份统一 diff 按行拆开。
 *
 * `+++` / `---` 是文件头，标成元信息而不是增删行：把它们染成绿行红行，读者会
 * 以为补丁里多改了两行。
 */
function patchLines(patch: string): DiffLine[] {
  return patch.split('\n').map((line) => {
    if (line.startsWith('+++') || line.startsWith('---') || line.startsWith('diff ')) {
      return { kind: 'meta' as const, text: line };
    }
    if (line.startsWith('@@')) return { kind: 'meta' as const, text: line };
    if (line.startsWith('+')) return { kind: 'add' as const, text: line };
    if (line.startsWith('-')) return { kind: 'del' as const, text: line };
    return { kind: 'same' as const, text: line };
  });
}

function count(lines: DiffLine[]): { added: number; removed: number } {
  let added = 0;
  let removed = 0;
  for (const line of lines) {
    if (line.kind === 'add') added += 1;
    if (line.kind === 'del') removed += 1;
  }
  return { added, removed };
}

/**
 * 一次工具调用折成的那份 diff；不是改动类工具时返回 `null`。
 *
 * 工具的入参名是各自的约定（`path` / `old_string` / `patch` / `content`），这
 * 一格之外的调用一个都不认——认错了会把一段命令行画成一份 diff。
 */
export function toolDiff(call: ToolCallRecord): ToolDiff | null {
  const args = parseArgs(call);
  if (!args) return null;
  const path = text(args.path) || null;

  if (call.name === 'edit_file') {
    const before = text(args.old_string);
    const after = text(args.new_string);
    if (!before && !after) return null;
    const lines = lineDiff(before, after);
    return { path, label: '这一处替换', lines, ...count(lines) };
  }

  if (call.name === 'apply_patch') {
    const patch = text(args.patch);
    if (!patch.trim()) return null;
    const lines = patchLines(patch);
    return { path, label: '这份补丁', lines, ...count(lines) };
  }

  if (call.name === 'write_file') {
    const content = text(args.content);
    if (!content) return null;
    // 没有改动前的内容：画的是这一整份文件。标明这一点，不假装它是 diff。
    const lines: DiffLine[] = content.split('\n').map((line) => ({ kind: 'add' as const, text: line }));
    return { path, label: '整份写进去的文件（改动前的内容不在这条记录里）', lines, ...count(lines) };
  }

  return null;
}
