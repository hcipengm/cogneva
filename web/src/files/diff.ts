/**
 * 并排 diff 的对齐。
 *
 * 服务端给的是 unified diff 的行，带两边的行号；这里只决定「哪一行跟哪一行
 * 并排放」。规则只有一条：上下文行两边同一行，连续的删除/新增按顺序配对，
 * 多出来的那几条各自占一行、另一侧留空。
 *
 * 不按行数把两侧拉齐，也不做任何模糊匹配：一处改动画成两处无关的改动，
 * 比没有 diff 更糟——读者会照着它去改别的行。
 */
export interface DiffLineLike {
  kind: string;
  old_no: number | null;
  new_no: number | null;
  text: string;
}

export interface DiffHunkLike {
  lines: DiffLineLike[];
}

export interface DiffRow<T extends DiffLineLike = DiffLineLike> {
  left: T | null;
  right: T | null;
}

export function pairHunk<T extends DiffLineLike>(hunk: DiffHunkLike & { lines: T[] }): DiffRow<T>[] {
  const rows: DiffRow<T>[] = [];
  let dels: T[] = [];
  let adds: T[] = [];

  const flush = () => {
    const pairs = Math.max(dels.length, adds.length);
    for (let i = 0; i < pairs; i += 1) {
      rows.push({ left: dels[i] ?? null, right: adds[i] ?? null });
    }
    dels = [];
    adds = [];
  };

  for (const line of hunk.lines) {
    if (line.kind === 'del') {
      dels.push(line);
      continue;
    }
    if (line.kind === 'add') {
      adds.push(line);
      continue;
    }
    flush();
    rows.push({ left: line, right: line });
  }
  flush();
  return rows;
}
