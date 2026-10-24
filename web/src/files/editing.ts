/**
 * 编辑器里两件「打字时立刻会用到」的事：回车之后站在哪一列，打左括号要不要
 * 顺手补右括号。
 *
 * 两个纯函数收到「正文 + 光标」，交回「新正文 + 新光标」。不碰 DOM：这两个行为
 * 每按一次键都跑一遍，而它们错了只表现为「缩进不对」「多出一个括号」——在屏上
 * 看不出是哪一层的错，所以判据只能落在函数本身。
 */
export interface Edit {
  text: string;
  /** 改完之后光标落在哪个下标（折叠态）。 */
  caret: number;
}

/** 缩进一格。制表符宽度在编辑器那一层是固定的，这里只按同一格加。 */
const INDENT = '  ';

const CLOSERS: Record<string, string> = {
  '(': ')',
  '[': ']',
  '{': '}',
  '"': '"',
  "'": "'",
  '`': '`',
};

function lineStartOf(text: string, at: number): number {
  if (at <= 0) return 0;
  return text.lastIndexOf('\n', at - 1) + 1;
}

/** 这一行到光标为止的缩进。 */
export function indentAt(text: string, at: number): string {
  const line = text.slice(lineStartOf(text, at), at);
  return /^[ \t]*/.exec(line)?.[0] ?? '';
}

/**
 * 回车：新的一行站在上一行的缩进上；上一行以 `{`、`[`、`(` 或 `:` 收尾时再缩进一格；
 * 光标前后正好是一对括号时，先缩进一格再补一行收尾，光标停在中间那一行。
 */
export function onEnter(text: string, start: number, end: number = start): Edit {
  const indent = indentAt(text, start);
  const line = text.slice(lineStartOf(text, start), start);
  const before = line.replace(/[ \t]+$/, '');
  const after = text.slice(end);
  const last = before.slice(-1);
  const opensBlock = last === '{' || last === '[' || last === '(' || last === ':';
  const next = after[0] ?? '';

  if (start === end && opensBlock && (next === '}' || next === ']' || next === ')')) {
    const inner = indent + INDENT;
    return {
      text: text.slice(0, start) + `\n${inner}\n${indent}` + after,
      caret: start + 1 + inner.length,
    };
  }

  const nextIndent = opensBlock ? indent + INDENT : indent;
  return {
    text: text.slice(0, start) + `\n${nextIndent}` + after,
    caret: start + 1 + nextIndent.length,
  };
}

/**
 * 打括号与引号。
 *
 * 交回 `null` 就是「照常输入」——不是所有键都该被接管，接管得越多，越容易在
 * 某个没想到的位置把用户想打的字挤到括号外面去。
 */
export function onBracket(
  text: string,
  start: number,
  end: number,
  typed: string
): Edit | null {
  // 有选区时不接管：默认行为是把选区整段换掉，那才是读者按下这一刻想要的。
  if (start !== end) return null;

  const next = text[start] ?? '';

  // 打的是右括号：右边已经站着一个同样的，跨过去，而不是插进它前面。
  if (typed === ')' || typed === ']' || typed === '}') {
    return next === typed ? { text, caret: start + 1 } : null;
  }

  const closer = CLOSERS[typed];
  if (!closer) return null;

  // 光标右边已经站着配对的另一半（引号算同一种身份）：跨过去，不再补一个。
  if (typed === closer) {
    if (next === typed) return { text, caret: start + 1 };
  } else if (next === closer) {
    return { text, caret: start + 1 };
  }

  // 右边紧挨着字词时不补：`(x` 补成 `(x)` 会把正要打的字留在括号外面。
  if (/[A-Za-z0-9_]/.test(next)) return null;

  return {
    text: text.slice(0, start) + typed + closer + text.slice(start),
    caret: start + 1,
  };
}

/**
 * 第 `line` 行（从 1 开始）的行首下标。越界时夹到首行/末行：读者输入一个
 * 大于总行数的数，落点该是最后一行，而不是什么都不做。
 */
export function offsetOfLine(text: string, line: number): number {
  const lines = text.split('\n');
  const target = Math.min(Math.max(Math.floor(line), 1), lines.length);
  let offset = 0;
  for (let i = 0; i < target - 1; i += 1) offset += lines[i].length + 1;
  return offset;
}
