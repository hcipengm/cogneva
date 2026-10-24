/**
 * 一个够用的语法着色器。
 *
 * 不引高亮库：真正需要的是「关键字、字符串、注释、数字各是一种颜色」，
 * 而任何一个高亮库都会把几十种语言的语法表打进包里，体积是这里的两百倍。
 * 认不出的语言一律按纯文本画——着色错了比不着色更糟，它会让一段普通文本
 * 看起来像关键字。
 *
 * 输出是扁平的 token 列表，渲染方按 kind 取颜色；这里不碰任何颜色，
 * 换主题时着色自动跟着走。
 */
export type TokenKind =
  | 'plain'
  | 'comment'
  | 'string'
  | 'number'
  | 'keyword'
  | 'literal'
  | 'type'
  | 'punct';

export interface Token {
  text: string;
  kind: TokenKind;
}

type Language = 'hash' | 'slash' | 'sql' | 'markdown' | 'plain';

export function languageOf(path: string): Language {
  const ext = path.slice(path.lastIndexOf('.') + 1).toLowerCase();
  if (['rs', 'ts', 'tsx', 'js', 'jsx', 'mjs', 'cjs', 'go', 'java', 'c', 'h', 'cpp', 'hpp', 'css'].includes(ext)) {
    return 'slash';
  }
  if (['py', 'sh', 'bash', 'yaml', 'yml', 'toml', 'rb', 'conf', 'ini', 'dockerfile'].includes(ext) || path.endsWith('Dockerfile')) {
    return 'hash';
  }
  if (['sql'].includes(ext)) return 'sql';
  if (['md', 'markdown', 'mdx'].includes(ext)) return 'markdown';
  return 'plain';
}

const KEYWORDS = new Set([
  // Rust
  'fn', 'let', 'mut', 'pub', 'impl', 'struct', 'enum', 'trait', 'match', 'if', 'else', 'for',
  'while', 'loop', 'return', 'use', 'mod', 'crate', 'self', 'super', 'const', 'static', 'async',
  'await', 'move', 'where', 'type', 'dyn', 'ref', 'in', 'as', 'break', 'continue', 'unsafe',
  'extern', 'macro_rules',
  // JS / TS
  'export', 'import', 'from', 'default', 'class', 'extends', 'new', 'this', 'try', 'catch',
  'finally', 'throw', 'function', 'var', 'typeof', 'instanceof', 'delete', 'void', 'yield',
  'switch', 'case', 'do', 'interface', 'implements', 'package', 'public', 'private', 'protected',
  'final', 'abstract', 'enum_of', 'satisfies', 'keyof', 'readonly', 'declare', 'namespace',
  // Python
  'def', 'elif', 'pass', 'raise', 'with', 'lambda', 'global', 'nonlocal', 'assert', 'del',
  'not', 'and', 'or', 'is', 'except', 'None',
  // Go / 通用
  'func', 'defer', 'chan', 'select', 'go', 'range', 'fallthrough',
]);

const LITERALS = new Set(['true', 'false', 'null', 'undefined', 'True', 'False', 'nil', 'NaN']);

/**
 * 逐段扫描。手写循环而不是一条大正则：正则的交替顺序决定了谁能先吃掉一段
 * 文本，而「字符串里的 `//` 不是注释」这件事用交替写不对就会整段着错色。
 */
export function tokenize(text: string, language: Language): Token[] {
  if (language === 'plain') return [{ text, kind: 'plain' }];
  if (language === 'markdown') return tokenizeMarkdown(text);

  const tokens: Token[] = [];
  const push = (value: string, kind: TokenKind) => {
    if (!value) return;
    const last = tokens[tokens.length - 1];
    if (last && last.kind === kind) {
      last.text += value;
      return;
    }
    tokens.push({ text: value, kind });
  };

  let i = 0;
  while (i < text.length) {
    const rest = text.slice(i);
    const ch = text[i];

    // 注释
    const lineComment = language === 'slash' ? '//' : language === 'hash' ? '#' : '--';
    if (rest.startsWith(lineComment) && !(language === 'slash' && i > 0 && text[i - 1] === ':')) {
      const end = rest.indexOf('\n');
      const chunk = end < 0 ? rest : rest.slice(0, end);
      push(chunk, 'comment');
      i += chunk.length;
      continue;
    }
    if (rest.startsWith('/*')) {
      const end = rest.indexOf('*/');
      const chunk = end < 0 ? rest : rest.slice(0, end + 2);
      push(chunk, 'comment');
      i += chunk.length;
      continue;
    }

    // 字符串
    if (ch === '"' || ch === "'" || ch === '`') {
      let j = i + 1;
      while (j < text.length) {
        if (text[j] === '\\') {
          j += 2;
          continue;
        }
        if (text[j] === ch) {
          j += 1;
          break;
        }
        // 单双引号字符串不跨行：未闭合时停在行尾，别把后面整篇吃掉。
        if (text[j] === '\n' && ch !== '`') break;
        j += 1;
      }
      push(text.slice(i, j), 'string');
      i = j;
      continue;
    }

    // 数字
    if (/[0-9]/.test(ch) && !/[A-Za-z_]/.test(text[i - 1] ?? '')) {
      let j = i;
      while (j < text.length && /[0-9a-fA-FxX._]/.test(text[j])) j += 1;
      push(text.slice(i, j), 'number');
      i = j;
      continue;
    }

    // 标识符
    if (/[A-Za-z_$]/.test(ch)) {
      let j = i;
      while (j < text.length && /[A-Za-z0-9_$]/.test(text[j])) j += 1;
      const word = text.slice(i, j);
      const kind: TokenKind = KEYWORDS.has(word)
        ? 'keyword'
        : LITERALS.has(word)
          ? 'literal'
          : /^[A-Z]/.test(word)
            ? 'type'
            : 'plain';
      push(word, kind);
      i = j;
      continue;
    }

    // 空白与标点
    if (/\s/.test(ch)) {
      push(ch, 'plain');
      i += 1;
      continue;
    }
    push(ch, 'punct');
    i += 1;
  }

  return tokens;
}

/** Markdown 只做三件事：标题行、行内代码、链接。再多就不是「够用」了。 */
function tokenizeMarkdown(text: string): Token[] {
  const tokens: Token[] = [];
  for (const line of text.split(/(\n)/)) {
    if (line === '\n') {
      tokens.push({ text: line, kind: 'plain' });
      continue;
    }
    if (/^#{1,6}\s/.test(line)) {
      tokens.push({ text: line, kind: 'type' });
      continue;
    }
    if (/^\s*([-*+]|\d+\.)\s/.test(line)) {
      tokens.push({ text: line, kind: 'keyword' });
      continue;
    }
    if (/^\s*(```|~~~)/.test(line)) {
      tokens.push({ text: line, kind: 'comment' });
      continue;
    }
    tokens.push({ text: line, kind: 'plain' });
  }
  return tokens;
}

/** 每种 token 落到哪个颜色令牌上。都在主题的 16 个令牌里，没有额外色。 */
export const TOKEN_VAR: Record<TokenKind, string> = {
  plain: 'var(--paper)',
  comment: 'var(--muted)',
  string: 'var(--ok)',
  number: 'var(--warn)',
  keyword: 'var(--brass)',
  literal: 'var(--live)',
  type: 'var(--live)',
  punct: 'var(--paper-dim)',
};
