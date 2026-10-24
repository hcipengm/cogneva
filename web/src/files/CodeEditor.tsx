import { useEffect, useMemo, useRef } from 'react';
import { offsetOfLine, onBracket, onEnter } from '@/files/editing';
import { TOKEN_VAR, findMatches, languageOf, tokenize } from '@/files/highlight';

/**
 * 代码编辑器：着色层 + 输入层 + 行号槽。
 *
 * 用「透明的 textarea 盖在着色好的 pre 上」而不是引一个编辑器框架：输入法、
 * 撤销、选中、移动端软键盘这些行为浏览器已经做对了，自己实现只会做错一部分。
 * 代价是两层的字体度量必须逐项相同（字号、行高、内边距、制表符宽度），
 * 差一个像素光标就会跑到字旁边去——所以这些值只在 `METRICS` 里写一次。
 */
const METRICS = {
  fontFamily: 'var(--font-mono)',
  fontSize: '12px',
  lineHeight: '1.65',
  padding: '8px 12px',
  tabSize: 2,
} as const;

export function CodeEditor({
  path,
  text,
  readOnly,
  onChange,
  findQuery,
  findIndex,
  goto,
}: {
  path: string;
  text: string;
  readOnly?: boolean;
  onChange: (next: string) => void;
  /**
   * 查找状态由外面持着：计数与上一处 / 下一处要画在同一根工具条上。
   * 两个原语而不是一个对象——对象每次渲染都是新的，跳转的 effect 会因此
   * 每帧重跑，选中区被反复重置，打字打不进去。
   */
  findQuery?: string;
  findIndex?: number;
  /**
   * 跳到某一行。带一个每次都换的 `serial`：跳到同一行第二次时，值没变而
   * 光标已经被挪开过，只比行号的话那一次跳转不会发生。
   */
  goto?: { line: number; serial: number } | null;
}) {
  const areaRef = useRef<HTMLTextAreaElement>(null);
  const preRef = useRef<HTMLPreElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);

  /** 键盘接管之后要把光标放回去：受控的 textarea 重渲染会把光标送回末尾。 */
  const pendingCaret = useRef<number | null>(null);

  const tokens = useMemo(() => tokenize(text, languageOf(path)), [text, path]);
  const lineCount = useMemo(() => text.split('\n').length, [text]);

  // 跳到当前匹配处：选中它，并把那一行滚到视野里。
  useEffect(() => {
    const area = areaRef.current;
    if (!area || !findQuery) return;
    const matches = findMatches(text, findQuery);
    if (matches.length === 0) return;
    const raw = findIndex ?? 0;
    const index = ((raw % matches.length) + matches.length) % matches.length;
    const [start, end] = matches[index];
    area.focus();
    area.setSelectionRange(start, end);
    const line = text.slice(0, start).split('\n').length - 1;
    const lineHeight = 12 * 1.65;
    area.scrollTop = Math.max(0, line * lineHeight - area.clientHeight / 3);
    if (preRef.current) {
      preRef.current.scrollTop = area.scrollTop;
      preRef.current.scrollLeft = area.scrollLeft;
    }
    if (gutterRef.current) gutterRef.current.scrollTop = area.scrollTop;
  }, [findQuery, findIndex, text]);

  // 打字接管之后落光标。放在正文变化之后跑：React 把新值写进 textarea 之后
  // 才轮得到设置选区，早一步设会被下一次重渲染覆盖。
  useEffect(() => {
    const area = areaRef.current;
    if (!area || pendingCaret.current === null) return;
    const at = pendingCaret.current;
    pendingCaret.current = null;
    area.selectionStart = at;
    area.selectionEnd = at;
  }, [text]);

  // 跳到第几行：光标落在行首，并把那一行滚进视野。
  useEffect(() => {
    const area = areaRef.current;
    if (!area || !goto) return;
    const at = offsetOfLine(text, goto.line);
    area.focus();
    area.setSelectionRange(at, at);
    const lineHeight = 12 * 1.65;
    area.scrollTop = Math.max(0, (Math.max(goto.line, 1) - 1) * lineHeight - area.clientHeight / 3);
    syncScroll();
    // `text` 故意不进依赖：改正文不该把光标拽回上一次跳转的那一行。
  }, [goto?.serial]);

  const syncScroll = () => {
    const area = areaRef.current;
    if (!area) return;
    if (preRef.current) {
      preRef.current.scrollTop = area.scrollTop;
      preRef.current.scrollLeft = area.scrollLeft;
    }
    if (gutterRef.current) {
      gutterRef.current.scrollTop = area.scrollTop;
    }
  };

  return (
    <div className="relative flex min-h-0 flex-1 overflow-hidden bg-[var(--ink-900)]">
      <div
        ref={gutterRef}
        aria-hidden="true"
        className="mono shrink-0 select-none overflow-hidden border-r border-[var(--line)] text-right text-[var(--muted)]"
        style={{
          fontFamily: METRICS.fontFamily,
          fontSize: METRICS.fontSize,
          lineHeight: METRICS.lineHeight,
          padding: '8px 8px 8px 12px',
          background: 'var(--ink-800)',
        }}
      >
        {Array.from({ length: lineCount }, (_, i) => (
          <div key={i}>{i + 1}</div>
        ))}
      </div>

      <div className="relative min-w-0 flex-1">
        <pre
          ref={preRef}
          aria-hidden="true"
          className="pointer-events-none absolute inset-0 overflow-hidden whitespace-pre"
          style={{
            fontFamily: METRICS.fontFamily,
            fontSize: METRICS.fontSize,
            lineHeight: METRICS.lineHeight,
            padding: METRICS.padding,
            tabSize: METRICS.tabSize,
            margin: 0,
          }}
        >
          {tokens.map((token, i) => (
            <span key={i} style={{ color: TOKEN_VAR[token.kind] }}>
              {token.text}
            </span>
          ))}
          {'\n'}
        </pre>
        <textarea
          ref={areaRef}
          value={text}
          readOnly={readOnly}
          spellCheck={false}
          autoCapitalize="off"
          autoCorrect="off"
          onChange={(event) => onChange(event.target.value)}
          onScroll={syncScroll}
          onKeyDown={(event) => {
            if (readOnly) return;
            // 回车与括号由这一层算：缩进与配对是编辑器的行为，不是浏览器的。
            if (event.key === 'Enter' && !event.shiftKey && !event.altKey) {
              event.preventDefault();
              const area = event.currentTarget;
              const edit = onEnter(text, area.selectionStart, area.selectionEnd);
              pendingCaret.current = edit.caret;
              onChange(edit.text);
              return;
            }
            if (event.key.length === 1 && !event.metaKey && !event.ctrlKey && !event.altKey) {
              const area = event.currentTarget;
              const edit = onBracket(text, area.selectionStart, area.selectionEnd, event.key);
              if (edit) {
                event.preventDefault();
                pendingCaret.current = edit.caret;
                onChange(edit.text);
                return;
              }
            }
            if (event.key !== 'Tab') return;
            event.preventDefault();
            const area = event.currentTarget;
            const { selectionStart: start, selectionEnd: end } = area;
            const next = `${text.slice(0, start)}  ${text.slice(end)}`;
            onChange(next);
            // 光标要落在插入的两个空格之后，否则连按 Tab 会一直插在同一个位置。
            requestAnimationFrame(() => {
              area.selectionStart = area.selectionEnd = start + 2;
            });
          }}
          className="absolute inset-0 h-full w-full resize-none overflow-auto whitespace-pre bg-transparent outline-none"
          style={{
            fontFamily: METRICS.fontFamily,
            fontSize: METRICS.fontSize,
            lineHeight: METRICS.lineHeight,
            padding: METRICS.padding,
            tabSize: METRICS.tabSize,
            // 字由下面那层画，这里只留光标与选区。
            color: 'transparent',
            caretColor: 'var(--paper)',
          }}
        />
      </div>
    </div>
  );
}
