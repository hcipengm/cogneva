import DOMPurify from 'dompurify';
import hljs from 'highlight.js/lib/common';
import { Marked } from 'marked';

/**
 * 一段回复的正文怎么画。
 *
 * 回复里带的是 Markdown：标题、列表、表格、行内代码、围起来的一整段代码，还有
 * 变更的 diff。按纯文本画出来，读者看到的是一堆 `**` 与两排 ```——一段贴回来的
 * 补丁要靠人自己对齐正负号。这里把它画成它本来的样子。
 *
 * 两件事是硬的：
 *
 * 1. **过一遍消毒**。这些字是模型写的，而模型读过仓库、网页、别人发来的消息：
 *    间接提示注入可以让一段回复里出现 `<img onerror=…>`。这一面在一个已经登录
 *    的浏览器里跑，不消毒等于把 XSS 交给对话内容。所以渲染出来的 HTML 一律经
 *    DOMPurify；`javascript:` 之类的链接由它整条拿掉。
 * 2. **认不出来的语言按原文画**。高亮器不认识的那种语言，硬套一个语法只会把
 *    关键字染错色；原文至少是准的。
 */

/** 文本按 HTML 转义。代码块与 diff 里的每一个字符都要走这一道。 */
function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * 一段 diff 按行上色。
 *
 * 认 `+` / `-` 开头的那两种行；`+++` / `---` 是文件头，标成中性；`@@` 是块头。
 * 一行一个 `<span>`，而不是给整块套一个颜色：整块染色之后，读者分不出哪几行是
 * 新加的。
 */
function diffLines(code: string): string {
  return code
    .split('\n')
    .map((line) => {
      const tone = line.startsWith('+++') || line.startsWith('---')
        ? 'diff-meta'
        : line.startsWith('+')
          ? 'diff-add'
          : line.startsWith('-')
            ? 'diff-del'
            : line.startsWith('@@')
              ? 'diff-meta'
              : 'diff-same';
      // 空行也要占一行高：拼回去的时候靠 `\n` 分段。
      return `<span class="${tone}">${escapeHtml(line) || ' '}</span>`;
    })
    .join('\n');
}

/** 围起来的一段代码：认语言的走高亮，diff 走正负号着色，其余按原文。 */
function codeBlock(code: string, lang: string | undefined): string {
  const language = (lang ?? '').trim().split(/\s+/)[0]?.toLowerCase() ?? '';
  if (language === 'diff') {
    return `<pre class="md-code md-diff"><code>${diffLines(code)}</code></pre>`;
  }
  if (language && hljs.getLanguage(language)) {
    let highlighted: string;
    try {
      highlighted = hljs.highlight(code, { language }).value;
    } catch {
      highlighted = escapeHtml(code);
    }
    return `<pre class="md-code"><code class="hljs">${highlighted}</code></pre>`;
  }
  return `<pre class="md-code"><code>${escapeHtml(code)}</code></pre>`;
}

const marked = new Marked({
  gfm: true,
  // 聊天里换行就是一个换行：不把两句话合成一段。
  breaks: true,
  renderer: {
    code(token) {
      return codeBlock(token.text, token.lang);
    },
    link(token) {
      // 同一段对话里的链接开新窗口：点一下不该把这一面（以及还没收尾的那一轮）
      // 换掉。
      const text = this.parser.parseInline(token.tokens);
      return `<a href="${token.href}" target="_blank" rel="noopener noreferrer">${text}</a>`;
    },
  },
});

/** 一段 Markdown 变成可以放进页面的 HTML。 */
export function renderMarkdown(text: string): string {
  const html = marked.parse(text) as string;
  return DOMPurify.sanitize(html, {
    // 链接的 `target` 由上面那一处渲染器写；其余属性与标签按 DOMPurify 的默认
    // 白名单走。
    ADD_ATTR: ['target'],
    // 行内样式不认：模型写出来的 `style` 可以盖住整页的布局。
    FORBID_ATTR: ['style'],
    FORBID_TAGS: ['style', 'form', 'input', 'button', 'iframe', 'object', 'embed'],
  });
}
