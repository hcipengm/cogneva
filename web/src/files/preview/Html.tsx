/**
 * HTML 文件的阅读面。
 *
 * `sandbox=""` 是空的：一份工作区里的 HTML 可以是任何人写进来的，它在这里只
 * 该被「看」，不该能发请求、能读本页的存储、能跑脚本。空 sandbox 把脚本、表单、
 * 同源访问一起关掉，代价是有些页面会只剩静态骨架——这个代价要付。
 *
 * 放在 iframe 里而不是塞进 DOM：样式是另一份世界的事，一份带全局选择器的 HTML
 * 会把控制台自己的排版一起改掉。
 */
export function Html({ source, title }: { source: string; title: string }) {
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex shrink-0 items-center gap-2 border-b border-[var(--line)] px-4 py-1">
        <span className="text-[10px] text-[var(--muted)]">脚本、表单、同源访问已关闭</span>
      </div>
      <iframe
        title={title}
        sandbox=""
        srcDoc={source}
        className="min-h-0 flex-1 border-0 bg-white"
      />
    </div>
  );
}
