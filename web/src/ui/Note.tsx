import {
  createElement,
  useEffect,
  useId,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent,
  type MouseEvent,
  type ReactNode,
} from 'react';

/** 注释长成它本来该是的那一格，而不是在外面再套一层。 */
type NoteTag = 'span' | 'div' | 'p' | 'td' | 'h1' | 'h2' | 'button';

/**
 * 一条注释：悬停或聚焦时浮出来，面板里的字能整段选中、复制。
 *
 * 原生 `title=` 做不到这件事：提示框由浏览器画在页面之外的一层，选不中、复制不了，
 * 出现前还有一段系统延迟；它也不说清自己属于哪一格，同一排里两个 `title` 只能挨个
 * 去试。这里把注释做成页面内的面板，锚在触发元素旁边：它是触发元素的子节点，鼠标
 * 移进去不会关掉，所以能从容地选字。
 *
 * 面板用 `position: fixed` 且坐标由触发元素的包围盒算出——`absolute` 会被表格与
 * 面板自己的滚动容器裁掉，那些容器恰恰是注释最多的地方。
 *
 * `as` 让注释就是那个元素本身（表格里的 `<td>`、标题、按钮），不额外包一层可聚焦的
 * 壳：壳包在按钮外面会让 Tab 要按两下，还会在可聚焦元素里再塞一个可聚焦元素。
 * 元素自己的属性（`role`、`aria-*`、`disabled`、`onClick`…）原样透传。
 *
 * `text` 为空（`undefined` / 空串）时不渲染面板——调用点常见的写法是「有条件才有
 * 注释」，条件不成立时这一格与没有注释的格子逐字一样。
 */
export function Note({
  as: Tag = 'span',
  text,
  children,
  ...rest
}: {
  as?: NoteTag;
  text: ReactNode;
  children?: ReactNode;
  onClick?: (event: MouseEvent<HTMLElement>) => void;
  onKeyDown?: (event: KeyboardEvent<HTMLElement>) => void;
} & Record<string, unknown>) {
  const id = useId();
  const [anchor, setAnchor] = useState<{ left: number; top: number; above: boolean } | null>(null);
  const host = useRef<HTMLElement | null>(null);
  const noted = text !== null && text !== undefined && !(typeof text === 'string' && text.trim() === '');

  const show = () => {
    const node = host.current;
    if (!node) return;
    const box = node.getBoundingClientRect();
    // 贴右边界的那一格往右长会跑出窗口，改成从右沿往左长；下沿放不下就翻到上方。
    const left = Math.max(8, Math.min(box.left, window.innerWidth - 336));
    const above = box.bottom + 180 > window.innerHeight;
    setAnchor({ left, top: above ? box.top - 4 : box.bottom + 4, above });
  };
  const close = () => setAnchor(null);

  // 滚动时锚点会飘到别的行上去：一块浮在错误位置上的注释比没有更坏，直接收起。
  useEffect(() => {
    if (!anchor) return;
    window.addEventListener('scroll', close, true);
    return () => window.removeEventListener('scroll', close, true);
  }, [anchor]);

  const {
    className,
    style,
    tabIndex,
    onKeyDown,
    ...attrs
  } = rest as {
    className?: string;
    style?: CSSProperties;
    tabIndex?: number;
    onKeyDown?: (event: KeyboardEvent<HTMLElement>) => void;
  };

  // 调用点自己的键盘处理（回车、空格打开一格）不能被注释抢走：两者都跑。
  const keyDown = noted
    ? (event: KeyboardEvent<HTMLElement>) => {
        if (event.key === 'Escape') close();
        onKeyDown?.(event);
      }
    : onKeyDown;

  return createElement(
    Tag,
    {
      ...attrs,
      ref: host,
      className: className ? `note ${className}` : 'note',
      style: { position: 'relative', ...style },
      // 按钮、链接本来就进得了 Tab，再给一个 tabIndex 就是两站。
      tabIndex: Tag === 'button' ? tabIndex : (tabIndex ?? 0),
      'aria-describedby': noted ? id : undefined,
      onMouseEnter: noted ? show : undefined,
      onMouseLeave: noted ? close : undefined,
      onFocus: noted ? show : undefined,
      onBlur: noted ? close : undefined,
      onKeyDown: keyDown,
    },
    children,
    noted && anchor
      ? createElement(
          'span',
          {
            id,
            role: 'note',
            className: 'note-panel',
            style: {
              left: anchor.left,
              top: anchor.top,
              transform: anchor.above ? 'translateY(-100%)' : undefined,
            },
          },
          text
        )
      : null
  );
}
