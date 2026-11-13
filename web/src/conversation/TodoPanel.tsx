import type { ToolCallRecord } from '@/store/streamStore';

/**
 * 一次工作里「还剩几步」的那张单子。
 *
 * 待办不是一次工具调用：模型每走一步都把整张单子重写一遍（`todo` 的语义就是
 * 「整体替换」），所以同一位写者的五次 `todo` 调用是同一张单子的五个版本。抄在
 * 对话流里，读者要在五份原文之间自己找哪份是最新的、哪几行已经做完。这里把它们
 * 折成一块面板：只画最后落地的那一版，后续每次重写原地更新，不新增一行。
 *
 * 两位写者（这段对话自己一个、它派出去的执行者一个）各是各的单子：服务端的
 * 清单按调用者分账（`crates/cog-agent/src/tools.rs` 的 `todo_book`），屏幕上
 * 合成一张就等于两张单子互相覆盖。
 */

/** 写待办的那只工具的名字。与 `tools.rs` 里注册的名字是同一个字。 */
const TODO_TOOL = 'todo';

export interface TodoItem {
  content: string;
  /**
   * 服务端写的那一个字，语出 `tools.rs` 里 `status` 的那个 enum
   * （`pending` / `in_progress` / `done`）。
   *
   * 不写死成联合类型：服务端哪天多加一种，闭集会让它在编译期消失，或者被折进
   * `else` 那一支画成「还没开始」。这里让它原样流过来，由下面的 `marker` 把
   * 认不出来的那一种原字写出来。
   */
  status: string;
}

/** 一位写者手上那一版清单。 */
export interface TodoList {
  /** 写这张单子的是谁；`null` 是总线没带调用者身份的那一次。 */
  agent: string | null;
  items: TodoItem[];
  /** 最后一次写入还没回来：画着的这一版是上一版。 */
  pending: boolean;
  /** 最后一次写入失败了：服务端手上的仍是这一版，模型以为的那一版没写进去。 */
  failed: boolean;
  /**
   * 这一版能不能读全。
   *
   * 工具记录在总线上按一格 8000 字留，超了截断并在末尾写明——一份五十项的清单
   * 有可能超过这一格，那时原文是半截 JSON，解析不出来。这时面板要写「这一版读
   * 不全」，而不是安静地不出现：后者看上去与「模型没有写过待办」一模一样。
   */
  read: boolean;
  /** 画着的这一版是什么时候写的。 */
  at: string;
}

function parseJson(value: string): unknown {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

/**
 * 一条工具记录里的清单。
 *
 * 两条来路都认：回来的那一版（`result.items`，工具亲口说写进去的是什么）与
 * 入参那一版（`args.items`，模型打算写什么）。两者的差别只在一次还没回来或
 * 失败了的写入上，那时屏幕上要画的是服务端手上那一版，取哪一版由调用方定。
 */
function itemsOf(value: string): TodoItem[] | null {
  const parsed = parseJson(value);
  const raw = (parsed as { items?: unknown } | null)?.items;
  if (!Array.isArray(raw)) return null;
  const items: TodoItem[] = [];
  for (const entry of raw) {
    const content = (entry as { content?: unknown } | null)?.content;
    const status = (entry as { status?: unknown } | null)?.status;
    if (typeof content !== 'string' || content.trim().length === 0) continue;
    // 认不出来的状态不往 `pending` 上折：那会把一次服务端改名画成「还没开始做」。
    // 原样带着它，界面上多出一个没登记的字，也比悄悄改掉它好找。
    items.push({ content, status: typeof status === 'string' ? status : '' });
  }
  return items;
}

/**
 * 把一段对话里的工具调用折成一张张清单：一位写者一张，只留最后落地的版本。
 *
 * 只认 `status === 'ok'` 的那次返回作为「服务端手上的那一版」。最后一次调用还在
 * 跑、或者失败了的时候，画的是上一次成功的那一版，并分别标上「正在写」与
 * 「没写进去」——把模型想写的那一版画成已写，等于替一次失败的写入背书。
 */
export function todoListsIn(calls: ToolCallRecord[]): TodoList[] {
  const byWriter = new Map<string, ToolCallRecord[]>();
  for (const call of calls) {
    if (call.name !== TODO_TOOL) continue;
    const key = call.agent ?? '';
    const list = byWriter.get(key);
    if (list) list.push(call);
    else byWriter.set(key, [call]);
  }

  const out: TodoList[] = [];
  for (const [key, calls] of byWriter) {
    // 进来的这一列是这一段的调用，顺序不定；按开始时刻排一遍，最后一次写入
    // 才是最后的那条。
    const ordered = [...calls].sort((a, b) => Date.parse(a.startedAt) - Date.parse(b.startedAt));
    const newest = ordered[ordered.length - 1];

    let settled: { call: ToolCallRecord; items: TodoItem[] } | null = null;
    for (const call of ordered) {
      if (call.status !== 'ok') continue;
      const items = itemsOf(call.result);
      if (items) settled = { call, items };
    }

    if (settled) {
      const newer = newest !== settled.call;
      out.push({
        agent: key === '' ? null : key,
        items: settled.items,
        pending: newer && newest.status === 'running',
        failed: newer && newest.status === 'error',
        at: settled.call.startedAt,
        read: true,
      });
      continue;
    }

    // 一次都没成功过：只有入参可看。它画的是「模型想写的那张」，标出来。
    const attempted = itemsOf(newest.args);
    out.push({
      agent: key === '' ? null : key,
      items: attempted ?? [],
      pending: newest.status === 'running',
      failed: newest.status === 'error',
      at: newest.startedAt,
      // 入参也读不出来（原文被截断）时留着这一块：让它写「读不全」，而不是让
      // 这块面板凭空消失——消失与「没写过待办」在屏幕上是一样的。
      read: attempted !== null,
    });
  }
  return out;
}

/**
 * 一块待办面板。
 *
 * 与对话流分开画，因为它是「这件事走到哪了」的现况，不是某一次执行的记录：清单
 * 贴在输入框上方，往下翻历史时它也不跟着走。完成的项划掉而不是消失——消失之后
 * 读的人只能从「还剩几项」推回去，而划掉的每一行都是一次已经做完的活。
 */
/** 一种状态在屏幕上怎么画：符号、颜色、行尾多的那几个字。 */
function marker(status: string): { glyph: string; color: string; note: string | null } {
  if (status === 'done') return { glyph: '✓', color: 'var(--ok)', note: null };
  if (status === 'in_progress') return { glyph: '▸', color: 'var(--brass)', note: '进行中' };
  if (status === 'pending') return { glyph: '·', color: 'var(--muted)', note: null };
  // 认不出来的那一种原字写出来：画成「还没开始」就是替一次服务端改名撒谎，
  // 而这一行看上去会完全正常。
  return { glyph: '?', color: 'var(--warn)', note: status || '没有状态' };
}

export function TodoPanel({ list }: { list: TodoList }) {
  // 读不全时只留一行字：这一块的存在本身就是「它写过待办」的证据。
  if (!list.read) {
    return (
      <div className="mb-1 flex flex-wrap items-baseline gap-2">
        <span className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
          待办
        </span>
        <span className="mono text-[10px]" style={{ color: 'var(--warn)' }}>
          这一版读不全（原文在总线上按格截断了）
        </span>
        {list.agent && (
          <span className="mono text-[10px] text-[var(--muted)]">写者 {list.agent.slice(0, 8)}</span>
        )}
      </div>
    );
  }
  // 空清单是模型把这张单子清掉的那一版，不是「它没有待办」：没有待办的时候
  // 它根本不调这只工具，一条调用都不会有。
  if (list.items.length === 0) return null;

  const done = list.items.filter((item) => item.status === 'done').length;
  const inProgress = list.items.filter((item) => item.status === 'in_progress').length;
  return (
    <div className="mb-1">
      <div className="mb-1 flex flex-wrap items-baseline gap-2">
        <span className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
          待办
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">
          {list.items.length} 项 · {done} 完成 · {inProgress} 进行中
        </span>
        {list.agent && (
          <span className="mono text-[10px] text-[var(--muted)]">写者 {list.agent.slice(0, 8)}</span>
        )}
        {list.pending && (
          <span className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
            正在写下一版…
          </span>
        )}
        {list.failed && (
          <span className="mono text-[10px]" style={{ color: 'var(--warn)' }}>
            最后一次更新没成功，这里是上一版
          </span>
        )}
      </div>
      <ul className="flex flex-col gap-0.5">
        {list.items.map((item, index) => {
          const { glyph, color, note } = marker(item.status);
          return (
            <li key={`${index}-${item.content}`} className="flex items-baseline gap-2 text-[12px]">
              <span className="mono w-3 shrink-0" style={{ color }}>
                {glyph}
              </span>
              <span
                className={
                  item.status === 'done'
                    ? 'text-[var(--muted)] line-through'
                    : item.status === 'in_progress'
                      ? 'text-[var(--paper)]'
                      : 'text-[var(--paper-dim)]'
                }
              >
                {item.content}
              </span>
              {note && (
                <span className="mono text-[10px]" style={{ color }}>
                  {note}
                </span>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}
