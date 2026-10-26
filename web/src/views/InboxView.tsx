import { useState } from 'react';
import type { NotificationItem } from '@/api/client';
import { useServices } from '@/shell/context';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, ViewFrame } from '@/ui/primitives';
import { BarList, type BarRow } from '@/ui/plots';

/**
 * 收件箱：平台发给你的话。
 *
 * 这就是对外的那个「邮箱」——它不往外发，也不依赖任何外部地址：通知由平台
 * 写进自己的库，界面上读、界面上确认。读失败与「没有通知」分开写，因为这一页
 * 恰恰是「消息没送到」时才有人打开的那一页。
 */
export function InboxView() {
  const { notifications } = useServices();
  const appendOutput = useShell((s) => s.appendOutput);
  const [unreadOnly, setUnreadOnly] = useState(false);

  const items = unreadOnly
    ? notifications.items.filter((n) => !n.is_read)
    : notifications.items;

  const mark = async (id: string) => {
    await notifications.markRead(id);
    appendOutput(`已把通知 ${id} 标为已读`);
  };

  return (
    <ViewFrame
      title="收件箱"
      meta={
        notifications.error
          ? '读数没读到'
          : `${notifications.unreadCount} 条未读 · 共 ${notifications.items.length} 条`
      }
      actions={
        <div className="flex items-center gap-1">
          <Button tone={unreadOnly ? 'accent' : 'plain'} onClick={() => setUnreadOnly((v) => !v)}>
            {unreadOnly ? '只看未读' : '全部'}
          </Button>
          <Button onClick={notifications.refresh} disabled={notifications.loading}>
            {notifications.loading ? '读取中…' : '重读'}
          </Button>
          <Button
            onClick={() => void notifications.markAllRead()}
            disabled={notifications.unreadCount === 0}
          >
            全部已读
          </Button>
        </div>
      }
    >
      {notifications.error ? (
        <ReadError what="通知" error={notifications.error} onRetry={notifications.refresh} />
      ) : notifications.loading && notifications.items.length === 0 ? (
        <Loading what="通知" />
      ) : (
        <>
          <Readings
            from={{ what: notifications.what, at: notifications.at }}
            source="GET /api/v1/notifications"
            caveat="列表这一份按 60 秒重读，推送进来的增量不等轮询；这一格的时刻是列表那一次读的，不是最后一条到达的时刻。"
          >
          <div className="flex flex-wrap border-b border-[var(--line)] py-2">
            <Reading
              label="未读"
              value={notifications.unreadCount}
              tone={notifications.unreadCount ? 'warn' : 'ok'}
            />
            <Reading label="已收" value={notifications.items.length} />
            <Reading label="这一屏" value={items.length} note={unreadOnly ? '只看未读' : '全部'} />
          </div>
          </Readings>
          {/* 「最近哪天来得多、昨天到今天是不是突然涨起来」在列表里读不出来：列表按
              时间倒序，一眼只能看见最上面那几条。按天画成条，涨没涨就是一眼的事。 */}
          <div className="border-b border-[var(--line)]">
            <BarList
              label="最近这些天每天收到几条"
              note="按这一页读回来的条数数（UTC 日期）；更早的那几页不在这一屏里"
              rows={byDay(items)}
            />
          </div>
          <InboxList
            items={items}
            unreadCount={notifications.unreadCount}
            onMarkRead={(id) => void mark(id)}
          />
        </>
      )}
    </ViewFrame>
  );
}

/**
 * 按天收拢这一页的通知。
 *
 * 只数读回来的这一页，不假装是全部历史——分页拉过的日子这里没有行，图上就没有
 * 那一格。缺的那一天画成 0 会把「没读到」说成「那天没有」。
 */
function byDay(items: NotificationItem[]): BarRow[] {
  const counted = new Map<string, number>();
  for (const item of items) {
    const day = item.created_at.slice(0, 10);
    counted.set(day, (counted.get(day) ?? 0) + 1);
  }
  return [...counted.entries()]
    .sort((a, b) => (a[0] < b[0] ? -1 : 1))
    .slice(-14)
    .map(([day, count]) => ({ id: day, label: day.slice(5), value: count }));
}

/**
 * 列表本身。它与未读数一起交到同一屏上：只有列表没有计数时，「有几条没看」
 * 还得数一遍；只有计数没有列表时，没人知道是哪几条。
 */
function InboxList({
  items,
  unreadCount,
  onMarkRead,
}: {
  items: NotificationItem[];
  unreadCount: number;
  onMarkRead: (id: string) => void;
}) {
  if (items.length === 0) {
    return (
      <Empty>
        {unreadCount === 0
          ? '没有未读通知。'
          : '这一屏没有可显示的通知。空列表也可能来自「通知存储没接上」——读失败时上面会单独写一行，不会混在这里。'}
      </Empty>
    );
  }

  return (
    <ul>
      {items.map((item) => (
        <li
          key={item.id}
          className="flex items-start gap-3 border-b border-[var(--line)] px-4 py-2"
          style={{
            background: item.is_read
              ? 'transparent'
              : 'color-mix(in srgb, var(--brass) 5%, transparent)',
          }}
        >
          <span
            className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full"
            style={{ background: item.is_read ? 'var(--line-strong)' : 'var(--brass)' }}
            aria-hidden="true"
          />
          <div className="min-w-0 flex-1">
            <div className="flex items-baseline gap-2">
              <span
                className="text-[12px]"
                style={{ color: item.is_read ? 'var(--paper-dim)' : 'var(--paper)' }}
              >
                {item.title}
              </span>
              <Moment iso={item.created_at} />
              {item.read_at && (
                <span className="text-[10px] text-[var(--muted)]">
                  已于 {new Date(item.read_at).toLocaleString('zh-CN')} 读过
                </span>
              )}
            </div>
            <p className="mt-0.5 whitespace-pre-wrap text-[12px] leading-relaxed text-[var(--paper-dim)]">
              {item.body}
            </p>
          </div>
          {!item.is_read && <Button onClick={() => onMarkRead(item.id)}>标为已读</Button>}
        </li>
      ))}
    </ul>
  );
}
