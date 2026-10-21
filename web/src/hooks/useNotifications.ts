import { useCallback, useEffect, useRef, useState } from 'react';
import {
  listNotifications,
  markAllNotificationsRead,
  markNotificationRead,
  type NotificationItem,
  type NotificationPage,
} from '@/api/client';

/** 面板里保留的行数上限：比网关默认页大，又不至于随会话无限长。 */
const MAX_ITEMS = 200;

/**
 * 通知中心的状态：列表端点给的存量 ＋ WebSocket 推来的增量。
 *
 * 两条路必须合成一个列表：轮询保证「刷新后还在」，推送保证「不用刷新就亮」。
 * 推送与轮询会撞同一条（推送到达时列表轮询恰好也带上了它），所以合并按 id
 * 去重——重复行比丢行好查，但没理由留着。
 *
 * `error` 与「没有通知」分开：读失败时列表清空会显示成「一切都好」，
 * 而这一面恰恰是通知没送到时才有人打开的那一面。
 */
export function useNotifications(enabled: boolean, pollMs = 60_000) {
  const [page, setPage] = useState<NotificationPage | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const seenIds = useRef<Set<string>>(new Set());

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const next = await listNotifications();
      seenIds.current = new Set(next.items.map((n) => n.id));
      setPage(next);
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setPage(null);
      setError(
        err?.message?.trim() || `读不到通知（HTTP ${err?.status ?? '?'}）`
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!enabled) return;
    refresh();
    const timer = window.setInterval(refresh, pollMs);
    return () => window.clearInterval(timer);
  }, [enabled, refresh, pollMs]);

  /** 把一条推送并进列表；已经出现过（推送与轮询撞车）就原样返回。 */
  const receive = useCallback((notification: NotificationItem) => {
    if (seenIds.current.has(notification.id)) return;
    seenIds.current.add(notification.id);
    setPage((prev) => {
      if (!prev) {
        return {
          items: [notification],
          unread_count: notification.is_read ? 0 : 1,
          next_cursor: null,
          has_more: false,
        };
      }
      return {
        ...prev,
        items: [notification, ...prev.items].slice(0, MAX_ITEMS),
        unread_count: prev.unread_count + (notification.is_read ? 0 : 1),
      };
    });
  }, []);

  const markRead = useCallback(
    async (id: string) => {
      const before = findItem(page, id);
      setPage((prev) => markOneRead(prev, id));
      try {
        await markNotificationRead(id);
      } catch (e) {
        // 失败就把这一行放回未读，并让下一次轮询把真值取回来。
        setPage((prev) => restoreUnread(prev, before));
        setError((e as { message?: string })?.message?.trim() || '标记已读失败');
      }
    },
    [page]
  );

  const markAllRead = useCallback(async () => {
    const before = page;
    setPage((prev) => markEveryRead(prev));
    try {
      await markAllNotificationsRead();
    } catch (e) {
      setPage(before);
      setError((e as { message?: string })?.message?.trim() || '全部已读失败');
    }
  }, [page]);

  return {
    items: page?.items ?? [],
    unreadCount: page?.unread_count ?? 0,
    hasMore: page?.has_more ?? false,
    loading,
    error,
    refresh,
    receive,
    markRead,
    markAllRead,
  };
}

function findItem(
  page: NotificationPage | null,
  id: string
): NotificationItem | undefined {
  return page?.items.find((n) => n.id === id);
}

function markOneRead(
  page: NotificationPage | null,
  id: string
): NotificationPage | null {
  if (!page) return page;
  let decremented = false;
  const items = page.items.map((n) => {
    if (n.id !== id || n.is_read) return n;
    decremented = true;
    return { ...n, is_read: true, read_at: new Date().toISOString() };
  });
  return {
    ...page,
    items,
    unread_count: Math.max(0, page.unread_count - (decremented ? 1 : 0)),
  };
}

function markEveryRead(page: NotificationPage | null): NotificationPage | null {
  if (!page) return page;
  const now = new Date().toISOString();
  return {
    ...page,
    items: page.items.map((n) =>
      n.is_read ? n : { ...n, is_read: true, read_at: now }
    ),
    unread_count: 0,
  };
}

function restoreUnread(
  page: NotificationPage | null,
  before: NotificationItem | undefined
): NotificationPage | null {
  if (!page || !before || before.is_read) return page;
  return {
    ...page,
    items: page.items.map((n) =>
      n.id === before.id && n.is_read ? { ...n, is_read: false, read_at: null } : n
    ),
    unread_count: page.unread_count + 1,
  };
}
