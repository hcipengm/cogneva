import type { NotificationItem } from '@/api/client';

function stamp(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function Row({
  notification,
  onRead,
}: {
  notification: NotificationItem;
  onRead: (id: string) => void;
}) {
  return (
    <tr className="border-t border-slate-800/60 align-top">
      <td className="py-2 pr-3">
        <span
          className={`inline-block h-2 w-2 rounded-full ${
            notification.is_read ? 'bg-slate-700' : 'bg-sky-400'
          }`}
          title={notification.is_read ? '已读' : '未读'}
        />
      </td>
      <td
        className={`py-2 pr-3 font-medium ${
          notification.is_read ? 'text-slate-400' : 'text-slate-100'
        }`}
      >
        {notification.title || '（无标题）'}
      </td>
      <td className="py-2 pr-3 text-slate-300">{notification.body}</td>
      <td className="py-2 pr-3 text-right tabular-nums text-slate-500">
        {stamp(notification.created_at)}
      </td>
      <td className="py-2 text-right">
        {notification.is_read ? (
          <span className="text-slate-600">
            {notification.read_at ? stamp(notification.read_at) : '—'}
          </span>
        ) : (
          <button
            onClick={() => onRead(notification.id)}
            className="rounded px-2 py-0.5 text-xs text-sky-300 transition hover:bg-slate-800"
          >
            标记已读
          </button>
        )}
      </td>
    </tr>
  );
}

export function NotificationsOverlay({
  items,
  unreadCount,
  loading,
  error,
  refresh,
  markRead,
  markAllRead,
  onClose,
}: {
  items: NotificationItem[];
  unreadCount: number;
  loading: boolean;
  error: string | null;
  refresh: () => void;
  markRead: (id: string) => void;
  markAllRead: () => void;
  onClose: () => void;
}) {
  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 p-6 backdrop-blur-sm">
      <div className="max-h-[90vh] w-full max-w-4xl overflow-y-auto rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl">
        <div className="mb-1 flex items-start justify-between">
          <h1 className="text-2xl font-semibold text-white">
            通知
            {unreadCount > 0 && (
              <span className="ml-2 rounded-full bg-sky-500/20 px-2 py-0.5 text-sm text-sky-300">
                {unreadCount} 未读
              </span>
            )}
          </h1>
          <div className="flex items-center gap-2">
            <button
              onClick={markAllRead}
              disabled={unreadCount === 0}
              className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200 disabled:opacity-40"
            >
              全部已读
            </button>
            <button
              onClick={refresh}
              className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              刷新
            </button>
            <button
              onClick={onClose}
              className="rounded-lg px-2 py-1 text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              ✕
            </button>
          </div>
        </div>

        <p className="mb-6 text-sm text-slate-400">
          发给你的通知：任务完成、配额预警、提及与系统消息。列表每分钟刷新，新通知经
          WebSocket 即时追加。
        </p>

        {error && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
            {error}
            <div className="mt-1 text-xs text-red-300/80">
              这一格没有读数——不是「没有通知」。
            </div>
          </div>
        )}

        {loading && items.length === 0 && (
          <p className="text-sm text-slate-400">读取中…</p>
        )}

        {!loading && items.length > 0 && (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500">
                <th className="pb-2 pr-3 text-left font-medium"></th>
                <th className="pb-2 pr-3 text-left font-medium">标题</th>
                <th className="pb-2 pr-3 text-left font-medium">内容</th>
                <th className="pb-2 pr-3 text-right font-medium">时间</th>
                <th className="pb-2 text-right font-medium">已读</th>
              </tr>
            </thead>
            <tbody>
              {items.map((n) => (
                <Row key={n.id} notification={n} onRead={markRead} />
              ))}
            </tbody>
          </table>
        )}

        {!loading && items.length === 0 && !error && (
          <p className="text-sm text-slate-400">还没有收到通知。</p>
        )}
      </div>
    </div>
  );
}
