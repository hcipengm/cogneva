import { GRAFANA_PREFIX } from '@/observability/deepLink';

/**
 * Grafana 的一块面，嵌在 WebUI 的同一页里。
 *
 * 不用 iframe 之外的方案：图是 Grafana 画的，WebUI 不重写它的渲染层（时间范围、
 * 缩放、图例、多数据源）。这里只负责把它放进一个同源的框里，并让「观测栈没装」
 * 有一个说法——空白框会让读者以为是加载失败。
 *
 * `kiosk` 去掉 Grafana 自己的导航条，否则嵌进来的页面里还有一层菜单，人会在两层
 * 界面之间迷路。
 */
export function GrafanaFrame({
  uid,
  panelId,
  height = 520,
  title,
}: {
  uid: string;
  panelId?: number;
  height?: number;
  title: string;
}) {
  const params = new URLSearchParams({ kiosk: '' });
  if (typeof panelId === 'number') params.set('viewPanel', String(panelId));
  const src = `${GRAFANA_PREFIX}/d/${uid}?${params.toString()}`;
  return (
    <figure className="rounded border" style={{ borderColor: 'var(--line)' }}>
      <figcaption className="px-3 py-2 text-[11px]" style={{ color: 'var(--muted)' }}>
        {title}
      </figcaption>
      <iframe
        title={title}
        src={src}
        style={{ width: '100%', height, border: '0' }}
        loading="lazy"
      />
    </figure>
  );
}
