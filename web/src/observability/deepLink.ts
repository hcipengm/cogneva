/**
 * 从 WebUI 跳到 Grafana 某一块面板的链接。
 *
 * 地址是**同源的相对路径**（`/grafana/...`），不是构建期注入的主机名：
 *
 * 1. 网关把 `/grafana/*` 反代到 Grafana 并复用现有鉴权，所以换了域名也不会断——
 *    这正是当初不肯从页面 host 猜地址的原因，现在由同源解决。
 * 2. 面板地址由后端随读数给出（`panel: {uid, id}`），前端不自己写一份 uid 清单：
 *    那块面板是网关从读数注册表算出来的，清单再写一遍迟早对不上。
 */

export type DeepLink = { href: string; why: null } | { href: null; why: string };

/** 网关给 Grafana 的反代前缀。改这里等于改网关那侧的挂载点，两处同名。 */
export const GRAFANA_PREFIX = '/grafana';

/**
 * 指向一块面板的链接。
 *
 * `uid` 为空表示接口没给面板地址——那是接口的问题，不是读者的错，所以说出来。
 * `from`/`to` 让链接打开时停在 WebUI 正在读的那个窗口上，否则点过去看到的是另一段
 * 时间，两处读数对不上。
 */
export function deepLink(
  panel: { uid: string; id?: number } | null | undefined,
  window?: { from: string; to: string },
): DeepLink {
  if (!panel || !panel.uid) {
    return {
      href: null,
      why: '读数接口没有给面板地址——数在这里，往下挖要自己写 PromQL',
    };
  }
  const params = new URLSearchParams();
  if (typeof panel.id === 'number') params.set('viewPanel', String(panel.id));
  if (window) {
    params.set('from', window.from);
    params.set('to', window.to);
  }
  const query = params.toString();
  return {
    href: `${GRAFANA_PREFIX}/d/${panel.uid}${query ? `?${query}` : ''}`,
    why: null,
  };
}
