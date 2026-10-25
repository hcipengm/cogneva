/**
 * 从 WebUI 跳到 Grafana 某一块面板的链接。
 *
 * 两件事不能交给界面各自去拼：
 *
 * 1. 地址不是恒定的。Grafana 在集群里以 `grafana.<domain>` 暴露，同一份前端可能被
 *    装在别的主机名下，所以地址只从 `VITE_GRAFANA_URL` 读，不从当前页面的 host 猜——
 *    猜出来的链接在换了域名之后指向一个不存在的面板，比没有链接更糟。
 * 2. 拼不出来必须说出来。没有配置地址、或者这条读数本来就没有面板，都要给出原因，
 *    让读者知道「这里为什么点不过去」，而不是给一个点下去什么也不发生的链接。
 *
 * 面板的 uid 由后端随读数一起给出（读数接口的 `dashboard_uid`），前端不自己写一份
 * uid 清单：那份清单迟早与仪表盘即代码的文件对不上。
 */

export type DeepLink = { href: string; why: null } | { href: null; why: string };

const base = (import.meta.env.VITE_GRAFANA_URL ?? '').trim().replace(/\/+$/, '');

/** Grafana 的地址；没配置就是 null，不猜。 */
export function grafanaBase(): string | null {
  return base === '' ? null : base;
}

/**
 * 指向一块面板的链接。
 *
 * `uid` 为空表示这条读数没有对应面板——这是真话，不是错误；`from`/`to` 让链接打开时
 * 停在 WebUI 正在读的那个窗口上，否则点过去看到的是另一段时间，两处读数对不上。
 */
export function deepLink(uid: string | null | undefined, window?: { from: string; to: string }): DeepLink {
  if (!uid) {
    return {
      href: null,
      why: '这条读数没有对应的 Grafana 面板（接口没有给 dashboard_uid）——数在这里，往下挖要自己写 PromQL',
    };
  }
  const root = grafanaBase();
  if (!root) {
    return {
      href: null,
      why: `未配置 Grafana 地址（VITE_GRAFANA_URL），不可深链；面板 uid 是 \`${uid}\`，可在 Grafana 里按 uid 打开`,
    };
  }
  const params = new URLSearchParams();
  if (window) {
    params.set('from', window.from);
    params.set('to', window.to);
  }
  const query = params.toString();
  return { href: `${root}/d/${uid}${query ? `?${query}` : ''}`, why: null };
}
