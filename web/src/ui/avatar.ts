// 一个人的脸由他的工号决定：同一个工号永远是同一张，没有第二份真相。
//
// 头像不是文件，是算出来的。工号做 seed，画风是本机打包进来的（`@dicebear/notionists`，
// MIT），所以离线部署照常、工号不出本机、名册／任务卡／账本三处读到的是同一张。
// 换画风只改这一个函数；调用点拿的是 data URI 与底色，不关心它是怎么画出来的。

import { createAvatar } from '@dicebear/core';
import * as notionists from '@dicebear/notionists';

/// 底色只按工号从这组里取。它是设计约定（花名册用的沉稳色），不是可调的旋钮。
const BACKGROUND = [
  '#dce9f9',
  '#e6e0f5',
  '#dff1e6',
  '#fbe6d8',
  '#e3e9ee',
  '#f5e2e8',
  '#e8f0d9',
  '#e0eef0',
];

/// FNV-1a：只用来把工号摊到调色板上。同一工号永远同一个下标，不需要密码学强度。
function paletteIndex(employeeId: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < employeeId.length; i += 1) {
    hash ^= employeeId.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash % BACKGROUND.length;
}

const cache = new Map<string, Face>();

export interface Face {
  /// 内联 SVG 的 data URI。`notionists` 画的是透明底的人像。
  src: string;
  /// 这张脸垫在什么底色上：由工号决定，与画风无关。
  background: string;
}

/// 工号 → 一张脸。`size` 参与缓存键，因为同一张脸的两种尺寸是两份字符串；
/// 同尺寸重复渲染直接命中，不重画。
export function avatarFor(employeeId: string, size = 64): Face {
  const key = `${employeeId}:${size}`;
  const hit = cache.get(key);
  if (hit !== undefined) return hit;

  const svg = createAvatar(notionists, { seed: employeeId, size }).toString();
  // `utf8` + encodeURIComponent：中文名字里的字符也不必先做 base64。
  const face: Face = {
    src: `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`,
    background: BACKGROUND[paletteIndex(employeeId)],
  };
  cache.set(key, face);
  return face;
}
