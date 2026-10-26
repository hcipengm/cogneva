/**
 * 一段对话的叫法。
 *
 * 并排三段对话时，三行都写着「新的对话」是没法指认的：说「左边那个」靠位置，位置
 * 一会就变了；说「对话 2」靠顺序，关掉一段之后序号全是错的。给每段一个自己带着的
 * 名字，是可以被念出来、被记住、也不随布局变化的。名字从会话 id 折出来，所以刷新
 * 之后还是同一个——它是这段对话的身份，不是这次渲染的编号。
 *
 * 折法只用 id 本身：不带时间、不带随机数。同一个 id 在任何一台打开的界面上都念作
 * 同一个名字，读者在另一台机器上看到的名字与这里一致。
 */

/** 十二个词，都短、都不同首字——念出来不会互相听混。 */
const WORDS = [
  '苍鹭',
  '渡鸦',
  '白鹤',
  '山雀',
  '雨燕',
  '麋鹿',
  '岩羊',
  '海獭',
  '灰狼',
  '赤狐',
  '雪貂',
  '石鸡',
];

/** FNV-1a：一段短字符串折成一个稳定的数，不需要密码学性质。 */
function hash(id: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < id.length; i += 1) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/** `苍鹭·3f`：一个词加两位十六进制。两位足够把并排的三五段分开，又短到能放在标题行里。 */
export function callsignOf(sessionId: string): string {
  const h = hash(sessionId);
  const word = WORDS[h % WORDS.length];
  return `${word}·${(h >>> 8).toString(16).padStart(2, '0').slice(-2)}`;
}
