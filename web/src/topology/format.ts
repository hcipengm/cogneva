/**
 * 拓扑这一族共用的数字写法。
 *
 * 单独一格是因为集群图、拓扑树、资源面读的是同一批读数：同一个数在两处用两种写法
 * 写出来，读者得先决定信哪一个。放在这里，谁都不必从「另一个组件的文件里」取一个
 * 与自己无关的函数——那也让这两张图互相 import，正是拿不准谁先加载的那种结构。
 */

/** 毫核写成「几核」。整数核不写小数位：`2 核` 比 `2.0 核` 少一个要读的字符。 */
export function formatCpu(millicores: number): string {
  if (millicores === 0) return '0';
  if (millicores % 1000 === 0) return `${millicores / 1000} 核`;
  return `${(millicores / 1000).toFixed(1)} 核`;
}

/** 字节按 1024 进位。两个有效数字够读：这一面比的是量级，不是账到字节。 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)} ${units[unit]}`;
}

/** 占比写成百分比。分母为零时是「算不出」，不是「用了 0%」。 */
export function percentOf(part: number, whole: number): string {
  if (whole <= 0) return '—';
  const value = (part / whole) * 100;
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)}%`;
}
