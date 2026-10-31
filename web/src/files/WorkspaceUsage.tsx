import { useCallback, useEffect, useState } from 'react';
import { getWorkspaceUsage, type WorkspaceUsage as Usage } from '@/api/client';
import { Note } from '@/ui/Note';

/**
 * 工作区配额：用了多少、还剩多少。
 *
 * 这一行存在的理由是「写入被拒」之前的那一步。上限在服务端是一条硬判据，
 * 撞上去会当场拒写并说明原因；但如果读者只有撞上那一刻才知道有上限，他就会
 * 在写了一下午之后被拦下。所以把同一份读数摆在文件树旁边。
 *
 * 每半分钟取一次，不是每次敲键都取：那是走一遍整棵树的 stat，不是查一个整数。
 */
const REFRESH_MS = 30_000;

function human(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes / 1024;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value < 10 ? value.toFixed(1) : Math.round(value)} ${units[index]}`;
}

export function WorkspaceUsage() {
  const [usage, setUsage] = useState<Usage | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setUsage(await getWorkspaceUsage());
      setError(null);
    } catch (e) {
      setError((e as { message?: string }).message ?? '读不到用量');
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [load]);

  if (error) {
    return (
      <p className="px-3 py-1 text-[10px]" style={{ color: 'var(--danger)' }}>
        {error}
      </p>
    );
  }
  if (!usage) return null;

  const unlimited = usage.limit_bytes === 0;
  const ratio = unlimited ? 0 : Math.min(1, usage.bytes / usage.limit_bytes);
  const tight = !unlimited && ratio >= 0.9;

  return (
    <div className="px-3 py-1.5">
      <div className="flex items-baseline gap-2 text-[10px] text-[var(--muted)]">
        <span className="mono" style={{ color: tight ? 'var(--danger)' : 'var(--paper-dim)' }}>
          {human(usage.bytes)}
          {unlimited ? ' · 不限' : ` / ${human(usage.limit_bytes)}`}
        </span>
        <span className="ml-auto">{usage.files} 个文件</span>
      </div>
      {!unlimited && (
        <Note
          as="div"
          className="mt-1 h-[3px] w-full overflow-hidden rounded-full"
          style={{ background: 'var(--line)' }}
          text={tight ? '快到上限了，再写会开始被拒' : undefined}
        >
          <div
            className="h-full transition-[width] duration-500"
            style={{
              width: `${Math.max(ratio * 100, usage.bytes > 0 ? 2 : 0)}%`,
              background: tight ? 'var(--danger)' : 'var(--brass)',
            }}
          />
        </Note>
      )}
    </div>
  );
}
