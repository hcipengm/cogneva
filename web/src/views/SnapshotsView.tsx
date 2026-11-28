import { getSnapshotUrl, listSnapshots } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError, Table, ViewFrame } from '@/ui/primitives';

/**
 * 快照面：任务在哪个事件偏移上留过一份可回到的点。
 *
 * 这一面此前只有接口：快照存着、能列、能取，界面上没有入口。空态要说得清——
 * 「没有快照」与「读到了空集」在这里长得一样，而前者说明没人在跑任务。
 * 画面：这一面列的是检查点，一行一个可回到的点；它回答「哪一步之后有落点」，不是分布。同一任务的多个快照在时间上相邻，成图只会把「有没有落点」画成一堆点。
 *
 */
export function SnapshotsView() {
  const page = useResource('快照', () => listSnapshots({ limit: 100 }), 30_000);
  // 快照不是谁写出来的，是任务跑到某一步留下的点：读它该订任务那一族帧，而不是
  // 等 30 秒一拍。任务在跑的时候，新落点跟着任务帧一起到。
  useKindTickedRefresh(['tasks'], page.refresh);
  const appendOutput = useShell((s) => s.appendOutput);

  /**
   * 取一份快照的取回地址。地址由观测后端现签，过了有效期就取不回来——所以它被
   * 念在输出格里，而不是挂在页面上当链接：一条看起来永久、点下去 403 的地址比
   * 没有更坏。
   */
  const fetchUrl = async (snapshotId: string) => {
    try {
      const res = await getSnapshotUrl(snapshotId);
      appendOutput(`快照 ${snapshotId} 的取回地址：${res.url}（现签，可能过期）`);
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`取快照 ${snapshotId} 的地址失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    }
  };

  if (page.error) return <ReadError what="快照" error={page.error} onRetry={page.refresh} />;
  if (!page.data) return <Loading what="快照" />;
  const rows = page.data.snapshots ?? [];
  return (
    <ViewFrame title="快照" meta="任务在事件流上的检查点；回到某一步从这里取">
      {rows.length === 0 ? (
        <Empty>没有任何快照。这一面只读检查点表——没有行说明没有任务留下过可回的点。</Empty>
      ) : (
        <Table
          columns={[
            { key: 'id', title: '快照', render: (r) => r.snapshot_id },
            { key: 'task', title: '任务', render: (r) => r.task_id },
            { key: 'off', title: '事件偏移', width: '110px', render: (r) => String(r.event_offset) },
            { key: 'at', title: '时间', width: '190px', render: (r) => r.timestamp },
            {
              key: 'url',
              title: '',
              width: '90px',
              render: (r) => <Button onClick={() => void fetchUrl(r.snapshot_id)}>取地址</Button>,
            },
          ]}
          rows={rows}
          rowKey={(r) => r.snapshot_id}
        />
      )}
    </ViewFrame>
  );
}
