import { useResource } from '@/hooks/useResource';
import { Empty, Loading, ReadError, Table, ViewFrame } from '@/ui/primitives';
import { getJson } from '@/api/client';

/**
 * 一份「按接口原样列出」的面：标题与路径都由调用方给。
 *
 * 这些能力此前只有接口没有入口——数据在、能查、页面上点不到。与其为每一个猜一套
 * 响应结构（猜错就是一条永远空的面），不如把它读到的东西**原样**列出来：键当列名、
 * 值当单元格。诚实的地方在于：它不知道语义，也不会假装知道。
 * 画面：这一面按接口原样列出记录——键当列名、值当单元格。它不解释结构，也就不该替读者选一种图形：形状是未知的，选哪一种都是猜。
 *
 */
export function RecordsView({ path, what }: { path: string; what: string }) {
  const page = useResource(what, () => getJson(path), 30_000);
  if (page.error) return <ReadError what={what} error={page.error} onRetry={page.refresh} />;
  if (!page.data) return <Loading what={what} />;

  const rows = rowsOf(page.data);
  if (rows.length === 0) {
    return (
      <ViewFrame title={what} what={`GET ${path}`}>
        <Empty>
          这一页没有任何行。接口答的是一个空集，或者答了一个没有数组的结构——两种情形在这里
          长得一样，所以不写成「没有数据」。
        </Empty>
      </ViewFrame>
    );
  }
  const keys = Array.from(new Set(rows.flatMap((r) => Object.keys(r)))).slice(0, 8);
  return (
    <ViewFrame title={what} what={`GET ${path}`}>
      <Table
        columns={keys.map((k) => ({
          key: k,
          title: k,
          render: (r: Record<string, unknown>) => {
            const v = r[k];
            return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean'
              ? String(v)
              : v === null || v === undefined
                ? '—'
                : JSON.stringify(v).slice(0, 80);
          },
        }))}
        rows={rows}
      />
    </ViewFrame>
  );
}

/// 找到响应里第一个对象数组。接口的封装各不相同（`items` / `data.rows` / 顶层数组），
/// 这里不猜字段名，只找形状。
function rowsOf(value: unknown): Record<string, unknown>[] {
  if (Array.isArray(value)) {
    return value.filter((v): v is Record<string, unknown> => typeof v === 'object' && v !== null);
  }
  if (value && typeof value === 'object') {
    for (const v of Object.values(value as Record<string, unknown>)) {
      const found = rowsOf(v);
      if (found.length > 0) return found;
    }
  }
  return [];
}
