-- Migration: 131_task_change_identity
-- 派发出去的那些变更身份，落成一张能按身份查的侧表。
--
-- 一个变更任务可以一次交多行身份（`input.change_identities` 是一个数组），所以它不是
-- 「一列」能装下的东西。而两跳对账（账本 ↔ 派发）要按键查：账本这一侧已经是真列
-- （`change_ledger.kind/scope/literal`），派发那一侧此前只在任务的输入文本里——读的人
-- 每次都要把那段文本解析一遍，键名打错读成 NULL 而不断言。
--
-- 写者（`insert_definition_row`）从这一刻起在写定义行时把数组落进来；这一条迁移把
-- 存量从输入文本里取出来落一次（写时取，不是查询时取）。
CREATE TABLE IF NOT EXISTS task_change_identity (
    task_id  TEXT   NOT NULL,
    rev      BIGINT NOT NULL,
    ordinal  INTEGER NOT NULL,
    kind     TEXT   NOT NULL,
    scope    TEXT   NOT NULL,
    literal  TEXT   NOT NULL,
    PRIMARY KEY (task_id, rev, ordinal)
);
INSERT INTO task_change_identity (task_id, rev, ordinal, kind, scope, literal)
SELECT d.task_id,
       d.rev,
       (e.ordinality - 1)::int,
       COALESCE(e.value ->> 'kind', ''),
       COALESCE(e.value ->> 'scope', ''),
       COALESCE(e.value ->> 'literal', '')
  FROM task_defined d,
       LATERAL jsonb_array_elements((d.input::jsonb) -> 'change_identities')
           WITH ORDINALITY AS e(value, ordinality)
 WHERE (d.input::jsonb) ? 'change_identities'
   AND jsonb_typeof((d.input::jsonb) -> 'change_identities') = 'array'
ON CONFLICT DO NOTHING;

-- 更老的写者只写单数对象 `change_identity`（现网存量的形状，与复数并列时它是数组的
-- 第一行）。两边都在就用数组——那是全量；只有单数时补这一行，否则这 15 个任务在
-- 侧表上会被读成「没有交接」，而文本读法一直认得出它们。
INSERT INTO task_change_identity (task_id, rev, ordinal, kind, scope, literal)
SELECT d.task_id,
       d.rev,
       0,
       COALESCE((d.input::jsonb) -> 'change_identity' ->> 'kind', ''),
       COALESCE((d.input::jsonb) -> 'change_identity' ->> 'scope', ''),
       COALESCE((d.input::jsonb) -> 'change_identity' ->> 'literal', '')
  FROM task_defined d
 WHERE NOT ((d.input::jsonb) ? 'change_identities')
   AND jsonb_typeof((d.input::jsonb) -> 'change_identity') = 'object'
ON CONFLICT DO NOTHING;
