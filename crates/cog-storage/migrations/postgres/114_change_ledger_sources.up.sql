-- Migration: 114_change_ledger_sources
--
-- 一份变更的「谁要它」原来存成 `sources` 里的一个对象数组：四个键写在单元格
-- 里，被 `->>'block'` 数、被 `->>'doc'` 筛。那是一张表写进一个单元格——按它查
-- 建不了索引，一份文档改口要重写整个数组，两个写者各改一份文档就是互相覆盖。
--
-- 拆成一张表：每一份引用是一行，`doc` 是身份的一部分。读者要的那个数组仍然
-- 拿得到，但它是**读的时候**从这张表拼出来的，库里没有第二份——所以没有第二份
-- 可以漂移。
CREATE TABLE IF NOT EXISTS change_ledger_sources (
    kind     VARCHAR(32)  NOT NULL,
    scope    VARCHAR(191) NOT NULL,
    literal  VARCHAR(512) NOT NULL,
    doc      VARCHAR(512) NOT NULL,
    verdict  TEXT,
    priority VARCHAR(16)  NOT NULL DEFAULT 'unstated',
    block    VARCHAR(191) NOT NULL DEFAULT 'none',
    PRIMARY KEY (kind, scope, literal, doc),
    FOREIGN KEY (kind, scope, literal)
        REFERENCES change_ledger (kind, scope, literal) ON DELETE CASCADE
);

-- 回填：数组里的每一项变成一行。缺省的键按写入端当时的读法补上（`COALESCE(...,
-- 'none')` 是原来那条 CASE 用的同一个默认），文档重复的按身份合并成一行。
INSERT INTO change_ledger_sources (kind, scope, literal, doc, verdict, priority, block)
SELECT c.kind, c.scope, c.literal,
       entry ->> 'doc',
       entry ->> 'verdict',
       COALESCE(entry ->> 'priority', 'unstated'),
       COALESCE(entry ->> 'block', 'none')
  FROM change_ledger c,
       jsonb_array_elements(c.sources) AS entry
 WHERE entry ->> 'doc' IS NOT NULL
ON CONFLICT (kind, scope, literal, doc) DO NOTHING;

-- `priority_rank` 与 `block` 是两列**生成列**，表达式读的正是 `sources`
-- （069 与 087）。生成列不能引用别的表，所以它们不能跟着搬到引用表上：要先把
-- 它们换成普通列，再让写引用的人在同一条事务里重算——那句话现在只有一处
-- （`change_ledger_store` 的 `recount_sql`），任何写引用的路径都在那里过一遍。
--
-- 两列的计算本身不重写：`change_ledger_priority_rank(sources jsonb)` 与
-- `change_ledger_block(sources jsonb)` 原样留着，重算时把引用表的行拼回它们认得
-- 的那个数组——一个映射只有一份。
DROP INDEX IF EXISTS change_ledger_outstanding_order_idx;
ALTER TABLE change_ledger DROP COLUMN IF EXISTS priority_rank;
ALTER TABLE change_ledger ADD COLUMN priority_rank SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE change_ledger DROP COLUMN IF EXISTS block;
ALTER TABLE change_ledger ADD COLUMN block TEXT NOT NULL DEFAULT 'none';

UPDATE change_ledger c
   SET priority_rank = change_ledger_priority_rank(sub.citations),
       block = change_ledger_block(sub.citations)
  FROM (
        SELECT s.kind, s.scope, s.literal,
               COALESCE(jsonb_agg(jsonb_build_object('priority', s.priority, 'block', s.block)),
                        '[]'::jsonb) AS citations
          FROM change_ledger_sources s
         GROUP BY s.kind, s.scope, s.literal
       ) sub
 WHERE c.kind = sub.kind AND c.scope = sub.scope AND c.literal = sub.literal;

CREATE INDEX IF NOT EXISTS change_ledger_outstanding_order_idx
    ON change_ledger (priority_rank DESC, measured_at ASC NULLS FIRST, first_seen_at ASC)
    WHERE state NOT IN ('landed', 'retired');

ALTER TABLE change_ledger DROP COLUMN sources;
