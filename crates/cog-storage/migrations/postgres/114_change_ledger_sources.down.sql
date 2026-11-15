-- 撤回 114：引用回到 `change_ledger.sources` 一个数组里。
ALTER TABLE change_ledger ADD COLUMN IF NOT EXISTS sources JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE change_ledger c
   SET sources = COALESCE((
           SELECT jsonb_agg(jsonb_build_object('doc', s.doc,
                                               'verdict', s.verdict,
                                               'priority', s.priority,
                                               'block', s.block)
                            ORDER BY s.doc)
             FROM change_ledger_sources s
            WHERE s.kind = c.kind AND s.scope = c.scope AND s.literal = c.literal
       ), '[]'::jsonb);

DROP TABLE IF EXISTS change_ledger_sources;
