-- Migration: 105_change_ledger_sources
--
-- PostgreSQL 侧 114 的同一条决定：一份变更的引用从单元格里的数组拆成一张表。
CREATE TABLE IF NOT EXISTS change_ledger_sources (
    kind     VARCHAR(32)  NOT NULL,
    scope    VARCHAR(191) NOT NULL,
    literal  VARCHAR(512) NOT NULL,
    doc      VARCHAR(512) NOT NULL,
    verdict  TEXT         NULL,
    priority VARCHAR(16)  NOT NULL DEFAULT 'unstated',
    block    VARCHAR(191) NOT NULL DEFAULT 'none',
    PRIMARY KEY (kind, scope, literal, doc),
    CONSTRAINT fk_change_ledger_sources_ledger
        FOREIGN KEY (kind, scope, literal)
        REFERENCES change_ledger (kind, scope, literal) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO change_ledger_sources (kind, scope, literal, doc, verdict, priority, block)
SELECT c.kind, c.scope, c.literal,
       JSON_UNQUOTE(JSON_EXTRACT(jt.entry, '$.doc')),
       JSON_UNQUOTE(JSON_EXTRACT(jt.entry, '$.verdict')),
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(jt.entry, '$.priority')), 'unstated'),
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(jt.entry, '$.block')), 'none')
  FROM change_ledger c,
       JSON_TABLE(c.sources, '$[*]' COLUMNS (entry JSON PATH '$')) AS jt
 WHERE JSON_EXTRACT(jt.entry, '$.doc') IS NOT NULL
ON DUPLICATE KEY UPDATE doc = VALUES(doc);

ALTER TABLE change_ledger DROP COLUMN sources;
