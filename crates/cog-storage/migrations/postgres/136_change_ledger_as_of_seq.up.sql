-- 变更账是一份可以从事实折回来的投影：这一列说这一行读到流上的哪个位置。
-- 0 是「还没有人折过它」——不是「折到了开头」，两者在增量重折时判得不一样。
ALTER TABLE change_ledger ADD COLUMN IF NOT EXISTS as_of_seq BIGINT NOT NULL DEFAULT 0;
