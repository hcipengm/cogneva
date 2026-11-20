-- Migration: 121_change_ledger_two_states
--
-- 变更账本的状态只有两个：`pending`（这件事还欠着）与 `landed`（它不再欠）。
--
-- 从前有六个词（conflict / pending / partial / undecided / landed / retired），每多一个
-- 词就多一种把一条变更挪出批次的手法：`retired` 等于「不做也可以」，`undecided` 等于
-- 「量不到就先不算」。而批次是「账上所有未落条目」——这些词让这句话不再成立。
--
-- 归并的语义：
--   retired    → landed：它不再欠。为什么不再欠仍在 `retire_reason` 里，读得到。
--   undecided  → pending：读数没量到，不等于不欠；理由在 `unsettled_reason` 里。
--   conflict / partial（历史词）→ pending：没落定就是没落定。
--
-- 之后加一条 CHECK：再出现第三个词，写入当场失败，而不是让某个读的地方自己猜。

UPDATE change_ledger SET state = 'landed',
       landed_at = COALESCE(landed_at, retired_at, now())
 WHERE state = 'retired';

UPDATE change_ledger SET state = 'pending'
 WHERE state IN ('undecided', 'conflict', 'partial');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'change_ledger'::regclass AND conname = 'change_ledger_two_states'
    ) THEN
        ALTER TABLE change_ledger
          ADD CONSTRAINT change_ledger_two_states CHECK (state IN ('pending', 'landed'));
    END IF;
END $$;
