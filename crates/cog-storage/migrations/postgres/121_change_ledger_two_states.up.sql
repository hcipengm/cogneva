-- Migration: 121_change_ledger_two_states
--
-- 变更账本的状态只有两个：`pending`（这件事还欠着）与 `landed`（它不再欠）。
--
-- 从前有六个词（conflict / pending / partial / undecided / landed / retired），每多一个
-- 词就多一种把一条变更挪出批次的手法：`retired` 等于「不做也可以」，`undecided` 等于
-- 「量不到就先不算」。而批次是「账上所有未落条目」——这些词让这句话不再成立。
--
-- 归并的语义（**逐类判决，不是一刀切**）：
--   retired    → pending：退役说的是「不再追这条」，不是「这件事已经做到」。
--                把退役直接当落地是拿一句「不查了」冒充「查过了」——绝大多数退役的
--                变更并没有被做掉。它们回到 pending，理由仍留在 `retire_reason` 里，
--                读者看得出它当初为什么被放掉；真已经做到的那些，要单独按证据判 landed。
--   undecided  → pending，而且**这条待办的工作就是「把它量到」**：读数量不到（没有可比
--                的序列、窗口里没数据）不是一种结论，是一个要先解决的缺陷。`unsettled_reason`
--                必须点名缺的是哪一条读数、以及补上它要做什么——「量不到」不许停在这里。
--   conflict / partial（历史词）→ pending：没落定就是没落定。
--
-- 之后加一条 CHECK：再出现第三个词，写入当场失败，而不是让某个读的地方自己猜。

UPDATE change_ledger SET state = 'pending'
 WHERE state IN ('retired', 'undecided', 'conflict', 'partial');

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
