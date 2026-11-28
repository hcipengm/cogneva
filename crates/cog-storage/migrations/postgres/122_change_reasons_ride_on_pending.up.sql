-- Migration: 122_change_reasons_ride_on_pending
--
-- 状态只剩 `pending` / `landed` 之后，两件曾与状态绑死的事要跟着换绑：
--
-- 1. `retire_reason` 过去是「这一行 retired 的理由」，用双条件约束与 state 锁在一起
--    （`(state = 'retired') = (retire_reason IS NOT NULL)`）。`retired` 已并入 pending，
--    那条双条件会把所有带着理由的 pending 行判成非法——正好把迁移 121 刚刚搬过来的
--    那批行挡在门外。现在理由的含义是「这行的身份，按普查的判断，是账上不该持有的
--    那一类」，它是 pending 行上的判断注记，所以约束改成「理由只许挂在 pending 上」。
--
-- 2. 变更事件的 `from_state` / `to_state` 还是六个词的闭集。历史事件里写着旧词，
--    先把它们按迁移 121 的同一判决搬到 pending，再把闭集收窄到两个词——否则事件流
--    写入方一改成新词就会被旧 CHECK 拒掉，而库里旧词的事件却合法，两边读出来的
--    「这条变更怎么走的」就对不上。
--
-- `retired_at` 保留：它是「哪一次测量把它判成不该持有」，与状态无关，读者仍要它。

UPDATE change_ledger_event SET to_state = 'pending'
 WHERE to_state IN ('retired', 'undecided', 'conflict', 'partial');

UPDATE change_ledger_event SET from_state = 'pending'
 WHERE from_state IN ('retired', 'undecided', 'conflict', 'partial');

ALTER TABLE change_ledger_event
    DROP CONSTRAINT IF EXISTS change_ledger_event_to_state_check;

ALTER TABLE change_ledger_event
    ADD CONSTRAINT change_ledger_event_to_state_check
    CHECK (to_state IN ('pending', 'landed'));

ALTER TABLE change_ledger_event
    DROP CONSTRAINT IF EXISTS change_ledger_event_from_state_check;

ALTER TABLE change_ledger_event
    ADD CONSTRAINT change_ledger_event_from_state_check
    CHECK (from_state IS NULL OR from_state IN ('pending', 'landed'));

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retired_reason_check;

-- 旧词残留在主表里时这条约束一样会挡住迁移 121 搬过来的行；再搬一次是把顺序说
-- 清楚：约束的合法性依赖 121 已经跑过，而迁移按编号顺序执行。
UPDATE change_ledger SET state = 'pending'
 WHERE state NOT IN ('pending', 'landed');

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_retired_reason_check
    CHECK (retire_reason IS NULL OR state = 'pending');
