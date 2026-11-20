-- Migration: 121_change_ledger_two_states (down)
--
-- 回滚只拆掉约束。被归并过的行**不还原**：`retired` 与 `undecided` 的原始归属已经写进
-- `retire_reason` / `unsettled_reason`，把它们再拆回去只是换一个字段名读同一件事。
ALTER TABLE change_ledger DROP CONSTRAINT IF EXISTS change_ledger_two_states;
