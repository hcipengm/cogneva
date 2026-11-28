-- 回退：把约束还原成「理由 ⇔ retired」与六词闭集。
--
-- 带着理由的 pending 行在旧 schema 里无处安放：旧双条件要求 `retire_reason` 非空
-- ⇔ state='retired'。所以回退把它们搬到 retired——**这一步是有损的**，搬回去的正是
-- 「退役＝不追了」那个含义，而它们本来是 pending。事件流里的 pending 无法反向还原
-- 成原来的某一个旧词（四个词在 122 里都塌成了同一个值），因此事件只恢复闭集，不改值。

ALTER TABLE change_ledger
    DROP CONSTRAINT IF EXISTS change_ledger_retired_reason_check;

UPDATE change_ledger SET state = 'retired'
 WHERE retire_reason IS NOT NULL AND state = 'pending';

ALTER TABLE change_ledger
    ADD CONSTRAINT change_ledger_retired_reason_check
    CHECK ((state = 'retired') = (retire_reason IS NOT NULL));

ALTER TABLE change_ledger_event
    DROP CONSTRAINT IF EXISTS change_ledger_event_to_state_check;

ALTER TABLE change_ledger_event
    ADD CONSTRAINT change_ledger_event_to_state_check
    CHECK (to_state IN ('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired'));

ALTER TABLE change_ledger_event
    DROP CONSTRAINT IF EXISTS change_ledger_event_from_state_check;

ALTER TABLE change_ledger_event
    ADD CONSTRAINT change_ledger_event_from_state_check
    CHECK (from_state IS NULL
           OR from_state IN ('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired'));
