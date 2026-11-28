-- 库只存列之后的第三条不变式：花了 token 的事实要挂任务。
--
-- 只能写成 `NOT VALID`：从 2026-10 到这一句之前写下的 usage 行里，有 120,258 行
-- 没挂任务、合计 157M token。它们已经落库，补一个任务名就是编一个没人观测过的事实。
-- `NOT VALID` 正是为这件事存在的形状：从这一刻起每一行都受约束，历史按历史读。
--
-- 拒绝行可以花零 token、也可以不挂任务（被熔断器挡下的一发没有任务在等它），
-- 所以条件是「花了才要挂」而不是「一律要挂」。
--
-- 表由 store 首启时建，迁移链里没有它：空库重放时这一步什么也不做，新库那边由
-- store 的 DDL 建出同一条约束。
DO $mig$
BEGIN
    IF to_regclass('gateway_llm_usage') IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'usage_that_spent_names_its_task'
           AND conrelid = 'gateway_llm_usage'::regclass
    ) THEN
        ALTER TABLE gateway_llm_usage ADD CONSTRAINT usage_that_spent_names_its_task
            CHECK (tokens_input + tokens_output <= 0 OR task_id IS NOT NULL) NOT VALID;
    END IF;
END
$mig$;
