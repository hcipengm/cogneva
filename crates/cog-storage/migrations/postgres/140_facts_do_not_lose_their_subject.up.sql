-- Migration: 140_facts_do_not_lose_their_subject
-- 审计与账单这两类**事实**行，不能在主体被删时被改写成「没有主体」。
--
-- 它们在建表时挂了 `ON DELETE SET NULL`：删一个用户，审计行里「谁做的」变成 NULL；
-- 删一个工作区，账单行里「记在谁头上」变成 NULL。行还在，事实没了——而这两张表正是
-- 用来回答「谁、在哪、做了什么」的。库里这一侧的有损普查（`scripts/db-loss-census.py`）
-- 把这类引用动作当作默认有损的人口，这两条是它点出来的。
--
-- 修法不是找一个理由留下它：审计与账单的主体标识即使主体行被删除也必须留存，所以
-- 外键连同它的引用动作一起去掉，列原样保留（写入时仍按当时的 id 记）。
ALTER TABLE IF EXISTS audit_logs
    DROP CONSTRAINT IF EXISTS audit_logs_user_id_fkey;
ALTER TABLE IF EXISTS billing_records
    DROP CONSTRAINT IF EXISTS billing_records_workspace_id_fkey;

-- 文件属于工作区：今天没有任何一条删除工作区的路径，SET NULL 唯一的作用是让一次
-- 意外删除把文件的归属悄悄抹掉。改成 RESTRICT——要删工作区，先把它的文件处理掉
-- （删除或迁移），这是一次显式的决定，不是一次级联。
ALTER TABLE IF EXISTS files
    DROP CONSTRAINT IF EXISTS files_workspace_id_fkey;
ALTER TABLE IF EXISTS files
    ADD CONSTRAINT files_workspace_id_fkey
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE RESTRICT;
