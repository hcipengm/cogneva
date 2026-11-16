-- Migration: 111_legacy_tables_and_columns
--
-- 085 那条规则再用一次：一张表只有被某条代码念到才留下。
--
-- `tasks` 与 `agents` 是 2026 年早期的形状。这一轮把两个名字在整个生产树和私有
-- 仓库的脚本里各搜了一遍（`FROM` / `INTO` / `UPDATE` / `JOIN` 后跟表名），一条也
-- 没有：没有 SELECT，没有 INSERT，没有 DELETE。任务图早就搬到 `task_defined` /
-- `task_executed` / `task_current`（095），表留着只让 schema 声称一套没有人能用的
-- 能力——读的人看见 `tasks` 会以为任务写在那里。
--
-- `workspaces` **不删**。没有代码念它，但 `billing_records` 有一个外键指着它
-- （`workspace_id … ON DELETE SET NULL`），而 `billing_records` 是被代码念到的
-- （`cog-quota/src/billing.rs` 的 MySQL 写入口）。在一个活表的形状上开一刀换掉一张
-- 没人读的表，是把「删掉没人用的东西」变成「改别人在用的东西」——那不是同一条规则。
-- 它的一列 `settings` 没有读者，按「库只存列」转 text。
--
-- `user_auth_methods` 也是活的：`cog-auth` 有两条 JOIN 读 `auth_type` / `auth_id`。
-- 但 `extra` 一列没有任何读者，它是引导期留下的「以后可能往这里放点什么」。
--
-- down 迁移把形状原样建回来，所以这个决定不用考古就能撤回。
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS agents;

ALTER TABLE user_auth_methods DROP COLUMN IF EXISTS extra;

-- 这张表的一列开集载荷：整块读、整块写，没有一条按 JSON 键查它。
ALTER TABLE IF EXISTS workspaces ALTER COLUMN settings TYPE TEXT USING settings::text;
