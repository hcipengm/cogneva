-- Migration: 111_legacy_tables_and_columns
--
-- 085 那条规则再用一次：一张表只有被某条代码念到才留下。
--
-- `tasks` / `agents` / `workspaces` 是 2026 年早期的形状。这一轮把三个名字在
-- 整个生产树和私有仓库的脚本里各搜了一遍（`FROM` / `INTO` / `UPDATE` / `JOIN`
-- 后跟表名），一条也没有：没有 SELECT，没有 INSERT，没有 DELETE。任务图早就搬到
-- `task_defined` / `task_executed` / `task_current`（095），表留着只让 schema
-- 声称一套没有人能用的能力——读的人看见 `tasks` 会以为任务写在那里。
--
-- `user_auth_methods` 是活的：`cog-auth` 有两条 JOIN 读 `auth_type` / `auth_id`。
-- 但 `extra` 一列没有任何读者，它是引导期留下的「以后可能往这里放点什么」。
-- 一列没人读的 JSON 不是余地，是把一段没有 schema 的状态放进库里：按它查会
-- 打错键返回零行，改它的两个写者会互相覆盖。
--
-- down 迁移把形状原样建回来，所以这个决定不用考古就能撤回。
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS agents;
DROP TABLE IF EXISTS workspaces;

ALTER TABLE user_auth_methods DROP COLUMN IF EXISTS extra;
