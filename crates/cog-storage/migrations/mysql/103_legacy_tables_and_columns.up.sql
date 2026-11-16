-- Migration: 103_legacy_tables_and_columns
--
-- PostgreSQL 侧 111 的同一条决定，在窄方言上写一遍：两张没有任何代码念到的表
-- 走掉，`user_auth_methods.extra` 这一列没有读者也跟着走。
--
-- `workspaces` 不删，理由与那边同一条：`billing_records` 与 `workspace_members`
-- 有外键指着它，而它们是被代码念到的表。它的 `settings` 转 text。
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS agents;

ALTER TABLE user_auth_methods DROP COLUMN extra;

ALTER TABLE workspaces MODIFY COLUMN settings TEXT;
