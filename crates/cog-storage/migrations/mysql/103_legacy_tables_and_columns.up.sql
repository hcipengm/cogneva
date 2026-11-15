-- Migration: 103_legacy_tables_and_columns
--
-- PostgreSQL 侧 111 的同一条决定，在窄方言上写一遍：三张没有任何代码念到的表
-- 走掉，`user_auth_methods.extra` 这一列没有读者也跟着走。
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS agents;
DROP TABLE IF EXISTS workspaces;

ALTER TABLE user_auth_methods DROP COLUMN extra;
