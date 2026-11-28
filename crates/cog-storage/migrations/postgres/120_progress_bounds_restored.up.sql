-- Migration: 120_progress_bounds_restored
--
-- 判停的界要跨进程记住：`progress_bounds` 由 096 建过，而现网 `schema_migrations`
-- 记着 96 已应用、表却不在（2026-11-19 实测：121 张表里没有它，相邻的
-- `backup_drill_attempts` 在）。没有这张表，每个新进程都从零开始量静默，界只能
-- 停在一个采样格上——而那正是「构建被判停」那类误杀的根。
--
-- 所以这里只做一件事：把表建回来。形状与 096 逐列相同，`IF NOT EXISTS` 让它在
-- 表还在的库上是空操作。

CREATE TABLE IF NOT EXISTS progress_bounds (
    job_class            TEXT PRIMARY KEY,
    longest_gap_seconds  DOUBLE PRECISION NOT NULL CHECK (longest_gap_seconds > 0),
    samples              BIGINT NOT NULL CHECK (samples >= 0),
    observed_at          TIMESTAMPTZ NOT NULL
);
