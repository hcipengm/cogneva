-- Migration: 120_progress_bounds_restored (down)
--
-- 回滚把表删掉。它是**读数**的持久面，不是事实正本：删掉之后判停退回「每个进程
-- 自己量」，代价是界不再跨重启收敛，这一点在 096 的注释里已经写明。
DROP TABLE IF EXISTS progress_bounds;
