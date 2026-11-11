-- Migration: 096_progress_bounds (down)
-- 回滚就是把这个读数面拿走：作业类回到「没有实测」，判停不成立。这比让它退回一个
-- 写死的钟更接近这套判据的本意，所以 down 不重建任何默认值。
DROP TABLE IF EXISTS progress_bounds;
