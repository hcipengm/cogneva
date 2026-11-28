-- Migration: 097_backup_drill_silence_bound (down)
-- 回滚就把这一格拿走。读数退回「只有 progress_bounds 的当前值」：界仍然是实测的，
-- 只是这一趟用过的是哪一个不再留在账上——与这一列不存在时一样。
ALTER TABLE backup_drills
    DROP CONSTRAINT IF EXISTS backup_drills_silence_bound_is_a_reading;

ALTER TABLE backup_drills
    DROP COLUMN IF EXISTS silence_bound;
