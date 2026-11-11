-- Migration: 098_backup_drill_attempts (down)
-- 回滚就把这块证据拿走：「这一趟开过工、结局没被看见」重新变得不可读，只剩 backup_drills
-- 里那些有结论的行。判据与账本都不依赖它，所以回滚是安全的——只是少一种读法。
DROP TABLE IF EXISTS backup_drill_attempts;
